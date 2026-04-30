%% @doc Real-UDP mDNS transport — IPv6 multicast via `gen_udp'.
%%
%% == Single-interface path (`query/2') ==
%%
%% Opens one socket with the kernel-chosen egress interface, sends a
%% single mDNS query to the link-local group `ff02::fb' on port 5353,
%% and collects unicast responses until the timeout elapses. Each
%% reply's source address + raw DNS bytes are returned for parsing.
%%
%% Multicast socket options:
%% <ul>
%%   <li>`inet6' — IPv6 socket.</li>
%%   <li>`multicast_ttl => 1' — link-local only; don't route.</li>
%%   <li>`multicast_loop => false' — do not receive our own query
%%       back as a loopback copy.</li>
%%   <li>`reuseaddr => true' — allow parallel probes on busy hosts.</li>
%% </ul>
%%
%% == Multi-interface fan-out (`query/3') ==
%%
%% Phase 6.4.y: multi-NIC hosts (laptops with wifi + wired + VPN;
%% beam nodes with `enp*' + `docker0' + `br-*') benefit from sending
%% the probe out of EACH eligible interface rather than relying on
%% the kernel's default pick. `query/3' takes an opts map with:
%%
%% <ul>
%%   <li>`interfaces' — list of
%%       `macula_bootstrap_mdns_ifaces:iface()' maps, OR the atom
%%       `default' (kernel picks — identical to `query/2').</li>
%%   <li>`socket_opener' — `fun(iface() | default) ->
%%       {ok, Sock} | {error, _}'. Pluggable for tests. Default
%%       opens a real `gen_udp' socket with the
%%       `IPV6_MULTICAST_IF' raw option bound to the interface's
%%       `index' field on Linux; falls through to kernel-default
%%       when `index' is `undefined'.</li>
%% </ul>
%%
%% Fan-out spawns one short-lived worker per interface, each with
%% its own socket. Replies are merged in the order workers finish.
%% The total wall-clock budget is bounded by `TimeoutMs' — a slow
%% interface never blocks a fast one.
-module(macula_bootstrap_mdns_udp).
-behaviour(macula_bootstrap_mdns_transport).

-export([query/2, query/3]).

-export_type([opts/0, socket_opener/0]).

-type iface() :: macula_bootstrap_mdns_ifaces:iface().

-type socket_opener() :: fun((iface() | default) ->
    {ok, gen_udp:socket()} | {error, term()}).

-type opts() :: #{
    interfaces    => [iface()] | default,
    socket_opener => socket_opener()
}.

%% IPPROTO_IPV6 + IPV6_MULTICAST_IF on Linux. Other platforms may
%% differ — the raw option is best-effort; a failure drops through
%% to the kernel's default egress pick.
-define(IPPROTO_IPV6,        41).
-define(IPV6_MULTICAST_IF,   17).

%%==================================================================
%% API
%%==================================================================

-spec query(binary(), pos_integer()) ->
            [macula_bootstrap_mdns_transport:reply()].
query(QueryBin, TimeoutMs) ->
    query(QueryBin, TimeoutMs, #{}).

-spec query(binary(), pos_integer(), opts()) ->
            [macula_bootstrap_mdns_transport:reply()].
query(QueryBin, TimeoutMs, Opts) ->
    dispatch(interfaces(Opts), QueryBin, TimeoutMs, Opts).

interfaces(#{interfaces := I}) -> I;
interfaces(_)                  -> default.

dispatch(default, QueryBin, TimeoutMs, Opts) ->
    run_one(default, QueryBin, TimeoutMs, Opts);
dispatch([],     _QueryBin,  _TimeoutMs, _Opts) ->
    [];
dispatch([Single], QueryBin, TimeoutMs, Opts) ->
    run_one(Single, QueryBin, TimeoutMs, Opts);
dispatch(Ifaces, QueryBin, TimeoutMs, Opts) when is_list(Ifaces) ->
    fan_out(Ifaces, QueryBin, TimeoutMs, Opts).

%%==================================================================
%% Fan-out
%%==================================================================

fan_out(Ifaces, QueryBin, TimeoutMs, Opts) ->
    Parent  = self(),
    Workers = [spawn_worker(Parent, Iface, QueryBin, TimeoutMs, Opts)
               || Iface <- Ifaces],
    collect(Workers, [], TimeoutMs + 500).

spawn_worker(Parent, Iface, QueryBin, TimeoutMs, Opts) ->
    spawn_link(fun() ->
        Replies = run_one(Iface, QueryBin, TimeoutMs, Opts),
        Parent ! {?MODULE, self(), Replies}
    end).

collect([], Acc, _Budget) ->
    lists:append(lists:reverse(Acc));
collect(Workers, Acc, Budget) when Budget =< 0 ->
    [exit(W, kill) || W <- Workers, is_process_alive(W)],
    lists:append(lists:reverse(Acc));
collect(Workers, Acc, Budget) ->
    Start = erlang:monotonic_time(millisecond),
    receive
        {?MODULE, From, Replies} ->
            Elapsed = erlang:monotonic_time(millisecond) - Start,
            collect(Workers -- [From], [Replies | Acc], Budget - Elapsed)
    after Budget ->
        [exit(W, kill) || W <- Workers, is_process_alive(W)],
        lists:append(lists:reverse(Acc))
    end.

%%==================================================================
%% Single-socket execution (used by both single + fan-out paths)
%%==================================================================

run_one(Iface, QueryBin, TimeoutMs, Opts) ->
    Opener = maps:get(socket_opener, Opts, fun default_opener/1),
    on_opened(Opener(Iface), QueryBin, TimeoutMs).

on_opened({ok, Sock}, QueryBin, TimeoutMs) ->
    try send_and_collect(Sock, QueryBin, TimeoutMs)
    after gen_udp:close(Sock)
    end;
on_opened({error, _}, _QueryBin, _TimeoutMs) ->
    [].

default_opener(default) ->
    gen_udp:open(0, base_opts());
default_opener(#{} = Iface) ->
    resolve_opener(gen_udp:open(0, base_opts()), Iface).

resolve_opener({ok, Sock} = Ok, Iface) ->
    _ = maybe_bind_multicast_if(Sock, Iface),
    Ok;
resolve_opener({error, _} = E, _Iface) ->
    E.

base_opts() ->
    [inet6, binary, {active, false},
     {multicast_ttl, 1}, {multicast_loop, false},
     {reuseaddr, true}].

%% Best-effort per-interface pin. A failure (non-Linux host,
%% permission denied, etc.) drops through to the kernel's default
%% egress pick — the fan-out then effectively collapses to a
%% single-interface probe, still correct but without the spread.
maybe_bind_multicast_if(_Sock, #{index := undefined}) ->
    ok;
maybe_bind_multicast_if(Sock, #{index := Idx}) when is_integer(Idx) ->
    _ = inet:setopts(Sock,
        [{raw, ?IPPROTO_IPV6, ?IPV6_MULTICAST_IF, <<Idx:32/native>>}]),
    ok;
maybe_bind_multicast_if(_Sock, _Iface) ->
    ok.

send_and_collect(Sock, QueryBin, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    ok_or_empty(
      gen_udp:send(Sock,
                   macula_bootstrap_mdns:multicast_group(),
                   macula_bootstrap_mdns:multicast_port(),
                   QueryBin),
      Sock, Deadline).

ok_or_empty(ok, Sock, Deadline)         -> recv_until(Sock, Deadline, []);
ok_or_empty({error, _}, _Sock, _Dl)     -> [].

recv_until(Sock, Deadline, Acc) ->
    Remaining = max(Deadline - erlang:monotonic_time(millisecond), 0),
    handle_recv(gen_udp:recv(Sock, 65535, Remaining), Sock, Deadline, Acc).

handle_recv({ok, {Addr, _Port, Bin}}, Sock, Deadline, Acc) ->
    recv_until(Sock, Deadline, [{Addr, Bin} | Acc]);
handle_recv({error, _}, _Sock, _Deadline, Acc) ->
    lists:reverse(Acc).
