%% @doc Real-UDP mDNS transport — IPv6 multicast via `gen_udp'.
%%
%% Sends a single mDNS query to the link-local group
%% `ff02::fb` on port 5353 and collects unicast responses until the
%% timeout elapses. Each reply's source address + raw DNS bytes are
%% returned to the caller for parsing.
%%
%% == Multicast socket options ==
%% <ul>
%%   <li>`inet6' — IPv6 socket.</li>
%%   <li>`multicast_ttl => 1' — link-local only; don't route.</li>
%%   <li>`multicast_loop => false' — do not receive our own query
%%       back as a loopback copy.</li>
%%   <li>`reuseaddr => true' — allow parallel probes on busy hosts.
%%       The station's steady-state mDNS responder (when we add one)
%%       can run on port 5353 alongside, but this transport binds to
%%       an ephemeral port for probing only.</li>
%% </ul>
%%
%% == Scope-id handling ==
%%
%% On multi-interface hosts the kernel chooses the egress interface.
%% IPv6 scope-ids (required to reach link-local addresses on a
%% specific interface) are not yet plumbed here; GUA peers work
%% today, link-local peers will once we add interface selection via
%% `{multicast_if, IfAddr}'. Tracked as a Phase 6.4.x follow-up.
-module(hecate_bootstrap_mdns_udp).
-behaviour(hecate_bootstrap_mdns_transport).

-export([query/2]).

-spec query(binary(), pos_integer()) ->
            [hecate_bootstrap_mdns_transport:reply()].
query(QueryBin, TimeoutMs) ->
    with_socket(fun(Sock) -> send_and_collect(Sock, QueryBin, TimeoutMs) end).

with_socket(Fun) ->
    Opts = [inet6, binary, {active, false},
            {multicast_ttl, 1}, {multicast_loop, false},
            {reuseaddr, true}],
    dispatch(gen_udp:open(0, Opts), Fun).

dispatch({ok, Sock}, Fun) ->
    try Fun(Sock)
    after gen_udp:close(Sock)
    end;
dispatch({error, _}, _Fun) ->
    [].

send_and_collect(Sock, QueryBin, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    ok_or_empty(
      gen_udp:send(Sock,
                   hecate_bootstrap_mdns:multicast_group(),
                   hecate_bootstrap_mdns:multicast_port(),
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
