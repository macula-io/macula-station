%% @doc Loopback-only admin HTTP API.
%%
%% Opens a plain TCP listen socket bound to the configured
%% `#admin_cfg.bind' + `#admin_cfg.port' and serves a tiny HTTP/1.1
%% surface suitable for `curl' or an operator's dashboard:
%%
%% <table>
%%   <tr><th>Method</th><th>Path</th><th>Body</th></tr>
%%   <tr><td>GET</td>  <td>/status</td>          <td>overall health snapshot</td></tr>
%%   <tr><td>GET</td>  <td>/dht/stats</td>       <td>routing-table size + self_id</td></tr>
%%   <tr><td>GET</td>  <td>/swim/members</td>    <td>current SWIM member list</td></tr>
%%   <tr><td>POST</td> <td>/bootstrap/rerun</td> <td>trigger a re-cascade</td></tr>
%% </table>
%%
%% TLS + client-cert auth are deferred to Phase 7; plan §8.6 scopes
%% this to loopback-only. An operator wanting remote access is
%% expected to front this with `ssh -L' for now.
%%
%% The gen_server owns the listen socket and a single acceptor child;
%% each incoming connection is handled in a spawned worker process so
%% the acceptor never blocks on a slow client.
-module(hecate_station_admin).
-behaviour(gen_server).

-include("hecate_station_cfg.hrl").

-export([start_link/1, stop/1, listen_port/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{admin := admin_cfg()}.

-record(state, {
    bind     :: inet:ip_address() | string(),
    port     :: inet:port_number(),
    listen   :: gen_tcp:socket(),
    acceptor :: pid()
}).

%%==================================================================
%% API
%%==================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc The OS-assigned port. Useful for tests that bound `port = 0'.
-spec listen_port(pid()) -> inet:port_number().
listen_port(Pid) -> gen_server:call(Pid, listen_port).

%%==================================================================
%% gen_server
%%==================================================================

init(#{admin := #admin_cfg{bind = Bind, port = Port}}) ->
    process_flag(trap_exit, true),
    on_listen(open_listen(Bind, Port), Bind).

open_listen(Bind, Port) ->
    gen_tcp:listen(Port, [
        binary,
        {ip,        to_ip(Bind)},
        {reuseaddr, true},
        {active,    false},
        {packet,    http_bin}
    ]).

on_listen({ok, Listen}, Bind) ->
    {ok, ActualPort} = inet:port(Listen),
    Self = self(),
    Acceptor = spawn_link(fun() -> accept_loop(Self, Listen) end),
    {ok, #state{bind = Bind, port = ActualPort,
                listen = Listen, acceptor = Acceptor}};
on_listen({error, Reason}, _Bind) ->
    {stop, {listen_failed, Reason}}.

handle_call(listen_port, _From, #state{port = P} = S) ->
    {reply, P, S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) -> {noreply, S}.

handle_info({'EXIT', Pid, _Reason}, #state{acceptor = Pid,
                                            listen = Listen} = S) ->
    %% Acceptor died — respawn it so one bad request does not take
    %% the admin API offline.
    Self = self(),
    New  = spawn_link(fun() -> accept_loop(Self, Listen) end),
    {noreply, S#state{acceptor = New}};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, #state{listen = L}) ->
    _ = catch gen_tcp:close(L),
    ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%==================================================================
%% Accept loop + per-connection worker
%%==================================================================

accept_loop(Parent, Listen) ->
    dispatch_accept(gen_tcp:accept(Listen), Parent, Listen).

dispatch_accept({ok, Sock}, Parent, Listen) ->
    _ = spawn(fun() -> handle_conn(Sock) end),
    accept_loop(Parent, Listen);
dispatch_accept({error, closed}, _Parent, _Listen) ->
    ok;
dispatch_accept({error, _Reason}, Parent, Listen) ->
    accept_loop(Parent, Listen).

handle_conn(Sock) ->
    case read_request(Sock) of
        {ok, Req}          -> send_response(Sock, route(Req));
        {error, _Reason}   -> send_response(Sock, bad_request())
    end,
    _ = catch gen_tcp:close(Sock),
    ok.

%%==================================================================
%% HTTP request parser — relies on `{packet, http_bin}' for the
%% start-line + headers, then switches to raw for the body.
%%==================================================================

-type req() :: #{method := binary(),
                 path   := binary(),
                 body   := binary()}.

-spec read_request(gen_tcp:socket()) -> {ok, req()} | {error, term()}.
read_request(Sock) ->
    on_start_line(gen_tcp:recv(Sock, 0, 5_000), Sock).

on_start_line({ok, {http_request, Method, {abs_path, Path}, _Vsn}}, Sock) ->
    on_headers(read_headers(Sock, #{}), Sock, Method, Path);
on_start_line({ok, _Other}, _Sock) ->
    {error, bad_start_line};
on_start_line({error, _} = E, _Sock) ->
    E.

on_headers({ok, Headers}, Sock, Method, Path) ->
    finish_body(Sock, Method, Path, Headers);
on_headers({error, _} = E, _Sock, _Method, _Path) ->
    E.

read_headers(Sock, Acc) ->
    read_headers_step(gen_tcp:recv(Sock, 0, 5_000), Sock, Acc).

read_headers_step({ok, http_eoh}, _Sock, Acc) ->
    {ok, Acc};
read_headers_step({ok, {http_header, _, Field, _, Value}}, Sock, Acc) ->
    read_headers(Sock, Acc#{header_key(Field) => Value});
read_headers_step({ok, _Other}, _Sock, _Acc) ->
    {error, bad_header};
read_headers_step({error, _} = E, _Sock, _Acc) ->
    E.

header_key(K) when is_atom(K)   -> string:lowercase(atom_to_binary(K, utf8));
header_key(K) when is_binary(K) -> string:lowercase(K).

finish_body(Sock, Method, Path, Headers) ->
    Len = content_length(Headers),
    assemble_request(read_body(Sock, Len), Method, Path).

content_length(Headers) ->
    parse_len(maps:get(<<"content-length">>, Headers, <<"0">>)).

parse_len(B) when is_binary(B) ->
    parse_len(binary_to_list(B));
parse_len(L) when is_list(L) ->
    case string:to_integer(L) of
        {N, _} when is_integer(N), N >= 0 -> N;
        _                                  -> 0
    end.

read_body(_Sock, 0) -> {ok, <<>>};
read_body(Sock, N) ->
    ok = inet:setopts(Sock, [{packet, raw}]),
    gen_tcp:recv(Sock, N, 5_000).

assemble_request({ok, Body}, Method, Path) ->
    {ok, #{method => to_method_bin(Method),
           path   => to_bin(Path),
           body   => to_bin(Body)}};
assemble_request({error, _} = E, _Method, _Path) ->
    E.

to_method_bin(M) when is_atom(M)   -> string:uppercase(atom_to_binary(M, utf8));
to_method_bin(M) when is_binary(M) -> string:uppercase(M).

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L)   -> iolist_to_binary(L).

%%==================================================================
%% Route dispatch
%%==================================================================

-type response() :: #{status := pos_integer(),
                      body   := iodata(),
                      content_type := binary()}.

-spec route(req()) -> response().
route(#{method := <<"GET">>,  path := <<"/status">>})          -> status();
route(#{method := <<"GET">>,  path := <<"/dht/stats">>})       -> dht_stats();
route(#{method := <<"GET">>,  path := <<"/swim/members">>})    -> swim_members();
route(#{method := <<"POST">>, path := <<"/bootstrap/rerun">>}) -> bootstrap_rerun();
route(_)                                                        -> not_found().

%%------------------------------------------------------------------
%% Handlers
%%------------------------------------------------------------------

status() ->
    Runtime = runtime_snapshot(),
    json_response(200, #{
        healthy     => maps:get(healthy, Runtime),
        node_id     => maps:get(node_id, Runtime),
        listen_addr => maps:get(listen_addr, Runtime),
        dht         => #{size => maps:get(dht_size, Runtime)},
        swim        => #{members => maps:get(swim_count, Runtime)},
        realms      => maps:get(realm_ids, Runtime),
        version     => hecate_station:version()
    }).

dht_stats() ->
    case hecate_station:dht() of
        {ok, Dht} ->
            json_response(200, #{
                size          => hecate_dht:size(Dht),
                self_id       => hex(hecate_dht:self_id(Dht)),
                bucket_count  => hecate_dht:bucket_count(Dht)
            });
        _ ->
            json_response(503, not_started_body())
    end.

swim_members() ->
    case hecate_station:swim() of
        {ok, Swim} ->
            Members = [member_json(M) || M <- hecate_swim:members(Swim)],
            json_response(200, #{members => Members});
        _ ->
            json_response(503, not_started_body())
    end.

bootstrap_rerun() ->
    run_rebootstrap(hecate_station:dht()).

run_rebootstrap({ok, Dht}) ->
    classify_bootstrap(hecate_station_bootstrap_runner:run(Dht));
run_rebootstrap(_) ->
    json_response(503, not_started_body()).

classify_bootstrap({ok, #{summary := Summary}}) ->
    json_response(200, #{result => <<"ok">>,
                         summary => summary_to_json(Summary)});
classify_bootstrap({error, Reason}) ->
    json_response(409, #{result => <<"error">>,
                         reason => reason_to_bin(Reason)}).

summary_to_json(#{observed := Obs, admitted := A, touched := T,
                  replaced := R, rejected := J}) ->
    #{observed => Obs, admitted => A, touched => T,
      replaced => R, rejected => J}.

member_json(#{node_id := N, state := S, last_seen := LS, since := Since}) ->
    #{node_id   => hex(N),
      state     => atom_to_binary(S, utf8),
      last_seen => LS,
      since     => Since}.

not_found() ->
    json_response(404, #{result => <<"error">>, reason => <<"not_found">>}).

bad_request() ->
    json_response(400, #{result => <<"error">>, reason => <<"bad_request">>}).

not_started_body() ->
    #{result => <<"error">>, reason => <<"not_started">>}.

reason_to_bin(no_tiers)                           -> <<"no_tiers">>;
reason_to_bin({bootstrap_failed, _})              -> <<"bootstrap_failed">>.

%%==================================================================
%% Runtime snapshot — composes /status
%%==================================================================

runtime_snapshot() ->
    Dht  = hecate_station:dht(),
    Swim = hecate_station:swim(),
    Listener = hecate_station:listener(),
    #{
        healthy     => (is_ok(Dht) andalso is_ok(Swim) andalso is_ok(Listener)),
        node_id     => snapshot_self_id(Dht),
        listen_addr => snapshot_listen_addr(),
        dht_size    => snapshot_size(Dht),
        swim_count  => snapshot_member_count(Swim),
        realm_ids   => snapshot_realm_ids()
    }.

is_ok({ok, _}) -> true;
is_ok(_)       -> false.

snapshot_self_id({ok, Dht}) -> hex(hecate_dht:self_id(Dht));
snapshot_self_id(_)         -> null.

snapshot_size({ok, Dht}) -> hecate_dht:size(Dht);
snapshot_size(_)         -> 0.

snapshot_member_count({ok, Swim}) -> length(hecate_swim:members(Swim));
snapshot_member_count(_)          -> 0.

snapshot_listen_addr() ->
    encode_addr(hecate_station:listen_addr()).

encode_addr({Bind, Port}) when is_integer(Port) ->
    iolist_to_binary(io_lib:format("~s:~B", [Bind, Port]));
encode_addr(_) ->
    null.

snapshot_realm_ids() ->
    [hex(hecate_station_realm:realm_id(P)) ||
        P <- hecate_station:realms()].

%%==================================================================
%% Response encoding
%%==================================================================

json_response(Status, Body) ->
    #{status       => Status,
      body         => json:encode(Body),
      content_type => <<"application/json">>}.

-spec send_response(gen_tcp:socket(), response()) -> ok | {error, term()}.
send_response(Sock, #{status := Status, body := Body,
                      content_type := CT}) ->
    Reason = status_text(Status),
    Len    = iolist_size(Body),
    Head = [
        <<"HTTP/1.1 ">>, integer_to_binary(Status), <<" ">>, Reason,
        <<"\r\nContent-Type: ">>, CT,
        <<"\r\nContent-Length: ">>, integer_to_binary(Len),
        <<"\r\nConnection: close\r\n\r\n">>
    ],
    gen_tcp:send(Sock, [Head, Body]).

status_text(200) -> <<"OK">>;
status_text(400) -> <<"Bad Request">>;
status_text(404) -> <<"Not Found">>;
status_text(409) -> <<"Conflict">>;
status_text(503) -> <<"Service Unavailable">>.

%%==================================================================
%% Helpers
%%==================================================================

to_ip(Addr) when is_tuple(Addr) -> Addr;
to_ip(Addr) when is_list(Addr) ->
    {ok, IP} = inet:parse_address(Addr),
    IP.

hex(Bin) when is_binary(Bin) ->
    iolist_to_binary([io_lib:format("~2.16.0b", [B]) || <<B>> <= Bin]).
