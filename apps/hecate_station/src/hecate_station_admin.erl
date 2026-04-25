%% @doc Loopback-only admin HTTP API.
%%
%% Opens a plain TCP listen socket bound to the configured
%% `#admin_cfg.bind' + `#admin_cfg.port' and serves a tiny HTTP/1.1
%% surface suitable for `curl' or an operator's dashboard:
%%
%% <table>
%%   <tr><th>Method</th><th>Path</th><th>Auth</th><th>Body</th></tr>
%%   <tr><td>GET</td>  <td>/status</td>                       <td>none</td>   <td>overall health snapshot</td></tr>
%%   <tr><td>GET</td>  <td>/dht/stats</td>                    <td>none</td>   <td>routing-table size + self_id</td></tr>
%%   <tr><td>GET</td>  <td>/swim/members</td>                 <td>none</td>   <td>current SWIM member list</td></tr>
%%   <tr><td>POST</td> <td>/bootstrap/rerun</td>              <td>none</td>   <td>trigger a re-cascade</td></tr>
%%   <tr><td>GET</td>  <td>/admin/identities</td>             <td>bearer</td> <td>list registered identities</td></tr>
%%   <tr><td>POST</td> <td>/admin/identities/:id/start</td>   <td>bearer</td> <td>spawn a new identity from a JSON spec</td></tr>
%%   <tr><td>POST</td> <td>/admin/identities/:id/stop</td>    <td>bearer</td> <td>terminate an identity</td></tr>
%%   <tr><td>POST</td> <td>/admin/identities/:id/reload</td>  <td>bearer</td> <td>stop + re-register with the same opts</td></tr>
%% </table>
%%
%% Bearer auth on `/admin/*' uses the `MACULA_ADMIN_TOKEN' env var.
%% A missing or empty token returns 503 (admin disabled); a wrong
%% Bearer header returns 401. The unauthenticated routes above stay
%% open per V1's loopback-only model (front with `ssh -L' for
%% remote access).
%%
%% TLS + client-cert auth are deferred to Phase 7; plan §8.6 scopes
%% this to loopback-only.
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

-type req() :: #{method  := binary(),
                 path    := binary(),
                 headers := #{binary() => binary()},
                 body    := binary()}.

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
    finish_body(Sock, Method, Path, normalise_headers(Headers));
on_headers({error, _} = E, _Sock, _Method, _Path) ->
    E.

%% Header values arrive as binary OR list (depending on
%% `{packet, http_bin}' decoding rules). Normalise to binary so
%% downstream pattern matches stay simple.
normalise_headers(Headers) ->
    maps:map(fun(_K, V) -> to_bin(V) end, Headers).

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
    assemble_request(read_body(Sock, Len), Method, Path, Headers).

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

assemble_request({ok, Body}, Method, Path, Headers) ->
    {ok, #{method  => to_method_bin(Method),
           path    => to_bin(Path),
           headers => Headers,
           body    => to_bin(Body)}};
assemble_request({error, _} = E, _Method, _Path, _Headers) ->
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
route(#{method := Method, path := Path} = Req) ->
    dispatch(Method, path_segments(Path), Req).

dispatch(<<"GET">>,  [<<"status">>],                  _Req) -> status();
dispatch(<<"GET">>,  [<<"dht">>, <<"stats">>],        _Req) -> dht_stats();
dispatch(<<"GET">>,  [<<"swim">>, <<"members">>],     _Req) -> swim_members();
dispatch(<<"POST">>, [<<"bootstrap">>, <<"rerun">>],  _Req) -> bootstrap_rerun();
dispatch(<<"GET">>,  [<<"admin">>, <<"identities">>],  Req) ->
    with_auth(Req, fun() -> list_identities() end);
dispatch(<<"POST">>, [<<"admin">>, <<"identities">>, Id, <<"start">>], Req) ->
    with_auth(Req, fun() -> start_identity(Id, Req) end);
dispatch(<<"POST">>, [<<"admin">>, <<"identities">>, Id, <<"stop">>], Req) ->
    with_auth(Req, fun() -> stop_identity(Id) end);
dispatch(<<"POST">>, [<<"admin">>, <<"identities">>, Id, <<"reload">>], Req) ->
    with_auth(Req, fun() -> reload_identity(Id) end);
dispatch(_Method, _Segments, _Req) ->
    not_found().

%% Split a `/foo/bar/baz' binary path into `[<<"foo">>, <<"bar">>,
%% <<"baz">>]'. Empty segments (leading slash, trailing slash, double
%% slashes) are dropped.
path_segments(<<"/", Rest/binary>>) ->
    binary:split(Rest, <<"/">>, [global, trim_all]);
path_segments(_) ->
    [].

%%==================================================================
%% Bearer-token auth — `MACULA_ADMIN_TOKEN'.
%%==================================================================

with_auth(Req, Handler) ->
    on_auth(check_auth(Req), Handler).

on_auth(ok, Handler)            -> Handler();
on_auth({error, Reason}, _H)    -> auth_error(Reason).

check_auth(#{headers := Headers}) ->
    on_token(os:getenv("MACULA_ADMIN_TOKEN"), Headers).

on_token(false, _Headers)             -> {error, not_configured};
on_token("",    _Headers)             -> {error, not_configured};
on_token(Token, Headers)              -> match_bearer(list_to_binary(Token), Headers).

match_bearer(Expected, Headers) ->
    classify_bearer(maps:get(<<"authorization">>, Headers, undefined), Expected).

classify_bearer(<<"Bearer ", Got/binary>>, Expected) when Got =:= Expected -> ok;
classify_bearer(_Other, _Expected) -> {error, unauthorized}.

auth_error(not_configured) ->
    json_response(503, #{result => <<"error">>,
                         reason => <<"admin_token_not_configured">>});
auth_error(unauthorized) ->
    json_response(401, #{result => <<"error">>,
                         reason => <<"unauthorized">>}).

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
        %% Stations are realm-agnostic — this field stays in the
        %% response for operator-tool compatibility but always empty.
        realms      => [],
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

%%==================================================================
%% /admin/identities — Phase 5
%%==================================================================

list_identities() ->
    Entries = [identity_status_json(K, P)
               || {K, P} <- hecate_station_identity_registry:list()],
    json_response(200, #{identities => Entries}).

identity_status_json(Key, Pid) ->
    Children = supervisor:which_children(Pid),
    Listener = listener_status(Children),
    #{
        identity_key => to_bin(Key),
        sup_pid      => list_to_binary(pid_to_list(Pid)),
        children     => [child_id_bin(Id) || {Id, _, _, _} <- Children],
        listener     => Listener
    }.

listener_status(Children) ->
    Listeners = [P || {hecate_station_listener, P, _, _} <- Children],
    listener_status_step(Listeners).

listener_status_step([])    -> #{state => <<"absent">>};
listener_status_step([Pid]) -> listener_addr_json(Pid).

listener_addr_json(Pid) ->
    classify_listener(catch hecate_station_listener:listen_addr(Pid)).

classify_listener({Bind, Port}) when is_integer(Port) ->
    #{state => <<"alive">>,
      addr  => iolist_to_binary(io_lib:format("~s:~B", [Bind, Port]))};
classify_listener(_) ->
    #{state => <<"unreachable">>}.

child_id_bin(A) when is_atom(A)   -> atom_to_binary(A, utf8);
child_id_bin(B) when is_binary(B) -> B.

%%------------------------------------------------------------------
%% POST /admin/identities/:id/start
%%------------------------------------------------------------------

start_identity(Id, #{body := Body}) ->
    on_decoded(Id, decode_spec(Body)).

on_decoded(_Id, {error, Reason}) ->
    json_response(400, #{result => <<"error">>, reason => Reason});
on_decoded(Id, {ok, Spec}) ->
    %% Plumbing: identity_key in the URL is authoritative — the
    %% body's `hostname' must agree (otherwise admin would let an
    %% operator desync the URL from the identity).
    on_id_match(Id, maps:get(hostname, Spec, undefined), Spec).

on_id_match(Id, Hostname, Spec) when Id =:= Hostname ->
    do_start(Spec);
on_id_match(_Id, _Hostname, _Spec) ->
    json_response(400, #{result => <<"error">>,
                         reason => <<"id_hostname_mismatch">>}).

do_start(Spec) ->
    on_box_secret(Spec, hecate_station_identity_config:load_box_secret()).

on_box_secret(_Spec, {error, R}) ->
    json_response(503, #{result => <<"error">>,
                         reason => to_bin_reason({box_secret_failed, R})});
on_box_secret(Spec, {ok, BoxSecret}) ->
    on_shared(Spec, BoxSecret,
              hecate_station_identity_config:shared_listener_opts()).

on_shared(_Spec, _Secret, {error, R}) ->
    json_response(503, #{result => <<"error">>,
                         reason => to_bin_reason(R)});
on_shared(Spec, BoxSecret, {ok, Shared}) ->
    Opts = hecate_station_identity_config:spec_to_opts(
             Spec, BoxSecret, Shared),
    Key  = maps:get(identity_key, Opts),
    on_register(Key, hecate_station_identity_registry:register(Key, Opts)).

on_register(Key, {ok, Pid}) ->
    json_response(201, #{result => <<"ok">>,
                         identity_key => to_bin(Key),
                         sup_pid      => list_to_binary(pid_to_list(Pid))});
on_register(_Key, {error, already_registered}) ->
    json_response(409, #{result => <<"error">>,
                         reason => <<"already_registered">>});
on_register(_Key, {error, Reason}) ->
    json_response(500, #{result => <<"error">>,
                         reason => to_bin_reason(Reason)}).

%%------------------------------------------------------------------
%% POST /admin/identities/:id/stop
%%------------------------------------------------------------------

stop_identity(Id) ->
    on_terminate(Id, hecate_station_identity_registry:terminate(Id)).

on_terminate(Id, ok) ->
    json_response(200, #{result => <<"ok">>, identity_key => Id});
on_terminate(_Id, {error, not_found}) ->
    json_response(404, #{result => <<"error">>, reason => <<"not_found">>}).

%%------------------------------------------------------------------
%% POST /admin/identities/:id/reload
%%------------------------------------------------------------------

reload_identity(Id) ->
    on_lookup_for_reload(Id, hecate_station_identity_registry:lookup_opts(Id)).

on_lookup_for_reload(_Id, {error, not_found}) ->
    json_response(404, #{result => <<"error">>, reason => <<"not_found">>});
on_lookup_for_reload(Id, {ok, Opts}) ->
    ok = hecate_station_identity_registry:terminate(Id),
    on_reregister(Id, hecate_station_identity_registry:register(Id, Opts)).

on_reregister(Id, {ok, Pid}) ->
    json_response(200, #{result => <<"ok">>,
                         identity_key => to_bin(Id),
                         sup_pid      => list_to_binary(pid_to_list(Pid))});
on_reregister(_Id, {error, Reason}) ->
    json_response(500, #{result => <<"error">>,
                         reason => to_bin_reason(Reason)}).

%%------------------------------------------------------------------
%% Body parsing — JSON ⇒ identity_spec()
%%------------------------------------------------------------------

decode_spec(<<>>) ->
    {error, <<"empty_body">>};
decode_spec(Body) ->
    on_decoded_json(catch json:decode(Body)).

on_decoded_json({'EXIT', _}) ->
    {error, <<"invalid_json">>};
on_decoded_json(Map) when is_map(Map) ->
    classify_spec_map(Map);
on_decoded_json(_) ->
    {error, <<"json_must_be_object">>}.

classify_spec_map(Map) ->
    Required = [<<"hostname">>, <<"city">>, <<"country">>,
                <<"lat">>, <<"lng">>],
    classify_spec_step(check_required(Required, Map), Map).

check_required([], _Map)            -> ok;
check_required([K | R], Map) ->
    case maps:is_key(K, Map) of
        true  -> check_required(R, Map);
        false -> {missing, K}
    end.

classify_spec_step({missing, K}, _Map) ->
    {error, iolist_to_binary([<<"missing field: ">>, K])};
classify_spec_step(ok, Map) ->
    finalise_spec(Map).

finalise_spec(Map) ->
    Hostname = to_bin(maps:get(<<"hostname">>, Map)),
    City     = to_bin(maps:get(<<"city">>, Map)),
    Country  = to_bin(maps:get(<<"country">>, Map)),
    Bind     = to_bind(maps:get(<<"bind">>, Map, undefined)),
    classify_lat_lng(maps:get(<<"lat">>, Map),
                     maps:get(<<"lng">>, Map),
                     Hostname, City, Country, Bind).

to_bind(undefined) -> undefined;
to_bind(Bin) when is_binary(Bin) -> Bin;
to_bind(L)   when is_list(L)     -> iolist_to_binary(L).

classify_lat_lng(Lat, Lng, Host, City, Country, Bind)
  when is_number(Lat), is_number(Lng) ->
    {ok, #{
        hostname => Host,
        city     => City,
        country  => Country,
        lat      => to_float(Lat),
        lng      => to_float(Lng),
        bind     => Bind
    }};
classify_lat_lng(_Lat, _Lng, _Host, _City, _Country, _Bind) ->
    {error, <<"lat/lng must be numeric">>}.

to_float(N) when is_float(N)   -> N;
to_float(N) when is_integer(N) -> float(N).

to_bin_reason(A) when is_atom(A)   -> atom_to_binary(A, utf8);
to_bin_reason(T) ->
    iolist_to_binary(io_lib:format("~p", [T])).

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
        swim_count  => snapshot_member_count(Swim)
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
status_text(201) -> <<"Created">>;
status_text(400) -> <<"Bad Request">>;
status_text(401) -> <<"Unauthorized">>;
status_text(404) -> <<"Not Found">>;
status_text(409) -> <<"Conflict">>;
status_text(500) -> <<"Internal Server Error">>;
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
