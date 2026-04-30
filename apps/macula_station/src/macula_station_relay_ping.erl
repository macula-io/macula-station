%%% @doc Per-identity relay ping pinger + responder.
%%%
%%% Ports V1 macula_relay_heartbeat's cross-box ping cycle:
%%%   * register `_relay.ping' as a fast-return RPC handler so peers
%%%     measuring latency to us get a sub-ms response.
%%%   * every 30s, for each connected outbound peer link, do
%%%     `macula_station_link:call/5' against `_relay.ping' and time
%%%     the round-trip.
%%%   * publish `_mesh.relay.ping' on the local pubsub_server with
%%%     `{relay, target, rtt_ms, lat, lng}' so the realm topology
%%%     view's LATENCY toggle can render the heat map.
%%%
%%% V1 used pubsub-driven ping/pong (publish `_relay.ping', wait for
%%% `_relay.pong'). V2 has macula RPC primitives (CALL/RESULT frames)
%%% which give us the same round-trip without manual ping_id
%%% bookkeeping — clean port that drops 100 LOC.
-module(macula_station_relay_ping).
-behaviour(gen_server).

-export([start_link/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(PING_INTERVAL_MS, 30_000).
-define(PING_TIMEOUT_MS,   5_000).

%% Realm tag the mesh-internal procedures + events live under.
-define(MESH_REALM, <<0:256>>).

-type opts() :: #{
    handler_registry := pid(),
    pubsub_registry  := pid(),
    overlay_seeder   := pid(),
    identity         := macula_identity:key_pair(),
    hostname         := binary(),
    lat              => float() | integer() | undefined,
    lng              => float() | integer() | undefined,
    identity_key     => term()
}.

-export_type([opts/0]).

-record(state, {
    handler_registry :: pid(),
    pubsub_registry  :: pid(),
    overlay_seeder   :: pid(),
    identity         :: macula_identity:key_pair(),
    hostname         :: binary(),
    lat              :: number() | undefined,
    lng              :: number() | undefined,
    timer_ref        :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{handler_registry := _, pubsub_registry := _,
             overlay_seeder := _, identity := _, hostname := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%%====================================================================
%% gen_server
%%====================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    set_logger_identity(Opts),
    State = #state{
        handler_registry = maps:get(handler_registry, Opts),
        pubsub_registry  = maps:get(pubsub_registry, Opts),
        overlay_seeder   = maps:get(overlay_seeder, Opts),
        identity         = maps:get(identity, Opts),
        hostname         = maps:get(hostname, Opts),
        lat              = to_number(maps:get(lat, Opts, undefined)),
        lng              = to_number(maps:get(lng, Opts, undefined))
    },
    advertise_responder(State),
    {ok, schedule(State)}.

set_logger_identity(#{identity_key := Key}) ->
    logger:set_process_metadata(#{identity_id => Key});
set_logger_identity(_) -> ok.

%% Register `_relay.ping' as a fast-return handler against the
%% per-identity registry. Inbound CALL frames hit this handler and
%% return the receive timestamp — caller subtracts from its own
%% sent timestamp for one-way / round-trip measurement.
advertise_responder(#state{handler_registry = Reg}) ->
    catch macula_handler_registry:advertise(
            Reg, <<"_relay.ping">>,
            fun(_Args) ->
                    {ok, #{ts => erlang:system_time(microsecond)}}
            end),
    ok.

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(ping_cycle, S) ->
    do_ping_cycle(S),
    {noreply, schedule(S)};
handle_info(_, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.

code_change(_Old, S, _Extra) -> {ok, S}.

%%====================================================================
%% Ping cycle
%%====================================================================

do_ping_cycle(S) ->
    Conns = peer_links(S#state.overlay_seeder),
    lists:foreach(fun(Conn) -> ping_one(S, Conn) end, Conns).

ping_one(S, {Url, LinkPid}) ->
    Target = hostname_from_url(Url),
    T0 = erlang:system_time(microsecond),
    case safe_call(LinkPid, ?MESH_REALM, <<"_relay.ping">>, #{}, ?PING_TIMEOUT_MS) of
        {ok, _} ->
            RttUs = erlang:system_time(microsecond) - T0,
            publish_result(S, Target, RttUs / 1000.0, cross_box);
        {error, _Reason} ->
            ok
    end.

safe_call(LinkPid, Realm, Proc, Payload, Timeout) ->
    try macula_station_link:call(LinkPid, Realm, Proc, Payload, Timeout)
    catch _:_ -> {error, exception}
    end.

%%====================================================================
%% Publish + helpers
%%====================================================================

publish_result(#state{pubsub_registry = Reg, identity = Kp,
                      hostname = Self, lat = Lat, lng = Lng},
               Target, RttMs, Type) ->
    Payload = iolist_to_binary(json:encode(#{
        <<"relay">>  => Self,
        <<"target">> => Target,
        <<"rtt_ms">> => RttMs,
        <<"type">>   => atom_to_binary(Type),
        <<"lat">>    => geo_or_null(Lat),
        <<"lng">>    => geo_or_null(Lng)
    })),
    case pubsub_server(Reg, Kp) of
        {ok, ServerPid} ->
            catch hecate_pubsub_server:publish(
                    ServerPid, <<"_mesh.relay.ping">>, Payload);
        _ -> ok
    end.

pubsub_server(Reg, Kp) ->
    try hecate_pubsub_registry:register(Reg, ?MESH_REALM, Kp)
    catch _:_ -> error
    end.

geo_or_null(N) when is_number(N) -> N;
geo_or_null(_)                   -> null.

hostname_from_url(<<"quic://", Rest/binary>>) -> strip_port(Rest);
hostname_from_url(<<"https://", Rest/binary>>) -> strip_port(Rest);
hostname_from_url(Bin) when is_binary(Bin) -> strip_port(Bin).

strip_port(Bin) ->
    case binary:split(Bin, <<":">>) of
        [H | _] -> H
    end.

peer_links(SeedPid) ->
    try macula_station_overlay_seeder:connections(SeedPid)
    catch _:_ -> []
    end.

to_number(N) when is_number(N) -> N;
to_number(_)                    -> undefined.

%%====================================================================
%% Schedule
%%====================================================================

schedule(S) ->
    Ref = make_ref(),
    erlang:send_after(?PING_INTERVAL_MS, self(), ping_cycle),
    S#state{timer_ref = Ref}.
