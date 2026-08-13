%%% @doc Singleton relay ping pinger + responder.
%%%
%%% Ports V1 macula_relay_heartbeat's cross-box ping cycle:
%%%   * register `_relay.ping' as a fast-return RPC handler so peers
%%%     measuring latency to us get a sub-ms response.
%%%   * every 30s, for each connected outbound peer link reported by
%%%     `macula_station_peer_links:connections/0', do
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

-include_lib("kernel/include/logger.hrl").

-export([start_link/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(PING_INTERVAL_MS, 30_000).
-define(PING_TIMEOUT_MS,   5_000).

%% Realm tag the mesh-internal procedures + events live under.
-define(MESH_REALM, <<0:256>>).

%% Registries are REGISTERED NAMES, resolved by gen_server:call/2 on
%% every use. A pid captured at child-spec time is dead the moment the
%% registry restarts, and supervisors reuse the original child spec, so
%% the stale pid would never be replaced.
-type opts() :: #{
    handler_registry := atom() | pid(),
    pubsub_registry  := atom() | pid(),
    identity         := macula_identity:key_pair(),
    hostname         := binary(),
    lat              => float() | integer() | undefined,
    lng              => float() | integer() | undefined
}.

-export_type([opts/0]).

-record(state, {
    handler_registry :: atom() | pid(),
    pubsub_registry  :: atom() | pid(),
    identity         :: macula_identity:key_pair(),
    hostname         :: binary(),
    lat              :: number() | undefined,
    lng              :: number() | undefined,
    timer_ref        :: reference() | undefined,
    %% Consecutive failed pings per target hostname.
    %%
    %% ⚠ This probe is the ONLY true wire round-trip in the station, and
    %% it used to publish only on SUCCESS: the failure clause was
    %% `{error, _Reason} -> ok'. Total wire failure and "no peers
    %% configured" therefore produced byte-identical output, which is
    %% nothing. On station-it-milan this ran ~3,600 times across 30
    %% hours of total outage and emitted not one fact.
    %%
    %% ⚠ It would have FIRED on milan, contrary to the first draft of
    %% the plan, which assumed this probe self-suppresses because a
    %% station that cannot dial has no links to ping over. Not so:
    %% `macula_station_outbound_link:init/1' calls
    %% `macula_station_peer_links:register/2' BEFORE the first dial,
    %% `connections/0' applies no verified filter (unlike
    %% `connected_hostnames/0', which is built on `verified_peers/0' and
    %% WAS empty on milan), and `unregister/1' has zero call sites in
    %% the repo. So milan held a registered, never-verified link and this
    %% probe pinged it every 30s for 30 hours — roughly 3,600 failures,
    %% every one discarded.
    %%
    %% Keyed by URL rather than by the hostname the payload carries:
    %% `hostname_from_url/1' strips the port, so two peers on one host
    %% would share a counter. The URL is the registry's own unique key.
    misses = #{} :: #{binary() => non_neg_integer()}
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{handler_registry := _, pubsub_registry := _,
             identity := _, hostname := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%%====================================================================
%% gen_server
%%====================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    State = #state{
        handler_registry = maps:get(handler_registry, Opts),
        pubsub_registry  = maps:get(pubsub_registry, Opts),
        identity         = maps:get(identity, Opts),
        hostname         = maps:get(hostname, Opts),
        lat              = to_number(maps:get(lat, Opts, undefined)),
        lng              = to_number(maps:get(lng, Opts, undefined))
    },
    advertise_responder(State),
    {ok, schedule(State)}.

%% Register `_relay.ping' as a fast-return handler against the
%% station-singleton registry. Inbound CALL frames hit this handler
%% and return the receive timestamp — caller subtracts from its own
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

handle_info(ping_cycle, S0) ->
    S = do_ping_cycle(S0),
    {noreply, schedule(S)};
handle_info(_, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.

code_change(_Old, S, _Extra) -> {ok, S}.

%%====================================================================
%% Ping cycle
%%====================================================================

%% Folds rather than foreach so a miss can be REMEMBERED. The previous
%% shape discarded the per-target outcome entirely.
%%
%% Rebuilds `misses' from an empty map each cycle, seeding each key from
%% the previous one. That prunes departed peers for free — a map that
%% only ever grew would keep counting a peer removed from the config,
%% and would eventually alarm about a peer nobody dials any more.
do_ping_cycle(#state{misses = Old} = S) ->
    Conns = macula_station_peer_links:connections(),
    S#state{misses = lists:foldl(fun(Conn, Acc) -> ping_one(S, Old, Conn, Acc) end,
                                 #{}, Conns)}.

ping_one(S, Old, {Url, LinkPid}, Acc) ->
    T0 = erlang:system_time(microsecond),
    Result = safe_call(LinkPid, ?MESH_REALM, <<"_relay.ping">>, #{},
                       ?PING_TIMEOUT_MS),
    classify_ping(Result, S, Url, maps:get(Url, Old, 0), T0, Acc).

classify_ping({ok, _}, S, Url, Prior, T0, Acc) ->
    RttUs = erlang:system_time(microsecond) - T0,
    publish_result(S, hostname_from_url(Url), RttUs / 1000.0, cross_box),
    announce_recovery(Prior, Url),
    Acc;
classify_ping({error, Reason}, S, Url, Prior, _T0, Acc) ->
    N = Prior + 1,
    publish_miss(S, hostname_from_url(Url), N, Reason),
    Acc#{Url => N}.

announce_recovery(0, _Target) ->
    ok;
announce_recovery(N, Target) ->
    ?LOG_NOTICE("[relay_ping] ~s answering again after ~B consecutive "
                "missed pings", [Target, N]),
    ok.

%% Publishes on its OWN topic rather than folding a failure into
%% `_mesh.relay.ping'. That topic's payload carries `rtt_ms', and a
%% miss has no round-trip time; inventing one (0, or null) would corrupt
%% every latency consumer already reading it.
publish_miss(#state{pubsub_registry = Reg, identity = Kp, hostname = Self},
             Target, N, Reason) ->
    Payload = iolist_to_binary(json:encode(#{
        <<"relay">>       => Self,
        <<"target">>      => Target,
        <<"consecutive">> => N,
        <<"reason">>      => iolist_to_binary(io_lib:format("~p", [Reason]))
    })),
    publish_to(pubsub_server(Reg, Kp), <<"_mesh.relay.ping.failed">>, Payload).

publish_to({ok, ServerPid}, Topic, Payload) ->
    catch hecate_pubsub_server:publish(ServerPid, Topic, Payload),
    ok;
publish_to(_Other, _Topic, _Payload) ->
    ok.

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

to_number(N) when is_number(N) -> N;
to_number(_)                    -> undefined.

%%====================================================================
%% Schedule
%%====================================================================

schedule(S) ->
    Ref = make_ref(),
    erlang:send_after(?PING_INTERVAL_MS, self(), ping_cycle),
    S#state{timer_ref = Ref}.
