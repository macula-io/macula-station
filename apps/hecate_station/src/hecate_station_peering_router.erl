%%% @doc Per-identity peering router.
%%%
%%% Drives the cross-relay pubsub plumbing V1 had built into
%%% `macula_relay_peering' (`maybe_subscribe_on_peers' /
%%% `maybe_unsubscribe_from_peers'). For each (Topic, Peer-station)
%%% pair the router maintains:
%%%
%%%   * an OUTBOUND forwarder process that joins the per-identity pg
%%%     scope on `{relay_topic, Topic}' and forwards local publishes
%%%     to that peer's station_link.
%%%
%%%   * an INBOUND subscription on the peer's station_link so EVENT
%%%     frames published by the peer flow into our local handler
%%%     (which fans out to local subscribers on the matching topic).
%%%
%%% The router polls every `?TICK_MS' seconds (default 15s):
%%%   1. asks the local pubsub_server for its current topics,
%%%   2. asks the overlay_seeder for its live `[{Url, LinkPid}]',
%%%   3. computes the desired (Topic, Peer) cross-product,
%%%   4. diffs against the running set, starts/stops forwarders +
%%%      adds/drops subscriptions accordingly.
%%%
%%% V1 used local subscribe/unsubscribe events as the trigger; V2's
%%% hecate_pubsub_server doesn't emit events on subscription change,
%%% so we poll. Same convergence guarantee, slightly higher latency.
-module(hecate_station_peering_router).
-behaviour(gen_server).

-export([start_link/1, stop/1, sync_now/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TICK_MS, 15_000).

%% Mesh-level events (presence + bloom gossip) live in the all-zeros
%% realm. The router's local subscription lookup goes against the
%% mesh-realm pubsub_server.
-define(MESH_REALM, <<0:256>>).

-type opts() :: #{
    pubsub_registry := pid(),
    identity        := macula_identity:key_pair(),
    forwarder_sup   := pid(),
    overlay_seeder  := pid(),
    pg_scope        := atom(),
    realm           := <<_:256>>,
    identity_key    => term()
}.

-export_type([opts/0]).

-record(state, {
    pubsub_registry :: pid(),
    identity        :: macula_identity:key_pair(),
    forwarder_sup   :: pid(),
    overlay_seeder  :: pid(),
    pg_scope        :: atom(),
    realm           :: <<_:256>>,
    %% Running forwarders keyed by `{Topic, LinkPid}'.
    forwarders      :: #{{binary(), pid()} => pid()},
    %% Active inbound subscriptions keyed by `{Topic, LinkPid}'.
    subs            :: #{{binary(), pid()} => reference()},
    timer_ref       :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{pubsub_registry := _, identity := _, forwarder_sup := _,
             overlay_seeder := _, pg_scope := _, realm := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Force a synchronous sync. Test hook.
-spec sync_now(pid()) -> ok.
sync_now(Pid) ->
    gen_server:call(Pid, sync_now, 5_000).

%%====================================================================
%% gen_server
%%====================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    set_logger_identity(Opts),
    State = #state{
        pubsub_registry = maps:get(pubsub_registry, Opts),
        identity        = maps:get(identity, Opts),
        forwarder_sup   = maps:get(forwarder_sup, Opts),
        overlay_seeder  = maps:get(overlay_seeder, Opts),
        pg_scope        = maps:get(pg_scope, Opts),
        realm           = maps:get(realm, Opts),
        forwarders      = #{},
        subs            = #{}
    },
    self() ! tick,
    {ok, State}.

set_logger_identity(#{identity_key := Key}) ->
    logger:set_process_metadata(#{identity_id => Key});
set_logger_identity(_) -> ok.

handle_call(sync_now, _From, S) ->
    {reply, ok, sync(S)};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(tick, S) ->
    {noreply, schedule_tick(sync(S))};
handle_info({'EXIT', Pid, _Reason}, S) ->
    {noreply, drop_dead(Pid, S)};
handle_info(_, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.

code_change(_Old, S, _Extra) -> {ok, S}.

%%====================================================================
%% Sync
%%====================================================================

sync(#state{pubsub_registry = Reg, identity = Kp,
            overlay_seeder  = SeedPid,
            forwarder_sup   = FwdSup,
            pg_scope        = Scope,
            realm           = Realm,
            forwarders      = Forwarders,
            subs            = Subs} = S) ->
    Topics = local_topics(Reg, Kp),
    Peers  = peer_links(SeedPid),
    Desired = desired_pairs(Topics, Peers),
    {Forwarders1, Subs1} =
        reconcile(Desired, Forwarders, Subs, FwdSup, Scope, Realm),
    S#state{forwarders = Forwarders1, subs = Subs1}.

%% Cartesian product of (Topic, LinkPid). Topics that start with
%% `_mesh.' are protocol-internal — bloom_exchange already broadcasts
%% them out-of-band; the forwarder/subscribe path would double-fan
%% them out and mess with the gossip cadence. Skip them.
desired_pairs(Topics, Peers) ->
    [{Topic, LinkPid}
     || Topic   <- Topics, not is_mesh_topic(Topic),
        {_Url, LinkPid} <- Peers,
        is_pid(LinkPid)].

is_mesh_topic(<<"_mesh.", _/binary>>) -> true;
is_mesh_topic(_) -> false.

reconcile(Desired, Forwarders, Subs, FwdSup, Scope, Realm) ->
    DesiredSet = sets:from_list(Desired),
    F1 = stop_forwarders_not_in(DesiredSet, Forwarders, FwdSup),
    S1 = drop_subs_not_in(DesiredSet, Subs),
    F2 = start_forwarders_for(Desired, F1, FwdSup, Scope, Realm),
    S2 = subscribe_for(Desired, S1, Realm),
    {F2, S2}.

stop_forwarders_not_in(Desired, Forwarders, FwdSup) ->
    maps:filter(
      fun(Key, Pid) ->
              case sets:is_element(Key, Desired) of
                  true  -> true;
                  false ->
                      catch hecate_station_forwarder_sup:stop_forwarder(FwdSup, Pid),
                      false
              end
      end, Forwarders).

drop_subs_not_in(Desired, Subs) ->
    maps:filter(
      fun(Key, SubRef) ->
              case sets:is_element(Key, Desired) of
                  true  -> true;
                  false ->
                      {_Topic, LinkPid} = Key,
                      catch macula_station_link:unsubscribe(LinkPid, SubRef),
                      false
              end
      end, Subs).

start_forwarders_for(Desired, Forwarders, FwdSup, Scope, Realm) ->
    lists:foldl(
      fun({Topic, LinkPid} = Key, Acc) ->
              case maps:is_key(Key, Acc) of
                  true  -> Acc;
                  false -> start_one_forwarder(Acc, Key, FwdSup, Scope, Realm,
                                               Topic, LinkPid)
              end
      end, Forwarders, Desired).

start_one_forwarder(Acc, Key, FwdSup, Scope, Realm, Topic, LinkPid) ->
    Opts = #{pg_scope => Scope, topic => Topic,
             peer_link => LinkPid, realm => Realm},
    case hecate_station_forwarder_sup:start_forwarder(FwdSup, Opts) of
        {ok, FwdPid} -> Acc#{Key => FwdPid};
        {error, _R}  -> Acc
    end.

subscribe_for(Desired, Subs, Realm) ->
    lists:foldl(
      fun({Topic, LinkPid} = Key, Acc) ->
              case maps:is_key(Key, Acc) of
                  true  -> Acc;
                  false -> subscribe_one(Acc, Key, Realm, Topic, LinkPid)
              end
      end, Subs, Desired).

subscribe_one(Acc, Key, Realm, Topic, LinkPid) ->
    %% Subscriber is `self()' so this router process receives
    %% inbound EVENT frames. The router doesn't fan out — it relies
    %% on the local pubsub_server to handle inbound delivery via
    %% the existing process_frame path. Subscribe-on-peer is the
    %% interest signal; the inbound event flow is owned elsewhere.
    try macula_station_link:subscribe(LinkPid, Realm, Topic, self()) of
        {ok, SubRef} -> Acc#{Key => SubRef}
    catch _:_         -> Acc
    end.

%% Forwarder pids may exit (peer link death). Strip the dead pid
%% from the bookkeeping so the next sync tick spawns a fresh one if
%% the link is back.
drop_dead(Pid, #state{forwarders = F} = S) ->
    F1 = maps:filter(fun(_, P) -> P =/= Pid end, F),
    S#state{forwarders = F1}.

%%====================================================================
%% Lookups
%%====================================================================

local_topics(Reg, Kp) ->
    try hecate_pubsub_registry:register(Reg, ?MESH_REALM, Kp) of
        {ok, Server} ->
            try hecate_pubsub_server:topics(Server)
            catch _:_ -> []
            end;
        _ -> []
    catch _:_ -> []
    end.

peer_links(SeedPid) ->
    try hecate_station_overlay_seeder:connections(SeedPid)
    catch _:_ -> []
    end.

%%====================================================================
%% Schedule
%%====================================================================

schedule_tick(S) ->
    Ref = make_ref(),
    erlang:send_after(?TICK_MS, self(), tick),
    S#state{timer_ref = Ref}.
