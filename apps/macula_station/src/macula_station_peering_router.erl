%%% @doc Singleton peering router.
%%%
%%% Drives the cross-relay pubsub plumbing V1 had built into
%%% `macula_relay_peering' (`maybe_subscribe_on_peers' /
%%% `maybe_unsubscribe_from_peers'). For each (Realm, Topic, Peer-station)
%%% triple the router maintains:
%%%
%%%   * an OUTBOUND forwarder process that joins the station-wide pg
%%%     scope on `{relay_topic, Topic}' and forwards local publishes
%%%     to that peer's station_link with the matching realm.
%%%
%%%   * an INBOUND subscription on the peer's station_link so EVENT
%%%     frames published by the peer flow into our local handler
%%%     (which fans out to local subscribers on the matching topic).
%%%
%%% The router polls every `?TICK_MS' seconds (default 15s):
%%%   1. asks the registry for every materialised realm,
%%%   2. for each realm, asks its pubsub_server for its current topics,
%%%   3. asks `macula_station_peer_links:connections/0' for the live
%%%      `[{Url, LinkPid}]' set,
%%%   4. computes the desired (Realm, Topic, Peer) cross-product,
%%%   5. diffs against the running set, starts/stops forwarders +
%%%      adds/drops subscriptions accordingly.
%%%
%%% V1 used local subscribe/unsubscribe events as the trigger; V2's
%%% hecate_pubsub_server doesn't emit events on subscription change,
%%% so we poll. Same convergence guarantee, slightly higher latency.
%%%
%%% **Multi-realm**: realms are first-class. The router enumerates
%%% them via `hecate_pubsub_registry:list_realms/1'. Stations are
%%% realm-agnostic infrastructure — the same router instance carries
%%% gossip for every realm a connected daemon has subscribed to.
%%% Mesh-protocol topics (`_mesh.bloom', `_mesh.station.*' etc.)
%%% live in the all-zeros realm and are filtered out below; their
%%% forwarding is `bloom_exchange's responsibility, and double-fan
%%% would mess up gossip cadence.
-module(macula_station_peering_router).
-behaviour(gen_server).

-export([start_link/1, stop/1, sync_now/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TICK_MS, 15_000).

-type opts() :: #{
    pubsub_registry := pid(),
    identity        := macula_identity:key_pair(),
    forwarder_sup   := pid()
}.

-export_type([opts/0]).

-type realm() :: <<_:256>>.
-type triple() :: {realm(), Topic :: binary(), LinkPid :: pid()}.

-record(state, {
    pubsub_registry :: pid(),
    identity        :: macula_identity:key_pair(),
    forwarder_sup   :: pid(),
    %% Running forwarders keyed by `{Realm, Topic, LinkPid}'.
    forwarders      :: #{triple() => pid()},
    %% Active inbound subscriptions keyed by `{Realm, Topic, LinkPid}'.
    subs            :: #{triple() => reference()},
    timer_ref       :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{pubsub_registry := _, identity := _, forwarder_sup := _} = Opts) ->
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
    State = #state{
        pubsub_registry = maps:get(pubsub_registry, Opts),
        identity        = maps:get(identity, Opts),
        forwarder_sup   = maps:get(forwarder_sup, Opts),
        forwarders      = #{},
        subs            = #{}
    },
    self() ! tick,
    {ok, State}.

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

sync(#state{pubsub_registry = Reg,
            forwarder_sup   = FwdSup,
            forwarders      = Forwarders,
            subs            = Subs} = S) ->
    Pairs = local_realm_topics(Reg),
    Peers = macula_station_peer_links:connections(),
    Desired = desired_triples(Pairs, Peers),
    {Forwarders1, Subs1} = reconcile(Desired, Forwarders, Subs, FwdSup),
    S#state{forwarders = Forwarders1, subs = Subs1}.

%% Cartesian product of (Realm, Topic, LinkPid). Topics that start with
%% `_mesh.' are protocol-internal — bloom_exchange already broadcasts
%% them out-of-band; the forwarder/subscribe path would double-fan
%% them out and mess with the gossip cadence. Skip them.
desired_triples(Pairs, Peers) ->
    [{Realm, Topic, LinkPid}
     || {Realm, Topic} <- Pairs, not is_mesh_topic(Topic),
        {_Url, LinkPid} <- Peers,
        is_pid(LinkPid)].

is_mesh_topic(<<"_mesh.", _/binary>>) -> true;
is_mesh_topic(_) -> false.

reconcile(Desired, Forwarders, Subs, FwdSup) ->
    DesiredSet = sets:from_list(Desired),
    F1 = stop_forwarders_not_in(DesiredSet, Forwarders, FwdSup),
    S1 = drop_subs_not_in(DesiredSet, Subs),
    F2 = start_forwarders_for(Desired, F1, FwdSup),
    S2 = subscribe_for(Desired, S1),
    {F2, S2}.

stop_forwarders_not_in(Desired, Forwarders, FwdSup) ->
    maps:filter(
      fun(Key, Pid) ->
              case sets:is_element(Key, Desired) of
                  true  -> true;
                  false ->
                      catch macula_station_forwarder_sup:stop_forwarder(FwdSup, Pid),
                      false
              end
      end, Forwarders).

drop_subs_not_in(Desired, Subs) ->
    maps:filter(
      fun(Key, SubRef) ->
              case sets:is_element(Key, Desired) of
                  true  -> true;
                  false ->
                      {_Realm, _Topic, LinkPid} = Key,
                      catch macula_station_link:unsubscribe(LinkPid, SubRef),
                      false
              end
      end, Subs).

start_forwarders_for(Desired, Forwarders, FwdSup) ->
    lists:foldl(
      fun({Realm, Topic, LinkPid} = Key, Acc) ->
              case maps:is_key(Key, Acc) of
                  true  -> Acc;
                  false -> start_one_forwarder(Acc, Key, FwdSup,
                                               Realm, Topic, LinkPid)
              end
      end, Forwarders, Desired).

start_one_forwarder(Acc, Key, FwdSup, Realm, Topic, LinkPid) ->
    Opts = #{topic => Topic, peer_link => LinkPid, realm => Realm},
    case macula_station_forwarder_sup:start_forwarder(FwdSup, Opts) of
        {ok, FwdPid} -> Acc#{Key => FwdPid};
        {error, _R}  -> Acc
    end.

subscribe_for(Desired, Subs) ->
    lists:foldl(
      fun({Realm, Topic, LinkPid} = Key, Acc) ->
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
    %% LinkPid may be a `macula_station_link' SDK client OR a
    %% `macula_station_outbound_link' (which gained the SDK API
    %% surface in commit afd3542 — both now handle subscribe).
    try macula_station_link:subscribe(LinkPid, Realm, Topic, self()) of
        {ok, SubRef} -> Acc#{Key => SubRef};
        _Other       -> Acc
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

%% Enumerate every (Realm, Topic) pair the local registry knows
%% about. Tolerate registry-down / per-server lookup failures —
%% downstream `desired_triples/2' just sees fewer pairs that tick
%% and reconciles on the next.
local_realm_topics(Reg) ->
    Realms = safe_list_realms(Reg),
    lists:flatmap(fun(Realm) ->
        [{Realm, Topic} || Topic <- safe_topics_for_realm(Reg, Realm)]
    end, Realms).

safe_list_realms(Reg) ->
    try hecate_pubsub_registry:list_realms(Reg)
    catch _:_ -> []
    end.

safe_topics_for_realm(Reg, Realm) ->
    try hecate_pubsub_registry:lookup(Reg, Realm) of
        {ok, Server} ->
            try hecate_pubsub_server:topics(Server)
            catch _:_ -> []
            end;
        _ -> []
    catch _:_ -> []
    end.

%%====================================================================
%% Schedule
%%====================================================================

schedule_tick(S) ->
    Ref = make_ref(),
    erlang:send_after(?TICK_MS, self(), tick),
    S#state{timer_ref = Ref}.
