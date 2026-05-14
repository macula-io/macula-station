%%% @doc Singleton peering router.
%%%
%%% Drives the cross-relay pubsub plumbing V1 had built into
%%% `macula_relay_peering' (`maybe_subscribe_on_peers' /
%%% `maybe_unsubscribe_from_peers'). For each (Realm, Topic, Peer-station)
%%% triple the router maintains an INBOUND subscription on the peer's
%%% station_link, so EVENT frames the peer publishes flow into our
%%% local handler (`macula_station_peer_observer'), which fans them
%%% out to local subscribers on the matching topic.
%%%
%%% (The companion OUTBOUND forwarder process — a V1-ported pg-fanout
%%% path that never got wired into the publish path — was removed in
%%% the pubsub Phase 2 cleanup. Cross-station EVENT delivery is owned
%%% entirely by `macula_station_peer_observer:fan_out_event/3' +
%%% `relay_publish'.)
%%%
%%% The router polls every `?TICK_MS' (default 2s):
%%%   1. asks the registry for every materialised realm,
%%%   2. for each realm, asks its pubsub_server for its current topics,
%%%   3. asks `macula_station_peer_links:connections/0' for the live
%%%      `[{Url, LinkPid}]' set,
%%%   4. computes the desired (Realm, Topic, Peer) cross-product,
%%%   5. diffs against the running subscriptions, adds/drops the
%%%      subscribe-on-peer for the changes,
%%%   6. diff-propagates local ADVERTISEs to connected peers.
%%%
%%% V1 used local subscribe/unsubscribe events as the trigger; V2's
%%% hecate_pubsub_server doesn't emit events on subscription change,
%%% so we poll. `peer_observer' also calls `sync_now/1' on every
%%% inbound SUBSCRIBE / UNSUBSCRIBE for sub-tick latency.
%%%
%%% **Multi-realm**: realms are first-class. The router enumerates
%%% them via `hecate_pubsub_registry:list_realms/1'. Stations are
%%% realm-agnostic infrastructure — the same router instance carries
%%% gossip for every realm a connected daemon has subscribed to.
%%% Mesh-protocol topics (`_mesh.bloom', `_mesh.station.*' etc.)
%%% live in the all-zeros realm and are filtered out below; their
%%% forwarding is `bloom_exchange's responsibility, and a second fan
%%% would mess with the gossip cadence.
-module(macula_station_peering_router).
-behaviour(gen_server).

-export([start_link/1, stop/1, sync_now/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Pre-Gap-B this was 15s, which left e2e cross-station probes
%% timing out before the router could propagate a fresh local
%% SUBSCRIBE to peers. 2s is the right balance: snappy convergence
%% under steady-state churn, still <1% CPU even on a heavily-
%% subscribed station. `peer_observer' also calls `sync_now/1' on
%% every inbound SUBSCRIBE / UNSUBSCRIBE for sub-tick latency.
-define(TICK_MS, 2_000).

-type opts() :: #{
    pubsub_registry := pid(),
    identity        := macula_identity:key_pair()
}.

-export_type([opts/0]).

-type realm() :: <<_:256>>.
-type triple() :: {realm(), Topic :: binary(), LinkPid :: pid()}.

-record(state, {
    pubsub_registry :: pid(),
    identity        :: macula_identity:key_pair(),
    %% Active inbound subscriptions keyed by `{Realm, Topic, LinkPid}'.
    subs            :: #{triple() => reference()},
    %% Per-peer (Realm, Proc) set we have ALREADY ADVERTISED on each
    %% peer's conn. Diff-driven: each tick we compute the desired set
    %% (= our local DIRECT advertises = remote_advertise entries we
    %% own) and send ADVERTISE / UNADVERTISE only for the changes.
    %% Steady state = zero frames. The previous (reverted) attempt
    %% broadcast on every received frame, which amplified under
    %% e2e advertise/unadvertise churn until peer_observer's mailbox
    %% climbed to 116k.
    %%
    %% NodeId → set of {Realm, Proc} we last sent to that NodeId.
    advertised      = #{} :: #{macula_identity:pubkey() => sets:set({realm(), binary()})},
    timer_ref       :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{pubsub_registry := _, identity := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Force a synchronous sync. Test hook (also called by
%% `peer_observer' on each inbound SUBSCRIBE / UNSUBSCRIBE).
-spec sync_now(pid()) -> ok.
sync_now(Pid) ->
    gen_server:call(Pid, sync_now, 5_000).

%%====================================================================
%% gen_server
%%====================================================================

init(Opts) ->
    State = #state{
        pubsub_registry = maps:get(pubsub_registry, Opts),
        identity        = maps:get(identity, Opts),
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
    %% Coalesce: every direct ADVERTISE / SUBSCRIBE on this station
    %% sends `Pid ! tick' to nudge the router. Under load (lots of
    %% daemons advertising at once), this can flood the mailbox with
    %% thousands of idempotent ticks the router cannot drain — each
    %% sync/1 call takes hundreds of ms because it touches every peer
    %% conn. Drain queued ticks here so each pass of the mailbox
    %% triggers exactly one sync.
    ok = drain_ticks(),
    {noreply, schedule_tick(sync(S))};
handle_info(_, S) ->
    {noreply, S}.

drain_ticks() ->
    receive
        tick -> drain_ticks()
    after 0 ->
        ok
    end.

terminate(_Reason, _S) -> ok.

code_change(_Old, S, _Extra) -> {ok, S}.

%%====================================================================
%% Sync
%%====================================================================

sync(#state{pubsub_registry = Reg,
            subs            = Subs,
            advertised      = Advertised,
            identity        = Kp} = S) ->
    Pairs   = local_realm_topics(Reg),
    Peers   = macula_station_peer_links:connections(),
    Desired = desired_triples(Pairs, Peers),
    Subs1       = reconcile_subs(Desired, Subs),
    Advertised1 = sync_advertises(Kp, Advertised),
    S#state{subs       = Subs1,
            advertised = Advertised1}.

%% Cartesian product of (Realm, Topic, LinkPid). Topics that start with
%% `_mesh.' are protocol-internal — bloom_exchange already broadcasts
%% them out-of-band; a subscribe-on-peer for them would re-fan and
%% mess with the gossip cadence. Skip them.
desired_triples(Pairs, Peers) ->
    [{Realm, Topic, LinkPid}
     || {Realm, Topic} <- Pairs, not is_mesh_topic(Topic),
        {_Url, LinkPid} <- Peers,
        is_pid(LinkPid)].

is_mesh_topic(<<"_mesh.", _/binary>>) -> true;
is_mesh_topic(_) -> false.

reconcile_subs(Desired, Subs) ->
    DesiredSet = sets:from_list(Desired),
    S1 = drop_subs_not_in(DesiredSet, Subs),
    subscribe_for(Desired, S1).

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

subscribe_for(Desired, Subs) ->
    lists:foldl(
      fun({Realm, Topic, LinkPid} = Key, Acc) ->
              case maps:is_key(Key, Acc) of
                  true  -> Acc;
                  false -> subscribe_one(Acc, Key, Realm, Topic, LinkPid)
              end
      end, Subs, Desired).

subscribe_one(Acc, Key, Realm, Topic, LinkPid) ->
    %% Subscriber is `self()' so this router process registers as the
    %% peer-side subscriber for the (realm, topic). The router does
    %% NOT fan inbound EVENTs out itself — `peer_observer' owns
    %% inbound delivery via `deliver_pubsub_typed(event, ...)';
    %% subscribe-on-peer is just the interest signal.
    %% LinkPid may be a `macula_station_link' SDK client OR a
    %% `macula_station_outbound_link' (which gained the SDK API
    %% surface in commit afd3542 — both handle subscribe).
    try macula_station_link:subscribe(LinkPid, Realm, Topic, self()) of
        {ok, SubRef} -> Acc#{Key => SubRef};
        _Other       -> Acc
    catch _:_         -> Acc
    end.

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
%% Cross-station ADVERTISE propagation (single-hop, diff-driven)
%%
%% Each tick we compute the set of (Realm, Procedure) pairs WE
%% directly know about — local handler_registry plus
%% remote_advertise entries whose advertiser is NOT a station we're
%% peering with (= a daemon connected directly to us). For each
%% connected peer (NodeId in peer_observer.conns), we diff against
%% the last set we sent to that peer and ship ADVERTISE for adds,
%% UNADVERTISE for drops. Steady state = zero frames.
%%
%% Single-hop only: we don't propagate gossip we received. That
%% bounds amplification at O(direct-advertises × peers) per change
%% event. For our partial-mesh topology (each station has 3
%% outbound + ~3 inbound peers), every two-stations-at-most pair
%% has at least one shared neighbour through which CALL forwarding
%% works.
%%====================================================================

sync_advertises(Kp, Advertised) ->
    SelfId = try macula_identity:public(Kp) catch _:_ -> undefined end,
    LocalSet = local_advertised_set(SelfId),
    PeerConns = peer_observer_conns(),
    maps:fold(fun(NodeId, ConnPid, Acc) ->
        Last = maps:get(NodeId, Acc, sets:new()),
        ToAdd  = sets:subtract(LocalSet, Last),
        ToDrop = sets:subtract(Last, LocalSet),
        send_advertise_diff(ConnPid, SelfId, ToAdd, ToDrop),
        Acc#{NodeId => LocalSet}
    end, prune_dropped_peers(Advertised, PeerConns), PeerConns).

%% NodeIds whose conns we still hold survive; entries for peers that
%% disconnected get cleared so we don't carry phantom advertise
%% state across reconnects.
prune_dropped_peers(Advertised, PeerConns) ->
    maps:filter(fun(NodeId, _) -> maps:is_key(NodeId, PeerConns) end,
                Advertised).

%% Pull (NodeId → preferred ConnPid) from peer_observer's conns map.
%% Direction preference: inbound first (matches the EVENT-fan-out
%% direction rationale — bytes route to the peer's outbound_link →
%% peer_observer dispatches ADVERTISE → registers in remote_advertise
%% with us as the next-hop target).
%%
%% Reads from peer_observer's public ETS mirror
%% (`macula_station_peer_observer_conns'), written by
%% `write_conn_table/2' on every connection-state change. The earlier
%% implementation called `sys:get_state(peer_observer, 1000)' here,
%% which serialises behind peer_observer's gen_server mailbox. Under
%% live-fleet DHT load that mailbox runs persistently 200-400 deep
%% (~85% `{frame, store}'/`{frame, store_ack}'), so each tick spent
%% ~1 s just reading state — and the router's own queue stayed ~320
%% ticks deep, making `notify_router_change' kicks effectively
%% invisible (cross_station_unary_rpc needed ?ADVERTISE_SETTLE_MS
%% above 2 s to converge). Measured 2026-05-13:
%%   - sys:get_state(peer_observer, 1000): 700-1000 ms per call
%%   - ets:tab2list(?CONNS_TABLE):              50-100 µs per call
peer_observer_conns() ->
    try ets:tab2list(macula_station_peer_observer_conns) of
        Entries ->
            lists:foldl(fun({NodeId, PeerConns}, Acc) ->
                pick_one_conn(NodeId, PeerConns, Acc)
            end, #{}, Entries)
    catch
        error:badarg ->
            %% Table missing — peer_observer not yet booted, or a
            %% stub observer in tests doesn't own the named table.
            %% Fall back to the gen_server path so the test seam
            %% still works.
            fallback_peer_observer_conns()
    end.

fallback_peer_observer_conns() ->
    case whereis(macula_station_peer_observer) of
        undefined -> #{};
        Pid       -> safe_peer_observer_conns(Pid)
    end.

safe_peer_observer_conns(Pid) ->
    try sys:get_state(Pid, 1_000) of
        State -> extract_conn_map(State)
    catch _:_ ->
        #{}
    end.

%% peer_observer's #state{} record: conns lives at element 9 (see
%% the record definition). Direction-aware shape:
%% `#{NodeId => #{inbound, outbound}}'. Pick a live conn per peer.
extract_conn_map(State) when is_tuple(State), tuple_size(State) >= 9 ->
    Conns = element(9, State),
    case is_map(Conns) of
        true  -> maps:fold(fun pick_one_conn/3, #{}, Conns);
        false -> #{}
    end;
extract_conn_map(_) ->
    #{}.

pick_one_conn(NodeId, #{inbound := Pid}, Acc) when is_pid(Pid) ->
    Acc#{NodeId => Pid};
pick_one_conn(NodeId, #{outbound := Pid}, Acc) when is_pid(Pid) ->
    Acc#{NodeId => Pid};
pick_one_conn(_NodeId, _Empty, Acc) ->
    Acc.

%% Set of (Realm, Procedure) the LOCAL station directly knows about:
%%   1. macula_handler_registry (DHT primitives etc.) — realm-blind,
%%      attributed to ?DHT_REALM = <<0:256>>.
%%   2. macula_remote_advertise_registry — daemon-direct entries.
%%      Entries whose advertiser is the SelfId are loops we caused
%%      ourselves and get filtered. (Daemon advertises always have
%%      advertiser = the daemon's pubkey, never SelfId.)
%%
%% We deliberately do NOT include gossip-received entries in this
%% set. Single-hop propagation: we only re-broadcast our own direct
%% advertises. Anti-loop is structural — gossip never echoes.
local_advertised_set(undefined) ->
    sets:new();
local_advertised_set(SelfId) ->
    HandlerSet  = handler_registry_set(),
    DirectAdvSet = direct_remote_advertise_set(SelfId),
    sets:union(HandlerSet, DirectAdvSet).

handler_registry_set() ->
    case whereis(macula_handler_registry) of
        undefined -> sets:new();
        Pid ->
            try
                Procs = macula_handler_registry:list(Pid),
                sets:from_list([{<<0:256>>, P} || P <- Procs])
            catch _:_ -> sets:new()
            end
    end.

direct_remote_advertise_set(SelfId) ->
    case whereis(macula_remote_advertise_registry) of
        undefined -> sets:new();
        Pid ->
            try
                Entries = macula_remote_advertise_registry:list(Pid),
                sets:from_list(
                  [{Realm, Proc}
                   || {Realm, Proc, #{advertiser := Adv}} <- Entries,
                      Adv =/= SelfId])
            catch _:_ -> sets:new()
            end
    end.

send_advertise_diff(_ConnPid, undefined, _ToAdd, _ToDrop) ->
    ok;
send_advertise_diff(ConnPid, SelfId, ToAdd, ToDrop) ->
    sets:fold(fun({Realm, Proc}, _) ->
        Frame = macula_frame:advertise(#{realm => Realm,
                                         procedure => Proc,
                                         advertiser => SelfId}),
        catch macula_peering:send_frame(ConnPid, Frame),
        ok
    end, ok, ToAdd),
    sets:fold(fun({Realm, Proc}, _) ->
        Frame = macula_frame:unadvertise(#{realm => Realm,
                                           procedure => Proc,
                                           advertiser => SelfId}),
        catch macula_peering:send_frame(ConnPid, Frame),
        ok
    end, ok, ToDrop),
    ok.

%%====================================================================
%% Schedule
%%====================================================================

schedule_tick(S) ->
    Ref = make_ref(),
    erlang:send_after(?TICK_MS, self(), tick),
    S#state{timer_ref = Ref}.
