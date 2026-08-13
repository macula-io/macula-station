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
-ifdef(TEST).
-export([advertise_to_send/3]).
-endif.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Pre-Gap-B this was 15s, which left e2e cross-station probes
%% timing out before the router could propagate a fresh local
%% SUBSCRIBE to peers. 2s is the right balance: snappy convergence
%% under steady-state churn, still <1% CPU even on a heavily-
%% subscribed station. `peer_observer' also calls `sync_now/1' on
%% every inbound SUBSCRIBE / UNSUBSCRIBE for sub-tick latency.
-define(TICK_MS, 2_000).
%% Every this-many diff ticks, do a FULL advertise re-assert to every
%% peer instead of a diff — reconciliation. 15 x 2s = 30s, matching
%% macula_station_bloom_exchange's periodic full-filter rebuild and
%% macula_station_dht_replicate's periodic full re-STORE. Both exist
%% because diff-only or write-once propagation drifts and never heals;
%% advertise propagation was the one gossip layer WITHOUT this, so a
%% re-advertise that the sender's diff believed it already sent (but the
%% peer had dropped + tombstoned) was lost permanently. See
%% plans/DESIGN_ADVERTISE_PROPAGATION_RECONCILE.md.
-define(RECONCILE_EVERY_TICKS, 15).

%% Registries are REGISTERED NAMES, resolved by gen_server:call/2 on
%% every use. A pid captured at child-spec time is dead the moment the
%% registry restarts, and supervisors reuse the original child spec, so
%% the stale pid would never be replaced.
-type opts() :: #{
    pubsub_registry := atom() | pid(),
    identity        := macula_identity:key_pair()
}.

-export_type([opts/0]).

-type realm() :: <<_:256>>.
-type triple() :: {realm(), Topic :: binary(), LinkPid :: pid()}.

-record(state, {
    pubsub_registry :: atom() | pid(),
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
    %% Diff ticks since the last full reconcile. At ?RECONCILE_EVERY_TICKS
    %% the next periodic tick re-asserts the full advertise set.
    ticks           = 0 :: non_neg_integer(),
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
    %% Deferred first sync, which also arms the single periodic timer.
    self() ! timer_tick,
    {ok, State}.

handle_call(sync_now, _From, S) ->
    {reply, ok, sync(S, false)};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

%% The periodic safety-net sync. Re-arm the timer HERE and ONLY here.
%%
%% The old code armed a fresh `send_after(?TICK_MS, tick)` on every handled
%% message — periodic tick AND every external kick — and threw away the timer ref
%% (it stored an unrelated `make_ref()` in `timer_ref`, which was then never read
%% or cancelled). So every kick that landed in its own mailbox pass PERMANENTLY
%% added another 2s timer. The outstanding-timer count ratcheted up until the
%% station ran back-to-back syncs forever: a live station's router was the top
%% reducer at 1.7M reds/s, always mid-`sync`, mailbox ~42. Distinguishing the
%% periodic `timer_tick` from kicks, and re-arming only on `timer_tick`, keeps
%% exactly one periodic timer outstanding. Measured 2026-07-25.
handle_info(timer_tick, S0) ->
    ok = drain_ticks(),
    {S1, Reconcile} = bump_reconcile(S0),
    {noreply, schedule_tick(sync(S1, Reconcile))};
%% A kick: every direct ADVERTISE / SUBSCRIBE / peer change on this station sends
%% `Pid ! tick'. Sync promptly (latency-sensitive), coalescing a burst via
%% drain_ticks, but do NOT arm a timer — the periodic `timer_tick` already does.
handle_info(tick, S) ->
    ok = drain_ticks(),
    {noreply, sync(S, false)};
handle_info(_, S) ->
    {noreply, S}.

drain_ticks() ->
    receive
        tick -> drain_ticks()
    after 0 ->
        ok
    end.

%% Count only the periodic tick. On the Nth, reset and signal a full
%% reconcile; otherwise a normal diff.
bump_reconcile(#state{ticks = T} = S) when T + 1 >= ?RECONCILE_EVERY_TICKS ->
    {S#state{ticks = 0}, true};
bump_reconcile(#state{ticks = T} = S) ->
    {S#state{ticks = T + 1}, false}.

terminate(_Reason, _S) -> ok.

code_change(_Old, S, _Extra) -> {ok, S}.

%%====================================================================
%% Sync
%%====================================================================

sync(#state{pubsub_registry = Reg,
            subs            = Subs,
            advertised      = Advertised,
            identity        = Kp} = S, Reconcile) ->
    Pairs   = local_realm_topics(Reg),
    Peers   = macula_station_peer_links:connections(),
    Desired = desired_triples(Pairs, Peers),
    Subs1       = reconcile_subs(Desired, Subs),
    Advertised1 = sync_advertises(Kp, Advertised, Reconcile),
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
    maps:filter(fun(Key, SubRef) -> keep_sub(sets:is_element(Key, Desired), Key, SubRef) end,
                Subs).

keep_sub(true, _Key, _SubRef) ->
    true;
keep_sub(false, Key, SubRef) ->
    {_Realm, _Topic, LinkPid} = Key,
    catch macula_station_link:unsubscribe(LinkPid, SubRef),
    false.

subscribe_for(Desired, Subs) ->
    lists:foldl(fun maybe_subscribe/2, Subs, Desired).

maybe_subscribe({Realm, Topic, LinkPid} = Key, Acc) ->
    subscribe_present(maps:is_key(Key, Acc), Acc, Key, Realm, Topic, LinkPid).

subscribe_present(true, Acc, _Key, _Realm, _Topic, _LinkPid) ->
    Acc;
subscribe_present(false, Acc, Key, Realm, Topic, LinkPid) ->
    subscribe_one(Acc, Key, Realm, Topic, LinkPid).

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
    Lookup = try hecate_pubsub_registry:lookup(Reg, Realm)
             catch _:_ -> error
             end,
    safe_topics_of(Lookup).

safe_topics_of({ok, Server}) ->
    try hecate_pubsub_server:topics(Server)
    catch _:_ -> []
    end;
safe_topics_of(_) ->
    [].

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

sync_advertises(Kp, Advertised, Reconcile) ->
    SelfId = try macula_identity:public(Kp) catch _:_ -> undefined end,
    LocalSet = local_advertised_set(SelfId),
    PeerConns = peer_observer_conns(),
    maps:fold(fun(NodeId, ConnPid, Acc) ->
        Last = maps:get(NodeId, Acc, sets:new()),
        {ToAdd, ToDrop} = advertise_to_send(Reconcile, LocalSet, Last),
        send_advertise_diff(ConnPid, SelfId, ToAdd, ToDrop),
        Acc#{NodeId => LocalSet}
    end, prune_dropped_peers(Advertised, PeerConns), PeerConns).

%% What to send a peer this tick. Pure, so the reconcile invariant can be
%% tested without a peer connection.
%%
%% Diff (false): send only what changed since we last synced this peer —
%% steady state is zero frames.
%%
%% Reconcile (true): re-assert the FULL local set as adds, regardless of
%% what we believe we already sent. This is the whole fix — the diff
%% skips an entry it thinks the peer already holds, and if the peer does
%% NOT hold it (dropped frame, tombstone race) that divergence is
%% otherwise permanent. Drops are still the diff (Last - LocalSet):
%% reconciling a stale entry the peer wrongly RETAINS needs the peer to
%% compare, which is deferred (see the DESIGN doc); this heals the
%% measured missing-add case.
advertise_to_send(true, LocalSet, Last) ->
    {LocalSet, sets:subtract(Last, LocalSet)};
advertise_to_send(false, LocalSet, Last) ->
    {sets:subtract(LocalSet, Last), sets:subtract(Last, LocalSet)}.

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
        Entries -> fold_conn_entries(Entries)
    catch
        error:badarg ->
            %% Table missing — peer_observer not yet booted, or a
            %% stub observer in tests doesn't own the named table.
            %% Fall back to the gen_server path so the test seam
            %% still works.
            fallback_peer_observer_conns()
    end.

fold_conn_entries(Entries) ->
    lists:foldl(fun pick_conn_entry/2, #{}, Entries).

pick_conn_entry({NodeId, PeerConns}, Acc) ->
    pick_one_conn(NodeId, PeerConns, Acc).

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
    handler_registry_set(whereis(macula_handler_registry)).

handler_registry_set(undefined) ->
    sets:new();
handler_registry_set(Pid) ->
    try
        Procs = macula_handler_registry:list(Pid),
        sets:from_list([{<<0:256>>, P} || P <- Procs])
    catch _:_ -> sets:new()
    end.

direct_remote_advertise_set(SelfId) ->
    direct_remote_advertise_set(whereis(macula_remote_advertise_registry), SelfId).

direct_remote_advertise_set(undefined, _SelfId) ->
    sets:new();
direct_remote_advertise_set(Pid, SelfId) ->
    try
        Entries = macula_remote_advertise_registry:list(Pid),
        sets:from_list(
          [{Realm, Proc}
           || {Realm, Proc, #{advertiser := Adv}} <- Entries,
              Adv =/= SelfId])
    catch _:_ -> sets:new()
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

%% Arms exactly one periodic timer and keeps its real ref (the old code stored a
%% bogus make_ref/0 and leaked the send_after ref, so timers could never be
%% cancelled and accumulated). Fires `timer_tick', distinct from kick `tick's, so
%% only the periodic path re-arms.
schedule_tick(S) ->
    TRef = erlang:send_after(?TICK_MS, self(), timer_tick),
    S#state{timer_ref = TRef}.
