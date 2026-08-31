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
%%% A full sync (`sync/2'):
%%%   1. asks the registry for every materialised realm,
%%%   2. for each realm, asks its pubsub_server for its current topics,
%%%   3. asks `macula_station_peer_links:connections/0' for the live
%%%      `[{Url, LinkPid}]' set,
%%%   4. computes the desired (Realm, Topic, Peer) cross-product,
%%%   5. diffs against the running subscriptions, adds/drops the
%%%      subscribe-on-peer for the changes,
%%%   6. diff-propagates local ADVERTISEs to connected peers.
%%%
%%% This used to run unconditionally on every `?TICK_MS' (2s) tick,
%%% continuously, whether or not anything had actually changed --
%%% O(topics x peers) work every 2 seconds forever, which dominates at
%%% high topic cardinality (see the topic-naming guide's own warning
%%% against per-entity topics: this router's cross-product is exactly
%%% where that cost lands). The moduledoc used to justify this by
%%% saying "V2's hecate_pubsub_server doesn't emit events on
%%% subscription change, so we poll" -- true when written, no longer
%%% true: EVERY production path that can change either half of the
%%% desired cross-product now kicks this router directly (`Pid !
%%% tick', handled below as a prompt out-of-band sync that does NOT
%%% re-arm the periodic timer):
%%%   - inbound SUBSCRIBE / UNSUBSCRIBE frames
%%%     (`macula_station_route_pubsub_frames:deliver_typed/6')
%%%   - a disconnected peer/daemon's subscriptions being purged
%%%     (`macula_station_peer_observer:purge_pubsub_now/3')
%%%   - a peer link registering, unregistering, or dying
%%%     (`macula_station_peer_links' register/unregister/DOWN)
%%%
%%% With every real change already kicking this router at sub-tick
%%% latency, the periodic `timer_tick' now does a full sync ONLY on:
%%% the very first tick after boot (subscriptions/peers may already
%%% exist before this process started, or before a kick could reach
%%% it -- a kick sent while `whereis/1' still resolves to `undefined'
%%% is silently lost), and the periodic RECONCILE (still
%%% `?RECONCILE_EVERY_TICKS', still exists to heal drift a lost kick
%%% would otherwise leave permanent -- same rationale as the ADVERTISE
%%% reconcile below, now applied symmetrically to the pubsub side).
%%% Every OTHER periodic tick is a no-op: re-arm the timer, nothing
%%% else. See `should_periodic_sync/2'.
%%%
%%% Three more fixes from the same design doc, addressing its OTHER
%%% two named cost sources (the first fix above was source A):
%%%
%%%   - **Source B (blocking calls).** The doc's own captured incident
%%%     evidence for the real 178% CPU spike shows the router blocked
%%%     in `gen:do_call' -- `macula_station_link:subscribe/unsubscribe'
%%%     is a synchronous `gen_server:call' with a 5s timeout, one per
%%%     added/removed triple. `subscribe_one/5' now spawns a
%%%     throwaway worker per call instead (see its own comment) so a
%%%     slow/wedged peer link can never stall this singleton; the
%%%     worker reports back via `{sub_result, Key, Result}' so `subs'
%%%     still ends up holding the real `SubRef' once the call actually
%%%     completes (`pending' in the meantime). Unsubscribe doesn't need
%%%     a response (idempotent on the wire) and is fired the same way.
%%%     The doc's OWN principle 2 also calls the router's read calls
%%%     (`list_realms'/`topics') out as blocking risks; those now use
%%%     an explicit short timeout (`?READ_TIMEOUT_MS') instead of the
%%%     bare `gen_server:call/2' default (5s) -- see `safe_list_realms/1'.
%%%     Deliberately NOT the doc's own bigger alternative (a full ETS
%%%     mirror of the pubsub registry's topic set, matching
%%%     `macula_station_peer_observer_conns''s existing pattern): that
%%%     needs new mirror/writer plumbing in the `macula' SDK repo
%%%     itself, a materially larger, cross-repo change left for a
%%%     separate pass.
%%%   - **Source C (kick amplification).** A burst of kicks (e.g. many
%%%     daemons subscribing at once) used to run one full sync PER
%%%     kick (`drain_ticks/0' only coalesced whatever was ALREADY
%%%     queued at dequeue time, not ones arriving mid-sync). Kicks are
%%%     now debounced: the first kick in a burst schedules a sync
%%%     `?KICK_DEBOUNCE_MS' later: every kick in that window is a
%%%     no-op that trusts the sync will still see its cause. Leading-
%%%     edge + fixed delay, not reset-on-every-kick, so sustained churn
%%%     cannot starve it indefinitely -- same shape as
%%%     `macula_station_bloom_exchange''s own established
%%%     `schedule_debounced_rebuild/1', at a much tighter window (this
%%%     router's own latency requirements are stricter -- see the
%%%     `?TICK_MS' comment on why 15s was already too slow for e2e
%%%     probes; 25ms is nowhere near that floor).
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
-export([advertise_to_send/3, should_periodic_sync/2, apply_sub_result/3]).
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

%% Leading-edge kick debounce (source C). Bounds how much added
%% latency a burst of kicks can cost -- fixed, not extended on every
%% new kick within the window -- while still coalescing the burst
%% into one sync instead of one per kick. See the moduledoc.
-define(KICK_DEBOUNCE_MS, 25).

%% Bare `gen_server:call/2' (used by `hecate_pubsub_registry:list_realms/1'
%% / `lookup/2' and `hecate_pubsub_server:topics/1') defaults to a 5s
%% timeout -- exactly the class of stall the design doc's own captured
%% incident evidence shows this router blocked on. `safe_list_realms/1'
%% and friends call the SAME internal messages those wrappers send, but
%% with this much shorter explicit timeout, so a slow/wedged registry
%% or pubsub_server can only stall a sync briefly, not for 5 whole
%% seconds. See the moduledoc's source B section.
-define(READ_TIMEOUT_MS, 500).

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
%% A real subscription reference once the async wire subscribe
%% completes, or `pending' while it's still in flight -- see
%% `subscribe_one/5'/`apply_sub_result/3'.
-type sub_state() :: reference() | pending.

-record(state, {
    pubsub_registry :: atom() | pid(),
    identity        :: macula_identity:key_pair(),
    %% Active inbound subscriptions keyed by `{Realm, Topic, LinkPid}'.
    subs            :: #{triple() => sub_state()},
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
    timer_ref       :: reference() | undefined,
    %% False until the first sync (kicked or periodic) actually runs.
    %% Forces the very first periodic tick to sync unconditionally --
    %% see `should_periodic_sync/2' and the moduledoc.
    synced_once     = false :: boolean(),
    %% The pending debounce timer's tag, or `undefined' if none is
    %% scheduled -- see `handle_info(tick, ...)' and the moduledoc's
    %% source C section. Matched against on fire so a stale message
    %% (already satisfied by a periodic sync in the meantime) no-ops,
    %% same idiom as `macula_station_bloom_exchange''s own
    %% `debounce_ref'.
    debounce_ref    = undefined :: reference() | undefined
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
%% The old code armed a fresh `send_after(?TICK_MS, tick)' on every handled
%% message — periodic tick AND every external kick — and threw away the timer ref
%% (it stored an unrelated `make_ref()' in `timer_ref', which was then never read
%% or cancelled). So every kick that landed in its own mailbox pass PERMANENTLY
%% added another 2s timer. The outstanding-timer count ratcheted up until the
%% station ran back-to-back syncs forever: a live station's router was the top
%% reducer at 1.7M reds/s, always mid-`sync', mailbox ~42. Distinguishing the
%% periodic `timer_tick' from kicks, and re-arming only on `timer_tick', keeps
%% exactly one periodic timer outstanding. Measured 2026-07-25.
handle_info(timer_tick, S0) ->
    {S1, Reconcile} = bump_reconcile(S0),
    {noreply, schedule_tick(maybe_periodic_sync(S1, Reconcile))};
%% A kick: every direct ADVERTISE / SUBSCRIBE / peer change on this station sends
%% `Pid ! tick'. Debounced (source C, see moduledoc): the first kick in a burst
%% schedules a sync `?KICK_DEBOUNCE_MS' later; every kick arriving before it
%% fires is a no-op (idempotent — nothing to reschedule, matching
%% `macula_station_bloom_exchange''s own `schedule_debounced_rebuild/1'). Do NOT
%% arm the periodic timer here — `timer_tick' already does.
handle_info(tick, #state{debounce_ref = undefined} = S) ->
    Ref = make_ref(),
    erlang:send_after(?KICK_DEBOUNCE_MS, self(), {debounced_sync, Ref}),
    {noreply, S#state{debounce_ref = Ref}};
handle_info(tick, S) ->
    {noreply, S};
handle_info({debounced_sync, Ref}, #state{debounce_ref = Ref} = S) ->
    {noreply, sync(S#state{synced_once = true, debounce_ref = undefined}, false)};
%% Stale: a periodic sync (or an even newer debounce) already ran and cleared
%% `debounce_ref' since this timer was armed. Matches
%% `macula_station_bloom_exchange''s own `{rebuild, Ref}' guard exactly.
handle_info({debounced_sync, _Stale}, S) ->
    {noreply, S};
%% A subscribe worker's outcome (source B, see moduledoc) — only accepted
%% while this Key is still `pending'; see `apply_sub_result/3' for what
%% happens otherwise.
handle_info({sub_result, Key, Result}, #state{subs = Subs} = S) ->
    {noreply, S#state{subs = apply_sub_result(Key, Result, Subs)}};
handle_info(_, S) ->
    {noreply, S}.

%% Whether a PERIODIC tick needs to run a real sync, vs. being a no-op
%% that just re-arms the timer. Pure, so the decision can be tested
%% without a live router — same idiom as `advertise_to_send/3'. Kicks
%% (`handle_info(tick, ...)' above) always sync; this only governs the
%% periodic-timer path. See the moduledoc for the full rationale.
-spec should_periodic_sync(Reconcile :: boolean(), SyncedOnce :: boolean()) -> boolean().
should_periodic_sync(_Reconcile, false) -> true;   %% first tick after boot: unconditional
should_periodic_sync(true,       true)  -> true;   %% the periodic reconcile safety net
should_periodic_sync(false,      true)  -> false.  %% nothing changed since the last kick

maybe_periodic_sync(S, Reconcile) ->
    do_maybe_periodic_sync(should_periodic_sync(Reconcile, S#state.synced_once), S, Reconcile).

%% Also clears `debounce_ref': this periodic sync makes any pending
%% debounced kick redundant, and clearing the field makes its eventual
%% `{debounced_sync, Ref}' arrival no-op via the ref-mismatch guard in
%% `handle_info/2' instead of running a second sync right after this
%% one -- same "periodic satisfies any pending debounce" shape as
%% `macula_station_bloom_exchange''s own `{rebuild, Ref}' clause.
do_maybe_periodic_sync(true,  S, Reconcile) -> sync(S#state{synced_once = true, debounce_ref = undefined}, Reconcile);
do_maybe_periodic_sync(false, S, _Reconcile) -> S.

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

keep_sub(true, _Key, _SubState) ->
    true;
%% Subscribe still in flight -- nothing real to cancel on the wire
%% yet. If/when the worker's result lands, `apply_sub_result/3' sees
%% this Key is no longer in `subs' and fires the unsubscribe then.
keep_sub(false, _Key, pending) ->
    false;
keep_sub(false, Key, SubRef) ->
    {_Realm, _Topic, LinkPid} = Key,
    async_unsubscribe(LinkPid, SubRef),
    false.

%% Fire-and-forget in a throwaway process (source B, see moduledoc) --
%% `macula_station_link:unsubscribe/2' is a synchronous 5s-timeout
%% call, and unsubscribe is documented idempotent on the wire, so
%% there's no result worth waiting for here the way there is for
%% subscribe (which needs the real SubRef back to unsubscribe with
%% LATER).
async_unsubscribe(LinkPid, SubRef) ->
    spawn(fun() -> catch macula_station_link:unsubscribe(LinkPid, SubRef) end),
    ok.

subscribe_for(Desired, Subs) ->
    lists:foldl(fun maybe_subscribe/2, Subs, Desired).

maybe_subscribe({Realm, Topic, LinkPid} = Key, Acc) ->
    subscribe_present(maps:is_key(Key, Acc), Acc, Key, Realm, Topic, LinkPid).

subscribe_present(true, Acc, _Key, _Realm, _Topic, _LinkPid) ->
    Acc;
subscribe_present(false, Acc, Key, Realm, Topic, LinkPid) ->
    subscribe_one(Acc, Key, Realm, Topic, LinkPid).

%% Spawns a throwaway worker to make the actual wire call (source B,
%% see moduledoc) instead of calling `macula_station_link:subscribe/4'
%% synchronously here -- a slow/wedged peer link (5s timeout) can no
%% longer stall this singleton, only the one throwaway process making
%% that one call. Marks `Key' `pending' immediately so a subsequent
%% sync in the same window doesn't ALSO try to subscribe it (matches
%% the existing `subscribe_present(true, ...)' no-op-if-already-there
%% behaviour -- `maps:is_key/2' doesn't care about the value).
%%
%% `Router' (captured here, in THIS process, before spawning) is
%% passed as the subscriber, not the worker's own pid -- the link
%% monitors whoever it's told is the subscription's owner, and that
%% must stay this router, not a process that's about to exit the
%% moment the call returns.
subscribe_one(Acc, Key, Realm, Topic, LinkPid) ->
    Router = self(),
    spawn(fun() -> subscribe_worker(Router, Key, Realm, Topic, LinkPid) end),
    Acc#{Key => pending}.

%% LinkPid may be a `macula_station_link' SDK client OR a
%% `macula_station_outbound_link' (which gained the SDK API surface in
%% commit afd3542 — both handle subscribe). The router does NOT fan
%% inbound EVENTs out itself — `peer_observer' owns inbound delivery
%% via `deliver_pubsub_typed(event, ...)'; subscribe-on-peer is just
%% the interest signal.
subscribe_worker(Router, Key, Realm, Topic, LinkPid) ->
    Result = try macula_station_link:subscribe(LinkPid, Realm, Topic, Router) of
        {ok, SubRef} -> {ok, SubRef};
        Other        -> {error, Other}
    catch Class:Reason -> {error, {Class, Reason}}
    end,
    Router ! {sub_result, Key, Result}.

%% What to do with a subscribe worker's outcome. Only a Key still
%% marked `pending' accepts it -- the one attempt we're actually
%% still waiting on. Any other case means this result is STALE (a
%% later sync already decided against this Key, or an even later
%% attempt already resolved it): on success that leaves a real
%% subscription nobody wants, so unsubscribe it right back; on
%% failure there's nothing to do either way. A genuinely concurrent
%% race (two attempts in flight for the same Key at once) is left to
%% the periodic reconcile to sort out, same tolerance the moduledoc
%% already documents for a lost kick.
-spec apply_sub_result(triple(), {ok, reference()} | {error, term()},
                       #{triple() => sub_state()}) -> #{triple() => sub_state()}.
apply_sub_result(Key, Result, Subs) ->
    do_apply_sub_result(maps:find(Key, Subs), Key, Result, Subs).

do_apply_sub_result({ok, pending}, Key, {ok, SubRef}, Subs) ->
    Subs#{Key => SubRef};
do_apply_sub_result({ok, pending}, Key, {error, _}, Subs) ->
    %% Retry on the next sync that still wants this Key -- simpler and
    %% safer than a bespoke retry/backoff loop here.
    maps:remove(Key, Subs);
do_apply_sub_result(_StaleOrAbsent, Key, {ok, SubRef}, Subs) ->
    {_Realm, _Topic, LinkPid} = Key,
    async_unsubscribe(LinkPid, SubRef),
    Subs;
do_apply_sub_result(_StaleOrAbsent, _Key, {error, _}, Subs) ->
    Subs.

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

%% Calls `list_realms'/`lookup'/`topics'' OWN internal messages
%% directly via `gen_server:call/3', bypassing `hecate_pubsub_registry'/
%% `hecate_pubsub_server''s own wrapper functions (which use the bare
%% 2-arity form, a 5s default timeout) -- see the moduledoc's source B
%% section for why a short explicit timeout matters here specifically.
safe_list_realms(Reg) ->
    try gen_server:call(Reg, list_realms, ?READ_TIMEOUT_MS)
    catch _:_ -> []
    end.

safe_topics_for_realm(Reg, Realm) ->
    Lookup = try gen_server:call(Reg, {lookup, Realm}, ?READ_TIMEOUT_MS)
             catch _:_ -> error
             end,
    safe_topics_of(Lookup).

safe_topics_of({ok, Server}) ->
    try gen_server:call(Server, topics, ?READ_TIMEOUT_MS)
    catch _:_ -> []
    end;
safe_topics_of(_) ->
    [].

%%====================================================================
%% Cross-station ADVERTISE propagation (multi-hop, diff + periodic reconcile)
%%
%% Each tick we compute the set of (Realm, Procedure) pairs WE
%% directly know about — local handler_registry plus
%% remote_advertise entries whose advertiser is NOT a station we're
%% peering with (= a daemon connected directly to us). For each
%% connected peer (NodeId in peer_observer.conns), we diff against
%% the last set we sent to that peer and ship ADVERTISE for adds,
%% UNADVERTISE for drops. Steady state = zero frames.
%%
%% ⚠ NOT single-hop. This comment used to claim "we don't propagate
%% gossip we received", but the code does: send_advertise_diff rewrites
%% `advertiser => SelfId' on every relayed frame, so a gossip entry
%% (whose advertiser is the RELAYING station, not self) passes the
%% `Adv =/= SelfId' filter in local_advertised_set/1 and IS re-broadcast.
%% Propagation is distance-vector — each hop re-attributes to itself and
%% routes one hop back toward the origin. The only loop guard is the
%% self-check. This is why a call crosses two stations with no direct
%% edge at all.
%%
%% Because it is diff-driven, it also needed a reconciliation pass, which
%% it did not have until 2026-08-14: see ?RECONCILE_EVERY_TICKS and
%% plans/DESIGN_ADVERTISE_PROPAGATION_RECONCILE.md.
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
%% ⚠ This includes GOSSIP-received entries too, despite the wording
%% below that predates the current code. A gossip entry's advertiser is
%% the relaying peer station (send_advertise_diff sets advertiser =
%% SelfId on relay), never our own SelfId, so `Adv =/= SelfId' keeps it.
%% That is what makes propagation multi-hop. The only entries filtered
%% are self-loops (advertiser = SelfId), which is the anti-echo guard.
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
