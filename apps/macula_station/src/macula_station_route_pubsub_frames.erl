%% @doc Routes inbound pubsub frames — receives `subscribe', `unsubscribe',
%% `publish' and `event' frames directly from `macula_peering_conn'
%% (SDK >= 4.4.4 `pubsub_recipient' opt), bypassing
%% `macula_station_peer_observer's mailbox.
%%
%% == Why it exists ==
%%
%% Before 4.4.4 every pubsub frame queued in `peer_observer's
%% gen_server mailbox alongside ADVERTISE / CALL / REPLY / DHT and
%% connection-lifecycle work. Each EVENT carries an Ed25519
%% `publisher_sig' that the relay verifies (~200 µs each) before
%% fanning out, and the multi-publisher e2e cases fire bursts of
%% dozens of EVENTs at once; the observer's mailbox stayed 5-10 k
%% deep through those bursts after the DHT bypass (4.4.3) removed
%% the `store'/`store_ack' chatter. This module owns its own mailbox
%% so EVENT bursts no longer back up the observer's queue, and the
%% observer's frame-dispatch latency stays bounded.
%%
%% == What it does NOT touch ==
%%
%% Connection state, ADVERTISE / CALL / REPLY / STREAM / DHT / SWIM
%% routing — all still owned by `macula_station_peer_observer'. This
%% module is wire-protocol-narrow: pubsub frames in, pubsub work out.
%%
%% == Why a gen_server (not spawn-per-frame) ==
%%
%% Frame ordering matters within a single peering connection — a
%% PUBLISH followed by an UNSUBSCRIBE must process in that order, or
%% the EVENT for the publish lands at a peer whose registry already
%% forgot the subscriber. A gen_server's FIFO mailbox preserves order
%% within a single sender; spawning a worker per frame would not.
%%
%% The same applies to (publisher, seq) dedup: the cache writes must
%% be strictly serialised against the frame stream from each peer.
%%
%% == Conns lookup ==
%%
%% Fan-out (`fan_out_event/3') reads the (NodeId → conn) map from the
%% peer_observer's public ETS mirror
%% (`macula_station_peer_observer_conns'). This is the same lookup
%% path the router uses after the ETS bypass in commit d0f0c8a; cost
%% is microseconds vs the seconds the older `sys:get_state(observer)'
%% path took under load.
-module(macula_station_route_pubsub_frames).
-behaviour(gen_server).

-export([start_link/1, stop/1]).
-export([deliver_verified/5]).
-export([delivery_stats/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type opts() :: #{
    pubsub_registry          => atom() | pid() | undefined,
    conns_table              => atom(),
    pubsub_verify_workers    => pos_integer()
}.

-export_type([opts/0]).

-ifdef(TEST).
%% Exports for unit tests — pure conn resolution over the conns mirror,
%% plus the fan-out entry and counter install so delivery outcomes can be
%% driven without standing up a station.
-export([fan_eligible/2, conn_pid/1, fan_out_event/3, install_counters/0]).
%% Dedup disposition + origin seeding, so the publisher-attestation rule
%% can be driven directly against the cache.
-export([event_dedup_disposition/1, record_origin_seq/2]).
%% Bloom-fan / pattern-fan union, so the two channels' independent
%% counting and de-duped merge can be driven directly against a real
%% bloom_exchange + a locally-built conns table, without standing up
%% the whole dispatcher gen_server.
-export([bloom_fan_extras/4]).
-endif.

-define(CONNS_TABLE, macula_station_peer_observer_conns).

%% Verify-worker pool size. Ed25519 `verify_pubsub' is ~200µs CPU-
%% bound; doing it on the dispatcher's main loop caps the station at
%% ~5k EVENT/s and lets sustained pubsub torture back up the mailbox
%% (observed 25k+ on a 22k-event flood at 1466 pubs/s pre-fix). The
%% workers hash by NodeId so per-peer frame ordering is preserved
%% (PUBLISH-then-UNSUBSCRIBE from one peer always lands on the same
%% worker's FIFO mailbox). Sized to match scheduler count by default
%% so verify scales with cores. Min 2; using 1 collapses to the
%% serial path with extra overhead.
-define(WORKER_POOL_KEY, pubsub_verify_workers).

%% Delivery-outcome counters.
%%
%% Every branch that ends an EVENT's journey without handing it to a
%% live conn used to be unobservable. Three of them (`fan_out_event/3'
%% on an empty target list, `send_to_conn/2' with no pid,
%% `on_relay_publish/3' on `{error, _}') just returned `ok' with no log
%% at all, and the two duplicate-drop branches log at `debug', which is
%% off in production. That is the direct reason months of "multi-hop
%% feels flaky" produced no evidence: the branches that lose a message
%% could not be counted, so no measurement could distinguish them.
%%
%% Read with `delivery_stats/0'. `counters' + `persistent_term' is the
%% idiom `macula_station_event_dedup' already uses for `dup_count/0'.
%% Bumps no-op when the array is absent (this module not started, e.g.
%% a test wiring only peer_observer), so instrumentation never changes
%% behaviour.
-define(PT_COUNTERS, {?MODULE, delivery_counters}).
-define(CTR_FORWARDED,         1).  %% frame handed to a live conn
-define(CTR_DUP_DROPPED,       2).  %% post-verify (publisher,seq) repeat
-define(CTR_DUP_DROPPED_PRE,   3).  %% pre-verify peek said seen
-define(CTR_NO_TARGETS,        4).  %% nobody to fan to at all
-define(CTR_NO_LIVE_CONN,      5).  %% target chosen, no live worker
-define(CTR_NO_BLOOM_MATCH,    6).  %% no peer bloom matched the topic
-define(CTR_RELAY_PUBLISH_ERR, 7).  %% registry refused the PUBLISH
%% Two DISTINCT refusals, deliberately separate counters. Bumping one
%% counter from both made the live signal uninterpretable: an unsigned
%% EVENT is harmless (it was always delivered and never needed the
%% cache), while a PUBLISH naming a third party means origin seeding
%% has STOPPED for that publisher. Only the second is a behaviour
%% change worth acting on.
-define(CTR_UNAUTH_EVENT,      8).  %% EVENT with no verified publisher_sig
-define(CTR_UNAUTH_ORIGIN,     9).  %% PUBLISH publisher =/= connection id
%% Attribution for `no_targets'. Most of it never reaches the bloom path at
%% all: an empty target list needs empty extras, and for a non-mesh binary
%% topic that always bumps `no_bloom_match' first -- so anything by which
%% `no_targets' EXCEEDS `no_bloom_match' is mesh-internal or topic-less, and
%% was previously unattributable. `_mesh.bloom.local' is the bulk of it: it is
%% a diagnostic feed the realm dashboard subscribes to, so at the four
%% stations where the realm is not subscribed it lands with no subscriber by
%% design. That is terminus, not loss, and it must be labelled as such or it
%% masquerades as a delivery failure.
-define(CTR_MESH_TOPIC,       10).  %% `_mesh.*', never bloom-fanned
-define(CTR_NO_TOPIC,         11).  %% frame carried no topic
-define(CTR_SEND_REFUSED,     12).  %% send_frame/2 refused the frame
%% The two reasons a bloom candidate is unreachable mean OPPOSITE things.
-define(CTR_BLOOM_NOT_NEIGHBOUR, 13).  %% transitive bloom named a non-peer
-define(CTR_BLOOM_CONN_DEAD,     14).  %% we held a conn to it and it died
%% Pattern-fan (2026-08-29) is a SEPARATE interest channel from the
%% Bloom, and gets its own counter rather than being folded into
%% `?CTR_NO_BLOOM_MATCH' -- deliberately, matching this module's own
%% established discipline (see the `?CTR_UNAUTH_EVENT'/`?CTR_UNAUTH_ORIGIN'
%% split above): conflating "no bloom match" with "no pattern match"
%% would make it impossible to tell which gossip channel is actually
%% failing to converge from the counters alone.
-define(CTR_NO_PATTERN_MATCH, 15).  %% no peer pattern matched the topic
-define(CTR_SLOTS,            15).

-record(state, {
    pubsub_registry :: atom() | pid() | undefined,
    conns_table     :: atom(),
    workers         :: tuple()       %% tuple of worker pids, 1..N
}).

%%==================================================================
%% API
%%==================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Deliver an already-verified pubsub frame. Entry point for the
%% LEGACY `controlling_pid' path in `macula_station_peer_observer'.
%%
%% The SDK routes pubsub frames straight here when `pubsub_recipient'
%% resolves, and falls back to `controlling_pid' when it does not — a
%% recipient restart gap, or an SDK older than 4.4.4. Those fallback
%% frames must be delivered by the SAME code as the bypass path.
%% peer_observer used to carry its own copy of the whole delivery path,
%% and the two drifted: fixes landed on one and not the other, and a
%% station could serve two different pubsub disciplines depending on
%% which path a given connection happened to take.
%%
%% The conns mirror is read instead of peer_observer's in-state map.
%% They are written in lockstep (see `write_conn_table/2' there: "Both
%% write the same value the `conns' map sees"), so this is the same
%% state, reached without a gen_server round trip.
-spec deliver_verified(macula_frame:frame_type(), <<_:256>>,
                       macula_identity:pubkey(), macula_frame:frame(),
                       pid()) -> ok.
deliver_verified(Type, Realm, NodeId, Verified, Reg) ->
    deliver_typed(Type, Realm, NodeId, Verified, Reg, ?CONNS_TABLE).

%% @doc Delivery outcomes since the counters were installed. Counts are
%% cumulative and survive a restart of this module on purpose — a
%% restart is precisely the event under investigation, so zeroing them
%% here would erase the evidence.
-spec delivery_stats() -> #{atom() => non_neg_integer()}.
delivery_stats() ->
    stats_of(persistent_term:get(?PT_COUNTERS, undefined)).

stats_of(undefined) ->
    #{};
stats_of(Ref) ->
    #{forwarded          => counters:get(Ref, ?CTR_FORWARDED),
      dup_dropped        => counters:get(Ref, ?CTR_DUP_DROPPED),
      dup_dropped_pre    => counters:get(Ref, ?CTR_DUP_DROPPED_PRE),
      no_targets         => counters:get(Ref, ?CTR_NO_TARGETS),
      no_live_conn       => counters:get(Ref, ?CTR_NO_LIVE_CONN),
      no_bloom_match     => counters:get(Ref, ?CTR_NO_BLOOM_MATCH),
      no_pattern_match   => counters:get(Ref, ?CTR_NO_PATTERN_MATCH),
      relay_publish_err  => counters:get(Ref, ?CTR_RELAY_PUBLISH_ERR),
      unauth_event       => counters:get(Ref, ?CTR_UNAUTH_EVENT),
      unauth_origin      => counters:get(Ref, ?CTR_UNAUTH_ORIGIN),
      mesh_topic         => counters:get(Ref, ?CTR_MESH_TOPIC),
      no_topic           => counters:get(Ref, ?CTR_NO_TOPIC),
      send_refused       => counters:get(Ref, ?CTR_SEND_REFUSED),
      bloom_not_neighbour => counters:get(Ref, ?CTR_BLOOM_NOT_NEIGHBOUR),
      bloom_conn_dead    => counters:get(Ref, ?CTR_BLOOM_CONN_DEAD)}.

install_counters() ->
    install_counters(persistent_term:get(?PT_COUNTERS, undefined)).

install_counters(undefined) ->
    persistent_term:put(?PT_COUNTERS,
                        counters:new(?CTR_SLOTS, [write_concurrency]));
install_counters(_Ref) ->
    ok.

bump(Slot) ->
    bump_ref(persistent_term:get(?PT_COUNTERS, undefined), Slot).

bump_ref(undefined, _Slot) -> ok;
bump_ref(Ref, Slot)        -> counters:add(Ref, Slot, 1).

%%==================================================================
%% gen_server
%%==================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    install_counters(),
    Reg = maps:get(pubsub_registry, Opts, undefined),
    CT  = maps:get(conns_table, Opts, ?CONNS_TABLE),
    Workers = spawn_workers(worker_pool_size(Opts), Reg, CT),
    {ok, #state{
        pubsub_registry = Reg,
        conns_table     = CT,
        workers         = Workers
    }}.

worker_pool_size(Opts) ->
    %% Default: max(8, schedulers_online). Even on a 2-scheduler
    %% container the extra slots improve hash distribution under
    %% bursty single-publisher load (round-robin already used for
    %% PUBLISH/EVENT, but more slots reduce phash2 collisions on
    %% SUBSCRIBE/UNSUBSCRIBE bursts from many distinct daemons).
    %% Process cost is negligible — workers idle in `receive' until
    %% work arrives.
    Configured = maps:get(?WORKER_POOL_KEY, Opts,
                          max(8, erlang:system_info(schedulers_online))),
    max(2, Configured).

spawn_workers(N, Reg, CT) ->
    list_to_tuple([spawn_worker(Reg, CT) || _ <- lists:seq(1, N)]).

spawn_worker(Reg, CT) ->
    spawn_link(fun() -> worker_loop(Reg, CT) end).

worker_loop(Reg, CT) ->
    receive
        {pubsub_frame, NodeId, Frame} ->
            Type = macula_frame:frame_type(Frame),
            T0 = erlang:monotonic_time(microsecond),
            process_frame(Type, Frame, NodeId, Reg, CT),
            T1 = erlang:monotonic_time(microsecond),
            macula_station_frame_telemetry:record(Type, dispatch_self, T1 - T0),
            worker_loop(Reg, CT);
        {pubsub_frame, NodeId, Frame, RecvAtUs} ->
            Type = macula_frame:frame_type(Frame),
            T0 = erlang:monotonic_time(microsecond),
            macula_station_frame_telemetry:record(Type, recv_to_dispatch,
                                                  T0 - RecvAtUs),
            process_frame(Type, Frame, NodeId, Reg, CT),
            T1 = erlang:monotonic_time(microsecond),
            macula_station_frame_telemetry:record(Type, dispatch_self, T1 - T0),
            worker_loop(Reg, CT);
        stop ->
            ok
    end.

%% Pre-verify dedup for inbound EVENTs. Receiver-side bloom-fan
%% causes O(mesh-size) EVENT amplification per publish (every node
%% fans to direct peers, dedup catches the duplicates downstream).
%% Verifying every duplicate before dropping costs ~200µs Ed25519 per
%% frame, which dominated the worker CPU budget under sustained load
%% (100k publishes * ~30 fan paths = ~3M verify calls across the
%% mesh, ~600 scheduler-seconds total).
%%
%% The (publisher, seq, topic) fields are present in the unverified
%% frame body. A duplicate-claim from an attacker can force a
%% *legitimate* EVENT to be dropped at this hop, but cannot inject
%% content (the deliver path still verifies for `new' arrivals).
%% Worst-case security impact = a denial-of-delivery for one
%% realm-topic, which requires the attacker to predict the publisher's
%% next seq. Net trade is a clear win.
%%
%% Other frame types (PUBLISH, SUBSCRIBE, UNSUBSCRIBE) verify as
%% before — they're not amplified, and PUBLISH carries the publisher
%% signature we want to enforce at the origin.
process_frame(event, Frame, NodeId, Reg, CT) ->
    case fast_dedup_check(Frame) of
        drop ->
            ok;
        proceed ->
            handle_pubsub_frame(verify_pubsub(Frame, NodeId), NodeId, Frame,
                                worker_state(Reg, CT))
    end;
process_frame(_Type, Frame, NodeId, Reg, CT) ->
    handle_pubsub_frame(verify_pubsub(Frame, NodeId), NodeId, Frame,
                        worker_state(Reg, CT)).

%% Look up (publisher, seq, topic) in the dedup cache without bumping
%% the counter. If already-seen, drop pre-verify. If new, return
%% `proceed' and let the post-verify path do the actual record (so
%% the cache only counts events we successfully verified + delivered
%% — this matches existing telemetry semantics).
%%
%% Defensive: missing/malformed publisher, seq or topic → proceed
%% (cannot dedup, never drop blindly).
fast_dedup_check(#{publisher := Pub, seq := Seq, topic := Topic})
  when is_binary(Pub), is_integer(Seq), is_binary(Topic) ->
    pre_verify_disposition(macula_station_event_dedup:peek(Pub, Seq, Topic));
fast_dedup_check(_) ->
    proceed.

pre_verify_disposition(seen) ->
    bump(?CTR_DUP_DROPPED_PRE),
    drop;
pre_verify_disposition(absent) ->
    proceed.

worker_state(Reg, CT) ->
    #state{pubsub_registry = Reg, conns_table = CT, workers = {}}.

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

%% Dispatcher fans inbound pubsub frames to a verify worker. Slot
%% selection depends on frame type:
%%
%%   * SUBSCRIBE / UNSUBSCRIBE — `phash2(NodeId, N)'. Per-peer FIFO
%%     ordering preserved (a SUB followed by UNSUB from one daemon
%%     must process in that order, or the server's subscriber set
%%     is wrong).
%%
%%   * PUBLISH / EVENT — `phash2(make_ref(), N)'. No per-peer
%%     ordering required: `event_dedup' is `(publisher, seq)'-keyed
%%     and atomic via `ets:insert_new/2', so out-of-order processing
%%     is dedup-safe. Round-robin spreads a single publisher's burst
%%     across all workers — without this, a single hot publisher
%%     pinned to one slot back-pressures only that worker (observed
%%     364k mailbox depth on a 100k-event torture).
handle_info({macula_peering, pubsub_frame, _ConnPid, NodeId, Frame},
            #state{pubsub_registry = Reg, workers = Ws} = S)
        when is_binary(NodeId), byte_size(NodeId) =:= 32,
             is_map(Frame), Reg =/= undefined ->
    dispatch_frame(Ws, NodeId, Frame, {pubsub_frame, NodeId, Frame}),
    {noreply, S};
handle_info({macula_peering, pubsub_frame, _ConnPid, NodeId, Frame, RecvAtUs},
            #state{pubsub_registry = Reg, workers = Ws} = S)
        when is_binary(NodeId), byte_size(NodeId) =:= 32,
             is_map(Frame), Reg =/= undefined ->
    dispatch_frame(Ws, NodeId, Frame, {pubsub_frame, NodeId, Frame, RecvAtUs}),
    {noreply, S};
handle_info({macula_peering, pubsub_frame, _ConnPid, _NodeId, _Frame}, S) ->
    %% No registry yet — drop. peer_observer used to log a warning
    %% here; we keep the behaviour quiet because this branch only
    %% fires for a brief boot window or in tests that don't wire
    %% the registry.
    {noreply, S};
handle_info({macula_peering, pubsub_frame, _ConnPid, _NodeId, _Frame, _T}, S) ->
    %% Same fall-through for the 6-tuple shape.
    {noreply, S};
handle_info({'EXIT', Pid, Reason},
            #state{pubsub_registry = Reg, conns_table = CT,
                   workers = Ws} = S) ->
    {noreply, S#state{workers = replace_dead_worker(Ws, Pid, Reason, Reg, CT)}};
handle_info(_Msg, S) ->
    {noreply, S}.

dispatch_frame(Ws, NodeId, Frame, Msg) ->
    Size = tuple_size(Ws),
    Idx  = slot_for(macula_frame:frame_type(Frame), NodeId, Size),
    element(Idx, Ws) ! Msg,
    ok.

%% Per-peer FIFO for SUBSCRIBE/UNSUBSCRIBE; round-robin for
%% PUBLISH/EVENT (see `handle_info' comment for the rationale).
%% Unknown frame types fall back to per-peer FIFO — safer default
%% for anything that might carry an ordering constraint.
slot_for(subscribe, NodeId, Size)   -> erlang:phash2(NodeId, Size) + 1;
slot_for(unsubscribe, NodeId, Size) -> erlang:phash2(NodeId, Size) + 1;
slot_for(publish, _NodeId, Size)    -> erlang:phash2(make_ref(), Size) + 1;
slot_for(event, _NodeId, Size)      -> erlang:phash2(make_ref(), Size) + 1;
slot_for(_Other, NodeId, Size)      -> erlang:phash2(NodeId, Size) + 1.

%% A dead worker is replaced with a fresh spawn-link at the same
%% slot. The new worker has an empty mailbox — any frames in the
%% dead worker's mailbox are lost, but a dead worker means a verify
%% crash on a malformed frame; preserving the rest of the mailbox
%% would re-crash on the same frame. Logged loud so a recurring
%% crash pattern is visible.
replace_dead_worker(Ws, Pid, Reason, Reg, CT) ->
    case find_worker_index(Ws, Pid, 1) of
        not_found ->
            Ws;
        Idx ->
            logger:warning(
              "[pubsub_dispatcher] verify worker died at slot ~p reason=~p — respawning",
              [Idx, Reason]),
            setelement(Idx, Ws, spawn_worker(Reg, CT))
    end.

find_worker_index(Ws, _Pid, Idx) when Idx > tuple_size(Ws) -> not_found;
find_worker_index(Ws, Pid, Idx) ->
    case element(Idx, Ws) of
        Pid   -> Idx;
        _Else -> find_worker_index(Ws, Pid, Idx + 1)
    end.

terminate(_Reason, #state{workers = Ws}) ->
    %% Best-effort orderly shutdown; the EXIT propagation from the
    %% dispatcher would kill them anyway, but `stop' message lets a
    %% currently-mid-verify worker finish without an abrupt exit.
    lists:foreach(fun(W) -> catch (W ! stop) end, tuple_to_list(Ws)),
    ok;
terminate(_Reason, _S) -> ok.
code_change(_Old, S, _Extra) -> {ok, S}.

%%==================================================================
%% Dispatch
%%==================================================================

handle_pubsub_frame({error, Reason}, NodeId, Frame, _S) ->
    logger:warning(
      "[pubsub_dispatcher] verify failed: ~p type=~p topic=~s "
      "realm=~s peer_node_id=~s publisher=~s has_publisher_sig=~p",
      [Reason,
       macula_frame:frame_type(Frame),
       maps:get(topic, Frame, <<>>),
       short_hex(maps:get(realm, Frame, <<>>)),
       short_hex(NodeId),
       short_hex(maps:get(publisher, Frame, <<>>)),
       maps:is_key(publisher_sig, Frame)]),
    ok;
handle_pubsub_frame({ok, Verified}, NodeId, _Frame,
                    #state{pubsub_registry = Reg,
                           conns_table     = CT}) ->
    Realm = maps:get(realm, Verified),
    Topic = maps:get(topic, Verified, undefined),
    Type  = macula_frame:frame_type(Verified),
    logger:debug("[pubsub_dispatcher] ~s realm=~p topic=~s",
                 [Type, Realm, Topic]),
    deliver_typed(Type, Realm, NodeId, Verified, Reg, CT).

%% PUBLISH from a remote daemon. Seed (publisher, seq) at the origin
%% so a looped-back EVENT for the same publish is recognised as a
%% duplicate; then build the EVENT frame in the realm's pubsub_server
%% and fan out to each matched local subscriber's peering connection,
%% plus to direct outbound peers whose Bloom filter matches the topic
%% (publisher-side bloom-fan, see `bloom_fan_extras/3').
deliver_typed(publish, Realm, NodeId, Verified, Reg, CT) ->
    record_origin_seq(Verified, NodeId),
    on_relay_publish(
      hecate_pubsub_registry:relay_publish(Reg, Realm, Verified),
      NodeId, CT);

%% Inbound EVENT — cross-station relay. Find local subscribers and
%% fan out. The (publisher, seq) dedup cache is the loop-kill for
%% publisher-signed EVENTs (they verify end-to-end at every hop, so
%% the older verify-fail accident no longer bounds them). A repeat
%% without a publisher_sig is logged but still delivered — those are
%% still bounded by the verify-fail mismatch one hop out.
%%
%% After local delivery, also bloom-fan to direct outbound peers
%% whose filter matches the topic — minus the source peer
%% (`NodeId') to avoid immediate echo. `event_dedup' at every
%% receiver kills further loops.
deliver_typed(event, Realm, NodeId, Verified, Reg, CT) ->
    Disp = event_dedup_disposition(Verified),
    case Disp of
        drop ->
            ok;
        deliver ->
            deliver_inbound_event(safe_lookup(Reg, Realm), Verified,
                                  NodeId, CT)
    end;

deliver_typed(subscribe, Realm, NodeId, Verified, Reg, _CT) ->
    _ = hecate_pubsub_registry:dispatch_frame(Reg, Realm, NodeId, Verified),
    %% Snap the router to a sync NOW so this fresh subscriber gets
    %% propagated to peer stations within milliseconds rather than
    %% waiting up to ?TICK_MS for the next periodic poll. Same
    %% rationale as the peer_observer path used to have — without
    %% this trigger, e2e probes time out before the router notices.
    notify_router_change(),
    %% Also nudge the bloom-exchange: the local topic set just gained
    %% an entry, so our outgoing Bloom should pick it up on the
    %% debounce (~2s) instead of the periodic 30s tick.
    notify_bloom_change(),
    ok;

deliver_typed(unsubscribe, Realm, NodeId, Verified, Reg, _CT) ->
    _ = hecate_pubsub_registry:dispatch_frame(Reg, Realm, NodeId, Verified),
    notify_router_change(),
    notify_bloom_change(),
    ok;

deliver_typed(_Other, Realm, NodeId, Verified, Reg, _CT) ->
    _ = hecate_pubsub_registry:dispatch_frame(Reg, Realm, NodeId, Verified),
    ok.

%%==================================================================
%% Helpers (lifted from macula_station_peer_observer; keep parity)
%%==================================================================

%% An EVENT carrying a publisher-end-to-end signature is verified
%% against the publisher (so it passes at any relay hop, not just
%% one); loop prevention falls to the (publisher, seq) dedup cache.
%% Everything else — SUBSCRIBE / UNSUBSCRIBE / PUBLISH, and EVENTs
%% from a daemon not yet emitting `publisher_sig' — is verified
%% against the connection's NodeId.
verify_pubsub(#{frame_type := event, publisher_sig := _} = Frame, _NodeId) ->
    macula_frame:verify_publisher(Frame);
verify_pubsub(Frame, NodeId) ->
    macula_frame:verify(Frame, NodeId).

safe_lookup(Reg, Realm) ->
    try hecate_pubsub_registry:lookup(Reg, Realm)
    catch _:_ -> {error, registry_unavailable}
    end.

deliver_inbound_event({ok, Server}, EventFrame, SourceNodeId, CT) ->
    Matched = try hecate_pubsub_server:deliver_event(Server, EventFrame)
              catch _:_ -> []
              end,
    fan_out_event(EventFrame,
                  Matched ++ bloom_fan_extras(EventFrame, Matched,
                                              [SourceNodeId], CT),
                  CT);
deliver_inbound_event(_Other, EventFrame, SourceNodeId, CT) ->
    %% No pubsub_server materialised here — still useful to bloom-fan,
    %% since we may sit on the gossip path between a publisher we have
    %% no chain from and downstream subscribers.
    fan_out_event(EventFrame,
                  bloom_fan_extras(EventFrame, [],
                                   [SourceNodeId], CT), CT).

on_relay_publish({ok, EventFrame, Matched}, _SourceNodeId, CT) ->
    %% Origin station — no source peer to exclude (the PUBLISH came
    %% from a locally-connected daemon, never in `peer_blooms').
    fan_out_event(EventFrame,
                  Matched ++ bloom_fan_extras(EventFrame, Matched, [], CT),
                  CT);
on_relay_publish({error, Reason}, NodeId, _CT) ->
    %% The registry refused the PUBLISH (realm mismatch, no server).
    %% This dropped the message and said nothing at all — not even at
    %% debug — so a station quietly discarding a realm's traffic was
    %% invisible. Warn, because unlike a duplicate this is never normal.
    bump(?CTR_RELAY_PUBLISH_ERR),
    logger:warning("[route_pubsub_frames] relay_publish refused: ~p peer=~s",
                   [Reason, short_hex(NodeId)]),
    ok.

%% Compute the bloom-fan extras for an EVENT frame: peer NodeIds
%% whose Bloom matches the topic, minus those already in `Matched'
%% (subscribe-on-peer chain), minus any in `Excluded' (the source
%% peer on a relay hop), minus peers we have no live conn to. The
%% conn intersection is critical: `peer_blooms' is transitively
%% merged, so it can name stations we don't directly peer with.
%%
%% Mesh-internal topics (`_mesh.*') are skipped — `bloom_exchange'
%% already broadcasts them directly; a second fan would mess with
%% the gossip cadence (same reason `peering_router' filters them).
bloom_fan_extras(EventFrame, Matched, Excluded, CT) ->
    case maps:get(topic, EventFrame, undefined) of
        undefined ->
            bump(?CTR_NO_TOPIC),
            [];
        <<"_mesh.", _/binary>> ->
            bump(?CTR_MESH_TOPIC),
            [];
        Topic when is_binary(Topic) ->
            bloom_fan_extras_for_topic(Topic, Matched, Excluded, CT)
    end.

%% Bloom fan and pattern fan are two SEPARATE, independently-counted
%% interest channels (see `?CTR_NO_PATTERN_MATCH''s own comment) —
%% unioned and deduped only at the very end, after each has had its
%% own chance to report "found nothing".
bloom_fan_extras_for_topic(Topic, Matched, Excluded, CT) ->
    lists:usort(
      count_bloom_match(bloom_fan_candidates(Topic, Matched, Excluded, CT))
      ++ count_pattern_match(pattern_fan_candidates(Topic, Matched, Excluded, CT))).

%% An empty bloom result means no downstream peer is known to want this
%% topic. Legitimate at a leaf, but also what a not-yet-gossiped bloom
%% looks like (30s tick, 2s debounce), and the two are indistinguishable
%% without this count.
count_bloom_match([]) ->
    bump(?CTR_NO_BLOOM_MATCH),
    [];
count_bloom_match(Candidates) ->
    Candidates.

%% Same "found nothing" ambiguity as `count_bloom_match/1', for the
%% pattern channel — legitimate (no wildcard subscriber anywhere cares)
%% or not-yet-gossiped (same 30s/2s cadence, shared with the bloom).
count_pattern_match([]) ->
    bump(?CTR_NO_PATTERN_MATCH),
    [];
count_pattern_match(Candidates) ->
    Candidates.

bloom_fan_candidates(Topic, Matched, Excluded, CT) ->
    %% ETS-bypass: read the peer_blooms mirror directly. Avoids the
    %% per-event `gen_server:call' to bloom_exchange that would
    %% serialise the entire station's pubsub fan-out path under
    %% sustained load (torture observed pubsub_dispatcher mailbox
    %% backing up to 25k+ when this used the gen_server path).
    Candidates = macula_station_bloom_exchange:peer_matches_ets(Topic),
    filter_fan_candidates(Candidates, Matched, Excluded, CT).

%% Same ETS-bypass shape as `bloom_fan_candidates/4`, reading the
%% pattern mirror instead. Reuses `filter_fan_candidates/4' unchanged —
%% the same "already matched / excluded source / no live conn"
%% filtering applies identically regardless of which channel a
%% candidate was found through.
pattern_fan_candidates(Topic, Matched, Excluded, CT) ->
    Candidates = macula_station_bloom_exchange:pattern_matches_ets(Topic),
    filter_fan_candidates(Candidates, Matched, Excluded, CT).

filter_fan_candidates(Candidates, Matched, Excluded, CT) ->
    Skip = sets:from_list(Matched ++ Excluded),
    [NodeId
     || NodeId <- Candidates,
        not sets:is_element(NodeId, Skip),
        fan_eligible(CT, NodeId)].

%% Separates the two reasons a bloom candidate is unreachable, because they
%% mean opposite things and lumping them together is why "no_live_conn = 0"
%% read as "nothing is being lost".
fan_eligible(CT, NodeId) ->
    classify_fan_conn(ets_lookup_conn(CT, NodeId)).

%% No conns row at all. `peer_observer' deletes the row once both directions
%% clear, so this is the steady state for a peer we simply do not neighbour —
%% and the transitive bloom NAMES non-neighbours by design (see
%% `macula_station_bloom_exchange:peer_matches/2', whose docstring requires
%% callers to intersect with their own direct peers). Benign, and expected to
%% be the common case on a merged-bloom mesh.
classify_fan_conn(error) ->
    bump(?CTR_BLOOM_NOT_NEIGHBOUR),
    false;
classify_fan_conn({ok, _} = Lookup) ->
    fan_conn_live(conn_pid(Lookup)).

%% A row exists but no direction resolves to a live pid: we held a conn to
%% this peer and it died without the DOWN having been processed yet. This is
%% the flap window, and it is the only one of the two that can drop an event
%% that was otherwise deliverable.
fan_conn_live(undefined) ->
    bump(?CTR_BLOOM_CONN_DEAD),
    false;
fan_conn_live(_Pid) ->
    true.

%% Resolve the conn worker an EVENT would actually be handed to.
%% Delivery PREFERS inbound and falls back to outbound (see
%% `send_event_to_sub/2'), so eligibility resolves in the same order —
%% otherwise eligibility and fan-out can disagree about the same peer.
conn_pid({ok, #{inbound := In, outbound := Out}}) ->
    prefer_live(live(In), Out);
conn_pid(_Other) ->
    undefined.

prefer_live(undefined, Out) -> live(Out);
prefer_live(Pid, _Out)      -> Pid.

%% `is_process_alive/1' raises `badarg' for a remote pid; conn workers
%% are same-BEAM by design, so locality is a guard, not a check.
live(Pid) when is_pid(Pid), node(Pid) =:= node() ->
    live_pid(Pid, erlang:is_process_alive(Pid));
live(Pid) when is_pid(Pid) ->
    Pid;
live(_NotAPid) ->
    undefined.

live_pid(Pid, true)   -> Pid;
live_pid(_Pid, false) -> undefined.

%% Fan-out walks the matched subscriber list and looks each one up in
%% the conns ETS table directly. Previously this function received a
%% pre-materialised map, but rebuilding that map for EVERY inbound
%% event (via `ets:tab2list/1' + `maps:from_list/1' over a 40-entry
%% table) added milliseconds-per-event of overhead — under the
%% bypass the dispatcher mailbox climbed to 365k as a result.
%% Per-NodeId `ets:lookup/2' is O(1) and ~µs.
%% Emptiness is counted HERE, once, on the whole target list — not in
%% the recursion's base clause, which also fires at the end of every
%% successful fan and would count those as losses.
fan_out_event(EventFrame, Targets, CT) ->
    count_empty_targets(Targets),
    send_each(EventFrame, Targets, CT).

count_empty_targets([]) -> bump(?CTR_NO_TARGETS);
count_empty_targets(_)  -> ok.

send_each(_EventFrame, [], _CT) ->
    ok;
send_each(EventFrame, [Sub | Rest], CT) ->
    send_event_to_sub(ets_lookup_conn(CT, Sub), EventFrame),
    send_each(EventFrame, Rest, CT).

ets_lookup_conn(CT, NodeId) ->
    try ets:lookup(CT, NodeId) of
        [{_, PeerConns}] -> {ok, PeerConns};
        []               -> error
    catch
        _:_ -> error
    end.

%% EVENT delivery PREFERS the inbound conn — the one the peer originally
%% sent its SUBSCRIBE through. Sending via outbound would land on the
%% peer's listener-side conn and dead-end (the peer has no subscribers
%% there). Falls back to outbound when inbound is missing OR DEAD.
%%
%% Liveness matters here as much as presence: `is_pid/1' alone accepted a
%% dead inbound worker and never tried the live outbound one, so the
%% frame was cast into a dead mailbox and dropped by the VM while a
%% usable path sat unused. Shares `conn_pid/1' with `fan_eligible/2' so
%% fan-eligibility and fan-out cannot disagree.
send_event_to_sub(Lookup, EventFrame) ->
    send_to_conn(conn_pid(Lookup), EventFrame).

send_to_conn(undefined, _EventFrame) ->
    %% A target we believed was reachable turned out to have no live
    %% worker. This is a LOST event, and it was the most silent branch
    %% in the module.
    bump(?CTR_NO_LIVE_CONN),
    ok;
send_to_conn(Pid, EventFrame) ->
    count_send(macula_peering:send_frame(Pid, EventFrame)).

%% `send_frame/2' validates before casting and returns `{error, _}' for an
%% unsendable frame WITHOUT sending it (`macula_peering:cast_checked/3').
%% Bumping `forwarded' before the call counted those refusals as deliveries
%% and inflated the denominator of every ratio derived from these counters.
%% On this fleet a refusal is not hypothetical: colliding wire keys and float
%% payloads have both produced one.
count_send(ok) ->
    bump(?CTR_FORWARDED),
    ok;
count_send({error, _Reason}) ->
    bump(?CTR_SEND_REFUSED),
    ok.

%% (publisher, seq, topic) dedup — see macula_station_peer_observer's
%% notes (Phase 2 step 2/3). Decides per-EVENT whether to deliver or
%% drop as a loop-back. Tolerates the cache being momentarily down
%% (boot transient) and frames missing publisher/seq/topic.
%% Only an AUTHENTICATED publisher may touch the cache.
%%
%% Topic joined the key 2026-09: a bare (publisher, seq) key assumes
%% one collision-free counter per identity across EVERY topic that
%% identity ever publishes to. Found live: macula-go's Session.Call
%% auto-publishes RPC telemetry facts under the caller's own identity
%% from a process-wide counter starting at 1, independent of whatever
%% counter an application uses for its own business publishes — the
%% first business publish on a fresh process, seeded seq=1 (macula-go's
%% own documented/tested convention), collided with that fact's key and
%% was silently dropped here as a "duplicate", losing a real message,
%% not a loop. See macula_station_event_dedup:seen_or_record/3's own
%% doc. Topic is already part of what publisher_sig covers below, so
%% this adds no new trust requirement.
%%
%% This used to key the cache off the raw `publisher' / `seq' fields of
%% any verified frame. For an EVENT with no `publisher_sig',
%% `verify_pubsub/2' checks the frame signature against the SENDING
%% peer's node id and never compares `publisher' to it — so a peer could
%% name a victim's pubkey with a chosen `seq', sign the frame with its
%% own key, pass verification, and insert that pair. The victim's genuine
%% publisher-signed EVENT then arrived, found the pair present, and was
%% dropped as a duplicate. Sequence numbers are trivially predictable
%% (`macula_client' seeds `publish_seq' from `system_time(microsecond)'
%% and increments by one), so one observed event yields every future key
%% for that publisher: a per-publisher, per-topic mute button lasting the
%% full 300s dedup window, costing one frame.
%%
%% A verified `publisher_sig' covers `(topic, realm, publisher, seq,
%% payload)', so its presence on an already-verified frame means the
%% publisher field is attested by the publisher itself. Unsigned events
%% never reach the cache now — they were already always delivered, so
%% they gained nothing from it and only supplied the attack surface.
event_dedup_disposition(V) ->
    event_disposition(authentic_event_key(V), V).

authentic_event_key(#{publisher_sig := _, publisher := Pub, seq := Seq, topic := Topic})
  when is_binary(Pub), is_integer(Seq), is_binary(Topic) ->
    {Pub, Seq, Topic};
authentic_event_key(_V) ->
    unauthenticated.

event_disposition(unauthenticated, _V) ->
    bump(?CTR_UNAUTH_EVENT),
    deliver;
event_disposition({Pub, Seq, Topic}, V) ->
    classify_event_dup(macula_station_event_dedup:seen_or_record(Pub, Seq, Topic), V).

classify_event_dup(new, _V) ->
    deliver;
%% Reached only for events whose publisher is attested (see
%% `authentic_event_key/1'), so the clause that used to handle an
%% unsigned duplicate is gone rather than left as unreachable code.
classify_event_dup(duplicate, V) ->
    %% Counted, not raised to `info': receiver-side bloom-fan makes
    %% duplicates EXPECTED and numerous (O(mesh) per publish), so a
    %% per-drop log at production level would flood. The counter gives
    %% the rate; the debug line gives the detail when chasing one.
    %%
    %% NOTE this counter cannot by itself distinguish a healthy loop
    %% kill from a lost message: both look like a repeat. Telling them
    %% apart needs to know whether THIS station already forwarded the
    %% key, which is the open record-on-forward question.
    bump(?CTR_DUP_DROPPED),
    logger:debug("[event_dedup] dropped loop-back EVENT publisher=~s seq=~p topic=~s",
                 [short_hex(maps:get(publisher, V)), maps:get(seq, V),
                  maps:get(topic, V, <<>>)]),
    drop.

%% Seed the dedup cache at the origin station so a looped-back EVENT
%% for the same publish is recognised as a duplicate here too.
%%
%% A PUBLISH is verified against the CONNECTION's node id, and unlike an
%% EVENT its `publisher_sig' is NOT checked on this path, so the only
%% publisher this station can attest to is the connected daemon itself.
%% A `publisher' naming anyone else is an unverified claim about a third
%% party, and seeding on it is the same poisoning primitive as the EVENT
%% path had. Refusals are counted rather than logged: if the fleet turns
%% out to publish under a pubkey that differs from its connection
%% identity, `unauth_publisher' will show it as a rate instead of a
%% silent loss of origin seeding.
record_origin_seq(Verified, NodeId) ->
    record_origin(origin_dedup_key(Verified, NodeId)).

origin_dedup_key(#{publisher := Pub, seq := Seq, topic := Topic}, NodeId)
  when is_binary(Pub), is_integer(Seq), is_binary(Topic), Pub =:= NodeId ->
    {Pub, Seq, Topic};
origin_dedup_key(_Verified, _NodeId) ->
    unauthenticated.

record_origin(unauthenticated) ->
    bump(?CTR_UNAUTH_ORIGIN),
    ok;
record_origin({Pub, Seq, Topic}) ->
    _ = macula_station_event_dedup:seen_or_record(Pub, Seq, Topic),
    ok.

notify_router_change() ->
    case whereis(macula_station_peering_router) of
        undefined -> ok;
        Pid       ->
            %% Async send so the dispatcher doesn't block on router
            %% sync (which can take seconds when fanning to many
            %% peers). The router treats `tick' as a kick: it syncs
            %% promptly but leaves the periodic `timer_tick' to re-arm.
            Pid ! tick, ok
    end.

notify_bloom_change() ->
    case whereis(macula_station_bloom_exchange) of
        undefined -> ok;
        Pid       -> macula_station_bloom_exchange:notify_local_change(Pid)
    end.

short_hex(B) when is_binary(B), byte_size(B) > 0 ->
    binary:encode_hex(binary:part(B, 0, min(8, byte_size(B))));
short_hex(_) ->
    <<"?">>.
