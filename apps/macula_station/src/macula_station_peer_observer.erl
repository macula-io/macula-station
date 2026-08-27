%% @doc Peer observer — glue between peering, DHT, and SWIM.
%%
%% Each station has exactly one observer (when booted via the
%% supervisor). It is the `controlling_pid' for every
%% `macula_peering_conn' worker the listener accepts and for every
%% outbound connect initiated via `macula_station:connect_to/1'. It
%% turns peering events into the mutations the rest of the station
%% expects:
%%
%% <ul>
%%   <li>`{macula_peering, connected, ConnPid, PeerNodeId}' →
%%       `macula_dht:observe/2' with an entry spec carrying
%%       `tier = t0' and `macula_swim:add_peer/3' so the failure
%%       detector starts probing the new member.</li>
%%   <li>`{macula_peering, frame, ConnPid, Frame}' — the observer
%%       verifies the signature and dispatches SWIM frames
%%       (ping / ack / suspect / confirm) to
%%       `macula_swim:handle_frame/3'. Application-layer frames
%%       (HyParView / Plumtree / pubsub / realm admission) are
%%       <em>not</em> a station concern — the station is
%%       realm-agnostic infrastructure per the railroad mental
%%       model; realm identity + overlay live in a separate
%%       `hecate-realm' / `macula-realm' service that dials this
%%       station like any other peer. Unknown frame types are
%%       dropped silently and flow end-to-end between the peering
%%       workers of the realm service and its clients.</li>
%%   <li>`{macula_peering, disconnected, ConnPid, _Reason}' →
%%       `macula_swim:remove_peer/2'.</li>
%% </ul>
%%
%% State is two maps — `peers' for the ConnPid → NodeId reverse
%% lookup used by disconnect handling, and `conns' for the forward
%% NodeId → ConnPid lookup exposed via `conn_for/2' so callers
%% (e.g. the future realm service when co-resident with a station)
%% can send frames to a known peer without re-discovering it.
-module(macula_station_peer_observer).
-behaviour(gen_server).

-export([
    start_link/1, stop/1,
    peers/1,
    conns/1,
    conn_for/2,
    swim_verdicts/1,
    station_view/1,
    overlay_relay_stats/0
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{dht := pid(), swim := pid(),
                  identity => macula_identity:key_pair()}.

%% Direction of a peering connection from THIS station's perspective:
%%   inbound  — the listener accepted a CONNECT from the peer (server-role).
%%   outbound — `outbound_link' dialled the peer (client-role).
%% Mutual peers (both sides have an outbound_peers entry for the other)
%% materialise BOTH directions; lone-direction peers materialise one.
-type direction() :: inbound | outbound.

%% Per-NodeId conns view. Held as a map so we can carry both directions
%% concurrently without one overwriting the other. `inbound' is the conn
%% that received the peer's SUBSCRIBE / CALL frames (its `controlling_pid'
%% is this peer_observer). `outbound' is the conn we dial out on (its
%% `controlling_pid' is the relevant `macula_station_outbound_link').
%%
%% EVENT delivery to a peer who subscribed via the inbound side MUST go
%% out on `inbound'; the bytes then arrive at the peer's
%% `outbound_link', whose subscriber-fan-out delivers
%% `{macula_event, ...}' messages to local Erlang processes (e.g.
%% `bloom_exchange'). If we mistakenly send via `outbound', the bytes
%% land on the peer's listener-side conn, dispatch into the peer's
%% pubsub_server, and dead-end — the peer has no local subscribers
%% there.
-type peer_conns() :: #{
    inbound  => pid() | undefined,
    outbound => pid() | undefined
}.

-record(state, {
    dht                  :: pid(),
    swim                 :: pid(),
    %% Per-identity RPC procedure registry (Track 2 — multi-identity
    %% mesh discovery). Optional: when undefined, CALL frames are
    %% dropped silently. Identity_sup wires it in when the handler
    %% app is part of the per-identity child set.
    handler_registry     :: atom() | pid() | undefined,
    %% Per-identity pubsub registry (Sprint A: SUBSCRIBE / UNSUBSCRIBE
    %% / EVENT frames route through here, keyed by realm tag).
    %% Optional: when undefined, those frames are dropped silently.
    pubsub_registry      :: atom() | pid() | undefined,
    %% Per-identity remote-advertise registry. Tracks procedures a
    %% connected peer has registered via ADVERTISE frames. CALL frames
    %% not matched by the local handler_registry fall through to here;
    %% if a match is found the CALL is forwarded over the advertiser's
    %% QUIC connection.
    remote_advertise     :: atom() | pid() | undefined,
    %% This station's own identity — RESULT / call_error frames
    %% carry it as the `responded_by' / `reported_by' pubkey so the
    %% caller knows who answered.
    self_id              :: macula_identity:pubkey() | undefined,
    %% Full key pair, needed only for `macula_peering:send_on_stream/3'
    %% (dedicated-stream relay) — see the comment at `identity' in
    %% `macula_station_app:observer_child/3'. `undefined' in tests that
    %% don't exercise streaming relay.
    identity              :: macula_identity:key_pair() | undefined,
    peers                :: #{pid() => macula_identity:pubkey()},
    %% NodeId → {inbound, outbound} ConnPids. Both can be set for mutual
    %% peers; clears entry-by-entry on `disconnected'.
    conns                :: #{macula_identity:pubkey() => peer_conns()},
    %% Reverse map ConnPid → its direction, populated alongside `peers'.
    %% Lets `on_disconnected' / DOWN-handler clear ONLY the affected
    %% direction in `conns' instead of nuking the whole NodeId entry
    %% (which would lose the still-live other direction).
    direction_of_pid = #{} :: #{pid() => direction()},
    %% In-flight forwarded CALLs. Maps each forwarded call_id to the
    %% origin connection that issued the CALL plus a TTL timer ref
    %% so an entry whose advertiser never replies (advertiser
    %% disconnects mid-call, advertiser process wedges, etc.) is
    %% reaped instead of leaking. The origin's station_link enforces
    %% its own deadline at the SDK level — the relay-side timer is
    %% pure cleanup.
    forwarded = #{}      :: #{<<_:128>> => {pid(), reference()}},
    %% In-flight streaming RPCs. Maps each stream_id to the caller-
    %% origin connection AND dedicated QUIC stream, the advertiser
    %% connection AND dedicated QUIC stream it was forwarded to, plus
    %% a TTL timer ref so a hung stream (advertiser crashed mid-stream,
    %% malformed STREAM_END) is reaped instead of leaking forever.
    %% Cleared on STREAM_END / STREAM_ERROR / STREAM_REPLY (final
    %% frames), on the timer, or on either side's connection dying.
    streams   = #{}      :: #{<<_:128>> => {pid(), reference(), pid(),
                                            reference(), reference()}},
    %% Reverse index for O(1) relay: a dedicated QUIC stream reference
    %% (either the caller's or the advertiser's) to the paired
    %% {OtherConnPid, OtherStream, Sid}. Populated in lock-step with
    %% `streams' — one route entry per side of the pair.
    stream_route = #{}   :: #{reference() => {pid(), reference(), <<_:128>>}},
    %% Per-dedicated-stream inbound byte buffer, `{OwnerConnPid, Buf}'.
    %% The owner pid is carried alongside the buffer because
    %% `{quic, Bin, Stream, Flags}' messages don't carry a ConnPid —
    %% mirrors `macula_station_link:stream_bufs' on the SDK side,
    %% which doesn't need the owner because it only ever has ONE
    %% peering connection.
    stream_bufs = #{}    :: #{reference() => {pid(), binary()}},
    %% Per-ConnPid `last_frame_at' (monotonic ms). Updated on every
    %% inbound application frame. Drives the periodic conn-aging
    %% sweep that force-closes daemon-class connections whose
    %% counterpart has gone silent — peering_conn doesn't detect
    %% dead daemon BEAMs until QUIC's idle-timeout fires (minutes),
    %% which leaks conns_tab + DHT routing-table entries fast enough
    %% to trip the cascade documented in
    %% docs/CASCADE_INVESTIGATION.md.
    last_frame_at = #{}  :: #{pid() => integer()},
    %% NodeId → boolean asserting "this peer is a relay station".
    %% Populated on `connected' from the peer's
    %% `macula_peering:peer_capabilities/1' bitmask (SDK 4.5.0+).
    %% Drives the direct-vs-gossip distinction in
    %% `on_advertise_match/6': frames from a station-flagged peer
    %% are gossip relays (first-write-wins keeps existing direct
    %% entries); frames from non-station peers are direct daemon
    %% advertises (always replace, so a daemon's reconnect or
    %% mobility-across-stations cannot get shadowed by an older
    %% gossip echo). Pre-4.5.0 SDKs report 0, which evaluates as
    %% "daemon" — preserves legacy behaviour.
    is_station    = #{}  :: #{macula_identity:pubkey() => boolean()},
    %% SWIM verdict tally. macula_swim computes alive -> suspect ->
    %% confirmed_failed and pushes each transition to its controlling pid,
    %% which is this process. Until 2026-07-27 there was NO CLAUSE for that
    %% message and it fell into the catch-all handle_info -- an adaptive
    %% failure detector whose verdict was consumed by nobody, after someone
    %% had deliberately rewired the controlling pid here to stop the
    %% notifications being dropped.
    %%
    %% This tally OBSERVES, it does not act. Nothing here forgets a peer,
    %% drops a conn or sheds work. The open question is whether the verdicts
    %% are TRUE at degree ~3 on a 5-7 station mesh, and acting on a signal of
    %% unknown accuracy is how a failure detector becomes a failure amplifier.
    swim_verdicts = #{} :: #{atom() => non_neg_integer()}
}).

%% Capability bit asserting a peer is a relay station. Matches
%% macula_peering:?CAP_STATION (1 bsl 0). See macula_station_config.
-define(CAP_STATION, 16#0000_0000_0000_0001).

%% Capability resolution retry budget. `unknown' is not `daemon', so retry --
%% but bounded, so a peer on an SDK that genuinely lacks the getter settles
%% rather than probing forever.
-define(STATION_RESOLVE_MAX,          5).
-define(STATION_RESOLVE_BACKOFF_MS, 500).

%% Hard upper bound on how long a forwarded CALL entry may live on
%% the relay. Above any reasonable per-call deadline (the longest
%% caller-side timeout in the fleet today is 10 s on
%% `_realm.membership.join_with_token_v1'). Defensive — purges
%% stragglers even when a buggy peer never sends RESULT/ERROR.
-define(FORWARDED_TTL_MS, 60_000).

%% Streaming RPC relay TTL — analogous to FORWARDED_TTL_MS but for
%% streams. Streaming workloads can be longer-lived than unary calls
%% (e.g. a server_stream paginating a large result set), so this
%% bound is generous. A normal stream is closed by an explicit
%% STREAM_END / STREAM_REPLY frame, which removes the entry well
%% before the timer fires.
-define(STREAM_TTL_MS, 300_000).

%% Conn-aging sweep cadence + idle threshold. A station-station
%% peering_conn sees bloom + SWIM frames every few seconds;
%% daemon-class peering_conn typically sees subscribe / publish /
%% RPC frames at non-trivial intervals. 300_000 ms (5 min) is a
%% generous threshold — anything older than that on a non-undefined
%% slot in the conns map is presumed-dead and force-closed via
%% `macula_peering:close/2'. The close fires DOWN naturally, which
%% routes to `on_disconnected' for the normal cleanup path.
-define(CONN_SWEEP_INTERVAL_MS, 60_000).
-define(CONN_IDLE_THRESHOLD_MS, 300_000).

%% Named ETS table mirroring the `conns' map so external callers
%% (DHT transport, anyone routing wire frames by NodeId) read
%% without serializing against the observer's gen_server mailbox.
-define(CONNS_TABLE, macula_station_peer_observer_conns).

%% Phase 3.5's overlay_relay path had zero counters until the incident
%% this fixes: the relay silently failed on the real fleet while CI's
%% local test-cluster suite stayed green, and there was nothing to read
%% to tell "bad signature" from "target not connected" from outside a
%% live trace. `counters' + `persistent_term' is the idiom
%% `macula_station_event_dedup' and `macula_station_route_pubsub_frames'
%% already use — bumps no-op when the array is absent, so instrumenting
%% this never changes behaviour.
-define(PT_OVERLAY_COUNTERS, {?MODULE, overlay_relay_counters}).
-define(CTR_OVERLAY_RELAYED,               1).
-define(CTR_OVERLAY_VERIFY_FAILED,         2).
-define(CTR_OVERLAY_TARGET_NOT_CONNECTED,  3).
-define(CTR_OVERLAY_SLOTS,                 3).

%%==================================================================
%% API
%%==================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

-spec peers(pid()) -> [{pid(), macula_identity:pubkey()}].
peers(Pid) -> gen_server:call(Pid, peers).

%% Reads from the public ETS conns mirror — bypasses the gen_server
%% mailbox so wire-send paths (the DHT's `send_frame' callback fires
%% per outgoing STORE / FIND_VALUE / etc.) do NOT serialize against
%% the observer's frame-handling loop. Under burst load the
%% gen_server:call path queued tens of conn_for lookups behind
%% in-flight ADVERTISE / EVENT processing and timed out at 5s,
%% cascading into supervised-process restarts. The ETS read is
%% O(1) and concurrent.
%%
%% `Pid' is retained in the API for caller-side ergonomics (callers
%% already hold `whereis(macula_station_peer_observer)' or pass it
%% explicitly) but is not used — `?CONNS_TABLE' is named, single
%% per BEAM, so a stale pid does not matter.
%% @doc Who is actually in our connection set, split by capability.
%%
%% Exists because the DHT routing table and the SWIM member list are fed
%% UNCONDITIONALLY from `on_connected_directional/5' -- every peer that
%% connects is observed into the DHT and added to SWIM, including daemons,
%% which implement neither protocol. Measured on the live fleet 2026-07-27:
%% frankfurt held 43 conns of which 4 were stations, a 30-entry DHT table of
%% which 4 were stations, and 43 SWIM members of which 4 were stations.
%%
%% Returns the station node-id SET rather than a count, so a caller can
%% intersect it with the DHT table and the SWIM membership and report the
%% split per container. Deliberately does NOT call into macula_dht or
%% macula_swim: this runs in the observer's handle_call, and a cross-server
%% call from here is the shape that produced the cascade in
%% docs/CASCADE_INVESTIGATION.md.
-spec station_view(pid()) -> #{stations := non_neg_integer(),
                               daemons  := non_neg_integer(),
                               station_ids := [macula_identity:pubkey()]}.
station_view(Pid) ->
    gen_server:call(Pid, station_view).

%% @doc The full per-NodeId conns view: `NodeId => #{inbound, outbound}'.
%%
%% `peers/1' flattens to a NodeId list and `conn_for/2' answers for ONE
%% direction, so neither can distinguish a mutual peer from a single-direction
%% one. That distinction decides whether losing a conn is an isolation or an
%% asymmetric loss, which is the difference between removing a peer from SWIM
%% and re-pointing it.
-spec conns(pid()) -> #{macula_identity:pubkey() => map()}.
conns(Pid) ->
    gen_server:call(Pid, conns).

%% @doc SWIM verdict tally. `confirmed_live_conn' is the interesting one: it
%% counts verdicts of `confirmed_failed' for a peer we STILL hold a live
%% connection to, which is a contradiction and therefore a suspected false
%% positive. `confirmed_no_conn' is the corroborated case. The ratio is the
%% measurement that decides whether any failure-reactive mechanism can be
%% trusted here.
-spec swim_verdicts(pid()) -> #{atom() => non_neg_integer()}.
swim_verdicts(Pid) ->
    gen_server:call(Pid, swim_verdicts).

%% @doc Phase 3.5 overlay_relay outcome counts since counters were
%% installed. Deliberately NOT a `gen_server:call' — this file's own
%% cascade history (see `station_view/1''s comment above) is exactly why
%% a future Prometheus scrape must read this without touching the
%% observer's mailbox.
-spec overlay_relay_stats() -> #{relayed := non_neg_integer(),
                                 verify_failed := non_neg_integer(),
                                 target_not_connected := non_neg_integer()}.
overlay_relay_stats() ->
    overlay_stats_of(persistent_term:get(?PT_OVERLAY_COUNTERS, undefined)).

overlay_stats_of(undefined) ->
    #{relayed => 0, verify_failed => 0, target_not_connected => 0};
overlay_stats_of(Ref) ->
    #{relayed              => counters:get(Ref, ?CTR_OVERLAY_RELAYED),
      verify_failed        => counters:get(Ref, ?CTR_OVERLAY_VERIFY_FAILED),
      target_not_connected => counters:get(Ref, ?CTR_OVERLAY_TARGET_NOT_CONNECTED)}.

install_overlay_counters() ->
    install_overlay_counters(persistent_term:get(?PT_OVERLAY_COUNTERS, undefined)).

install_overlay_counters(undefined) ->
    persistent_term:put(?PT_OVERLAY_COUNTERS,
                        counters:new(?CTR_OVERLAY_SLOTS, [write_concurrency]));
install_overlay_counters(_Ref) ->
    ok.

bump_overlay(Slot) ->
    bump_overlay_ref(persistent_term:get(?PT_OVERLAY_COUNTERS, undefined), Slot).

bump_overlay_ref(undefined, _Slot) -> ok;
bump_overlay_ref(Ref, Slot)        -> counters:add(Ref, Slot, 1).

-spec conn_for(pid(), macula_identity:pubkey()) ->
    {ok, pid()} | error.
conn_for(Pid, <<_:256>> = NodeId) ->
    on_ets_lookup(ets_find_conns(NodeId), Pid, NodeId).

on_ets_lookup(table_missing, Pid, NodeId) ->
    %% Stub observers in tests don't own the table; defer to their
    %% gen_server:call handler. Production peer_observer always
    %% creates the table in init/1, so this clause is unreachable
    %% in production paths.
    gen_server:call(Pid, {conn_for, NodeId});
on_ets_lookup(Result, _Pid, _NodeId) ->
    primary_conn_lookup(Result).

%%==================================================================
%% gen_server
%%==================================================================

init(#{dht := Dht, swim := Swim} = Opts)
  when is_pid(Dht), is_pid(Swim) ->
    %% Say once, loudly, whether the capability getter exists. `is_station' is
    %% resolved through `macula_peering:peer_capabilities/1'. A missing getter
    %% is reported as `unknown' by `resolve_capability/2' and retried, not
    %% silently read as daemon -- but if it is missing FLEET-WIDE every peer
    %% exhausts its retries and settles as daemon, and SWIM would end up
    %% empty. That only misroutes an ADVERTISE today, but it is the
    %% single point on which any future capability-gating of the DHT or SWIM
    %% would rest, and a silent all-daemon verdict is exactly the failure that
    %% must never be quiet.
    log_capability_getter(
      erlang:function_exported(macula_peering, peer_capabilities, 1)),
    %% `protected' = anyone reads, only owner writes. `set' = unique
    %% NodeId key, last write wins (matches the `conns' map's
    %% per-NodeId semantics). `read_concurrency' biases the BEAM
    %% scheduler for the read-heavy access pattern (every wire frame
    %% routes through one read).
    %%
    %% Idempotent: on supervisor restart the prior peer_observer
    %% process is already gone, taking the table with it; if for
    %% some reason the table still exists, recreate cleanly.
    catch ets:delete(?CONNS_TABLE),
    ?CONNS_TABLE = ets:new(?CONNS_TABLE,
                           [named_table, protected, set,
                            {read_concurrency, true}]),
    %% Telemetry histograms — see macula_station_frame_telemetry. Init
    %% here so dispatch sites (in this module, the DHT server, and the
    %% pubsub_dispatcher) can write without checking whether the table
    %% exists. Idempotent.
    ok = macula_station_frame_telemetry:init(),
    ok = install_overlay_counters(),
    State0 = #state{dht              = Dht,
                    swim             = Swim,
                    handler_registry = maps:get(handler_registry, Opts, undefined),
                    pubsub_registry  = maps:get(pubsub_registry, Opts, undefined),
                    remote_advertise = maps:get(remote_advertise, Opts, undefined),
                    self_id          = maps:get(self_id, Opts, undefined),
                    identity         = maps:get(identity, Opts, undefined),
                    peers            = #{},
                    conns            = #{}},
    erlang:send_after(?CONN_SWEEP_INTERVAL_MS, self(), conn_sweep),
    %% Self-forming: derive the initial peers/conns view from the
    %% peer_links registry (canonical source of truth for outbound
    %% dials) instead of relying on having received every `connected'
    %% event up to this point. Boot order, observer restart, and
    %% missed notifications all become non-issues because the observer
    %% reconciles whatever is true at init time and uses
    %% `erlang:monitor/2' for ongoing death detection.
    %%
    %% ⚠⚠ AND THE SAME FOR INBOUND, WHICH IS THE HALF THAT WAS MISSING AND
    %% COST A STATION A DAY. The paragraph above claimed observer restart was
    %% a non-issue. It was a non-issue for the peers this station DIALS,
    %% because peer_links remembers them. Nothing remembered the peers that
    %% dialled US, so on 2026-08-05 station-de-frankfurt's observer crashed,
    %% restarted with an empty conns mirror, re-registered only its three
    %% outbound station links, and every client connection — all of which
    %% were still perfectly alive — stayed invisible to pubsub fan-out for
    %% the rest of the node's life. Publishes were accepted and delivered to
    %% nobody, with no error anywhere and `/health' reporting healthy.
    %%
    %% A crash must cost a blip, not a day. The listener is the canonical
    %% record of inbound accepts, so it is now asked, exactly as peer_links
    %% is asked for outbound.
    {ok, reconcile_inbound_accepts(reconcile_outbound_dials(State0))}.

%% Walk the listener's current inbound peer index and fold each live worker
%% in through the standard on_connected path. Tolerant throughout: at first
%% boot the listener may not exist yet (nothing to reconcile, which is
%% correct), and a worker that died between the call and the fold is simply
%% skipped — its DOWN would have been handled anyway.
reconcile_inbound_accepts(S) ->
    lists:foldl(fun({NodeId, Pid}, Acc) -> absorb_known_accept(NodeId, Pid, Acc) end,
                S, listener_peers()).

listener_peers() ->
    try macula_station:listener() of
        {ok, Pid} -> macula_station_listener:peers(Pid);
        _NotStarted -> []
    catch
        _:_ -> []
    end.

absorb_known_accept(NodeId, Pid, S) when is_pid(Pid) ->
    absorb_live_accept(is_process_alive(Pid), NodeId, Pid, S);
absorb_known_accept(_NodeId, _Other, S) ->
    S.

absorb_live_accept(false, _NodeId, _Pid, S) ->
    S;
absorb_live_accept(true, NodeId, Pid, S) ->
    logger:info("[peer_observer] reconciled inbound accept pid=~p peer=~p",
                [Pid, NodeId]),
    %% `[]' endpoints, matching the live inbound path: a peer that dialled us
    %% told us nothing about where to reach it.
    on_connected_directional(inbound, Pid, NodeId, [], S).

%% Walk peer_links' current verified entries; for each link with a
%% known peer node-id, query the link for its underlying conn_pid and
%% fold it into our maps via the standard on_connected path. Failures
%% (race with a link about to die, link not responding) are tolerated
%% — anything we miss here will be picked up by a later `connected'
%% notification from outbound_link's forward_to_observer.
reconcile_outbound_dials(S) ->
    Verified = try macula_station_peer_links:verified_peers()
               catch _:_ -> []
               end,
    lists:foldl(fun(#{pid := LinkPid, node_id := NodeId} = Link, Acc) ->
        %% peer_links already holds the dialled host/port for this link, so
        %% the reconciled entry gets a real address like the live-event path.
        absorb_known_dial(LinkPid, NodeId, link_endpoints(Link), Acc)
    end, S, Verified).

%% Same shape `direct_peer_spec/2' expects. Missing host/port yields `[]',
%% which observes an address-less entry rather than a wrong one.
link_endpoints(#{host := H, port := P}) ->
    [#{host => H, port => P, transport => quic}];
link_endpoints(_) ->
    [].

absorb_known_dial(LinkPid, NodeId, Endpoints, S) ->
    try macula_station_outbound_link:conn_pid(LinkPid) of
        undefined -> S;
        ConnPid when is_pid(ConnPid) ->
            logger:info("[peer_observer] reconciled outbound dial "
                        "pid=~p peer=~p", [ConnPid, NodeId]),
            on_connected_directional(outbound, ConnPid, NodeId, Endpoints, S)
    catch
        _:_ -> S
    end.

handle_call(station_view, _From, #state{is_station = IsSt} = S) ->
    Ids = [N || {N, true} <- maps:to_list(IsSt)],
    {reply, #{stations    => length(Ids),
              daemons     => maps:size(IsSt) - length(Ids),
              station_ids => Ids}, S};
handle_call(conns, _From, #state{conns = C} = S) ->
    {reply, C, S};
handle_call(swim_verdicts, _From, #state{swim_verdicts = V} = S) ->
    {reply, V, S};
handle_call(peers, _From, #state{peers = P} = S) ->
    {reply, maps:to_list(P), S};
handle_call({conn_for, NodeId}, _From, #state{conns = C} = S) ->
    %% Kept for direct gen_server callers (tests, debug) — the
    %% public `conn_for/2' export now uses the ETS mirror and never
    %% reaches this clause in production.
    {reply, primary_conn_lookup(maps:find(NodeId, C)), S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({macula_peering, connected, ConnPid, PeerNodeId}, S) ->
    %% Inbound (listener-accepted) handshake — peer dialled us.
    logger:info("[peer_observer] connected pid=~p peer=~p direction=inbound",
                [ConnPid, PeerNodeId]),
    {noreply, on_connected_directional(inbound, ConnPid, PeerNodeId, [], S)};
handle_info({macula_peering, connected_outbound, ConnPid, PeerNodeId,
             Endpoints}, S) when is_list(Endpoints) ->
    %% 5-tuple carries the endpoint WE DIALLED, so the routing-table entry for
    %% an outbound peer is reachable rather than address-less. Only
    %% `macula_station_outbound_link' emits this.
    logger:info("[peer_observer] connected pid=~p peer=~p direction=outbound",
                [ConnPid, PeerNodeId]),
    {noreply, on_connected_directional(outbound, ConnPid, PeerNodeId,
                                       Endpoints, S)};
handle_info({macula_peering, connected_outbound, ConnPid, PeerNodeId}, S) ->
    %% Outbound (client-side) handshake — `outbound_link' relays
    %% peering events here with the `_outbound' suffix so direction
    %% is unambiguous. Without it the conn map would race between
    %% inbound + outbound for mutual peers and EVENT delivery would
    %% land on the wrong side of the link.
    logger:info("[peer_observer] connected pid=~p peer=~p direction=outbound",
                [ConnPid, PeerNodeId]),
    {noreply, on_connected_directional(outbound, ConnPid, PeerNodeId, [], S)};
handle_info({macula_peering, frame, ConnPid, Frame}, S) ->
    %% Legacy 4-tuple from peering_conn without `timing_enabled' — no
    %% mailbox-wait timestamp available. Still record dispatch latency.
    Type = macula_frame:frame_type(Frame),
    T0 = erlang:monotonic_time(microsecond),
    NewS = on_frame(ConnPid, Frame, touch_conn(ConnPid, S)),
    T1 = erlang:monotonic_time(microsecond),
    macula_station_frame_telemetry:record(Type, dispatch_self, T1 - T0),
    {noreply, NewS};
handle_info({macula_peering, frame, ConnPid, Frame, RecvAtUs}, S) ->
    %% 5-tuple from a peering_conn with `timing_enabled = true'. Compute
    %% mailbox wait first, then time the dispatch body separately.
    Type = macula_frame:frame_type(Frame),
    T0 = erlang:monotonic_time(microsecond),
    macula_station_frame_telemetry:record(Type, recv_to_dispatch,
                                          T0 - RecvAtUs),
    NewS = on_frame(ConnPid, Frame, touch_conn(ConnPid, S)),
    T1 = erlang:monotonic_time(microsecond),
    macula_station_frame_telemetry:record(Type, dispatch_self, T1 - T0),
    {noreply, NewS};
handle_info({macula_peering, new_dedicated_stream, ConnPid, Stream},
            #state{stream_bufs = Bufs} = S) ->
    %% A peer opened a fresh dedicated QUIC stream on ConnPid — the
    %% first bytes we see on it decide what it's for (see
    %% `dispatch_dedicated_frame/3'). Seed an empty buffer now so the
    %% first `{quic, Bin, Stream, _}' has somewhere to land.
    {noreply, S#state{stream_bufs = Bufs#{Stream => {ConnPid, <<>>}}}};
handle_info({quic, Bin, Stream, _Flags}, #state{stream_bufs = Bufs} = S)
        when is_binary(Bin), is_map_key(Stream, Bufs) ->
    {OwnerConn, Buf} = maps:get(Stream, Bufs),
    {Frames, Tail} = macula_frame:parse_stream(<<Buf/binary, Bin/binary>>),
    NewS = lists:foldl(
             fun(Frame, Acc) ->
                 dispatch_dedicated_frame(Frame, OwnerConn, Stream, Acc)
             end,
             S#state{stream_bufs = Bufs#{Stream => {OwnerConn, Tail}}},
             Frames),
    {noreply, NewS};
handle_info({macula_peering, disconnected, ConnPid, Reason}, S) ->
    logger:info("[peer_observer] disconnected pid=~p peer=~p reason=~p "
                "direction=inbound",
                [ConnPid, maps:get(ConnPid, S#state.peers, undefined), Reason]),
    {noreply, on_disconnected(ConnPid, S)};
handle_info({macula_peering, disconnected_outbound, ConnPid, Reason}, S) ->
    logger:info("[peer_observer] disconnected pid=~p peer=~p reason=~p "
                "direction=outbound",
                [ConnPid, maps:get(ConnPid, S#state.peers, undefined), Reason]),
    {noreply, on_disconnected(ConnPid, S)};
handle_info({'DOWN', _Ref, process, Pid, _Reason},
            #state{peers = P} = S) ->
    %% A conn worker we were monitoring died for any reason — explicit
    %% close, peer disconnect, upstream outbound_link crash, network
    %% drop. on_disconnected is idempotent on unknown pids.
    case maps:is_key(Pid, P) of
        true  -> {noreply, on_disconnected(Pid, S)};
        false -> {noreply, S}
    end;
handle_info({forwarded_timeout, CallId}, #state{forwarded = F} = S) ->
    {noreply, on_forwarded_timeout(maps:take(CallId, F), S)};
handle_info({stream_timeout, Sid}, #state{streams = St} = S) ->
    {noreply, on_stream_timeout(maps:take(Sid, St), S)};
handle_info(conn_sweep, S) ->
    erlang:send_after(?CONN_SWEEP_INTERVAL_MS, self(), conn_sweep),
    {noreply, run_conn_sweep(S)};
handle_info({is_station_resolved, NodeId, Attempt, unknown}, S) ->
    {noreply, retry_resolve(maps:is_key(NodeId, S#state.conns),
                            NodeId, Attempt, S)};
handle_info({is_station_resolved, NodeId, _Attempt, {ok, Flag}}, S) ->
    %% Apply the deferred answer iff the NodeId is still connected. A
    %% race-disconnect would already have removed the entry via
    %% `drop_is_station_if_isolated/3'; preserve that by gating on conns.
    {noreply, apply_station_flag(maps:is_key(NodeId, S#state.conns),
                                 NodeId, Flag, S)};
handle_info({retry_resolve_is_station, NodeId, Attempt}, S) ->
    {noreply, respawn_resolve(primary_conn_lookup(
                                maps:find(NodeId, S#state.conns)),
                              NodeId, Attempt, S)};
%% THE CLAUSE THAT DID NOT EXIST. Without it this message hit the catch-all
%% below and the detector's entire output was discarded.
handle_info({macula_swim, member_state, NodeId, State}, S) ->
    {noreply, tally_swim_verdict(NodeId, State, S)};
handle_info(_Msg, S) ->
    {noreply, S}.

%% Update last-frame timestamp for ConnPid. Called on every inbound
%% application frame. Cheap — single map insert. Entries for
%% unknown ConnPids are tolerated (frames may arrive between
%% `connected' and the on_connected_directional handler running).
touch_conn(ConnPid, #state{last_frame_at = LF} = S) ->
    S#state{last_frame_at = LF#{ConnPid => now_ms()}}.

%% Conn-aging sweep. Prunes `last_frame_at' entries whose ConnPid is
%% no longer in the `peers' map — defensive, in case a DOWN message
%% was lost.
%%
%% Previously this also force-closed any peering_conn that had been
%% silent at the application layer for >?CONN_IDLE_THRESHOLD_MS. That
%% was wrong: macula_quic runs a 15s keep_alive PING at the QUIC
%% layer, so a live but app-idle peer has its QUIC kept alive by
%% transport pings without sending any Macula frame the observer can
%% see. Stations chatter on `_mesh.bloom' + SWIM and never trip the
%% old threshold, but SDK pools that only call on demand (the e2e
%% harness, ad-hoc CLI tools, anything not on a periodic publish
%% cadence) do — and the sweep closed perfectly healthy connections
%% from under them, which surfaced as `_dht.put_record' timeouts the
%% moment a probe came in 5+ minutes after the last frame. Real
%% failures (counterpart BEAM crashed, network partition) take down
%% the QUIC connection within idle_timeout=300s, which fires DOWN
%% through the peering_conn monitor and routes through
%% `on_disconnected' for normal cleanup. We don't need a second
%% app-layer liveness check on top of that.
%%
%% Also sweeps `remote_advertise' for entries whose `conn_pid' is
%% already dead — the same "in case a DOWN message was lost" case,
%% just never extended past `last_frame_at'. Confirmed live: a
%% station's registry held an advertise entry whose `conn_pid'
%% `is_process_alive' returned `false' for, unreachable until
%% something happened to overwrite it — no natural process re-derives
%% or re-checks an existing entry's liveness otherwise. Bounds any
%% such staleness to one sweep interval regardless of how the DOWN
%% was actually lost, rather than depending on correctly diagnosing
%% that mechanism.
run_conn_sweep(#state{last_frame_at = LF, peers = P, remote_advertise = R} = S) ->
    Live = maps:filter(fun(Pid, _Last) -> maps:is_key(Pid, P) end, LF),
    sweep_dead_advertisers(R),
    S#state{last_frame_at = Live}.

sweep_dead_advertisers(undefined) ->
    ok;
sweep_dead_advertisers(R) ->
    DeadConns = dead_advertiser_conns(macula_remote_advertise_registry:list(R)),
    lists:foreach(fun(Pid) ->
        logger:warning("[peer_observer] conn_sweep: purging dead "
                       "advertiser conn_pid=~p", [Pid]),
        macula_remote_advertise_registry:purge_conn(R, Pid)
    end, DeadConns).

dead_advertiser_conns(Entries) ->
    lists:usort([Pid || {_Realm, _Proc, #{conn_pid := Pid}} <- Entries,
                        not is_process_alive(Pid)]).

%% TTL fired with no reply ever arriving. The origin's station_link
%% has already given up at the SDK level (its own deadline timer
%% surfaced `{error, timeout}' to the caller), so we simply clear
%% our entry. Logged at info so a flood of timeouts surfaces in
%% operations dashboards before it becomes a memory leak.
on_forwarded_timeout(error, S) ->
    S;
on_forwarded_timeout({{_Origin, _TRef}, NewF}, S) ->
    logger:info("[peer_observer] forwarded CALL timed out — purging",
                []),
    S#state{forwarded = NewF}.

on_stream_timeout(error, S) ->
    S;
on_stream_timeout({{_CallerConn, CallerStream, _AdvConn, AdvStream, _TRef}, NewSt},
                  S) ->
    logger:info("[peer_observer] stream relay timed out — purging", []),
    close_stream_pair(CallerStream, AdvStream, S#state{streams = NewSt}).

terminate(_Reason, _S) -> ok.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%==================================================================
%% connected
%%==================================================================

on_connected_directional(Direction, ConnPid, NodeId, Endpoints, S0) ->
    %% Fire-and-forget admit. `macula_dht:observe/2' is a sync
    %% gen_server:call; under accumulated daemon-conn load the DHT
    %% server gets slow enough that the call times out (5s default)
    %% and crashes peer_observer, taking the named conns ETS table
    %% with it and triggering a fleet-wide cascade as the supervisor
    %% restarts. The admit-result is unused here, so the cast variant
    %% is strictly safer. SWIM is already cast (see `add_peer/3').
    %% Empirical evidence: cascade root-cause investigation 2026-05-10
    %% caught peer_observer dying with mailbox=504, exit=
    %% gen_server:call timeout to macula_dht:observe.
    %% OUTBOUND is observed immediately and needs no capability probe: this
    %% station only ever dials peers from its own `outbound_peers' config, so
    %% an outbound conn IS a station by construction. It is also the only
    %% direction that carries a dialable address, so deferring it would either
    %% lose the endpoint or require carrying it to resolution.
    %%
    %% INBOUND is deferred to the capability probe. Observing it here put every
    %% daemon into the routing table: measured on the fleet, frankfurt held a
    %% 30-entry table of which 4 were stations, and 26 daemons with live conns
    %% all answered a DHT ping with {error, timeout} because they do not speak
    %% DHT at all.
    %%
    %% Direction is a fast path, NOT the discriminator. Using direction ALONE
    %% is what reverted commit c08ef8d did, and it collapsed tables to the
    %% outbound peer count because inbound stations were excluded; those still
    %% arrive, via resolution.
    observe_if_outbound(Direction, NodeId, Endpoints, S0),
    %% SWIM membership is NOT added here. It is added when the capability
    %% probe resolves the peer as a station -- see handle_info for
    %% {is_station_resolved, ...}. Adding at connect time added every daemon
    %% to SWIM, and daemons cannot answer a SWIM probe, so every one of them
    %% timed out into suspect and then confirmed_failed. Measured before this
    %% change: 142 of 142 confirmed_failed verdicts on the fleet were
    %% contradicted by a live conn; the two leaves, whose only peer is a real
    %% station, produced zero.
    %% Monitor ConnPid so observer cleans up on death without
    %% depending on a `disconnected' event flowing through some
    %% controlling_pid we may or may not own. Idempotent re-monitoring
    %% is fine — extra DOWN messages hit `on_disconnected' which is
    %% map-remove based.
    _ = erlang:monitor(process, ConnPid),
    %% Same-NodeId reconnect: an OldConnPid is occupying our slot AND
    %% it differs from the incoming one. The old QUIC conn died (or
    %% never surfaced its `disconnected' notify), so cleanup never
    %% ran for it. Synchronously evict the stale pid's state — remote
    %% advertise registry entries, peers map mapping, direction map,
    %% last_frame_at — so the new connection isn't shadowed by stale
    %% routes pointing at a dead Pid. Without this the registry sends
    %% inbound CALLs to the dead OldConnPid (silent drop, harness
    %% timeout) until OldConnPid eventually dies and triggers
    %% `on_disconnected'. Closes the "needs stub restart" regression.
    S1 = purge_stale_slot(NodeId, Direction, ConnPid, S0),
    Existing = maps:get(NodeId, S1#state.conns, empty_peer_conns()),
    Updated  = Existing#{Direction => ConnPid},
    write_conn_table(NodeId, Updated),
    %% Async-populate the is_station flag: a synchronous
    %% `macula_peering:peer_capabilities/1' call here would block the
    %% observer's gen_server for up to its own timeout (~1s) per
    %% connect, serialising the entire dispatch path behind it. Spawn
    %% a transient worker that does the call + casts the answer back
    %% via `{is_station_resolved, NodeId, bool}'. Default the entry to
    %% `false' (daemon) so any ADVERTISE arriving before the resolver
    %% replies is treated as direct — which is the legacy behaviour
    %% and the right answer for the daemon case (and for the gossip
    %% case the resolver flips it shortly after, before steady-state
    %% propagation).
    spawn_resolve_is_station(ConnPid, NodeId),
    S1#state{peers            = (S1#state.peers)#{ConnPid => NodeId},
             conns            = (S1#state.conns)#{NodeId => Updated},
             direction_of_pid = (S1#state.direction_of_pid)#{ConnPid => Direction},
             last_frame_at    = (S1#state.last_frame_at)#{ConnPid => now_ms()},
             is_station       = maps:put(NodeId,
                                         maps:get(NodeId, S1#state.is_station, false),
                                         S1#state.is_station)}.

apply_station_flag(false, _NodeId, _Flag, S) ->
    S;
apply_station_flag(true, NodeId, false, #state{is_station = IsSt} = S) ->
    S#state{is_station = IsSt#{NodeId => false}};
apply_station_flag(true, NodeId, true, #state{is_station = IsSt} = S) ->
    %% An inbound station enters the routing table here. Endpoints are `[]'
    %% because we do not know an inbound peer's LISTENING address. If it is
    %% already present from the outbound fast path, `insert' answers `touched'
    %% and keeps the existing entry, so the real address is not clobbered.
    macula_dht:observe_async(S#state.dht, direct_peer_spec(NodeId, [])),
    %% Read the CURRENT conn pid rather than the one the resolver closed over:
    %% `purge_stale_slot/4' may have evicted that pid while the probe was in
    %% flight, and SWIM stores the pid it is handed and probes with it.
    add_station_to_swim(primary_conn_lookup(maps:find(NodeId, S#state.conns)),
                        NodeId, S),
    S#state{is_station = IsSt#{NodeId => true}}.

observe_if_outbound(outbound, NodeId, Endpoints, #state{dht = Dht}) ->
    macula_dht:observe_async(Dht, direct_peer_spec(NodeId, Endpoints));
observe_if_outbound(_Inbound, _NodeId, _Endpoints, _S) ->
    ok.

add_station_to_swim({ok, ConnPid}, NodeId, #state{swim = Swim}) ->
    ok = macula_swim:add_peer(Swim, NodeId, ConnPid);
add_station_to_swim(error, _NodeId, _S) ->
    ok.

%% Bounded retry. `unknown' means we could not ask, not that the answer is no,
%% so retry with backoff while the peer is still connected. Capped so a peer
%% whose SDK genuinely lacks the getter cannot retry forever.
retry_resolve(false, _NodeId, _Attempt, S) ->
    S;
retry_resolve(true, NodeId, Attempt, S) when Attempt < ?STATION_RESOLVE_MAX ->
    _ = erlang:send_after(?STATION_RESOLVE_BACKOFF_MS * (Attempt + 1), self(),
                          {retry_resolve_is_station, NodeId, Attempt + 1}),
    S;
retry_resolve(true, NodeId, _Attempt, S) ->
    logger:warning("[peer_observer] gave up resolving station capability for "
                   "~s -- treating as daemon, so it will NOT join SWIM",
                   [binary:encode_hex(binary:part(NodeId, 0, 4))]),
    S.

respawn_resolve({ok, ConnPid}, NodeId, Attempt, S) ->
    spawn_resolve_is_station(ConnPid, NodeId, Attempt),
    S;
respawn_resolve(error, _NodeId, _Attempt, S) ->
    S.

log_capability_getter(true) ->
    logger:info("[peer_observer] macula_peering:peer_capabilities/1 exported "
                "-- station capability detection is LIVE"),
    ok;
log_capability_getter(false) ->
    logger:error("[peer_observer] macula_peering:peer_capabilities/1 NOT "
                 "exported -- EVERY peer will be classified as a daemon. "
                 "Any capability-gated behaviour is now wrong fleet-wide."),
    ok.

spawn_resolve_is_station(ConnPid, NodeId) ->
    spawn_resolve_is_station(ConnPid, NodeId, 0).

%% TRI-STATE, deliberately. The previous resolver answered a plain boolean and
%% `peer_caps_station(_) -> false' swallowed every error, so a transient
%% `{error, not_connected}' during a handshake race became a PERMANENT daemon
%% verdict -- the resolver is one-shot and nothing ever re-probed. That was
%% survivable while the flag only steered ADVERTISE routing. Now that it gates
%% SWIM membership it would silently exclude a real station for the whole life
%% of the connection, so `unknown' is distinguished from `daemon' and retried.
spawn_resolve_is_station(ConnPid, NodeId, Attempt) ->
    Self = self(),
    spawn(fun() ->
        Self ! {is_station_resolved, NodeId, Attempt,
                resolve_capability(ConnPid)}
    end),
    ok.

-spec resolve_capability(pid()) -> {ok, boolean()} | unknown.
resolve_capability(ConnPid) ->
    resolve_capability(
      erlang:function_exported(macula_peering, peer_capabilities, 1), ConnPid).

%% A missing getter says nothing about the PEER, so it is unknown rather than
%% daemon. The boot-time log already shouts if it is missing fleet-wide.
resolve_capability(false, _ConnPid) ->
    unknown;
resolve_capability(true, ConnPid) ->
    classify_caps(catch macula_peering:peer_capabilities(ConnPid)).

classify_caps({ok, Caps}) when is_integer(Caps) ->
    {ok, (Caps band ?CAP_STATION) =/= 0};
classify_caps(_Other) ->
    unknown.

%% Evict any stale ConnPid sitting in the (NodeId, Direction) slot so
%% the incoming ConnPid starts from a clean lane. Idempotent for the
%% common no-stale case (slot empty or already pointing at NewPid).
purge_stale_slot(NodeId, Direction, NewPid,
                 #state{peers = P, conns = C, direction_of_pid = D,
                        last_frame_at = LF, remote_advertise = R,
                        forwarded = F} = S) ->
    Existing = maps:get(NodeId, C, empty_peer_conns()),
    case maps:get(Direction, Existing, undefined) of
        undefined ->
            S;
        NewPid ->
            %% Idempotent re-fire of `connected' for the same pid;
            %% nothing stale to evict.
            S;
        OldPid when is_pid(OldPid) ->
            maybe_purge_advertise(R, OldPid),
            S#state{peers            = maps:remove(OldPid, P),
                    direction_of_pid = maps:remove(OldPid, D),
                    last_frame_at    = maps:remove(OldPid, LF),
                    forwarded        = drop_forwarded_for(OldPid, F)}
    end.

now_ms() -> erlang:monotonic_time(millisecond).

%% ETS mirror writers — must run from the gen_server process (table
%% is `protected'). Both write the same value the `conns' map sees,
%% so external readers via `conn_for/2' get consistent state.
write_conn_table(NodeId, PeerConns) ->
    ets:insert(?CONNS_TABLE, {NodeId, PeerConns}).

delete_conn_table(NodeId) ->
    ets:delete(?CONNS_TABLE, NodeId).

%% Tolerant of missing table: tests that swap in a stub observer
%% via `macula_station_peer_observer' API expect the gen_server:call
%% fallback path. Fall through to that when the named ETS table
%% isn't present in this BEAM.
ets_find_conns(NodeId) ->
    try ets:lookup(?CONNS_TABLE, NodeId) of
        [{_, PeerConns}] -> {ok, PeerConns};
        []               -> error
    catch
        error:badarg -> table_missing
    end.

empty_peer_conns() ->
    #{inbound => undefined, outbound => undefined}.

%% Pick the conn we want to use for fire-and-forget outbound
%% writes that don't require a specific direction (replies, forwarded
%% CALL trampolines, the public `conn_for/2' API). Inbound is
%% preferred because EVENT-delivery uses the same lookup and
%% inbound is the right answer there; non-EVENT callers don't care
%% which direction they get.
primary_conn_lookup(error) ->
    error;
primary_conn_lookup({ok, #{inbound := Pid}}) when is_pid(Pid) ->
    {ok, Pid};
primary_conn_lookup({ok, #{outbound := Pid}}) when is_pid(Pid) ->
    {ok, Pid};
primary_conn_lookup({ok, _Empty}) ->
    error.

%% `endpoints' is the address we DIALLED for an outbound peer, and `[]' for an
%% inbound one — we do not know an inbound peer's LISTENING address, and its
%% source port is ephemeral, so publishing that would be worse than publishing
%% nothing: it would turn an honest `no_route' into a connection attempt
%% against an address that cannot accept one.
%%
%% This used to be `[]' unconditionally, which made every routing-table entry
%% on the fleet undialable and confined every DHT store to peers we already
%% had a connection to. `asn' and `country' are still placeholders; they feed
%% `macula_dht_diversity' and are a separate problem.
%% Corroborate the verdict against the ground truth this process already
%% owns. A `confirmed_failed' for a NodeId we still hold a live conn to is
%% self-contradictory and counts as a suspected false positive; one with no
%% conn is corroborated. Everything is counted, nothing is acted on.
-spec tally_swim_verdict(macula_identity:pubkey(), atom(), #state{}) ->
          #state{}.
tally_swim_verdict(NodeId, confirmed_failed, #state{conns = C} = S) ->
    Key = verdict_key(maps:find(NodeId, C)),
    log_confirmed(Key, NodeId),
    bump_verdict(Key, bump_verdict(confirmed_failed, S));
tally_swim_verdict(_NodeId, State, S) ->
    bump_verdict(State, S).

%% A conn entry with BOTH directions undefined is a husk, not reachability.
verdict_key(error) ->
    confirmed_no_conn;
verdict_key({ok, #{inbound := undefined, outbound := undefined}}) ->
    confirmed_no_conn;
verdict_key({ok, _Live}) ->
    confirmed_live_conn.

log_confirmed(confirmed_live_conn, NodeId) ->
    logger:warning("[peer_observer] SWIM says confirmed_failed for ~s but we "
                   "still hold a live conn -- suspected false positive",
                   [binary:encode_hex(binary:part(NodeId, 0, 4))]);
log_confirmed(_Corroborated, _NodeId) ->
    ok.

bump_verdict(Key, #state{swim_verdicts = V} = S) ->
    S#state{swim_verdicts = maps:update_with(Key, fun(N) -> N + 1 end, 1, V)}.

direct_peer_spec(NodeId, Endpoints) when is_list(Endpoints) ->
    #{node_id   => NodeId,
      endpoints => Endpoints,
      asn       => 0,
      country   => <<"??">>,
      tier      => t0}.

%%==================================================================
%% frame — station cares about SWIM only; everything else is
%% realm-/application-level and flows end-to-end between the
%% peering workers of the relevant service.
%%==================================================================

on_frame(ConnPid, Frame, #state{peers = P} = S) ->
    %% TEMP DIAGNOSTIC (overlay_relay WAN-only vanishing-frame incident)
    %% — remove once root-caused. Both the `route/4' undefined-NodeId
    %% branch and `dispatch_overlay/5''s catch-all are silent; this logs
    %% every frame's actual wire frame_type + resolved category right
    %% at the point of entry, before ANY branching, so a frame_type
    %% value mismatch (e.g. atom round-trip) or a category
    %% misclassification is directly visible instead of inferred.
    catch macula_diagnostics:event(
            <<"_macula.peer_observer.on_frame">>,
            #{conn_pid  => ConnPid,
              node_id   => case frame_source(ConnPid, P) of
                                undefined -> undefined;
                                N -> binary:encode_hex(binary:part(N, 0, 4))
                            end,
              frame_type => macula_frame:frame_type(Frame),
              category   => frame_category(Frame)}),
    route(Frame, ConnPid, frame_source(ConnPid, P), S).

frame_source(ConnPid, P) ->
    maps:get(ConnPid, P, undefined).

route(Frame, ConnPid, undefined, S) ->
    %% TEMP DIAGNOSTIC (overlay_relay WAN-only vanishing-frame incident)
    %% — remove once root-caused. `ConnPid' has no entry in `peers', so
    %% the frame is silently dropped here with NO log and NO counter —
    %% this is the one unlogged branch in the entire dispatch path from
    %% `{quic, Bin, Stream, Flags}' down to `dispatch_overlay/5', and
    %% every diagnostic release so far (macula 10.5.1-10.5.3) proved the
    %% frame is read correctly, delivered to the correct owner, and
    %% enqueued into this process's mailbox intact — yet
    %% overlay_relay_stats/0's three counters stay at zero, meaning
    %% dispatch_overlay/5 is never reached at all. This is the only
    %% remaining place in the whole path capable of explaining that.
    catch macula_diagnostics:event(
            warning, <<"_macula.peer_observer.frame_source_unresolved">>,
            #{conn_pid => ConnPid, frame_type => macula_frame:frame_type(Frame)}),
    S;
route(Frame, ConnPid, NodeId, S) ->
    dispatch(frame_category(Frame), Frame, ConnPid, NodeId, S).

frame_category(Frame) -> classify(macula_frame:frame_type(Frame)).

classify(swim_ping)    -> swim;
classify(swim_ack)     -> swim;
classify(swim_suspect) -> swim;
classify(swim_confirm) -> swim;
classify(call)         -> call;
classify(result)       -> reply;
classify(error)        -> reply;
classify(advertise)    -> advertise;
classify(unadvertise)  -> advertise;
classify(subscribe)    -> pubsub;
classify(unsubscribe)  -> pubsub;
classify(publish)      -> pubsub;
classify(event)        -> pubsub;
classify(ping)         -> dht;
classify(pong)         -> dht;
classify(find_node)    -> dht;
classify(nodes)        -> dht;
classify(find_value)   -> dht;
classify(value)        -> dht;
classify(store)        -> dht;
classify(store_ack)    -> dht;
classify(_)            -> other.

claimed_caller(#{caller := <<_:256>> = Pub}) -> Pub;
claimed_caller(_)                            -> <<0:256>>.

claimed_replier(#{responded_by := <<_:256>> = Pub}) -> Pub;
claimed_replier(#{reported_by  := <<_:256>> = Pub}) -> Pub;
claimed_replier(_)                                  -> <<0:256>>.

%% `signer' for non-OPEN stream frames (stream_data / stream_end /
%% stream_error). SDK >= 4.4.9 stamps the emitter's pubkey here so
%% multi-hop relays can verify the signature end-to-end. Older SDKs
%% don't include the field; callers fall back to the inbound conn's
%% NodeId (single-hop only).
claimed_signer(#{signer := <<_:256>> = Pub}) -> Pub;
claimed_signer(_)                            -> undefined.

dispatch(swim, Frame, _ConnPid, NodeId, #state{swim = Swim} = S) ->
    deliver_swim(macula_frame:verify(Frame, NodeId), Frame, NodeId, Swim),
    S;
dispatch(call, Frame, ConnPid, NodeId, S) ->
    %% CALL is signed end-to-end by the original caller and forwarded
    %% as-is across relay hops, so verify against the frame's claimed
    %% caller — NOT the connection's NodeId, which would only match on
    %% a direct dial (caller -> first relay) and fail on every
    %% subsequent hop in a multi-station path.
    deliver_call(macula_frame:verify(Frame, claimed_caller(Frame)),
                 Frame, ConnPid, NodeId, S);
dispatch(reply, Frame, _ConnPid, _NodeId, S) ->
    %% RESULT signed by `responded_by'; call_error signed by
    %% `reported_by'. Both are preserved end-to-end across the relay
    %% chain, same reasoning as CALL above.
    deliver_reply(macula_frame:verify(Frame, claimed_replier(Frame)),
                  Frame, S);
dispatch(advertise, Frame, ConnPid, NodeId, S) ->
    deliver_advertise(macula_frame:verify(Frame, NodeId), Frame, ConnPid, NodeId, S),
    S;
dispatch(pubsub, Frame, _ConnPid, NodeId, S) ->
    deliver_pubsub(verify_pubsub(Frame, NodeId), Frame, NodeId, S),
    S;
dispatch(dht, Frame, _ConnPid, NodeId, #state{dht = Dht} = S) ->
    ok = macula_dht:handle_frame(Dht, NodeId, Frame),
    S;
dispatch(other, Frame, ConnPid, NodeId, S) ->
    dispatch_overlay(macula_frame:frame_type(Frame), Frame, ConnPid, NodeId, S).

%% Phase 3.5 — point-to-point overlay relay by NodeId. `overlay_relay' is
%% the only frame type this station acts on out of everything that falls
%% into `other'; every other unrecognised type is still silently dropped,
%% same as always. This is deliberately the narrowest possible relay: no
%% new state, no session/pairing bookkeeping, no cleanup on disconnect —
%% `conns' and its `:DOWN' handling are exactly what CALL forwarding
%% already uses (`on_remote_lookup/5' above), reused as-is. A target not
%% currently connected is DROPPED, not retried: HyParView is a
%% soft-state gossip protocol whose own shuffle/retry timers are the
%% recovery path, the same way they already have to tolerate ordinary
%% packet loss. It is no longer SILENT, though — both this and a bad
%% signature bump a counter (`overlay_relay_stats/0') and emit a
%% structured event, added after this exact ambiguity (attack/bug vs.
%% routine gossip loss, indistinguishable from outside a live trace)
%% blocked diagnosing a real incident.
dispatch_overlay(overlay_relay, Frame, _ConnPid, NodeId, #state{conns = C} = S) ->
    relay_overlay(macula_frame:verify(Frame, NodeId), NodeId, C),
    S;
dispatch_overlay(_Other, _Frame, _ConnPid, _NodeId, S) ->
    S.

relay_overlay({ok, #{peer := Target, payload := Payload}}, Origin, C) ->
    forward_overlay(primary_conn_lookup(maps:find(Target, C)), Origin, Target, Payload);
relay_overlay({error, Reason}, Origin, _C) ->
    %% Envelope signature doesn't verify against the authenticated
    %% sender — never relayed, same trust posture as CALL/ADVERTISE.
    %% `warning', not `info': unlike a routine unreachable-target drop
    %% below, a bad signature here is either an attack or a real
    %% protocol bug, and it was completely unobservable before this —
    %% the exact ambiguity that blocked diagnosing the incident this
    %% instrumentation exists to fix.
    bump_overlay(?CTR_OVERLAY_VERIFY_FAILED),
    catch macula_diagnostics:event(
            warning, <<"_macula.overlay_relay.verify_failed">>,
            #{origin => binary:encode_hex(binary:part(Origin, 0, 4)),
              reason => Reason}),
    ok.

forward_overlay({ok, TargetConn}, Origin, _Target, Payload) ->
    %% The relayed copy always carries OUR OWN authenticated identity for
    %% the sender — Origin comes from this connection's own verified
    %% NodeId, never from anything the original frame claimed about
    %% itself, so the receiving end's `Meta.sender' can't be spoofed by
    %% the relayed peer.
    bump_overlay(?CTR_OVERLAY_RELAYED),
    macula_peering:send_frame(
        TargetConn,
        macula_frame:overlay_relay(#{peer => Origin, payload => Payload}));
forward_overlay(error, Origin, Target, _Payload) ->
    %% `info', not `warning': an unreachable target is the expected,
    %% routine outcome of a soft-state gossip protocol (see this
    %% function's own moduledoc above), not an anomaly — but it was
    %% completely unobservable before this, indistinguishable from the
    %% verify-failed branch above from outside a live trace.
    bump_overlay(?CTR_OVERLAY_TARGET_NOT_CONNECTED),
    catch macula_diagnostics:event(
            <<"_macula.overlay_relay.target_not_connected">>,
            #{origin => binary:encode_hex(binary:part(Origin, 0, 4)),
              target => binary:encode_hex(binary:part(Target, 0, 4))}),
    ok.

deliver_swim({ok, _}, Frame, NodeId, Swim) ->
    ok = macula_swim:handle_frame(Swim, NodeId, Frame);
deliver_swim({error, _Reason}, _Frame, _NodeId, _Swim) ->
    ok.

%% CALL dispatch — verify signature, then look the procedure up
%% first against the local per-identity handler_registry (DHT
%% primitives etc), and on miss fall through to the remote-advertise
%% registry (procedures registered by connected peers via the
%% ADVERTISE wire frame). On a remote-advertise hit we forward the
%% CALL across the advertiser's QUIC connection and remember the
%% origin in `forwarded' so the inbound RESULT / call_error can be
%% relayed back. Handler invocation for local procedures runs inside
%% `macula_handler_dispatch' which traps crashes; the observer
%% process is never blocked by a misbehaving handler.
deliver_call(_Verified, _Frame, _ConnPid, _NodeId,
             #state{handler_registry = undefined,
                    remote_advertise = undefined} = S) ->
    S;
deliver_call({error, _}, _Frame, _ConnPid, _NodeId, S) ->
    S;
deliver_call({ok, Frame}, _OrigFrame, ConnPid, NodeId, S) ->
    on_call_lookup(local_lookup(Frame, S), Frame, ConnPid, NodeId, S).

%% ⚠ A REGISTRY THAT IS SLOW COSTS ONE DROPPED CALL, NEVER THIS PROCESS.
%%
%% This exact line killed station-de-frankfurt on 2026-08-05:
%%
%%   exception exit: {timeout, {gen_server,call,
%%                    [macula_handler_registry, {lookup, <<"_macula.ping">>}]}}
%%
%% The observer died, the supervisor restarted it, `init/1' recreated the
%% public conns ETS mirror EMPTY, and pubsub fan-out — which resolves
%% subscriber connections through that mirror — was permanently blind to
%% every client that connected afterwards. `/health' said healthy for hours.
%%
%% The lookup itself is now an ETS read (see `macula_handler_registry'), so
%% this should not be reachable. It is caught anyway, because the comment on
%% `on_connected_directional/5' records the SAME crash from a different
%% callee in 2026-05, and the lesson that was not learnt then is that this
%% process must survive ANY callee, not the one that last misbehaved.
local_lookup(_Frame, #state{handler_registry = undefined}) ->
    {error, not_found};
local_lookup(Frame, #state{handler_registry = Registry}) ->
    Procedure = maps:get(procedure, Frame),
    guarded(fun() -> macula_handler_registry:lookup(Registry, Procedure) end,
            handler_registry, Procedure).

%% Not found, loudly. A dropped CALL is a caller-visible timeout and a line in
%% the log; a dead observer is a silent station. Never swallow the reason: the
%% 2026-08-05 outage was diagnosed from a single crash report, and had this
%% path already existed the log would have named the culprit on the first frame
%% instead of on the one that happened to kill the process.
guarded(Fun, Which, Procedure) ->
    try Fun()
    catch Class:Reason ->
        logger:warning("[peer_observer] ~p lookup failed for ~p: ~p:~p — "
                       "dropping this CALL, staying alive",
                       [Which, Procedure, Class, Reason]),
        {error, not_found}
    end.

on_call_lookup({ok, _Handler}, Frame, _ConnPid, NodeId,
               #state{handler_registry = Registry,
                      self_id = SelfId, conns = C} = S) ->
    %% Spawn a worker per local CALL so the observer doesn't block on
    %% handler execution. DHT primitives like `_dht.find_record' do a
    %% one-hop fanout that can sit for up to 1.7s waiting on remote
    %% peers (?REMOTE_FIND_PER_PEER_MS = 1500ms + a small buffer);
    %% running that inline serialised every concurrent CALL on this
    %% station behind one another and turned 10 parallel finds into
    %% ~17s, which surfaced as `cross_station_many_concurrent_dht_records'
    %% timeouts. Observer state is read-only for the duration of the
    %% handler — we snapshot the target ConnPid here and let the worker
    %% send the reply over it directly.
    Target = primary_conn_lookup(maps:find(NodeId, C)),
    _ = spawn(fun() ->
                  Reply = macula_handler_dispatch:dispatch_call(Frame,
                                                                Registry,
                                                                SelfId),
                  send_reply_to(Target, Reply)
              end),
    S;
on_call_lookup({error, not_found}, Frame, ConnPid, NodeId, S) ->
    on_remote_lookup(remote_lookup(Frame, S), Frame, ConnPid, NodeId, S).

%% Guarded for the same reason as `local_lookup/2', and it is on the same
%% frame path: every CALL that is not a local procedure reaches this one.
remote_lookup(_Frame, #state{remote_advertise = undefined}) ->
    {error, not_found};
remote_lookup(Frame, #state{remote_advertise = R}) ->
    Realm     = maps:get(realm,     Frame),
    Procedure = maps:get(procedure, Frame),
    guarded(fun() -> macula_remote_advertise_registry:lookup(R, Realm, Procedure) end,
            remote_advertise_registry, Procedure).

%% Remote-advertise miss — synthesize a signed `unknown_next_peer'
%% reply to the caller (origin connection) so they fail fast.
on_remote_lookup({error, not_found}, Frame, _ConnPid, NodeId,
                 #state{self_id = SelfId, conns = C} = S) ->
    Reply = unknown_next_peer_reply(Frame, SelfId),
    send_reply_to(primary_conn_lookup(maps:find(NodeId, C)), Reply),
    S;
on_remote_lookup({ok, #{conn_pid := AdvertiserConn}}, Frame, _ConnPid, NodeId,
                 #state{conns = C, forwarded = F} = S) ->
    %% Forward the CALL frame as-is over the advertiser's connection.
    %% The advertiser's `macula_station_link' will dispatch it to its
    %% local handler and emit a RESULT or call_error back. Track the
    %% origin so we can route the reply.
    macula_peering:send_frame(AdvertiserConn, Frame),
    Origin = origin_for_reply(primary_conn_lookup(maps:find(NodeId, C))),
    track_forwarded(maps:get(call_id, Frame), Origin, F, S).

origin_for_reply({ok, ConnPid}) -> ConnPid;
origin_for_reply(error)         -> undefined.

track_forwarded(_CallId, undefined, _F, S) ->
    %% Origin already gone; the eventual RESULT will be dropped on
    %% arrival because no `forwarded' entry exists. Safe.
    S;
track_forwarded(CallId, Origin, F, S) ->
    %% TTL timer ensures the entry is purged even when no reply ever
    %% arrives (advertiser disconnects mid-call, advertiser process
    %% hangs, malformed reply). Cancelled in `relay_forwarded_reply'
    %% on a normal RESULT/ERROR.
    TRef = erlang:send_after(?FORWARDED_TTL_MS, self(),
                             {forwarded_timeout, CallId}),
    S#state{forwarded = F#{CallId => {Origin, TRef}}}.

unknown_next_peer_reply(#{call_id := CallId}, SelfId) ->
    macula_frame:call_error(#{call_id     => CallId,
                              code        => 16#01,
                              reported_by => SelfId}).

%%==================================================================
%% STREAM dispatch — dedicated QUIC streams, NOT the shared control
%% stream. Each session has its own stream on the caller's connection
%% and its own stream on the advertiser's connection; this relay is a
%% full participant on both, opening the advertiser-side stream
%% itself and mirroring frames verbatim between the two.
%%
%% The first frame on a fresh dedicated stream (no `stream_route'
%% entry yet) is STREAM_OPEN (routes by procedure, exactly like a
%% CALL) or CALL itself (content transfer, PLAN_PER_STREAM_QUIC_
%% ISOLATION.md Phase 2 — see the CALL clause below). Every later
%% frame on either side of an established STREAM_OPEN route is looked
%% up directly by its physical stream reference (no stream_id map
%% lookup needed) and relayed onto the paired stream.
%%==================================================================

dispatch_dedicated_frame(Frame, ConnPid, Stream, S) ->
    on_dedicated_frame(maps:find(Stream, S#state.stream_route),
                       macula_frame:frame_type(Frame), Frame, ConnPid,
                       Stream, S).

%% Content transfer never populates `stream_route' — a content stream
%% carries one independent local CALL after another, each dispatched
%% and replied on this same stream reference, with no cross-connection
%% relay and no established "route" to remember between them. So this
%% clause fires uniformly for the FIRST call on a fresh content stream
%% and every one after it, not just the first.
on_dedicated_frame(error, call, Frame, _ConnPid, Stream, S) ->
    dispatch_content_call(macula_frame:verify(Frame, claimed_caller(Frame)),
                          Frame, Stream, S);
on_dedicated_frame(error, stream_open, Frame, ConnPid, Stream, S) ->
    NodeId = frame_source(ConnPid, S#state.peers),
    on_stream_open_verify(macula_frame:verify(Frame, claimed_caller(Frame)),
                          Frame, ConnPid, Stream, NodeId, S);
on_dedicated_frame(error, _NotOpen, _Frame, _ConnPid, _Stream, S) ->
    %% Nothing but STREAM_OPEN or CALL legitimately opens a fresh
    %% dedicated stream — anything else arriving first is a protocol
    %% violation from a misbehaving or pre-dedicated-stream peer.
    %% Drop it; the stream simply never gets a route and its buffer
    %% is reclaimed on disconnect.
    S;
on_dedicated_frame({ok, {_OtherConn, OtherStream, Sid}}, Type, Frame,
                   ConnPid, _Stream, S) ->
    on_routed_frame(verify_dedicated(Type, Frame, ConnPid, S#state.peers),
                    Type, Sid, OtherStream, S).

%% Local handler dispatch only — content procedures are never
%% remote-advertised (every station serves `_content.*' from its own
%% store, see macula_station_content_handlers), so there is no
%% forward-to-advertiser branch here the way `deliver_call/5' has one
%% for ordinary CALL. `dispatch_call/3' already produces a well-formed
%% RESULT or call_error frame — including for an unregistered
%% procedure (`unknown_next_peer') — so a caller that mistakenly
%% routes a non-local procedure through a dedicated stream gets a
%% clean error, not silence.
dispatch_content_call({error, _}, _Frame, _Stream, S) ->
    S;
dispatch_content_call({ok, _Frame}, _OrigFrame, _Stream,
                      #state{handler_registry = undefined} = S) ->
    S;
dispatch_content_call({ok, Frame}, _OrigFrame, Stream,
                      #state{handler_registry = Registry, self_id = SelfId,
                             identity = Id} = S) ->
    spawn(fun() ->
        Reply = macula_handler_dispatch:dispatch_call(Frame, Registry, SelfId),
        _ = catch macula_peering:send_on_stream(Stream, Reply, Id)
    end),
    S.

%% STREAM_REPLY is signed end-to-end by `responded_by' (same pattern
%% as a RESULT). STREAM_DATA / STREAM_END / STREAM_ERROR: SDK >= 4.4.9
%% stamps the emitter's pubkey in `signer'; older SDKs fall back to
%% the inbound conn's NodeId (single-hop only, matches legacy
%% behaviour on the shared-stream path this replaces).
verify_dedicated(stream_reply, Frame, _ConnPid, _Peers) ->
    macula_frame:verify(Frame, claimed_replier(Frame));
verify_dedicated(_Other, Frame, ConnPid, Peers) ->
    Pub = case claimed_signer(Frame) of
              undefined -> frame_source(ConnPid, Peers);
              Signer    -> Signer
          end,
    macula_frame:verify(Frame, Pub).

on_routed_frame({error, _}, _Type, _Sid, _OtherStream, S) ->
    S;
on_routed_frame({ok, Frame}, Type, Sid, OtherStream, S) ->
    relay_on_stream(OtherStream, Frame, S),
    maybe_close_stream_route(Type, Sid, S).

on_stream_open_verify({error, _}, _Frame, _ConnPid, _Stream, _NodeId, S) ->
    S;
on_stream_open_verify({ok, Frame}, _OrigFrame, ConnPid, Stream, _NodeId, S) ->
    on_stream_open_lookup(remote_lookup(Frame, S), Frame, ConnPid, Stream, S).

%% Procedure not advertised on this station — synthesize a
%% `stream_error' back to the caller, directly on their dedicated
%% stream, so the SDK fails fast instead of waiting for the deadline.
on_stream_open_lookup({error, not_found}, Frame, _CallerConn, CallerStream,
                      #state{self_id = SelfId} = S) ->
    reply_on_stream(CallerStream, stream_unknown_reply(Frame, SelfId), S);
on_stream_open_lookup({ok, #{conn_pid := AdvertiserConn}}, Frame,
                      CallerConn, CallerStream, S) ->
    %% Open OUR side of the advertiser's dedicated stream (mirrors
    %% what the caller's SDK already did on its side), forward
    %% STREAM_OPEN as the first bytes on it, and remember the route
    %% both ways so subsequent frames need no procedure lookup.
    open_advertiser_stream(macula_peering:open_dedicated_stream(AdvertiserConn),
                           Frame, CallerConn, CallerStream, AdvertiserConn, S).

open_advertiser_stream({ok, AdvStream}, Frame, CallerConn, CallerStream,
                       AdvertiserConn, S) ->
    S1 = reply_on_stream(AdvStream, Frame, S),
    track_stream_route(maps:get(stream_id, Frame), CallerConn, CallerStream,
                       AdvertiserConn, AdvStream, S1);
open_advertiser_stream({error, _Reason}, Frame, _CallerConn, CallerStream,
                       _AdvertiserConn, #state{self_id = SelfId} = S) ->
    reply_on_stream(CallerStream,
                    stream_unavailable_reply(Frame, SelfId), S).

track_stream_route(Sid, CallerConn, CallerStream, AdvConn, AdvStream,
                   #state{streams = Streams, stream_route = Route,
                          stream_bufs = Bufs} = S) ->
    TRef = erlang:send_after(?STREAM_TTL_MS, self(), {stream_timeout, Sid}),
    S#state{
        streams = Streams#{Sid => {CallerConn, CallerStream, AdvConn,
                                   AdvStream, TRef}},
        stream_route = Route#{CallerStream => {AdvConn, AdvStream, Sid},
                              AdvStream    => {CallerConn, CallerStream, Sid}},
        %% We opened AdvStream ourselves, so no `new_dedicated_stream'
        %% notification seeds its buffer the way an inbound one does —
        %% seed it here instead.
        stream_bufs = Bufs#{AdvStream => {AdvConn, <<>>}}
    }.

stream_unknown_reply(#{stream_id := Sid}, _SelfId) ->
    macula_frame:stream_error(#{stream_id => Sid,
                                code      => <<"unknown_next_peer">>,
                                message   => <<"procedure not advertised">>}).

stream_unavailable_reply(#{stream_id := Sid}, _SelfId) ->
    macula_frame:stream_error(#{stream_id => Sid,
                                code      => <<"unavailable">>,
                                message   => <<"failed to open relay stream">>}).

%% STREAM_END / STREAM_ERROR / STREAM_REPLY are terminal. For
%% server_stream (the only mode our suite exercises today) the
%% server emits STREAM_END(role=send) and the stream is done. Bidi
%% would close on a matched pair of STREAM_END(role=send) frames; a
%% stricter implementation would wait for both. Close on the first
%% terminal frame for now — the TTL timer catches the bidi case if
%% the second END never arrives.
maybe_close_stream_route(stream_end,   Sid, S) -> drop_stream_route(Sid, S);
maybe_close_stream_route(stream_error, Sid, S) -> drop_stream_route(Sid, S);
maybe_close_stream_route(stream_reply, Sid, S) -> drop_stream_route(Sid, S);
maybe_close_stream_route(_Other, _Sid, S)      -> S.

drop_stream_route(Sid, #state{streams = Streams} = S) ->
    close_stream_entry(maps:take(Sid, Streams), S).

close_stream_entry(error, S) ->
    S;
close_stream_entry({{_CallerConn, CallerStream, _AdvConn, AdvStream, TRef},
                    NewStreams}, S) ->
    _ = erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
    close_stream_pair(CallerStream, AdvStream, S#state{streams = NewStreams}).

%% Reclaim both dedicated QUIC streams and their routing/buffer
%% entries. Both are OURS to close: `CallerStream' custody was
%% transferred to us via `open_and_handoff' when the caller's SDK
%% opened it, and `AdvStream' is one we opened ourselves.
close_stream_pair(CallerStream, AdvStream,
                  #state{stream_route = Route, stream_bufs = Bufs} = S) ->
    catch macula_quic:close_stream(CallerStream),
    catch macula_quic:close_stream(AdvStream),
    S#state{
        stream_route = maps:without([CallerStream, AdvStream], Route),
        stream_bufs  = maps:without([CallerStream, AdvStream], Bufs)
    }.

%% Send a frame directly on a dedicated stream. Frames relayed
%% verbatim already carry the original signer's `signature' — the
%% station's own identity is only ever used for frames the STATION
%% itself originates (stream_error synthesized here).
reply_on_stream(Stream, Frame, #state{identity = Id} = S) ->
    _ = catch macula_peering:send_on_stream(Stream, Frame, Id),
    S.

relay_on_stream(Stream, Frame, #state{identity = Id}) ->
    _ = catch macula_peering:send_on_stream(Stream, Frame, Id),
    ok.

%% RESULT / call_error from an advertiser destined for a previously-
%% forwarded CALL. Match by call_id; relay back to the origin.
deliver_reply({error, _}, _Frame, S) ->
    S;
deliver_reply({ok, Frame}, _OrigFrame, S) ->
    relay_forwarded_reply(maps:take(maps:get(call_id, Frame, <<>>),
                                    S#state.forwarded), Frame, S).

relay_forwarded_reply(error, _Frame, S) ->
    %% Not a forwarded reply (e.g. station-internal callers like
    %% relay_ping carry their own state_link gen_server pending map).
    %% Pass through.
    S;
relay_forwarded_reply({{Origin, TRef}, NewF}, Frame, S) ->
    _ = erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
    macula_peering:send_frame(Origin, Frame),
    S#state{forwarded = NewF}.

%% ADVERTISE / UNADVERTISE — register or drop a remote procedure
%% routing entry. Frames are signed by the advertiser; verification
%% already happened in `dispatch'. The advertiser pubkey on the
%% wire MUST equal the connection-bound NodeId so a peer cannot
%% advertise on behalf of another identity.
deliver_advertise({error, _}, _Frame, _ConnPid, _NodeId, _S) ->
    ok;
deliver_advertise({ok, Frame}, _OrigFrame, ConnPid, NodeId,
                  #state{remote_advertise = R, is_station = IsSt}) ->
    on_advertise_frame(R, macula_frame:frame_type(Frame), Frame,
                       ConnPid, NodeId, advertise_source(NodeId, IsSt)).

%% Classify an inbound ADVERTISE by whether the connection carrying
%% it is a relay station (gossip) or a daemon (direct). Stations OR
%% `?CAP_STATION' into their CONNECT capabilities; daemons leave it
%% unset. Pre-4.5.0 SDKs read as `daemon' (legacy behaviour).
advertise_source(NodeId, IsSt) ->
    case maps:get(NodeId, IsSt, false) of
        true  -> gossip;
        false -> direct
    end.

on_advertise_frame(undefined, _Type, _Frame, _ConnPid, _NodeId, _Src) ->
    ok;
on_advertise_frame(R, advertise, Frame, ConnPid, NodeId, Source) ->
    Realm     = maps:get(realm,      Frame),
    Procedure = maps:get(procedure,  Frame),
    Adv       = maps:get(advertiser, Frame),
    %% Reject mismatched advertiser to keep the per-conn invariant.
    on_advertise_match(Adv =:= NodeId, R, Realm, Procedure, Adv, ConnPid, Source);
on_advertise_frame(R, unadvertise, Frame, _ConnPid, NodeId, Source) ->
    Realm     = maps:get(realm,      Frame),
    Procedure = maps:get(procedure,  Frame),
    Adv       = maps:get(advertiser, Frame),
    on_unadvertise_match(Adv =:= NodeId, R, Realm, Procedure, Source).

on_advertise_match(false, _R, _Realm, _Proc, _Adv, _ConnPid, _Source) ->
    ok;
on_advertise_match(true, R, Realm, Proc, Adv, ConnPid, Source) ->
    %% An ADVERTISE frame queued on a connection that has since died
    %% (a newer handshake from the same peer superseded it via
    %% `purge_stale_slot/4', or it crashed outright) can still be
    %% sitting in this process's mailbox and get processed here AFTER
    %% the connection is already gone. Confirmed live: a station's
    %% registry held an entry whose `conn_pid' was already dead --
    %% `is_process_alive' came back `false' for a pid `list/1' still
    %% returned, on a fleet station, well after the connection that
    %% sent this exact frame had been superseded. Refusing to
    %% register/replace against a dead `ConnPid' closes the race
    %% regardless of ordering against `purge_stale_slot/4' — no
    %% special-casing needed for exactly which of the two ran first.
    live_advertise_match(is_process_alive(ConnPid), R, Realm, Proc, Adv, ConnPid, Source).

live_advertise_match(false, _R, _Realm, _Proc, _Adv, _ConnPid, _Source) ->
    ok;
live_advertise_match(true, R, Realm, Proc, Adv, ConnPid, Source) ->
    %% Direct vs gossip gate. A `direct' ADVERTISE comes straight
    %% from the original daemon's connection (CAP_STATION unset) —
    %% it always wins, replacing any existing entry. A `gossip'
    %% ADVERTISE is relayed from another station (CAP_STATION set)
    %% and is first-write-wins: it never overwrites a live entry.
    %%
    %% Without the gate, two failure modes alternate:
    %%   * Pre-this-fix (gossip-always-skips-existing): a direct
    %%     ADVERTISE arriving AFTER a gossip echo silently no-ops,
    %%     so CALL routes via the gossip path (2+ hops) instead of
    %%     direct, and daemon-mobility-across-stations strands
    %%     forwarding loops.
    %%   * No-gate (last-write-wins): a gossip echo bouncing back
    %%     to the station that originated the direct entry would
    %%     overwrite the daemon conn_pid with the peer-station
    %%     conn_pid, sending CALLs on a roundtrip through the mesh.
    %%
    %% The router only re-propagates on a notify_router_change/0
    %% kick, so we kick on every state-change (register OR replace),
    %% not just on the not_found path. Otherwise a direct replace
    %% of a stale gossip entry wouldn't trip the gossip-out diff.
    Existing = macula_remote_advertise_registry:lookup(R, Realm, Proc),
    on_advertise_gate(Existing, Source, R, Realm, Proc, Adv, ConnPid).

on_advertise_gate({error, not_found}, Source, R, Realm, Proc, Adv, ConnPid) ->
    macula_remote_advertise_registry:register(
      R, Realm, Proc, #{advertiser => Adv, conn_pid => ConnPid, source => Source}),
    notify_router_change();
on_advertise_gate({ok, #{conn_pid := ConnPid, advertiser := ExAdv}}, _Source,
                  _R, _Realm, _Proc, Adv, ConnPid) when ExAdv =:= Adv ->
    %% Exact same entry already there — no-op. Common case for
    %% idempotent ADVERTISE replays.
    ok;
on_advertise_gate({ok, _Existing}, direct, R, Realm, Proc, Adv, ConnPid) ->
    %% Direct trumps anything — replace.
    macula_remote_advertise_registry:register(
      R, Realm, Proc, #{advertiser => Adv, conn_pid => ConnPid, source => direct}),
    notify_router_change();
on_advertise_gate({ok, _Existing}, gossip, _R, _Realm, _Proc, _Adv, _ConnPid) ->
    %% Gossip never overwrites a live entry; first-write-wins.
    ok.

on_unadvertise_match(false, _R, _Realm, _Proc, _Source) ->
    ok;
on_unadvertise_match(true, R, Realm, Proc, Source) ->
    %% Same direct-vs-gossip gate `on_advertise_gate/7' applies to ADD —
    %% previously missing here entirely, which was a real bug: gossip is
    %% distance-vector (`macula_station_peering_router:sync_advertises/3'
    %% re-attributes every relayed frame to the relaying station's own
    %% SelfId), so a peer that ever briefly lost its OWN gossip-learned
    %% copy of an entry — for any transient reason on ITS side — would
    %% compute an honest UNADVERTISE diff and echo it back to US over
    %% the very connection that carries our DIRECT daemon's own
    %% registration. `Adv =:= NodeId' passes on that echo exactly like
    %% it does on a real direct unadvertise (the relaying station's
    %% frame legitimately claims itself, having rewritten `advertiser'
    %% to its own SelfId), so with no gate a transient gossip hiccup one
    %% hop away could permanently erase a daemon that never once asked
    %% to stop serving. Direct still trumps anything, matching ADD:
    %% only a gossip-sourced unadvertise needs the existing entry to
    %% ALSO be gossip-sourced (or absent) before it is honoured.
    on_unadvertise_gate(Source, macula_remote_advertise_registry:lookup(R, Realm, Proc),
                        R, Realm, Proc).

on_unadvertise_gate(direct, _Existing, R, Realm, Proc) ->
    do_unregister(R, Realm, Proc);
on_unadvertise_gate(gossip, {error, not_found}, _R, _Realm, _Proc) ->
    ok;
on_unadvertise_gate(gossip, {ok, Entry}, R, Realm, Proc) ->
    %% `source' defaults to `direct' when absent (matches
    %% macula_remote_advertise_registry's own moduledoc: "legacy
    %% callers that don't pass source default to direct so legacy
    %% registrations stay routable") — an entry of unknown provenance
    %% is treated as protected, not as fair game for a gossip echo.
    on_unadvertise_source_gate(maps:get(source, Entry, direct), R, Realm, Proc).

on_unadvertise_source_gate(direct, _R, _Realm, _Proc) ->
    %% A gossip echo trying to retract a live direct entry — refused,
    %% same as a gossip ADVERTISE never overwriting one.
    ok;
on_unadvertise_source_gate(gossip, R, Realm, Proc) ->
    do_unregister(R, Realm, Proc).

do_unregister(R, Realm, Proc) ->
    macula_remote_advertise_registry:unregister(R, Realm, Proc),
    %% Propagate the removal promptly too (see on_advertise_match/6).
    notify_router_change().

send_reply_to(error, _Reply) ->
    %% Caller's connection is gone — RESULT goes nowhere. The
    %% connection cleanup already removed it from `conns'; nothing
    %% to do.
    ok;
send_reply_to({ok, ConnPid}, Reply) ->
    macula_peering:send_frame(ConnPid, Reply).

%% PubSub dispatch — verify signature, route SUBSCRIBE / UNSUBSCRIBE
%% / EVENT into the per-identity registry (which keys by realm tag
%% and forwards to the matching `hecate_pubsub_server'). The registry
%% creates a new server on first use, so SUBSCRIBE on an unseen realm
%% works without explicit pre-registration.
deliver_pubsub(_Verified, _Frame, _NodeId,
               #state{pubsub_registry = undefined}) ->
    logger:warning("[peer_observer] pubsub frame dropped — no registry"),
    ok;
deliver_pubsub({error, Reason}, _Frame, _NodeId, _S) ->
    logger:warning("[peer_observer] pubsub frame verify failed: ~p", [Reason]),
    ok;
deliver_pubsub({ok, Verified}, _Frame, NodeId,
               #state{pubsub_registry = Reg}) ->
    Realm = maps:get(realm, Verified),
    Topic = maps:get(topic, Verified, undefined),
    Type  = macula_frame:frame_type(Verified),
    logger:debug(
      "[peer_observer] pubsub ~s realm=~p topic=~s",
      [Type, Realm, Topic]),
    %% Delegate to the ONE implementation of the delivery path. This
    %% module used to carry its own copy of all of it — dedup
    %% disposition, origin seeding, local fan-out, bloom-fan, conn
    %% lookup — and the two copies drifted: fixes landed on one and not
    %% the other, so a station could serve two different pubsub
    %% disciplines depending on which path a connection happened to
    %% take. See `macula_station_route_pubsub_frames:deliver_verified/5'.
    macula_station_route_pubsub_frames:deliver_verified(
      Type, Realm, NodeId, Verified, Reg).

notify_router_change() ->
    case whereis(macula_station_peering_router) of
        undefined -> ok;
        Pid       ->
            %% Async cast so peer_observer doesn't block on router
            %% sync (sync can take seconds when fanning out to many
            %% peers). The router treats `tick' as a kick: it syncs
            %% promptly but leaves the periodic `timer_tick' to re-arm.
            Pid ! tick, ok
    end.
%% Pubsub Phase 2 step 3 — verify an inbound pubsub frame. An EVENT
%% carrying a publisher-end-to-end signature is verified against the
%% publisher (so it passes at any relay hop, not just one); loops are
%% killed by `event_dedup_disposition/1', not by a verify mismatch.
%% Everything else — SUBSCRIBE / UNSUBSCRIBE / PUBLISH, and EVENTs
%% from a daemon not yet emitting `publisher_sig' — is verified
%% against the connection's NodeId, exactly as before.
verify_pubsub(#{frame_type := event, publisher_sig := _} = Frame, _NodeId) ->
    macula_frame:verify_publisher(Frame);
verify_pubsub(Frame, NodeId) ->
    macula_frame:verify(Frame, NodeId).

%%==================================================================
%% disconnected
%%==================================================================

on_disconnected(ConnPid, #state{dht = Dht, swim = Swim,
                                peers = P, conns = C,
                                direction_of_pid = D,
                                forwarded = F,
                                remote_advertise = R,
                                last_frame_at = LF} = S) ->
    NodeId    = maps:get(ConnPid, P, undefined),
    Direction = maps:get(ConnPid, D, undefined),
    NewConns  = drop_directional_conn(NodeId, Direction, ConnPid, C),
    %% Remove the peer from SWIM only when BOTH directions are gone —
    %% a mutual-peer with one direction still alive is still reachable.
    %% When it IS still reachable, re-point SWIM at the surviving conn:
    %% it was handed the pid that just died and would probe it forever.
    Resynced = resync_swim_after_conn_loss(NodeId, NewConns, Swim,
                                           S#state.is_station),
    %% Same isolation rule for DHT: only forget when both directions
    %% are gone. DHT routing-table entries are expensive to rebuild
    %% (re-discovery via cross-station propagation), so keep them as
    %% long as ANY connection to the NodeId remains. When isolation
    %% IS reached, prune — without this, daemon entries leak forever
    %% and macula_dht's mailbox grows unbounded under sustained load
    %% (see docs/CASCADE_INVESTIGATION.md).
    maybe_forget_if_isolated(NodeId, NewConns, Dht, S#state.is_station),
    maybe_purge_advertise(R, ConnPid),
    NewIsStation = drop_is_station_if_isolated(NodeId, NewConns, S#state.is_station),
    S1 = bump_if_resynced(Resynced, S),
    %% Dedicated QUIC streams don't self-clean like the shared control
    %% stream did: each is a real resource this observer holds custody
    %% of. Without this, the peer on the OTHER side of a session
    %% routed through the dying ConnPid would sit until the 5-minute
    %% TTL fires, and the dead stream refs would leak in `stream_route'
    %% / `stream_bufs' the whole time.
    S2 = drop_streams_for(ConnPid, S1),
    S3 = drop_orphan_stream_bufs_for(ConnPid, S2),
    S3#state{peers            = maps:remove(ConnPid, P),
             conns            = NewConns,
             direction_of_pid = maps:remove(ConnPid, D),
             forwarded        = drop_forwarded_for(ConnPid, F),
             last_frame_at    = maps:remove(ConnPid, LF),
             is_station       = NewIsStation}.

%% Tear down every routed stream session that has either its caller
%% or its advertiser leg on the connection that just died — the
%% session cannot continue with one side gone.
drop_streams_for(ConnPid, #state{streams = Streams} = S) ->
    maps:fold(fun(Sid, Entry, Acc) ->
        drop_stream_if_owned(ConnPid, Sid, Entry, Acc)
    end, S, Streams).

drop_stream_if_owned(ConnPid, Sid,
                     {ConnPid, CallerStream, _AdvConn, AdvStream, TRef}, S) ->
    drop_dead_stream_entry(Sid, CallerStream, AdvStream, TRef, S);
drop_stream_if_owned(ConnPid, Sid,
                     {_CallerConn, CallerStream, ConnPid, AdvStream, TRef}, S) ->
    drop_dead_stream_entry(Sid, CallerStream, AdvStream, TRef, S);
drop_stream_if_owned(_ConnPid, _Sid, _Entry, S) ->
    S.

drop_dead_stream_entry(Sid, CallerStream, AdvStream, TRef,
                       #state{streams = Streams} = S) ->
    _ = erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
    close_stream_pair(CallerStream, AdvStream,
                      S#state{streams = maps:remove(Sid, Streams)}).

%% A dedicated stream whose STREAM_OPEN never fully arrived (still
%% sitting only in `stream_bufs', with no `streams'/`stream_route'
%% entry yet) leaks otherwise — `drop_streams_for/2' above only
%% catches sessions that made it to a tracked route.
drop_orphan_stream_bufs_for(ConnPid, #state{stream_bufs = Bufs} = S) ->
    S#state{stream_bufs = maps:filter(fun(_Stream, {OwnerConn, _Buf}) ->
                                          OwnerConn =/= ConnPid
                                      end, Bufs)}.

bump_if_resynced(resynced, S) -> bump_verdict(conn_resynced, S);
bump_if_resynced(_Other,   S) -> S.

%% DAEMONS are forgotten on isolation. STATIONS are not.
%%
%% Delete-on-disconnect was shipped in May to stop daemon entries leaking
%% forever and growing macula_dht's mailbox without bound
%% (docs/CASCADE_INVESTIGATION.md). It solved that, but it applies the same rule
%% to stations, and for a station it is a RATCHET: a station entry is expensive
%% to rebuild (re-discovery via cross-station propagation) and there is almost
%% nothing on this fleet that rebuilds one — `macula_dht_lookup' runs only from
%% the rebootstrap watchdog, and the core stations have no watchdog at all. So
%% every station disconnect permanently shrinks the table.
%%
%% That matters more the moment anything dials: a successful-then-dropped dial
%% would delete the very entry the dial created, and walk-learned entries
%% (currently immortal, because no conn ever existed for them) would become
%% forgettable. The precedent is on file — docs/DHT_FIND_FLAKE_ATTEMPT.md:23-30
%% records a change that took the table from ~8 entries to ~3 and
%% `cross_station_dht_put_find' from 2/5 to 0/5. "The real lever is keeping the
%% routing table FULLER, not smaller."
%%
%% Stations are a bounded, long-lived population (7 on this fleet); daemons are
%% the unbounded churning one, so keeping stations does not reopen the leak.
%%
%% ⚠ NOT the whole fix. The cascade doc scopes it as conn-aging PLUS
%% routing-table TTL eviction. TTL is deliberately NOT added here: with no
%% periodic bucket refresh in the tree, a TTL sweep would age out walk-learned
%% entries faster than anything recreates them and drain the table by a second
%% route. TTL and bucket refresh have to land together.
%%
%% `is_station' is resolved asynchronously and defaults to `false', so a station
%% that drops before its capability probe returns is still treated as a daemon
%% and forgotten. That is today's behaviour, so it is not a regression -- just
%% not yet the improvement.
maybe_forget_if_isolated(undefined, _NewConns, _Dht, _IsStation) ->
    ok;
maybe_forget_if_isolated(NodeId, NewConns, Dht, IsStation) ->
    forget_if_isolated(is_isolated(maps:find(NodeId, NewConns)),
                       maps:get(NodeId, IsStation, false), NodeId, Dht).

is_isolated(error) ->
    true;
is_isolated({ok, #{inbound := undefined, outbound := undefined}}) ->
    true;
is_isolated(_) ->
    false.

%% isolated? | station? |
forget_if_isolated(false, _Station, _NodeId, _Dht) -> ok;
forget_if_isolated(true,  true,     _NodeId, _Dht) -> ok;
forget_if_isolated(true,  false,     NodeId,  Dht) ->
    macula_dht:forget(Dht, NodeId).

drop_directional_conn(undefined, _Direction, _ConnPid, C) ->
    C;
drop_directional_conn(NodeId, undefined, ConnPid, C) ->
    %% Direction unknown (DOWN before any `connected' notification
    %% landed, or pre-upgrade entry from an older shape). Best-effort:
    %% scrub the pid from BOTH slots, preserving anything else.
    Existing = maps:get(NodeId, C, empty_peer_conns()),
    Cleared = maps:fold(fun(K, V, Acc) ->
        Acc#{K => clear_if_match(V, ConnPid)}
    end, #{}, Existing),
    on_cleared_peer_conns(NodeId, Cleared, C);
drop_directional_conn(NodeId, Direction, ConnPid, C) ->
    Existing = maps:get(NodeId, C, empty_peer_conns()),
    Cleared = Existing#{Direction => clear_if_match(maps:get(Direction, Existing, undefined),
                                                    ConnPid)},
    on_cleared_peer_conns(NodeId, Cleared, C).

clear_if_match(Pid, Pid)            -> undefined;
clear_if_match(Existing, _OtherPid) -> Existing.

%% Drop the NodeId entry entirely once both slots are empty so the
%% conns map doesn't leak `#{inbound => undefined, outbound => undefined}'
%% husks for every disconnected peer. ETS mirror is updated in
%% lock-step so external `conn_for/2' readers never see a stale
%% pid for a peer whose connection has died.
on_cleared_peer_conns(NodeId, #{inbound := undefined, outbound := undefined}, C) ->
    delete_conn_table(NodeId),
    maps:remove(NodeId, C);
on_cleared_peer_conns(NodeId, Cleared, C) ->
    write_conn_table(NodeId, Cleared),
    C#{NodeId => Cleared}.

%% Drop the NodeId's is_station flag once ALL its conns are gone.
%% Otherwise the flag would persist across full disconnects and a
%% same-NodeId reconnect from a peer that has since rebooted with
%% different capabilities would see stale state. Idempotent for
%% never-seen NodeIds.
drop_is_station_if_isolated(undefined, _NewConns, IsStation) ->
    IsStation;
drop_is_station_if_isolated(NodeId, NewConns, IsStation) ->
    case maps:find(NodeId, NewConns) of
        error -> maps:remove(NodeId, IsStation);
        {ok, #{inbound := undefined, outbound := undefined}} ->
            maps:remove(NodeId, IsStation);
        _ -> IsStation
    end.

%% SWIM holds exactly ONE `conn_pid' per member, handed to it once by
%% `apply_station_flag/4' when capability resolution completed. A mutual
%% station pair holds TWO conns, so when one direction dies the peer is not
%% isolated, is not removed, and no fresh resolution re-adds it — leaving SWIM
%% probing a DEAD pid forever. `is_pid/1' is true for a dead pid, so nothing
%% downstream noticed, and every probe timed out into a `confirmed_failed'
%% verdict about a station that was reachable the whole time.
%%
%% Same pathogen as the 2026-07-26 multihop pubsub root cause, where a stale
%% bypass recipient pid silenced every pre-existing conn totally and silently.
%%
%% So: isolated means remove, surviving means RE-POINT at the survivor.
-spec resync_swim_after_conn_loss(macula_identity:pubkey() | undefined,
                                  map(), pid() | atom(), map()) ->
          resynced | ok.
resync_swim_after_conn_loss(undefined, _NewConns, _Swim, _IsStation) ->
    ok;
resync_swim_after_conn_loss(NodeId, NewConns, Swim, IsStation) ->
    %% `primary_conn_lookup/1' already collapses "no entry" and "both slots
    %% empty" to `error', which is exactly the isolation test this used to
    %% spell out, and yields the surviving pid otherwise.
    on_surviving_conn(primary_conn_lookup(maps:find(NodeId, NewConns)),
                      NodeId, Swim,
                      maps:get(NodeId, IsStation, false)).

on_surviving_conn(error, NodeId, Swim, _IsStation) ->
    maybe_remove(NodeId, Swim);
%% ⚠ ONLY re-point a STATION. `macula_swim:add_peer/3' upserts, so calling it
%% for a daemon would ADD a member that the capability gate deliberately keeps
%% out — silently re-polluting the membership this fleet just spent a whole
%% investigation draining. A daemon that was never in SWIM stays out.
on_surviving_conn({ok, _ConnPid}, _NodeId, _Swim, false) ->
    ok;
on_surviving_conn({ok, ConnPid}, NodeId, Swim, true) ->
    ok = macula_swim:add_peer(Swim, NodeId, ConnPid),
    resynced.

%% Counted so a quiet fleet is readable. If `conn_resynced' sits at zero for
%% weeks then the asymmetric-loss regime this fix addresses never occurred in
%% production, and no amount of steady-state observation has validated it.

%% Drop in-flight forwarded entries whose origin (or advertiser, for
%% bulk-purge by either endpoint) has just gone. Cancels TTL timers
%% on the entries being dropped so we don't accumulate stale
%% `forwarded_timeout' messages in our mailbox.
drop_forwarded_for(ConnPid, F) ->
    maps:fold(fun(CallId, {Origin, TRef}, Acc) ->
        keep_or_drop(Origin =:= ConnPid, CallId, Origin, TRef, Acc)
    end, #{}, F).

keep_or_drop(true, _CallId, _Origin, TRef, Acc) ->
    _ = erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
    Acc;
keep_or_drop(false, CallId, Origin, TRef, Acc) ->
    Acc#{CallId => {Origin, TRef}}.

maybe_purge_advertise(undefined, _ConnPid) ->
    ok;
maybe_purge_advertise(R, ConnPid) ->
    macula_remote_advertise_registry:purge_conn(R, ConnPid).

maybe_remove(undefined, _Swim) -> ok;
maybe_remove(NodeId,     Swim) -> macula_swim:remove_peer(Swim, NodeId).

