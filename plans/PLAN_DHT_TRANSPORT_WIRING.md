# PLAN — DHT Outbound Transport Wiring

**Status:** Draft, ready for implementation
**Date:** 2026-05-05
**Trigger:** Live Kademlia test from ghent against kortrijk's pubkey returned `{error, no_transport}` in 0ms. Routing-table walk returned local k-bucket contents only with `addresses => []` for every entry. The DHT module exposes the full Kademlia API (find_node, find_value, lookup_nodes, send_store, k_closest, populated_buckets) but the outbound wire transport is not connected — a single map-literal in `macula_station_app:dht_child/1` boots the DHT without `send_frame` or `identity`.

## Section 1: Current state, with line references

**Where `no_transport` originates** — `macula_dht_server.erl`:

| Function | Guard line | Frame type |
|---|---|---|
| `dispatch_ping/4`        | 507-509 | PING |
| `dispatch_find_node/5`   | 541-543 | FIND_NODE |
| `dispatch_find_value/5`  | 577-579 | FIND_VALUE |
| `dispatch_send_store/5`  | 658-660 | STORE |

All five short-circuit when `state.send_frame =:= undefined`. Inbound handlers `on_ping/3` (786), `on_find_node/3` (832), `on_find_value/3` (880), `on_store/3` (697), `persist_if_valid/4` (706-720) silently early-return when `send_frame` is missing — so even if a frame somehow arrived, the server could not reply.

**Intended outbound interface** is `macula_dht_server:send_fun/0` — `apps/macula_dht/src/macula_dht_server.erl:83-84`:

```erlang
-type send_fun() :: fun((macula_identity:pubkey(), macula_frame:frame()) ->
                              ok | {error, term()}).
```

Installed via `Opts#{send_frame => Fun, identity => Kp}` at `start_link/1` (init reads them at lines 322-323). The module docstring (lines 10-17) explicitly says "Production wires `send_frame` to `macula_peering`; the Session 3.12 CT harness wires it to direct in-VM `macula_dht_server` pid dispatch." The design is settled — only the wiring is missing.

**Where the gap is in production boot** — `apps/macula_station/src/macula_station_app.erl:431-440`. `dht_child/1` builds:

```erlang
start    => {macula_station_sup, start_dht, [#{self_id => Self}]}
```

No `identity`, no `send_frame`. That single map literal is the production-side root cause.

**Existing inbound `handle_frame/3` shape** — `macula_dht.erl:232-234` delegates to `macula_dht_server:handle_frame/3` (250-252), which casts `{frame, FromNodeId, Frame}` to itself. `dispatch_frame/3` (753-757) verifies via `macula_dht_protocol:verify/2` then routes by `macula_frame:frame_type(Frame)` (768-776) over the eight DHT frame types: `ping`, `pong`, `find_node`, `nodes`, `find_value`, `value`, `store`, `store_ack`. `replicate` and `replicate_ack` are reserved for a future custodian-handover protocol, not needed now.

**`macula_frame` is fully implemented**: every DHT frame is encoded, parsed, signed. No frame work is needed.

**Inbound dispatch gap on the station side** — `apps/macula_station/src/macula_station_peer_observer.erl:204-219`. `classify/1` returns `other` for every DHT frame; `dispatch(other, …, S) -> S` (234-235) drops them. So even after the SDK's QUIC layer hands a DHT frame to the observer, nothing routes it into `macula_dht:handle_frame/3`. This is the inbound-side fix.

**NodeId → ConnPid lookup**: already exists. `macula_station_peer_observer` keeps `conns :: #{NodeId => pid()}` (line 74), populated in `on_connected/3` (177), and exposes `conn_for/2` (106-109).

**Address propagation gap (`addresses => []`)** — three layers:
1. `macula_dht_protocol:entry_to_station_ref/2:152` hardcodes `addresses => []` for every NODES reply.
2. `macula_station_peer_observer:direct_peer_spec/1:180-185` passes `endpoints => []` because peering's `connected` notification carries only `peer_node_id`.
3. Stations learn each other's addresses from `node_record` payloads in the announcer at `macula_station_announcer.erl`. Once DHT records actually propagate, addresses are recoverable from the DHT's record store. The k-bucket-entry `addresses` field is therefore secondary.

## Section 2: Architecture decision

**Three options:**

* **(A) DHT calls peering directly.** The DHT installs a `send_frame` closure that does `macula_station_peer_observer:conn_for(Observer, NodeId)` then `macula_peering:send_frame(ConnPid, Frame)`. Pros: zero new modules. Cons: closure captures the observer pid; one_for_all restart stales the closure.

* **(B) Peering exposes a "DHT plugin".** New SDK API. Pros: DHT does not know about observer. Cons: requires a new SDK release; couples peering to DHT semantics.

* **(C) Separate adapter module `macula_station_dht_transport`.** Stateless module with `send_frame/2`, calls `whereis(macula_station_peer_observer)` per send. Pros: testable in isolation, restart-safe, single chokepoint for telemetry/logging.

**Recommendation: (C).** Reasons:

1. The DHT documents the production wiring as "an opaque callback" (`macula_dht_server.erl:83`). An adapter module is the natural home.
2. The observer can restart; capturing its pid in a closure is fragile. An adapter that calls `whereis/1` per send is restart-tolerant.
3. Inbound-side wiring also belongs in the same place: the observer needs to call `macula_dht:handle_frame/3` for every DHT frame it sees, but coordination logic wants a single owner. The adapter owns both directions.
4. Future telemetry (`_macula.dht.frame_sent`, `_macula.dht.unroutable`) lives in one module rather than smeared across observer + DHT closure.
5. Zero SDK changes — keeps macula 3.15.x pinned, no hex release coordination.

Trade-off accepted: one extra hop (DHT → adapter → observer → conn). Cost is a `whereis/1` and a `gen_server:call` to the observer per frame, both microsecond-class.

## Section 3: Frame design

**No new frames needed.** All required DHT frames already exist in macula 3.15.x's `macula_frame`:

| Frame | Constructor | Spec line |
|---|---|---|
| PING / PONG | `ping/1`, `pong/1` | 242-243 |
| FIND_NODE / NODES | `find_node/1`, `nodes/1` | 245-254 |
| FIND_VALUE / VALUE | `find_value/1`, `value/1` | 256-264 |
| STORE / STORE_ACK | `store/1`, `store_ack/1` | 266-272 |

`macula_dht_protocol` already builds all of them. Wire-format and signature scheme are unchanged.

**`replicate` / `replicate_ack`** are out of scope. The custodian replication loop (`macula_dht_replicate.erl:172`) uses **STORE**, not REPLICATE. Replicate frames are reserved for a future custodian-handover signal.

**Versioning**: this is a **wire-compatible** addition. We are not introducing new frame types; we are starting to *send and route* frames whose constructors and parsers have been compiled into every station for 3.x. Old 3.15.3 stations that receive a DHT frame from a new station will:
- Successfully decode (the SDK parser is identical).
- Fall through `peer_observer:classify/1` to `other` and drop silently. No log noise.

So **no SDK version bump required**. Mixed-fleet behaviour degrades gracefully.

## Section 4: Ordered implementation steps

Each step is a self-contained commit. "Wire-compat" means a station running this commit interoperates with a station still on `main`.

1. **Add `set_send_frame/2` and `set_identity/2` setters to `macula_dht_server`.**
   Files: `apps/macula_dht/src/macula_dht_server.erl`, `apps/macula_dht/src/macula_dht.erl`.
   Mirrors the existing `set_on_record_stored/2` (server line 303-306). Lets us install transport at runtime instead of at `start_link`. Test: eunit — start DHT without `send_frame`, assert `{error, no_transport}`, call setter, assert `ping_peer` now reaches the test stub. Wire-compat: yes.

2. **Create `macula_station_dht_transport` adapter module (plain module, NOT gen_server).**
   File (new): `apps/macula_station/src/macula_station_dht_transport.erl`.
   Stateless. Identity captured at boot via persistent_term. Exports `send_frame/2` (taking `NodeId` + already-built `Frame`). Body: looks up `macula_station_peer_observer:conn_for(NodeId)`, on `{ok, ConnPid}` calls `macula_peering:send_frame(ConnPid, Frame)`, on `error` returns `{error, no_route}` and emits a `_macula.dht.unroutable` diagnostic event. Test: eunit with a fake observer + a fake conn pid that records sends.

3. **Wire transport at boot.**
   Files: `apps/macula_station/src/macula_station_app.erl`, `apps/macula_station/src/macula_station_sup.erl`.
   Add a transport child started AFTER the observer (between `observer_child` and `listener_child` in `boot_listener/3` around line 240). Right after the transport is up, call `macula_dht:set_send_frame(DhtPid, fun(NodeId, Frame) -> macula_station_dht_transport:send_frame(NodeId, Frame) end)` and `macula_dht:set_identity(DhtPid, Kp)`. Test: CT — bring up a single-station release, observe `macula_dht:stats/1` shows `send_frame` is set. Wire-compat: yes.

4. **Route inbound DHT frames in `peer_observer`.**
   File: `apps/macula_station/src/macula_station_peer_observer.erl`.
   Extend `classify/1` (line 206-219) with a new category `dht` matching `ping | pong | find_node | nodes | find_value | value | store | store_ack`. Add a `dispatch(dht, Frame, _ConnPid, NodeId, S)` clause that calls `macula_dht:handle_frame(S#state.dht, NodeId, Frame)`. Test: eunit — feed a synthetic PING into `on_frame/3`, assert observer forwards a `{frame, FromNodeId, Frame}` cast to a fake DHT pid. Wire-compat: yes.

5. **End-to-end CT: 2-station local cluster, PING.**
   File (new): `apps/macula_station/test/macula_station_dht_ping_SUITE.erl`.
   Boot two `macula_station_server` instances, dial one to the other, wait for HyParView convergence, call `macula_dht:ping_peer(StationADhtPid, StationBNodeId)`, assert `{ok, #{rtt_ms := _}}`. Smallest end-to-end check.

6. **End-to-end CT: 3-station cluster, FIND_NODE returns the third peer.**
   Same suite. A peers with B; B peers with C; A asks B for k-closest to C's NodeId; assert C appears in the reply with at least its NodeId.

7. **End-to-end CT: 3-station cluster, STORE + FIND_VALUE.**
   Station A puts a record (custodian B is k-closest). Force `macula_dht_replicate:tick/1`. Assert `macula_dht:find_local_record(B, Key)` returns the record. Then `macula_dht:find_value(C, Key, BNodeId)` returns `{value, [Record]}`. Full wire test for the production read path.

8. **(SDK) Pass `addresses` from peering's `connected` notification.** Defer if SDK release is undesirable.
   File: SDK `_build/.../macula_peering_conn.erl` `absorb_peer_info/2` (253) + `transition_to_connected/1` (260-262).
   Current notify shape: `{macula_peering, connected, Pid, NodeId}`. Change to `{macula_peering, connected, Pid, #{node_id := NodeId, addresses := [...], station_id := ..., realms := [...]}}`. Old observers ignore the extra fields. Requires macula 3.16.0. Keep the legacy shape as a fallback.

9. **Populate `endpoints` in `direct_peer_spec/1`.**
   File: `apps/macula_station/src/macula_station_peer_observer.erl:180-185`.
   After step 8 lands, change `endpoints => []` to `endpoints => Addresses`.

10. **Use real addresses in `entry_to_station_ref`.**
    File: `apps/macula_dht/src/macula_dht_protocol.erl:142-154`.
    `addresses => []` → `addresses => macula_dht_entry:endpoints(Entry)`. NODES replies carry routable address info; secondary `addresses => []` bug goes away. Wire-compat: yes.

11. **Telemetry hooks in the adapter.**
    File: `apps/macula_station/src/macula_station_dht_transport.erl`.
    Emit `_macula.dht.frame_sent`, `_macula.dht.unroutable`, `_macula.dht.inbound_frame`. Used by realm dashboard.

12. **Live verification: re-run the ghent → kortrijk find_value test.**
    Roll the change to one station first (canary, e.g. ghent). Verify ghent can serve PING, FIND_NODE, FIND_VALUE for any peer it has connections to. Then roll fleet-wide. After the roll, the original test should return either `{value, [Record]}` (kortrijk's record made it via the replication tick) or `{nodes, [...]}` (cache miss, returns k-closest), but NOT `{error, no_transport}`.

## Section 5: Knock-on changes outside the SDK

* **macula-station release**: bump version, push to ops gitops repo.
* **macula-realm**: opportunity (not requirement) to replace the broadcast `find_records_by_type` walk with `find_value(StationPubkey)` per known station. Today realm topology relies on `_dht.find_records_by_type` over the peer set; once DHT replication actually works, every relay holds replicated records and the broadcast walk gets cheaper for free without code changes.
* **hecate-daemon**: nothing. Daemons interact with the DHT only via the `_dht.put_record` / `_dht.find_record` / `_dht.find_records_by_type` RPCs (`apps/macula_station/src/macula_station_dht_handlers.erl:74-83`), which are unchanged.
* **Address learning watchout**: today k-bucket entries have `endpoints => []` because peer_observer hardcodes it. Step 9 fixes this for direct peers; for indirectly-discovered peers (returned via NODES), addresses come through the wire from the answering station's routing table, which is now populated in step 10.

## Section 6: Risks

* **Old stations log unexpected_frame**: peering's `drop_unexpected/4` only fires for unexpected events at the gen_statem level, not for frame-type-mismatches. An old station receiving a DHT frame would parse it, hand to its observer, fall to `classify(_) -> other`, and drop silently. **No log pollution risk.** No connection drops.

* **Custodian replication burst on first tick**: once transport is wired, every station's `macula_dht_replicate` will, at its 1h tick, send STORE for every locally-held record to up to k=20 peers. With ~7 stations holding ~7 self-records each, that's ~7 × 20 = 140 STORE frames per tick fleet-wide. Trivial. At 100+ stations could spike to ~2000 STOREs per tick with churn, still negligible.

* **K-replication factor**: with K=20 and 7 nodes, every record lives on every node. With K=20 and 100 nodes, ~20% of nodes hold each record. `macula_dht_server:?DEFAULT_K` (line 77) does not need tuning. The tunable that DOES matter at scale is `macula_dht_replicate:?DEFAULT_INTERVAL_MS` (1h, line 41) — at very large fleets that 1h cadence drives steady-state STORE volume.

* **1h replicate interval is too slow for fresh fleets**: a freshly-deployed fleet would wait up to 1h before records propagate. Make the interval configurable (env var) or trigger an initial tick at boot. Recommended: env var `MACULA_DHT_REPLICATE_INTERVAL_MS`, default 3600000 (1h), drop to 60000 (1 min) for small fleets.

* **Lookup latency under partial failures**: the iterative `macula_dht_lookup` walk uses α=3 paths × per-request timeout 5s × overall 15s. With 7 nodes the walk converges in 1-2 RTTs. With 100+ nodes, factor in 3-5 RTT hops per path. Watch p95 lookup latency post-rollout.

## Section 7: Test strategy

* **Unit (eunit) on adapter** — `apps/macula_station/test/macula_station_dht_transport_tests.erl` (new). With a fake observer and fake conn pid: assert `send_frame/2` resolves NodeId → ConnPid → calls peering, returns `ok`; on missing NodeId returns `{error, no_route}` and emits a diagnostic.

* **Unit on observer routing** — extend `apps/macula_station/test/macula_station_peer_observer_tests.erl`: feed a signed PING into `on_frame/3`, assert the fake DHT pid receives `{frame, FromNodeId, _}`. Mirror for STORE.

* **Integration (CT) 3-station local cluster** — new `apps/macula_station/test/macula_station_dht_ping_SUITE.erl`:
  - Test 1: 2-station PING returns `{ok, #{rtt_ms := _}}`.
  - Test 2: 3-station FIND_NODE through one hop.
  - Test 3: 3-station put_record + replicate-tick + find_local_record on custodian.
  - Test 4: 3-station find_value end-to-end.

* **Live verification** — after canary deploy on ghent, from ghent's remsh:
  ```erlang
  KortrijkKey = <<146,26,251,140, ...>>,
  GhentSelfId = macula_dht:self_id(macula_dht),
  macula_dht:find_value(macula_dht, KortrijkKey, GhentSelfId, 5000)
  ```
  Pre-fix: `{error, no_transport}` in 0ms. Post-fix: either `{value, [Record]}` if replication has run, or `{nodes, [...]}` from the closest peer. Either result confirms the wire works. Then `macula_dht:lookup_nodes(macula_dht, KortrijkKey, #{timeout => 5000})` should return references whose `addresses` field is non-empty (after step 10 lands fleet-wide).

* **Telemetry sanity** — observe `_macula.dht.frame_sent` events flowing in the diagnostics stream after the roll.

## Critical files for implementation

- `apps/macula_dht/src/macula_dht_server.erl`
- `apps/macula_dht/src/macula_dht.erl`
- `apps/macula_station/src/macula_station_app.erl`
- `apps/macula_station/src/macula_station_peer_observer.erl`
- `apps/macula_dht/src/macula_dht_protocol.erl`
- (new) `apps/macula_station/src/macula_station_dht_transport.erl`
