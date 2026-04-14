# Phase 3 — S/Kademlia DHT: Session Breakdown

**Parent plan:** `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md` §8
**Spec:** `PLAN_MACULA_V2_PART3_DISCOVERY.md` (normative)
**Wire:** `PLAN_MACULA_V2_PART6_PROTOCOL.md` §7 (DHT frames)
**Status:** Phase 3 started 2026-04-14 — Sessions 3.1 – 3.8 green.

---

## Scope

S/Kademlia DHT with tier-diverse buckets. NOT in scope: source routing (Part 4), Plumtree (Part 5), bootstrap cascade (Part 6). DHT is self-contained behind a facade.

## Constants (locked)

| Name | Value | Source |
|------|-------|--------|
| `K` | 20 | bucket size + replica count |
| `ALPHA` | 3 | lookup concurrency per path |
| `D` | 3 | disjoint paths (S/Kademlia) |
| `S` | 16 | sibling list size |
| `BUCKET_COUNT` | 256 | one per NodeId-bit |
| `T_REPLICATE_MS` | 3_600_000 | 1h |
| `T_REPUBLISH_MS` | 86_400_000 | 24h |
| `T_EXPIRE_MS` | 172_800_000 | 48h |
| `QUORUM_WRITES` | 16 | of K replicas |
| `DIVERSITY_ASN_MIN` | 5 | per full bucket |
| `DIVERSITY_COUNTRY_MIN` | 3 | per full bucket |
| `DIVERSITY_TIER_MIN` | 2 | per bucket ≥ 8 entries |
| `REPLICA_ASN_MIN` | 8 | per stored record |
| `REPLICA_COUNTRY_MIN` | 5 | per stored record |
| `REPLICA_TIER_MIN` | 3 | per stored record |

All centralised in `hecate_dht_const.hrl` when needed.

## Decisions taken at Phase 3 start

- **O1 (crypto-puzzle difficulty):** hardcode `16` for now. `foundation_parameter` records accepted but not acted on. Adaptive bumping deferred post-V2.0.
- **Simulation strategy:** Common Test harness — N synthetic `hecate_dht` instances in one BEAM VM, frame-delivery via `erlang:send/2`. QUIC path tested separately.
- **Persistence:** routing table reconstructs from node-record DHT on boot; no local table snapshot in Phase 3.
- **Storage backend:** ETS per-process (owner = `hecate_dht_server`). No DETS, no mnesia. Pillar 1 (process-resource binding).

## Session breakdown (12 micro-sessions)

| # | Session | Deliverable | Modules |
|---|---------|-------------|---------|
| 3.1 | **Pure foundation** | XOR metric, entry record, diversity scoring, bucket primitive | `hecate_dht_xor`, `hecate_dht_entry`, `hecate_dht_diversity`, `hecate_dht_bucket` |
| 3.2 | Routing table | 256 buckets, admission + eviction, sibling list | `hecate_dht_routing_table`, `hecate_dht_siblings` |
| 3.3 | Server + API | `hecate_dht_server` (gen_server), `hecate_dht` facade, supervisor | `hecate_dht_sup`, `hecate_dht_server`, `hecate_dht` |
| 3.4 | DHT frames in SDK | Extend `macula_frame` with `ping/pong/find_node/nodes/find_value/value/store/store_ack/replicate/replicate_ack` | (macula repo) |
| 3.5 | PING/PONG + FIND_NODE request-response | First wire-level DHT op; uses SWIM-independent liveness | `hecate_dht_server`, `hecate_dht_protocol` |
| 3.6 | Lookup — disjoint paths | d=3 disjoint peers per iteration, α=3 parallel, termination rule | `hecate_dht_lookup` |
| 3.7 | FIND_VALUE + record types | Extend `macula_record` with realm_directory, realm_stations, procedure_advertisement | (macula repo) + `hecate_dht_server` |
| 3.8 | STORE + quorum + placement | Diversity-constrained replica placement (k=20, ≥16 acks) | `hecate_dht_store`, `hecate_dht_placement` |
| 3.9 | tReplicate custodian loop | 1h timer, re-STORE to current k-closest | `hecate_dht_replicate` |
| 3.10 | tRepublish owner loop + tExpire reaper | 24h owner republish, 48h custodian expiry + tombstone | `hecate_dht_republish`, `hecate_dht_expire` |
| 3.11 | Observability | Bucket-diversity + replica-diversity telemetry via `macula_diagnostics` | `hecate_dht_monitor` |
| 3.12 | Acceptance suite | CT suite: N=100 synthetic stations, accelerated clock, convergence + diversity + republish cycles | `test/hecate_dht_SUITE.erl` |

Each session ends green: `rebar3 compile xref eunit ct dialyzer` all pass, commit pushed with audit-trail message.

## Phase 3 acceptance (unchanged from Part 7 §8.3)

- [ ] Lookup success rate > 99.5% at N=100
- [ ] Bucket diversity ≥ 5 ASN / ≥ 3 country satisfied for > 95% of buckets with adequate population
- [ ] Replica placement satisfies ≥ 8 ASN / ≥ 5 country / ≥ 3 tier constraints
- [ ] tRepublish + tReplicate cycles observed over simulated 48h (accelerated)
- [ ] All modules dialyzer clean, no `{nowarn, ...}`, no `try/catch` in hot paths
- [ ] CT suite green; no flakes over 10 runs

## Session 3.1 (shipped — commit `8e9d383`)

**Scope:** four pure modules — no processes, no sockets. Everything testable by eunit alone.

1. `hecate_dht_xor` — XOR distance, common-prefix-bits, bucket-index, closer comparator
2. `hecate_dht_entry` — routing-table entry record + constructors/accessors/touch
3. `hecate_dht_diversity` — ASN/country/tier counting, constraint checker, novelty scorer
4. `hecate_dht_bucket` — ordered list of entries, admission with scoring eviction (k=20)

Acceptance: 68 eunit green, xref + dialyzer clean, no module exposes mutable state.

## Session 3.8 (shipped)

**Scope:** STORE + quorum + diversity-constrained replica placement.

1. `hecate_dht_placement` — pure algorithm (no SDK changes needed;
   STORE/STORE_ACK frames were added in 3.4). Greedy closest-first
   with forward-looking diversity admission: a candidate is rejected
   only if accepting it would leave too few slots to cover the
   maximum remaining diversity deficit. Operator cap (≤3 per ASN)
   is a hard constraint. If Phase 1 under-fills K, Phase 2 tops up
   with closest unchosen candidates (op-cap only) and the result is
   returned with `degraded = true`. Default constraints match Part
   3 §5.2: K=20, ≥8 ASN / ≥5 country / ≥3 tier / ≤3 per operator.

2. `hecate_dht_store` — blocking orchestrator running in the
   caller's process. Resolves candidates (explicit `candidates'
   opt OR `k_closest/3` on the local routing table), calls
   placement, spawns one unlinked worker per chosen peer. Each
   worker calls `hecate_dht:send_store/4` and reports back. Collect
   waits for all workers or the overall deadline — no early
   termination (a ref-tag on messages isolates calls so eunit's
   reused runner pid doesn't cross-contaminate tests). Returns
   `{ok, Outcome}` on quorum (≥16 acks by default) or
   `{error, quorum_not_met | no_candidates, Outcome}` otherwise.

3. `hecate_dht_server` wire extensions:
   - `send_store/3,4` RPC with `pending_stores` correlation
     (keyed on `{PeerId, StorageKey}`).
   - Incoming STORE → verify record signature, put on success,
     always reply STORE_ACK (stored=true OR stored=false with an
     atom reason on verify failure).
   - Incoming STORE_ACK → correlate, reply `{ok, #{stored, reason}}`.

4. `hecate_dht_protocol`: `build_store/2`, `build_store_ack/4`.

5. `hecate_dht` facade: `send_store/3,4`, `store/2,3`.

Acceptance: +15 eunit (total 209). Two new modules' tests:
- `hecate_dht_placement_tests` — empty pool, K-cap, operator cap,
  diversity satisfied, forward-looking preference for diverse
  tail candidates, dedup, degraded when unreachable.
- `hecate_dht_store_tests` — single-peer wire path (persist +
  return `{ok, stored: true}`), no-transport error, timeout,
  orchestrated multi-custodian store with quorum met, quorum-not-met
  on silent custodian, degraded flag propagation, no-candidates
  error.

rebar3 compile xref eunit dialyzer all green.

Refs: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §5.2, §5.6, §10.4.

## Session 3.7 (shipped — SDK `macula-io/macula@1399192`)

**Scope:** FIND_VALUE + record types.

**SDK side:**
1. `macula_record` gains three constructors — `realm_directory/3,4`
   (type tag 0x03, owner = RealmId), `realm_stations/2,3` (type tag
   0x04, owner = RealmId, stored at `SHA-256("station_set"||RealmId)`),
   `procedure_advertisement/3,4` (type tag 0x06, owner = advertiser
   NodeId, stored at `SHA-256(procedure_uri)`).
2. `storage_key/1` helper — given any record, returns the 32-byte
   DHT storage key per Part 3 §3.3 (envelope key for node_record /
   realm_directory / tombstone; derived hash for realm_stations and
   procedure_advertisement).

**Station side:**
1. ETS record store (`bag` keyed on storage_key, value = record())
   owned by `hecate_dht_server`. `put` preserves records from other
   envelope-owners at the same storage key (crucial for
   procedure_advertisement where many advertisers share one URI
   hash) and replaces the same-owner's prior record so versions
   don't accumulate.
2. New API: `put_record/2`, `find_local_record/2`, `record_count/1`,
   `find_value/3,4` (wire RPC with `{value, Records} | {nodes, Refs}
    | {error, timeout | no_transport | term()}` result).
3. Protocol builders: `build_find_value/3`, `build_value_reply/3`.
4. Incoming FIND_VALUE handler — local-store hit → VALUE reply;
   miss → NODES reply (k-closest from RT) per Part 3 §4.5.
5. Incoming VALUE handler — correlate against a new
   `pending_find_values` table, reply `{value, Records}`.
6. Rewritten incoming NODES handler — match `pending_find_values`
   first (FIND_VALUE fallback case) then `pending_find_nodes`
   (standard FIND_NODE RPC).

Acceptance: +22 eunit (total 194 across the app + 69 in macula_record).
rebar3 compile xref eunit dialyzer all green on both repos.

Refs: plans/PLAN_MACULA_V2_PART6_PROTOCOL.md §9.4/§9.5/§9.7, §7.3;
      plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §3.3/§4.5/§5.1/§5.3.

## Session 3.6 (shipped)

**Scope:** S/Kademlia iterative FIND_NODE lookup with d=3 disjoint
paths.

1. `hecate_dht_lookup` — blocking orchestrator in the caller's
   process. Maintains a `state()` map with `d` paths, each path a
   `#{id, shortlist, queried, in_flight}`. `claimed` set sits at the
   state level and enforces peer-level disjointness: a peer
   admitted to one path's shortlist cannot enter another's. Workers
   are short-lived unlinked procs that block on `hecate_dht:find_node/4`
   and send `{lookup_result, PathId, PeerId, Result}' back to the
   orchestrator. Termination fires when every path has `in_flight = ∅`
   AND every shortlist member has been queried, or when the overall
   deadline expires. Final result is top-N across all shortlists,
   deduped by NodeId and sorted by ascending XOR distance.
2. `hecate_dht` facade — `lookup_nodes/2,3` delegates.

Acceptance: +7 eunit (total 172). Tests include:
- Empty-RT → `{ok, []}`.
- Single-hop — A knows B with 3 peers; lookup returns those peers.
- Multi-hop chain A→B→C→D with only one-peer-per-hop — lookup
  from A for D's NodeId reaches D and returns D as the closest
  (distance 0).
- Target-count capping.
- Dedup + distance-sort ordering when paths overlap.
- Overall-timeout clean termination with partial result.
- Disjointness — with two paths and both B+C knowing D, D is
  queried exactly once (counted at a stat-tracking router).

rebar3 compile xref eunit dialyzer all green.

Refs: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §4.5.

## Session 3.5 (shipped)

**Scope:** first wire-level DHT op — PING/PONG + FIND_NODE/NODES
request-response machinery inside the DHT server.

1. `hecate_dht_protocol` — pure frame-builder module. `build_ping/1`
   returns `{Frame, Nonce}`; `build_pong/2` echoes the nonce;
   `build_find_node/4` and `build_nodes_reply/3` wrap the
   Session-3.4 `macula_frame` constructors; `verify/2` is a thin
   `macula_frame:verify/2` delegate; `entry_to_station_ref/1,2`
   converts a routing-table entry to the wire payload (atom tier →
   int, monotonic last_seen → wall-clock ms).
2. `hecate_dht_server` extended with:
   - `identity` (key pair for signing outgoing) and pluggable
     `send_frame/2` callback — transport-agnostic, so the
     Session-3.12 CT harness wires one callback and production
     wires a `macula_peering` callback with identical behaviour.
   - `pending_pings :: #{Nonce => {TargetId, From, TimerRef,
     StartedMono}}` and `pending_find_nodes :: #{{PeerId, Key} =>
     {From, TimerRef}}` — per-request correlation.
   - `ping_peer/2,3` and `find_node/3,4` — synchronous-to-caller,
     async internally: call, cast outgoing frame, park `From`
     until matching response arrives (or timer fires).
   - `handle_frame/3` — cast entry point for incoming frames;
     signature-verifies then dispatches to the four type handlers.
     Unsigned / tampered / wrong-peer frames drop silently.
   - Successful PONG triggers `routing_table:touch/3` +
     `siblings:touch/3` on the sender in addition to replying to
     the caller with RTT in ms.
3. `hecate_dht` facade — mirrors the new server API.

Acceptance: +15 eunit (total 165 across the app). Includes a
two-server wire harness with a dispatch-router pid that routes
frames between gen_server instances, plus a tampering-router test
that proves signature verification drops mutated frames.

rebar3 compile xref eunit dialyzer all green.

Refs: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §4.5, §10.1;
      plans/PLAN_MACULA_V2_PART6_PROTOCOL.md §7.1, §7.2.

## Session 3.4 (shipped — macula-io/macula commit `92729aa`)

**Scope:** cross-repo — extend `macula_frame` (SDK) with Part 6 §7
DHT frame types.

1. **ping / pong** — 16-byte nonce-matched pairs.
2. **find_node / nodes** — iterative lookup; `nodes` carries a list of
   validated `station_ref()` entries (node_id + station_id + tier +
   country + ASN + addresses + last_seen_at).
3. **find_value / value** — key-by-key record retrieval; `value`
   returns a list of signed pkarr records.
4. **store / store_ack** — primary write path; ack carries a boolean
   `stored` plus optional atom `reason` (`quota | invalid_sig | ...`).
5. **replicate / replicate_ack** — custody handover (Part 3 §5.5); the
   `new_custodian` boolean signals a join-time takeover.
6. **station_ref/1** — validated helper for the NODES payload.

All frames use the existing `base/2` header (`capabilities => 0`),
`sign/verify` and `encode/decode` paths unchanged. +29 eunit tests
(total 57 for macula_frame, 155 repo-wide); xref + dialyzer clean.

Downstream: `hecate-station` recompiles cleanly against the new SDK
commit; all 150 station eunit tests still green.

## Session 3.3 (shipped)

**Scope:** first stateful session — pid-scoped DHT server + facade + supervisor.

1. `hecate_dht_server` — `gen_server` owning a `hecate_dht_routing_table`
   and a `hecate_dht_siblings` for a single station's `SelfId`. `observe/2`
   offers the peer to both containers (RT admission is scored, sibling
   admission is pure distance). Reports RT outcome as `admitted | touched |
   {replaced, Evicted} | rejected`. `touch/2` and `forget/2` are cast-based
   and hit both containers. `stats/1` aggregates sizes.
2. `hecate_dht` — thin public facade delegating every call to the server.
   Only module external callers should use. Supports multiple instances
   per BEAM VM (pid-scoped, no registered name).
3. `hecate_dht_sup` — supervisor with `hecate_dht_server` as its single
   child. `start_link/1` takes the opts map and forwards it to the child;
   `get_server/1` returns the running server pid. Room for ETS record
   store (3.7), custodian timers (3.9/3.10), observability (3.11).

Acceptance: +39 eunit tests (total 150 across app), xref + dialyzer clean.
One supervisor-restart test exercises the child's permanent restart policy.

## Session 3.2 (shipped)

**Scope:** two pure modules on top of 3.1 — still no processes, no sockets.

1. `hecate_dht_routing_table` — 256-slot sparse map of buckets addressed by
   `bucket_index/2`. Dispatches insert/touch/remove/find to the right bucket,
   collapses buckets when they drain to zero entries. Self-insert rejected
   (bucket_index = -1). `k_closest/3` flattens + keysort over all entries.
2. `hecate_dht_siblings` — S=16 bounded sorted set of peers by ascending
   XOR distance to self. Pure-distance admission (not scored). Self-insert
   rejected. Closer-than-farthest replaces; farther-than-farthest rejects.

Acceptance: +43 eunit tests (total 111 across app), xref + dialyzer clean.
Both modules pure values — only construction, inspection, and immutable
`{admitted | touched | replaced | rejected, new_state()}` transitions.

## Deferred (tracked elsewhere)

- Phase 2 Lifeguard extensions → `PHASE_2_LIFEGUARD_GAPS.md`
- Adaptive puzzle difficulty (O1) → post-V2.0
- Foundation parameter record publication → Phase 6
- Full Phase 3 record-type catalog (9 of 17 types) → Phases 3.7 / 4 / 5 / 6

## Process discipline

- **Idiomatic Erlang:** pattern-match on function clauses; avoid `if`/`case` except in true multi-way decisions; never `try/catch` for flow control.
- **Low nesting:** fold/map/filter over explicit recursion; max 1 `case` level per function.
- **Declarative:** functions describe *what*, not *how*. Accessors over field probes.
- **No orphaned state:** every ETS table owned by a named process; purity everywhere it can live.
- **Tests first-class:** every public function has at least one eunit test; edge cases (empty input, max input, boundary values) explicit.
