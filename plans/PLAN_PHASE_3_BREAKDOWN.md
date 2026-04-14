# Phase 3 — S/Kademlia DHT: Session Breakdown

**Parent plan:** `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md` §8
**Spec:** `PLAN_MACULA_V2_PART3_DISCOVERY.md` (normative)
**Wire:** `PLAN_MACULA_V2_PART6_PROTOCOL.md` §7 (DHT frames)
**Status:** Phase 3 started 2026-04-14 — Sessions 3.1 + 3.2 green.

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
