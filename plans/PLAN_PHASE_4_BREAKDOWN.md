# Phase 4 — Source Routing + CALL State Machine: Session Breakdown

**Parent plan:** `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md` §9
**Spec:** `PLAN_MACULA_V2_PART3_DISCOVERY.md` §6 (source routing),
          `PLAN_MACULA_V2_PART4_LIFECYCLE.md` §6 (fast-fail + CALL state
          machine), `PLAN_MACULA_V2_PART6_PROTOCOL.md` §5 + §11 + §13
          (CALL frames, source-route header, BOLT#4 taxonomy).
**Status:** Phase 4 started 2026-04-14 — Sessions 4.1 + 4.2 shipped.

---

## Scope

Cross-relay CALLs over k=3 disjoint source-computed paths, with
BOLT#4-coded structured failures and a CALL gen_statem driving
per-transition deadlines. Out of scope for Phase 4: intra-realm
overlay (Plumtree/HyParView lands in Phase 5), bootstrap cascade
(Phase 6), Lifeguard SWIM extensions (deferred to Phase 7 hardening).

## Constants (locked)

| Name | Value | Source |
|------|-------|--------|
| `K_PATHS` | 3 | Suurballe disjoint count |
| `MAX_HOPS` | 8 | source-route header cap |
| `PATH_CACHE_TTL_MS` | 300_000 | 5 min |
| `RESOLVING_DEADLINE_MS` | 100 | RT lookup |
| `SELECTED_TARGET_DEADLINE_MS` | 500 | one Kademlia hop |
| `CONNECTING_DEADLINE_MS` | 200 | QUIC 0/1-RTT |
| `AWAITING_ACK_DEADLINE_MS` | 200 | first byte |
| `INTERACTIVE_RETRY_BUDGET` | 2 | retries |
| `INTERACTIVE_MAX_LATENCY_MS` | 5_000 | wall-clock cap |
| `BACKGROUND_RETRY_BUDGET` | 5 | retries |
| `BACKGROUND_MAX_LATENCY_MS` | 30_000 | wall-clock cap |

## Session breakdown (8 micro-sessions)

| # | Session | Deliverable | Modules |
|---|---------|-------------|---------|
| 4.1 | **BOLT#4 taxonomy + CALL frames** | 16-code catalog, CALL/RESULT/ERROR frames | SDK: `macula_bolt4`, `macula_frame:call/1`/`result/1`/`call_error/1` |
| 4.2 | Source-route header codec | 44-byte header encode/decode + path_hash | SDK: `macula_source_route` (or extension to `macula_frame`) |
| 4.3 | Routing graph + Suurballe | k=3 vertex-disjoint shortest paths over RT graph | `hecate_routing` |
| 4.4 | Path cache + invalidation | 5min TTL + SWIM-event tear-down | `hecate_routing_cache` |
| 4.5 | CALL state machine | gen_statem with 5 deadlines, BOLT#4 wiring | `hecate_call` |
| 4.6 | Per-hop relay forwarding | path_hash verify + next-hop dispatch + signed errors | server extensions to `hecate_dht_server` (or new `hecate_relay`) |
| 4.7 | Retry budget + path rotation | budget enforcement, k-rotation on retryable codes | `hecate_call` extensions |
| 4.8 | Acceptance suite + chaos | mid-path-kill reroute test, end-to-end CT | `apps/hecate_dht/test/hecate_phase4_SUITE.erl` |

## Phase 4 acceptance (from Part 7 §9.3)

- [ ] Cross-relay RPC p95 latency < 200 ms (intra-EU; relaxed to in-VM bounds for testbed).
- [ ] Failed-edge reroute p95 < 50 ms.
- [ ] V1 blocker `dist-tunnel-blocker.md` resolved: cross-relay CALL works across > 2 hops.
- [ ] CT suite green; no flakes over 10 runs.

## Session 4.2 (shipped — SDK `macula-io/macula@b90f1fb`)

**Scope:** SDK source-route header codec.

`macula_source_route` — Part 6 §11 wire format. Fixed 27-byte
overhead (1 version + 1 total_hops + 1 current_hop + 8 deadline
+ 16 path_hash) plus N × 16 bytes of truncated NodeId hops.
path_hash is the first 16 bytes of `SHA-256(concat(hops))`,
computed once at the origin and reverified on every hop.

API:
- `new/2,3` builds a header from full NodeIds (auto-truncates to
  16 bytes) plus a deadline. Validates hop count 1..8 and
  current_hop ≤ total_hops.
- `encode/1` and `decode/1` for the wire format. `decode/1` runs
  the path_hash check inline so callers can trust the structure.
- Decode error taxonomy: `bad_version | bad_total_hops |
  bad_current_hop | path_hash_mismatch | truncated`.
- `verify/1`, `advance/1`, `current_hop_id/1`, `next_hop_id/1`,
  `is_complete/1`, `is_final_hop/1`, `truncate_hop/1` cover the
  per-hop processing flow from Part 3 §6.6.

Acceptance: +25 SDK eunit (108 in macula_frame, 226 repo-wide).
Tests cover round-trip including after advance, structural
guards, tampering detection (mutate one hop byte → path_hash
mismatch), and a 3-hop traversal exercising the position helpers.

rebar3 compile xref eunit dialyzer all green.

Refs: plans/PLAN_MACULA_V2_PART6_PROTOCOL.md §11;
      plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §6.6.

## Session 4.1 (shipped — SDK `macula-io/macula@5707180`)

**Scope:** SDK foundation — error catalog and request/response envelopes.

1. `macula_bolt4` — the 16-entry error taxonomy with `code/1`,
   `name/1`, `info/1`, `table/0`, `is_retryable/1`. Codes are
   stable across V2 minor versions; `is_retryable/1` projects out
   the three non-retryable retry policies (`none`, `application`,
   `crypto_drop`) so the CALL state machine doesn't need to
   re-implement the classification.
2. `macula_frame` extended with:
   - `call/1` — CALL request envelope (call_id, procedure, realm,
     payload, deadline_ms, caller, source_route, retry_budget).
   - `result/1` — RESULT success response (call_id, payload,
     responded_by, source_route_reverse).
   - `call_error/1` — ERROR structured failure (call_id, code,
     auto-derived name, reported_by, detail, offending_hop,
     source_route_partial). Avoids `erlang:error/1` clash by
     using the `call_error/1` name.
3. `frame_type()` union extended with `call | result | error`.

Acceptance: +26 SDK eunit (12 bolt4 + 14 frame; 83 total in
macula_frame, 201 repo-wide). Round-trip name↔code for every
catalog entry; sign/verify + wire roundtrip for all three new
frame types; size guards reject malformed inputs.

rebar3 compile xref eunit dialyzer all green.

Refs: plans/PLAN_MACULA_V2_PART4_LIFECYCLE.md §6.1;
      plans/PLAN_MACULA_V2_PART6_PROTOCOL.md §5, §13.

## Process discipline

Same as Phase 3: idiomatic Erlang, low nesting, no `try/catch` for
flow control, every public function eunit-tested at minimum,
acceptance gates via `rebar3 compile xref eunit dialyzer` + a
final CT acceptance run.

## Deferred (tracked elsewhere)

- Phase 2 Lifeguard extensions → `PHASE_2_LIFEGUARD_GAPS.md`
  (still deferred to Phase 7 hardening per Phase 3 plan).
- Onion-routing-lite for hop unlinkability → Phase 9+.
- PUBLISH frame extensions → Phase 5.
