# Phase 5 — Intra-realm Overlay: Session Breakdown

**Parent plan:** `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md` §10
**Spec:** `PLAN_MACULA_V2_PART3_DISCOVERY.md` §7 (intra-realm overlay),
          `PLAN_MACULA_V2_PART6_PROTOCOL.md` §6 (PubSub frames).
**Status:** Phase 5 started 2026-04-14 — Session 5.1 shipped.

## Session 5.1 (shipped)

**Scope:** HyParView active + passive view data structure
(Part 3 §7.1).

`hecate_overlay_view` — pure functional bounded views:
- `new/1,2` with `active_cap` (default 5) + `passive_cap`
  (default 4×active). Constructors validate caps ≥ 1 and
  passive ≥ active.
- `add_active/2` — add to active. Self / already-active are
  no-ops; passive peer gets promoted; full active demotes a
  random member to passive before inserting.
- `add_passive/2` — add to passive. Self / already-known
  no-ops; full passive evicts a random member.
- `promote/2` — passive → active (with eviction).
- `demote/2` — active → passive.
- `remove_active/2`, `remove_passive/2`.
- `random_active/1` (`{ok, P} | empty`),
  `random_active_subset/2`, `random_passive_subset/2`.
- `merge_shuffle/2` — merge incoming SHUFFLE_REPLY peers into
  passive view, filtering self / already-known.
- `counts/1` for telemetry.

Module is pure: every operation returns a new view. Wire-protocol
orchestration (JOIN / FORWARD_JOIN / NEIGHBOR / SHUFFLE /
SHUFFLE_REPLY) lands in 5.2 on top of this.

Acceptance: +25 eunit (total 333). Tests cover construction
defaults + cap guards, idempotent active+passive adds, self
filtering, capacity-driven eviction (active→passive demotion),
promotion both via explicit `promote/2` and via `add_active/2`
on a passive peer, demotion, removal, sampling (one + subset
+ empty), shuffle merge with self/known filtering, and counts
helper.

rebar3 compile xref eunit dialyzer all green.

Refs: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §7.1.

---

## Scope

Realm-scoped gossip mesh layered on top of the DHT. HyParView
maintains per-realm partial views (active + passive); Plumtree
disseminates messages over the active-view tree with lazy-push
recovery; OR-Set CRDTs converge realm-shared mutable state;
realm-scoped PubSub fans out via Plumtree. Realm-join handshake
uses the `realm_member_endorsement` records the SDK already
supports.

## Constants (locked, from Part 3 §7)

| Name | Value | Source |
|------|-------|--------|
| `ACTIVE_VIEW_DEFAULT` | `max(5, ceil(log₂(realm_size)))` capped at 15 | §7.1 |
| `PASSIVE_VIEW_RATIO` | 4 × active size | §7.1 |
| `SHUFFLE_INTERVAL_MS` | 30_000 (30 s) | §7.1 |
| `ACTIVE_REPAIR_RETRIES` | 3 | §7.1 |
| `PLUMTREE_TREE_REPAIR_TIMEOUT_MS` | TBD per Part 7 §10 | §7.2 |

## Session breakdown (6 micro-sessions)

| # | Session | Deliverable | Modules |
|---|---------|-------------|---------|
| 5.1 | **HyParView views** | Pure active + passive view data structure | `hecate_overlay_view` |
| 5.2 | HyParView protocol | JOIN / FORWARD_JOIN / NEIGHBOR / SHUFFLE / SHUFFLE_REPLY frames + view orchestration | SDK frame extensions + `hecate_overlay_proto` |
| 5.3 | Plumtree | eager + lazy push, IHAVE / GRAFT / PRUNE, tree repair | `hecate_plumtree` |
| 5.4 | OR-Set CRDT | add/remove convergence + delta-state compaction | `hecate_or_set` |
| 5.5 | Realm PubSub | PUBLISH / SUBSCRIBE / EVENT frames + realm-scoped dispatch | SDK frames + `hecate_pubsub` |
| 5.6 | Realm-join + acceptance | join handshake using `realm_member_endorsement`; CT acceptance suite | `hecate_realm_join` + `hecate_phase5_SUITE.erl` |

## Phase 5 acceptance (from Part 7 §10.3)

- [ ] 20-station realm converges on add/remove in <1 s.
- [ ] HyParView active-view repair after single failure <3 s.
- [ ] Plumtree delivers every message to every member at least once.
- [ ] No cross-realm leakage: realm-A gossip never reaches realm-B station.
- [ ] CT suite green; no flakes over 10 runs.

## Process discipline

Same as Phases 3 + 4: idiomatic Erlang, low nesting, no
`try/catch` for flow control, every public function eunit-tested
at minimum, acceptance gated via
`rebar3 compile xref eunit dialyzer` plus a final CT acceptance
run held to the no-flakes-in-10-runs bar.
