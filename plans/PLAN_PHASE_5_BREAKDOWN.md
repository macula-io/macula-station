# Phase 5 — Intra-realm Overlay: Session Breakdown

**Parent plan:** `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md` §10
**Spec:** `PLAN_MACULA_V2_PART3_DISCOVERY.md` §7 (intra-realm overlay),
          `PLAN_MACULA_V2_PART6_PROTOCOL.md` §6 (PubSub frames).
**Status:** Phase 5 started 2026-04-14 — Sessions 5.1 + 5.2 + 5.3 shipped.

## Session 5.3 (shipped — SDK `macula-io/macula@041fae9`)

**Scope:** Plumtree push-lazy gossip layer (Part 3 §7.2).

**SDK side** — four new realm-scoped frames in `macula_frame`:
`plumtree_gossip` (realm + msg_id + round + payload),
`plumtree_ihave` (realm + msg_id + round),
`plumtree_graft` (realm + msg_id + round),
`plumtree_prune` (realm). +6 SDK eunit (130 in macula_frame).

**Station side** — `hecate_plumtree`. Pure functional state +
dispatcher.

State maps:
- `eager_push :: sets:set(peer())` — peers receiving full
  GOSSIP. The eager-push set IS the spanning tree.
- `lazy_push :: sets:set(peer())` — peers receiving only IHAVE
  announcements.
- `received :: #{msg_id() => payload()}` — delivered messages
  (used for dedup + GRAFT replies).
- `missing :: #{msg_id() => sets:set(peer())}` — pending
  IHAVEs awaiting payload.

API: `new/2`, `add_peer/2`, `remove_peer/2`, `publish/3` (local
publish — delivers locally, GOSSIP eager + IHAVE lazy),
`process/3` (dispatch incoming frame). All return
`{NewState, [{send, Peer, Frame}], [{MsgId, Payload}]}`.

Handlers per §7.2:
- GOSSIP first time → deliver, forward eager (excl. sender),
  IHAVE lazy (excl. sender), promote sender to eager.
- GOSSIP duplicate → PRUNE the sender + demote to lazy.
- IHAVE for unknown → GRAFT to sender (MVP eager-grafts; real
  deployment delays briefly).
- IHAVE for known → silent.
- GRAFT for known → reply with GOSSIP, promote sender to eager.
- GRAFT for unknown → silent (sender still promoted).
- PRUNE → demote sender to lazy.

Tests: +13 station eunit (359 total). Coverage: state
construction + view changes; publish emits GOSSIP per eager +
IHAVE per lazy peer; first-time GOSSIP delivers + forwards
without back-sending to sender; duplicate GOSSIP triggers
PRUNE + demotion; IHAVE for unknown emits GRAFT + records
missing; IHAVE for known is silent; GRAFT for known replies
with payload + promotes sender; PRUNE demotes; end-to-end
3-node chain delivers exactly once at each hop.

rebar3 compile xref eunit dialyzer all green on both repos.

Refs: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §7.2.

## Session 5.2 (shipped — SDK `macula-io/macula@a67d721`)

**Scope:** HyParView wire frames (SDK) + protocol orchestration
(station).

**SDK side** — six new realm-scoped frames in `macula_frame`:
`hyparview_join` (realm + new_member), `hyparview_forward_join`
(realm + new_member + ttl + arwl + prwl),
`hyparview_neighbor` (realm + priority high|low),
`hyparview_disconnect` (realm),
`hyparview_shuffle` (realm + origin + ttl + peer_sample),
`hyparview_shuffle_reply` (realm + peer_sample).

Every frame validated for size guards on the 32-byte realm and
peer_sample entries; `priority` constrained to `high | low`.
+16 SDK eunit (242 total).

**Station side** — `hecate_overlay_proto`. Pure orchestration
on top of `hecate_overlay_view`. Single dispatch entry point
`process(View, FromId, Frame, Ctx) -> {NewView, Actions}` with
handlers per Part 3 §7.1:

- JOIN: add_active(joiner) + NEIGHBOR(high) reply +
  FORWARD_JOIN(ttl=ARWL) to other actives. Active-view
  eviction emits DISCONNECT to the demoted peer.
- FORWARD_JOIN: ttl=0 OR active empty → accept locally;
  else if ttl == PRWL also add to passive, decrement ttl,
  forward to a random non-sender active.
- NEIGHBOR(high): always add_active.
- NEIGHBOR(low): add_active iff there's room; else add_passive.
- DISCONNECT: demote sender (active → passive).
- SHUFFLE: ttl > 0 → forward to random active not sender / not
  origin; else build SHUFFLE_REPLY against our own sample,
  send to origin, merge incoming sample into passive.
- SHUFFLE_REPLY: merge incoming sample into passive.

`build_join/1` + `build_shuffle/1` builders for events the local
process initiates. Defaults: ARWL=6, PRWL=3, shuffle TTL=4,
shuffle sample 3 active + 4 passive.

+13 station eunit (346 total). Tests cover every handler with
explicit view-state + actions assertions: JOIN admits sender +
forwards to other actives; FORWARD_JOIN accepts at ttl=0,
adds-to-passive at ttl=PRWL, forwards at higher ttl;
NEIGHBOR(high) evicts to admit, NEIGHBOR(low) demotes when
full; DISCONNECT demotes; SHUFFLE forwards or replies; reply
merges into passive.

rebar3 compile xref eunit dialyzer all green on both repos.

Refs: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §7.1.

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
