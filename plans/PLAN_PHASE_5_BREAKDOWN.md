# Phase 5 — Intra-realm Overlay: Session Breakdown

**Parent plan:** `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md` §10
**Spec:** `PLAN_MACULA_V2_PART3_DISCOVERY.md` §7 (intra-realm overlay),
          `PLAN_MACULA_V2_PART6_PROTOCOL.md` §6 (PubSub frames).
**Status:** Phase 5 COMPLETE — 2026-04-14. Sessions 5.1 – 5.6 shipped.

## Session 5.6 (shipped — SDK `macula-io/macula@fd45334`)

**Scope:** realm-join handshake + Phase 5 acceptance CT suite.

**SDK side** — `realm_member_endorsement` record (Part 6 §9.6) in
`macula_record`:
- Payload: `realm`, `member_node`, `roles`, `valid_from`,
  `valid_until`.
- Envelope key: RealmId (admin signs).
- Storage key: `SHA-256("member_endorsement" || RealmId || Member)`
  — distinct from the realm_directory key so the two admin-owned
  records cannot collide.
- Default validity window: 30 days via `?DEFAULT_ENDORSEMENT_TTL_MS`.

+5 SDK eunit (46 in macula_record, 262 SDK-wide).

**Station side** — two new modules.

`hecate_realm_join` — pure verifier. `verify_endorsement/3` checks:
- Record type = 0x05 (realm_member_endorsement).
- Envelope signature valid (admin signed) and not expired.
- `realm` field matches expected realm id.
- `member_node` field matches the claiming peer id (prevents stealing
  another member's endorsement).
- Current time inside the `valid_from .. valid_until` window.

Returns `{ok, Roles}` on success or `{error, Reason}` with one of
`bad_record | signature_invalid | expired | wrong_type |
wrong_realm | wrong_member | not_yet_valid | endorsement_expired`.
`build_join/4` produces a signed HYPARVIEW_JOIN frame for the joiner.

+10 station eunit (404 total).

**Phase 5 CT suite** — `hecate_phase5_SUITE` + `phase5_helper`.
The helper spawns in-VM stations, each holding `hecate_overlay_view`
+ `hecate_plumtree` + `hecate_pubsub` per realm, wired through a
synchronous router. Dispatches HyParView / Plumtree / PubSub frames
into the right module; on plumtree delivery it feeds payload into
the local pubsub state so subscribers fire.

Tests (4, 10/10 stable):
- `realm_join_admits_new_member` — seed admits joiner after admin
  endorsement verifies; joiner appears in seed's active view.
- `realm_join_rejects_bogus_endorsement` — seed drops a joiner
  whose endorsement is signed by an impostor; a follow-up with a
  real admin-signed endorsement admits.
- `plumtree_delivers_to_all_subscribers` — 5-station chain
  (a-b-c-d-e), all subscribe to `chat`, publish from a delivers
  exactly once to every subscriber via GOSSIP forwarding.
- `cross_realm_isolation` — two realms sharing identities with
  different wiring; events published in R1 never surface on R2
  subscribers (even on stations that belong to both realms).

rebar3 compile xref eunit dialyzer all green on both repos.

**Deferred to network-integrated suite** (Part 7 §10.3):
- 20-station realm convergence < 1 s
- Active-view repair after single failure < 3 s

These need real (or simulated-latency) transport; the in-VM router
delivers synchronously so timing-based acceptance bars aren't
meaningful here.

Refs: plans/PLAN_MACULA_V2_PART6_PROTOCOL.md §9.6,
      plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §7.

## Session 5.5 (shipped — SDK `macula-io/macula@f4c68f0`)

**Scope:** realm-scoped PubSub frames + dispatch state.

**SDK side** — four realm-scoped PubSub frames in `macula_frame`
per Part 6 §6:
- `publish` (topic + realm + publisher + seq + payload +
  published_at_ms + optional ttl_ms)
- `subscribe` (topic + realm + subscriber + optional filter +
  options)
- `unsubscribe` (topic + realm + subscriber)
- `event` (topic + realm + publisher + seq + payload +
  delivered_via plumtree|dht|direct)

EVENT carries the publisher's signature end-to-end so every
subscriber can verify authenticity without trusting
intermediaries; intermediaries do NOT re-sign per hop.

+9 SDK eunit (139 in macula_frame, 257 SDK-wide).

**Station side** — `hecate_pubsub` pure module holding the
topic-to-subscriber index for one realm. Cross-realm leakage is
impossible — the realm is baked into state and every dispatch
checks it.

API: `new/1`, `subscribe/3`, `unsubscribe/3`, `is_subscribed/3`,
`subscribers/2`, `topics/1`, `topic_count/1`,
`subscriber_count/1`, `deliver_event/2`, `build_event/3`,
`process/3`. Dispatch:
- SUBSCRIBE / UNSUBSCRIBE for matching realm → state mutation.
- EVENT → list of local subscribers for the topic.
- Wrong-realm frames silently ignored (defensive).

`build_event/3` produces a publisher-signed EVENT frame the
wrapper feeds into `hecate_plumtree:publish/3` for fan-out.

Acceptance: +15 station eunit (394 total). Coverage: subscribe
basics + idempotence + multi-subscriber per topic; unsubscribe
drops subscriber and removes empty-topic entries; event
delivery returns matching subscribers + ignores wrong realm;
build_event signs with publisher identity; process/3 dispatch
for all four frame types including wrong-realm rejection.

rebar3 compile xref eunit dialyzer all green on both repos.

Refs: plans/PLAN_MACULA_V2_PART6_PROTOCOL.md §6.

## Session 5.4 (shipped)

**Scope:** Observed-Remove Set CRDT (Part 3 §7.4).

`hecate_or_set` — pure data structure for realm-shared mutable
state (member lists, chat threads, directory metadata).

State:
- `data :: #{Element => sets:set(Tag)}` — currently-active tags
  per element. Element "in the set" iff its tag set is non-empty.
- `tombstones :: sets:set(Tag)` — tags removed by an observed
  `remove/2`. Suppresses delayed re-adds of removed elements.

Each `add/2` mints a fresh 16-byte random tag. `remove/2`
tombstones every currently-observed tag for the element. The
classical OR-Set property: concurrent add+remove of the same
element keeps the element (the new tag wasn't observed by the
remover).

Two convergence interfaces:
- `merge/2` — full-state union for catch-up sync. Element-wise
  tag union, subtract merged tombstones, drop elements with no
  live tags.
- `apply_delta/2` — incremental per-op delta application for
  Plumtree gossip. Idempotent. Suppresses adds whose tag is
  already tombstoned.

API: `new/0`, `add/2`, `remove/2`, `members/1`, `contains/2`,
`size/1`, `is_empty/1`, `tags_for/2`, `tombstones/1`,
`tombstone_count/1`, `merge/2`, `apply_delta/2`.

Acceptance: +20 station eunit (379 total). Coverage: empty +
add + remove basics; concurrent add+remove keeps element;
later-remove-after-observed-add drops it; readd after remove;
two-replicas-each-add then one removes keeps the other's tag;
merge is commutative + idempotent + associative; delta
application is idempotent; delayed re-broadcast of a removed
add does NOT resurrect.

rebar3 compile xref eunit dialyzer all green.

Refs: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §7.4.

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

- [ ] 20-station realm converges on add/remove in <1 s. *(deferred — network suite)*
- [ ] HyParView active-view repair after single failure <3 s. *(deferred — network suite)*
- [x] Plumtree delivers every message to every member at least once. *(CT — 5-station chain)*
- [x] No cross-realm leakage: realm-A gossip never reaches realm-B station. *(CT)*
- [x] CT suite green; no flakes over 10 runs. *(10/10)*

## Process discipline

Same as Phases 3 + 4: idiomatic Erlang, low nesting, no
`try/catch` for flow control, every public function eunit-tested
at minimum, acceptance gated via
`rebar3 compile xref eunit dialyzer` plus a final CT acceptance
run held to the no-flakes-in-10-runs bar.
