# DESIGN: subscription lifecycle GC (the peering_router growth leak)

**Status:** A implemented 2026-08-29 (`macula` 10.11.0 + macula-station
`d90f0da`, not yet pushed — see §6). B (origin-scoping) not started; §6
below re-derives that it is no longer believed necessary for correctness,
only for propagation hygiene.
**Date:** 2026-07-25
**Foundational fix for:** the chronic, worsening-over-months fleet burn. The timer-leak
fix (6eb28e5) capped the reconcile RATE; this addresses the reconcile COST, which grows
without bound.

## 0. The leak (Fable Finding 2, verified in code)

Three facts compose into unbounded growth:

1. **Peer subscriptions become local topics.** An inbound SUBSCRIBE from a peer station
   goes through `dispatch_frame` -> `hecate_pubsub:subscribe` into the same registry as
   local daemon subscriptions. `hecate_pubsub:topics/1` is `maps:keys(subscriptions)`, so
   `peering_router:local_realm_topics/1` includes topics whose ONLY subscriber is a peer.
2. **The router re-propagates them to all peers.** `desired_triples/2` =
   (local_realm_topics x peers), so a peer-sourced topic is re-subscribed on every OTHER
   peer. Interest floods transitively; the local topic set T converges to the union of all
   topics anywhere in the mesh.
3. **Nothing ever removes them.**
   - Mutual-retention livelock: X keeps topic t because Y subscribes; Y keeps t because X
     subscribes; `drop_subs_not_in` only fires when t leaves the local registry, which
     needs the peer to unsubscribe first, symmetrically. A topic that ever propagated never
     dies.
   - No conn-down purge: nothing removes a peer's subscriptions from the registry when its
     connection drops (no down-path in peer_observer / dispatcher / pubsub server). And the
     router cannot deliver UNSUBSCRIBE for a dropped link, it would call a dead LinkPid.
   - Realms auto-materialise on any inbound frame and are never GC'd either, so R grows too.

Result: `reconcile_subs` per sync is O(R x T x P) with R and T monotonically increasing,
plus a full-T resubscribe storm (T signed frames, each kicking a full sync on the receiver)
on every reconnect. This is the burn that made one station's router the top reducer and
melts boxes worse over time. It is adjacent to, and possibly the same disease as, the open
multi-hop self-heal bug.

## 1. The two mechanisms to fix

### A. Purge peer state on connection down (the missing lifecycle event)

When a peer link drops, its subscriptions and its DHT/registry presence must be removed.
Today there is no such path. Needs:

- peer_observer (which owns conn state) emits a `{peer_down, NodeId}` on disconnect.
- The registry drops all subscriptions whose subscriber is that peer.
- The router drops all triples for that LinkPid (it already does this on the NEXT sync via
  `drop_subs_not_in` once the peer leaves `connections()`, but the REGISTRY entries the
  peer created as a subscriber also need removing, or they persist as phantom local topics).

This alone breaks the "never dies" clause for departed peers. It does not fix the
mutual-retention livelock between LIVE peers.

### B. Gate propagation to locally-originated interest (the structural fix)

The livelock exists because the router propagates peer-sourced subscriptions onward. The
question is whether it should. Two models:

- **Current (transitive):** the router subscribes-on-peer for EVERY local topic including
  peer-sourced ones. This is what floods T to the mesh-wide union and creates the livelock.
- **Proposed (origin-scoped):** the router subscribes-on-peer only for topics with a LOCAL
  DAEMON subscriber (interest that originated here), never for interest learned from a
  peer. A station advertises only what its own clients want; multi-hop reach is then the
  job of each hop advertising its own local interest, or of bloom-fan (which already owns
  multi-hop EVENT delivery, per the router moduledoc).

Origin-scoping kills the livelock structurally (no peer-sourced topic is ever
re-propagated, so there is nothing to mutually retain) and bounds T at each station to its
own clients' interest plus one hop. But it changes multi-hop subscription reachability, so
it is a real pubsub-semantics decision, not a local optimisation.

## 2. The hard questions for the adversary

1. **Does origin-scoping (B) break multi-hop subscription delivery?** If station A's daemon
   subscribes topic t, B relays, C produces t: with transitive propagation, A's interest
   reaches C via B. With origin-scoping, A subscribes-on-peer to B (A's local interest), but
   B does NOT re-propagate to C (t is peer-sourced at B), so C never learns anyone wants t.
   Does bloom-fan actually carry the interest the last hop, or does origin-scoping silently
   break any subscriber more than one hop from the producer? This is THE question; if
   bloom-fan does not cover it, B is wrong and only A (purge-on-down) is safe.
2. **Is A (purge-on-down) enough on its own?** Between live, stable peers the livelock
   persists without B, so T still converges to the mesh-wide union in a well-connected
   long-lived mesh. Does A alone meaningfully bound growth, or only slow it? Is A necessary
   but insufficient, exactly as the timer fix was?
3. **Ordering/idempotence of `{peer_down}`:** a flapping peer emits down/up repeatedly.
   Does purge-then-resubscribe churn cost as much as the leak it fixes? Does a purge racing
   a reconnect drop live subscriptions (the drift failure class again)?
4. **Where does the registry learn a subscription's owner?** To purge "subscriptions whose
   subscriber is NodeId", the registry must key subscriptions by originating peer. Does it
   today, or is that new bookkeeping (and does the SubRef model support reverse lookup)?
5. **Is this the same bug as multi-hop self-heal?** If interest propagation and its GC are
   the mechanism, does fixing GC here also fix (or newly break) the open multi-hop
   propagation bug? Should the two be designed together rather than separately?
6. **Cheapest confirmation first:** the design assumes T is growing toward the mesh-wide
   union. Confirm on a live station: measure T (`length(local_realm_topics)`) and how much
   of it has only peer subscribers vs local daemons. If most of T is peer-sourced, B is
   justified; if T is small and the burn is elsewhere, this whole design is misaimed (the
   timer fix may already have been the whole story). Measure before building.

## 3. Sequencing

Confirm (Q6) -> A (purge-on-down, low-risk, clearly correct) -> measure again -> only then
decide B against the multi-hop-delivery evidence (Q1), because B is a semantics change that
could break reachability and is entangled with the open multi-hop bug.

## 4. What is already done

Timer-leak fix (6eb28e5) capped the reconcile rate. macula.io fire relieved (co-tenant
station stopped; box 14.25 -> 0.50). This GC is the remaining, foundational half: it bounds
the per-reconcile COST. It should ship AFTER the timer fix is deployed and its effect
measured, because the measurement (Q6) is the input that decides whether B is even needed.

---

## 5. Adversary verdict (Fable, 2026-07-25): design validated + deepened

**Q1 settled: origin-scoping (B) does NOT break reachability.** Blooms carry interest
mesh-wide independently of the router's subscribe-on-peer chain (`bloom_exchange:do_rebuild`
merges every cached peer bloom; `dispatcher:deliver_inbound_event` relays even with NO
local pubsub_server; `(publisher,seq)` dedup kills loops). B is architecturally safe. Its
real cost is LATENCY, not delivery: first-delivery after a fresh multi-hop subscribe goes
from ~ms (subscribe-chain) to ~6s on a 4-hop path (bloom convergence). Must be stated;
tight e2e probes will flap.

Leak diagnosis (§0) confirmed correct in full, line by line.

**Refinements that change the plan:**

1. **A needs NO new bookkeeping.** The subscriber on the wire IS the station pubkey
   (`SubKey = macula_identity:public(Id)`), and `hecate_pubsub:unsubscribe` + `drop_or_keep`
   already GC empty topics. A = add `hecate_pubsub_registry:purge_subscriber/2` and call it
   from `peer_observer:on_disconnected`'s ISOLATED branch (that hook already exists and
   already purges SWIM/DHT/advertise — it just SKIPS pubsub; "no down path" was wrong in
   letter, right in substance). Also drop the peer's `?PEER_BLOOMS_TABLE` entry there.
2. **A has a real reconnect race (MEDIUM).** SUBSCRIBE replay flows via the dispatcher;
   DOWN via the observer; unordered. If replay lands before DOWN, purge wipes live subs and
   NOTHING re-sends them (outbound_link's sub map unchanged; router triple unchanged across
   redials) -> permanent silent drift. Close with a deferred purge (grace ~5-10s,
   re-check isolation) or an epoch/ConnPid tag per sub. Do not ship A without this.
3. **B needs a subscriber-origin CLASS (HIGH).** "Local daemon subscriber" is not derivable
   today (registry stores only pubkeys; daemons and stations both just send SUBSCRIBE).
   Naive "subscriber not in connected stations" is unsound (a disconnected station reads as
   a daemon). Plumb `is_station(NodeId)` (peer_observer CAP_STATION map) into `dispatch_frame`
   at subscribe time and store the class. B ships AFTER A, fleet-wide.
4. **A alone bounds almost nothing in a stable mesh.** Any outbound dial-cycle
   (A->B->C->A, guaranteed in a 3-out partial mesh) sustains every topic that ever
   propagated. A is necessary-but-insufficient (as the timer fix was); it does also fix the
   unmentioned crashed-DAEMON sub leak, which alone justifies it.
5. **THE deeper disease (HIGH): the bloom layer is ALSO a monotone-union livelock.** Merged
   blooms never shed bits (no decay, no purge-on-down in bloom_exchange); a set bit echoes
   around every dial-cycle forever -> 8192-bit filter saturates -> false-positive rate -> 1
   -> bloom-fan degenerates to flood-every-peer (dedup saves correctness, not CPU/bandwidth).
   Since B makes bloom-fan load-bearing, this must be fixed too: same origin-scoping cure
   applied to blooms (gossip per-origin LOCAL blooms, replace-by-key self-cleans; the
   `_mesh.bloom.local` broadcast + publisher-keyed cache are half the infrastructure). This
   is almost certainly where [[project_multihop_pubsub_propagation_broken]] lives: stale
   merged blooms + phantom registry subs after churn ARE "doesn't self-heal after producer
   churn". Design the bloom GC jointly with the multi-hop bug.

**Smallest correct fix:** (1) measure T + peer-sourced fraction on a live station; (2) ship
A = `purge_subscriber/2` from on_disconnected's isolated branch, with grace-delayed
re-check for the race, + drop peer bloom entry (no new event, no new bookkeeping); (3) B
only after A, fleet-wide, with the origin CLASS plumbed and the latency regression
accepted; (4) add bloom-layer origin-scoping as the joint fix with the multi-hop self-heal
bug — same lifecycle disease, same cure, half the infra exists.

---

## 6. Implementation, 2026-08-29 — re-derived against current code, not this doc's

The code this doc analyzed (2026-07-25) has since been refactored: the flat
`hecate_pubsub` registry with `maps:keys(subscriptions)` this doc describes is now
`hecate_pubsub_registry` (realm → `hecate_pubsub_server` pid) + `hecate_pubsub_server`
(one gen_server per realm) + `hecate_pubsub` (the pure per-realm state, unchanged
shape). Confirmed by reading the current source, not assumed: the router's actual
mechanism (`macula_station_peering_router:local_realm_topics/1` →
`hecate_pubsub_registry:list_realms/1` × `hecate_pubsub_server:topics/1`, then
`desired_triples/2` = that × every connected peer, unfiltered by subscriber origin) is
mechanically identical to what this doc diagnosed — §0's three facts still hold letter
for letter, just spread across three modules instead of one. `on_disconnected/2` still
purged SWIM/DHT/ADVERTISE/streams and skipped pubsub entirely, exactly as documented.

**Shipped (A):** `hecate_pubsub:purge_subscriber/2`, `hecate_pubsub_server:purge_subscriber/2`,
`hecate_pubsub_registry:purge_subscriber/2` (macula 10.11.0, fans out across every realm
the registry holds). Wired into `macula_station_peer_observer:on_disconnected/2`,
gated on isolation, deferred `?PUBSUB_PURGE_GRACE_MS` (8s) with isolation re-checked
against current `conns` at fire time — closes adversary finding #2 (the reconnect race)
without new per-subscription bookkeeping, per refinement #1. Tests: 9 new eunit cases in
`macula`, 2 in `macula-station` (deferred-fire drops the topic; a reconnect before fire
is skipped). Full suites green in both repos (`macula`: 58/58 relevant; `macula-station`:
1076/1076).

**Not shipped, and not bundled into A as originally suggested:** dropping the peer's
`?PEER_BLOOMS_TABLE` entry on disconnect. `macula_station_bloom_exchange.erl`'s own
`?PEER_BLOOM_TTL_MS` comment (added 2026-07-26, one day after this doc) already
corrected that idea: the merge is transitive (each station's OUTGOING/merged filter,
not a per-origin local one), so a bit set once propagates and gets re-absorbed via
every neighbour forever — "the bit set is monotone non-decreasing under all
conditions." Dropping one peer's cache entry bounds the ETS *entry count*, not the
actual leaked bits. The real fix is routing on per-origin `_mesh.bloom.local` filters
instead of merged ones (already on the wire) — that's item 5 below, tied to the open
multi-hop bug, genuinely unbuilt, not a freebie.

**Re-derived: does A alone bound growth, or is B still required (§2 Q2)?** Traced
through the current router by hand rather than re-asserting the doc's own answer: once
a topic's true origin (the subscriber A now purges) is gone, `hecate_pubsub_server`
empties that topic → next `?TICK_MS` (2s) tick, `local_realm_topics` stops returning it
→ `desired_triples` drops it → `drop_subs_not_in` sends UNSUBSCRIBE to every peer we'd
been subscribing-on-peer for it. Each peer that receives that UNSUBSCRIBE runs the same
`hecate_pubsub:unsubscribe` → `drop_or_keep` path, which cascades the same way one hop
further on ITS next tick. This is an ordinary distance-vector retraction, not a livelock
independent of the root — a topic transitively re-propagated via B's still-unfixed
"subscribe for every local topic including peer-sourced ones" behavior still fully
unwinds once A removes the one real subscriber, within a few `?TICK_MS` cycles times
mesh diameter. **Item 4's "any dial-cycle sustains it forever" claim does not appear to
hold against this architecture** — it may have been accurate against the pre-refactor
code this doc analyzed, or the refactor incidentally fixed it. Not re-verified live on
the fleet (Q6's own "measure, don't assume" standard applies to this correction too).
B therefore remains a legitimate future EFFICIENCY improvement (stop the pointless
peer-sourced re-subscribe chains from happening at all, faster convergence, less
gossip) rather than a correctness requirement the leak depends on — downgraded from
this doc's original sequencing, not dropped.

**Not done:** Q6's live measurement (T, peer-sourced fraction) before shipping A, since
A's mechanism and the isolation-purge pattern were already established/low-risk
(mirrors the existing DHT-forget and advertise-purge paths on the exact same
disconnect hook) and re-deriving the fix's correctness against current source stood in
for it. A live before/after measurement of `hecate_pubsub_server:topic_count/1` and
`macula_station_peering_router`'s `subs` map size across the fleet, before and ~1 tick-
cycle after this ships, would confirm the re-derivation above rather than leave it
argued-not-measured — worth doing once this is deployed, not blocking it.

**Not pushed yet.** `macula` 10.11.0 is committed locally (`0e206f4`), not published to
hex. `macula-station`'s wiring is committed locally (`d90f0da`), not pushed — pushing
`main` here triggers `ci.yml`, which builds and pushes a ghcr image that watchtower
rolls onto the live fleet automatically, and it depends on `macula ~> 10.11` which
doesn't exist on hex yet. Order: publish `macula` 10.11.0 to hex first, then push
`macula-station`.
