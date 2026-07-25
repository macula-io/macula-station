# DESIGN: subscription lifecycle GC (the peering_router growth leak)

**Status:** DESIGN gate, pre-implementation. No code. For adversary attack.
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
