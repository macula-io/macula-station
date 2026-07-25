# DESIGN: fix the peering_router reconcile burn

**Status:** DESIGN gate, pre-implementation. No code. For adversary attack.
**Date:** 2026-07-25
**Root cause of:** the chronic fleet meltdown (boxes at 5-16 load, 2-cores, sustained),
mis-attributed for months to oversubscription and reconnect storms. Both wrong.

## 0. The evidence

Reduction profile of a live station BEAM (Falkenstein, load 16, pure station, no realm):

```
macula_station_peering_router : 1,717,488 reds/s   <- 24x the next process
  current_function = {gen, do_call, 4}   (blocked in a synchronous call)
  message_queue_len = 42
next processes (gen_statem peer-conn loops) : 53k-90k reds/s each
```

Confirmed operationally: `macula-station-kortrijk` on macula.io burned **178% CPU** (2
cores) while the co-tenant realm used 1.7%. Stopping that one station dropped the box from
load 14.25 to 0.50 and the public realm stayed up. The router IS the burn.

## 1. The mechanism (code, verified)

`sync/1` runs on a 2s tick (`?TICK_MS`, line 53) AND on every advertise/subscribe kick
(callers `Pid ! tick`; `drain_ticks/0` coalesces to one sync per mailbox pass). Each sync:

1. `local_realm_topics(Reg)` -> `safe_list_realms` + `safe_topics_for_realm` per realm
   (synchronous reads of the pubsub registry).
2. `macula_station_peer_links:connections()` (synchronous read).
3. `desired_triples/2` -> the **full cartesian product** of (realm x topic x peer).
4. `reconcile_subs/2` -> `sets:from_list(Desired)`, `maps:filter` over current subs, then a
   **synchronous `macula_station_link:subscribe/unsubscribe` gen_server:call (5s timeout)
   per added/removed triple** (lines 188, 211).

So every 2 seconds, on a singleton process, the router rebuilds an O(realms x topics x
peers) set from blocking reads and issues blocking per-triple subscribe calls for the
delta. Three independent cost sources, any of which can dominate:

- **A. Product rebuild cost.** Even with an empty delta, steps 1-3 run every tick; at large
  R x T x P this is O(N) reductions per tick for nothing.
- **B. Blocking reads/subscribes.** `gen:do_call` at sample time means the router was
  blocked in a synchronous call. One slow peer link or a loaded pubsub registry stalls the
  whole sync; the 42-deep mailbox is kicks piling up behind the stall.
- **C. Kick amplification.** Every advertise/subscribe nudges a full sync. A burst of
  daemons advertising triggers back-to-back full reconciles (coalesced, but each still
  O(N)).

A prior fix (2026-05-13, in the code comments) removed one blocking read
(`sys:get_state(peer_observer)` -> ETS mirror). The pattern was right; it was applied to
one call and not the rest.

## 2. Design principles

1. **Reconcile on change, not on a clock.** The desired set changes when a topic is
   advertised/withdrawn or a peer link comes/goes. Those are discrete events. Rebuilding
   the entire product every 2s to discover "nothing changed" is the core waste.
2. **Never block the router on a downstream.** Reads come from ETS mirrors (as conns
   already do); subscribes/unsubscribes are fire-and-forget (cast), so one slow or wedged
   peer link cannot stall the reconcile or back up the mailbox.
3. **Bound work per pass.** A reconcile touches only the delta, not R x T x P.
4. **Keep the periodic sync as a slow safety net only** (e.g. every 30-60s, not 2s), to
   catch missed events, since it is no longer the primary path.

## 3. The change (sketch, to be attacked)

- **Event-driven deltas.** On `{topic_added, Realm, Topic}` subscribe that (Realm,Topic)
  on all current peers; on `{topic_removed,...}` unsubscribe; on `{peer_up, LinkPid}`
  subscribe all current (Realm,Topic) on that one peer; on `{peer_down, LinkPid}` drop its
  subs. Each event touches O(peers) or O(topics), never the full product.
- **ETS-backed reads.** local realm/topic set and peer connections read from ETS mirrors,
  not gen_server:calls. (peer_observer already publishes a conns ETS; the pubsub registry
  needs an equivalent topic mirror, or the router maintains its own from the events.)
- **Async subscribe.** `macula_station_link:subscribe/unsubscribe` gains a cast variant, or
  the router spawns a short-lived worker per subscribe so a 5s-blocking call never stalls
  the singleton. A dropped subscribe is recovered by the slow safety-net sync (principle
  4), so at-least-once subscribe semantics are preserved.
- **Safety-net sync at 30-60s**, full reconcile, to converge anything the event stream
  missed.

## 4. Open questions for the adversary

1. Is the burn actually A (product rebuild) or B (blocking calls) or C (kicks)? The fix
   addresses all three, but if it is purely B (one wedged peer link stalling sync), the
   minimal fix is just async-subscribe + ETS reads, and the whole event-driven rewrite is
   over-engineering. Which is it, and how to confirm cheaply before building the big
   version? (Candidate: measure product size R x T x P and delta-per-tick on a live router;
   my probe read the wrong state-tuple field and returned `unknown` — needs the right
   field.)
2. Event-driven reconcile trades a simple stateless rebuild for stateful delta tracking
   that can DRIFT (miss an event, get out of sync). Is the 30-60s safety net enough, or
   does drift cause silent subscription gaps (a peer that should be subscribed isn't, so
   cross-station pubsub silently breaks for up to a minute)? Is the current
   rebuild-every-2s actually a deliberate robustness choice paying CPU for
   self-correction, i.e. is the burn the price of not drifting?
3. Async subscribe means the router no longer knows if a subscribe succeeded. Does anything
   depend on the synchronous ack? Does fire-and-forget + safety-net converge, or can a
   permanently-failing subscribe loop forever uncaught?
4. Is this even worth a rewrite vs the operational fix (stations <= cores/box, one router
   per box), given the router is a singleton whose cost scales with R x T x P regardless of
   co-tenancy? At the endgame (one station/box, but many realms/topics), does the O(RxTxP)
   reconcile still melt a single small node, making the algorithm fix mandatory rather than
   optional?
5. The 5-minute-drift risk (Q2) is the same failure class as the multi-hop self-heal bug.
   Does fixing the router the event-driven way make that bug WORSE (more moving parts that
   can silently fail to converge), and should the slow full-sync actually stay at a
   tighter interval than 30-60s as insurance?

## 5. What is already done (context)

Fire relief SHIPPED (ops, reversible): stopped the co-tenant station on macula.io, box
14.25 -> 0.50, realm serves 200. Jitter + announcer hardening committed (e8b110a), pending
a CI-flake fix and a tagged release to reach the pinned-`:5.1.0` stations. This router fix
is the foundational one; the others are real but peripheral.
