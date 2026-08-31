# DESIGN: fix the peering_router reconcile burn

**Status:** A, B, C implemented 2026-08-31 (commits `26d012a`, and the one
immediately after it — see §6). Not adversary-reviewed before shipping (see
§6 for why, and what that means for confidence in this fix).
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

## 6. Implementation, 2026-08-31 — NOT adversary-reviewed first, read this before trusting it

Found this doc mid-session while investigating a DIFFERENT question (mesh topic-cardinality
scaling, asked by the user) that turned out to be the same disease this doc already
root-caused. Built the fix organically from re-deriving the mechanism against current
source, not from re-reading this doc's own sketch first — the two ended up close but not
identical. Flagging explicitly: this doc says **"For adversary attack"** and none happened.
Shipped anyway on an explicit user go-ahead after a mid-session checkpoint, not because the
review step stopped mattering.

**A (product rebuild every 2s even with an empty delta): fixed, commit `26d012a`.**
Every production path that changes either half of the desired set now kicks the router
directly (`Pid ! tick`) instead of relying on the 2s poll to notice — see that module's own
moduledoc for the full list. The periodic tick now runs a real sync only on the first tick
after boot and on the existing `?RECONCILE_EVERY_TICKS` safety net; every other periodic
tick is a no-op. Verified: `should_periodic_sync/2` is a pure, directly-tested decision
function; full station suite 1104/1104, elvis clean, dialyzer clean at that commit.

**B (blocking calls): fixed for subscribe/unsubscribe specifically, NOT for reads via the
sketched ETS-mirror approach.** `subscribe_one/5` now spawns a throwaway worker per call
(this doc's own "OR" alternative to a cast variant on `macula_station_link`, chosen because
it needs no SDK-repo API change) instead of blocking the router in a synchronous 5s-timeout
`gen_server:call` — matches the incident's own captured evidence (`current_function =
{gen, do_call, 4}`) more directly than any other single change here. `local_realm_topics/1`'s
own reads (`list_realms`/`lookup`/`topics`) still block, but now on an explicit 500ms
timeout instead of the bare call's 5s default — bounds the worst case without the new
ETS-mirror plumbing this doc sketches (§3, second bullet), which needs new mirror/writer
code in the `macula` SDK repo itself and was deliberately left out of this pass as a
materially bigger, cross-repo undertaking.

**C (kick amplification): fixed with a 25ms leading-edge debounce**, not the full
event-driven per-topic delta tracking this doc's §3 sketches (each kick still triggers a
whole-set resync, just at most once per 25ms window instead of once per kick) — same
shape `macula_station_bloom_exchange` already uses for its own local-change debounce,
at a much tighter window given this router's own stricter latency requirements.

**Open questions from §4 this pass does NOT answer:**
- Q1 (is the burn actually A, B, or C) — never measured on the live fleet before or after;
  this pass fixed all three roughly as this doc sketched, so the question of which one
  actually dominated the historical 178% CPU incident remains unconfirmed.
- Q2/Q3 (does async subscribe's fire-and-forget model risk permanent drift or a
  never-resolving `pending` entry) — addressed at the design level (`apply_sub_result/3`
  distinguishes "still pending, still wanted" from three stale cases, and the 30s reconcile
  is the safety net for anything it can't resolve on its own), not verified against a real
  wedged-peer-link scenario or a genuinely lost worker (e.g. brutally killed before it can
  report back, which would leave that one `Key` stuck `pending` until the process
  restarts — a real, narrow, deliberately-accepted residual risk, not a scenario this pass
  built a timeout/retry mechanism for).
- Q4/Q5 (rewrite vs. operational fix; interaction with the open multi-hop self-heal bug) —
  not revisited.

**Not measured**: no before/after live-fleet CPU/reduction profile, the same gap
`DESIGN_SUBSCRIPTION_LIFECYCLE_GC.md` §6 already flagged for its own A. Worth doing once
this is deployed and observed for a while, same as that doc recommends for its own fix.
