# Advertise propagation has no reconciliation, and the two-hop pair pays for it

**Status:** Root cause CONFIRMED. Reconcile fix SHIPPED 2026-08-14 (01aed36).
Honest limit below: it bounds the failure, it does not yet prevent it.
**Created:** 2026-08-13
**One line:** so a service's re-advertise reliably crosses two stations that
have no direct edge.

---

## 1. The measured defect

The two-service torture (`macula_e2e_duel`) runs across `station-fi-helsinki` ↔
`station-de-nuremberg`, the fleet's ONE core pair with no direct edge. Three
symptoms appear there and on **no** direct-edge pair (paris↔falkenstein,
helsinki↔paris, nuremberg↔falkenstein all pass 22/23):

| symptom | rate | character |
|---|---|---|
| re-advertise loses its route (`unknown_next_peer`) | 2/4 | **permanent when it fails** |
| first single publish lost | 2/4 | intermittent, one direction |
| 1 MiB content late | 4/4 | recovers on retry |

Station identity is excluded (both stations appear in a clean direct-edge run).
The only variable left is hop count.

---

## 2. Root cause — CONFIRMED in code

Advertise propagation between stations is **diff-based distance-vector with no
reconciliation.** Three facts from `macula_station_peering_router.erl`, each
verified:

**(a) It is distance-vector — each hop rewrites the advertiser to itself.**
`send_advertise_diff/4` (~line 421) builds every relayed frame with
`advertiser => SelfId`. So a procedure advertised by a daemon on nuremberg
reaches falkenstein as `advertiser = nuremberg`, and falkenstein relays it on to
helsinki as `advertiser = falkenstein`. The receiver's `Adv =:= NodeId` gate in
`macula_station_peer_observer:on_advertise_match/7` therefore passes for gossip,
and routes point one hop back toward the origin. This is why cross-hop RPC works
at all — and it means the **"Single-hop only: we don't propagate gossip we
received" comment at `macula_station_peering_router.erl:275` is stale.** The code
propagates multi-hop; only self-loops are filtered (`Adv =/= SelfId`,
`direct_remote_advertise_set/2` ~line 406).

**(b) Updates are diffs against the SENDER's memory, never the receiver's
state.** `sync_advertises/2` (~line 283) computes, per peer,
`ToAdd = LocalSet - Last` and `ToDrop = Last - LocalSet`, where `Last` is *what
this station believes it last sent that peer*. It then records
`Acc#{NodeId => LocalSet}` as the new `Last`. Nothing ever compares against what
the peer actually holds.

**(c) The periodic tick does not fix this.** There is a `timer_tick` (~line 138)
that re-runs `sync/1` → `sync_advertises/2`. But it re-diffs against the same
`Last`, so when `LocalSet` is unchanged it sends **nothing**. A periodic sync
that diffs against its own memory cannot reconcile a divergence; only a change to
`LocalSet` can, and an unrelated one at that.

### Why this produces exactly the observed failures

Once the sender's `Last` claims it sent P to a peer, but the peer does **not**
hold P — because a frame was dropped, or the peer dropped-and-tombstoned P on an
UNADVERTISE and the re-ADVERTISE raced the tombstone or the diff — the two are
**permanently inconsistent.** The sender's diff is empty (it thinks P is already
there); the peer never relearns P. Nothing resyncs until P's presence in
`LocalSet` toggles again, which for a quiet procedure is never.

- **Permanent when it fails** — no reconciliation path exists. ✔
- **Intermittent** — it needs an unlucky interleaving of async ticks, the
  cross-hop UNADVERTISE/re-ADVERTISE, and the 10 s tombstone on the intermediate.
  Most interleavings converge; some wedge. ✔
- **Multi-hop only** — a direct pair re-learns on its own link the moment either
  side changes; only the no-direct-edge pair depends entirely on *relayed*
  advertise state surviving through an intermediate. ✔

### The other two symptoms, same family (INFERRED, not yet code-traced)

First-publish-loss and content-lateness plausibly share this cause: pubsub
routing (`macula_station_bloom` / `bloom_exchange`) and DHT record placement use
the same peer-state gossip that has to be converged through the intermediate
before the first frame can route. The assessment inferred one shared cause; this
is it for advertise, and the same diff-without-reconcile shape is worth checking
in the bloom path before claiming it there.

---

## 2.5 What shipped, and its honest limit

**Shipped:** every 15th periodic tick (30s) the router re-asserts the FULL local
advertise set to each peer instead of a diff — `?RECONCILE_EVERY_TICKS`,
`advertise_to_send/3`. Chosen mechanism is Option C-as-reconcile (periodic full
re-send), NOT the digest+pull of Option A, because it needs no new wire frame, a
station on the old build understands a re-sent ADVERTISE, and it matches
`bloom_exchange` (30s full-filter rebuild) and `dht_replicate` (5-min full
re-STORE) exactly. Scope is advertise-only: the bloom/pubsub path was audited and
already reconciles, which is why bloom-carried pubsub RECOVERS after a loss.

**Effective heal time ~30s, not 60s:** the origin's reconcile re-registers the
entry on the intermediate, which is a state change there and kicks the
intermediate's router to diff-propagate onward immediately — the second hop does
not wait for its own reconcile period.

**⚠ The limit, stated plainly:** this converts *permanent* into *≤~30s*. It does
NOT make an immediate re-advertise reliable. A re-advertise that wedges still
fails for up to a reconcile period before the safety net heals it. That is the
same recovery profile bloom-carried pubsub already has (first-publish-loss
recovers on the next 30s rebuild), so advertise now matches the rest of the
mesh — but "the procedure is callable the instant you re-advertise it" is not
guaranteed, and the existing torture round (`rpc_readvertise_restores_serving`,
~12s window) will still show the wedge on the runs where it happens; a re-check
30-45s later should now find it healed.

Preventing the wedge outright needs the exact triggering interleaving pinned
(§5), which the reconcile deliberately did not require. That is the follow-up.

## 3. Fix options (for reference / the deferred drop-reconciliation)

The class is fixed by making propagation **self-healing** rather than
diff-only. Ranked.

**Option A — periodic full advertise digest + pull (recommended).**
Each station periodically sends each peer a compact digest (hash or sorted
key list) of the advertise set it believes that peer *should* hold from it.
The peer compares against what it actually holds and pulls the diff. This is
what the DHT layer already does for records (`macula_dht_replicate` — a
custodian re-STOREs the full held set every 5 min precisely because "churn
moves peers in and out and without periodic refresh a record ends up held only
by nodes no longer closest"). Advertise propagation needs the same medicine and
does not have it. Cost: one periodic digest exchange per peer; bounded by
advertise-set size, which is small.

**Option B — receiver-acked deltas.** The peer acks each ADVERTISE/UNADVERTISE;
the sender only advances `Last` on ack. Reconciles by never recording a send it
cannot confirm. Cheaper wire, but adds an ack round-trip to a hot path and still
loses if an ack is dropped without a periodic backstop — so it wants A anyway.

**Option C — drop the diff, re-send the full set every tick.** Simplest, and
what the pre-diff code did. Rejected in the current code for amplification
(`O(direct-advertises × peers)` per tick), but the advertise set is small and
the tick is slow; worth re-measuring whether the amplification is actually a
problem at fleet scale before dismissing it. It is the smallest change that
reconciles.

Recommendation: **A**, because it matches the DHT layer's already-proven pattern
and reconciles against the receiver's truth rather than the sender's memory,
which is the exact thing that is broken.

---

## 4. Before building — checkpoint

This touches the routing core, so per the repo's "one-line checkpoint before a
work package" rule this is the checkpoint, not a commit-and-go.

Open questions for Raf:
1. Option A, B or C?
2. Fix advertise only, or also audit the bloom/pubsub path for the same
   diff-without-reconcile shape (which would explain first-publish-loss too)?
3. The stale "single-hop only" comment at `peering_router.erl:275` is a live
   trap — it tells the next reader the opposite of what the code does. Correct
   it regardless of which fix ships.

---

## 4.5 Live verification — reliable, but the wedge was not reproduced

Deployed to the fleet 2026-08-14 (all 7 stations rolled to 2bdfd86). Then, on the
two-hop pair helsinki<->nuremberg:

- **4/4 full-duel runs**: `rpc_readvertise_restores_serving`, `rpc_deadline_is_enforced`
  and `torture_concurrent_calls` all GREEN — the cascade that characterised the
  pre-fix failures did not occur once.
- **11/11 isolated heal runs** (`scripts/duel-heal.sh`, 3 pre-fix + 8 post-fix):
  every one "healed after 2s".

Fifteen clean cross-hop re-advertise cycles, zero failures, against a pre-fix
baseline of 2/4.

⚠ **This is consistent with the fix, not a controlled proof of it, and I will
not claim more.** Two honest gaps:

1. **The wedge was never reproduced post-fix.** The smoking gun I wanted — a run
   reporting "healed after ~30s", showing a wedge that the reconcile then healed
   — never appeared, because no run wedged. Every re-advertise worked
   immediately. So the reconcile's healing path was never actually exercised on
   the live fleet; only its no-op steady state was.
2. **The pre-fix 2/4 was measured earlier in the session on a different SDK
   (macula 7.x) and station build.** The fleet changed a great deal since (macula
   8, the peer_observer fixes, this fix). So the improvement from 2/4 to 15/15
   cannot be attributed to the reconcile alone; other changes confound it.

The fix's correctness rests on the code, the unit tests (RED-verified), and the
two matching precedents (`bloom_exchange`, `dht_replicate`), NOT on this live
data. The live data says the mesh is healthy on it and re-advertise is reliable
now; it does not isolate the reconcile as the cause.

A clean proof would reproduce the wedge under full-suite churn with a >30s
observation window and watch it heal — an experiment that needs the wedge to
trigger on demand, which it did not here.

---

## 5. What is NOT yet done

- The exact wedging interleaving has not been caught in the act on the live
  fleet — the root cause is from code plus the measured symptom pattern, not
  from observing a diverged `Last` vs peer state directly. A reproduction that
  snapshots a station's `Last` set against a peer's actual registry during a
  wedged re-advertise would close that gap. It is not needed to justify the fix
  (reconciliation heals the whole class), but it would confirm the mechanism
  beyond doubt.
- Content-lateness may be plain two-hop latency rather than this defect; it
  "recovers on retry" where the advertise failure is permanent. Not assumed to
  be the same cause.
