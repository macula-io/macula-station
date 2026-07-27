# Phase 2 Lifeguard Gaps (deferred follow-up)

**Phase 2 shipped 2026-04-14** with classic direct-probe SWIM — sufficient to detect confirmed failures within ~8 s for the 3-station walking skeleton. The four Lifeguard extensions below are deferred; they are not blockers for Phase 3 (DHT is independent of SWIM depth) but must land before real-WAN deployment to tame false positives under packet loss and congestion.

**Reference:** `PLAN_MACULA_V2_PART4_LIFECYCLE.md` §5 (SWIM + Lifeguard), DSN 2002 + arXiv 2017.

| # | Extension | Purpose | Trigger for implementation |
|---|-----------|---------|----------------------------|
| L1 | **Indirect-ping via K buddies** | Direct PING timeout → ask `k=3` random alive members to ping target; ACK via any of them refutes. Kills false positives under localised packet loss. | Before exposing SWIM to networks with > 1 % loss. |
| L2 | **Self-awareness multiplier** | If local node suspects > T % of its view in one period, it assumes its own uplink is congested and multiplies its suspicion-timeout by `1 + local_health`. Prevents false-positive cascades. | Before running on mobile / residential links. |
| L3 | **Refutation-buddy (NACK relay)** | Suspected peer can relay its "I'm alive" via a buddy when its direct path to the suspector is lossy. | With L1 — they share the buddy-selection path. |
| L4 | **NACK (explicit refutation)** | Buddy that confirms target alive sends explicit NACK to the suspector, short-circuiting the suspect-timer without waiting for the next probe. | With L1 + L3. |

## Shipped 2026-07-27 — not one of L1–L4, but same family

**Refute on any verified message from a suspect** (`macula_swim:refute_if_suspect/2`).

`on_ping_timeout/3` reaps the pending round before the ACK can arrive, so the
old `on_ack/3` fall-through (`_ -> S`) silently discarded a verified, signed,
freshly-arrived ACK from the very peer it had just suspected. That threw away
the one frame proving the peer was alive.

This matters more than it looks, because a suspect is otherwise **unreachable
by design**: `pick_alive_target/1` selects only `alive` members, so we never
re-probe it. The sole remaining rescue was the suspect happening to pick *us*
out of its own membership, which is `(1 - 1/M)^3` and therefore RISES with mesh
size — 30% at M=3, 73% at M=10, 93% at M=43. Measured against the model to
within binomial noise in `macula_swim_conversion_tests`.

So this is the cheap part of what L1/L3/L4 buy: a rescue channel proportional
to real traffic rather than to luck. It does NOT replace them. L1 (indirect
ping) is still the only thing that helps when the direct path is lossy in both
directions, and L4's explicit NACK still short-circuits the suspect timer
rather than waiting for traffic that may never come.

A `confirmed_failed` member is deliberately NOT resurrected by this rule: that
verdict has already been published to the consumer. ⚠ That makes it a one-way
door — nothing currently re-admits a still-connected peer that was wrongly
confirmed, so it stays confirmed until the next restart. Open.

**Also shipped, and it changes L1's priority:** the false-confirm rate this was
tuned against was dominated by two bugs, not by network loss. SWIM held ONE
`conn_pid` per member, handed over once, and `is_pid/1` is true for a dead pid,
so a mutual pair losing one direction left SWIM probing a corpse forever. With
that fixed (`resync_swim_after_conn_loss/4` in the observer) plus the
capability gate excluding daemons, the fleet's steady-state suspicion rate went
from ~15/hour to ~0/hour. **Re-derive the L1 trigger from post-fix telemetry
before implementing it** — the ">1% loss" threshold was set against a
population that no longer exists.

## Why deferred

- Phase 2 acceptance test (3 stations, LAN, confirmed\_failed within 8 s) passes *without* Lifeguard extensions.
- Lifeguard tuning is data-driven (T %, buddy-set size `k`, timeout multiplier bounds) and wants real-network telemetry before calibration.
- DHT (Phase 3) depends only on SWIM *membership-state events*, not on Lifeguard quality.

## Non-goals during Phase 3

- Do **not** fold Lifeguard into DHT liveness — the DHT has its own `PING/PONG` layer (Part 6 §7.1).
- Do **not** remove the Phase 2 direct probes when L1–L4 land; Lifeguard extends the classic protocol, it does not replace it.

## Trigger

"Resume Phase 2 — Lifeguard extensions" — picks up L1 first, then L3 + L4 together (shared buddy logic), then L2 last (self-awareness multiplier depends on fleet-scale baselines).
