# Phase 2 Lifeguard Gaps (deferred follow-up)

**Phase 2 shipped 2026-04-14** with classic direct-probe SWIM — sufficient to detect confirmed failures within ~8 s for the 3-station walking skeleton. The four Lifeguard extensions below are deferred; they are not blockers for Phase 3 (DHT is independent of SWIM depth) but must land before real-WAN deployment to tame false positives under packet loss and congestion.

**Reference:** `PLAN_MACULA_V2_PART4_LIFECYCLE.md` §5 (SWIM + Lifeguard), DSN 2002 + arXiv 2017.

| # | Extension | Purpose | Trigger for implementation |
|---|-----------|---------|----------------------------|
| L1 | **Indirect-ping via K buddies** | Direct PING timeout → ask `k=3` random alive members to ping target; ACK via any of them refutes. Kills false positives under localised packet loss. | Before exposing SWIM to networks with > 1 % loss. |
| L2 | **Self-awareness multiplier** | If local node suspects > T % of its view in one period, it assumes its own uplink is congested and multiplies its suspicion-timeout by `1 + local_health`. Prevents false-positive cascades. | Before running on mobile / residential links. |
| L3 | **Refutation-buddy (NACK relay)** | Suspected peer can relay its "I'm alive" via a buddy when its direct path to the suspector is lossy. | With L1 — they share the buddy-selection path. |
| L4 | **NACK (explicit refutation)** | Buddy that confirms target alive sends explicit NACK to the suspector, short-circuiting the suspect-timer without waiting for the next probe. | With L1 + L3. |

## Why deferred

- Phase 2 acceptance test (3 stations, LAN, confirmed\_failed within 8 s) passes *without* Lifeguard extensions.
- Lifeguard tuning is data-driven (T %, buddy-set size `k`, timeout multiplier bounds) and wants real-network telemetry before calibration.
- DHT (Phase 3) depends only on SWIM *membership-state events*, not on Lifeguard quality.

## Non-goals during Phase 3

- Do **not** fold Lifeguard into DHT liveness — the DHT has its own `PING/PONG` layer (Part 6 §7.1).
- Do **not** remove the Phase 2 direct probes when L1–L4 land; Lifeguard extends the classic protocol, it does not replace it.

## Trigger

"Resume Phase 2 — Lifeguard extensions" — picks up L1 first, then L3 + L4 together (shared buddy logic), then L2 last (self-awareness multiplier depends on fleet-scale baselines).
