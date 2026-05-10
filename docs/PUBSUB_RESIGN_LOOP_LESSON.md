# Pubsub multi-hop re-sign — lessons from four failed attempts

Status as of 2026-05-10: shipped a logger filter (commit `edafdb6`) that suppresses the `[peer_observer] pubsub frame verify failed: signature_invalid` warning flood. The underlying protocol behaviour is unchanged. Four prior attempts to fix the protocol all regressed cross-station traffic and were reverted.

This document explains why every protocol-side fix we tried fell over, so a future attempt starts from data instead of intuition.

## TL;DR

The `signature_invalid` warning that fires on cross-station pubsub looks like a bug. It is **load-bearing loop prevention**.

Every station holds mutual `_mesh.bloom` SUBSCRIBEs on every other station so blooms can propagate over the partial mesh. When a relay station receives an EVENT and re-fans it, the recipient list includes the original sender. Old code accidentally killed that loop at the receiver's signature-verify step: the EVENT was signed by the upstream relay, conn NodeId on the receive side is the immediate sender, mismatch → drop with `signature_invalid`. The drop is the loop kill.

Per-hop re-sign would make verify pass at every hop. EVENTs then loop unbounded, saturate the cross-station send paths, and starve every other primitive that shares those paths (CALL forwarding, DHT replication, content transfer).

Because the "fix" surfaces a previously-hidden topology coupling, even narrow variants (mesh-realm-only dedup, mesh-realm 1-hop cap) regress orthogonal cross-station tests. The protocol fix needs Phase 2 — publisher-end-to-end signed EVENT envelopes — not an incremental relay-layer patch.

## What's deployed today

| Commit | What |
|---|---|
| `edafdb6` | Logger filter on the default handler. Drops `[peer_observer] pubsub frame verify failed:` warnings at `warning` level. Lives in `apps/macula_station/src/macula_station_log_filters.erl`, installed from `macula_station_app:start/2`. |

Verified: `0` `signature_invalid` lines on all 9 stations across full container lifetime; e2e baseline `12/13` (only the always-failing `weather_subscribe` daemon-side stub fails).

## What we tried and why each failed

### Attempt 1 — naive per-hop re-sign

Commits: `9693b2f` shipped, `4f8161e` reverted.

`hecate_pubsub_server:relay_event/2` re-signed every inbound EVENT with the local station's identity before fan-out, mirroring the CALL/RESULT claimed-signer fix from `05e0fbe`.

Result: e2e collapsed `12/13 → 8/13`. Every cross-station primitive failed (pubsub, unary RPC, DHT, content). Single-station primitives still passed. Instrumented build measured **43 600 `_mesh.bloom` relay events on a single station in 8 minutes** — sustained ~90/sec flood from a runaway loop.

Mechanism: see TL;DR. Per-hop re-sign defeats the verify-fail loop kill. With mutual `_mesh.bloom` subscriptions across the mesh, EVENTs cycle indefinitely. The flood saturates the per-conn send queues and `peer_observer`'s gen_server mailbox, which all cross-station traffic shares.

### Attempt 2 — cap mesh-realm EVENTs to 1 hop

Commits: `479adda` shipped, `6a7e3ff` reverted.

Hypothesis: if bloom is "1-hop by design" (each station broadcasts its OWN bloom), short-circuiting `deliver_pubsub_typed(event, ?MESH_REALM, ...)` prevents the loop without touching anything else.

Result: e2e collapsed `12/13 → 7/13`. Bloom convergence killed.

Mechanism: bloom broadcast goes only to the 3 outbound peers (`peer_links:connections()`), not full mesh. Stations whose direct neighbours don't include some bloom publisher rely on multi-hop relay through `peer_observer.fan_out_event` to learn that publisher's bloom. Disabling the relay broke partial-mesh convergence. The verify-fail loop kill in baseline doesn't prevent multi-hop relay; it kills only the specific re-arrivals that trip verify. The unbounded multi-hop relay is necessary; the loop kill is what makes it bounded.

### Attempt 3 — publisher+seq dedup + re-sign (all realms)

Commits: `8f73da5` shipped, `21ba509` reverted.

`relay_event/2` got an ETS-backed `(publisher, seq)` dedup cache, TTL-swept every 30 s. Both `relay_publish` and `relay_event` consult it before processing. The re-sign now only happens for fresh frames; loop-back duplicates short-circuit to `{error, duplicate}`.

Tests: 130/130 unit tests green. Single-test trace (`cross_station_pubsub` in isolation) shows the EVENT propagating brussels → ghent → kessel-lo → antwerp with correct dedup at every loop-back hop. Test passes.

Result: full e2e suite flaky — three runs returned `10/13`, `8/13`, `0/13`. After the third run the fleet entered `fleet_not_reachable` state (suite skipped at init). Ghent observed at 80 % CPU; brussels at 9 % — asymmetric load.

Best-guess mechanism: the per-frame Ed25519 sign + ETS insert + 30 s `select_delete` sweep multiplies under the suite's combined SUBSCRIBE/UNSUBSCRIBE churn + bloom traffic. The single-test trace doesn't exercise that combined load. We did not get clean root-cause data before reverting.

### Attempt 4 — publisher+seq dedup + re-sign (mesh realm only)

Commits: `5394ab8` shipped, `8360eba` reverted.

Variant of attempt 3: route only `?MESH_REALM` events through the new `relay_event/2`; user-realm pubsub keeps the original `deliver_event/2` path. Reasoning: bloom is the only path that demonstrably loops; mesh-realm rate is bounded (~24/sec mesh-wide for 9-station gossip at 30 s ticks), so per-event Ed25519 cost is negligible.

Tests: 130/130 green.

Result: stable at `10/13` across 3 consecutive runs (down from 12/13 baseline). Same two regressed tests every run: `cross_station_pubsub` and `cross_station_dht_put_find`. After several rounds, fleet entered `fleet_not_reachable`. Bloom convergence on antwerp persistently at 6/8 peers' blooms cached (vs 8/8 on brussels and bruges).

Mechanism: not fully understood. The mesh-realm change should not affect user-realm pubsub or DHT. Best guess: the mesh-realm dedup eliminates SOME bloom EVENT paths that, in baseline, the unbounded flood happened to traverse — and downstream protocols (DHT routing-table convergence, peering-router SUBSCRIBE chain timing) depend on the timing/completeness of bloom propagation in subtle ways. The bloom flood, even though wasteful, may incidentally seed downstream convergence faster than the dedup-controlled propagation does.

## Why all four attempts surfaced cross-protocol regressions

The bloom flood + verify-fail loop kill is **load-bearing in two distinct ways**:

1. **Loop kill.** The drop at verify is what makes the unbounded re-fan terminate.
2. **Convergence floor.** The unbounded flood happens to reach peer-routing-table state and SUBSCRIBE-chain machinery on schedules that downstream protocols (DHT, user-realm pubsub) implicitly depend on.

Any change that tightens bloom propagation — even narrowly — touches both, and the second is invisible at code-review time. You only see the regression by running the full e2e suite on the live fleet.

## What a future protocol attempt needs

In rough order of preference:

1. **Publisher-end-to-end signed EVENT envelopes (Phase 2).** The `hecate_pubsub_server.erl` header at line 124 already calls this out: "Phase 2 tightens to publisher-end-to-end auth via UCAN". Once EVENT carries the original publisher's signature and verify checks against the `publisher` field instead of conn NodeId, no re-sign is needed at any hop. The loop becomes irrelevant. This is a wire-protocol change but is the structurally correct fix.

2. **Diagnose the bloom-incompleteness root cause first.** Antwerp persistently sits at 6/8 cached peer blooms in baseline. If that asymmetry is intrinsic to the partial-mesh topology and downstream protocols are tuned around it, every "improvement" to bloom propagation will rattle them. Understanding why baseline is what it is must precede any change.

3. **Lighter dedup mechanism if a Phase 1 fix is still attempted.** Bloom-filter (false-positive-tolerant) instead of ETS to skip the per-frame Ed25519 cost; or async re-sign via a worker pool to keep `pubsub_server`'s gen_server mailbox responsive. Don't ship without measuring full-suite e2e stability across at least 5 sequential rounds.

## File map

- `apps/hecate_overlay/src/hecate_pubsub_server.erl` — where re-sign / dedup logic would live. Header at line 124 calls out the Phase 2 plan.
- `apps/hecate_overlay/src/hecate_pubsub.erl` — pure subscriber lookup; `deliver_event/2` does the realm + topic match.
- `apps/macula_station/src/macula_station_peer_observer.erl` — `deliver_pubsub_typed/5` is the dispatch entry; `deliver_inbound_event/3` is the relay entry; `fan_out_event/3` is the actual send.
- `apps/macula_station/src/macula_station_bloom_exchange.erl` — `?MESH_REALM = <<0:256>>`, topic `_mesh.bloom`. Broadcast goes to `peer_links:connections()` (the 3 outbound peers).
- `apps/macula_station/src/macula_station_peering_router.erl` — wires SUBSCRIBE chain across outbound peers; `desired_triples/2` does NOT filter by bloom (peering subscribes on every peer).
- `apps/macula_station/src/macula_station_log_filters.erl` — the shipped logger filter.

## How to detect this kind of regression earlier

The instrumentation that caught attempt 1 in 8 minutes:

```erlang
logger:warning("[pubsub_server] relay_event ENTER realm=~w topic=~p via=~p has_via=~p",
    [erlang:phash2(R), maps:get(topic, Frame, undefined),
     maps:get(delivered_via, Frame, missing),
     maps:is_key(delivered_via, Frame)]).
```

Plus capturing logs to a local file via `docker logs --since 1s -f ... > /tmp/...log` while running e2e. `wc -l` told the story instantly: 84 k lines / 8 min on a single station = log flood = traffic flood.

Before deploying any relay-path change to the live mesh:

- Check whether the change affects any topic on the all-zeros mesh realm.
- If yes, count expected EVENT-rate × peer-count × hops and ask "is this bounded?".
- If a frame can be re-fanned by recipients, dedup is mandatory before re-sign.
- Run the full e2e suite for at least 5 sequential rounds. The cross-protocol degradation surfaces around round 2-3, not round 1.

## Commit chain

| Commit | Effect |
|---|---|
| `9693b2f` | Attempt 1: per-hop re-sign, no dedup |
| `4f8161e` | Revert of attempt 1 |
| `479adda` | Attempt 2: mesh-realm 1-hop cap |
| `6a7e3ff` | Revert of attempt 2 |
| `8f73da5` | Attempt 3: dedup + re-sign, all realms |
| `21ba509` | Revert of attempt 3 |
| `5394ab8` | Attempt 4: dedup + re-sign, mesh realm only |
| `8360eba` | Revert of attempt 4 |
| `edafdb6` | Logger filter — what shipped |
