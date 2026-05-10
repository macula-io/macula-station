# Cross-station DHT find flake — attempt 1 (REVERTED)

**Status:** Reverted 2026-05-10. Hypothesis was wrong; the attempt's narrative is preserved here so the next attempt starts from data, not memory.

## The flake

`cross_station_dht_put_find` and the related `cross_station_tombstone_propagation` (which depends on the same find path) flake at ~60% on a fully-converged 9-station Leuven mesh. Even with bloom convergence complete (every station seeing every other station's bloom) and macula 4.2.9 + conn-aging deployed, the per-iteration hit rate hovers around 2/5.

## Hypothesis (attempt 1)

`peer_observer:on_connected_directional/4` calls `macula_dht:observe_async` for every connected peer regardless of direction. `direct_peer_spec/1` hardcodes `tier => t0`. So daemons — transient SDK clients with no `macula_dht_server` — landed in the DHT routing table alongside real stations.

When `fanout_find_value` (in `macula_station_dht_handlers.erl`) asks `macula_dht:k_closest(Dht, Key, 20)` and fans `find_value` RPC across the result, daemon entries silently never respond with `{value, _}`. The theory was: if XOR-closest entries skewed toward daemons, iterative find returned `not_found` even when stations had the record.

## The fix (commit `c08ef8d`)

Gate `macula_dht:observe_async` on `Direction == outbound`. Reasoning: stations dial each other in the partial mesh; daemons only inbound-dial stations. So outbound = station-class peer (observe in DHT), inbound = daemon OR yet-unobserved station (skip DHT).

Tests updated: `one_connected_peer` helper switched from `connected` to `connected_outbound`; new test `inbound_does_not_observe_into_dht_test_` locked the contract; `external_peer_dial_lands_in_dht_and_swim_test_` renamed to `..._in_swim_not_in_dht_test_` and inverted to assert the new contract. 117/117 unit tests green.

## What actually happened

**0/5 cross_station_dht_put_find on the post-fix fleet.** REGRESSION from the 2/5 baseline.

Mechanism: shrinking observation to outbound-only collapsed each station's DHT routing table to ~3 entries (its outbound peer count). This broke TWO mechanisms simultaneously:

1. **Eager replication** (`macula_dht_replicate:replicate_one`) uses `k_closest(Dht, Key, k=8)` to pick custodians. With ≤3 entries in the routing table, eager replication only reached 3 stations — far fewer than the previous ~8.
2. **1-hop iterative find** (`fanout_find_value` line 167) also uses `k_closest(Dht, Key, 20)`. With ≤3 entries, the fan-out queries 3 peers. Probability that none of those 3 happen to be among the 3 stations the writer eager-replicated to: not negligible. Find returns `not_found`.

The reduction in routing-table entries hurt MORE than the daemon pollution did.

**Reverted in commit `4bccf1c`.**

## Where the original theory was wrong

`fanout_find_value` collects via `collect_first_value` which returns on the FIRST `{value, _}` reply. Daemon non-responses are NOISE, not failures. So mixing daemons into the routing table doesn't BLOCK find from succeeding; it just delays the answer slightly.

The real lever for fix is keeping the routing table FULLER, not smaller.

## Where to look next

In rough order of preference:

1. **Multi-round Kademlia iterative.** Currently the find is 1-hop only — `fanout_find_value` doesn't recurse into `{nodes, _}` responses to chase closer custodians. Walking `{nodes, _}` until convergence (or a depth cap) would let the find traverse the full mesh from any starting point even with a small local routing table. The original code comment at lines 155-160 acknowledges: *"NOT a multi-round Kademlia walk: we don't recurse into `{nodes, ...}` responses to chase closer custodians. One hop is sufficient for the partial-mesh sizes we operate at (≤ low hundreds of stations) given that eager replication places copies with a fan-out of `k = 20'."* The "k = 20" assertion turns out to be aspirational — actual fan-out is bounded by routing-table size.

2. **Wider eager replication.** Same story but applied at put-time: drive `replicate_one` through a Kademlia walk to find more custodians than the local routing table knows about. Improves hit rate without changing find logic.

3. **Tier-aware fan-out without observe-time filter.** Keep all peers in routing table (so eager + iterative reach stays high), but filter at fan-out time to skip daemon-class entries. Requires actually distinguishing daemon vs station — the wire protocol doesn't currently expose this. Would need either a capabilities bit or behavioural inference (e.g., "peer that successfully responded to our `_dht.ping` is a station; one that doesn't is a daemon"). Behavioural inference would be discovered slowly.

4. **Don't fix it. Document the ~60% hit rate as substrate baseline at this scale.** The handover's Phase 4+ note already documented that 1-hop iterative is "enough for ≤ low hundreds of stations" — but the partial-mesh + small routing table case is a counter-example. For DNS-readiness, the consumer (DNS slice) can poll-with-retry: a single retry doubles effective hit rate to ~84%, two retries ~94%, three ~97%. For production-scale (>9 stations) the multi-round walk becomes mandatory.

Option 4 is the honest pragmatic answer for today. Option 1 is the structurally correct answer for any scale beyond the immediate test fleet.

## Probes that surfaced this

- `cross_station_dht_put_find` (existing): 2/5 baseline, sensitive to the issue.
- `cross_station_tombstone_propagation` (added in commit `8831d1e`): same sensitivity, fails on `original_not_visible` step.

Both probes will START PASSING when whichever fix lands. They are durable regression detectors.

## Commits

- `c08ef8d` (macula-station) — broken attempt 1
- `4bccf1c` (macula-station) — revert of attempt 1
