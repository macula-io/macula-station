# Torture-cascade — root-cause investigation

Status as of 2026-05-10: cascade-via-crash mechanism fixed (commit `b0340b7`). Underlying daemon-conn accumulation deferred to a follow-up task.

## TL;DR

Each e2e suite iteration adds ~3-4 entries to bootstrap stations' `peer_observer` `conns_tab` and `macula_dht` routing table. The peering_conn workers don't detect daemon BEAM exits until QUIC's idle-timeout fires (seconds to minutes), so entries leak per iteration. Eventually `macula_dht` is slow enough that `peer_observer`'s sync `gen_server:call` to `macula_dht:observe` times out (5s default) and crashes `peer_observer`. The supervisor restarts it; the named `conns_tab` ETS dies with the owner; all in-flight forwarded RPCs and stream relays die. **That's the cascade.**

The fix in `b0340b7` makes the `observe` call a fire-and-forget cast (mirrors `macula_swim:add_peer/3` which is already a cast). `peer_observer` no longer crashes from DHT load. The cascade-via-crash is gone.

But removing the crash also removed the accidental self-heal — the ETS reset on supervisor restart was clearing stale state. Now stale state monotonically accumulates instead of crashing back to zero. The fix is necessary but not sufficient; conn-aging is required to actually drain the accumulation. That's tracked separately.

## How the investigation ran

Everything below was made possible by the harness changes shipped earlier in this session — per-test diagnostic capture (commit `1f2119d` in macula-e2e), soak mode + cascade detection (`b1f31bd`), conns_tab focused probe (one-off script).

### 1. Force-recreate fleet to clean baseline

```
fresh-baseline: centrum=9 gasthuisberg=8 bertem=3 haasrode=4 kessel-lo=7 linden=4 bruges=7 hasselt=5 wijgmaal=3
```

Each station has 3-9 conns_tab entries from the inter-station partial mesh.

### 2. Run 4 e2e iterations + sample conns_tab between each

| iter | result | centrum | gasthuisberg | haasrode | kessel-lo | bruges |
|---|---|---|---|---|---|---|
| 0 (pre) | — | 17 | 9 | 9 | 10 | 13 |
| 1 | Failed 2/14 | 21 (+4) | 10 | 10 (+1) | 11 (+1) | 14 |
| 2 | Failed 2/14 | 24 (+3) | 10 | 11 (+1) | 12 (+1) | 14 |
| 3 | Failed 8/14 | 26 (+2) | 10 | 12 (+1) | 12 | 14 |
| 4 | Failed 12/14 | **3 (RESET)** | 15 | 16 | 15 | 17 |

Centrum (Pool/Other bootstrap) and haasrode (Cross bootstrap) accumulate monotonically. Non-bootstrap stations (bertem, linden, wijgmaal) stay flat at 3-4. The reset on centrum at iter 4 = peer_observer crashed and supervisor restarted it (named ETS dies with owner).

### 3. Confirm the crash signal in station logs

```
=CRASH REPORT==== 10-May-2026::16:26:48.275761 ===
  crasher:
    initial call: macula_station_peer_observer:init/1
    pid: <0.749.0>
    registered_name: macula_station_peer_observer
    exception exit: {timeout, {gen_server, call,
                       [<dht_pid>, {observe, ...}]}}
    in function gen_server:call/2 (gen_server.erl, line 1142)
    in call from macula_station_peer_observer:on_connected_directional/4
    message_queue_len: 504
```

Mailbox at 504. Sync `gen_server:call` to `macula_dht:observe` timed out. The DHT was overloaded by the same accumulation. Cascade.

### 4. Root cause

Read `peer_observer:on_connected_directional/4`:

```erlang
on_connected_directional(Direction, ConnPid, NodeId, ...) ->
    _ = macula_dht:observe(Dht, direct_peer_spec(NodeId)),  %% sync, result discarded
    ok = macula_swim:add_peer(Swim, NodeId, ConnPid),       %% already a cast
    ...
```

The DHT `observe` was sync (`gen_server:call` chain) but the result was unused. SWIM `add_peer/3` was already a cast. Symmetric-cast was the obvious fix.

### 5. The fix (commit `b0340b7`)

- `macula_dht.erl`: add `observe_async/2` API.
- `macula_dht_server.erl`: add `handle_cast({observe_async, Spec}, S)` that mutates state without replying.
- `peer_observer.erl::on_connected_directional/4`: switch the call site to `observe_async`.

207/207 unit tests green. Image deployed `b0340b7`.

### 6. Verification on the live fleet

Re-ran the same 4-iter cascade probe against the fixed image:

| iter | result | centrum conns_tab |
|---|---|---|
| 0 (pre) | — | 7 |
| 1 | Failed 4/14 | 11 (+4) |
| 2 | Failed 8/14 | 13 (+2) |
| 3 | Skipped 14/14 (fleet_not_reachable) | 13 (no change) |
| 4 | Skipped 14/14 (fleet_not_reachable) | 13 (no change) |

`peer_observer` did NOT crash — the conns_tab held at 13 instead of resetting to 3. **Fix works as designed.** But the test results are still bad: iter 3+ go to mass-skip because init_per_suite times out at the 30s WAIT_HEALTHY_MS. Probing centrum at end of run:

```
peer_observer: mailbox=9, status=waiting (idle, healthy)
macula_dht:    mailbox=140, status=running (processing backlog)
listener:      mailbox=0, status=waiting (idle)
DHT routing table size: 14 (vs bertem baseline of 2)
```

The bottleneck moved from peer_observer to macula_dht. `peer_observer` is correctly decoupled but `macula_dht` is now drowning in the same accumulated state.

## What this means

The cascade-via-crash chain is broken. The fleet no longer hits the catastrophic `peer_observer` crash + supervisor restart + state-loss cascade.

But the underlying accumulation problem remains: **station-side `peering_conn` workers don't detect dead daemon counterparts fast enough.** Pre-fix, accumulation was self-limited because peer_observer crashed and reset state. Post-fix, accumulation grows monotonically until the slow DHT degrades the handshake path enough to time out new pool dials.

The proper fix is conn-aging:
- `peer_observer` periodic sweep of `conns_tab`, force-close ConnPids whose last frame is too old
- `macula_dht` routing-table eviction by TTL (verify the existing periodic eviction actually fires; tune TTL for daemon-class)

That work is scoped in the follow-up task.

## Operational note

The pre-fix fleet recovered from the cascade by force-recreate (containers fresh, no accumulated state). The post-fix fleet recovers the same way. The OPERATIONAL workaround "force-recreate every N hours of operation under expected load" remains the safety net until conn-aging lands.

For the e2e harness specifically, force-recreate before any soak run remains the recommended discipline.

## Commit chain

| Commit | Effect |
|---|---|
| `1f2119d` (macula-e2e) | Per-test diagnostic capture — made the investigation possible |
| `6f2bb01` (macula-e2e) | cross_station_streaming_rpc probe — surfaced unrelated bug |
| `b1f31bd` (macula-e2e) | torture-mesh.sh soak mode + CSV — would have caught this faster |
| `156df16` (macula-e2e) | docker fault injection helpers — not yet used here |
| `b0340b7` (macula-station) | The cascade-via-crash fix |

Future: a single follow-up task tracks conn-aging in peer_observer + DHT routing table pruning. With that done, the fleet should be able to run indefinitely under e2e soak load without force-recreate.
