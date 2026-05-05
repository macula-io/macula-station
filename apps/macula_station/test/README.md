# macula_station tests

Two test mechanisms live here:

- **eunit** (`*_tests.erl`) — pure-function and in-VM unit tests. Fast, run on every build.
- **CT (Common Test)** (`*_SUITE.erl`) — multi-process integration tests. Each suite spawns isolated BEAM peer nodes that communicate over real QUIC, asserting station-to-station behaviour.

This README covers the CT side, since that's where the multi-process harness lives.

## The harness

`macula_station_test_cluster.erl` boots N isolated BEAM nodes, each running a full `macula_station` instance with its own QUIC listener bound to `::1` + an ephemeral port. The test driver coordinates with each peer over OTP's `peer:call` (controller stdio); inter-station traffic crosses real loopback UDP.

### Lifecycle

```erlang
%% In your CT testcase:
Handles = macula_station_test_cluster:spawn_cluster(N, Opts),
try
    %% your assertions
after
    macula_station_test_cluster:stop_cluster(Handles)
end.
```

`Opts` is a map. Use `#{base_dir => ?config(priv_dir, Config)}` so leaked data dirs land under CT's auto-cleaned priv_dir.

### Station handle API

| Function | Returns |
|---|---|
| `pubkey/1` | `<<_:256>>` (Ed25519 public key) |
| `listen_addr/1` | `{inet:ip6_address(), port()}` |
| `peer_node/1` | the peer's node atom |
| `peer_pid/1` | the peer controller pid |
| `data_dir/1` | path to the station's temp data directory |

### Connecting stations

```erlang
ok = macula_station_test_cluster:dial(A, B),
ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 10_000).
```

`dial/2` calls `macula_station:connect_to/1` on `A`'s peer, opening an outbound QUIC connection to `B`'s listen address. Returns `ok` when the handshake completes.

`wait_for_handshakes/3` polls `macula_station_peer_observer:peers/1` on the peer until at least `MinPeers` peers (inbound + outbound) are observed, or the deadline fires.

### Calling code on the peer

```erlang
SelfId = macula_station_test_cluster:rpc(A, macula_dht, self_id, [macula_dht]),
Result = macula_station_test_cluster:rpc(A, macula_dht, find_value,
                                          [macula_dht, Pubkey, SelfId, 5_000]).
```

Default RPC timeout is 30s. Use `rpc/5` for explicit timeout.

## Conventions for new integration suites

1. **Live in this directory** — `apps/macula_station/test/`. Vertical-slice with the integration being tested, not under a horizontal `test_helpers/` layer.

2. **Named `*_SUITE.erl`** — rebar3's CT runner picks them up automatically.

3. **One concern per suite** — `macula_station_dht_transport_SUITE` covers DHT-transport behaviour, `macula_station_test_cluster_SUITE` covers the harness itself, etc. Don't bundle unrelated assertions into a single suite.

4. **Per-testcase priv_dir** — pass `?config(priv_dir, Config)` as `base_dir` so CT's automatic cleanup catches any leaks if the testcase crashes mid-cluster.

5. **5-min timetrap default** — per-station boot can take ~30s under contention; multi-station tests need headroom.

6. **Run individual suites:**
   ```
   rebar3 as test ct --suite=apps/macula_station/test/macula_station_test_cluster_SUITE
   rebar3 as test ct --suite=apps/macula_station/test/macula_station_dht_transport_SUITE
   ```

7. **Run all CT suites:**
   ```
   rebar3 as test ct
   ```

## Background

The harness was created in Phase 1 of the V2 recovery work
(`plans/PLAN_PHASE_1_MULTI_PROCESS_CT_HARNESS.md`). It exists because the prior in-VM CT pattern (50-station fleet sharing one BEAM with a synthetic router) hid the C1 regression: `macula_dht:find_value/4` returned `{error, no_transport}` in production because the SDK's transport callback was never installed at boot, but the in-VM harness installed it directly. Multi-process CT is the only way to assert real wire behaviour.

## Known load-bearing details

- **`peer:start` (NOT `peer:start_link`)** — see `spawn_one_station/2`. Linking the peer to the test driver caused eunit to forcibly kill the peer between tests; `peer:start` gives `stop_cluster/1` deterministic teardown.

- **Guardian process for the supervisor** — see `on_peer_boot_station/0`. `peer:call` routes through a transient request-handler process; if `macula_station_app:start/2` runs there, `supervisor:start_link` links the sup to the transient process, and the sup dies the moment `peer:call` returns. The guardian holds the sup link for the test's lifetime.

- **Peer-side env loading** — `boot_station_on_peer/6` uses `application:ensure_all_started(macula)` (SDK only) + manual `application:set_env` + `macula_station_app:start/2` directly. `application:ensure_all_started(macula_station)` cascades into starting `macula_content`, which has external boot requirements not met in the test environment.

- **`peer:call` 5s default timeout** — explicit `peer:call(..., 130_000)` is required when calling `wait_until_ready` (which has a 120_000ms inner deadline). Without it, `peer:call` times out at 5s while the inner readiness loop is still polling.
