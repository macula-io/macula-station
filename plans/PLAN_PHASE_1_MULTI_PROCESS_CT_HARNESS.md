# PLAN — Phase 1: Multi-Process CT Harness

**Status:** Draft, ready for implementation after sanity-check
**Date:** 2026-05-05
**Trigger:** Phase 0 (PLAN_V1_V2_PARITY.md) revealed 30+ 🟡 wired-but-untested rows whose status cannot be upgraded to 🟢 without a multi-process integration test framework. The DHT `no_transport` regression (PLAN_DHT_TRANSPORT_WIRING.md) survived to production because the only DHT test (`macula_dht_SUITE.erl`) shares one BEAM VM with a synthetic router — the test harness IS the missing wire. Phase 1 closes that gap before any C1-C6 fix proceeds.

## Goal

A reusable test harness that:
1. Spawns ≥2 isolated BEAM nodes, each running a full `macula_station` instance (DHT + peering + observer + listener + identity).
2. Lets stations talk to each other over **real QUIC** (not in-VM message passing).
3. Lets the test driver introspect station state, issue API calls, and inject failures via `erpc`.
4. Cleans up reliably across test failure modes.
5. Boots fast enough for CI (target: ≤2s per station, ≤10s for a 3-station cluster from spawn to "all peers handshaked").

The harness's **first test is the C1 reproducer** (`find_value` from station A for station B's pubkey). Pre-fix it returns `{error, no_transport}`; post-fix it returns `{value, [_]}` or `{nodes, [_]}`. Phase 1 lands this test in a known-failing state. Phase 4 (PLAN_DHT_TRANSPORT_WIRING) makes it pass.

## Why this is Phase 1

Per the recovery ordering agreed in this session: gates first, fixes second. Without the harness, every "fix" for C1-C6 ships unverified — recreating the exact failure mode that caused the regression. The harness is what flips the engineering pressure: a regression in the wire-bound paths becomes a CI failure, not a production discovery weeks later.

This document blocks all C1-C6 fix work until merged.

## Architecture decision

### Three options for spawning isolated stations

| Option | Mechanism | Pros | Cons |
|---|---|---|---|
| **(A) Erlang `peer`** (OTP 25+) | `peer:start_link/1` spawns child BEAM node; `erpc` for control | stdlib, deterministic, fast (~500ms boot per peer); each peer has own VM and own QUIC listener; real wire between peers | Test driver communicates via Erlang dist (cookie + epmd), but station-to-station traffic is real QUIC |
| **(B) Docker containers** | One container per station; tests dial admin endpoints | Most production-like | Slow (~5s container boot); heavy CI infra dep; hard to introspect state; cleanup brittle |
| **(C) OS processes via release script** | `bin/macula_station start` N times | Identical to production | Requires release build for tests; brittle setup/teardown; no easy state introspection |

**Recommendation: (A).** Reasons:

1. The test concern is "do stations communicate correctly over the wire" — Option A satisfies this exactly. Each peer node has its own VM, runs its own `macula_station` supervisor tree, binds its own QUIC listener. Station-to-station traffic crosses real UDP sockets.
2. Test driver coordination via `erpc:call(PeerNode, M, F, A)` is fast, deterministic, and lets us read internal state (k-bucket contents, DHT record counts, peer_links membership) without hacks.
3. `peer` cleanup on driver crash is automatic (peer dies when parent dies).
4. CI cost: 3 BEAMs × ~50MB heap = 150MB; well within standard runner budget.
5. OTP 27 (current) ships `peer` natively. No new deps.

Option B reserved for a future "Phase 1.5: Production-shape integration tests" if needed; Phase 1 doesn't require it.

### File organization

Per the workspace's vertical-slicing rule: harness lives **with the integration it tests**, not in a horizontal `apps/test_helpers/` layer.

```
apps/macula_station/test/
├── macula_station_test_cluster.erl        ← THE HARNESS (new)
├── macula_station_dht_transport_SUITE.erl ← First user (new; Phase 1)
├── macula_station_peer_observer_tests.erl ← existing eunit
└── ...                                     ← existing
```

`macula_station_test_cluster` is exported from the test dir; other apps' integration suites can use it via `-include_lib("macula_station/test/macula_station_test_cluster.hrl")` if needed. Most integration tests will live in `apps/macula_station/test/` because macula_station is the integration point.

Naming follows the screaming-architecture convention: `test_cluster` says exactly what it is. No `helpers/`, `utils/`, or `support/`.

## Harness API

```erlang
-module(macula_station_test_cluster).

-export_type([station_handle/0]).

%% A station_handle is opaque; it carries the peer node, the station's
%% pubkey, the QUIC listener address, and the priv_dir for cleanup.
-opaque station_handle() :: #{
    peer_node    := node(),
    pubkey       := <<_:256>>,
    listen_addr  := {inet:ip6_address(), inet:port_number()},
    data_dir     := file:filename(),
    peer_pid     := pid()
}.

%% --- Cluster lifecycle ---

-spec spawn_cluster(N :: pos_integer(), Opts :: map()) -> [station_handle()].
%% Boots N isolated stations bound to ::1 with ephemeral UDP ports.
%% Each gets its own keypair, data_dir, and full station supervision tree.
%% Returns immediately when all stations have started; does NOT wait for peering.

-spec stop_cluster([station_handle()]) -> ok.
%% Idempotent. Tears down peer nodes, removes data_dirs, releases ports.

%% --- Connectivity ---

-spec dial(From :: station_handle(), To :: station_handle()) -> ok | {error, term()}.
%% From station opens an outbound QUIC connection to To.
%% Returns when CONNECT/HELLO handshake completes (peer_node_id known on both sides).
%% Times out after 5s.

-spec wait_for_handshakes(StationHandle, MinPeers :: non_neg_integer(),
                          TimeoutMs :: pos_integer()) -> ok | {error, timeout}.
%% Polls peer_links:verified_peers/0 on the station until count >= MinPeers
%% or timeout fires. 100ms poll interval.

%% --- State introspection / API calls ---

-spec rpc(station_handle(), module(), atom(), [term()]) -> term().
%% Wraps erpc:call(PeerNode, M, F, A). Used for:
%%   - macula_dht:find_value/4 (the C1 test)
%%   - macula_dht:stats/1 (assertions on routing table state)
%%   - macula_station_peer_links:verified_peers/0 (peering assertions)
%% Re-raises remote exceptions in the test driver.

-spec pubkey(station_handle()) -> <<_:256>>.
-spec listen_addr(station_handle()) -> {inet:ip6_address(), inet:port_number()}.
-spec peer_node(station_handle()) -> node().
%% Trivial accessors; no remote call.

%% --- Failure injection (Phase 1 minimum; expand later) ---

-spec kill_station(station_handle()) -> ok.
%% Forcibly stops the peer node. Used to test peer-loss cascade refresh.
%% Returns immediately; cleanup is best-effort.

%% --- Future, NOT in Phase 1 (deferred) ---
%% partition/2, heal_partition/0 — chaos testing, Phase 7+
%% inject_latency/3 — performance regression testing, later
```

## The first failing test

```erlang
%% apps/macula_station/test/macula_station_dht_transport_SUITE.erl
-module(macula_station_dht_transport_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([find_value_walks_the_network/1]).

all() -> [find_value_walks_the_network].

init_per_suite(Config) ->
    Cluster = macula_station_test_cluster:spawn_cluster(2, #{}),
    [{cluster, Cluster} | Config].

end_per_suite(Config) ->
    macula_station_test_cluster:stop_cluster(?config(cluster, Config)),
    ok.

%% This test FAILS pre-Phase-4. It is the C1 regression test.
%% Once PLAN_DHT_TRANSPORT_WIRING.md ships, it passes.
find_value_walks_the_network(Config) ->
    [A, B] = ?config(cluster, Config),
    ok = macula_station_test_cluster:dial(A, B),
    ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 5000),
    BPubkey  = macula_station_test_cluster:pubkey(B),
    ASelfId  = macula_station_test_cluster:rpc(A, macula_dht, self_id, [macula_dht]),
    Result   = macula_station_test_cluster:rpc(A, macula_dht, find_value,
                                               [macula_dht, BPubkey, ASelfId, 5000]),
    %% Pre-fix: Result =:= {error, no_transport}
    %% Post-fix: matches {value, [_]} OR {nodes, [_|_]}
    ?assertMatch(R when element(1, R) =:= value orelse element(1, R) =:= nodes,
                 Result,
                 "find_value should return B's record or k-closest nodes; "
                 "got no_transport means PLAN_DHT_TRANSPORT_WIRING is not yet shipped").
```

This test is the smallest end-to-end check that exercises every piece of the wiring. If it passes, C1 is closed. If it fails with `{error, no_transport}`, C1 is open. Either way, it tells the truth.

## Ordered implementation steps

Each step is a self-contained commit.

1. **Add `macula_station_test_cluster` skeleton.** New file `apps/macula_station/test/macula_station_test_cluster.erl` with `-export([spawn_cluster/2, stop_cluster/1])` returning empty list / ok. Compiles. Wire into rebar.config test profile if needed.

2. **Implement `spawn_cluster/2` minimum (1 station).** Spawns one peer node with `peer:start_link`, sets up a unique cookie, copies code paths, starts `macula_station` application via `erpc:call`. Generates an Ed25519 keypair and a temp data_dir. Returns one handle. Test: `eunit` — spawn 1 station, assert it has a pubkey and a peer_node, stop it, assert peer_node is dead.

3. **Bind QUIC listener to ::1 + ephemeral port.** Modify station boot config to support `MACULA_BIND_ADDR` + `MACULA_PORT=0` (ephemeral) for tests. Read back actual port via `inet:port/1` on the listener socket. Update station_handle to carry `listen_addr`. Test: spawn 1 station, assert `listen_addr` is `{::1, P}` with `P > 0`.

4. **Extend `spawn_cluster/2` to N stations.** Spawn N peers in parallel. Wait for all `macula_station` apps to be `started` (poll via `erpc:call(Node, application, which_applications, [])`). Test: spawn 3 stations, assert all 3 have unique pubkeys and unique listen_addrs.

5. **Implement `dial/2`.** Calls `erpc:call(FromNode, macula_station_outbound_link, dial, [ToHost, ToPort, []])` (or whatever the V2 dial API is — verify file:line). Polls `peer_node_id/1` on the resulting connection until non-`undefined` or 5s timeout. Test: spawn 2 stations, dial A→B, assert peer_links:verified_peers/0 on both shows the other.

6. **Implement `wait_for_handshakes/3`.** 100ms poll loop on `peer_links:verified_peers/0` count. Test: spawn 3 stations, dial A→B and A→C, wait_for_handshakes(A, 2, 5000) returns ok.

7. **Implement `rpc/4`, `pubkey/1`, `listen_addr/1`, `peer_node/1`.** Trivial wrappers. Test: spawn 1 station, rpc to call `macula_dht:self_id/1`, assert returned binary equals the station's pubkey.

8. **Implement `kill_station/1` + `stop_cluster/1`.** `peer:stop/1` on each handle's peer_node. Remove data_dirs. Tolerant of already-dead peers. Test: spawn 2 stations, kill one, assert stop_cluster on both succeeds.

9. **Add the first user: `macula_station_dht_transport_SUITE.erl`.** As shown above. Test runs, fails with `{error, no_transport}`. **This is the intended state until Phase 4 lands.**

10. **CI integration (defer the gate to Phase 2).** Add `make integration-test` target that runs CT suites. Run it in CI as **non-blocking** for now (the C1 reproducer is supposed to fail). Phase 2 flips it to blocking once C1 is closed.

11. **Documentation: `apps/macula_station/test/README.md`.** Short doc explaining the harness API and the conventional pattern for new integration tests. Cross-link from the parity checklist.

## CI gating (Phase 2 preview)

Phase 1 leaves CI permissive — the C1 reproducer is expected to fail. Phase 2 closes that loop:

- Drop `|| true` from eunit invocation in `.github/workflows/ci.yml` and `.forgejo/workflows/ci.yml`.
- Make the integration-test target blocking.
- Add deploy-time smoke test (described in PLAN_V1_V2_PARITY.md "What this means going forward").

These changes happen AFTER Phase 4 closes C1, so CI doesn't block on a known-broken test in the interim. **Sequencing: Phase 1 (harness) → Phase 4 (DHT fix) → Phase 2 (CI gates) → Phase 5+ (close remaining 🟡 rows).**

This ordering is deliberate. Phase 2 before Phase 4 would block `main` on a failing test until the fix lands; Phase 4 before Phase 1 would ship the fix unverified. The chosen order: harness first (Phase 1) so the fix has a target; fix next (Phase 4) so the gate can land; gate last (Phase 2) so the next regression of this class is caught immediately.

## Risks

* **`peer` boot time on cold cache.** First `peer:start_link` per CI run can take 1-3s while OTP loads. After that, ~500ms per peer. Budget: 3-station test suite ~5-10s end-to-end. Acceptable.

* **Port exhaustion on flaky cleanup.** If `stop_cluster` doesn't run (test timeout, CI killed), QUIC sockets stay bound. Mitigation: bind to `::1` (loopback only — no production conflict); use ephemeral ports (kernel reuses on TIME_WAIT after 60s); `peer:start_link` registers a monitor so peer dies if driver dies. Add a CI step that explicitly kills stray BEAMs before each run as belt-and-braces.

* **Test driver and station code-path mismatch.** The driver runs the *test build* of macula_station; the peer node loads code paths from the same `_build/test/lib/`. Need to verify `peer:start_link` propagates code paths correctly — `peer` Opts: `#{args => ["-pa", PathA, "-pa", PathB, ...]}`. Step 2 of implementation must verify this works.

* **State pollution across tests.** Each station has its own data_dir + ephemeral port + fresh keypair, so no shared state. `persistent_term` is per-VM (each peer is a separate VM). `application:set_env` is per-VM. **Verified safe.**

* **OTP version skew between dev machine and CI.** `peer` is OTP 25+; the codebase already uses OTP 27 features per the prior session log (`stdlib 6.1.2`). CI must pin the same OTP version. Add explicit `otp-version` to CI matrix; reject builds on older OTP.

## Test strategy for the harness itself

The harness is code; it needs its own tests.

* **eunit on each helper:** spawn_cluster/1, spawn_cluster/3, dial/2, wait_for_handshakes/3, kill_station/1. Each test boots the minimum stations needed and asserts the helper does what its docstring claims.
* **Self-test in CI:** the integration-test target runs the harness's own test suite first, before any consumer. If the harness is broken, no further test runs.
* **"Drop the harness in a test you don't control" smoke test:** add a no-op consumer test that just calls `spawn_cluster(2, #{})` and `stop_cluster/1`. If it passes, the harness boots/teardown cycle is healthy.

## Knock-on / leverage from harness existence

Once Phase 1 lands, the team has a tool that converts the parity checklist's 🟡 → 🟢 work from "blocked on missing infra" to "write a test, watch it run." The expected pace after Phase 1:

- 1-2 days per integration test for routine wire-bound capabilities (RPC call, subscribe, advertise, find_value)
- Each test landed updates one row in PLAN_V1_V2_PARITY.md from 🟡 to 🟢
- After ~10-20 such tests, V2's "actually proven working" surface area roughly matches V1's
- The 🟢 count starts mattering to deployment confidence in a way it doesn't today

This is the leverage. Phase 1 isn't producing customer value directly; it's producing the substrate that lets every subsequent fix prove itself.

## What's NOT in Phase 1

Explicitly deferred to keep scope tight:

- **Realm-side integration tests** (Phoenix/Elixir node spawning). The realm dials stations, so realm-test setup is heavier (release build vs `peer`). Defer to Phase 5 alongside C3.
- **Daemon-side integration tests** for the V2 migration. Wait for PLAN_DAEMON_V2_MIGRATION.md to land first.
- **Chaos/partition testing** (`partition/2`, `heal_partition/0`). Phase 7+.
- **Performance regression testing** (latency injection, throughput assertions). Later.
- **Multi-OS-platform testing** (Windows, macOS for stations). Linux-only for now.

## Acceptance criteria for Phase 1

This plan is "done" when:

1. `apps/macula_station/test/macula_station_test_cluster.erl` exists, compiles, and exports the documented API.
2. Each of the 11 implementation steps has its own commit with passing tests for that step.
3. `apps/macula_station/test/macula_station_dht_transport_SUITE.erl` exists and runs in CI (non-blocking — expected to fail until Phase 4).
4. Running `rebar3 ct --suite=macula_station_dht_transport_SUITE` locally produces a clear "find_value returned no_transport" failure with a message pointing to PLAN_DHT_TRANSPORT_WIRING.md.
5. `apps/macula_station/test/README.md` documents the harness API and the convention for adding new integration suites.
6. PLAN_V1_V2_PARITY.md is updated: a new "Verification log" entry notes that the harness is now the gating mechanism for 🟢 conversion.

When all six are met, Phase 1 closes and Phase 4 (PLAN_DHT_TRANSPORT_WIRING) can begin.
