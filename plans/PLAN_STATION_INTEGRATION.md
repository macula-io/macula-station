# PLAN_STATION_INTEGRATION.md — wiring the running station

**Parent:** `PLAN_MACULA_V2_ROOT.md` §10 (phase roadmap), sitting
between Phase 6 (shipped — all library code + adapters + bridge) and
Phase 7 (hardening — chaos, Sybil, burn-in).

**Purpose:** take the library code + chain adapters + bootstrap
bridge that Phase 6 delivered and compose a running station — one
that an operator can start on a beam node, observe via `/status',
reboot without losing identity, participate in SWIM, answer DHT
lookups, and host a realm. Each session below leaves the tree green
(xref + eunit + ct + dialyzer clean) and one step closer to that.

**Cross-references:**
- `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md` — original Phase 6–8
  task lists.
- `PLAN_STATION_RUNBOOK.md` — operational guide for the assembled
  station.
- `PLAN_DEFERRED_WORK.md` — items the integration sprint
  <em>won't</em> try to land (chaos suite, 6.5.x UDP BT-DHT, etc).
- `PLAN_PHASE_6_BREAKDOWN.md` — what the library side shipped.

---

## 1. Scope boundaries

<b>In scope</b> (this sprint):
- Composing identity + QUIC + bootstrap + DHT + SWIM + overlay +
  admin API under `hecate_station_sup`.
- Persistent identity on disk (generate on first boot, load
  thereafter).
- Config-driven station wiring via `sys.config`.
- Graceful shutdown (tombstone, state flush).
- A four-node CT on the beam cluster (or on loopback) that
  demonstrates cold boot → cascade → DHT seed → SWIM convergence
  → DHT lookup succeeds.

<b>Out of scope</b> (tracked elsewhere):
- Chaos suite / Sybil / eclipse tests — Phase 7.
- 72 h burn-in — Phase 7.
- Real-network adapters 6.5.x / 6.6.y / 6.3.5 / 6.3.6 — see
  `PLAN_DEFERRED_WORK.md §1`.
- Lifeguard L1-L4 — `PHASE_2_LIFEGUARD_GAPS.md`.
- Realm admission — separate plan.
- Local-first boot — separate plan.
- Admin HTTP-over-TLS + client-cert auth — Session 8.6 ships HTTP
  on loopback only; TLS is Phase 7.

---

## 2. Session sequence (8 focused sessions)

Each session is sized to ship green in one sitting. Numbering is
"Session 8.x" because this is the first concrete slice of Phase 8
(Lab cutover) — everything before cutover needs integration.

### 8.1 Persistent identity + config loader ✅ SHIPPED (2026-04-15)

**Deliverable:** station loads or generates an Ed25519 identity from
disk; config loader reads `sys.config` into a typed `#station_cfg{}`
record.

**Landed:**
- `hecate_station_identity` — `path_for/1`, `load/1`, `generate/1`,
  `load_or_generate/1`. Atomic 0600 persist via
  `macula_identity:save/2`.
- `apps/hecate_station/include/hecate_station_cfg.hrl` — the
  `#station_cfg{}` record.
- `hecate_station_config:from_env/0` — reads `sys.config` under the
  `hecate_station` app, applies `HECATE_STATION_*` env-var
  overrides, loads/generates identity, returns
  `{ok, #station_cfg{}} | {error, {bad_config, Reason}}`.
- `hecate_station_config:to_opts/1` — projects the typed record to
  the legacy `station_opts()` map the walking-skeleton server
  consumes.
- `hecate_station_sup:init/1` — starts a single
  `hecate_station_server` child when the app env is populated;
  tolerates empty env (walking-skeleton / chaos CT drive the server
  directly); returns `{stop, {bad_config, Reason}}` on parse errors.
- Tests: `hecate_station_identity_tests` (7 cases, covers cold-boot,
  warm-boot, missing-dir recovery, mode 0600, load/generate) and
  `hecate_station_config_tests` (10 cases, covers legacy `load/1`
  contract, `from_env/0` happy path, missing/bad field errors,
  env-var override, identity continuity).

**Pipeline:** `rebar3 xref/eunit/ct/dialyzer` all green
(685 eunit / 31 ct, 0 dialyzer warnings).

**Files:**
- `hecate_station_identity` — `load/1`, `generate/1`, `path_for/1`.
  Uses `file:write_file/3` with `exclusive' + `chmod 0600`.
- `hecate_station_config` (already exists Phase 0 skeleton; expand) —
  full field coverage + env-var overrides.
- `hecate_station_sup:init/1` — one child: `hecate_station_server`
  with the loaded config and identity.

**Acceptance:**
- Cold boot generates `identity.erl.bin` once; warm boot reads the
  same file and keeps the same NodeId.
- Missing file + missing-dir recovery writes the dir + file.
- Identity file owned by current user, mode 0600.
- Config parse errors surface as `{stop, {bad_config, Reason}}`.
- Eunit: identity round-trip, config happy + error paths.

### 8.2 Bootstrap → DHT → SWIM boot sequence ✅ SHIPPED (2026-04-15)

**Deliverable:** `hecate_station_sup` starts — in order —
bootstrap cascade (via `hecate_bootstrap:run/0`), DHT server (with
the station's NodeId), ingest bridge (already shipped in 6.10), and
SWIM server (seeded from DHT).

**Landed:**
- `hecate_station_bootstrap_runner` — one-shot orchestrator
  (`run/1,2`) composing `hecate_bootstrap:run/0,1` with
  `hecate_station_bootstrap:ingest/2`. Returns
  `{ok, #{peers, summary}}` on success, verbatim
  `{error, no_tiers}` to let the caller refuse SWIM, or
  `{error, {bootstrap_failed, Reason}}` for other cascade errors.
- `hecate_station_sup:start_dht/1`, `start_swim/1` — name-registering
  wrappers. Children added at boot time via
  `supervisor:start_child/2` rather than in `init/1` so the cascade
  runs between DHT start and SWIM start.
- `hecate_station_app:start/2` — boot pipeline (flat pattern-matched
  heads, no nesting):
  1. start empty sup,
  2. return early if env disabled,
  3. `from_env/0` + halt sup on `{bad_config, _}`,
  4. start DHT child,
  5. run cascade + ingest via runner,
  6. halt sup on `no_tiers` / `bootstrap_failed`,
  7. start SWIM child.
- `hecate_station:dht/0`, `swim/0` — runtime accessors
  returning `{ok, pid()} | {error, not_started}` via
  `whereis/1` on the registered names.
- `hecate_station_stub_tier` (test) — deterministic
  `hecate_bootstrap_tier` implementation for unit tests.
- Tests: `hecate_station_bootstrap_runner_tests` (4 cases: happy
  path, `no_tiers`, cascade_failed wrapping, app-env read) and
  `hecate_station_app_tests` (4 cases: disabled env yields empty sup,
  happy path boots DHT + SWIM with seeded routing table, `no_tiers`
  aborts boot cleanly, warm-boot preserves identity across app
  restarts).

**Pipeline:** `rebar3 xref/eunit/ct/dialyzer` all green
(693 eunit / 31 ct, 0 dialyzer warnings, 0 xref issues).

**Deferred:** two-station CT on loopback is deferred to 8.3 — it
needs the QUIC listener to be meaningful (two sups in one VM fight
over the `hecate_dht`/`hecate_swim` registered names; without a
listener there is nothing to "meet" over). The runtime-accessor
contract is exercised by the eunit app tests.

**Files:**
- `hecate_station_sup` gains children in a fixed startup order:
  1. `hecate_station_identity` worker.
  2. `hecate_dht` worker (takes `self_id`).
  3. `hecate_station_bootstrap_runner` — transient worker that
     calls `run/0` + `ingest/2` once, then exits normally.
  4. `hecate_swim` worker (takes seed peers from DHT at init).
- `hecate_station_server` becomes a thin façade that exposes the
  supervised components via the station API (`identity/1`,
  `peers/1`, `swim_members/1`).

**Acceptance:**
- Starting the app on an empty box reaches SWIM `alive` on at
  least `min_peers` within 10 s (using fakes in CI).
- Restarting the app reuses the same identity + cached routing
  table entries.
- If bootstrap returns `{error, no_tiers}`, the sup stops with a
  clear error (don't start SWIM with zero peers).
- CT: spin up two station sups on loopback, confirm they meet via
  the bridge.

### 8.3 QUIC listener + macula_peering integration ✅ SHIPPED (2026-04-15)

**Deliverable:** the station actually listens for incoming QUIC
connections on the configured port; incoming peers trigger
`macula_peering:handshake_and_record/3` which in turn updates the
DHT routing table with observed ASN/country/tier.

**Landed:**
- `hecate_station_peer_observer` — single gen_server that is the
  `controlling_pid` for every `macula_peering_conn' the station
  spawns (inbound via listener, outbound via `connect_to/1`). On
  `connected` it calls `hecate_dht:observe/2` with a `tier=t0`
  direct-peer spec and `hecate_swim:add_peer/3`. On `frame` it
  verifies the signature and routes SWIM frames to
  `hecate_swim:handle_frame/3`; unknown/unsigned frames are
  dropped. On `disconnected' it calls `hecate_swim:remove_peer/2`.
- `hecate_station_listener` — gen_server owning the
  `macula_transport' listener + async accept loop; each
  `{quic, new_conn, ...}' message calls `macula_peering:accept/2'
  with `controlling_pid = Observer' and re-arms accept.
- `hecate_station_sup:start_observer/1` + `start_listener/1` —
  name-registering wrappers analogous to `start_dht/1` /
  `start_swim/1`.
- `hecate_station_app:start/2` — extended pipeline: SWIM → observer
  → listener. Halt reasons added: `{observer_start_failed, _}`,
  `{listener_start_failed, _}`. After listener starts, the station
  caches a dial template (identity + realms + capabilities) in
  `persistent_term` so `connect_to/1` can build peering opts
  without re-reading the config file.
- `hecate_station` — new accessors `observer/0`, `listener/0`,
  `listen_addr/0` plus `connect_to/1` that dials a remote endpoint
  through the observer. `remember_dial_opts/1` /
  `forget_dial_opts/0` internals used by the app callback.
- Tests: `hecate_station_peer_observer_tests` (7 cases — connected
  observes tier=t0 + adds to SWIM, duplicate observe is `touched`,
  disconnected removes from SWIM, signed SWIM frames route through
  the observer, unsigned frames + frames from unknown conns are
  dropped without crashing). `hecate_station_app_tests` gained
  two cases — full-runtime boot verifies DHT + SWIM + observer +
  listener all alive and `listen_addr/0` returns the expected
  tuple; an end-to-end test dials the station from an independent
  `macula_peering:connect/1' with a fresh identity and asserts the
  external peer's NodeId lands in the station's DHT (tier=t0) +
  SWIM within 10 s.

**Pipeline:** `rebar3 xref/eunit/ct/dialyzer` all green
(701 eunit / 31 ct, 0 dialyzer warnings, 0 xref issues).

**Deferred:**
- Two-station-per-VM CT — would require per-sup instance names
  (the sup currently registers `hecate_dht`, `hecate_swim`,
  `hecate_station_peer_observer`, `hecate_station_listener`
  atoms globally). Multi-station fleet testing is Session 8.8
  via the beam cluster (separate BEAM VMs, no name collision).
  The end-to-end eunit above already proves the full path by
  dialing the station from an independent peering client.
- Rich `hecate_dht` observe spec (ASN / country inferred from
  peer IP) — needs a local `geo_check' lookup, deferred to
  8.3.x per `PLAN_DEFERRED_WORK.md`.

**Files:**
- `hecate_station_listener` — wraps the `macula_transport` NIF
  listener; owns the accept loop; each new conn spawns a
  `macula_peering` handshake under a simple_one_for_one sup.
- `hecate_station_peer_observer` — glue that takes completed
  handshakes and calls `hecate_dht:observe/2` with richer spec
  (endpoints + inferred ASN/country from IP geolocation,
  once we have a local `geo_check`).

**Acceptance:**
- Two stations on loopback connect, exchange node_records,
  show up in each other's DHT with `tier=t0`.
- Handshake failure (bad signature, expired record) drops the
  connection and does NOT update the DHT.
- `hecate_dht:observe/2` called a second time for the same peer
  returns `touched`, not `admitted` (confirms the 6.10 bridge +
  the observer stay consistent).

### 8.4 HyParView + Plumtree overlay startup per realm

**Deliverable:** for every realm the station serves (from config), a
HyParView + Plumtree pair starts and joins the realm's intra-realm
overlay using realm directory records from the DHT.

**Files:**
- `hecate_station_realm_sup` — simple_one_for_one, one child per
  realm.
- `hecate_station_realm` — `gen_server` that owns the HyParView
  view + the Plumtree state for one `RealmId`.
- Config: `realms: [#{realm_id, roles, policy_url}]` per station
  opts.

**Acceptance:**
- Two stations in the same realm exchange `realm_member_endorsement`
  records, land in each other's HyParView active views within 3 s.
- A Plumtree message from one reaches the other within 100 ms on
  loopback (single-hop).
- Shutting down one realm child does not affect the other realms.

### 8.5 Periodic re-bootstrap + routing-table persistence

**Deliverable:** stations cache their routing table to disk and
trigger a re-bootstrap on long partition / DHT size collapse.

**Files:**
- `hecate_station_cache` — periodic `ets:tab2file/2` of the
  routing table to `$DATA/cache/routing-table.ets`.
- `hecate_station_rebootstrap` — `gen_server` tracking `hecate_dht:size/1`;
  if size drops below `min_viable` (default 8) for > 60 s,
  triggers `hecate_bootstrap:run/0` again and ingests.

**Acceptance:**
- Warm boot loads cached routing table before the cascade runs
  (so stations with recent contacts don't hit the full cascade on
  every restart).
- Partition simulation (kill all peers) triggers exactly one
  re-bootstrap, not a tight loop.

### 8.6 Admin HTTP API (loopback-only; TLS deferred)

**Deliverable:** a small HTTP server (plain `inets:httpd` on
loopback `::1`) exposing `/status`, `/metrics`,
`/bootstrap/rerun`, `/bootstrap/add-peer`,
`/dht/stats`, `/swim/members`.

**Files:**
- `hecate_station_admin` — HTTP handler module + JSON encoder (uses
  OTP `json`).
- `hecate_station_admin_sup` — supervises the listener.

**Acceptance:**
- `curl --unix-socket /tmp/hecate-admin.sock http://localhost/status`
  or `curl http://[::1]:8443/status` returns the JSON described in
  `PLAN_STATION_RUNBOOK.md §5.3`.
- POSTs to `/bootstrap/rerun` trigger a re-cascade and return the
  summary.
- CT: start a station, hit each endpoint, assert payload shape.

### 8.7 Graceful shutdown (tombstone + flush)

**Deliverable:** `hecate_station:stop/1,2` publishes an owner
tombstone for the station's own node_record, flushes caches, closes
QUIC, exits 0. Crashes (unclean exit) skip the tombstone — Part 4
§11's TTL handles the abandoned record.

**Files:**
- `hecate_station_server:terminate/2` — orders the shutdown:
  1. Tell SWIM to leave cleanly.
  2. Tell overlay to broadcast `REALM_LEAVE`.
  3. Call `hecate_dht:put_record` with the tombstone (Part 6 §9.13).
  4. Flush the routing-table cache to ETS.
  5. Close QUIC listener + connections.
  6. Release identity file lock.

**Acceptance:**
- Shutdown within 5 s under normal conditions.
- Tombstone lands in peer DHTs (verified by a separate station's
  `find_local_record`).
- SIGKILL path (no terminate) does NOT corrupt `identity.erl.bin`
  nor the routing-table cache (atomic writes).

### 8.8 Multi-node CT on the beam fleet

**Deliverable:** a CT suite (run manually via the fleet script,
skipped in CI) that:
- Deploys the latest station image to beam00–beam03 via podman
  auto-update.
- Starts all four stations simultaneously with empty caches.
- Asserts cold-boot cascade completes on all four within 60 s.
- Asserts DHT is tier-diverse within 30 s of boot.
- Kills beam02; asserts the other three mark it `confirmed_failed`
  within 10 s.
- Restores beam02; asserts it rejoins SWIM + DHT within 30 s.
- Partition beam03 from beam00–02 (iptables); heal; verify
  SWIM reconverges.

**Files:**
- `scripts/fleet-ct.sh` — SSH orchestrator.
- `scripts/fleet-deploy.sh` — GitOps commit + wait for
  reconciliation.
- `test/fleet_SUITE.erl` — CT suite with a `beam_nodes' init
  per-group.
- `test/fleet_chaos.erl` — kill/partition helpers.

**Acceptance:**
- Script completes 0/0 in ≤ 5 min on healthy fleet.
- Each acceptance bar above passes.
- Report written to `/bulk0/.hecate/reports/$(date +%F)/<suite>.html`.

---

## 3. Supervision structure (target)

```
hecate_station_app
└── hecate_station_sup (one_for_one, intensity 5, period 10)
    ├── hecate_station_identity (worker, permanent)
    ├── hecate_station_listener (worker, permanent)
    ├── hecate_dht              (worker, permanent)
    ├── hecate_swim             (worker, permanent)
    ├── hecate_station_bootstrap_runner (transient — one-shot on boot)
    ├── hecate_station_rebootstrap (worker, permanent)
    ├── hecate_station_cache (worker, permanent)
    ├── hecate_station_realm_sup (simple_one_for_one, permanent)
    │   └── hecate_station_realm (per realm; started by config)
    └── hecate_station_admin_sup (one_for_one, permanent)
        └── hecate_station_admin (worker, permanent)

hecate_bootstrap_app
└── hecate_bootstrap_sup (one_for_one)
    └── hecate_bootstrap_mdns_responder (worker if configured)
```

Startup order enforced by `hecate_station_sup`'s child list (strict
left-to-right). `hecate_bootstrap_sup` starts first via
`applications` dependency.

---

## 4. Config surface — added in this sprint

Beyond what `PLAN_STATION_RUNBOOK.md §4.2` already showed:

```erlang
[
    {hecate_station, [
        …
        {realms, [
            #{realm_id => <<"…hex…">>,
              roles    => [<<"member">>],
              active_view_size   => 5,
              passive_view_size  => 20,
              plumtree_fanout    => 3}
        ]},
        {rebootstrap, #{
            min_viable_peers    => 8,
            check_period_ms     => 5_000,
            partition_window_ms => 60_000
        }},
        {cache, #{
            flush_period_ms => 30_000,
            path            => "/fast/.hecate/station/cache"
        }}
    ]}
]
```

---

## 5. Dependency matrix (what each session unlocks)

| Session | Unlocks | Needs |
|---------|---------|-------|
| 8.1 | Every later session (identity + config) | nothing |
| 8.2 | Fleet CT (8.8) | 8.1 |
| 8.3 | DHT becomes useful between real boxes | 8.2 + `macula_peering` already shipped |
| 8.4 | Multi-realm fleet testing | 8.3 + `hecate_overlay` already shipped |
| 8.5 | Warm-boot behaviour + partition recovery | 8.3 |
| 8.6 | `PLAN_STATION_RUNBOOK.md §5.3`-style observability | 8.2 |
| 8.7 | Clean Phase 8 cutover (tombstones make V1 retirement clean) | 8.3 |
| 8.8 | Phase 7 hardening can start (burn-in needs a real fleet) | 8.1–8.7 |

Parallel-friendly: 8.5, 8.6 can land in either order once 8.3 is
in. 8.7 can start as soon as 8.3 is in.

---

## 6. Acceptance checkpoint — end of sprint

The sprint is done when ALL of the following hold:

- [ ] A station boots with an empty data dir and reaches "healthy"
      (`/status` → `healthy: true`) within 30 s, using fakes.
- [ ] A second station on the same LAN joins the first one via
      Tier B mDNS within 5 s.
- [ ] Cold boot on beam cluster (4 nodes) converges (cascade + DHT
      + SWIM + overlay) within 60 s.
- [ ] Kill + restore a node → cluster heals within 30 s of restore.
- [ ] Partition + heal via iptables → SWIM reconverges within 30 s.
- [ ] Graceful shutdown writes a tombstone reachable by peers
      within 5 s.
- [ ] Warm boot reuses identity + loads cached routing table; no
      re-bootstrap unless `min_viable_peers` is violated.
- [ ] `/status`, `/metrics`, `/bootstrap/rerun`, `/dht/stats`,
      `/swim/members` all return sensible data.
- [ ] Full pipeline (`xref`, `eunit`, `ct`, `dialyzer`) green
      after every session.
- [ ] Runbook (`PLAN_STATION_RUNBOOK.md`) reviewed and updated
      where reality differs.

Once all boxes are checked, Phase 7 (hardening) can start.

---

## 7. Timing + effort estimate

| Session | Estimated AI sessions | Calendar effort |
|---------|-----------------------|-----------------|
| 8.1 Identity + config | 1 | 2–3 h |
| 8.2 Boot sequence | 1–2 | 4–8 h |
| 8.3 QUIC + peering | 2 | 8–12 h |
| 8.4 Overlay per realm | 1 | 4–6 h |
| 8.5 Cache + rebootstrap | 1 | 4 h |
| 8.6 Admin API | 1 | 4–6 h |
| 8.7 Graceful shutdown | 1 | 3 h |
| 8.8 Fleet CT | 1–2 | 6–10 h |

<b>Total:</b> 9–11 AI sessions; 2–4 calendar weeks depending on
review cadence.

---

## 8. Risks + mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| macula_peering API mismatches what 8.3 expects | Medium | Slows 8.3 | Small pre-session scope read of `macula_peering'; adapt early |
| beam cluster podman auto-update races across nodes during fleet CT | Medium | Flaky CT | `scripts/fleet-deploy.sh` waits for every node's podman to confirm the new image before asserting health |
| Cold-boot cascade budget too tight (60 s) on first real deploy | Medium | Test failures | Start with 120 s budget for fleet CT; tighten after first green run |
| Tombstone storm on mass restart | Low | Peers see many confirmed_failed + new records | Tombstones carry `valid_until`; old ones expire; no mitigation needed for dev |
| Partition test leaves iptables rules behind | High | Node loses ability to talk to anyone | `scripts/partition.sh` uses a fresh `macula-test` chain; traps exit + flushes on cleanup |

---

## 9. Trigger phrases

- `Start station integration sprint` — begins Session 8.1.
- `Continue station integration` — picks up at the next pending
  session in order.
- `Skip to Session 8.N` — jumps to a specific session if its
  dependencies are satisfied.
- `Run fleet CT now` — invokes Session 8.8 as a one-shot even if
  prior sessions have regressions (for smoke-testing on the real
  fleet).
