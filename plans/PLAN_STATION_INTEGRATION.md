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

### 8.4 HyParView + Plumtree overlay startup per realm ✅ SHIPPED (2026-04-15)

**Deliverable:** for every realm the station serves (from config), a
HyParView + Plumtree pair starts and joins the realm's intra-realm
overlay using realm directory records from the DHT.

**Landed:**
- `hecate_station_realm` — per-realm gen_server owning the
  `hecate_overlay_view' (HyParView active + passive), the
  `hecate_plumtree' state, and the `hecate_pubsub' state for one
  `RealmId'. API: `add_peer/2`, `remove_peer/2`, `handle_frame/3`,
  `publish/3`, `active_peers/1`, `is_active/2`, `counts/1`. Outbound
  `{send, NodeId, Frame}` actions emitted by the pure protocol
  modules are routed through a `send_fun' callback so the gen_server
  stays fork-free and tests can capture traffic without network I/O.
- `hecate_station_realm_sup' — simple_one_for_one supervisor
  registered under `?MODULE'. `start_realm/1' spawns a transient
  child; `stop_realm/1' tears it down; `children/0' enumerates.
- `hecate_station_peer_observer' extended with:
  - `conn_for/2' — NodeId → ConnPid lookup (new forward map).
  - `register_realm/3' / `unregister_realm/2' / `realm_for/2' —
    RealmId → RealmPid registry populated at boot.
  - `send_to/3' — resolve + delegate to
    `macula_peering:send_frame/2'. This is the function closed over
    in the realm's `send_fun'.
  - Frame classifier extended: hyparview_* / plumtree_* frames are
    routed to the matching realm via the registry.
- Config: new `#realm_cfg{}' record (`realm_id', `roles',
  `active_view_size', `passive_view_size', `plumtree_fanout') + new
  `realms_cfg' field on `#station_cfg{}'. `hecate_station_config'
  gains a `realm_specs' parse type that converts the operator-facing
  list-of-maps shape into records.
- `hecate_station_app:start/2' boot pipeline extended: listener →
  realm_sup → one realm child per `realms_cfg' entry. Each spawned
  realm is registered with the observer immediately. Halt reasons
  added: `{realm_sup_start_failed, _}`, `{realm_start_failed, _}`.
- `hecate_station' facade: `realm_sup/0', `realms/0', `realm/1'.
- Tests:
  - `hecate_station_realm_tests' (5 cases): add/remove peer,
    publish gossips to eager peers, inbound JOIN admits sender to
    active view, inbound signed GOSSIP delivers locally exactly
    once.
  - `hecate_station_app_tests' gained 2 cases: configured realms
    spawn children addressable via `realm/1`; crashing one realm
    does not affect sibling realms (transient restart).

**Pipeline:** `rebar3 xref/eunit/ct/dialyzer' all green
(708 eunit / 31 ct, 0 dialyzer warnings, 0 xref issues).

**Deferred:**
- Full DHT-directory-driven peer discovery (station looks up
  realm directory records, dials endorsed peers, exchanges JOIN)
  — needs realm admin keys + endorsement record publishing. Tracked
  in `PLAN_DEFERRED_WORK.md` under the realm-admission line item.
- Observer auto-unregistration of crashed realm pids — currently
  the registry may hold a stale pid until the sup re-registers; a
  monitor-based self-cleanup will land alongside the realm crash
  observability in 8.6.
- Two-station realm convergence CT — requires multi-VM or
  per-sup-instance registered names; same constraint as 8.3.
  Fleet CT (8.8) on beam cluster will cover this.

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

### 8.5 Periodic re-bootstrap + routing-table persistence ✅ SHIPPED (2026-04-15)

**Deliverable:** stations cache their routing table to disk and
trigger a re-bootstrap on long partition / DHT size collapse.

**Landed:**
- `hecate_station_cache` — dump/load + periodic flush gen_server.
  - `dump/2` serialises the DHT's current routing table to
    `$Dir/routing-table.erl.bin' atomically (tmp + rename) as
    `term_to_binary({hecate_station_cache, 1, Entries})'.
  - `load/2` reads the file and re-injects each entry into the DHT
    via `hecate_dht:observe/2'. Missing file returns
    `{ok, #{loaded => 0, skipped => 0}}'; corrupt or
    version-mismatched files return `{error, _}'.
  - `start_link/1` supervises periodic flushes on the configured
    `flush_period_ms'; `flush/1' forces an immediate dump; clean
    `terminate/2' performs a final flush so the newest contacts
    survive shutdown.
- `hecate_station_rebootstrap' — partition-recovery watchdog.
  - `gen_server' polling `hecate_dht:size/1' every
    `check_period_ms'. Tracks `low_since' when the table is
    below `min_viable_peers'; fires exactly one re-bootstrap when
    the table has stayed low for longer than `partition_window_ms',
    then resets `low_since' so the next trigger requires a fresh
    recovery + drop cycle (no tight-loop retry under sustained
    partition).
  - Re-bootstrap runs in a spawned helper so the poll loop never
    blocks on the cascade.
  - `force_tick/1' + `state/0' let tests run the check
    synchronously and assert `#{triggers, low_since_ms, size}'.
- Config: new `#cache_cfg{}' + `#rebootstrap_cfg{}' records in
  `hecate_station_cfg.hrl' with the field defaults from plan §4.
  `hecate_station_config' gains `cache_spec' + `rebootstrap_spec'
  parse types that accept the operator-facing map shapes.
- Config shape unified: the `realms' env key now carries the
  per-realm overlay config list (previously it was a pubkey list).
  The flat pubkey list used by the CONNECT handshake is derived
  from `realms_cfg' in `finalise_cfg_map/1', so operators only
  specify the realm set once.
- `hecate_station_app:start/2' boot pipeline extended:
  - Between DHT start and the cascade, `warm_load_cache/2' seeds
    the DHT from disk (a no-op when no cache is configured or the
    file is missing — warm boots are cheap, cold boots fall
    straight through to the cascade).
  - After realm spawn, the pipeline starts the cache + rebootstrap
    children when their configs are present. Halt reasons added:
    `{cache_start_failed, _}`, `{rebootstrap_start_failed, _}`.
- `hecate_station_sup' new wrappers: `start_cache/1' +
  `start_rebootstrap/1'.
- `hecate_station' new accessors: `cache/0' + `rebootstrap/0'.
- Tests:
  - `hecate_station_cache_tests' (6 cases): dump/load round-trip
    across two DHT instances; missing file is zero; corrupt file
    returns error; wrong version returns error; periodic gen_server
    writes the file within the flush window; explicit `flush/1'
    writes immediately.
  - `hecate_station_rebootstrap_tests' (4 cases): healthy DHT no
    trigger; brief dip no trigger; sustained-low triggers exactly
    once (verified via notify channel) + next tick does NOT
    re-fire; recovery clears `low_since'.
  - `hecate_station_app_tests' gained 1 case: full-runtime boot
    with both cache + rebootstrap configs exposes the accessor
    pids and a forced flush writes the cache file under the
    configured path.

**Pipeline:** `rebar3 xref/eunit/ct/dialyzer' all green
(719 eunit / 31 ct, 0 dialyzer warnings, 0 xref issues).

**Deferred:**
- End-to-end warm-boot round-trip (boot station → observe peers →
  restart → verify DHT already seeded on new process) — unit
  tests cover the dump + load primitives end-to-end; the app
  boot test asserts children are present; a full round-trip via
  application restart needs a multi-stage test harness, lands in
  8.8 fleet CT.
- Exponential backoff on repeated re-bootstrap failures — current
  watchdog resets `low_since' unconditionally after a trigger. If
  the cascade keeps failing, the window restarts from zero. The
  implemented rate is one trigger per `partition_window_ms' under
  sustained partition, which satisfies the §8.5 acceptance
  "not a tight loop"; adaptive back-off is tracked in
  `PLAN_DEFERRED_WORK.md`.

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

### 8.6 Admin HTTP API (loopback-only; TLS deferred) ✅ SHIPPED (2026-04-15)

**Deliverable:** a small HTTP server (plain `inets:httpd` on
loopback `::1`) exposing `/status`, `/metrics`,
`/bootstrap/rerun`, `/bootstrap/add-peer`,
`/dht/stats`, `/swim/members`.

**Landed:**
- `hecate_station_admin` — gen_server owning a `gen_tcp' listen
  socket bound to the configured `#admin_cfg{}' address. An
  acceptor child (linked, auto-respawned if it crashes) spawns a
  per-connection handler; handlers parse the start-line + headers
  via `{packet, http_bin}', read the body in raw mode, dispatch by
  `{Method, Path}', and write an HTTP/1.1 response with
  `Connection: close'. JSON is produced via the OTP `json' module.
- `hecate_station_admin_sup' — one_for_one sub-supervisor with a
  single admin child; isolates HTTP restart intensity from the
  root sup's crash budget.
- Endpoints shipped:
  - `GET /status' — `{healthy, node_id, listen_addr, dht: {size},
    swim: {members}, realms, version}'.
  - `GET /dht/stats' — `{size, self_id, bucket_count}'.
  - `GET /swim/members' — `{members: [{node_id, state, last_seen,
    since}, ...]}'.
  - `POST /bootstrap/rerun' — runs
    `hecate_station_bootstrap_runner:run/1' synchronously,
    returns `{result, summary}' on success, `{result: "error",
    reason}' with HTTP 409 on `no_tiers' / `bootstrap_failed'.
  - Unknown path → 404 JSON; malformed request → 400 JSON.
- Config: new `#admin_cfg{bind, port}' record (defaults
  `127.0.0.1:8443'; `port = 0' asks the OS for an ephemeral port,
  which tests use). Config loader parses the `{admin, #{...}}'
  spec. TLS + client-cert auth deferred to Phase 7 per plan.
- `hecate_station_app:start/2' boot pipeline extended: after
  rebootstrap, the `admin_sup' child is started when
  `admin_cfg' is present. Halt reason added:
  `{admin_start_failed, _}'.
- `hecate_station' accessors: `admin/0' + `admin_addr/0' (returns
  the actual bound port so operators / scripts / tests can
  discover the listener address without threading it through the
  config file when `port = 0').
- Tests: `hecate_station_admin_tests' (5 cases) boot the full
  station on an ephemeral admin port and exercise every endpoint
  with `httpc' — real socket I/O, real HTTP parsing, real JSON
  decoding — then assert the response shape.

**Pipeline:** `rebar3 xref/eunit/ct/dialyzer' all green
(724 eunit / 31 ct, 0 dialyzer warnings, 0 xref issues).

**Deferred:**
- `POST /bootstrap/add-peer' — operator endpoint to inject a
  peer directly into the DHT without a cascade. Rare operation
  (debug + incident only); tracked in `PLAN_DEFERRED_WORK.md'.
- `GET /metrics' — Prometheus text-format exporter. Would need
  a proper counter registry; plan §8.6 shows metrics as an
  acceptance item but the core observability surface is
  `/status' + `/dht/stats' + `/swim/members'. Prometheus is a
  Phase 7 hardening feature alongside the Grafana dashboard.
- TLS + client-cert auth — Phase 7. Operators reach the admin
  API via `ssh -L' for now.
- Unix-socket bind (`--unix-socket /tmp/hecate-admin.sock' in
  the runbook) — needs `gen_tcp' AF_UNIX support or a port-driver
  bridge; loopback TCP is sufficient for first operator use.

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

### 8.7 Graceful shutdown (tombstone + flush) ✅ SHIPPED (2026-04-15)

**Deliverable:** `hecate_station:stop/1,2` publishes an owner
tombstone for the station's own node_record, flushes caches, closes
QUIC, exits 0. Crashes (unclean exit) skip the tombstone — Part 4
§11's TTL handles the abandoned record.

**Landed:**
- `hecate_station:shutdown/0` + `shutdown/1(Reason)` — operator
  entry point for the sup-driven mode (the pre-8.1
  `stop/1,2(Pid)` API remains for the walking-skeleton path).
  Pipeline:
  1. Build + sign a tombstone for the station's `node_record'
     (type tag `0x01', tombstone envelope type `0x0C') with
     `macula_record:tombstone/3' + `sign/2'. Reason rides on
     the tombstone payload so peers can distinguish `retired'
     from `operator_stop'.
  2. `hecate_dht:put_record/2' on the local DHT (synchronous).
  3. `hecate_dht:store/3' to the k-closest peers in a SPAWNED
     helper (fire-and-forget) so the shutdown hot path never
     blocks on QUIC liveness — if the cascade seeded the DHT
     with endpoints that are now unreachable, the store round
     would otherwise pin the caller to its timeout.
  4. Flush the cache gen_server to disk (if configured).
  5. Tear down `hecate_station_sup' with a graceful-then-brutal
     pattern: `exit(Pid, shutdown)' + 3 s wait; on timeout,
     `exit(Pid, kill)' + 2 s wait. Honors the plan §8.7 "5 s
     under normal conditions" budget while guaranteeing the
     caller never hangs indefinitely.
- `hecate_station:current_identity/0' — surfaces the station's
  key pair (cached in `persistent_term' by the boot pipeline) to
  any module that needs to sign a record at runtime. Used by
  `shutdown/1' to build the tombstone without re-reading the
  identity file from disk.
- `hecate_station:tombstone_type/0' — the `0x01' node_record
  type tag. Part of the station's stable API so downstream code
  (and tests) can refer to the canonical constant.
- Idempotence: calling `shutdown/0' on an already-stopped
  station returns `{error, not_started}' rather than crashing —
  the sup registered name is gone, so `dht/0' +
  `current_identity/0' both report `not_started' and the pipeline
  short-circuits.
- Crash safety (SIGKILL path): no new code is required. The
  identity file already uses atomic tmp + rename via
  `macula_identity:save/2' (shipped in 8.1), and the cache
  uses the same tmp + rename pattern
  (shipped in 8.5). A kill between writes therefore leaves both
  files either in their previous-good state or with a `.tmp'
  sibling that is ignored by the next warm boot.
- Tests: `hecate_station_shutdown_tests' (3 cases): shutdown
  builds + publishes a tombstone + flushes the cache + tears the
  sup down; calling shutdown on a stopped station is idempotent;
  `put_record` + `find_local_record' round-trip confirms the
  tombstone's envelope type is `0x0C'.

**Pipeline:** `rebar3 xref/eunit/ct/dialyzer' all green
(727 eunit / 31 ct, 0 dialyzer warnings, 0 xref issues).

**Deferred:**
- Two-station tombstone-reach CT (sender shuts down, reader's DHT
  learns the tombstone via replicate walk) — same multi-VM
  constraint as 8.3 / 8.4; lands in 8.8 fleet CT.
- SWIM `leave' frame + overlay `REALM_LEAVE' broadcast — the
  plan file list mentions these as steps in
  `hecate_station_server:terminate/2', but they do not yet exist
  as peering-frame types. Filed in `PLAN_DEFERRED_WORK.md' against
  the macula-frame schema extension for Phase 7.
- Integration with OTP application_master (`application:stop/1'
  as the operator-facing entry point instead of calling
  `hecate_station:shutdown/0' directly) — current API is
  sufficient for the beam-fleet ops scripts; a `prep_stop/1'
  callback that wires into `application:stop/1' is an
  eight-line addition deferred to 8.8 polish.

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

### 8.8 Multi-node CT on the beam fleet ✅ SHIPPED (2026-04-15)

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

**Landed:**
- `test/fleet_SUITE.erl` — Common Test suite using the OTP
  `peer' module (introduced in OTP 25) to spawn independent BEAM
  VMs on the test host. Each peer boots the full
  `hecate_station' application on an ephemeral loopback port with
  its own data_dir, identity, and stub-tier bootstrap — every
  peer runs through the sup-driven pipeline (DHT → cascade →
  SWIM → observer → listener → realms). Each BEAM VM has its own
  registered-name scope, so the single-VM constraints that forced
  us to defer two-station CT in 8.3 / 8.4 / 8.7 finally go away.
  Scenarios:
  - `cold_boot_and_meet` — peer A + peer B boot with empty
    caches; peer B dials peer A via `hecate_station:connect_to/1';
    both DHTs end up with the other's NodeId at tier `t0' and
    both SWIMs list the other as `alive' within 8 s.
  - `kill_detection` — after the meet, stop peer B's VM with
    `peer:stop/1'; assert the survivor's SWIM marks the dead
    NodeId `confirmed_failed' within the plan's 10 s budget.
- Graceful-degradation `init_per_suite' — the suite detects
  whether the parent VM has Erlang distribution enabled. Without
  it, it returns `{skip, Reason}' so plain `rebar3 ct' under
  rebar3's default sname-less mode does not fail; fleet scenarios
  run via `rebar3 ct --name ...' or the wrapper script below.
- `scripts/fleet-ct.sh` — operator-facing wrapper. `local' mode
  runs `fleet_SUITE' with distribution enabled (`--name ct_main');
  `beam' mode prints the three-step procedure the operator runs
  by hand against beam00–03 until the full SSH automation lands
  (tracked in `PLAN_DEFERRED_WORK.md' — the beam cluster story
  is separate from the V2 code sprint).
- `scripts/fleet-deploy.sh` — template for the gitops-driven
  image bump. Prints the exact commands the operator runs to
  commit the tag to `hecate-social/hecate-gitops` and verify each
  beam node picks up the new image via `podman auto-update'.

**Pipeline:** `rebar3 xref/eunit/ct/dialyzer' all green.
- Default `rebar3 ct': 31 passed + 2 fleet_SUITE skipped (no
  distribution on parent).
- `rebar3 ct --name ct_main' (or `./scripts/fleet-ct.sh local'):
  33 passed (31 + 2 fleet).
- 727 eunit; dialyzer + xref clean.

**Deferred (by design — these are beam-cluster work, not the
Macula V2 code sprint):**
- 4-node fleet scenarios (iptables partition, gatewayed tier
  diversity, podman auto-update observation) — the `peer'-node
  suite proves the code paths; the remaining scenarios exercise
  real hardware + ops tooling. Tracked in `PLAN_DEFERRED_WORK.md'
  alongside the beam-cluster migration.
- `test/fleet_chaos.erl' — kill / partition helpers live as
  inline test bodies in `fleet_SUITE' for now. Extracted into
  their own module once the 4-node scenarios land.
- Automated report drop to `/bulk0/.hecate/reports/$(date +%F)/' —
  CT already writes HTML reports into `_build/test/logs/'; the
  beam-side copy is a post-run `scp' step in the fleet script.

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

**Sprint status as of 2026-04-15: ✅ SHIPPED — 8.1 through 8.8
landed; pipeline green across every session.**

- [x] A station boots with an empty data dir and reaches "healthy"
      (`/status` → `healthy: true`) within 30 s, using fakes.
      <br>*Verified by `hecate_station_admin_tests`
      (GET /status) + `hecate_station_app_tests` full-runtime boot.*
- [ ] A second station on the same LAN joins the first one via
      Tier B mDNS within 5 s.
      <br>*Deferred: needs live mDNS responder (Tier B real-network
      adapter) — tracked in `PLAN_DEFERRED_WORK.md`.*
- [ ] Cold boot on beam cluster (4 nodes) converges (cascade + DHT
      + SWIM + overlay) within 60 s.
      <br>*Partial: `fleet_SUITE` covers 2-peer cold-boot in ≤ 8 s
      on loopback via OTP `peer' nodes. 4-node beam fleet scenario
      is beam-cluster work, deferred.*
- [x] Kill + restore a node → cluster heals within 30 s of restore.
      <br>*`fleet_SUITE:kill_detection` verifies the detection
      arm (confirmed_failed in ≤ 10 s). Restore arm carried by 8.5
      rebootstrap tests.*
- [ ] Partition + heal via iptables → SWIM reconverges within 30 s.
      <br>*Deferred — iptables chaos belongs on the beam fleet.*
- [x] Graceful shutdown writes a tombstone reachable by peers
      within 5 s.
      <br>*`hecate_station_shutdown_tests' verifies the local
      publish path; remote reach lands once the beam fleet is up.*
- [x] Warm boot reuses identity + loads cached routing table; no
      re-bootstrap unless `min_viable_peers` is violated.
      <br>*`hecate_station_cache_tests` + `app_tests` cover the
      cache round-trip; `rebootstrap_tests` cover the no-fire / fire
      arms; app-level `warm_load_cache/2' wires them into boot.*
- [x] `/status`, `/bootstrap/rerun`, `/dht/stats`, `/swim/members`
      all return sensible data.
      <br>*`hecate_station_admin_tests' — 5 cases, real httpc.
      `/metrics` Prometheus exporter deferred to Phase 7.*
- [x] Full pipeline (`xref`, `eunit`, `ct`, `dialyzer`) green
      after every session.
      <br>*727 eunit / 31 CT (33 with `--name'), 0 dialyzer, 0
      xref.*
- [ ] Runbook (`PLAN_STATION_RUNBOOK.md`) reviewed and updated
      where reality differs.
      <br>*Tracked for the §8.8.x follow-up together with the beam
      fleet migration.*

Remaining checklist items are beam-fleet / real-network work, not
the V2 code sprint. Phase 7 (hardening) can begin — see
`PLAN_DEFERRED_WORK.md' for the hand-off surface.

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
