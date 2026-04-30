# PLAN_STATION_RUNBOOK.md — Run and troubleshoot Hecate stations

**Audience:** whoever is on-call when a Macula V2 station
misbehaves. Also: whoever is running multi-node acceptance tests on
the real fleet.

**Scope:** every operational concern — boot, configure, observe,
diagnose, recover, cut — mapped onto the <em>actual</em> fleet we
own. This is not a hypothetical — it assumes you can SSH to the
boxes listed in §2, push Docker images to ghcr.io, and read Erlang
crash dumps.

Cross-reference:
- `PLAN_STATION_INTEGRATION.md` — how the running station was
  assembled (session-by-session).
- `PLAN_DEFERRED_WORK.md` — what is still known-missing.
- `PLAN_PHASE_6_BREAKDOWN.md` — bootstrap cascade specifics.
- `PLAN_MACULA_V2_PART4_LIFECYCLE.md` — what "healthy" means.
- `PLAN_MACULA_V2_PART8_VERIFICATION.md` — acceptance bars.

---

## 1. Invariants the runbook enforces

- Every station has a <b>unique</b> NodeId (32-byte Ed25519
  pubkey). Never reuse. Loss of private key = retire the NodeId.
- <b>Every running node persists</b> identity + routing-table cache +
  own tombstones to disk (`~/.hecate/station/…'). Rebooting does
  not lose NodeId.
- <b>Every deployment is reproducible</b>: same commit hash + same
  `sys.config' + same OTP release digest → byte-identical behaviour.
- <b>Every production station is observable</b>: `/status',
  `/metrics', `journalctl', and a live `remsh' shell all work.
- <b>No one modifies production by SSH alone</b>: changes go
  through `macula-demo' GitOps. SSH is for observation, tombstones,
  and one-shot recovery.

---

## 2. Fleet topology

### 2.1 What a station is

A station is an **IPv6-reachable always-on host** — either with a
public AAAA record, a self-owned DNS name, an internal-only
corporate name, or (for some deployments) no DNS at all, reached
only through the bootstrap cascade. It is **not** a laptop, not a
workstation, not user hardware. User machines run
`hecate-daemon', which is outbound-only and dials a station; see
the daemon repo for that story.

### 2.2 Deployment categories we design for

The plan targets four operator classes. Each has a different
network shape; the cascade + the station's optional subsystems
(mDNS responder, admin API, cache, etc.) are scoped to serve all
four.

| Category | IPv6 | DNS | LAN siblings | Adversarial pressure |
|----------|------|-----|--------------|---------------------|
| **Telco** — tower / street cabinet | Carrier-allocated fixed prefix per cabinet. | AAAA varies by carrier policy; internal-only common. | Possibly — cabinets on the same backhaul segment can be link-local-adjacent. | Carrier egress filtering; occasional DoH / UDP restrictions. |
| **Data centre** — hyperscaler to small colo | Provider block, fixed. | Almost always public AAAA. | Yes, if operator runs siblings in the same rack / VLAN. | Cloud providers filter multicast; transit generally open. |
| **Corporate on-prem** — server room, fixed /48 or /56 | Corp-allocated block. | Often internal-only DNS; public AAAA absent. | Usually — multiple stations on a shared /64. | Egress firewalls may block DoH / chain RPC selectively. |
| **Tech-savvy individual** — fixed public /56 or /64 from ISP | Fixed residential allocation. | Self-owned domain (if any). | Rarely — typically one station per operator. | Residential ISP filtering; DPI in some jurisdictions. |

Cascade coverage against these categories:

- **Tier A (DoH + PKARR)** — universal primary. Works for any
  station with outbound HTTPS. Onboards a fresh operator with no
  prior knowledge of peers.
- **Tier B (mDNS)** — on-prem + small DC + telco where stations
  are link-local-adjacent. Irrelevant to hyperscaler DC
  (multicast filtered) and hobbyist single-station setups.
- **Tier C (BT-DHT)** — censorship-resistance across every
  category. Any environment that blocks DoH + DNS but not
  BitTorrent.
- **Tier D (chain anchors)** — deepest censorship resistance.
  Blocking the mechanism costs a nation-state blocking public
  chain RPC mirrors.
- **Tier E (operator paste)** — universal manual override + the
  only mechanism for airgapped labs.

### 2.3 The simulation rig — where we run tests today

The following hosts are the **test rig**. They let us exercise
the station code against real networking without waiting for
operators to deploy. They are not the design target; the four
categories above are.

#### 2.3.1 Attended workstations (laptops) — dev only

| Box | Role | Notes |
|-----|------|-------|
| `work-laptop` | rebar3 shell / eunit / ct harness | Not a station — a dev target that runs station code under test. |

Purpose: rapid iteration, unit + CT runs, manual chaos in
`rebar3 shell'. Data dir `~/.hecate/station-dev/<profile>/' —
each profile is a disposable NodeId. A laptop is NEVER a
production station.

#### 2.3.2 BEAM cluster (staging / integration rig)

| Node | Address | RAM | NVMe | Bulk | Role in the rig |
|------|---------|-----|------|------|------|
| beam00 | 192.168.1.10 | 16 GB | 224 GB | 1 × 932 GB HDD | Multi-station integration, cache persistence under real I/O |
| beam01 | 192.168.1.11 | 32 GB | 224 GB | 2 × 932 GB HDD | Larger steady-state workloads |
| beam02 | 192.168.1.12 | 32 GB | 224 GB | 2 × 932 GB HDD | Chaos target (kill / restart / partition) |
| beam03 | 192.168.1.13 | 32 GB | 932 GB | 2 × 932 GB HDD | Archive target + spare NVMe |

These are **on-prem test hardware** behaving like a "corporate
on-prem" deployment. Access `ssh rl@beam0X.lab'. Runtime
systemd-user + podman. Station data `/fast/.hecate/'. They
validate the on-prem deployment category against real hardware
without any production consequence.

#### 2.3.3 Public-internet test stations (currently running V1 code)

| Box | Region | Role |
|-----|--------|------|
| relays-hetzner-nuremberg | DE | DC-class test target |
| relays-hetzner-helsinki  | FI | DC-class test target |
| relays-linode-paris      | FR | DC-class test target |

These host **V1 `macula-relay' code** (frozen at 1.4.23) while
V2 stations exist only on the BEAM cluster. They validate the
DC deployment category against cross-region IPv6. Cutover to V2
per `PLAN_DEFERRED_WORK.md §4'.

#### 2.3.4 Production (post-cutover)

`macula.io' on Linode via Docker Compose. Today: V1 only. After
the V2 cutover, a V2 station; still a DC-category deployment, not
the full fleet.

### 2.4 What the rig does NOT cover

The simulation deliberately leaves gaps that only real operators
can close:

- **Telco street-cabinet networking** — carrier-grade NAT,
  IPv6-only backhaul quirks, radio-link flapping.
- **Corporate egress firewalls** — actual DoH-blocked / UDP-blocked
  corporate networks. The beam cluster has no such filtering.
- **Residential ISP filtering** — we have clean uplinks from
  Hetzner / Linode / home-ISP; adversarial ISPs are not in
  the rig.

These categories will validate themselves in the field once we
have operators in each. Until then, treat them as design targets
to keep faithful to, not as things our CT covers.

---

## 3. Station boot sequence (as shipped, post-§8.4 reversal)

Stations are realm-agnostic infrastructure. Realm identity /
overlay lives in a separate `hecate-realm' / `macula-realm'
service (deferred — see `PLAN_DEFERRED_WORK.md §6'). The station
provides the mesh; any realm service dials it like any other peer.

```
 1. beam VM starts → kernel + stdlib + crypto + ssl + inets up.
 2. macula_station_app:start/2 → macula_station_sup:start_link/0
    (empty children list initially).
 3. Config loaded via macula_station_config:from_env/0.
    Identity loaded or generated at `{data_dir}/identity.erl.bin'
    (mode 0600, atomic tmp+rename).
 4. DHT child (macula_dht) started via macula_station_sup wrapper,
    registered under the `macula_dht' atom. self_id =
    Ed25519 public key.
 5. Warm cache load: if cache_cfg present, macula_station_cache:load/2
    re-injects persisted entries into the DHT before the cascade runs.
 6. Bootstrap cascade — macula_station_bootstrap_runner:run/1 →
    macula_bootstrap:run/0 → macula_station_bootstrap:ingest/2.
    `{error, no_tiers}' halts the sup with a clear reason.
 7. SWIM child (macula_swim) started; seeded from the DHT via the
    peer observer in step 9.
 8. Observer child (macula_station_peer_observer). Single
    controlling_pid for every peering worker. Routes SWIM frames
    only; application-layer frames pass end-to-end between peers.
 9. Listener child (macula_station_listener) — QUIC accept loop.
    Each new_conn → macula_peering:accept with observer as
    controlling_pid.
10. Cache child (macula_station_cache) — periodic flush, if
    cache_cfg present.
11. Rebootstrap child (macula_station_rebootstrap) — partition
    watchdog, if rebootstrap_cfg present.
12. Admin sub-sup (macula_station_admin_sup) — loopback HTTP API,
    if admin_cfg present.
```

Each Sup child:
- has `restart => permanent' (or `transient' for one-shot workers),
- named children register under fixed atoms so the facade can
  resolve them (`macula_station:dht/0', `swim/0', etc.),
- graceful shutdown via `macula_station:shutdown/0,1' or OTP
  `application:stop(macula_station)' (publishes tombstone + flushes
  cache before tearing the sup down).

---

## 4. Configuration layout

### 4.1 Files

```
~/.hecate/station/
├── identity.erl.bin        # Ed25519 private key (mode 0600)
├── pubkey.hex              # Human-readable pubkey
├── config/
│   ├── sys.config          # OTP application config
│   └── vm.args             # BEAM flags
├── cache/
│   ├── routing-table.ets   # Persisted DHT entries
│   └── tombstones/         # Pending tombstones
└── log/                    # Rotated logs (if not going to journald)
```

### 4.2 Minimum `sys.config` (as shipped)

Keys are the exact names `macula_station_config:from_env/0' parses.
`HECATE_STATION_*' env-var overrides are available for a subset
(see `macula_station_config' source for the list).

```erlang
[
    {macula_station, [
        %% Required.
        {data_dir, "/fast/.hecate/station"},
        {bind,     "::"},                            %% IPv6 any
        {port,     7443},
        {certfile, "/fast/.hecate/station/cert.pem"},
        {keyfile,  "/fast/.hecate/station/key.pem"},
        %% Optional.
        {capabilities, 16#FF},
        {cache, #{path            => "/fast/.hecate/station/cache",
                  flush_period_ms => 30_000}},
        {rebootstrap, #{min_viable_peers    => 8,
                        check_period_ms     => 5_000,
                        partition_window_ms => 60_000}},
        {admin, #{bind => "127.0.0.1", port => 8443}}
    ]},
    {macula_bootstrap, [
        {tiers, [
            {macula_bootstrap_tier_a, #{
                resolvers => [
                    {macula_bootstrap_doh_http, <<"https://1.1.1.1/dns-query">>},
                    {macula_bootstrap_doh_http, <<"https://9.9.9.9/dns-query">>},
                    {macula_bootstrap_doh_http, <<"https://doh.mullvad.net/dns-query">>}
                ],
                corroboration => 2, timeout_ms => 1500}},
            {macula_bootstrap_tier_b, #{
                handshake_fun => fun macula_peering:handshake_and_record/3,
                timeout_ms    => 2000}},
            {macula_bootstrap_tier_c, #{
                dht_transport => macula_bootstrap_dht_udp,   %% when 6.5.x lands
                timeout_ms    => 10_000}},
            {macula_bootstrap_tier_d, #{
                chains => [
                    {macula_bootstrap_chain_eth_jsonrpc,
                     #{endpoint => <<"https://eth.llamarpc.com">>,
                       contract => <<"0x…foundation…">>,
                       topic    => <<"0x…AnchorPublished…">>}},
                    {macula_bootstrap_chain_esplora,
                     #{base_url => <<"https://blockstream.info/api">>,
                       address  => <<"bc1q…foundation…">>}}],
                timeout_ms => 20_000}},
            {macula_bootstrap_tier_e, #{
                peer_urls => []       %% CLI-added as needed
            }}
        ]},
        {cascade_opts, #{min_peers => 3, timeout_ms => 60_000}},
        {doh_zone_base, <<"macula.io">>},
        {responder, disabled}            %% avahi collision; opt-in
    ]},
    {kernel, [
        {logger_level, info},
        {logger, [
            {handler, default, logger_std_h,
             #{formatter => {logger_formatter,
                             #{template => [time, " ", level, " ",
                                            mfa, ":", line, " ",
                                            msg, "\n"]}}}}]}
    ]}
].
```

### 4.3 `vm.args`

```
-name hecate@{hostname}.macula.beam
-setcookie macula-station-<fleet-name>
+K true
+A 32
+sbwt very_short
+sbwtdcpu very_short
+sbwtdio very_short
-kernel inet_dist_use_interface {10,0,0,0}
```

### 4.4 Environment variables (container mode)

| Var | Purpose | Default |
|-----|---------|---------|
| `HECATE_STATION_CONFIG' | absolute path to sys.config | `/etc/hecate/sys.config' |
| `HECATE_STATION_DATA' | data root | `/fast/.hecate/station' |
| `HECATE_STATION_IDENTITY' | private key path | `$DATA/identity.erl.bin' |
| `MACULA_FOUNDATION_PUBKEYS' | comma-sep hex pubkeys, override firmware | unset (use embedded placeholders) |
| `MACULA_DOH_ENABLE' | gated-suite switch | `0' |
| `MACULA_ETH_ENABLE' | gated-suite switch | `0' |
| `MACULA_BTC_ENABLE' | gated-suite switch | `0' |

---

## 5. Observability

### 5.1 Structured logs

- BEAM uses the `logger' framework (OTP 27+).
- Production writes to journald via `logger_std_h' + systemd
  journaling of stdout.
- Dev writes to the shell.

Every important event emits a structured log line:

```
2026-04-15T13:42:01 info macula_bootstrap:cascade/2 started
    tiers=[a,b,c,d,e] min_peers=3 timeout_ms=60000
2026-04-15T13:42:02 info macula_bootstrap_tier_a:probe/1 returned
    peers=20 corroboration_hit=true
2026-04-15T13:42:02 info macula_station_bootstrap:ingest/2 summary
    observed=20 admitted=20 touched=0 replaced=0 rejected=0
```

Log levels:
- `debug': high-cardinality traces (off in prod).
- `info': lifecycle transitions, cascade results, DHT admissions,
  SWIM state changes.
- `notice': interesting but non-critical (rare duplicate pubkey
  observed, DHT bucket full).
- `warning': degraded but functional (Tier A failed, fell through
  to Tier B).
- `error': something broke (`cascade_failed', SWIM confirmed_failed
  on self, QUIC listener crashed).
- `critical': crash loop, unrecoverable (identity file unreadable,
  foundation pubkey trust-boundary rejected).

### 5.2 `/metrics' (Prometheus)

**Deferred to Phase 7 hardening.** The admin API shipped in §8.6
with four JSON endpoints (`/status', `/dht/stats',
`/swim/members', `/bootstrap/rerun'); a text-format Prometheus
exporter + counter registry lands alongside the Grafana dashboard.
Sample metrics we will expose:

```
macula_station_up{node_id="…"} 1
macula_station_uptime_seconds{…} 12345
macula_bootstrap_cascade_duration_ms{…,winning_tier="a"} 421
macula_dht_size{…} 87
macula_swim_alive_members{…} 14
macula_swim_suspected_members{…} 1
macula_swim_confirmed_failed_members{…} 2
```

Overlay / realm metrics move to the future `hecate-realm' /
`macula-realm' service (they are not station concerns).

### 5.3 `/status' (JSON — as shipped)

```json
{
    "healthy":     true,
    "node_id":     "…hex…",
    "listen_addr": "127.0.0.1:7443",
    "dht":         { "size": 87 },
    "swim":        { "members": 14 },
    "realms":      [],
    "version":     "0.1.0-phase1"
}
```

The `realms' field stays in the response shape for operator-tool
compatibility but always returns an empty list — stations are
realm-agnostic infrastructure. Richer DHT + SWIM detail comes
from `/dht/stats' and `/swim/members'.

### 5.4 Live shell access

```bash
# on the beam node
ssh rl@beam00.lab
systemctl --user status hecate-daemon
journalctl --user -u hecate-daemon -f

# open a remsh to peek inside
erl -name "remsh@beam00.macula.beam" \
    -setcookie macula-station-…\
    -remsh hecate@beam00.macula.beam
```

Useful one-liners inside the shell:

```erlang
%% Station facts
macula_station:version().
macula_station:identity(SupPid).           %% Ed25519 key_pair
macula_station:listen_addr(SupPid).

%% Bootstrap
macula_bootstrap:run().                    %% force a re-cascade

%% DHT
macula_dht:stats(DhtPid).
macula_dht:k_closest(DhtPid, Target, 20).
macula_dht:siblings(DhtPid).

%% SWIM
macula_swim:members(SwimPid).
```

(Overlay introspection is not a station concern — it lives in the
future `hecate-realm' / `macula-realm' service.)

### 5.5 Telemetry events (emitted via `telemetry' library)

- `[hecate, bootstrap, cascade, start]' / `[…, stop]'
- `[hecate, bootstrap, tier, a | b | c | d | e, probe]'
- `[hecate, dht, observe]'
- `[hecate, dht, lookup, start]' / `[…, stop]'
- `[hecate, swim, state, change]'

Observed by a per-station `telemetry_handler' that writes
to logger + updates Prometheus gauges.

---

## 6. Troubleshooting — by symptom

### 6.1 "Station won't start"

1. `systemctl --user status hecate-daemon' on the box. Check the
   last crash log.
2. If `exit_status=1' with no log, check disk: `df -h /fast'. Full
   disk = ETS persistence fails, sup restarts loop.
3. If `identity.erl.bin' missing + not recoverable → generate a new
   identity (ONLY if the old one is truly gone — the NodeId is
   gone forever otherwise).
4. If port 7443 already in use: `ss -tulpn | grep 7443' to find the
   stale process; `systemctl --user kill' it.
5. If `sys.config' parse fails: `erl -noshell -eval 'file:consult("…")' '
   to see the parse error.

### 6.2 "No peers after boot / cascade_failed"

1. `curl -s --unix-socket …/admin.sock http://localhost/status' → check
   `bootstrap.last_run' / `winning_tier' / `peers_ingested'.
2. `journalctl --user -u hecate-daemon --since "10 minutes ago" |
   grep macula_bootstrap'.
3. Identify which tier failed:
   - Tier A failure likely means DoH resolvers unreachable or
     foundation pubkey mismatch. Run:
     ```
     curl -s 'https://1.1.1.1/dns-query?name=_pkarr.<b32>.macula.io&type=TXT' \
         -H 'accept: application/dns-message'
     ```
   - Tier B silent: check mDNS with `avahi-browse -a' or `dns-sd -B'.
     Is port 5353 reachable? Are peers announcing?
   - Tier C silent: foundation may not have published BEP 44 items
     yet (6.6.y / 6.3.6 blockers).
   - Tier D silent: confirm `MACULA_ETH_ENABLE=1' / `MACULA_BTC_ENABLE=1'
     and endpoints reachable with `curl'.
4. Manual recovery: add operator peer URL:
   ```
   hecate bootstrap add-peer 'macula-peer:…base64url…'
   ```
   then trigger re-cascade:
   ```
   hecate bootstrap rerun
   ```
5. If all automated tiers fail and operator paste also fails: check
   upstream connectivity (`ping6 2001:4860:4860::8888'). Could be
   ISP-level IPv6 blackhole.

### 6.3 "DHT lookups fail"

1. `macula_dht:stats(Dht)' — `size' should be > 16 after bootstrap.
   If it's 0, the ingest step didn't happen (see 6.2).
2. `macula_dht:siblings(Dht)' — empty sibling set = very small
   network, not a bug.
3. `macula_dht:ping_peer(Dht, PeerId)' — if every peer times out,
   QUIC stack is broken (check `macula_transport' logs).
4. `macula_dht:lookup_nodes(Dht, Target)' — `{error, no_progress}'
   after N rounds means the routing table is stale; force a
   re-bootstrap.

### 6.4 "SWIM flaps members"

1. `macula_swim:members(Swim)' — observe the churn. Is it symmetric
   (all peers see the same flap) or asymmetric (only one peer
   flaps)?
2. If asymmetric: local node's uplink is the problem (packet loss).
   Lifeguard self-awareness (L2) is the mitigation; until then,
   manually increase `suspect_timeout_ms' in `sys.config'.
3. If symmetric: the suspected peer IS having trouble. Check its
   own logs via `ssh rl@<peer>.lab journalctl'.
4. Cross-check the DHT: if SWIM says failed but DHT lookups to that
   peer still return answers, Lifeguard would have refuted — note
   the discrepancy for post-mortem.

### 6.5 "High CPU / memory"

1. `erlang:memory().' inside remsh.
2. `recon:proc_count(memory, 10).' to find the top 10 processes by
   memory. (Require `recon' on the classpath.)
3. `recon:bin_leak(10).' for binary refcount leaks.
4. ETS growth: `ets:all()' + `ets:info(Tab, size)' on each.
5. Likely culprits: unbounded tombstone list (check
   `macula_dht:record_count(Dht)'), unbounded SWIM event log, or
   an errant `subscribe/3' handler that never unsubscribes.
6. Short-term mitigation: `erlang:garbage_collect()' across all
   processes. If that only delays the problem, it's a leak — open
   an issue with the process id and stack.

### 6.6 "Can't reach peer X"

1. `macula_dht:find(Dht, PeerId)' — is the peer even known?
2. `macula_dht:ping_peer(Dht, PeerId, 3000)' — does PING round-trip?
3. If PING fails and the peer IS in the table: routing-table entry
   has a stale address. `macula_dht:forget(Dht, PeerId)' then
   `macula_dht:lookup_nodes(Dht, PeerId)' to re-learn.
4. If PING fails and the peer is NOT in the table: the source-route
   computation (Part 3 §6.3) couldn't find a path. Run
   `macula_routing:compute_paths(…)' manually — debug output shows
   which hop choked.

### 6.7 "Cascade times out"

1. Check `cascade_opts.timeout_ms' in sys.config — default 60 s.
2. If Tier D is in the list and no testnet anchor is live, Tier D
   spends 20 s waiting before falling through. Either disable
   Tier D in the station's tier list OR set its `timeout_ms' to
   something short (~2 s) so the cascade moves on quickly.
3. Run cascade with extra logging:
   ```
   logger:set_application_level(macula_bootstrap, debug).
   macula_bootstrap:run().
   logger:set_application_level(macula_bootstrap, info).
   ```

### 6.8 "Foundation record rejected"

This is usually `{error, not_foundation_signed}' or
`{error, wrong_type}' in the logs.

- `not_foundation_signed': the embedded firmware pubkey set doesn't
  include the signer of the record. Either:
  - Firmware is outdated (foundation rotated keys) → pull a newer
    station image.
  - Operator set `MACULA_FOUNDATION_PUBKEYS' to an incorrect list
    → unset the override or fix.
  - Genuine attack (an impostor is serving fake records) → note
    the NodeId + IP, report upstream.
- `wrong_type': a tier is passing a non-foundation record type
  through the foundation verify step. Usually a code bug; file
  an issue with the tier name + record type.

---

## 7. Running the fleet as a test harness

### 7.1 Single-host CT (dev laptop)

```
cd ~/work/github.com/macula-io/macula-station
rebar3 ct --suite=apps/macula_bootstrap/test/macula_phase6_SUITE
```

12 tests today, all fakes. Runs in < 30 s. First guardrail.

### 7.2 Multi-station CT on beam00 (single-box, two instances)

```
# SSH into beam00
ssh rl@beam00.lab
cd /home/rl/macula-station
# Spin up two stations with different NodeIds on different ports
./scripts/dev-multi.sh --count 2 --data-root /fast/.hecate/dev
```

Acceptance bars (Phase 1 style):
- Two stations exchange signed node_records over QUIC within 2 s.
- SWIM confirms failure of the killed station within 8 s.
- DHT on the surviving node returns the tombstone when queried for
  the dead station's key.

### 7.3 4-node cluster CT on beam00-03

```
# From any beam node or laptop:
./scripts/fleet-ct.sh beam00.lab,beam01.lab,beam02.lab,beam03.lab
```

Acceptance bars (Phase 2/3 style):
- All four stations know each other within 30 s.
- DHT has tier-diverse buckets (at least 3 of the 4 ASes or
  countries represented in the top bucket).
- A `macula_dht:lookup_nodes/2' walk from beam00 converges in ≤ 3
  rounds.
- Kill beam02: within 10 s, beam00/01/03 mark beam02 as
  `confirmed_failed' via SWIM.

### 7.4 Partition + heal on beam cluster

```
# iptables-based partition: isolate beam03 from beam00-02.
./scripts/partition.sh --isolate beam03 --from beam00,beam01,beam02
# wait 60 s, assert beam03 is confirmed_failed from each of 00/01/02
# and 00/01/02 are confirmed_failed from beam03.
./scripts/partition.sh --heal
# assert the SWIM membership reconverges within 30 s, and the DHT
# rebuilds bucket links via fresh PINGs.
```

### 7.5 Real mDNS on office LAN (laptop)

Start a station on the laptop with `responder: #{node_id=>…,
port=>7443, tier=>0}'. Start a second station on another laptop
on the same Wi-Fi. Both should appear to each other via Tier B
within seconds. Useful to confirm the mDNS responder is sane before
the beam cluster tests it with synthetic fakes.

### 7.6 Relay-box interaction (Tier C real Mainline DHT)

Once 6.5.x is in, a beam-cluster station can actually query the
public Mainline DHT to fetch foundation-published PKARR records.
This touches the public internet — only run when the foundation has
actually published a test record.

### 7.7 Production-like (macula.io, post-cutover)

Not yet. Blocked on Phase 8 cutover (`PLAN_DEFERRED_WORK.md §4').

### 7.8 Burn-in

```
./scripts/burnin.sh --duration 72h --fleet beam00,beam01,beam02,beam03 \
    --report /bulk0/.hecate/reports/$(date +%F)/burnin.html
```

What gets checked every 5 min:
- Every station `/status' reports `healthy: true'.
- `hecate_erlang_processes_total' does not grow > 5% over the
  window.
- `macula_dht_size' is stable ± 20%.
- No `error' or `critical' log lines.
- `recon:bin_leak(10)' returns no growing refcounts.

---

## 8. Incident response

### 8.1 Severity levels

- <b>SEV1</b> — production down OR fleet-wide partition OR
  confirmed malicious peer on trust-list. Page immediately.
- <b>SEV2</b> — one station down in a multi-station realm, SWIM
  flap > 5 min on one node, cascade fails on > 10% of boots.
  Email / Slack, triage within 2 h.
- <b>SEV3</b> — cosmetic, log noise, single failed lookup that
  retries OK. Open an issue, handle in a scheduled window.

### 8.2 First actions

1. Snapshot logs: `journalctl --user -u hecate-daemon > /tmp/$(date +%s).log'.
2. Snapshot `/status' and `/metrics' via `curl'.
3. `erlang:system_info(procs)' + top-10 memory via remsh, save to disk.
4. If SEV1 and a safe rollback exists: rebase Quadlet to previous
   `:latest' tag via `~/.hecate/gitops/' commit + push.
5. Never `rm -rf' a data dir as first response — it loses the
   NodeId which cannot be recovered. Rename to `…-quarantine-<date>'
   instead.

### 8.3 Post-mortem template

```
# Incident <YYYY-MM-DD>-<short-slug>

## Summary
<one-paragraph>

## Timeline (UTC)
- HH:MM — anomaly detected (how, by whom)
- HH:MM — first action
- …
- HH:MM — resolved

## Impact
- Stations affected:
- Users affected:
- Duration of user-visible impact:

## Root cause
<what actually went wrong, cite commit hashes, config values>

## What worked
<monitoring + recovery we want to keep>

## What did not
<missing signals, slow pager, misleading log>

## Fixes
- [ ] Code change: …
- [ ] Config change: …
- [ ] Runbook change: …
- [ ] Monitoring / alert change: …
```

Stored under `~/.hecate/incidents/' per box + git-checked in
`macula-demo/incidents/'.

### 8.4 Shutting down a misbehaving station cleanly

```
# Remote graceful stop:
curl -X POST https://beam02.lab:8443/admin/shutdown \
    --cert ~/.hecate/admin.pem
# Or via remsh:
macula_station:stop(whereis(macula_station_sup), administrative).
```

Either path:
- Writes a tombstone record for our own NodeId.
- Flushes routing-table + own records to ETS on disk.
- Closes every QUIC connection cleanly.
- Exits with code 0.

### 8.5 Recovering from identity-key loss

If `identity.erl.bin' is gone AND no backup exists: the NodeId is
permanently retired. The station boots with a fresh identity and
joins the network as a "new" peer. Former peers see the old NodeId
as `confirmed_failed' once its records expire (48 h default, Part 4
§11).

Never share or restore an identity file across multiple running
stations — two stations with the same NodeId break SWIM and the
DHT. The file bit `chmod 0600' + filesystem permissions are a
hard invariant.

---

## 9. Quick reference commands (cheat sheet)

```
# Build + ship (CI does this automatically on push to main)
rebar3 compile && rebar3 release
podman build -t ghcr.io/macula-io/macula-station:<tag> .
podman push ghcr.io/macula-io/macula-station:<tag>

# Per-node operations (beam cluster)
ssh rl@beam0X.lab systemctl --user status hecate-daemon
ssh rl@beam0X.lab journalctl --user -u hecate-daemon -f
ssh rl@beam0X.lab podman auto-update       # force pull :latest

# GitOps deploy
cd ~/.hecate/gitops
# edit the relevant Quadlet (.container) file; commit; push.
# systemd reconciler picks it up within 60 s.

# Interactive diagnostics
ssh rl@beam0X.lab
erl -name "$(id -un)-remsh@$(hostname)" -setcookie <cookie> \
    -remsh hecate@$(hostname)

# Run a fleet CT
./scripts/fleet-ct.sh beam00,beam01,beam02,beam03

# 72 h burn-in (starts detached, tails the report)
tmux new -d -s burnin './scripts/burnin.sh --duration 72h --fleet beam00..03'
tmux attach -t burnin
```

---

## 10. What this runbook does <em>not</em> cover (yet)

- TLS cert rotation for the admin API.
- Live OTP hot-upgrades (V2 plan: cold reboots only until V2.1).
- Cross-region failover (realm-level concern; see
  `PLAN_MNS_AND_REALM_JOIN.md').
- Backup + restore of realm-level state (not V2 scope).

These are tracked in `PLAN_DEFERRED_WORK.md §6' (separate plans)
and `§5' (open questions).
