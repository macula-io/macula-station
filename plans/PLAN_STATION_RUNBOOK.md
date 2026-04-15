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

## 2. Fleet topology (where things run)

### 2.1 Dev / attended workstations (laptops)

| Box | Role | Notes |
|-----|------|-------|
| `work-laptop` (this one) | Dev + Tier-B mDNS sandbox (LAN discovery on office Wi-Fi) | rebar3 shell, eunit, ct, manual chaos |

- Purpose: rapid iteration, unit/CT runs, real mDNS against office
  LAN or home network.
- Startup: `rebar3 shell' from the repo.
- Data dir: `~/.hecate/station/<profile>/' (each profile = separate
  NodeId).

### 2.2 BEAM cluster (staging / integration)

| Node | Address | RAM | NVMe | Bulk | Role |
|------|---------|-----|------|------|------|
| beam00 | 192.168.1.10 | 16 GB | 224 GB | 1 × 932 GB HDD | Multi-station CT host, Tier-B mDNS, routing-table persistence |
| beam01 | 192.168.1.11 | 32 GB | 224 GB | 2 × 932 GB HDD | Heavier per-node workloads |
| beam02 | 192.168.1.12 | 32 GB | 224 GB | 2 × 932 GB HDD | Chaos target |
| beam03 | 192.168.1.13 | 32 GB | 932 GB | 2 × 932 GB HDD | Archive / `/fast' at 932 GB |

- Access: `ssh rl@beam0X.lab' (password `rl').
- Runtime: systemd-user units + podman (no k3s).
- Station data: `/fast/.hecate/' (NVMe). Bulk datasets: `/bulk*'.
- OS: Ubuntu 20.04; kernel from kubic repo provides podman 3.4.2.

### 2.3 Relay boxes (shared infra the station depends on)

| Box | Region | Role | Notes |
|-----|--------|------|-------|
| relays-hetzner-nuremberg | DE | 100 virtual relay identities (V1 code) | Shared with hecate-daemon |
| relays-hetzner-helsinki | FI | 100 virtual relay identities | — |
| relays-linode-paris | FR | Realm relays (macula.io) | — |

- These host the <b>V1 macula-relay</b> code (frozen at 1.4.23). V2
  stations on the BEAM cluster run in parallel without touching
  them.
- Cutover at Phase 8 (see `PLAN_DEFERRED_WORK.md §4').

### 2.4 Production (post-cutover)

macula.io on Linode — Docker Compose. Today: V1 only. After Phase 8:
V2 stations replace V1 relays.

---

## 3. Station boot sequence (reference)

Below is the <em>intended</em> boot flow once `PLAN_STATION_INTEGRATION.md`
is done. Each step has a "healthy signal" and a "common failure
mode". Use this as the mental model when diagnosing.

```
 1. beam VM starts → kernel + stdlib + crypto + ssl + inets up.
 2. hecate_station application:start/2 →
    hecate_station_sup:start_link/0.
 3. Sup starts identity child →
    load NodeId from ~/.hecate/station/identity or generate + persist.
 4. Sup starts QUIC listener child →
    bind IPv6 port from sys.config, listen ready.
 5. Sup starts hecate_bootstrap app transitively (already declared
    in applications) → hecate_bootstrap_sup starts mDNS responder
    if configured.
 6. Sup starts DHT child → hecate_dht:start_link(#{self_id => …}).
 7. Sup starts bootstrap orchestrator child (first-boot) →
    hecate_bootstrap:run/0 → returns peers.
 8. hecate_station_bootstrap:ingest(Dht, Peers) → routing table
    seeded.
 9. Sup starts SWIM child →
    hecate_swim join with seed peers from DHT.
10. Sup starts overlay child (HyParView + Plumtree) per realm.
11. Sup starts admin API (HTTP/8443 with client-cert auth).
12. /status reports ready, /metrics exposes prometheus data.
```

Each Sup child:
- has `restart => permanent' (or `transient' for bootstrap-once),
- reports to the station's own health registry,
- emits telemetry events at start/stop.

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

### 4.2 Minimum `sys.config`

```erlang
[
    {hecate_station, [
        {listen_port, 7443},
        {listen_if,   "::"},                         %% IPv6 any
        {identity_path, "/fast/.hecate/station/identity.erl.bin"},
        {cache_dir,   "/fast/.hecate/station/cache"},
        {admin_api,   #{bind => "::1", port => 8443,
                        ca_cert => "..."}}
    ]},
    {hecate_bootstrap, [
        {tiers, [
            {hecate_bootstrap_tier_a, #{
                resolvers => [
                    {hecate_bootstrap_doh_http, <<"https://1.1.1.1/dns-query">>},
                    {hecate_bootstrap_doh_http, <<"https://9.9.9.9/dns-query">>},
                    {hecate_bootstrap_doh_http, <<"https://doh.mullvad.net/dns-query">>}
                ],
                corroboration => 2, timeout_ms => 1500}},
            {hecate_bootstrap_tier_b, #{
                handshake_fun => fun macula_peering:handshake_and_record/3,
                timeout_ms    => 2000}},
            {hecate_bootstrap_tier_c, #{
                dht_transport => hecate_bootstrap_dht_udp,   %% when 6.5.x lands
                timeout_ms    => 10_000}},
            {hecate_bootstrap_tier_d, #{
                chains => [
                    {hecate_bootstrap_chain_eth_jsonrpc,
                     #{endpoint => <<"https://eth.llamarpc.com">>,
                       contract => <<"0x…foundation…">>,
                       topic    => <<"0x…AnchorPublished…">>}},
                    {hecate_bootstrap_chain_esplora,
                     #{base_url => <<"https://blockstream.info/api">>,
                       address  => <<"bc1q…foundation…">>}}],
                timeout_ms => 20_000}},
            {hecate_bootstrap_tier_e, #{
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
-setcookie hecate-station-<realm-id-suffix>
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
2026-04-15T13:42:01 info hecate_bootstrap:cascade/2 started
    tiers=[a,b,c,d,e] min_peers=3 timeout_ms=60000
2026-04-15T13:42:02 info hecate_bootstrap_tier_a:probe/1 returned
    peers=20 corroboration_hit=true
2026-04-15T13:42:02 info hecate_station_bootstrap:ingest/2 summary
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

Exposed by the admin API. Minimum metrics:

```
hecate_station_up{node_id="…",realm_id="…"} 1
hecate_station_uptime_seconds{…} 12345
hecate_bootstrap_cascade_duration_ms{…,winning_tier="a"} 421
hecate_bootstrap_cascade_peers_total{…,winning_tier="a"} 20
hecate_dht_size{…} 87
hecate_dht_bucket_count{…} 18
hecate_dht_admits_total{…} 203
hecate_dht_rejects_total{…} 7
hecate_swim_alive_members{…} 14
hecate_swim_suspected_members{…} 1
hecate_swim_confirmed_failed_members{…} 2
hecate_overlay_active_view_size{…,realm_id="…"} 4
hecate_overlay_plumtree_eager_peers{…,realm_id="…"} 4
```

### 5.3 `/status' (JSON)

Single endpoint for humans + dashboards:

```json
{
    "station": {
        "node_id":    "…hex…",
        "uptime_ms":  12345678,
        "version":    "0.1.0-phase8",
        "healthy":    true
    },
    "bootstrap": {
        "last_run":       "2026-04-15T13:42:02Z",
        "winning_tier":   "a",
        "peers_ingested": 20
    },
    "dht":    { "size": 87, "buckets": 18, "siblings": 16 },
    "swim":   { "alive": 14, "suspected": 1, "confirmed_failed": 2 },
    "realms": [
        { "id": "…", "active_view": 4, "eager_plumtree": 4 }
    ]
}
```

### 5.4 Live shell access

```bash
# on the beam node
ssh rl@beam00.lab
systemctl --user status hecate-daemon
journalctl --user -u hecate-daemon -f

# open a remsh to peek inside
erl -name "remsh@beam00.macula.beam" \
    -setcookie hecate-station-…\
    -remsh hecate@beam00.macula.beam
```

Useful one-liners inside the shell:

```erlang
%% Station facts
hecate_station:version().
hecate_station:identity(SupPid).           %% Ed25519 key_pair
hecate_station:listen_addr(SupPid).

%% Bootstrap
hecate_bootstrap:run().                    %% force a re-cascade

%% DHT
hecate_dht:stats(DhtPid).
hecate_dht:k_closest(DhtPid, Target, 20).
hecate_dht:siblings(DhtPid).

%% SWIM
hecate_swim:members(SwimPid).

%% Overlay
hecate_overlay:active_view(Pid, RealmId).
hecate_overlay:plumtree_peers(Pid, RealmId).
```

### 5.5 Telemetry events (emitted via `telemetry' library)

- `[hecate, bootstrap, cascade, start]' / `[…, stop]'
- `[hecate, bootstrap, tier, a | b | c | d | e, probe]'
- `[hecate, dht, observe]'
- `[hecate, dht, lookup, start]' / `[…, stop]'
- `[hecate, swim, state, change]'
- `[hecate, overlay, view, change]'

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
   grep hecate_bootstrap'.
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

1. `hecate_dht:stats(Dht)' — `size' should be > 16 after bootstrap.
   If it's 0, the ingest step didn't happen (see 6.2).
2. `hecate_dht:siblings(Dht)' — empty sibling set = very small
   network, not a bug.
3. `hecate_dht:ping_peer(Dht, PeerId)' — if every peer times out,
   QUIC stack is broken (check `macula_transport' logs).
4. `hecate_dht:lookup_nodes(Dht, Target)' — `{error, no_progress}'
   after N rounds means the routing table is stale; force a
   re-bootstrap.

### 6.4 "SWIM flaps members"

1. `hecate_swim:members(Swim)' — observe the churn. Is it symmetric
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
   `hecate_dht:record_count(Dht)'), unbounded SWIM event log, or
   an errant `subscribe/3' handler that never unsubscribes.
6. Short-term mitigation: `erlang:garbage_collect()' across all
   processes. If that only delays the problem, it's a leak — open
   an issue with the process id and stack.

### 6.6 "Can't reach peer X"

1. `hecate_dht:find(Dht, PeerId)' — is the peer even known?
2. `hecate_dht:ping_peer(Dht, PeerId, 3000)' — does PING round-trip?
3. If PING fails and the peer IS in the table: routing-table entry
   has a stale address. `hecate_dht:forget(Dht, PeerId)' then
   `hecate_dht:lookup_nodes(Dht, PeerId)' to re-learn.
4. If PING fails and the peer is NOT in the table: the source-route
   computation (Part 3 §6.3) couldn't find a path. Run
   `hecate_routing:compute_paths(…)' manually — debug output shows
   which hop choked.

### 6.7 "Cascade times out"

1. Check `cascade_opts.timeout_ms' in sys.config — default 60 s.
2. If Tier D is in the list and no testnet anchor is live, Tier D
   spends 20 s waiting before falling through. Either disable
   Tier D in the station's tier list OR set its `timeout_ms' to
   something short (~2 s) so the cascade moves on quickly.
3. Run cascade with extra logging:
   ```
   logger:set_application_level(hecate_bootstrap, debug).
   hecate_bootstrap:run().
   logger:set_application_level(hecate_bootstrap, info).
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
cd ~/work/github.com/hecate-social/hecate-station
rebar3 ct --suite=apps/hecate_bootstrap/test/hecate_phase6_SUITE
```

12 tests today, all fakes. Runs in < 30 s. First guardrail.

### 7.2 Multi-station CT on beam00 (single-box, two instances)

```
# SSH into beam00
ssh rl@beam00.lab
cd /home/rl/hecate-station
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
- A `hecate_dht:lookup_nodes/2' walk from beam00 converges in ≤ 3
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
- `hecate_dht_size' is stable ± 20%.
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
hecate_station:stop(whereis(hecate_station_sup), administrative).
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
podman build -t ghcr.io/hecate-social/hecate-station:<tag> .
podman push ghcr.io/hecate-social/hecate-station:<tag>

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
