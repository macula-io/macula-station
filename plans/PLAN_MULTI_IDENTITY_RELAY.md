# Plan: Multi-Identity Relay Support

**Status:** Phase 1 shipped — see "Progress" below
**Created:** 2026-04-25
**Last Updated:** 2026-04-25

## Progress

| Phase | Status | Notes |
|---|---|---|
| 1 | ✅ shipped | identity registry + identity_sup skeleton; rebar3 override for macula debug_info |
| 2 | ✅ shipped | de-singletoned pubsub_registry + content_announcer; deleted pubsub_server_sup; identity_sup wires per-identity registry + announcer |
| 3 | ✅ shipped | per-identity DHT/SWIM/observer/listener procedural startup; cross-talk test (two identities dialing each other via QUIC). Cascade ingest deferred to Phase 4. |
| 4 | ✅ shipped | identity config parser (`MACULA_RELAY_IDENTITIES`); deterministic per-identity Ed25519 keys (HMAC-SHA256(box_secret, hostname)); cascade in identity_sup chain; station_app multi-identity boot branch; 3-identity boot test |
| 5 | ✅ shipped | admin endpoints `GET /admin/identities`, `POST .../start|stop|reload`; bearer auth via `MACULA_ADMIN_TOKEN`; registry remembers opts for reload; admin sup also boots under multi-identity flow |
| 6 | ✅ shipped | per-identity logger metadata on workers we own (pubsub_registry + content_announcer); `GET /admin/identities/:id/health` returning DHT/SWIM/listener/announcer/pubsub_registry status. Prometheus exporter still pending — out of 0.5-session scope. |
| 7 | pending | cutover ops |

## Overview

`hecate-station` currently runs **one identity per BEAM process**. To replace
`macula-relay` V1 on the existing relay fleet (Hetzner Nuremberg / Helsinki,
Linode Paris — 1-2 vCPU, 1.9-3.7 GB RAM each), it must run **N identities
in one BEAM process**, the way V1 multiplexes 100 virtual relay identities
per box. Spawning N BEAMs per box is not viable on this hardware.

This plan introduces an in-process multi-identity layer so a single
`hecate-station` instance can host arbitrarily many (identity, listener,
DHT, SWIM) tuples, each bound to a distinct IPv6 address, while sharing
frame parsing, peer-pool, content-store, and other identity-agnostic
infrastructure.

## Hardware constraints (recorded for posterity)

| Box | CPU | RAM |
|---|---|---|
| relays-hetzner-nuremberg | 2 vCPU AMD EPYC | 3.7 GB |
| relays-hetzner-helsinki | 2 vCPU Intel Xeon | 3.7 GB |
| relays-linode-paris | 1 vCPU EPYC 7713 | 1.9 GB |

Each box currently runs ONE `macula-relay` container holding 100 virtual
identities. ~1.4 GB used at idle. Spinning up N BEAM nodes per box is
not affordable.

## V1 paradigm to preserve

- **One container per box, one BEAM inside.**
- **N IPv6 addresses bound** to the network interface (e.g.
  `2a01:4f8:1c1f:8ab8::100..167`).
- **Per-identity hostnames**: `relay-be-leuven.macula.io`,
  `relay-fr-paris.macula.io`, …
- **One Caddy wildcard cert** (`*.macula.io`) shared across all identities.
- **Admin API** on `/admin/*` (bearer-auth) for runtime identity lifecycle.
- **Identity config** loaded from `MACULA_RELAY_IDENTITIES` env var:
  `hostname:city:country:lat:lng:ipv6,...`

## Architectural shape

The lower half of the station is already pid-based and multi-instance-safe:

- ✅ `hecate_swim` — anonymous `gen_server:start_link/3`
- ✅ `hecate_dht_server` — anonymous `gen_server:start_link/3`
- ✅ `hecate_peering_conn` — pid-based gen_statem

The **singletons** that block multi-identity:

- ❌ `hecate_station_server` — `{local, ?MODULE}`
- ❌ `hecate_pubsub_registry` — `{local, ?MODULE}` (Step 2 commit)
- ❌ `hecate_pubsub_server_sup` — `{local, ?MODULE}`
- ❌ `hecate_content_store` — `{local, ?SERVER}`
- ❌ `hecate_content_transfer` — `{local, ?SERVER}`
- ❌ `hecate_content_announcer` — `{local, ?MODULE}`
- ❌ All `*_sup` modules using `{local, ?MODULE}`

**Per-identity vs shared decision matrix:**

| Component | Per-identity | Shared | Reasoning |
|---|---|---|---|
| QUIC listener | ✅ | | bound to identity's IPv6 |
| Station identity (Ed25519) | ✅ | | identity = the keypair |
| DHT (routing table, lookups) | ✅ | | each identity is a distinct node |
| SWIM | ✅ | | per-identity membership |
| Peering pool (outbound conns) | ✅ | | identity-scoped TLS |
| HyParView/Plumtree overlays | ✅ | | per-realm-per-identity already |
| PubSub registry | ✅ | | scoped to identity's overlay |
| Content store (filesystem) | | ✅ | content is fleet-global |
| Content transfer | | ✅ | one bitswap pool, per-identity announces |
| Content announcer | ✅ | | each identity announces itself as provider |
| Frame codec / protocol | | ✅ | identity-agnostic |
| Admin API | | ✅ | manages all identities |

## Phases

### Phase 1: Identity supervisor skeleton — 1 session

Lay the OTP shape. No behavioural changes yet — existing single-identity
flow keeps working.

- New `hecate_station_identity_sup` (per-identity `one_for_all`).
  Children (initially): `hecate_station_server`, `hecate_swim`,
  `hecate_dht_server`, `hecate_overlay_sup` (per-identity instance).
- New `hecate_station_identity_registry` — gen_server holding
  `{IdentityKey => identity_sup_pid()}`. Public API: `register/2`,
  `lookup/1`, `list/0`, `terminate/1`.
- `hecate_station_sup` becomes top-level: supervises the registry +
  shared infrastructure (content_store, content_transfer, admin API
  later) + dynamic identity_sup children (via the registry).

**Files:** new `hecate_station_identity_sup.erl`,
`hecate_station_identity_registry.erl`. Modify `hecate_station_sup.erl`,
`hecate_station_app.erl`.

**Tests:** identity_sup starts/stops cleanly under registry control;
multiple identity_sup instances coexist.

### Phase 2: De-singleton high-level gen_servers — 2 sessions

Refactor each singleton-registered gen_server to take an identity
parameter and register under a unique name (or stay anonymous and
return pid).

- `hecate_station_server` — `start_link(IdentityOpts) -> {ok, Pid}`,
  no `{local, _}` registration. Callers acquire pid via the registry.
- `hecate_pubsub_registry` — register as
  `{via, gproc, {n, l, {hecate_pubsub_registry, IdentityKey}}}`.
- `hecate_pubsub_server_sup` — same gproc-keyed registration.
- `hecate_content_announcer` — per-identity (each identity announces
  its own provider info).

**Stays singleton:** `hecate_content_store`, `hecate_content_transfer`
— shared across identities.

**Files:** the six modules above + their supervisor parents +
all call sites. Extensive sed-rename. Likely 30+ files touched.

**Tests:** N station_servers in one BEAM coexist with disjoint state.
Pubsub registry per-identity isolation. Content announcer per-identity
publishes distinct provider_info maps.

### Phase 3: Per-identity QUIC bind + routing — 1 session

Make the listener identity-aware.

- `hecate_station_server` (per-identity) opens its OWN
  `hecate_transport:listen/1` on the identity's bind address +
  shared cert. Each identity gets its own listener ref.
- Inbound peer connections route to the identity that owns the
  bind address. (No SNI inspection needed if each identity has a
  distinct IPv6 — the listener IS the routing.)
- Station-to-station outbound dialing goes through the originating
  identity's keypair.

**Files:** `hecate_station_server.erl`, `hecate_station_listener.erl`.

**Tests:** two identities in one BEAM, two listeners on different
loopback ports, cross-talk between them via QUIC.

### Phase 4: Identity config loader + boot — 1 session

Wire identity definitions in.

- `hecate_station_identity_config.erl` — parses
  `MACULA_RELAY_IDENTITIES` env var (V1 format: comma-separated
  `hostname:city:country:lat:lng:ipv6` records). Also supports a
  TOML / JSON config file path for cleaner ops.
- Per-identity keypair: deterministic from box-secret + identity
  hostname (HKDF), or per-identity files in `~/.hecate/identities/`.
  V1 deterministic-from-hostname is the path of least friction.
- `hecate_station_app:start/2` reads config at boot, calls
  `hecate_station_identity_registry:register/2` for each identity.

**Files:** new `hecate_station_identity_config.erl` +
`hecate_station_identity_keys.erl`. Modify `hecate_station_app.erl`.

**Tests:** parse the existing `relay-identities.txt` files from
`macula-demo/infrastructure/`. Boot the station with all 100
Nuremberg identities and verify all 100 listeners come up.

### Phase 5: Admin API — 1 session

V1-compatible HTTP admin endpoints under bearer-token auth.

- `hecate_station_admin` (already exists per filesystem survey;
  audit and extend rather than rewrite).
- `GET  /admin/identities` — list current identities + status
- `POST /admin/identities/:id/start` — instantiate a new identity
  (config in body)
- `POST /admin/identities/:id/stop` — terminate one
- `POST /admin/identities/:id/reload` — restart with same config
- Bearer auth via `MACULA_ADMIN_TOKEN` env var (same as V1).

**Files:** extend `apps/hecate_station/src/hecate_station_admin.erl`,
`hecate_station_admin_sup.erl`. Add Cowboy or similar HTTP layer
if not present.

**Tests:** end-to-end CT — start station, hit admin endpoints,
verify identities come and go.

### Phase 6: Operational tooling — 0.5 session

- Per-identity log enrichment (identity_id in every log line).
- Per-identity health endpoint (`/admin/identities/:id/health`).
- Optional: Prometheus exporter with per-identity gauges
  (peer_count, dht_size, swim_alive, etc.).

**Files:** logger filter, `hecate_station_admin.erl` extensions.

### Phase 7: Cutover ops — 0.5 session + ongoing

- Adapt `macula-demo/infrastructure/relays-hetzner-nuremberg/`
  docker-compose.yml: new image
  `ghcr.io/hecate-social/hecate-station:main`, same env vars,
  same Caddy cert volume, same network_mode: host.
- Canary one box (probably Linode Paris — smallest blast radius).
- Compare metrics for a soak period; flip the other two on
  parity.

**Files:** `macula-demo/infrastructure/relays-*/docker-compose.yml`,
`.env.example`, deploy scripts.

## Files inventory (rough)

**New:**
- `apps/hecate_station/src/hecate_station_identity_sup.erl`
- `apps/hecate_station/src/hecate_station_identity_registry.erl`
- `apps/hecate_station/src/hecate_station_identity_config.erl`
- `apps/hecate_station/src/hecate_station_identity_keys.erl`

**Refactor (de-singleton + identity-aware):**
- `apps/hecate_station/src/hecate_station_server.erl`
- `apps/hecate_station/src/hecate_station_listener.erl`
- `apps/hecate_station/src/hecate_station_sup.erl`
- `apps/hecate_station/src/hecate_station_app.erl`
- `apps/hecate_overlay/src/hecate_pubsub_registry.erl`
- `apps/hecate_overlay/src/hecate_pubsub_server_sup.erl`
- `apps/hecate_overlay/src/hecate_overlay_sup.erl`
- `apps/hecate_content/src/hecate_content_announcer.erl`
- `apps/hecate_content/src/hecate_content_sup.erl`

**Extend (admin):**
- `apps/hecate_station/src/hecate_station_admin.erl`
- `apps/hecate_station/src/hecate_station_admin_sup.erl`

**Touch (call-site renames in):**
- All hecate_pubsub_*, hecate_content_* tests
- `hecate_station_server` callers across the umbrella
- The new `hecate_station_identity_sup` children's start_link sites

**Adapt:**
- `macula-demo/infrastructure/relays-hetzner-nuremberg/docker-compose.yml`
- Same for helsinki + linode-paris

## Test strategy per phase

| Phase | Layer | Strategy |
|---|---|---|
| 1 | Skeleton | eunit on registry register/lookup/terminate; CT spinning up two empty identity_sups |
| 2 | De-singleton | eunit verifying N gen_servers coexist, gproc lookups; existing tests must keep passing (1132 baseline) |
| 3 | Multi-bind | CT with two identities binding loopback ports, verify cross-talk |
| 4 | Config + boot | eunit on parser; CT booting station with 10 identities from a fixture |
| 5 | Admin API | eunit on auth + handlers; CT end-to-end with curl-like client |
| 6 | Tooling | manual smoke (logs grep'able by identity_id) |
| 7 | Cutover | live monitoring on Linode Paris canary for ≥48h before flipping the other two |

## Success criteria

- [ ] hecate-station boots N identities (N = 100) in one BEAM on a 3.7 GB
      Hetzner box, peak RSS < 2 GB, idle RSS < 1.5 GB
- [ ] Each identity has its own DHT routing table, SWIM membership,
      QUIC listener bound to its IPv6, peering pool, signed records
- [ ] Admin API allows runtime add / remove / reload of identities
- [ ] Existing macula-demo deployment scripts work with hecate-station
      image swapped in (same `MACULA_RELAY_IDENTITIES` format,
      same Caddy mount, same env vars)
- [ ] All 1132 existing eunit tests still pass; new tests cover
      multi-identity invariants
- [ ] Linode Paris (smallest box) handles 100 identities under 1.5 GB
      RAM (the box has 1.9 GB total, of which ~400 MB is Caddy +
      watchtower + OS overhead)

## Risk + rollback

**Risks:**

1. **Memory ceiling.** 100 identities in 1.9 GB on the Linode Paris
   nanode is the tightest test. If V2 doesn't fit, we either (a) shrink
   per-identity infrastructure further, (b) shed identities from that
   box, (c) upsize to a Linode 2GB shared CPU.

2. **gproc dep.** Currently hecate-station has no gproc dep. Adding it
   pulls in another transitive lib. Alternative: hand-rolled
   `{IdentityKey => Pid}` ETS table. Marginal win; gproc is mature
   and tiny.

3. **Per-identity content store contention.** All identities share one
   `hecate_content_store`. The gen_server becomes the bottleneck for
   block writes. If it's slow, GC and integrity checks block everyone.
   **Mitigation:** measure before optimising. Filesystem I/O is the
   real bottleneck, not the gen_server — V1 has the same shape.

4. **DHT routing-table bloat.** N independent routing tables × 1024
   buckets × 20 entries = N × 20480 entries. At 100 identities ×
   per-entry ~200 bytes = ~400 MB just for routing tables. Tight on
   1.9 GB. **Mitigation:** prune aggressively, share peer-info
   storage across identities (a peer that talks to identity A is
   probably reachable from identity B too), monitor.

**Rollback:**

- DNS records are owned by Linode; revert
  `relays-{nuremberg,helsinki,paris}.macula.io` AAAA back to the
  old box if hecate-station fails on the canary box.
- Container image is on ghcr.io with semver tags; pin
  `macula-relay:v0.x` (the working V1 build) in docker-compose.yml
  to roll back.
- macula main 3.x SDK already published — no rollback needed there.

## Estimate

- Phase 1: 1 session
- Phase 2: 2 sessions
- Phase 3: 1 session
- Phase 4: 1 session
- Phase 5: 1 session
- Phase 6: 0.5 session
- Phase 7: 0.5 session + soak time

**Total: ~7 focused sessions of code work**, plus ≥48h soak per cutover
canary.

## Out of scope (deliberate)

- **Per-identity content store** — single shared store is a feature,
  matches V1.
- **Cross-identity bridge / relay** — identities don't relay traffic
  for each other. They're peers in the same DHT(s) but otherwise
  independent.
- **Per-realm pubsub mesh** — already shipped (Step 2). Nothing here
  changes that.
- **Bigger relay boxes** — explicitly avoided. The whole point is to
  fit V1's footprint on the same hardware.
- **24h burn-in / chaos** — separate plan
  (Phase 7.x in `PLAN_DEFERRED_WORK.md`).
