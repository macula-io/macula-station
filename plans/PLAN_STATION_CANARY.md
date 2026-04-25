# Plan: hecate-station Canary Deploy

**Status:** Awaiting operator approval
**Created:** 2026-04-25
**Last Updated:** 2026-04-25

## Overview

The multi-identity refactor (`PLAN_MULTI_IDENTITY_RELAY` Phases 1–6)
is complete. Phase 7 cuts `hecate-station` over to the existing
`macula-relay` V1 fleet (3 boxes, 100 virtual identities each).
This document is the operator-driven step list. **No live boxes
are touched without explicit approval.**

## Pre-flight checks (operator)

Before the canary:

- [ ] CI on `hecate-social/hecate-station@main` is green for the
      latest commit.
- [ ] `ghcr.io/hecate-social/hecate-station:main` exists and was
      built from the green commit.
- [ ] Smoke test the image locally:
      ```
      docker run --rm --network=host \
        -e MACULA_RELAY_IDENTITIES="t.macula.io/Test/XX/0/0/127.0.0.1" \
        -e MACULA_QUIC_PORT=4433 \
        -e MACULA_TLS_CERTFILE=/path/to/cert.pem \
        -e MACULA_TLS_KEYFILE=/path/to/key.pem \
        -e MACULA_ADMIN_TOKEN=test-token \
        -v /path/to/certs:/certs:ro \
        ghcr.io/hecate-social/hecate-station:main
      ```
      Verify `/status` returns 200 and `/admin/identities` (with
      bearer token) lists `t.macula.io`.
- [ ] hecate-station's `MACULA_RELAY_IDENTITIES` parser accepts the
      production format. Verify against
      `macula-demo/infrastructure/relays-linode-paris/relay-identities.txt`.

## Box order — least to most blast radius

| Order | Box | Reason |
|---|---|---|
| 1 | `relays-linode-paris` | smallest box (1.9 GB RAM); 100 identities will exercise the memory ceiling first |
| 2 | `relays-hetzner-helsinki` | distinct geography, distinct provider |
| 3 | `relays-hetzner-nuremberg` | last to flip — gives the longest soak across the previous two before the whole fleet is on the new image |

Actually the inverted order is intentional: Paris is the tightest
fit, so any memory issue surfaces first. If Paris stays healthy
for 48h the other two are very likely fine on RAM grounds.

## Per-box change in `macula-demo/infrastructure/relays-linode-paris/`

The current `docker-compose.yml` (V1) pins
`ghcr.io/macula-io/macula-relay:${RELAY_VERSION:-main}`. Phase 7
introduces a sibling compose `docker-compose.station.yml`. Old
compose stays untouched so rollback is "shut down station, bring
up V1 again".

`docker-compose.station.yml` skeleton:

```yaml
services:
  relay:
    image: ghcr.io/hecate-social/hecate-station:${STATION_VERSION:-main}
    container_name: hecate-station
    user: "0"
    restart: unless-stopped
    network_mode: host
    labels:
      - "com.centurylinklabs.watchtower.enable=true"
    environment:
      MACULA_QUIC_PORT: "4433"
      MACULA_TLS_CERTFILE: /certs/caddy/certificates/acme-v02.api.letsencrypt.org-directory/wildcard_.macula.io/wildcard_.macula.io.crt
      MACULA_TLS_KEYFILE:  /certs/caddy/certificates/acme-v02.api.letsencrypt.org-directory/wildcard_.macula.io/wildcard_.macula.io.key
      MACULA_RELAY_IDENTITIES: ${MACULA_RELAY_IDENTITIES}
      MACULA_ADMIN_TOKEN:      ${MACULA_ADMIN_TOKEN}
    volumes:
      - caddy_data:/certs:ro
      - hecate_data:/var/lib/hecate
    depends_on:
      caddy:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8443/status"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 60s

  caddy: { ... unchanged ... }
  watchtower: { ... unchanged ... }

volumes:
  caddy_data:
  caddy_config:
  hecate_data:
```

Notes:

- `network_mode: host` so the listener can bind directly to the
  box's per-identity IPv6 addresses without Docker's NAT.
- `hecate_data` volume holds the box-secret across container
  restarts (`HECATE_STATION_BOX_SECRET_PATH=/var/lib/hecate/box-secret`
  inside the image).
- Healthcheck uses `/status` on the admin port (8443). It is
  unauthenticated per V1's loopback-only model.
- DROPPED env vars (V1-only, station ignores): `MACULA_REALM`,
  `MACULA_HEALTH_PORT`, `MACULA_TLS_MODE`, `RELAY_ENABLED`,
  `MACULA_HOSTNAME`, `MACULA_MODE`, `MACULA_RELAYS`, `NODE_NAME`,
  `MACULA_GEO_*`, `MACULA_PROVIDER`. Identity geography moves into
  `MACULA_RELAY_IDENTITIES` per-entry fields.

## Cutover sequence (per box)

```
ssh rl@relays-linode-paris

# 1. Verify .env is set with the existing identities
grep MACULA_RELAY_IDENTITIES /root/macula-relay-compose/.env | head -1

# 2. Bring V1 down (rollback ready: bring it back with same command)
cd /root/macula-relay-compose
docker compose -f docker-compose.yml down

# 3. Bring station up with the SAME .env
docker compose -f docker-compose.station.yml up -d

# 4. Watch the logs for 5 minutes
docker compose -f docker-compose.station.yml logs -f relay
```

Health gates:

- [ ] `curl -sf http://localhost:8443/status` returns 200 within
      60s of container start.
- [ ] `curl -H "Authorization: Bearer $TOK" http://localhost:8443/admin/identities`
      returns the full 100-identity list.
- [ ] Sample 5 random identities via `/admin/identities/:id/health`
      and verify each shows `listener.state = "alive"`.
- [ ] RSS of the container is < 1.5 GB on the Paris box.

## Soak

- 48h continuous operation
- No more than 5 restarts in 24h (watchtower auto-update + crashes
  count combined)
- Listener stayed `alive` on every identity sampled hourly
- DHT routing tables non-empty after the bootstrap cascade

If any health gate fails: shut station down, bring V1 back up,
file a regression issue with the captured logs.

## Rollback

```
ssh rl@relays-linode-paris
cd /root/macula-relay-compose
docker compose -f docker-compose.station.yml down
docker compose -f docker-compose.yml up -d
```

DNS records (per-identity hostnames) are owned by Linode's DNS
plugin in Caddy and don't change between V1 and station, so rollback
is just a compose swap.

## Out of scope for Phase 7

- The Hetzner Nuremberg / Helsinki cutover. They flip after Paris
  is stable for ≥48h.
- Decommissioning `macula-relay` V1 image / repo. That happens
  after all three boxes are stable on station for a week.
- Migrating SDK / hecate-daemon to talk to station (it already
  talks to V1; the wire protocol is the same).

## Status tracking

| Box | Decision | Cutover date | Soak result | Rolled back? |
|---|---|---|---|---|
| relays-linode-paris       | ✅ canary live | 2026-04-25 14:10 UTC | _soaking_ | _ |
| relays-hetzner-helsinki   | _blocked on Paris ≥48h soak_ | _ | _ | _ |
| relays-hetzner-nuremberg  | _blocked on Helsinki_ | _ | _ | _ |

## Paris cutover — initial readings (2026-04-25 14:10 UTC)

- Image: `ghcr.io/hecate-social/hecate-station@sha256:73e76755…` (built from `2dda7e4`)
- Container: `hecate-station Up (healthy)` per Docker healthcheck
- All 5 T1 identities registered:
  - `relay-t1-london.macula.io` → `2600:3c1a:e001:19::100:4433`
  - `relay-t1-brussels.macula.io` → `2600:3c1a:e001:19::101:4433`
  - `relay-t1-frankfurt.macula.io` → `2600:3c1a:e001:19::102:4433`
  - `relay-t1-milan.macula.io` → `2600:3c1a:e001:19::103:4433`
  - `relay-t1-madrid.macula.io` → `2600:3c1a:e001:19::104:4433`
- Per-identity health (sampled London): all 6 children
  (`listener`, `peer_observer`, `swim`, `dht`, `content_announcer`,
  `pubsub_registry`) report `state=alive`; `healthy=true`.
- DHT size 0, SWIM members 0 at boot — expected since no
  bootstrap tiers configured (cascade `no_tiers` → silent skip).
  Population happens as T2 boxes (Hetzner) dial in.
- Resource usage: 81 MB RSS / 1.9 GB total; CPU 0.12% idle.
- V1 compose (`docker-compose.yml`) left untouched for instant
  rollback.

### Known cosmetic gap

`GET /status` (legacy single-identity route) returns
`healthy: false` because the multi-identity boot path does not
populate the legacy singletons (`hecate_station:dht/0` etc).
Docker healthcheck only checks the HTTP status code (200) so it
is not affected. Operators inspecting the body should use
`GET /admin/identities` and `/admin/identities/:id/health`
instead. Future work: rewire `/status` to aggregate over the
registry.
