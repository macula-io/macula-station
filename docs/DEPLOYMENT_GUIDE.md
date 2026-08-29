# Deployment Guide

How to actually run a macula-station in production: the container image, the
config file, the way this org actually runs its own fleet (Docker +
watchtower, on public Hetzner/Linode boxes — never a LAN/lab machine, since
a station has to accept inbound QUIC from the public internet), plus Podman
Quadlet and Kubernetes references for operators who want them — neither of
those two is exercised by this org's own fleet, said plainly rather than
left to be discovered later.

Every config key and CLI detail below is read from the actual source
(`macula_station_config.erl`, `Dockerfile`, `.github/workflows/ci.yml`), not
assumed — see the citation in each section if you want to verify it yourself.

---

## 1. What you're deploying

One container = one station = one Ed25519 keypair. A station is
realm-agnostic infrastructure: it never joins a realm itself, it routes for
daemons (SDK clients) that do. Stations peer with each other outbound —
`outbound_peers` in config, below — to form the mesh; nothing about a
station's own boot sequence requires DNS, a load balancer, or inbound
firewall exceptions beyond the one QUIC port.

Two network endpoints, both from `Dockerfile`:

| Port | Protocol | Purpose |
|---|---|---|
| `4433` | UDP (QUIC) | The mesh wire protocol. Daemons and peer stations dial this. |
| `8443` | TCP | Admin/health HTTP. `/wire` is the liveness probe (unauthenticated); `/admin/*` needs bearer auth. |

---

## 2. The container image

Built from the repo's own `Dockerfile` — multi-stage: an `erlang:28.1-slim`
builder stage that also installs a Rust toolchain (the SDK's QUIC transport
is a Rust NIF, `macula_quic`, built from source here rather than fetched as a
precompiled artifact — a prebuilt release NIF once hung every connect and
took the realm dark, so this repo does not trust that shortcut), producing an
`rebar3 as prod release`; then a slim `debian:bookworm` runtime stage that
copies just the release out.

```bash
docker build -t ghcr.io/macula-io/macula-station:latest .
```

**Published tags** (`.github/workflows/ci.yml`):

| Trigger | Tags pushed | Platforms |
|---|---|---|
| Push to `main` | `:main`, `:<git-sha>` | `linux/amd64` only (fast path) |
| Push a `v*` tag | `:vX.Y.Z`, `:vX.Y`, `:latest` | `linux/amd64` + `linux/arm64` |

**`:latest` only moves on a version tag, not on every `main` push.** A
watchtower-tracked box does not roll on ordinary commits — only on an
actual release. If you need last-known-good, every `:<git-sha>` from a
`main` build stays in the registry as a pinnable rollback target; every
`vX.Y.Z` does too, permanently.

---

## 3. Config file

One JSON file, mounted read-only, pointed to by `MACULA_STATION_CONFIG`
(default `/etc/macula-station/config.json`). Full shape, defaults included
where the loader has one (`macula_station_config.erl`):

```json
{
  "data_dir":     "/var/lib/macula/station",
  "identity_file": "/var/lib/macula/station/identity.erl.bin",
  "bind":         "2600:3c1a:e001:19::be:01",
  "port":         4433,
  "certfile":     "/certs/station.crt",
  "keyfile":      "/certs/station.key",
  "capabilities": 0,

  "outbound_peers": [
    { "host": "station-de-frankfurt.macula.io", "port": 4433 }
  ],

  "cache": {
    "path": "/var/lib/macula/station/cache",
    "flush_period_ms": 30000
  },

  "rebootstrap": {
    "min_viable_peers": 8,
    "check_period_ms": 5000,
    "partition_window_ms": 60000
  },

  "peering_redundancy": {
    "min_station_peers": 3,
    "check_period_ms": 60000,
    "cooldown_ms": 300000,
    "candidate_pool": 32
  },

  "admin": { "bind": "127.0.0.1", "port": 8443 },

  "geo": {
    "hostname": "station-be-brussels.macula.io",
    "city":     "Brussels",
    "country":  "BE",
    "lat":      50.8503,
    "lng":      4.3517,
    "power_m":  1200
  },

  "bootstrap": {
    "discoverers": [],
    "cascade_opts": {}
  }
}
```

Required: `data_dir`, `bind`, `port`, `certfile`, `keyfile`. Everything else
has a default and can be omitted — `admin`, `cache`, `rebootstrap`, and
`peering_redundancy` fall back to the values shown above if the whole block
is absent.

**`geo.hostname` is optional and deliberately so.** A station with no DNS
entry still boots, peers, and routes fine — `geo.hostname` only feeds the
realm topology dashboard's display, not connectivity. This is a supported,
tested configuration (a real no-DNS station has been run live specifically
to prove it), not an edge case the loader merely tolerates.

`outbound_peers` is how a station finds the rest of the mesh — it dials
these on boot and stays connected. A station with an empty list boots
successfully but is an island until either a peer dials *it*, or you add at
least one entry.

---

## 4. The certificate gotcha that will bite you once

**The certificate at `certfile`/`keyfile` must be self-signed from the
station's own Ed25519 identity — an ordinary CA-issued or ad-hoc
`openssl req` certificate (EC/P-256, RSA, whatever) will NOT work.**

Why: a station's peers validate its connection two ways — `Trust::WebPki`
(ordinary CA-chain validation, needs a real DNS name and a real
CA-issued cert) or `Trust::Pinned{node_id}` (pins the station's raw Ed25519
public key straight from the cert's SPKI, no CA and no DNS required — this
is what makes a no-DNS station like the one in §3 reachable at all). Pinned
trust can only work if the certificate's key actually **is** the station's
identity key — a generic self-signed cert generated independently has a
different keypair, and Pinned trust will reject it outright
(`expected Ed25519 SPKI, got OID ...` is the error you'll see; this is not
hypothetical, it's the exact failure a real deployment hit and had to
diagnose from the wire error).

**The fix — a two-phase boot, because the identity doesn't exist until the
station has booted once:**

1. **Phase 1**: start the container with a throwaway placeholder cert (any
   valid EC cert works — it only needs to let the process come up long
   enough to generate its identity) and the real config otherwise. On
   first boot, the station generates or loads its Ed25519 identity at
   `identity_file`.
2. Derive the real cert from that identity, from inside the running
   container:

   ```bash
   docker exec <container> /opt/macula_station/bin/macula_station eval '
     {ok, Id} = macula_station_identity:load_or_generate(
                   macula_station_identity:path_for(<<"/var/lib/macula/station">>)),
     Pub  = macula_identity:public(Id),
     Priv = macula_identity:private(Id),
     {ok, {Cert, Key}} = macula_quic:generate_self_signed_cert(Pub, Priv, [<<"your.hostname.or.anything">>]),
     ok = file:write_file("/certs/station.crt", Cert),
     ok = file:write_file("/certs/station.key", Key).
   '
   ```

3. **Phase 2**: restart the container — now with the real, identity-derived
   cert in place. `Trust::Pinned` connections will work from here on.

This whole sequence is scripted end to end (bootstrap placeholder → wait for
identity → derive real cert → restart) in
[`macula-demo/infrastructure/stations-linode-toronto/deploy.sh`](https://github.com/macula-io/macula-demo/blob/main/infrastructure/stations-linode-toronto/deploy.sh)
— copy that pattern rather than re-deriving it; it documents this exact
failure inline at the point it was found.

---

## 5. Docker + watchtower (this org's real station fleet's pattern)

**This is how the actual macula-station fleet runs** — seven boxes, one
station each, all on the public internet (Hetzner + Linode VPSes; see
`macula-demo/infrastructure/FLEET.md` for the current roster and IPs if you
have access to that repo). Every one of them is a standalone bare-metal/VPS
box with a public IPv4/IPv6 address — **not** a LAN or lab machine. A
station's entire job is accepting inbound QUIC from clients and other
stations anywhere on the internet; it cannot do that from behind NAT or a
private network with no public address of its own. `network_mode: host`
below only helps if the host itself already has one.

`macula-demo/infrastructure/FLEET.md` confirms this is genuinely the live
mechanism, not aspirational: "All seven track `STATION_VERSION=main`, so
watchtower's 60s poll auto-rolls them on every CI build of main."

**docker-compose.yml:**

```yaml
services:
  station:
    image: ghcr.io/macula-io/macula-station:latest
    restart: unless-stopped
    network_mode: host
    environment:
      MACULA_STATION_CONFIG: /etc/macula-station/config.json
      MACULA_NODE_NAME: macula_station
    volumes:
      - ./config.json:/etc/macula-station/config.json:ro
      - ./certs:/certs:ro
      - station_data:/var/lib/macula/station
    labels:
      - "com.centurylinklabs.watchtower.enable=true"

  watchtower:
    image: containrrr/watchtower
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    command: --interval 300 --label-enable

volumes:
  station_data:
```

`network_mode: host` — the station needs to bind a real routable
IPv4/IPv6 address for QUIC (`bind` in config.json), not a container-bridge
address peers can't reach. This is why the compose file has no `ports:`
section; the host network makes one unnecessary.

Watchtower polls ghcr for a new digest at the currently-running tag (usually
`:latest`) and recreates the container in place when it changes — no
redeploy step beyond pushing a `v*` tag (§2). It does not touch config files
or volumes.

**⚠ A push to `main` that isn't path-filtered rebuilds `:main`/`:<sha>`, not
`:latest` — but if any box in your fleet tracks `:main` instead of a real
release tag, that box rolls on every commit, code included.** Point
watchtower-managed boxes at `:latest` (or a pinned `vX.Y.Z`) and reserve
`:main`/`:<sha>` for staging.

---

## 6. Podman + Quadlet

**No macula-station instance anywhere in this org runs on Podman.** This
org does run Podman elsewhere (a lab box, for unrelated services — never a
station), which is the only reason an earlier draft of this guide wrongly
implied it was also a station pattern; it isn't, and nothing below is
verified against a real station deployment. Docker + watchtower (§5) is
this org's only actual station deployment mechanism. This section is a
correct starting point for an operator who wants to run a station under
Podman on their own infrastructure, not a description of anything this org
does.

Podman is genuinely different infrastructure from Docker/watchtower, not an
interchangeable substitute — it uses systemd-native Quadlet units plus
`podman auto-update` instead of watchtower. Don't run watchtower alongside
Podman-managed containers: watchtower recreates containers directly and
fights with systemd for ownership of the same unit.

`~/.config/containers/systemd/macula-station.container`:

```ini
[Unit]
Description=Macula Station

[Container]
Image=ghcr.io/macula-io/macula-station:latest
Network=host
Volume=%h/macula-station/config.json:/etc/macula-station/config.json:ro
Volume=%h/macula-station/certs:/certs:ro
Volume=macula-station-data.volume:/var/lib/macula/station
Label=io.containers.autoupdate=registry

[Service]
Restart=always

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user start macula-station
```

`podman-auto-update.timer` (system-wide, one timer serves every
`io.containers.autoupdate`-labeled unit on the host — its cadence applies to
everything, not just this one) pulls a changed digest at the tracked tag and
restarts the **unit**, which is what keeps the systemd-managed state and the
running container from drifting apart. A content edit to the config or
volume mount itself still needs a manual `daemon-reload && restart` — the
timer only reacts to image digest changes, not file changes.

---

## 7. Kubernetes

**Not how this org runs macula-station today** — the real fleet is Docker +
watchtower on public boxes (§5), and a prior k3s deployment (for unrelated
services, not stations) was decommissioned org-wide. This section is for
operators who want to run a station on their own cluster; treat it as a
correct starting point, not a battle-tested one.

The main adaptation from the compose/Quadlet forms: QUIC needs a real
routable address per station, and `hostNetwork` is the direct Kubernetes
equivalent of `network_mode: host` — a `ClusterIP`/`LoadBalancer` Service in
front of a UDP QUIC listener does not preserve the per-connection
5-tuple the way stations expect, so don't put one there. Each station pod
gets the node's own IP.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: macula-station
spec:
  serviceName: macula-station
  replicas: 1
  selector:
    matchLabels: { app: macula-station }
  template:
    metadata:
      labels: { app: macula-station }
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      containers:
        - name: station
          image: ghcr.io/macula-io/macula-station:v1.0.0   # pin a real tag, not :latest
          env:
            - { name: MACULA_STATION_CONFIG, value: /etc/macula-station/config.json }
          ports:
            - { containerPort: 4433, protocol: UDP }
            - { containerPort: 8443, protocol: TCP }
          volumeMounts:
            - { name: config, mountPath: /etc/macula-station, readOnly: true }
            - { name: certs,  mountPath: /certs, readOnly: true }
            - { name: data,   mountPath: /var/lib/macula/station }
          readinessProbe:
            httpGet: { path: /wire, port: 8443 }
            periodSeconds: 10
          livenessProbe:
            httpGet: { path: /wire, port: 8443 }
            periodSeconds: 30
            failureThreshold: 3
      volumes:
        - name: config
          configMap: { name: macula-station-config }
        - name: certs
          secret: { secretName: macula-station-certs }
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: [ReadWriteOnce]
        resources: { requests: { storage: 5Gi } }
```

Pin an explicit `vX.Y.Z` tag rather than `:latest` here — nothing in a plain
Kubernetes Deployment/StatefulSet polls the registry the way watchtower or
`podman auto-update` do; `:latest` on Kubernetes just means "whatever was
current the first time this pod scheduled," silently, which is worse than
either fleet pattern above. If you want auto-updates, put a tool like Flux
or Keel in front of it.

Same certificate rule as §4 applies: derive `certs`'s Secret from the
station's own identity after first boot, don't hand it a generic
self-signed cert.

---

## 8. Health and readiness

`GET /wire` on the admin port (`8443` by default) — **not** `/status`.
`/status` is hardcoded `200` regardless of actual health; a station can be
receiving every packet sent to it and dispatching none while `/status`
stays green (this happened live, for 30 hours, before the healthcheck was
fixed to use `/wire` instead — see the `Dockerfile`'s own `HEALTHCHECK`
comment). `/wire` returns `503` specifically when the kernel is holding
undispatched datagrams on the station's own listener socket, which is the
actual failure mode that matters.

**Going unhealthy is not sufficient on its own to recover a stuck
station** — a `restart: unless-stopped` policy (Docker) or default pod
restart policy only reacts to the process actually exiting, not to
`HEALTHCHECK`/`readinessProbe` reporting unhealthy while the process stays
up. An unhealthy-but-alive station is visible in `docker ps` / `kubectl get
pods` and to fleet monitoring, but nothing restarts it automatically from
that signal alone unless you wire something that watches for it
(Kubernetes' `livenessProbe` with `failureThreshold`, shown above, does
handle this correctly out of the box; the Docker/Podman forms in §5 and §6
do not, and need an external watcher if you want the same guarantee).

---

## 9. See also

- [`README.md`](../README.md) — what a station is, architecture overview
- `macula-demo/infrastructure/FLEET.md` — the real fleet's current roster, IPs, and dial graph (private repo)
- [`docs/CASCADE_INVESTIGATION.md`](CASCADE_INVESTIGATION.md) — a real production incident and its fix, useful context for what "unhealthy" has actually meant in practice on this fleet
