# Macula Station

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Erlang/OTP](https://img.shields.io/badge/Erlang%2FOTP-28-brightgreen)](https://www.erlang.org)
[![Container](https://img.shields.io/badge/ghcr.io-macula--station-blue?logo=docker)](https://github.com/macula-io/macula-station/pkgs/container/macula-station)
[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-support-yellow.svg)](https://buymeacoffee.com/rlefever)

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/macula-station-full-dark.svg">
    <img src="assets/macula-station-full-light.svg" alt="Macula Station" width="320">
  </picture>
</p>

<p align="center">
  <strong>The relay station for the Macula mesh</strong>
</p>

---

## What is a station?

A station is a **realm-agnostic relay** — one process, one Ed25519 identity,
peering outbound with other stations over QUIC to form a mesh. Clients
(the [Macula SDK](https://github.com/macula-io/macula), and anything built
on it — daemons, apps, services) connect outbound to a station; the station
routes their RPC calls, fans out their pub/sub events, relays their content
transfers and streams, and answers DHT queries on their behalf. No open
inbound ports on the client side, no VPN, no central broker any one
operator controls.

A station never joins a realm itself and holds no application data —
realm membership and identity belong to the daemons that attach through it.
Run one station and it's an island; give it `outbound_peers` (or let
another station dial it) and it's part of the mesh.

**What a station does, concretely:**

- **DHT** — Kademlia routing table, signed TTL'd records (advertisements,
  endpoints, presence facts), multi-round iterative lookup.
- **SWIM-Lifeguard** liveness — direct + indirect probing, failure
  detection, membership gossip.
- **PubSub** — per-realm topic/subscriber registries, cross-station EVENT
  relay (bloom-filtered fan-out so interest doesn't flood the whole mesh).
- **RPC relay** — CALL/RESULT routing to whichever station a provider is
  actually advertised on, including multi-hop (a call can cross two
  stations with no direct peering edge between them).
- **Streaming RPC relay** — the same routing for `STREAM_OPEN`/`DATA`/
  `END`, dedicated QUIC streams per session.
- **Content transfer** — content-addressed block/manifest store (MCID,
  BLAKE3), chunked put/get, discovery.
- **Overlay** (HyParView + Plumtree) — bounded partial-view membership and
  epidemic broadcast trees, absorbed from the standalone
  `macula-hyparview`/`macula-plumtree` packages.
- **Bootstrap cascade** — ordered peer-discovery strategies for a station
  joining the mesh cold.

The station server itself is Erlang/OTP; it consumes the
[Macula SDK](https://github.com/macula-io/macula) for identity, the wire
protocol, and the QUIC transport (a Rust NIF), the same SDK every client
uses — a station and a daemon speak the identical wire format because
they're built on the identical codec.

---

## Status

**In production, first tagged release `v0.1.0`.** This has been under
continuous development since 2026-04-14 (500+ commits) and runs live today
on a real multi-station fleet — the DHT, SWIM, pub/sub relay, RPC relay,
streaming relay, and content transfer are all exercised against real
traffic, not just tests. Sub-apps stay tagged `0.1.0` internally by design;
the deployable unit is a container image (`:main`/`:<sha>` off every
commit, `:vX.Y.Z`/`:vX.Y`/`:latest` off a real tag — see the
[Deployment Guide](docs/DEPLOYMENT_GUIDE.md)), not a hex package. If you
need the exact behavior a given box is running, the image tag is the
source of truth, not a version number in this repo — `v0.1.0` marks
"first release worth pointing an outsider at," not a stability guarantee.

The full architecture plans (`plans/PLAN_MACULA_V2_*`) predate the current
state and describe design intent, not current status — treat them as
background, not as a phase tracker; there is no "Phase N" this repo is
currently "in."

---

## Quick start

```bash
docker pull ghcr.io/macula-io/macula-station:latest
```

A station needs three things to boot: a config file, a certificate
**derived from its own identity** (this is the part that trips people up —
see the Deployment Guide before you improvise one), and a place to persist
its identity and cache.

```bash
docker run --network=host \
  -e MACULA_STATION_CONFIG=/etc/macula-station/config.json \
  -v ./config.json:/etc/macula-station/config.json:ro \
  -v ./certs:/certs:ro \
  -v station_data:/var/lib/macula/station \
  ghcr.io/macula-io/macula-station:latest
```

**Read [`docs/DEPLOYMENT_GUIDE.md`](docs/DEPLOYMENT_GUIDE.md) before running
this for real** — it covers the full config schema, the certificate/identity
bootstrap sequence (a station's cert has to be self-signed from its own
Ed25519 key, not an arbitrary one — the wrong kind of cert boots fine and
then silently fails every `Trust::Pinned` connection), docker-compose +
watchtower, Podman + Quadlet, and a Kubernetes manifest.

---

## Architecture

Nine sub-applications under one umbrella:

| App | Role |
|---|---|
| `macula_station` | Core: server, peer/connection observer, config loader, station identity, peering router. Everything else plugs into this. |
| `macula_dht` | Kademlia DHT — routing table, signed record storage, iterative lookup. |
| `macula_swim` | SWIM-Lifeguard failure detector — liveness probing, membership. |
| `macula_routing` | Path computation over the routing-table graph (Dijkstra + iterative-greedy). |
| `macula_bootstrap` | Ordered peer-discovery cascade for cold start. |
| `macula_content` | Content-addressed block/manifest store — MCID, BLAKE3, chunking. |
| `macula_handler` | Station-level RPC procedure registry — local handlers + remote-advertise tracking that makes multi-hop CALL routing possible. |
| `macula_transport` | Adapter over the SDK's `macula_quic` (Rust NIF) transport. |
| `hecate_realm` | Transitional placeholder — realm-adjacent functionality still migrating to its own service repo. |

---

## Build & test

```bash
rebar3 compile
rebar3 eunit
```

1076 tests, no CT/live-fleet dependency for the standard suite. Dialyzer is
part of CI (`.github/workflows/ci.yml`).

---

## Documentation

| Guide | Description |
|---|---|
| [Deployment Guide](docs/DEPLOYMENT_GUIDE.md) | Config schema, the certificate/identity bootstrap, Docker + watchtower, Podman + Quadlet, Kubernetes |
| [Cascade Investigation](docs/CASCADE_INVESTIGATION.md) | A real production incident (fleet-wide restart cascade) — root cause and fix |
| [PubSub Resign Loop Lesson](docs/PUBSUB_RESIGN_LOOP_LESSON.md) | Why a specific warning log line is load-bearing, and what regressed four times trying to remove it |
| [Subscribe Records Gap](docs/SUBSCRIBE_RECORDS_GAP.md) | A topic-name mismatch between SDK and station that silently broke a callback — resolved |
| [DHT Find Flake Attempt](docs/DHT_FIND_FLAKE_ATTEMPT.md) | Two attempts at a cross-station DHT lookup flake, what the first one broke, what the second one fixed |
| [`plans/`](plans/) | Original design documents (`PLAN_MACULA_V2_ROOT.md` onward) — architecture intent, not current-status tracking |

---

## Relationship to other repos

| Repo | Role |
|---|---|
| [`macula-io/macula`](https://github.com/macula-io/macula) | The SDK every client (and this station) is built on — identity, wire protocol, QUIC transport. |
| `macula-io/macula-station` (this) | The relay station server. |
| `macula-io/macula-realm` | Realm identity + certificate authority service. |
| [`macula-io/macula-demo`](https://github.com/macula-io/macula-demo) | Live example deployments, including the reference no-DNS station setup the Deployment Guide's certificate section points to. |

---

## License

Apache-2.0 — see [`LICENSE`](LICENSE).

---

<p align="center">
  <sub>Built with the BEAM</sub>
</p>
