# Plan: Mesh Discovery Infrastructure

**Status:** Draft for review
**Created:** 2026-04-25
**Last Updated:** 2026-04-25

## Why

The macula-realm dashboard counts 101 relays after the Paris + Helsinki
station cutover (only Nuremberg V1's 100 heartbeats + 1 macula.io local
relay). Stations are invisible to the realm because:

1. **macula SDK has no DHT record API.** `find_value`/`store` exist at
   the wire level but no client wrappers.
2. **hecate-station has no RPC infrastructure.** `hecate_handler/` is an
   empty app. Stations cannot advertise procedures (e.g. `list_stations`).
3. **No station-side publish injection point exposed yet.** V1's
   `macula_relay_identity:publish/2` had pg-based broadcast; station has
   `hecate_pubsub_server:publish/3` but no canonical "system event" path.

V1 papered over (1)+(2) by inventing `_mesh.relay.up` heartbeats every
60s and a Caddy-fronted `/topology` HTTP API. Both are wrong shape:
heartbeats waste mesh bandwidth, HTTP polling bypasses the mesh entirely.

This plan closes the gap properly. **No V1-compat band-aids — alpha
fleet, no production constraints.**

## Architectural shape

Discovery uses two complementary mechanisms:

1. **DHT records** for state (signed, TTL'd, cross-station-replicated).
   Late-joining subscribers query the current snapshot.
2. **PubSub events** (`_mesh.station.up`, `_mesh.station.down`) for
   live updates. One event per state change, not periodic.

No heartbeats. No realm-tag stuffing. Realm participates as a normal
mesh client, no privileged access to the underlying box.

## Track 1: macula SDK feature add

**Repo:** `macula-io/macula`

New client APIs:

```erlang
-spec put_record(client(), record()) -> ok | {error, term()}.
-spec find_record(client(), record_key()) -> {ok, record()} | {error, not_found}.
-spec find_records_by_type(client(), record_type()) -> {ok, [record()]} | {error, term()}.
-spec subscribe_records(client(), record_type(), fun()) -> {ok, ref()} | {error, term()}.
```

- `record()` is a signed payload with `{type_tag, key, payload, ttl,
  expires_at, signature, pubkey}`.
- Wire mapping uses existing `find_value` / `store` messages plus new
  type-indexed extension.
- `subscribe_records` is the live-update path: relay forwards
  record-stored events to subscribers filtered by type.

Tests, version bump (3.2.0), hex publish.

## Track 2: hecate-station RPC infrastructure

**Repo:** `hecate-social/hecate-station`

Fill in `apps/hecate_handler/`:

- `hecate_handler_registry` — gen_server tracking
  `{Procedure => HandlerFun}` per identity.
- `hecate_handler_advertise/2,3` — publish a procedure handler.
- `hecate_handler_dispatch/2` — called from the listener path when a
  CALL frame arrives, looks up handler, invokes it, builds REPLY frame.
- Wire CALL frames into the station listener's frame dispatch
  (currently lands at the peer observer + falls through unknown).

Tests + integration with station boot.

## Track 3: hecate-station station_announcer

**Repo:** `hecate-social/hecate-station`

Per-identity worker under `hecate_station_identity_sup`:

- On register: build a signed `station_record` (type tag `0x02`)
  carrying `{hostname, city, country, lat, lng, endpoint, pubkey,
  expires_at}`. Put into per-identity DHT via
  `hecate_dht:put_record/2` (already works server-side).
- Periodic refresh before TTL (e.g. TTL=10min, refresh=8min).
- On graceful unregister (operator stop / SIGTERM): publish a signed
  tombstone record + emit `_mesh.station.down` event via
  `hecate_pubsub_server:publish/3`.
- On register: emit `_mesh.station.up` event for live subscribers.

Topic + record type live in `_mesh` namespace, no realm tag stuffing.

Tests for register/refresh/unregister/crash paths.

## Track 4: cross-station mesh propagation

**Repo:** `hecate-social/hecate-station`

For pubsub events (`_mesh.station.up`/`down`) and DHT records to
propagate from Paris to a realm subscribed to Helsinki, the mesh
overlay must gossip system topics + records across station peers.

Audit: does HyParView/Plumtree currently carry traffic across
station peers? `hecate_overlay_sup` is an empty shell as of Phase 2
— need to check whether per-identity overlay instances peer with
remote stations or just serve their local subscribers.

If broken: wire it. This is the core mesh property — without it, the
realm needs to connect to every station in the fleet.

## Track 5: macula-realm migration

**Repo:** `macula-io/macula-realm`

- Drop V1 `_mesh.relay.up` heartbeat aggregation.
- Subscribe to `_mesh.station.up`/`_mesh.station.down` via macula SDK
  (per-identity hostname seeds, not box hostnames).
- On connect, call `find_records_by_type(client, 0x02)` for snapshot
  state.
- Aggregate into the existing topology read model. UI count flips to
  "stations" terminology.
- Drop the `MeshSubscriber.fetch_topology_from_relays` HTTP poller —
  obsolete once DHT-records work.

## Track 6: deploy + verify

- Bump macula 3.2.0 on hex.
- hecate-station rebuilds against 3.2.0.
- Roll out new station image to Paris + Helsinki + Nuremberg.
- Roll out new realm image to macula.io.
- Verify: macula.io dashboard shows 205 stations (Paris 5 + Helsinki
  100 + Nuremberg 100 once it's flipped).

## Estimate

| Track | Effort |
|---|---|
| 1. SDK records | 2-3 sessions |
| 2. station RPC | 2 sessions |
| 3. station_announcer | 1 session |
| 4. mesh propagation audit + fix | unknown — could be hours or weeks |
| 5. realm migration | 1 session |
| 6. deploy | 0.5 session |
| **Total** | **~7 sessions + propagation work** |

## Out of scope (deliberate)

- V1 macula-relay deprecation — happens after this lands and is
  stable.
- The Nuremberg cutover from V1 to station — re-evaluate after
  Track 4 (cross-station mesh) is verified, since 100 identities
  per box is the real propagation test.
- "MACULA_*" env-var rename to `HECATE_*` — separate cleanup.

## Open questions

1. **Where does the box hostname go?** Realm currently configured
   with box hostnames as seeds. Should those DNS records resolve to
   one of the per-identity IPv6s, or be removed entirely so realm
   uses per-identity hostnames?
2. **Record type registry.** Type tag `0x01` is `node_record` (per
   `hecate_station:tombstone_type/0`). `0x02` for `station_record`.
   Need a central registry (probably in macula's record codec) so
   types don't clash with future record types (capability_announcement,
   etc.).
3. **Permissioned put_record?** Anyone connected to a station can
   put records. Should there be a record-type allowlist per realm
   tag? Outside scope of discovery — tracked as a follow-on once
   there's a clear threat model.
