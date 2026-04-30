# Plan: Migrate macula-station wire codec from BERT to CBOR

**Status:** In progress
**Created:** 2026-04-26
**Drives:** macula-station, macula SDK (additive — frame type IDs)

## Why

macula-station and the macula SDK speak different wire formats. macula
3.x ships CBOR (RFC 8949) per Part 6 §3 — the canonical Macula V2 wire.
macula-station's `hecate_frame.erl` ships BERT (`term_to_binary`
deterministic). When a macula client (the realm's `macula_mesh_client`
or any daemon) connects to a macula-station listener:

1. QUIC handshake completes (TLS works — both speak `*.macula.io` ALPN).
2. macula client sends a CBOR-encoded CONNECT frame.
3. Station's `hecate_peering_conn:consume_handshake/2` calls
   `hecate_frame:parse_stream/1`.
4. Parse fails or yields garbage; no `connect` frame surfaces.
5. Handshake stalls; ConnPid never registered in `peer_observer`'s
   `peers` map.
6. Subsequent CALL frames hit `route(_Frame, undefined, _S)` and are
   silently dropped — realm sees timeouts.

This breaks every realm/daemon → station integration. Fixing it is the
last step before `PLAN_DHT_FIRST.md` (macula-realm) actually delivers
read models from DHT records.

## Inventory

| Frame family | macula-station | macula SDK | Action |
|---|---|---|---|
| CONNECT / HELLO / GOODBYE | `hecate_frame` | `macula_protocol_types` (`connect`, `disconnect`) | Align |
| SWIM (ping/ack/suspect/confirm/update) | `hecate_frame` | (none) | Add to SDK |
| DHT (ping/pong/find_node/nodes/find_value/value/store/store_ack/replicate/replicate_ack) | `hecate_frame` | (none) | Add to SDK |
| CALL / RESULT / ERROR | `hecate_frame` (Part 6 §5) | `macula_protocol_types` (`call`, `result`) | Align |
| HyParView (join/forward_join/neighbor/disconnect/shuffle/shuffle_reply) | `hecate_frame` | (none) | Add to SDK |
| Plumtree (gossip/ihave/graft/prune) | `hecate_frame` | (none) | Add to SDK |
| PUBLISH | `hecate_frame` (deferred) | `macula_protocol_types` (`pubsub_event`) | Align |

## Wire layout

Adopt macula's existing 8-byte header:

```
0       7        15       23       31
+--------+--------+--------+--------+
| ver    | type   | flags  | rsvd   |
+--------+--------+--------+--------+
|         payload_length          |
+----------------------------------+
|         CBOR payload            |
+----------------------------------+
```

`type` is a 1-byte type id (per `macula_protocol_types`). Hecate frames
get type ids in the unused high range (0x40–0xFF for now). SDK gains
those ids in `macula_protocol_types:message_type_id/1`.

Replaces hecate's current `<<Length:32/big, BERT/binary>>` framing.

## Phases

1. **Inventory + reserve type ids** — add hecate-specific frame ids to
   `macula_protocol_types` in macula SDK; bump SDK to 3.6.0.
2. **Rewrite `hecate_frame.erl` wire codec** — `encode/1` / `decode/1`
   / `parse_stream/1` / `canonical_unsigned/1` swap `term_to_binary`
   for `macula_cbor_nif:pack`. 8-byte header layout. `frame_type` atom
   stays internal; serialised as type id.
3. **Update `hecate_frame_tests.erl`** — every BERT round-trip test
   becomes a CBOR round-trip test. Same field assertions.
4. **Verify downstream consumers** — `hecate_peering`, `macula_dht`,
   `macula_swim`, `hecate_overlay` all use `hecate_frame` constructors
   only; no direct `term_to_binary` calls outside the codec module.
   Should be a recompile.
5. **Verify station boots** — `rebar3 eunit` + `rebar3 ct`.
6. **Re-deploy fleet** — Nuremberg / Helsinki / Paris pull the new
   image.
7. **Verify realm sees DHT records** — `MeshSubscriber` snapshot
   succeeds, `[RealmIdentities] recorded`, `[license_*] appended`.

## Out of scope

- Frame schema alignment with macula SDK (field-by-field). hecate's
  CONNECT carries `realms`, `capabilities`, `station_id`; macula's
  CONNECT differs. Schema unification is its own follow-up.
  This plan only changes the BYTES on the wire — same frame map,
  CBOR encoded — so the macula client can decode the type byte and
  the hecate-side handshake fields land in the right map keys.
- Removing `hecate_frame.erl` entirely (replacing with macula SDK
  primitives). That's the bigger refactor; this plan ships wire
  compatibility first.

## Status

| Phase | Status |
|---|---|
| 1 — type ids in SDK | pending |
| 2 — codec rewrite | pending |
| 3 — tests | pending |
| 4 — downstream verify | pending |
| 5 — local CI green | pending |
| 6 — fleet redeploy | pending |
| 7 — realm read-models populate | pending |
