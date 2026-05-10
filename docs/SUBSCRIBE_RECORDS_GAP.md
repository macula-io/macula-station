# `subscribe_records/3` — substrate-SDK topic mismatch

**Status: RESOLVED 2026-05-10.** Both probes green. Fix shipped in macula `v4.2.9` (SDK callback decode) + macula-station commit `57f4c8d` (per-type substrate publication on `_dht.records.<type>.stored`).

This document is preserved as a record of what the gap was and how it was closed, for whichever future engineer hits a similar substrate-SDK contract mismatch.

## Original report

Surfaced 2026-05-10 by the new e2e probes `subscribe_records_local` and `subscribe_records_cross_station`. The SDK API doesn't fire because the substrate publishes record-stored events on a different pubsub topic than the SDK subscribes to.

## What the probes show

```
=== subscribe_records_local ===
Failed 1 tests. Passed 0 tests.
=== subscribe_records_cross_station ===
Failed 1 tests. Passed 0 tests.
```

Both single-station and cross-station fail with `{no_record_callback_within_ms, 5000}` (or 10000 for cross-station). The probe correctly subscribes via `macula:subscribe_records/3`, puts a record via `macula:put_record/2`, and waits. The callback never fires.

## The mismatch

**SDK** (`macula-io/macula/src/macula.erl`):

```erlang
subscribe_records(Pool, Type, Callback)
  when is_pid(Pool), is_integer(Type), Type >= 0, Type =< 255,
       is_function(Callback, 1) ->
    Topic = record_stored_topic(Type),         %% "_dht.records.<type>.stored"
    macula_pubsub:subscribe_callback(Pool, ?DHT_REALM, Topic,
                                     wrap_record_callback(Callback)).

record_stored_topic(Type) ->
    iolist_to_binary(io_lib:format("_dht.records.~B.stored", [Type])).
```

So a `subscribe_records(Pool, 1, F)` call subscribes to topic `_dht.records.1.stored` on realm `<<0:256>>`.

**Substrate** (`macula-internal/macula-station/apps/macula_station/src/macula_station_record_fanout.erl`):

```erlang
node_announce_topic(<<"daemon">>) -> <<"_mesh.daemon.announced_v1">>;
node_announce_topic(_)            -> <<"_mesh.station.announced_v1">>.

node_depart_topic(<<"daemon">>) -> <<"_mesh.daemon.departed_v1">>;
node_depart_topic(_)            -> <<"_mesh.station.departed_v1">>.
```

The substrate publishes only `_mesh.station.announced_v1`, `_mesh.daemon.announced_v1`, `_mesh.station.departed_v1`, `_mesh.daemon.departed_v1`. ONLY for `node_record` (type 0x01). NOT on the topic the SDK subscribes to. NOT for any other record type.

## Impact

The PLAN_DNS_OVER_MESH_PART1 design relies on `subscribe_records/3` for the `on_record_observed_invalidate_cache` PM:

> PMs in the DNS slice:
> - `on_record_observed_invalidate_cache` — reacts to `record_observed_v1` events from `macula_dht`
> - `on_realm_directory_changed_warm_cache` — reacts to `realm_directory_changed_v1` events

Neither event currently exists in the substrate. The DNS slice as designed cannot use the SDK API for cache invalidation; it would have to fall back to TTL-based polling against `expires_at`, or wait for one side of the mismatch to be fixed.

## Possible resolutions

1. **Fix the substrate side.** Add per-type publication (`_dht.records.<type>.stored`) in `macula_station_record_fanout` so the SDK API works as documented. Smallest delta. Doesn't break existing `_mesh.station.announced_v1` subscribers.

2. **Fix the SDK side.** Change `subscribe_records/3` to subscribe to `_mesh.<class>.announced_v1` / `_mesh.<class>.departed_v1` and dispatch to the callback based on payload kind. More invasive — changes the SDK's public API contract.

3. **Replace the SDK API.** Acknowledge that `subscribe_records/3` is the wrong abstraction and provide a different per-class API (`subscribe_announcements`, `subscribe_departures`). Largest change.

Option 1 is recommended — it makes the existing SDK API work, leaves the substrate's `_mesh.station.*` topics intact for whichever consumer needs them, and unblocks the DNS slice's cache-invalidation PM design.

## Until resolved

The probes `subscribe_records_local` and `subscribe_records_cross_station` will fail. Treat them as regression detectors: when either side of the mismatch gets fixed, the probes will start passing and surface that fact in the e2e suite output. Don't suppress them.

The DNS slice scaffold can proceed; the cache-invalidation PM should be sketched against `subscribe_records/3` per the plan but left as TODO, with a fallback noted that activates if `subscribe_records` doesn't fire within a startup smoke-check window.

## Commits

- `8831d1e` (macula-e2e) — added `subscribe_records_local` + `subscribe_records_cross_station` probes that surfaced the gap
- `4a599c6` (macula-io/macula `v4.2.9`) — SDK side: `wrap_record_callback` decodes the wire payload via `macula_record:decode/1` before invoking the user fun
- `57f4c8d` (macula-station) — substrate side: `record_fanout` publishes on `_dht.records.<type>.stored` for every record type, alongside the existing `_mesh.station.*` / `_mesh.daemon.*` topics
- `983307f` (macula-station) — bumps macula dep 4.2.8 → 4.2.9 (hex)
- `5c2cf1c` (macula-e2e) — same dep bump for the suite

Verification: `subscribe_records_local` + `subscribe_records_cross_station` pass 6/6 across consecutive runs after the fix lands.
