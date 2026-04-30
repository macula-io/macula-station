# PLAN_RECORD_FANOUT_REFACTOR.md — collapse per-identity fact_publisher into a node-singleton fanout

**Status:** in flight 2026-04-29
**Trigger:** mesh stream_closed-after-30s outage (R=4 D=1) traced to per-identity
`macula_station_fact_publisher` heap leak under `pg`-based O(N²) cross-publish
fan-out.
**Scope:** one PR, ~150 LOC. Same protocol, same delivery semantics.
**Out of scope:** signing-model changes, single shared pubsub_server per realm
(C-3 — separate sprint).

## 1. Problem

Each virtual relay identity runs its own `macula_station_fact_publisher`
gen_server. When identity X's DHT stores a record, X's publisher:

1. Classifies the record + publishes on X's per-identity `pubsub_server`.
2. Casts `{cross_publish, R}` to **all N-1 sibling publishers on the same node**
   via `pg`. Each sibling re-runs classify+publish on its own `pubsub_server`.

Step 2 exists so a realm subscribed to ANY one identity sees records from ALL
identities on that box. It is documented as "Phase 1 gossip" in the source.

Under load (≥30 active identities, daemon reconnect cycles):

- N publishers × every record → **O(N²) casts per second, network-wide.**
- Each cast inflates the receiver's heap with the Record term.
- Default `fullsweep_after = 65535` → minor GCs collect young gen only,
  old-gen heap monotonically grows.
- Per-publisher heap rises ~1 MB/min. 30 pubs × 1 MB/min × 30 min = ~1 GB.
  Helsinki peaked at 1.8 GB before kernel pressure → GC pauses → QUIC heartbeat
  misses → daemon reconnect storm → station CPU peg → put_record CALL
  timeouts → stream_closed → daemons cycle → R=4 D=1 floor.

Forensics evidence: top-N memory hogs on Nuremberg are all
`macula_station_fact_publisher`, all at ~36 MB at 23-min uptime; bin_refs
account for <1 MB; the rest is Erlang heap retention. See session
`2026-04-29_mesh-stream-closed-bug-handover.md` and the diagnostic scripts
under `scripts/forensics.sh` + `scripts/inspect_proc.sh`.

## 2. Goal

Collapse N fact_publishers into a single node-singleton fanout process. Same
external semantics (every per-identity `pubsub_server` still receives every
record), but:

- Fan-out runs in **one** gen_server, not N.
- Total work per record: **N synchronous publishes from one process** instead
  of **N² casts across N processes**.
- One bounded heap (hibernate + low fullsweep_after), not N unbounded ones.
- `pg`-based cross-publish broadcast is deleted entirely. The `pg` scope
  `macula_station_facts` is removed.

## 3. Non-goals (explicitly deferred)

- **Single shared pubsub_server per realm per node** (would aggregate
  subscribers across identities). Defer to C-3 sprint — touches the EVENT
  signing model and the SUBSCRIBE/UNSUBSCRIBE dispatch path.
- **Cross-box gossip** (Plumtree). Already deferred per
  `PLAN_DEFERRED_WORK.md` §6 — unchanged.
- **Per-call backpressure** on the fanout. Hibernate + heap-sizing addresses
  the immediate memory failure mode; deeper backpressure is C-3 territory.

## 4. New shape

```
                 +-----------------------------+
                 | macula_station_record_fanout |  (singleton, gen_server)
                 +-----------------------------+
                          ^
                          | cast {record_stored, IdentityKey, Record}
                          |
   +----------+   +----------+         +----------+
   | DHT (X) |   | DHT (Y) |   ...     | DHT (Z) |   (per-identity)
   +----------+   +----------+         +----------+
        ^              ^                    ^
        | each DHT's on_record_stored callback
        | fires the SAME singleton, identifying source by IdentityKey

On cast, fanout snapshots the identity_registry's IdentityKey → handles
map and, for EACH identity I:
    - call I's pubsub_server:publish/3 (returns {Frame, MatchedSubs})
    - resolve each MatchedSub's ConnPid via I's peer_observer
    - macula_peering:send_frame(ConnPid, Frame)
```

Same delivery surface as today; one process doing the work; one heap to
bound. The per-identity pubsub_server still owns its subscription state —
no protocol change.

## 5. Files

### New

- `apps/macula_station/src/macula_station_record_fanout.erl`
  - `start_link/1`, `on_record/3` (cast API), `init/1`, `handle_cast/2`
  - `{spawn_opt, [{fullsweep_after, 10}, {min_heap_size, 1024}]}` at start
  - `{noreply, S, hibernate}` after each record dispatch
  - State holds `identity_registry :: pid()` only. Identity snapshot fetched
    per-cast (cheap, ETS-backed).
- `apps/macula_station/test/macula_station_record_fanout_tests.erl`

### Modified

- `apps/macula_station/src/macula_station_identity_registry.erl`
  - Add `snapshot/1` returning `#{IdentityKey => {pubsub_registry_pid,
    peer_observer_pid, identity}}`. Powered off the existing internal map
    (which already keys by hostname). Tracking pubsub_registry + observer
    requires the registry to learn those pids when each identity starts —
    plumb via the `on_identity_ready` notification from
    `macula_station_identity_sup` once the Phase-3 chain has produced both.
- `apps/macula_station/src/macula_station_identity_sup.erl`
  - Replace `install_dht_callback/2` body — callback now invokes
    `macula_station_record_fanout:on_record(IdentityKey, Record)` instead of
    `macula_station_fact_publisher:on_record_stored(FpPid, Record)`.
  - Drop the `seed_fact_publisher/4` step from the procedural Phase-3 chain.
    Boot order becomes `…peer_observer → overlay_seeder → announcer →
    listener` (skipping fact_publisher entirely).
  - At the point where fact_publisher used to install the DHT callback, the
    sup announces `{identity_ready, IdentityKey, PubsubRegPid, PeerObsPid,
    Identity}` to the singleton fanout (or to identity_registry, which then
    relays).
- `apps/macula_station/src/macula_station_sup.erl`
  - Add the singleton `macula_station_record_fanout` as a permanent child.

### Deleted

- `apps/macula_station/src/macula_station_fact_publisher.erl`
- `apps/macula_station/test/macula_station_fact_publisher_tests.erl`

(Vertical slicing — its sole responsibility is moving into the fanout. No
"thin helper" left behind. Rename the existing tests' coverage into the new
fanout tests where it still applies.)

## 6. Semantics preserved

| Property | Before | After |
|----------|--------|-------|
| Realm subscribed to identity A sees records put on identity B? | Yes (via pg) | Yes (fanout publishes on every identity's pubsub_server) |
| Per-identity pubsub_server signs EVENT frames with identity's keypair? | Yes | Yes |
| Subscriber's connection-by-pubkey lookup uses the identity-attached peer_observer? | Yes | Yes |
| `_mesh.station.announced_v1` / `_mesh.daemon.announced_v1` topic catalog? | Yes | Yes (classify clauses move to fanout verbatim) |
| pg scope `macula_station_facts`? | Used for cross-publish | **Removed** |

No protocol change. No frame-shape change. No subscriber-visible difference.

## 7. Test strategy

- **Unit (eunit):** new `macula_station_record_fanout_tests` mirrors the
  existing fact_publisher_tests classification cases (record types 0x01,
  0x0C, payload kinds station/daemon, key-shape variants).
- **Integration (CT):** the existing per-identity station CTs already cover
  "subscriber attached to identity A sees records from identity B" — they
  must continue to pass after the refactor. No new CT.
- **Memory regression:** add a small load-loop in eunit that drives 1000
  record_stored casts through fanout against 30 fake identity entries, then
  asserts `process_info(FanoutPid, memory) < 5_000_000` post-hibernate.
  Catches future regressions of the heap-leak shape.

## 8. Rollout

1. Code + tests in feature branch.
2. `rebar3 compile`, `rebar3 eunit`, `rebar3 ct`, `rebar3 dialyzer`,
   `rebar3 xref` all clean.
3. Push branch + open PR. CI builds and pushes the OCI image to ghcr.io
   under a canary tag (or the branch's auto-tag).
4. Deploy via `macula-demo/infrastructure/relays-*/` — bump
   `STATION_VERSION` to the new commit on Helsinki ONLY first. Watch for
   30 min: memory should plateau, not climb.
5. If Helsinki stable, roll Nuremberg + Paris. Stub fleet stays OFF
   throughout — re-enable only after R/D topology recovers under base+city
   load.
6. Once steady, restart the stub fleet in tranches (50 at a time, 5-min gap)
   and watch.

## 9. Acceptance

- `process_info(fanout_pid, memory)` stays <10 MB under 100 record_stored/s
  for 30 min on the Helsinki box.
- Realm topology view recovers to pre-outage R/D values once daemons +
  city daemons reconnect.
- No new error logs from the fanout under load (no overflow, no
  pubsub_server timeouts).

## 10. Follow-up: C-3 (separate sprint)

When the fleet is stable, plan C-3:
- Single shared `hecate_pubsub_server` per realm per node (not per
  identity).
- Subscriber state aggregated, tagged with `source_identity_key` so
  delivery still routes via the right peer_observer.
- Frame signing: a station-canonical signing identity, OR per-recipient
  resign — open question.
- Drops the per-identity pubsub_registry + pubsub_server from N copies to
  1 per realm. Reduces base memory another 5-10x.

C-3 is the right end-state. C-2 buys the breathing room to design it
properly without an active outage.
