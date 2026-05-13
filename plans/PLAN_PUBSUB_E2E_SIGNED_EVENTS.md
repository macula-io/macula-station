# PLAN — Publisher-end-to-end signed EVENT envelopes ("pubsub Phase 2")

**Status:** design — not yet implemented.
**Author:** 2026-05-12.
**Supersedes:** the four reverted incremental attempts catalogued in `docs/PUBSUB_RESIGN_LOOP_LESSON.md`.
**Spans:** this repo (`macula-station`) **and** the `macula` SDK (`macula-io/macula`, hex-published) — coordinated change.

---

## 1. The problem

Cross-station pubsub delivery is bounded to **1 hop from the origin station**, by accident.

### Today's flow

1. A daemon publishes: `macula:publish/4,5` → sends a **PUBLISH frame**, signed by the publisher daemon, fields `{topic, realm, publisher, seq, payload}`. (`replication_factor` defaults to **1** — the PUBLISH hits exactly one station link.)
2. The origin station `SO` receives it: `macula_station_peer_observer:deliver_pubsub_typed(publish, …)` → `hecate_pubsub_registry:relay_publish` → `hecate_pubsub_server:relay_publish` → `build_relay_event` **re-signs a new EVENT frame with `SO`'s own identity** (`delivered_via => direct`), then `hecate_pubsub:deliver_event` returns the matched local subscribers.
3. `peer_observer:fan_out_event` sends that `SO`-signed EVENT to every matched subscriber = local daemon subscribers ∪ peer-station pubkeys that subscribed to the topic via `macula_station_peering_router`.
4. A direct peer station `S1` receives the EVENT: `dispatch(pubsub, Frame, _ConnPid, NodeId, S) -> deliver_pubsub(macula_frame:verify(Frame, NodeId), …)` — verified against the **connection's NodeId**. `NodeId == SO == the EVENT's signer` → verify passes → `deliver_inbound_event` → `deliver_event` matches `S1`'s local subscribers, **and `fan_out_event` re-fans the same `SO`-signed EVENT to `S1`'s peer stations**.
5. A 2-hop station `S2` receives the EVENT from `S1`: `NodeId == S1`, but the EVENT is signed by `SO` → `verify(Frame, S1)` **fails** → dropped with `signature_invalid`.

Step 5's drop is the **only** loop prevention in the pubsub relay path. It is *load-bearing* — every station holds mutual `_mesh.bloom` SUBSCRIBEs on every other station, so without the drop, EVENTs cycle the partial mesh unbounded and saturate every cross-station primitive (CALL forwarding, DHT replication, content transfer). See `docs/PUBSUB_RESIGN_LOOP_LESSON.md`.

### Consequences

- A subscriber daemon receives a topic's events **iff** it is connected to the publish-origin station `SO`, or to a direct peer of `SO`. In the partial Leuven mesh (each station ≈ 3 outbound + ≈ 3 inbound peers) and with each daemon independently choosing its own "first connected link" as the `replication_factor=1` publish target, that coverage is non-deterministic. Symptom: mpong games never reliably pair.
- `macula_station_peering_forwarder` (a V1-ported per-`(Topic, PeerLink)` forwarder that fans local publishes over a pg group) is **dead code — zero callers**.
- `hecate_plumtree` exists but is **not wired** into the station (used only in `apps/hecate_overlay/test/phase5_helper.erl`).
- `hecate_pubsub_server`'s own moduledoc admits: *"the publish path … does NOT fan out across the cluster — that requires the Plumtree wire layer … which land in subsequent commits."*

### Interim mitigation already shipped (hecate-daemon)

`hecate_mesh_client` now publishes with `replication_factor => 99` → the PUBLISH fans to **all** connected station links. Since every daemon in the deployment connects to the full station list, every subscriber is then **0 hops** from an origin station. This works for the current topology but: (a) it's O(stations) PUBLISH frames per publish; (b) a subscriber sees each event once per shared station — handlers must be idempotent; (c) it does nothing for a topology where a subscriber's station set ≠ the publisher's. Phase 2 removes the need for it.

---

## 2. The target design

Mirror what **CALL/RESULT already do** (`dispatch(call, …)` verifies against `claimed_caller(Frame)`, not the conn NodeId — see `commit 05e0fbe`, "claimed-signer fix"): the EVENT is **signed once, end-to-end, by the original publisher daemon**, and relay stations forward it **verbatim** (no re-sign). Loop prevention moves from the verify-fail accident to an explicit **`(publisher, seq)` dedup cache** at each station.

### 2.1 EVENT becomes the unit of pubsub (preferred — option A)

- `macula:publish/4,5` builds and signs an **EVENT frame** directly: `{frame_type => event, topic, realm, publisher => self_pubkey, seq => monotonic_per_publisher, payload, published_at_ms}`, signed by the publisher's key. (`seq` is assigned by the *publisher*, monotonic per `publisher`, so `{publisher, seq}` is globally unique. Add a per-pool seq counter to `macula_client` / `macula_pubsub`.)
- The daemon sends the EVENT to its station link(s). `replication_factor` semantics unchanged (default can stay 1 once multi-hop works — or keep the daemon override as belt-and-braces).
- The station's `deliver_pubsub_typed(event, …)` is the single entry point: dedup `{publisher, seq}` → if fresh, `deliver_event` to local subscribers **and** `fan_out_event` to peer stations, verbatim.
- The **PUBLISH frame type is retired** (or kept as a deprecated alias that the station immediately treats as an EVENT). `hecate_pubsub_server:publish/3`, `relay_publish/2`, `build_relay_event/2` are removed; `hecate_pubsub:build_event/3` (publisher-signs) stays but moves caller-side into the SDK.
- **`delivered_via` is removed** from the signed content (it was a per-hop annotation; keep it out, or make it a non-signed wire field).

### 2.2 Less-invasive alternative — `publisher_sig` envelope (option B)

If retiring the PUBLISH frame is too disruptive in one step:

- Keep the daemon→station PUBLISH frame as is (signed by the publisher).
- Add a `publisher_sig` field to the **EVENT frame** = the publisher's signature over the canonical tuple `(topic, realm, publisher, seq, payload)`. The daemon supplies it when publishing (it has the key; this is the same content the PUBLISH frame already signs — but a *separate, frame-type-independent* canonicalisation so it survives PUBLISH→EVENT conversion).
- `build_relay_event` copies `publisher_sig` (and `publisher`, `seq`) into the EVENT verbatim instead of re-signing with the station key. The station may optionally still sign the EVENT envelope with its own key (vestigial / backward-compat), but verification uses `publisher_sig`.
- Verification (stations and subscribers): check `publisher_sig` over the canonical tuple against `publisher`. Valid regardless of which connection it arrived on.

**Recommendation:** option A. Option B is a stepping stone if A's blast radius needs splitting.

### 2.3 The `(publisher, seq)` dedup cache

- One per station (or per `hecate_pubsub_registry`). ETS `set`, key `{publisher_pubkey, seq}`, value `monotonic_ms`.
- Consulted in `peer_observer:deliver_pubsub_typed(event, …)` (and, under option B, in `relay_publish` for inbound PUBLISH) **before** processing/re-fanning. Hit → drop (loop-back or duplicate). Miss → insert, then process.
- TTL sweep every 30–60 s (`ets:select_delete` on entries older than ~2 min) **or** a fixed-capacity ring to bound memory under high publish rates without periodic scans. Prefer the ring — attempt 3's flakiness was attributed in part to `30 s select_delete` churn under combined SUBSCRIBE/UNSUBSCRIBE + bloom load. Size: ~64 k entries ≈ a few MB; sized for the busiest realistic publisher rate over the relay-latency window.
- **No per-frame signing in this path** — the only cost is one ETS lookup + insert per event. Attempt 3 = dedup + per-hop *re-sign*; the re-sign (Ed25519 per frame) was the expensive part. Phase 2 has zero signing on the relay path.

### 2.4 Verify change in `peer_observer`

```erlang
%% before
dispatch(pubsub, Frame, _ConnPid, NodeId, S) ->
    deliver_pubsub(macula_frame:verify(Frame, NodeId), Frame, NodeId, S), S;

%% after — EVENT verified against the claimed publisher (end-to-end);
%% SUBSCRIBE/UNSUBSCRIBE/PUBLISH still verified against the conn NodeId
%% (those are hop-by-hop: a daemon's SUBSCRIBE is signed by the daemon,
%% a router-issued peer SUBSCRIBE is signed by the local station, and
%% in both cases NodeId == signer).
dispatch(pubsub, Frame, ConnPid, NodeId, S) ->
    Signer = case macula_frame:frame_type(Frame) of
                 event -> claimed_publisher(Frame);
                 _     -> NodeId
             end,
    deliver_pubsub(macula_frame:verify(Frame, Signer), Frame, NodeId, S), S.

claimed_publisher(#{publisher := <<_:256>> = P}) -> P;
claimed_publisher(_)                              -> <<0:256>>.
```

(Under option B, `event` verifies via `publisher_sig` against `publisher` — a small dedicated check, since the EVENT envelope's own signature may be the station's.)

### 2.5 `peering_router` / forwarders

The router's job (propagate SUBSCRIBE interest to peer stations, maintain peer-side subscriptions so EVENTs flow back) is **unchanged** and still needed — that's how a station learns which peers want a topic. With multi-hop EVENT propagation working, the `peering_router`'s subscribe-on-peer + `peer_observer.fan_out_event` chain now reaches the *whole* mesh instead of 1 hop.

Decide and document: keep `macula_station_peering_forwarder` (dead pg path) or delete it. Recommendation: **delete** — `fan_out_event` is the live path; the pg forwarder is V1 residue and a second, divergent fan-out path is a maintenance hazard.

### 2.6 Subscriber side (macula SDK)

When a daemon's `macula_pubsub` receives an EVENT, it must verify the publisher signature against the EVENT's `publisher` (option A) / `publisher_sig` (option B), not against the delivering station. Today the subscriber likely trusts the station-signed envelope; Phase 2 makes the trust anchor the publisher, which is the point.

---

## 3. Cross-repo work breakdown

### `macula` SDK (`macula-io/macula`)

1. `macula_frame`: EVENT frame carries `publisher` + `seq` as signed content (option A) **or** add a `publisher_sig` field + a canonical-tuple signer/verifier (option B). Drop `delivered_via` from signed content. `macula_frame:event/1` + `sign/2` produce a publisher-signed EVENT.
2. `macula_client` / `macula_pubsub`: `publish` builds & signs an EVENT (option A) or attaches `publisher_sig` (option B); add a monotonic per-pool/per-publisher `seq` counter. Decide `replication_factor` default (1 is fine once multi-hop works; can keep daemon-side override).
3. `macula_pubsub` subscriber path: verify inbound EVENT against `publisher` / `publisher_sig`.
4. CHANGELOG + version bump (4.4.0 — wire change). Stations and daemons must be on a 4.4.x-compatible pair (coordinate the rollout; see `docs/PUBSUB_RESIGN_LOOP_LESSON.md` for why partial upgrades regressed before).

### `macula-station`

5. `macula_station_peer_observer`: `dispatch(pubsub, …)` verifies EVENT against `claimed_publisher/1`; add `claimed_publisher/1`.
6. New module `macula_station_event_dedup` (ETS ring/set, `seen/2 :: {Publisher,Seq} -> boolean`, sweep or fixed-capacity). Wired into `deliver_pubsub_typed(event, …)` (and `relay_publish` under option B) before processing.
7. `hecate_pubsub_server` / `hecate_pubsub`: option A — remove `publish/3`, `relay_publish/2`, `build_relay_event/2`, `do_relay_publish/2`; the station never re-signs. `deliver_event` / `deliver_pubsub_typed(event, …)` is the one path. Option B — `build_relay_event` copies `publisher_sig`/`publisher`/`seq` instead of re-signing.
8. `peer_observer:deliver_pubsub_typed(publish, …)` — option A: a PUBLISH from a daemon is treated as (or converted to) an EVENT (the daemon should just send EVENTs; PUBLISH becomes a deprecated alias the station upgrades). Option B: unchanged except `relay_publish` copies the sig.
9. Remove `macula_station_peering_forwarder` + `macula_station_forwarder_sup` (dead pg fan-out path) — or explicitly justify keeping them. Update `macula_station_peering_router` (it constructs `forwarder` opts) accordingly.
10. Remove the `signature_invalid` logger filter (`macula_station_log_filters`) once the verify-fail loop-kill is gone — the warning won't fire anymore, and keeping the filter would hide real verify failures.
11. CHANGELOG; bump the `macula` dep to the new 4.4.x.

---

## 4. Implementation order & test harness

The regression detector is **`macula-e2e`'s cross-station probes** (per `docs/PUBSUB_RESIGN_LOOP_LESSON.md`: "macula-e2e cross-station probes are the regression detectors"). Baseline before this work: `12/13` (only the `weather_subscribe` daemon-side stub fails). Every step must keep e2e ≥ baseline.

1. **SDK, behind a feature flag / new frame field, additive.** Add `publisher`+`seq` to the EVENT (option A) or `publisher_sig` (option B) — *populate it, but the station still re-signs and verifies the old way.* No behaviour change. Ship `macula` 4.4.0-rc.
2. **Station: dedup cache, install-only.** Add `macula_station_event_dedup`, wire it into `deliver_pubsub_typed(event, …)` to *count* (log) duplicates but not yet drop them — confirm it sees the loop-backs in production telemetry. e2e unchanged.
3. **Station: switch verify to `claimed_publisher` for EVENT** *and* flip the dedup to actually drop, *and* stop re-signing — atomically (these three are interdependent: stop re-signing → 2-hop verify needs the publisher-signed path → loops need the dedup). Run the full e2e suite under combined SUBSCRIBE/UNSUBSCRIBE + bloom churn (the load profile that broke attempts 3 & 4). Watch per-station CPU asymmetry (ghent ran hot in attempt 3).
4. **Subscriber-side verify** against `publisher` in the SDK; daemon publishes plain EVENTs (drop the `replication_factor => 99` hecate-daemon stopgap once multi-hop is confirmed).
5. **Cleanup:** delete `macula_station_peering_forwarder`/`_sup`; remove the `signature_invalid` logger filter; CHANGELOGs; final `macula` 4.4.0 + `macula-station` `:main`; redeploy fleet via watchtower.

### Why this is *not* a 5th incremental hack

Attempts 1–4 all kept the re-signing and tried to bolt loop prevention on top (dedup, hop caps) — fighting the verify-fail accident instead of removing it. Phase 2 **removes the re-sign** (the EVENT is publisher-signed end-to-end like CALL/RESULT already are), which makes multi-hop verify pass *correctly* and makes the dedup the *primary* (not auxiliary) loop kill. The expensive per-frame Ed25519 sign that flaked attempt 3 is simply gone. It is a wire-format change (4.4.0) done deliberately, with a coordinated daemon+station rollout, not a relay-layer patch.

---

## 5. Open questions

- **Replay window vs memory:** size the dedup ring for the busiest realistic `(publisher, rate)` over the worst-case relay latency. A publisher emitting 100 events/s with a 5 s mesh-propagation tail needs ≥ 500 entries *for that publisher alone*; budget for ~hundreds of publishers. 64 k–256 k entries total.
- **`seq` persistence:** does a publisher's `seq` need to survive a daemon restart? If not, a restarted daemon resets `seq` to 0 → collides with cached `{publisher, seq}` from before the restart → its first events get deduped-as-duplicates and dropped. Mitigations: seed `seq` from `erlang:system_time/0` at boot; or include a per-session nonce in the dedup key; or accept the ≤ TTL-window blackout after restart.
- **Authorization vs authenticity:** the publisher signature proves *authenticity* (this daemon emitted it), not *authorization* (this daemon may publish on this realm/topic). Realm-level publish authz is out of scope here — it belongs with realm membership credentials, not the station relay. Note it; don't conflate.
- **`hecate_plumtree`:** retire it, or is it the intended long-term replacement for the `peering_router` + `fan_out_event` relay? If the latter, Phase 2 should land *into* a plumtree-based relay rather than the current ad-hoc one. Decide before step 3.

---

## Status (2026-05-13)

| Step | What | State |
|---|---|---|
| 1 | `publisher_sig` frame plumbing (`macula_frame:sign_publisher/2`, `verify_publisher/1`; `canonical_unsigned/1` excludes it) | ✅ macula **4.4.0** |
| 1b | `macula_station_link` emits `publisher_sig` on PUBLISH, gated on app env `pubsub_emit_publisher_sig` (default off) | ✅ macula **4.4.1** |
| 2 | `macula_station_event_dedup` `(publisher,seq)` cache, observe-only; wired into `peer_observer:deliver_pubsub_typed(event,…)` | ✅ macula-station |
| 3 | relay (`build_relay_event`) carries `publisher_sig` onto the EVENT; `peer_observer:dispatch(pubsub,…)` verifies EVENTs-with-`publisher_sig` against the publisher; dedup **drops** publisher-signed loop-backs; origin records its `(publisher,seq)` | ✅ macula-station |
| 4 | `macula_station_link` verifies `publisher_sig` on inbound EVENTs (lenient; strict via `pubsub_strict_publisher_sig`); `hecate_app` mirrors `HECATE_PUBSUB_PUBLISHER_SIG`/`HECATE_PUBSUB_STRICT_PUBLISHER_SIG` env vars into those macula app envs | ✅ macula **4.4.2** + hecate-daemon |
| 5 | delete dead `macula_station_peering_forwarder`/`_sup`; gut the forwarder bits from `peering_router` | ✅ (this commit) |

### Cutover

`HECATE_PUBSUB_PUBLISHER_SIG=true` was set on the beam daemon fleet 2026-05-13 (macula-demo `infrastructure/*/hecate-daemon.env.example` + `update-beam-relays.sh`). Verified: beam logs show `[hecate] macula pubsub_emit_publisher_sig enabled`; station `macula_station_event_dedup` live and counting loop-backs; no storm, no crashes.

### macula-e2e (2026-05-13, against the live Leuven fleet, BOOTSTRAP=centrum BOOTSTRAP_OTHER=kessel-lo)

- **cutover ON** (e2e pools also emit `publisher_sig`): **17/28** passed.
- cutover OFF (baseline): 13/28 passed.

→ The cutover is **non-regressive — slightly beneficial** (+4 cross-station probes). The remaining failures (`cross_station_pubsub` → `{no_event_in,75000}`; `cross_station_unary_rpc`/`streaming_rpc` → `unknown_next_peer`; the `*_many_concurrent_*` ones cascading off the e2e "other" pool going `noproc` mid-run) **fail in the baseline too** — they are pre-existing fleet cross-station-routing / e2e-flakiness issues, not Phase 2.

### Still NOT done (deliberately — gated on cross-station pubsub actually being green)

- **Drop the `replication_factor => 99` stopgap in hecate-daemon** (`hecate_mesh_client`). It's still what keeps mpong/pubsub working in the demo — `cross_station_pubsub` e2e is still red, so the stopgap (publish to *all* station links) is load-bearing until that's fixed.
- **Remove the `signature_invalid` logger filter** (`macula_station_log_filters`). Still fires for unsigned EVENTs (`_mesh.bloom` gossip etc.), of which there are plenty.

### Open follow-ups (separate from Phase 2)

1. Why `cross_station_pubsub` doesn't deliver even with publisher-signed EVENTs — is it the e2e "other" pool crashing, or genuine non-delivery between specific stations? (centrum↔kessel-lo are direct peers, so 1-hop should suffice.)
2. The `unknown_next_peer` cross-station RPC routing gap — the beam daemons hit it too on catch-up (`[catch_up.realm_licenses] replay RPC failed: {call_error,1,unknown_next_peer}`). The `peering_router`'s single-hop ADVERTISE propagation isn't covering the partial mesh.
3. `seq` persistence across daemon restart (open question #2 in §5 above) — `macula_station_link`'s `publish_seq` resets to 0 on respawn → collides with cached `(publisher, 0)` in the dedup window.
