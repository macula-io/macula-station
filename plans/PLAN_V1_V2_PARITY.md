# PLAN — V1 ↔ V2 Parity Checklist

**Status:** Draft, awaiting human sanity-check before any fix work proceeds
**Date:** 2026-05-05
**Scope:** Every capability the macula-realm, hecate-daemon, and hecate-stub depended on from V1 macula-relay, with current V2 status, evidence, and a verified-working date when applicable.

## Why this exists

V2 (macula-station + macula 3.x SDK) was a rewrite driven by V1's failure modes (the seven structural bugs in 72 hours). It explicitly enumerated seven design pillars to prevent those failures. **It did not enumerate V1's working features that needed to be preserved.** The result is a system that compiles, deploys, and reaches `/status: healthy` while one of its load-bearing primitives (distributed DHT lookup) returns `{error, no_transport}` immediately — a regression invisible to the test suite by construction. This document is the source of truth for V2 status going forward. Every gap-fix updates one row; no fix can claim parity until the verification column is dated.

## Status definitions (strict)

| Status | Meaning |
|---|---|
| 🟢 **wired-and-tested** | V2 has the capability AND a multi-process integration test (NOT in-VM mock) proves it works end-to-end over the wire. |
| 🟡 **wired-but-untested** | V2 has the capability AND it's plausibly invoked at boot/runtime, but no integration test proves it. |
| 🔴 **scaffolded-but-unwired** | V2 has the API surface but production code never exercises it OR a critical dependency is missing (canonical example: `macula_dht:find_value/4` exists, returns `{error, no_transport}` because production never installs the `send_frame` callback). |
| ⚫ **missing** | No V2 equivalent exists. |
| ⚪ **unknown** | Couldn't determine without further investigation. Treat as untested. |

A row is only 🟢 if there is a CT or eunit test that boots ≥2 separate BEAM nodes, has them communicate over real QUIC (not in-VM message passing), and asserts the capability works. The DHT 50-station in-VM `macula_dht_SUITE.erl` is **not** sufficient — it shares one VM with a synthetic router.

## Summary counts

(Auto-counted from table below; update when rows change.)

| Status | Realm | Daemon | Stub | Total |
|---|---:|---:|---:|---:|
| 🟢 wired-and-tested | 0 | 0 | 6 | 6 |
| 🟡 wired-but-untested | 17 | 8 | 5 | 30 |
| 🔴 scaffolded-but-unwired | 4 | 0 | 1 | 5 |
| ⚫ missing | 0 | 1 | 1 | 2 |
| ⚪ unknown | 0 | 7 | 0 | 7 |
| **Total** | **21** | **16** | **13** | **50** |

The 🟢 count is genuinely tiny. The 🟡 bucket is large because most V2 surface area is invoked at runtime but never integration-tested. Whether 🟡 rows actually work is **unknown** until proven. A working integration-test harness (Phase 1 of the recovery plan) is the prerequisite for converting any 🟡 to 🟢.

## CRITICAL: 🔴 and ⚫ rows (work items)

These are the gaps that must be closed before V2 can claim feature-parity with V1. Each must be addressed with: (1) a failing test, (2) the fix, (3) test passes, (4) CI gate.

| # | Capability | Consumer | V1 evidence | V2 evidence | Status | Notes |
|---|---|---|---|---|---|---|
| C1 | DHT outbound transport (`find_value`, `find_node`, `send_store`) | realm + stub + (daemon, indirectly) | `macula_relay_dht.erl:199-228` registers transport via `persistent_term` and implements `send_to_peer/3` + `send_and_wait/4` | `macula_dht_server.erl:507-660` returns `{error, no_transport}` for every wire-bound op; `macula_station_app:dht_child/1` boots with no `send_frame` opt | 🔴 scaffolded-but-unwired | Root cause of empty topology. PLAN_DHT_TRANSPORT_WIRING.md addresses. |
| C2 | DHT k-bucket entries carry endpoints | implicit (dependency of C1) | V1 routing-table entries had endpoints learned via Kademlia probes | `macula_dht_protocol.erl:152` hardcodes `addresses => []`; `macula_station_peer_observer.erl:180-185` passes `endpoints => []` because peering's `connected` notification carries only `peer_node_id` | 🔴 scaffolded-but-unwired | Steps 8-10 of PLAN_DHT_TRANSPORT_WIRING.md. Needs SDK 3.16.0 for the connected-notification shape change. |
| C3 | Realm topology snapshot via `find_records_by_type` returns full mesh state | realm | `macula_relay_dht.erl` aggregated 100+ records per box (V1 multi-identity model) | `macula_station_link.erl:90` wraps; `macula_dht_server` `list_records/1` returns LOCAL store only (1 record per single-identity container) | 🔴 scaffolded-but-unwired | Local-only by design (Kademlia can't enumerate by type). V1 masked this with high record-count-per-box; V2 single-identity-per-container exposes it. Needs design choice (Phase 5): use `find_value(StationPubkey)` per-station OR rely on K-replication. |
| C4 | Daemon presence in realm topology (effective gap) | realm + daemon | V1 daemon → V1 relay over MessagePack; daemon visible in topology via relay's `find_records_by_type` aggregate | hecate-daemon pinned to `macula ~> 3.7` and uses `_build/.../v1/macula_multi_relay.erl` (V1 facade preserved in SDK). Realm queries `find_records_by_type` (broken per C1/C3) on stations. Daemon publishes `kind=daemon` records to its connected relay's DHT, but daemon's relay is V1-stack while realm queries V2 stations. | 🔴 scaffolded-but-unwired (effective) | Diagnostic of the symptom. The fix is C6 (daemon migration). |
| C5 | Geo-identity from `HECATE_GEO_*` env vars | stub | `macula_connection.erl:606` `build_node_identity/1` parsed env | No V2 equivalent. `macula_identity` is crypto-only. Stub passes geo as a map argument now. | ⚫ missing | Stub-only impact. Workaround: caller constructs the geo map from env. May be intentional removal — confirm with user before reinstating. |
| C6 | hecate-daemon on V2 API (currently V1 facade) | daemon | n/a (V1 ran on V1 stack natively) | Daemon code uses `v1/macula_multi_relay.erl` (legacy facade); macula pin `~> 3.7` (very old). No V2 station_link / peering / DHT calls anywhere in `apps/hecate_mesh/` or `apps/serve_git_over_mesh/`. | ⚫ missing | The actual work item. PLAN_DAEMON_V2_MIGRATION.md (to be drafted) covers: bump macula dep `~> 3.7` → `~> 3.15.3`, replace `macula_multi_relay`/`macula_mesh_client` calls with `macula_station_link` equivalents, exercise via multi-process CT alongside a real V2 station, decommission V1-facade usage. Cross-cuts every D-row in this checklist. |

## 🟡 wired-but-untested (most of V2)

These are plausibly working but unverified. Each needs a multi-process integration test before being upgraded to 🟢. Listed per consumer.

### Realm (17)

| # | Capability | Realm consumer | V2 implementation | Notes |
|---|---|---|---|---|
| R1 | RPC procedure call (inbound, 5 core procedures) | `mesh/rpc_handlers.ex:47-228` | `macula_station_link.erl:186-197` (call/5 + procedures map at L155-159) | Includes check_health, verify_api_key, join_with_token, get_shared_key, get_member_public_keys |
| R2 | RPC procedure call (outbound via gitops) | `gitops.ex:91` (`:macula.call/4`) | V1 facade `:macula.call/4` → `macula_mesh_client:call/4` | Realm uses V1 path here, not V2 station_link; legacy preserved |
| R3 | Live mesh event subscription (4 typed topics) | `topology/mesh_subscriber.ex:338-347, 453-467` | `macula_station_link.erl:91-92` (subscribe/4); EVENT frame dispatch | Subscribes to `_mesh.station.announced_v1` + `.departed_v1` + `.daemon.announced_v1` + `.daemon.departed_v1` |
| R4 | Identity facts polling (type 0x20) | `project_realm_identities/.../identity_listener.ex:87-100` | `RecordSubscriber` polling at 30s; `macula_station_link:find_records_by_type/2` | Realm member pubkey ingestion |
| R5 | License-issued facts polling (type 0x26) | `guide_license_registry/.../issued_batch_listener.ex` | Same path as R4 | |
| R6 | License-revoked facts polling (type 0x27) | `guide_license_registry/.../revoked_listener.ex` | Same | |
| R7 | License-rewrapped facts polling (type 0x28) | `guide_license_registry/.../rewrapped_listener.ex` | Same | |
| R8 | Realm membership resigned facts polling (type 0x21) | `guide_realm_lifecycle/.../membership_resigned_listener.ex:79-94` | Same | |
| R9 | Station connectivity lifecycle (connect/reconnect/monitor) | `mesh_subscriber.ex:158-212, 405-442` | `macula_station_link.erl:51-68`; `macula_peering` notify | Includes Process.monitor + DOWN handler + respawn |
| R10 | Advertiser identity (Ed25519 keypair persistence) | `mesh/rpc_advertiser.ex:50-52, 130-140` | `macula_identity:save/2` | Persisted to MACULA_REALM_ADVERTISER_KEYFILE for stable NodeId across restarts |
| R11 | Procedure discovery over mesh (cross-relay) | `mesh/rpc_handlers.ex` advertises | `macula_station_link.erl:155-159` procedures map; ADVERTISE frames per-connection | Cross-relay discovery depends on station peering gossip — unverified |
| R12 | Admin RPC procedures (revoke_membership, rotate_key, scenario control) | `mesh/admin_rpc_handlers.ex:35-155` | `macula_station_link.erl` advertise path | Auth via MACULA_ADMIN_TOKEN env (plaintext over QUIC for Phase 1) |
| R13 | Mesh subscriber anti-entropy snapshot polling | `mesh_subscriber.ex:220-286` | `macula_station_link.find_records_by_type` async via spawned worker | 8s timeout, zombie detection at L261-286 — depends on C1/C3 |
| R14 | Realm bridge legacy topics (`_mesh.site.up`, `_mesh.weather`, `_mesh.relay.ping`) | `mesh_subscriber.ex:371-363` | Bypasses DHT; legacy peering pub/sub per PLAN_DHT_FIRST.md §3 | |
| R15 | License replay RPC for daemon catch-up | `guide_license_registry/.../replay_events_rpc.ex:21-26` | `macula_station_link` advertise; reads from `:realms_licenses_store` ReckonDB stream | |
| R16 | Mesh publish to typed topics | (no realm consumer; intended-unused) | `macula_station_link.erl:87` (publish/4) | Realm is subscriber-only; marked for completeness |
| R17 | DHT record put/find-by-key | (no realm consumer; intended-unused) | `macula_station_link.erl:88-89` | Realm queries by type, not by key |

### Daemon (8 — but see ⚪ caveat below)

The daemon uses macula `~> 3.7` and the V1 facade (`v1/macula_multi_relay.erl` in the SDK). Most of its capability rows are about V1-facade code paths, not V2 architecture. They may all work fine — the V1 facade is preserved precisely so old consumers don't break — but they don't exercise V2's transport layer. **Whether the V1 facade in macula 3.15.x correctly bridges to V2 stations is itself an open question (see C4).**

| # | Capability | Daemon consumer | V2 implementation | Notes |
|---|---|---|---|---|
| D1 | Pub/Sub publish to relay | `hecate_mesh_client.erl:328` | `_build/.../v1/macula_multi_relay.erl:86-87` | V1 facade |
| D2 | Pub/Sub subscribe + unsubscribe | `hecate_mesh_client.erl:388, 394` | `v1/macula_multi_relay.erl:78-83` | V1 facade |
| D3 | RPC advertise + unadvertise | `hecate_mesh_client.erl:401, 469` | `v1/macula_multi_relay.erl:90-95` | V1 facade |
| D4 | RPC unary call | `hecate_mesh_client.erl:413`; `relay_git_rpc_api.erl:129` | `v1/macula_multi_relay.erl:98-99` | V1 facade |
| D5 | RPC streaming advertise + call | `hecate_mesh_client.erl:428, 459` | `v1/macula_multi_relay.erl:106-113` | V1 facade; placeholder test in `git_over_mesh_SUITE.erl:1-8` |
| D6 | DHT put/find/find_records_by_type | `hecate_mesh_client.erl:351, 362, 374`; `announce_daemon_presence.erl:153` | `v1/macula_multi_relay.erl` → SDK DHT path | Depends on C1 once daemon migrates to V2 |
| D7 | Realm join via RPC (`join_with_token_v1`) | `hecate_mesh_client.erl:552` | `v1/macula_multi_relay.erl` call path | V2 RPC; V1 had HTTP POST |
| D8 | Bootstrap relay seed discovery | `hecate_mesh_client.erl:175-181` | `v1/macula_multi_relay.erl:137`; MACULA_RELAYS env | |

### Stub (5)

| # | Capability | Stub consumer | V2 implementation | Notes |
|---|---|---|---|---|
| S1 | DHT put_record (multi-station replication) | `hecate_stub_daemon.erl:175` | `macula_station_link.erl:253` | API exists; multi-process replication unverified — depends on C1 |
| S2 | Mesh publish to realm topic | `hecate_stub_probe.erl:169`; `hecate_stub_weather.erl:88` | `macula_mesh_client:publish/3` (SDK v1/) | V1 facade |
| S3 | Mesh subscribe with callback | `hecate_stub_probe.erl:144` | `macula_mesh_client:subscribe/3` (SDK v1/) | V1 facade |
| S4 | Mesh RPC call (request-response over QUIC) | `hecate_stub_weather.erl:194` | `macula_mesh_client:call/4,5` (SDK v1/) | Used for weather daemon RPC; no end-to-end test |
| S5 | Canonical topic construction (5-segment realm fact) | `hecate_stub_probe.erl:116` | `macula_topic:realm_fact/4` | Module exists; not in integration tests |

## 🟢 wired-and-tested (the small list of actually-proven)

| # | Capability | Test (file:line) | V2 implementation |
|---|---|---|---|
| T1 | Connect via seed URL with identity keypair (handshake) | `phase1_walking_skeleton_SUITE.erl:67-91` | `macula_station_link.erl:178` |
| T2 | Generate ephemeral Ed25519 keypair | `phase1_walking_skeleton_SUITE.erl:128` | `macula_identity.erl:43` |
| T3 | Extract public key from keypair | `phase1_walking_skeleton_SUITE.erl:62` | `macula_identity.erl:16` |
| T4 | Build + sign node_record | `phase1_walking_skeleton_SUITE.erl:107-118` | `macula_record.erl:20, 48` |
| T5 | Get peer's station NodeId after handshake | `phase1_walking_skeleton_SUITE.erl:86-89` | `macula_station_link.erl:96` |
| T6 | Tombstone record construction + signing | `phase1_walking_skeleton_SUITE.erl:93-105` | `macula_record.erl:30` |

These six are all crypto + handshake-level. Nothing on the distributed, mesh-formation, or topology side is in this column yet. That's the work.

## ⚪ unknown rows

The daemon agent claimed several rows as "wired-and-tested" citing `cqrs_integration_SUITE.erl` and `git_over_mesh_SUITE.erl`. Spot-checking shows those suites are placeholders or test V1-facade paths only. The following daemon rows are flagged ⚪ until a multi-process integration test is verified to exist:

| # | Capability | Claim source | Status flag |
|---|---|---|---|
| U1 | `macula_multi_relay:start_link/1` lifecycle (referenced as `cqrs_integration_SUITE.erl`) | daemon agent | needs verification |
| U2 | Pub/Sub fan-out across multiple connections | daemon agent | needs verification |
| U3 | RPC unary call cross-relay | daemon agent | needs verification |
| U4 | DHT put/find-by-key over real wire | daemon agent | needs verification — likely 🔴 (depends on C1) |
| U5 | DHT find_records_by_type cross-station | daemon agent | needs verification — likely 🔴 (depends on C1) |
| U6 | join_with_token RPC end-to-end | daemon agent | needs verification |
| U7 | Daemon presence record retention | daemon agent | needs verification — depends on DHT TTL behavior in absence of replication |

## Open questions

These are gaps in the audit itself, not in V2. They need answers before the checklist can be considered complete.

1. ~~Does the V1 facade in SDK 3.15.x correctly interoperate with V2 stations on the same wire?~~ **Resolved by C6 — daemon is migrating off the V1 facade entirely.** The interop question becomes moot once the daemon uses `macula_station_link`. The realm's residual V1-facade use (`:macula.call` in gitops.ex:91) becomes a separate small migration tracked under R2.

2. **Is `macula_dht_replicate` running today and what is it doing?** The module exists, the supervisor probably starts it, but every replication call goes through `send_store` which returns `{error, no_transport}`. So the periodic loop is firing into the void. Confirmed via grep but not via runtime trace.

3. **What does the stub's geo-identity-from-env regression actually break in practice?** ⚫ rows are usually scariest, but this one might be intentional. Confirm with user before adding it to the work queue.

4. **Are there capabilities the realm/daemon/stub use that none of the three audit agents found?** The grep was scoped to common patterns (`:macula.*`, `macula_station_link:*`). Less obvious paths (HTTP fallbacks, `Phoenix.PubSub` bridging into the mesh, custom WebSocket handlers) may not be captured. Suggest a follow-up "broader grep" sweep before the checklist is locked.

5. **What did V1 macula-relay's `/topology` HTTP endpoint expose?** The realm now consumes mesh topics + DHT snapshots, but V1's HTTP `/topology` is no longer in the realm's path. Was anything else (browser dashboards, ops scripts, monitoring) consuming `/topology`? Unverified.

## Decisions recorded

### D-001 (2026-05-05) — Pluggable serialization (CBOR + MessagePack) — REJECTED

**Question raised:** Should V2 support pluggable serialization to allow MessagePack alongside CBOR?

**Decision:** No. CBOR is the sole wire format.

**Reasons:**
1. CBOR was deliberately chosen for ecosystem alignment (UCAN, DID, COSE, IPLD all use CBOR; IETF-standardized RFC 8949; deterministic encoding rules required for signed payloads). MessagePack lacks all of this. Re-introducing MessagePack would re-introduce a stack the team explicitly walked away from at macula 3.0.
2. Format negotiation is its own bug surface — handshake state-machine branches, version-skew failure modes, hybrid-format states. The seven-pillars discipline argues for *fewer* states.
3. Maintenance tax for a solo engineer: two encoder/decoder paths, two test matrices, two NIF integrations, two signature-canonicalization stories.
4. Solves the wrong problem. The V1→V2 pain was the *flag-day cutover*, not the format choice. Coexistence at the API layer (V1 facade on CBOR wire) already exists; daemon migrates API, not wire.

**Underlying concern (next major-version transition) — separate response:** add a documented coexistence/deprecation policy to SDK design docs, so a future "Macula 4.0" lands with one of: (a) deprecation window where old readers stay alive, (b) translator-station node type, (c) compatibility shim like `v1/macula_multi_relay`. The lesson is "always have a coexistence path", not "always support N formats."

### D-002 (2026-05-05) — CBOR custom application-level payloads — SUPPORTED TODAY

**Question raised:** Does the CBOR model support custom application-level payloads?

**Answer:** Yes. The `macula_record` envelope handles identity + signing + TTL; the `payload` field is a CBOR map the application owns. Already proven: the kortrijk station's record carries application fields like `geo`, `caps_hint`, `cpu_cores`, `ram_mb`, `realms`, `hostname` — all in the payload map, all signed by the envelope.

**How to add a new fact type:**
1. Pick an unused `type` tag (currently used: 0x01 node_record, 0x0C tombstone, 0x20 realm_member_identity_v1, 0x21 realm_member_resigned_v1, 0x26-0x28 license events).
2. Define the payload schema (CBOR keys + value types).
3. Publish via `put_record` with that type.
4. Subscribers filter by `type` tag.

**Constraints application code must respect:**
1. **Deterministic encoding (RFC 8949 §4.2.1).** Signatures cover the canonical encoding; non-canonical maps (wrong key order, non-shortest int form) won't verify. The SDK's `macula_record_cbor` enforces this on encode but applications constructing payloads via `cbor_term:encode` directly must use the deterministic profile.
2. **Type tag namespace.** No central registry today — types are coordinated informally. **Follow-up:** add `apps/macula_record/TYPE_TAGS.md` (or equivalent) so two teams don't both pick `0x42` for different facts. Small, low-risk.

**Quirk worth flagging:** CBOR encoder preserves Erlang term distinctions, so payloads can mix atom keys (`:hostname`) and tagged-binary keys (`{:text, "ram_mb"}`). Realm code at `mesh_subscriber.ex:557-613` handles both via `Map.get(payload, :hostname) || Map.get(payload, "hostname")`. **Follow-up:** consider a small SDK-level helper to normalize payloads to all-binary-keys before handing to consumers.

## How to maintain this document

- Every PR that closes a 🔴/⚫ row updates the row to 🟡 (wired) or 🟢 (wired-and-tested).
- A row only moves to 🟢 if a multi-process integration test exists, is gated in CI, and is named in the row's verification column.
- New ⚪ rows discovered during work are added immediately; never silently dropped.
- The summary count table at the top is updated when status counts change.

## Verification log

| Date | Change | By |
|---|---|---|
| 2026-05-05 | Initial draft assembled from three parallel audits + spot-checks of `macula_multi_relay` location and daemon SDK pin (~> 3.7). | Claude |
| 2026-05-05 | Added C6 (daemon → V2 migration) as work item; reframed C4 as diagnostic-of-symptom that C6 addresses. Added D-001 (pluggable serialization rejected) and D-002 (CBOR custom payloads supported). Marked Q1 resolved. | Claude (per user direction) |
