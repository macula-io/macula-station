# PLAN — Direct-dial discovery (lean, slice-by-slice)

**Status:** Planning — all gating decisions made 2026-08-19 (Q5–Q9); every slice
is now ungated. Foundation slices 1–5 ready to build; 6–8 defined.
**Created:** 2026-08-18
**Design:** `DESIGN_DIRECT_DIAL_DISCOVERY.md` (this plan builds it).
**One line:** so a consumer reaches a capability in ONE hop by dialing a station
that already serves it, instead of relaying a CALL across the mesh.

---

## How to read this plan

Every slice is a **BUILD**, not a CLAIM (it wires infrastructure; it asserts
nothing about the world), so none needs an adversarial science gate. Each slice
is small, independently shippable, and carries a **DONE-WHEN** inspection — the
one observation that proves it works. Keep them in order: 1–5 are the shared
foundation and need NO open question answered; 6–8 are gated on decisions and are
deliberately deferred (per the repo's no-rabbit-holes rule).

Before starting any slice, one-line checkpoint to Raf: which slice, its DONE-WHEN,
rough size. Cheap to veto.

---

## Phase map

| # | Slice | Depends on | Gated? |
|---|-------|-----------|--------|
| 1 | Un-narrow the DHT read | — | **DONE 2026-08-19** |
| 2 | Activate `procedure_advertisement` | 1 | **DONE 2026-08-19 (e2e verified)** |
| 3 | Activate `station_endpoint` | — | **DONE 2026-08-19 (e2e verified)** |
| 4 | Pool dynamic dial | — | **DONE 2026-08-19 (e2e green on hex 8.4.0)** |
| 5 | Wire the data plane end-to-end | 1–4 | no |
| 6 | Managed-realm registry | 5 | decided: `macula-realm` (Q6) |
| 7 | Dual-trust enforcement | 5 | decided: open-default + org-root + `unauthorized` (Q7–Q9) |
| 8 | Retire the gossip routing | 5, 7 | decided: flip realm-by-realm then delete (Q5) |

Slices 1–4 have no dependency on each other except where noted and can proceed in
parallel; 5 joins them. All gating decisions were made 2026-08-19; 6–8 are no
longer blocked, only ordered after the foundation.

---

## Shared foundation (ungated)

### Slice 1 — Un-narrow the DHT read — DONE 2026-08-19

**For:** so a resolve returns EVERY provider of a procedure, not just one.
The store is already multi-value (ETS `bag`, signer-deduped); only the read
narrows (§8.1 of the design).

- **Built:** added `_dht.find_records` returning the full `find_local_record`
  list (local hit, else one-hop remote returning the whole value list, miss =
  `{ok, []}`); exposed `macula:find_records/2 -> {ok, [record()]}`. `find_record/2`
  left untouched.
- **Files:** `macula_station_dht_handlers.erl` (new `_dht.find_records` handler +
  advertise + `peer_ids/2` extraction), `macula-station .../test/macula_station_dht_handlers_tests.erl`
  (multi-advertiser + empty-key tests), `macula/src/macula.erl`
  (facade `find_records/2` + `classify_find_list`).
- **DONE-WHEN met (station half):** `find_records_returns_all_advertisers_for_one_procedure`
  — two providers advertise one procedure_uri (different signers, same station), a
  single `find_records` returns both; `find_record` still returns one. RED-verified
  (3 failures with the handler stashed), then GREEN 8/0.
- **SDK half RELEASED in macula 8.1.0** (2026-08-19; commit `fcb57aa`, tag
  `v8.1.0`). Verified resolvable: the resolver registry `repo.hex.pm/packages/macula`
  lists 8.1.0 and the tarball is live (not a repeat of the 8.0.1 list-but-not-
  resolvable saga). `macula:find_records/2` is now on hex.
- **Consumer lock bump deferred (deliberate).** `macula-station` and `hecate-om`
  pin `{macula, "~> 8.0"}`, so 8.1.0 is eligible, but their locks still sit at
  8.0.0. No current code CALLS `find_records/2` yet (the station handler is native;
  the first caller is hecate-om in Slice 2 / the wiring in Slice 5). So the
  `rebar3 upgrade macula` lands as ONE deliberate bump when Slice 2 consumes it,
  not a premature no-op now.
- **SDK release cadence:** Slices 3 and 4 also touch the macula facade. They can
  ride the next macula release; no need for one release per slice.
- **Not run:** dialyzer (WIP-slice; specs are straightforward thin wrappers).

### Slice 2 — Activate `procedure_advertisement` — DONE 2026-08-19

**Shipped + PUBLISHED:** macula **8.2.0** (`read_procedure_advertisement/1`,
`procedure_key/1`) and hecate-om **0.11.0** (keypair retention in
`hecate_om_identity` + `keypair/0`; `hecate_om_capabilities` rewritten from pubsub
to DHT records: writes a signed `procedure_advertisement` per capability on
register + a 30s republish tick, resolves via `find_records/2` +
`read_procedure_advertisement` with per-record signature verification; pubsub
`_mesh.cap.announce` + `peers/0` deleted; macula floor tightened to `~> 8.2`).
Both on hex and resolver-registry-verified. Tests: 9/0 eunit (pure helpers +
gen_server graceful-degradation, RED-verified), elvis + dialyzer clean; cover 53%
capabilities (remainder = live-pool glue, proven by the cross-station SUITE).

**Consumers still owed:** the services that use hecate-om (hecate-rag, …) get DHT
discovery only after moving to `hecate_om ~> 0.11` and redeploying — the
coordinated cutover, not a live migration.

**E2E VERIFIED (gap closed):** `macula_station_procedure_advertisement_SUITE`
(CT, real 2-station QUIC cluster via `macula_station_test_cluster`) proves the
DONE-WHEN over the wire: a `procedure_advertisement` put on the provider station
is resolved from the consumer station and decodes to the right provider
(`advertisement_resolves_cross_station`), and two providers of one procedure both
come back (`two_providers_both_resolve_cross_station`). This is the only test that
exercises `read_procedure_advertisement/1` against a real CBOR round-trip, not a
hand-built payload. Both pass. (Scope: the suite drives the DHT wire + reader
directly via `find_value`; the `_dht.find_records` handler wrapper and the SDK
facade are unit-tested in Slice 1 + macula.) Earlier concern that only hecate-om
lacked a harness was right about hecate-om but wrong to conclude e2e was
un-runnable — the station repo has the cluster harness.

Original notes retained below.



**For:** so a provider's capability location lives in the DHT as a signed record,
and a consumer can resolve it cold. WIRED into the real capability lifecycle (not a
side helper) and REPLACING the pubsub `_mesh.cap.announce` discovery — greenfield,
no coexistence (see [[project_direct_dial_discovery_track]]).

**Scope correction (2026-08-19):** hecate-om has NO RPC advertise hook; capabilities
are `#{name, version}` maps announced over pubsub, the service keypair is not
retained, and cap names are not namespaced URIs. So this slice: retains the keypair,
writes a signed `procedure_advertisement` per capability on register, and resolves
via the DHT on lookup.

- **macula 8.2.0 (DONE, pending publish):** `macula_record:read_procedure_advertisement/1`
  (record → typed fields, robust to canonical + wire key shapes) and
  `macula_record:procedure_key/1` (uri → storage key). Commit `0228ad2`, tag
  `v8.2.0`. eunit 70/0 (RED-checked), dialyzer + ex_doc clean.
- **hecate-om (NEXT, needs 8.2.0 published + dep bump):** retain keypair in
  `hecate_om_identity` (+ `keypair/0`); in `hecate_om_capabilities` register, write
  one signed `procedure_advertisement` per capability (advertiser = service key,
  serving_station = a connected station via `macula:links/1`, procedure_uri =
  realm-namespaced cap name); make lookup resolve via `macula:find_records/2` +
  `read_procedure_advertisement/1`; delete the pubsub-announce discovery.
- **DONE-WHEN:** a service boots, and a separate consumer given only a capability
  resolves that provider from the DHT with no pubsub and no prior hand-off.
- **Constraint:** signing needs a stable `identity_key_path`; ephemeral services
  are (correctly) invisible to DHT discovery — degrade to no-op + log.
- **Deferred as genuine sequencing (not caution):** org segment of the URI rides
  with Slice 7 trust (realm-scoped for now); multi-station-per-provider is Q10 /
  Slice 5 (one serving station now, store dedups by signer).

### Slice 3 — Activate `station_endpoint` — DONE 2026-08-19

**For:** so a station pubkey resolves to a dialable host:port. Direct-dial cannot
proceed without it; the record type exists (`0x12`) but was dormant (§8.2).

- **Built:** the station announcer publishes a signed
  `station_endpoint(own_pubkey, quic_port, #{host_advertised => [host]})` on boot
  and every refresh cycle, alongside its `node_record`. Port + host threaded from
  `station_cfg` via `announcer_child`. No SDK change/release — the constructor is
  already in macula 8.2.0.
- **Files:** `macula_station_announcer.erl` (`publish_station_endpoint`, state
  `host`/`port`), `macula_station_app.erl` (`announcer_child` passes `port`),
  `macula_station_endpoint_SUITE.erl` (e2e).
- **DONE-WHEN met (e2e):** `macula_station_endpoint_SUITE` (real 2-station QUIC
  cluster) — B publishes its endpoint on boot, A resolves B's pubkey to B's real
  listen port + a non-empty advertised host. RED-verified (`{nodes, ...}` badmatch
  without the publish); GREEN 1/1. Elvis clean.
- **Deferred to Slice 4 (genuine sequencing):** the SDK reader helpers
  (`station_endpoint_key/1`, `read_station_endpoint/1`) so the consumer resolves
  without parsing payload internals — batched into the Slice 4 macula release.

### Slice 4 — Pool dynamic dial — SDK DONE (macula 8.3.0), e2e post-publish

**For:** so a consumer can open a link to a station OUTSIDE its seed set and call
through it. The pool was fixed to its seeds (§8.2, Q2).

- **Built (macula 8.3.0, pending publish):** `macula:call_station/6` — ensure
  (reuse or dial+monitor) a link to a specific station URL on the existing
  `start_link_for_seed` seam, await the handshake within the deadline, call there;
  `{error, not_connected}` on timeout. Plus `station_endpoint_key/1` +
  `read_station_endpoint/1` (Slice 3's consumer readers, batched here).
- **Files:** `macula_client.erl` (`call_station/6` + `ensure_link` +
  `call_when_connected`), `macula.erl` (facade), `macula_record.erl` (readers).
- **Tested:** unreachable-dial degradation unit-tested (`call_station` → clean
  `{error, not_connected}`, pool survives; RED-verified); record readers 72/0
  (RED-verified); dialyzer + ex_doc clean.
- **Also needed (surfaced by the e2e): TLS-policy forwarding, macula 8.4.0.** The
  SDK pool always verified `webpki`, so it could not dial the harness's self-signed
  stations. `macula:connect/2` now forwards `verify` / `expected_node_id` to its
  links (8.4.0). A real direct-dial gap, not test-only: a consumer dialing a
  resolved serving_station must control TLS (pin the station's Ed25519 identity via
  `expected_node_id`, or `verify => none` on loopback).
- **DONE-WHEN met (e2e verified):** `macula_station_call_station_SUITE` — a pool
  seeded to NOTHING dials station B by URL (outside the seed set) with
  `verify => none`, a CALL to `_dht.find_record` returns `{ok, _}`, and a second
  call reuses the link. Proven LOCALLY via `_checkouts` (macula-station → local
  macula) BEFORE releasing 8.4.0; passes in CI once 8.4.0 is published (station
  resolves `~> 8.x`).
- **Deferred (hardening, not caution):** a fan-out cap / LRU eviction of
  dynamically-dialed links (reuse-if-present is in); and per-call `expected_node_id`
  pinning on `call_station` for production (pool-level `verify` is enough for the
  e2e). Both are refinements on a working path.

### Slice 5 — Wire the data plane end-to-end

**For:** so `(realm, procedure)` becomes resolve → dial → call in one path, with
failover, and no multi-hop relay.

- **Build:** a consumer-side direct-call path — resolve the advertisement set
  (Slice 2), resolve endpoints (Slice 3), dial one station (Slice 4), call; on
  error, fail over to another station in the set.
- **Files:** a small `macula` direct-call module (or a `hecate-om` consumer helper).
- **DONE-WHEN:** an end-to-end direct-dial call succeeds with the gossip path
  disabled for that procedure; killing the chosen station triggers failover to
  another advertised station and the call still returns.

---

## Later slices (decided; ordered after the foundation)

### Slice 6 — Managed-realm registry — targets `macula-realm` (Q6)

**For:** so a managed realm's realm service is the authority for the station set.
The service already holds the CA, the station directory (`hostname_for/1`), and
per-station dialing (`client_for_pubkey/1`) — §6.1/§6.2.

- **Build:** in `macula-realm` (Q6 decided), the realm service publishes + serves
  the `realm_stations` set it curates; consumers in that realm resolve from it
  instead of the DHT.
- **DONE-WHEN:** a consumer in a managed realm resolves a capability's stations
  from the realm service (no DHT walk) and dials one directly.
- **Note:** the `macula-realm` directory machinery (`Topology.Directory`,
  `StationLinks`) is demo-grade today; this slice promotes it to a supported
  discovery path, it does not build it from scratch.

### Slice 7 — Dual-trust enforcement — open-default + org-root + `unauthorized`

**For:** so provider→consumer and consumer→provider trust are both checked, on the
already-mutual QUIC session, without a live authority (§6.3). Primitive
(`macula_ucan_nif:verify/2`) and wire slot (`ucan_token`) exist; enforcement does not.

- **Build (Q7 open-by-default):** a bare advertisement serves any identified
  caller; a provider opts into "UCAN required" per procedure.
- **Build (Q8 org-key root):** a consumer verifies the advertisement chains to the
  org key owning the `<org>` segment of the `procedure_uri` (or a pinned pubkey);
  a gated provider verifies the CALL's `ucan_token` against the chain it recognizes.
- **Build (Q9):** add an `unauthorized` code to the BOLT#4 taxonomy.
- **DONE-WHEN:** an open provider serves any identified caller; a gated provider
  serves a caller with a valid org-rooted UCAN and refuses one without it with
  `unauthorized` (not a timeout); a squatter advertisement fails the consumer's
  chain check.

### Slice 8 — Retire the gossip routing — flip realm-by-realm, then delete (Q5)

**For:** so the fragile distance-vector advertise propagation
(`macula_station_peering_router.erl`) is removed once direct-dial is proven.

- **Build:** move realms to direct-dial one at a time, watching each; once every
  realm is across, delete the advertise-gossip RPC routing substrate and its
  tombstone/reconcile machinery. Source-route primitives may stay for a
  privacy-only path (Part 3 §6.7), not as the default.
- **DONE-WHEN:** every realm resolves + dials directly, the gossip advertise path
  is deleted, and the suite is green with it gone (RED-verified: the old path is
  actually removed, not dormant).
- **Order:** only after Slice 7, so nothing loses authorization when the old path
  goes.

---

## Non-negotiables carried from the design

- **Pubsub is out of scope.** Direct-dial is RPC / point-to-point only; pubsub
  stays find-stations-then-Plumtree (design §7).
- **Never delete a feature to add one.** `find_record/2` stays; `find_records/2` is
  additive. The gossip path is removed only in Slice 8, only after direct-dial works.
- **Freshness is not optional.** Every activated record is a live record (TTL +
  republish) and every consumer re-resolves on a dial failure — a listed station
  can be dead (design §8.3-equivalent, the HEALTHY-forever hazard).

---

## Gating decisions — ALL MADE 2026-08-19

- **Q5** cutover → **flip realm-by-realm, then delete** (Slice 8).
- **Q6** realm-service repo home → **stay in `macula-realm`** (Slice 6).
- **Q7** default trust posture → **open by default**, gating opt-in (Slice 7).
- **Q8** namespace root → **org (shop) key**; realm → org → server chain; also
  answers how shops are told apart (Slice 7, §6.4).
- **Q9** BOLT#4 `unauthorized` code → **yes, add it** (Slice 7).

Nothing is blocked. Slices 1–5 are the ungated foundation; 6–8 are decided and
ordered after it. Non-gating design questions Q3 (managed resolution shape) and Q4
(multi-homing degree K) remain open but do not block any slice.

Start at Slice 1.
