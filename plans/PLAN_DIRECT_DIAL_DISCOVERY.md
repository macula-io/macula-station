# PLAN — Direct-dial discovery (lean, slice-by-slice)

**Status:** Planning — foundation slices ready; trust + cutover gated on decisions.
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
| 1 | Un-narrow the DHT read | — | no |
| 2 | Activate `procedure_advertisement` | 1 | no |
| 3 | Activate `station_endpoint` | — | no |
| 4 | Pool dynamic dial | — | no |
| 5 | Wire the data plane end-to-end | 1–4 | no |
| 6 | Managed-realm registry | 5 | **Q6** (repo home) |
| 7 | Dual-trust enforcement | 5 | **Q7–Q9** (posture, root, code) |
| 8 | Retire the gossip routing | 5, 7 | **Q5** (migration cadence) |

Slices 1–4 have no dependency on each other except where noted and can proceed in
parallel; 5 joins them.

---

## Shared foundation (ungated)

### Slice 1 — Un-narrow the DHT read

**For:** so a resolve returns EVERY provider of a procedure, not just one.
The store is already multi-value (ETS `bag`, signer-deduped); only the read
narrows (§8.1 of the design).

- **Build:** add `_dht.find_records` returning the full `store_lookup` list;
  expose `macula:find_records/2 -> {ok, [record()]}`. Leave `find_record/2`
  untouched (never delete features).
- **Files:** `macula_station_dht_handlers.erl` (new handler, stop taking the
  list head), `macula_handler_registry` (advertise the proc), `macula/src/macula.erl`
  (facade `find_records/2` + list classify).
- **DONE-WHEN:** two providers advertise one procedure; a single `find_records`
  returns both records. Unit test RED-verified before the fix.

### Slice 2 — Activate `procedure_advertisement`

**For:** so a provider's capability location lives in the DHT as a signed record,
and a consumer can resolve it cold.

- **Build:** on advertise, the provider also builds + signs a
  `procedure_advertisement` (advertiser_node, procedure_uri, serving_station) and
  `put_record`s it. A resolve helper maps `procedure_uri -> [{advertiser, station}]`
  via Slice 1.
- **Files:** `hecate-om` advertise path (also put_record), a small resolve helper
  in `macula` (`macula_record:procedure_advertisement/3` already exists).
- **DONE-WHEN:** a fresh consumer, given only a `procedure_uri`, finds a provider
  it was never handed.

### Slice 3 — Activate `station_endpoint`

**For:** so a station pubkey resolves to a dialable host:port. Direct-dial cannot
proceed without it; the record type exists (`0x12`) but is dormant (§8.2).

- **Build:** stations publish a signed `station_endpoint(pubkey, quic_port)` on
  boot + periodic refresh; a resolver maps station pubkey -> endpoint.
- **Files:** `macula_station_announcer.erl` (publish), `macula` resolve helper
  (`macula_record:station_endpoint/2` exists).
- **DONE-WHEN:** given only a 32-byte station pubkey, the consumer obtains a
  host:port it can dial.

### Slice 4 — Pool dynamic dial

**For:** so a consumer can open a link to a station OUTSIDE its seed set and call
through it. Today the pool is fixed to its seeds (§8.2, Q2).

- **Build:** expose a pool API to ensure a link to an ad-hoc endpoint (built on the
  existing `start_link_for_seed/2` seam), reuse one link per station, cap fan-out.
- **Files:** `macula/src/client/macula_client.erl` (ensure_link / call-at API),
  `macula/src/macula.erl` (facade).
- **DONE-WHEN:** a one-hop call to a station the consumer never seeded to returns
  a result; a second call to the same station reuses the link.

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

## Gated (deferred behind decisions)

### Slice 6 — Managed-realm registry — GATED on Q6

**For:** so a managed realm's realm service is the authority for the station set.
The service already holds the CA, the station directory (`hostname_for/1`), and
per-station dialing (`client_for_pubkey/1`) — §6.1/§6.2.

- **Build:** realm service publishes + serves the `realm_stations` set it curates;
  consumers in that realm resolve from it instead of the DHT.
- **Blocked by:** Q6 — does realm-service code stay in `macula-realm` or migrate
  to the empty `hecate-social/hecate-realm`? Do not wire into a repo about to move.

### Slice 7 — Dual-trust enforcement — GATED on Q7–Q9

**For:** so provider→consumer and consumer→provider trust are both checked, on the
already-mutual QUIC session, without a live authority (§6.3). Primitive
(`macula_ucan_nif:verify/2`) and wire slot (`ucan_token`) exist; enforcement does not.

- **Build:** provider verifies the CALL's `ucan_token` where its policy requires;
  consumer verifies the advertisement chains to a trusted namespace root or a pinned
  pubkey; add a BOLT#4 `unauthorized` code.
- **Blocked by:** Q7 (open- vs gated-by-default), Q8 (namespace root of trust — also
  answers how shops are told apart, §6.4), Q9 (the `unauthorized` code).

### Slice 8 — Retire the gossip routing — GATED on Q5

**For:** so the fragile distance-vector advertise propagation
(`macula_station_peering_router.erl`) is removed once direct-dial is proven.

- **Build:** delete the advertise-gossip RPC routing substrate and its
  tombstone/reconcile machinery; source-route primitives may stay for a
  privacy-only path (Part 3 §6.7), not as the default.
- **Blocked by:** Q5 — hard switch, or per-realm coexistence during cutover?
  Retire only after Slice 7, so nothing loses authorization when the old path goes.

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

## Open questions (mirror of design §11, the ones that gate slices)

- **Q5** migration cadence → gates Slice 8.
- **Q6** realm-service repo home → gates Slice 6.
- **Q7** default trust posture → gates Slice 7.
- **Q8** namespace root of trust (also = how shops are told apart) → gates Slice 7.
- **Q9** BOLT#4 `unauthorized` code → gates Slice 7.

Slices 1–5 need none of these. Start there.
