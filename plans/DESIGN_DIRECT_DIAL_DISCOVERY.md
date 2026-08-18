# Direct-dial discovery: resolve via records, connect to the serving station

**Status:** Proposal / Draft — for decision, not yet building.
**Created:** 2026-08-18
**One line:** so a consumer reaches a capability in ONE hop by dialing a station
that already serves it, instead of relaying a CALL across the mesh.

---

## 0. The end goal this serves

A consumer of a capability (RPC today, other point-to-point capabilities later)
should learn *which stations serve it*, then open a direct connection to one of
them and call it there. No multi-hop data path. The mesh's job shrinks to
answering "where is X" cheaply and correctly; getting the bytes to X stops being
the mesh's problem.

Everything below is measured against that one line. A mechanism that does not
help a consumer reach a capability in one hop does not belong in this design.

---

## 1. The question

Macula routes cross-station RPC today by relaying a CALL through intermediate
stations, and that relay layer has been a recurring source of hard bugs (see
`DESIGN_ADVERTISE_PROPAGATION_RECONCILE.md`: permanent wedge on the one
no-direct-edge station pair, healed only to `<=30s` by a reconcile pass).

The thesis: stop relaying. Have a provider advertise the stations where it is
reachable, have a consumer resolve that set and dial one of those stations
directly. This doc records what the three repos support today, what Macula V2
Part 3 already intended, and exactly where the thesis reclaims the plan versus
where it departs from it.

Scope: `macula-io/macula-station`, `macula-io/macula` (SDK + records),
`hecate-services/hecate-om` (the service runtime that advertises and consumes).

---

## 2. What actually ships and routes RPC today

Three discovery-ish mechanisms exist. Only one carries load, and it is the
fragile one.

### 2.1 The live path: distance-vector advertise gossip

- A service advertises a procedure via a wire ADVERTISE frame. The receiving
  station records it in `macula_remote_advertise_registry`
  (`apps/macula_handler/src/macula_remote_advertise_registry.erl`), which holds,
  per station, `(realm, procedure) -> single advertiser entry`, with an explicit
  **single-provider-per-station invariant** (module docs, lines 18-24).
- Cross-station reach comes from `macula_station_peering_router.erl`, which
  propagates those entries station-to-station as **distance-vector gossip**: each
  hop rewrites `advertiser => SelfId` and routes one hop back toward the origin
  (lines 289-311, `send_advertise_diff`). A CALL is then resolved hop by hop; a
  missing route yields `unknown_next_peer` (`macula_handler_dispatch.erl`).
- This is the "complex compute and algorithms to support multi-hop" the thesis
  targets. It is diff-driven and only recently gained a 30s reconcile pass, which
  bounds the wedge without preventing it.

**This mechanism appears nowhere in the Part 3 design.** It is a pragmatic
stopgap that made cross-station calls work and then became the whole routing
layer.

### 2.2 The app-level capability layer (hecate-om)

- `hecate_om_capabilities.erl` publishes each service's capability summary on the
  pubsub topic `_mesh.cap.announce`, subscribes to the same topic, builds a
  `service_name -> summary` map, and `lookup/1` returns the services advertising a
  capability. The comment at line 15 already frames it as "caller uses that list
  to pick a target for `macula:call/5`."
- But the summary payload (`summary_payload/2`, lines 144-150) is
  `type, service, capabilities, published_at`. **No origin pubkey, no station
  list, no endpoints.** It answers "which service" and never "where," so the
  consumer still falls back to the multi-hop call path. It disseminates over
  pubsub (itself the multi-hop mesh) and goes stale after two minutes.

This layer is the closest thing to the thesis that exists, and it is exactly one
field short: it never carries location.

### 2.3 The signed DHT record layer (present, dormant)

`macula/src/record/macula_record.erl` defines typed, signed, PKARR-style DHT
records. Two are precisely the thesis primitives:

- `procedure_advertisement(AdvertiserNode, ProcedureUri, ServingStation)`
  (lines 385-399), keyed at `SHA-256(procedure_uri)`, signed by the advertiser.
  Carries a **single** serving station per record.
- `realm_stations(RealmId, [#{station_id, roles}])` (lines 328-340), keyed at
  `SHA-256("station_set" || RealmId)`, signed by the realm admin. A **set** of
  stations, but at realm granularity, not per capability.

A grep across station + hecate-om finds these two types referenced only in
`macula_record.erl` and tests. **Nothing in the live routing path writes or reads
them.** The hard, correctness-critical part (canonical CBOR, signing domains,
storage-key derivation) is done and tested; the wiring is absent.

---

## 3. What Part 3 actually intended

`PLAN_MACULA_V2_PART3_DISCOVERY.md` draws one seam and the thesis falls on both
sides of it. §5.3: *"discovery is separate from routing. V1 conflated these; V2
separates."*

### 3.1 Discovery — the thesis IS the plan

- §3.3 key families include `ProcedureKey = SHA-256(procedure_uri)` and
  `RealmStationsKey = SHA-256("station_set" || RealmId)`.
- §5.3: *"LOOKUP(SHA-256(procedure_uri)) -> returns 1..20 advertisement records ->
  caller picks preferred advertiser."*
- §9 (cross-realm CALL): *"DHT returns up to 20 procedure_advertisement records.
  Station A filters by realm, tier, latency. Station A picks target T."*
- §10.3: `FIND_VALUE` returns a `record_list`, i.e. the multi-value get the thesis
  needs is the intended shape, not an extension.

So the resolve-then-choose-a-provider half of the thesis is Part 3 as written.
The dormant records (§2.3) are the assets it specified. On discovery, the thesis
**reclaims an intention** that the shipped gossip layer (§2.1) quietly bypassed.

### 3.2 Routing — the thesis DELETES the plan's largest chapter

Part 3's data plane is **source routing** (§6): Suurballe k=3 disjoint paths, a
44-byte source-route header, tier-increasing-then-decreasing path validity
(§6.5), per-hop verification, onion-lite privacy layers for a later phase. It is
multi-hop by design.

That chapter exists to relay a CALL across a mesh where the origin **cannot**
reach the target directly. §6.5 makes the assumption explicit: cross-country
T0-only paths are rejected "because they would consume bandwidth of
residential-grade stations." The design assumes many stations are residential /
edge, not publicly dialable, not transit-capable.

The thesis throws source routing away and dials the serving station directly.

### 3.3 Source routing is also dormant

Like the records, the source-route layer is present-but-unwired.
`apps/macula_routing/src/macula_relay.erl` implements the per-hop relay logic of
§6.6 as a pure module, and its own header says: *"the wrapper that actually
transmits the returned frame lands in Session 4.7+ when the orchestrator gains
its full I/O surface."* That session never came. `macula_source_route.erl` exists
in the SDK as a frame primitive.

So **neither half of Part 3 is the live path.** Discovery records are dormant;
source routing is a pure module waiting for an I/O wrapper. The gossip stopgap
(§2.1) carries everything while both designed layers sit on the shelf.

---

## 4. What licenses the deletion: universal reachability

The thesis rests on one topology constraint, stated by Raf:

> Stations are always intended to be publicly reachable.

Source routing is the answer to "the target is unreachable directly." Make every
station publicly dialable and the reachability reason for source routing
evaporates. This is coherent, but it is a **topology decision**, not only a
routing one: it sets aside the tiered topology of Part 2 for the data path. Tiers
may survive for capacity, replica diversity, and trust weighting, but
"you must be relayed through a gateway tier to be reached" is exactly the premise
being removed. The doc records that consequence openly so it is a choice, not a
side effect.

---

## 5. The thesis, stated precisely

1. **Providers multi-home.** A service connects to K stations (K small and
   deliberate, e.g. 3-8) and advertises exactly those K. A provider is therefore
   never more than one hop from an advertised station, so there is nothing left
   to route. K is the redundancy-and-load knob, bounded, never a function of
   fleet size N.
2. **Advertisement carries origin + serving stations.** Extend the record /
   summary so it names the provider pubkey and the set of stations where the
   capability is reachable.
3. **Consumers resolve, then dial one station directly.** The consumer's own
   station resolves the set (the consumer stays a thin client and never
   multi-hops); the consumer opens a direct connection to one station in the set
   and calls there. Failover picks another station in the set.

At hundreds of thousands of stations, resolution MUST stay O(log N): no station
holds full membership (Kademlia buckets are O(log N)), so a gossiped full catalog
is impossible. Resolution is a lookup, not a broadcast.

---

## 6. Discovery substrate, chosen per realm flavor

Realms come in two flavors (public, and managed by a realm service). The data
plane (direct-dial) is orthogonal to how discovery resolves, so the substrate can
differ:

| Realm flavor | Resolution substrate | Record | Signer |
|---|---|---|---|
| **Public** | S/Kademlia DHT lookup, O(log N) | `procedure_advertisement` at `SHA-256(procedure_uri)`, returning a record list | advertiser |
| **Managed** | Ask the realm service (it issues the certs, so it already knows every principal and its stations) | `realm_stations` at `SHA-256("station_set" || RealmId)` | realm admin / service |

Managed realms are the cheaper starting point: `realm_stations` is already a
set, already keyed, already admin-signed, and needs the least new design. The
realm service becomes a discovery authority (replicate it so it is not a single
point of failure), consistent with the authority it already holds for identity.

---

## 7. Pubsub does NOT fold into direct-dial

For RPC the advertised set is K, small, "dial one" works. For a popular topic the
set of stations with a publisher or subscriber can be tens of thousands: it does
not fit a record and cannot be dialed one-by-one.

The plan already agrees. §5.4 aggregates `topic_subscription_hint` per station
per realm to find serving **stations** (not subscribers); §7 fans out inside the
realm over HyParView + Plumtree. And this is the one part of Part 3 that is
genuinely built: `apps/hecate_overlay/` has `hecate_plumtree.erl`,
`hecate_or_set.erl`, and the realm-join flow.

So pubsub stays find-stations-then-gossip, in both the plan and this thesis.
Direct-dial is scoped to RPC and other point-to-point capabilities.

---

## 8. Feasibility gaps to close before building

These are the load-bearing unknowns. None is obviously blocking; all need a look.

1. **Multi-value get.** `macula:find_record/2` returns a single record today
   (`classify_find` in `macula.erl`), but Part 3 §10.3 intends `FIND_VALUE` to
   return a `record_list`. Either the `procedure_advertisement` payload grows from
   one `ServingStation` to a station set (one signed record per provider, whole
   set inside), or the DHT get becomes genuinely multi-value so many providers'
   records coexist under one procedure key. `realm_stations` sidesteps this by
   putting the whole set in one admin-signed record.
2. **Dialing a station outside the seed list.** The SDK pool is one link per seed
   (`macula:connect/2`, `macula:links/1`). Direct-dial requires the consumer pool
   to open a fresh link to an arbitrary advertised station it did not seed to.
   Confirm the pool supports (or can support) on-demand link creation, connection
   reuse per station, and a fan-out cap.
3. **Staleness / liveness.** The advertised set is a snapshot. The fleet has a
   documented failure where "a station whose transport is dead reports HEALTHY
   forever," so a consumer will dial a dead-but-listed station. The advertised
   record must be a live record (TTL + republish-on-change, which Kademlia already
   does for records, and which the realm service must do for the managed case),
   and the consumer needs an independent liveness check plus re-resolution, not
   just "retry the same list."
4. **Authz surface widens.** Direct-dial turns every station into a front door for
   every consumer in the realm. Each station must enforce realm auth for
   strangers (the L2 service-principal certs exist for this). State the model:
   who may open an inbound link and call, and how a station verifies it.

---

## 9. What this deletes, keeps, and builds

**Delete (eventually):**
- The distance-vector advertise gossip as the RPC routing substrate
  (`macula_station_peering_router.erl` advertise-propagation path, the
  tombstone / reconcile machinery in `macula_remote_advertise_registry.erl`).
- Source routing as the RPC data plane (`macula_relay.erl` for CALLs, the
  source-route CALL header path), on the strength of universal reachability.

**Keep:**
- The Kademlia DHT itself (it is the O(log N) resolver for public realms).
- The signed record layer (activate `procedure_advertisement` /
  `realm_stations`).
- The `hecate_overlay` Plumtree / HyParView / OR-Set overlay for pubsub.
- Source-route primitives may be retained for privacy-required realms (Part 3
  §6.7, a later phase), but not as the default RPC path.

**Build:**
- Wire `procedure_advertisement` / `realm_stations` into advertise and resolve.
- Extend the hecate-om capability summary to carry origin + stations, so
  `lookup/1` returns something dialable.
- Consumer-pool on-demand direct link to a resolved station, with failover.
- Live-record maintenance + consumer liveness / re-resolution.

---

## 10. The seam, in one sentence

The thesis keeps Part 3's discovery (build what was specified and left dormant)
and deletes Part 3's routing (replace source routing with direct-dial), and the
cut falls exactly on the §5.3 line "discovery is separate from routing." The
fragile gossip layer being removed was never the plan; it was the improvisation
that outlived its remit.

---

## 11. Open questions

- **Q1** Public-realm record shape: grow `procedure_advertisement` to carry a
  station set, or make the DHT get multi-value and keep one record per provider?
- **Q2** Does the SDK pool support dialing a station outside the seed list today,
  or is that new pool work? (Blocks the data-plane half.)
- **Q3** Managed-realm resolution: realm service answers resolution queries
  directly, or publishes `realm_stations` into the DHT and consumers read it
  there? (Affects the SPOF / replication story.)
- **Q4** K (provider multi-homing degree): fixed, per-service configurable, or
  load-adaptive?
- **Q5** Migration: can direct-dial run alongside the gossip path per realm during
  cutover, or is it a hard switch?

---

## 12. Checkpoint before any build

Per the repo's "one-line checkpoint before a work package" rule, this doc is the
checkpoint, not a commit-and-go. It is a BUILD (wiring dormant infrastructure to
the wire), not a CLAIM about the world, so it does not need an adversarial
science gate. The decisions owed to Raf before code:

1. Confirm the topology consequence in §4 is intended: universal station
   reachability, tiers no longer relaying the data path.
2. Pick the managed-realm-first path (§6) as the smallest starting slice, or
   another entry point.
3. Answer Q1 and Q2 (§11), the two feasibility questions that gate the data
   plane.
