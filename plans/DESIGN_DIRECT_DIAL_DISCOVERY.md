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

**Confirmed 2026-08-18: tiers are set aside for the data path.**

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

### 4.1 ELI5 — what "tiers set aside for the data path" means

A **station is a hub**, like a telephone exchange. The homes plugged into it are
the services and daemons; they have no front door of their own (daemons are
outbound-only, they dial out and never accept incoming calls). So the homes are
not the reachable things. The **stations** are.

The old design ranked the exchanges into **tiers**: small local exchanges at the
bottom, regional above, national at the top. A request had to climb up the ranks
and back down — from your local exchange, up through regional and national ones,
across, then down the far side to the exchange your target is plugged into. It
worked that way because it assumed you could not place a line straight to a
far-off exchange.

The reachability rule changes exactly that: **every exchange has a public,
dial-anytime number** (the exchanges, not the homes). So instead of climbing the
hierarchy, a consumer opens a direct line to the specific station its target is
plugged into, and reaches it there. One direct connection to the right hub.

So "tiers set aside for the data path" does NOT mean removing stations — they are
the hubs, homes cannot reach the mesh without them. It means dropping the
**ranking among stations as a relay chain**. The provider helps by plugging into
several stations (its K) and advertising them, so a directly-dialable station is
always one short step from the provider. Tiers may still rank stations for trust,
capacity, or discovery diversity; they just stop being a ladder that a request
climbs.

This is also why §8.2's `station_endpoint` gap matters: the reachability promise
lands on the **station**, and today the records name a station but do not carry
its dial number. "Any station is dialable" is a promise the data cannot keep
until the station's endpoint is published.

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

### 6.1 What a managed realm already is (macula-realm)

**DECIDED 2026-08-18: managed-realm-first is the entry slice.**

A managed realm is a running service, the `macula-realm` app ("Macula Portal"),
not a config flag. A public realm has none of this: a realm is a 32-byte tag,
stations are realm-agnostic, trust is self-sovereign (each advertiser signs its
own records, discovery is a DHT lookup, you verify signatures yourself). The
managed realm adds a front desk.

**⚠ Three realm-named things; only one has code. Do not build against the wrong
repo.**

| Thing | Location | State |
|---|---|---|
| `macula-realm` | `macula-io/macula-realm` | The real managed-realm service. All of §6.1 below. |
| `hecate-realm` | `hecate-social/hecate-realm` | Empty placeholder repo (`.gitignore` + `LICENSE`, one commit 2026-02-01, no code). |
| `hecate_realm` | `macula-station/apps/hecate_realm` | Empty station sub-app (`.app.src` only, no modules). |

The station README says its transitional `hecate_realm` / `hecate_overlay`
sub-apps keep the `hecate_` prefix "until they migrate to the realm-service
repository," and the empty `hecate-social/hecate-realm` looks like the reserved
destination. **OPEN OWNERSHIP QUESTION (Q6, for Raf): does realm-service code
stay in `macula-realm`, or migrate to `hecate-realm`?** This changes only which
repo the managed-realm slice targets, not the design. Resolve before wiring the
slice, so it is not built into a repo about to be emptied.

In running code, `macula-realm` already:

1. **Is the realm CA.** `MaculaRealm.Identity.Certificate` runs Realm CA → Org CA
   → per-service leaf certs; services are provisioned at
   `POST /api/v1/services/provision` (`ServicePrincipalIssuanceController`) and
   the cert is what `hecate_om_identity` loads. The realm decides membership
   cryptographically.
2. **Runs the admission gate** (join tokens / join sessions / provisional +
   refresh issuance).
3. **Keeps a live station directory.** `MaculaRealm.Topology.Directory` holds a
   pubkey-keyed stations graph from mesh presence (`_mesh.station.*`) + DHT
   node-record snapshots, with `hostname_for/1` (pubkey → address) and
   `Topology.StationLinks.client_for_pubkey/1` (a direct link to one chosen
   station).
4. **Reads records-as-state** (`Dht.RecordSubscriber`, 30s poll, per the in-repo
   `PLAN_DHT_FIRST.md` — same "facts as state" philosophy as this thesis).

### 6.2 Why this is the smallest first slice

The three things the data plane needs are dormant/missing on the public path but
already built and running in the managed realm:

| Need | Public path | Managed realm today |
|---|---|---|
| Sign a `realm_stations` set | no authority | owns the Realm CA + curates the directory |
| Station pubkey → dial address (§8.2 gap) | records carry pubkey only; `station_endpoint` dormant | `hostname_for/1` already resolves it |
| Dial one specific station (Q2 gap) | pool fixed to seed set | `StationLinks.client_for_pubkey/1`; proper fix `subscribe_on_station` on macula `BACKLOG.md` |

First slice: have the realm service publish and serve the station set it already
knows, and let a consumer resolve it and dial one station directly, reusing the
existing per-station link machinery. No Kademlia, no crypto-puzzle, no source
routing. The public-realm version (DHT-resolved, self-signed) follows on the same
data plane.

### 6.3 Public realm and dual-trust

The public realm is the general case and the one that must reach 100k+ stations
(no central directory, discovery is O(log N) DHT lookups). Two requirements shape
it:

- **Fully open is required in any case.** Anyone may advertise, anyone may find.
  The discovery layer stays permissionless.
- **Dual-trust is also required.** Trust is bidirectional, not the one-directional
  server-authenticates-client of classic RPC:
  - **consumer → provider** — is this the legitimate server of the procedure, not
    a squatter who wrote a `procedure_advertisement` next to the real one?
  - **provider → consumer** — should I serve *this* caller at all? Serving costs
    resources and may expose a sensitive operation. Direct-dial makes every
    station a public front door, so the provider must decide who it answers.

Direct-dial makes this cleaner, not harder: it collapses the path to ONE QUIC/TLS
session between exactly two sovereign identities, which is the natural place for a
mutual handshake.

**Feasibility (checked 2026-08-18): the primitive and wire slot exist; only
enforcement is missing.**

- `macula_ucan_nif:verify/2` (token + pubkey → payload or typed error:
  `invalid_token` / `invalid_signature` / `expired` / `not_yet_valid`), Rust NIF
  with an Erlang fallback. Offline, delegatable, attenuable capability
  verification is present. `macula_did_nif` sits beside it.
- `macula_protocol_types.erl` already carries an optional `ucan_token => binary()`
  on call / cast / publish / subscribe frames, plus `default_ucan` for
  session-wide grants. A capability can already travel on a CALL.
- Nothing on the inbound CALL path reads it (`macula_handler_dispatch` just looks
  up and invokes; `hecate_om_identity` notes the mesh does not yet verify realm
  membership at connect/publish). So dual-trust is decision logic at the two
  endpoints, NOT new crypto and NOT a wire change.

**The model: openness and trust are per-endpoint policy, not a global mode.** The
discovery layer stays open; each endpoint independently chooses what it checks.
Four valid combinations, negotiated per connection:

| | Open | Strict |
|---|---|---|
| **Provider** | serves any caller (no `ucan_token` required) | requires a valid UCAN granting the right to call |
| **Consumer** | dials any advertised provider | requires the advertisement to chain to a trusted root, or a pinned pubkey |

**Fully open is not anonymous.** Every connection is Ed25519 peer-bound at QUIC,
so the provider ALWAYS knows the caller's identity, even in open mode. "Open"
means "I serve any *identified* caller," so a provider can always rate-limit or
blocklist by identity. Dual-trust adds *positive* authorization (a capability
allowlist) on top of an identity that is always present. Identity is the
sovereignty layer; the UCAN is the authorization layer above it. This matches
[[feedback_no_anonymity_only_sovereignty]].

**Both directions use the same primitive, verified offline:**

- provider → consumer: the CALL's `ucan_token` is verified against the chain the
  provider recognizes; absent/invalid where required → refuse (needs a BOLT#4
  `unauthorized` code, see Q9). The root key delegated the capability to the
  caller ahead of time; the provider checks it offline, no live authority.
- consumer → provider: the signed `procedure_advertisement` is verified to chain
  (via UCAN delegation) from the key owning the procedure namespace, or to match a
  pinned pubkey. Squatter records fail the chain.

Same shape both ways: signed, attenuable, offline-verifiable tokens rooted in a
key already trusted, delegated in advance rather than enforced live. That is what
lets dual-trust coexist with fully-open discovery without putting an authority
back in the path. The sub-decisions it opens are Q7–Q9 (§11).

### 6.4 Origin: same binary, different shops

An announcement carries origin at three levels, so two providers running the same
service binary but belonging to different "shops" are distinguishable:

1. **In the procedure name.** A `procedure_uri` is namespaced, not bare: Part 3's
   example is `realm42/org/app/weather/get_forecast_v1`, and service principals
   bind to `mri:app:io.macula/<org>/...`. So the URI is `realm / org / app / proc`.
   Two shops on the same binary have DIFFERENT procedure_uris (the `org` segment
   differs) → different `SHA-256(procedure_uri)` → separate DHT records. Same code
   does not mean same address.
2. **In the record.** `procedure_advertisement_payload` carries `advertiser_node`
   (the provider's pubkey) alongside `procedure_uri` and `serving_station`, and the
   envelope is signed by that key. The multi-value bag store keeps one per signer,
   so even two providers of the identical URI stay distinct by signature.
3. **In the realm tag.** Everything is realm-scoped; the station registry is keyed
   `{realm, procedure}`, so different realms never collide even on one station.

**Where origin flattens today:** the live gossip path enforces
single-provider-per-station keyed `{realm, procedure}`, and `call/5` takes only
`(realm, procedure)`. So if two providers ever advertise the *exact same*
fully-qualified URI, one survives on a station and the caller cannot select
between them. This is a property of the gossip path, not of the data.

**What a "shop" maps to (decision):**

- **Shop = org (or realm).** Different procedure_uris already, separate records,
  no collision, no change needed. This is the intended use of the namespace.
- **Shop = bare instance** (same realm+org+procedure, different keypair). Then by
  the addressing model these are *replicas of one capability*, not different shops;
  origin still exists (`advertiser_node`) but the call path treats them as
  interchangeable.

**Direct-dial is better at this than the gossip it replaces.** A resolve returns
the whole SET of advertisements, each with its `advertiser_node` and
`serving_station`, so the consumer SEES the shops as distinct signed entries. To
call a specific one on a shared station: the CALL frame already has an optional
`target` node field (the SDK just does not expose it), and the station keys routing
by `{realm, procedure, advertiser}` instead of `{realm, procedure}`. A small,
well-defined relaxation, not a redesign.

**This is Q8 in disguise.** "Which shop is this" and "is this the legitimate
provider" are the same question: the org key that owns the namespace prefix is
exactly what a consumer verifies to know both that the record is not a squat AND
which shop it belongs to. Answering Q8 answers how shops are told apart.

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

1. **Multi-value get — ANSWERED 2026-08-18: the store is already multi-value.**
   The DHT local store is an ETS **`bag`** (`macula_dht_server.erl:343`).
   `store_put/2` dedupes by envelope key (the signer) and keeps every other
   signer's record under the same storage key; `store_lookup/2` /
   `find_local_record/2` return the full `[record()]`; the wire `FIND_VALUE`
   reply already carries the list. So N providers advertising one procedure
   already produce N coexisting records under `SHA-256(procedure_uri)`. Keep
   `procedure_advertisement` one-station-per-record; the set is the collection
   under the key (Part 3 §5.3, "1..20 advertisement records"). The ONLY defect is
   the read path narrowing to one: `macula_station_dht_handlers.erl`
   `on_local_hit([Record | _], ...) -> {ok, Record}` drops the tail, and the SDK
   `macula:find_record/2` classifies a single record. Fix is additive: a
   list-returning read (`_dht.find_records` + `macula:find_records/2`) returning
   `store_lookup` whole. No storage change, no `FIND_VALUE` wire change. **Do NOT
   grow the payload and do NOT build multi-value storage — both already exist.**
2. **Dialing a station outside the seed list — ANSWERED 2026-08-18: yes, new work,
   bounded, plus a second gap.** The pool is fixed to its seed set:
   `macula_client:connect/2` spawns one `macula_station_link` per seed
   (`start_link_for_seed/2` over `Seeds`), `links = #{seed() => link_state}`,
   respawn is per-existing-seed, and there is no exported add-link / dial API. Two
   pieces of work: **(a) runtime dial + reuse** — the seam exists
   (`start_link_for_seed/2` already spawns a link for an ad-hoc seed under an
   arbitrary `links` key), so this is exposing a dial-then-call API with one link
   per station reused across capabilities and a fan-out cap, not a rewrite;
   **(b) address resolution** — a NodeId is not dialable and NO discovery record
   carries a dial endpoint (`realm_stations` entries are `#{station_id, roles}`;
   `procedure_advertisement` `ServingStation` is a pubkey; the routing layer zeroes
   addresses: `macula_dht_store` outcome note, `entry_to_station_ref` publishes
   `addresses => []`). The purpose-built `station_endpoint(StationPubkey, QuicPort)`
   record (type `0x12`) exists but is DORMANT (unwired in the station). So
   direct-dial needs the target's host:port carried in the advertisement, or
   resolvable via an activated `station_endpoint` record. This is the concrete form
   of constraint 2: "stations are publicly reachable" must become "stations publish
   a dialable endpoint."
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

- **Q1 — ANSWERED 2026-08-18 (§8.1).** Neither: the DHT store is already an ETS
  `bag`, multi-value and signer-deduped. Keep `procedure_advertisement`
  one-station-per-record; only un-narrow the read path (add a list-returning
  `find_records`). No storage or wire change.
- **Q2 — ANSWERED 2026-08-18 (§8.2).** New work, but bounded and additive: (a) a
  runtime dial+reuse API on the pool, built on the existing `start_link_for_seed`
  seam; (b) a dialable endpoint in the advertisement, by activating the dormant
  `station_endpoint` record or adding host:port to the advertised record. The
  latter is the data-level form of the universal-reachability constraint.
- **Q3** Managed-realm resolution: realm service answers resolution queries
  directly, or publishes `realm_stations` into the DHT and consumers read it
  there? (Affects the SPOF / replication story.)
- **Q4** K (provider multi-homing degree): fixed, per-service configurable, or
  load-adaptive?
- **Q5** Migration: can direct-dial run alongside the gossip path per realm during
  cutover, or is it a hard switch?
- **Q6** Realm-service home: stays in `macula-realm`, or migrates to the reserved
  (currently empty) `hecate-social/hecate-realm`? Decides which repo the
  managed-realm slice targets (§6.1). Design-neutral; blocks only the wiring.

Dual-trust in the public realm (§6.3):

- **Q7** Default posture: open-by-default (a bare advertisement means "anyone may
  call," gating is opt-in) or gated-by-default (no capability, no service)?
  Open-by-default matches "fully open in any case"; gated-by-default is safer but
  noisier.
- **Q8** Root of trust for a procedure namespace: a `procedure_uri` like
  `realm/org/app/proc` needs a key at some prefix that owns it, so the consumer
  knows what the advertisement must chain to. Realm key (the 32-byte tag), an org
  key beneath it, or the app/resource-owner key?
- **Q9** Add an `unauthorized` code to the BOLT#4 taxonomy so a gated provider
  refuses legibly instead of timing out, and a consumer can tell "not allowed"
  from "not reachable."

---

## 12. Checkpoint before any build

Per the repo's "one-line checkpoint before a work package" rule, this doc is the
checkpoint, not a commit-and-go. It is a BUILD (wiring dormant infrastructure to
the wire), not a CLAIM about the world, so it does not need an adversarial
science gate. The decisions owed to Raf before code:

1. ~~Confirm the topology consequence in §4 is intended: universal station
   reachability, tiers no longer relaying the data path.~~ **Confirmed
   2026-08-18** (see §4, §4.1).
2. ~~Pick the managed-realm-first path (§6) as the smallest starting slice, or
   another entry point.~~ **Decided 2026-08-18: managed-realm-first** (§6.1,
   §6.2). The realm service already holds the CA, the station directory
   (`hostname_for/1`), and per-station dialing (`client_for_pubkey/1`), so the
   three data-plane gaps are already solved in that context.
3. ~~Answer Q1 and Q2 (§11), the two feasibility questions that gate the data
   plane.~~ **Done 2026-08-18.** Q1: store is already multi-value, un-narrow the
   read (§8.1). Q2: bounded new pool work + a dialable endpoint in the
   advertisement via the dormant `station_endpoint` record (§8.2). Neither gates
   the build; both are additive on existing seams. The remaining decision is §4
   (confirm the topology consequence) and §6 (managed-realm-first entry point).
