# BRAINSTORM: the station cluster as the unit of service

**Status:** foundational reflection. NOT a design, NOT a plan. For adversary attack on the
PREMISE, not the details.
**Date:** 2026-07-25
**Provoked by:** a session spent trying to make a single starved station survive — three
failed fixes — when the real lesson is that a single station should never have been the
unit of service.

---

## 0. The reframe

The 2026-07-24 incident: one station on a starved 2-core box, its peer links dropping,
~20 minutes of lost delivery, self-recovered. Every fix attempted since — shed DHT work,
tune the silence probe, prioritise keepalives, async the puts — tried to make *that one
station* survive its own starvation. All three attempts were wrong, and the adversary's
verdict on the last one was blunt: the levers that matter (accept admission control,
reconnect jitter) address the connection lifecycle, and even those only make a single node
degrade *less* badly.

The root is not any mechanism. It is that **the single station is the unit of service, so
any single station's failure is a service failure.** No amount of single-node hardening
changes that; it only changes how often the single point fails.

**Proposal to attack:** a macula-station is never a single-node story. It always belongs
to a *station cluster* of at least three members that fail independently and jointly
present as one resilient relay, so that any one member starving, crashing, or reconnecting
is absorbed by the cluster and invisible to the mesh.

This is not hardening on top of the tiny-node model. It is the claim that **a single tiny
node is not a viable unit of service at all** — too small, too easily starved, too churny
— and that the viable unit is the smallest group that can lose a member without losing
service. If the endgame is thousands of tiny nodes, redundancy is not optional polish; it
is what makes tiny nodes usable.

---

## 1. Why three

Two is not enough and the reason is failure *detection*, not just failure *tolerance*:

- Two members cannot distinguish "my partner died" from "the link between us
  partitioned." Each may conclude it is the survivor and both act as primary — split
  brain.
- Three members give a majority: a member that can reach only itself knows it is the
  minority and steps back; the two that can still reach each other carry on. This is the
  same reason Raft/quorum systems start at three.

But note the crucial difference from a database (see §4): three is the minimum for
*honest failure detection and anti-split-brain*, NOT necessarily for a write quorum. A
relay serving best-effort from one live member beats a relay refusing service because it
lost quorum. The cluster should tolerate degrading 3 → 2 → 1 while still serving, and only
a total (3-of-3) loss is an outage. Quorum governs *coordination decisions* (who owns what),
not *whether the relay answers*.

---

## 2. Two tiers, two transports

The design that falls out of this is explicitly two-tier, and the tiers want different
transports:

**Tier 1 — intra-cluster (tight).** 3–5 members, chosen for independent failure. Tight
coupling: sub-second failure detection, shared subscription state, coordinated peer-link
ownership, hot-record replication. This is a small, high-trust, low-latency group. It is
the natural home for **Erlang distribution over QUIC via `macula-dist`**: `pg` process
groups for subscription fan-out, `global`/registry for "who owns peer link X", `ra`/khepri
only for the little state that genuinely needs consensus. BEAM clustering is built for
exactly this size and trust level.

**Tier 2 — inter-cluster (loose).** Clusters federate over the existing public QUIC mesh —
the macula-station overlay, Kademlia DHT, SWIM. WAN-scale, low-trust, eventually
consistent. Clusters are the vertices; the mesh is the graph between them.

The critical rule: **clusters connect to each other ONLY over Tier 2, never over Erlang
distribution.** Erlang dist is all-to-all and chatty; one giant Erlang cluster of thousands
of nodes is a well-known catastrophe. Keeping Erlang dist strictly intra-cluster (≤5 nodes)
and the QUIC mesh strictly inter-cluster is what makes BEAM-native clustering safe here.
Two transports for two fundamentally different coupling regimes.

---

## 3. How it dissolves the incident

Starved Nuremberg member's links drop and it thrashes on reconnect. Its two cluster-mates
(on independent boxes, not starved) still hold the subscription state and still have live
peer links to the producers. Delivery continues *through them*. The starved member rejoins
when the box recovers, and re-syncs from its mates. **No 20-minute gap** — the cluster
degrades 3 → 2 and keeps serving; the single-node starvation is invisible to the mesh.

This is the graceful degradation I kept trying and failing to build at the node level,
achieved at the layer where it is actually cheap: not "make the starved node survive," but
"make the starved node's failure a non-event."

---

## 4. The hard questions — where this idea has to survive attack

None of these are rhetorical. Each could sink the premise.

1. **Identity.** Does a cluster present as one relay (shared keypair — simple for the mesh,
   but a shared private key across 3 boxes is a security and rotation problem) or as three
   coordinated relays (each its own key — safer, but the mesh must understand they are a
   redundancy group to route around a dead member)? Kademlia already puts records on the
   *k* closest nodes; is a "cluster" partly just making that replication explicit and
   tight, or is it a genuinely new object?

2. **Consistency model.** Subscriptions must survive a member failure or subscribers miss
   events during failover — that needs tighter-than-eventual sharing. The DHT is already
   eventually consistent and fine with it. So the cluster likely needs *two* consistency
   regimes internally: fast/reliable for live subscription ownership, lazy for record
   replication. Is that coherent, or two systems wearing one name?

3. **Correlated failure — the killer.** Clustering assumes members fail independently. But
   mesh load is correlated: a churn wave loads everyone at once. If the same event starves
   all three members, the cluster gives nothing. Independent placement (different boxes,
   power, network, ideally region) is therefore mandatory, not advisory — and the mesh
   must actively resist correlated load, or clustering is a Maginot line.

4. **Does it just move the single point of failure up one level?** A cluster that stops
   serving on quorum loss is a bigger SPOF, not a smaller one. Avoided *only* if the
   cluster is replication-not-quorum for serving (any 1 live member answers) and reserves
   quorum for coordination. If that separation cannot be cleanly held, the idea inverts
   into a worse failure mode.

5. **Overhead vs the endgame.** This triples node count and adds constant intra-cluster
   chatter. Justified only if a single tiny node is genuinely non-viable as a unit. Is it?
   The churn/starvation evidence says maybe — but "maybe" is not "yes," and 3× the nodes is
   a real cost the tiny-node thesis was partly meant to avoid.

6. **Membership and formation.** How does a lone station find its two partners? Static
   config (operational burden, no self-healing), bootstrap-assigned (central-ish), or
   self-organising by keyspace proximity (Kademlia-native, but proximity ≠ failure
   independence — the closest nodes might share a box)? How does a cluster replace a
   permanently-dead member without a human?

7. **The delta over what already exists.** Kademlia k-replication + SWIM failure detection
   already give loose redundancy. What does an explicit tight cluster add? The honest
   answer is *speed and coordination*: sub-second failover for LIVE subscriptions (vs the
   DHT's minutes-to-hours reconcile) and explicit responsibility hand-off (vs emergent,
   uncoordinated replication). If that delta is not real, the cluster is re-inventing
   primitives the mesh already has, worse.

8. **The deepest doubt.** Is "a cluster of three" a database-thinking pattern (quorum
   groups, replicated state machines) imported into a mesh that was deliberately built on
   better-suited loose-coupling primitives (Kademlia, SWIM, gossip, CRDTs)? Meshes usually
   get resilience from *many loosely-coupled* nodes and stochastic replication, precisely
   to AVOID the coordination cost and split-brain hazards of tight groups. Does bolting
   tight 3-clusters onto a loose mesh get the worst of both — the coordination cost of
   consensus and the uncertainty of a mesh?

---

## 5. What I believe, stated so it can be attacked

The reframe (cluster-as-unit, not node-as-unit) is right and the session's evidence forces
it. The *implementation* instinct (tight Erlang-dist 3-clusters over macula-dist,
federated over the QUIC mesh) is attractive and might be exactly wrong — it may be
importing quorum-group thinking where the mesh's own loose primitives, used more
deliberately, would give the same resilience without the coordination cost. The question I
cannot answer alone: **is the right unit a tightly-coordinated cluster, or is it the same
loose mesh with an explicit rule that no service depends on any single node — redundancy
by stochastic placement, not by a named group?**

That is the fork worth arguing.

---

## 6. Adversary verdict (Fable, 2026-07-25): keep the reframe, discard the object

The loose side of the fork wins decisively. The named cluster-of-3 is quorum-group
thinking imported into a mesh deliberately built on Kademlia k-replication, SWIM, and
gossip to avoid exactly that — and §1 already strips out the quorum that would justify a
group (serving is declared quorum-free, any 1 member answers). A group with the quorum
removed keeps every cost of a group and none of the guarantees. §4.8's own fear, confirmed.

**What survives:** the reframe (stop hardening the single node; make single-node failure a
non-event), the invariant that subscription state must never live on one node, and the
rule that Erlang dist never spans the WAN. None of the three needs a named cluster.

**What sinks the object, ranked:**

1. **§3 is circular, and the incident had a cheaper cause.** The dissolved-incident
   scenario stipulates cluster-mates "on independent boxes, not starved" — but the
   incident's cause *was* co-tenancy (3 stations, 2 cores). The independent-placement
   discipline smuggled into §3 would alone have prevented it with zero clustering. Worse,
   the 20-min gap has a known explanation already on file: the open multi-hop pubsub
   self-healing bug ([[project_multihop_pubsub_propagation_broken]]). Clustering is a
   bandage over a routing-layer liveness bug and would not even cover it — if cross-relay
   propagation cannot self-heal, intra-cluster failover fixes nothing for a subscriber
   more than one hop away.
2. **The "no gap" failover is undeliverable or a hot-path tax.** For B to cover A's live
   subscriptions gaplessly it needs A's registrations (easy), a pre-warmed link to every
   producer (triples fan-out) or a cold start (a gap + the exact TLS handshake burst that
   caused the incident), AND the per-subscriber in-flight cursor replicated ahead of A's
   death. Cursor replication is either synchronous consensus in the path of *every event*
   on nodes defined as starved, or asynchronous — which is the at-least-once-with-gaps the
   loose mesh already gives for free. Seamless failover is the silence-probe problem's big
   brother: the hard part (agreeing what was delivered, faster than a node can fail) is the
   same hard part clustering claimed to dissolve.
3. **It breaks the commons contribution model — the Macula-specific killer.** Mandatory
   3-clustering forces either one operator running three boxes (triples cost, kills the
   marginal contributor, *re-correlates* the failure domain) or strangers clustering over
   Erlang dist — a full-trust primitive with no authz: "contribute a node" becomes "give
   two strangers root," and macula-dist changes the transport, not the trust. The
   shared-identity variant hands a compromised member the relay's private key. Macula is
   low-trust federation with per-node identity; this inverts it at the layer with the most
   power.
4. **The coordination tax is a starvation cascade.** Dist heartbeats, pg sync, ra
   elections, hot-record replication — permanent load on nodes whose defining property is
   "near limit." And failover *is* a load spike (re-sync + link establishment + handshake
   storm) delivered to the survivors exactly when the cluster is degraded. Three at 80%,
   one dies, its load lands on two already near limit, they follow. The design schedules a
   handshake storm as the recovery procedure — the incident's own physics, weaponised.
5. **net_tick is the silence probe reborn one layer down.** On a CPU-starved node the
   scheduler stalls past net_ticktime, dist tears the connection down, pg/global
   reconfigure, rejoin, re-sync storm, repeat — structurally identical to the refuted
   "starvation forges last_frame_at" bug. Plus dist head-of-line blocking and two failure
   detectors (SWIM + net_tick) on different clocks flapping against each other. And §1's
   split-brain argument self-defeats: majority-of-3 stops split brain only for
   quorum-gated decisions, but §1 declares serving quorum-free, so two partitioned members
   both serve as "the relay" with divergent subscription state — split brain at the
   delivery layer. The justification for *three* applies only to decisions the doc calls
   non-essential.
6. **Correlated failure kills independence as an engineerable property.** Thousands of
   volunteer tiny nodes = shared providers (the real fleet is a handful of Hetzner/Linode
   boxes), identical software (one bug fails all three mates at once — the incident's three
   co-tenants ran the same code), correlated load, and §4.6's Kademlia-proximity formation
   would select the *most* correlated nodes as mates. Anti-affinity needs a truthful global
   failure-domain registry a decentralized commons cannot verify. A named 3-cluster draws
   its three lottery tickets once at formation and holds them until a human intervenes.

**The answer to adopt:** the unit of service is neither the node nor a named cluster — it
is the **topic/subscription**, and the invariant is *"every live subscription is
materialized on at least k independently-placed nodes,"* enforced through the primitives
already deployed: Kademlia placement for subscription registrations (they become just
another k-replicated record, like DHT records already are), SWIM for detection, multi-path
re-route for delivery. Achievable target: fast at-least-once re-attach with a small
gap-or-dup — the same modest guarantee, reached with no formation protocol, no second
transport, no group identity, no stranger-trust. Plus the two boring truths the incident
was actually about: **fix the open multi-hop pubsub self-healing bug**, and **one station
per box.**

The user's instinct was right — *don't depend on a single node* — and the implementation
instinct was wrong: it reached for a database's replicated-state-machine where the mesh's
own loose primitives, used deliberately, give the same resilience without the coordination
bill, the trust inversion, or the cascade.

---

## 7. Refinement: seed-backed auto-clustering, and the synthesis (2026-07-25)

The user rejects the anti-commons framing of a MANDATORY 3-box cluster. Instead: the mesh
runs a **seed-cluster** (well-provisioned, always-on, mesh-operated nodes); a lone operator
runs ONE station; on join it auto-forms a group with 2 seed members. One contributed box,
two mesh-provided. This is a real improvement and it kills two of §6's objections:

- **Anti-commons (§6.3): defeated.** No operator needs three boxes and no one clusters with
  untrusted strangers.
- **Correlated failure (§6.6): materially improved.** Two random volunteer partners share a
  provider, a codebase, a churn wave. Two engineered, well-placed seeds fail independently
  by construction. A volatile leaf backed by a solid backbone gets redundancy three tiny
  nodes never could. It also matches reality: the backbone already exists (the relay fleet).

But taken LITERALLY (a coordination group over Erlang dist), the *mechanism* objections
survive untouched:

- **net_tick between a starved leaf and the seeds = the silence-probe false-positive reborn
  one layer down.** Dead vs descheduled, again.
- **Trust relocates, it does not vanish.** Dist is full-trust (rpc/spawn/inspect). Seeds
  dist-connected to arbitrary joiners must trust every random node or grow the authz layer
  dist lacks. macula-dist changes transport, not trust. And the seed tier becomes a central,
  privileged dependency — soft tension with "no central authority."
- **Live-link / cursor failover unchanged.** Seed reliability does not make hot-path cursor
  consensus cheap.

**The synthesis.** What must survive a leaf's death splits cleanly, and only one part is
hard:

1. Subscription registrations (passive) → k-replicated records, like DHT records already
   are. A well-connected seed is naturally a durable custodian in that set.
2. DHT records the leaf held (passive) → Kademlia already k-replicates. Solved.
3. Live peer links (active) → cannot be replicated, and do not need to be: with the
   subscription replicated onto a seed, the leaf's death re-routes publishes to the seed
   (holding the replica), which delivers over ITS OWN links. Multi-path routing, not
   warm-standby link mirroring.

Result: the user's desired outcome (a lone station backed by two reliable others, its
failure a non-event) with NONE of the cluster machinery — no formation protocol, no Erlang
dist, no net_tick, no split-brain, no trust inversion. **The seed is not a coordination
group member; it is a durable custodian in the replication topology.** The user reached
this from redundancy ("back the leaf with reliable seeds"); the loose-primitives argument
reached it from replication ("k-replicate the subscription"). Same design, two directions.

Residue: the **cursor** (exactly-what-was-delivered across failover) is genuinely hard and
seed reliability does not help. Target: fast at-least-once re-attach with small gap/dup,
which the capture pipeline's own `{epoch, seq}` already makes detectable and recoverable.

### 7b. Adversary verdict on the synthesis (Fable, 2026-07-25) — REFUTED against the code

Every load-bearing assumption was checked in the source and is false:

- **Subscriptions are NOT records.** They are live hop-by-hop routing state:
  `hecate_pubsub_server` subscriber sets + per-link SUBSCRIBE frames + bloom gossip
  (`macula_station_bloom_exchange`, 30s tick / 2s debounce). The delivery path routes by
  blooms ∩ live connections and **never consults the DHT** (verified: zero
  `find_record`/`k_closest`/`macula_dht` references in the whole pubsub/overlay delivery
  path). So "k-replicate subscriptions like DHT records" describes an unbuilt subsystem
  that the delivery path would not even read. "Like DHT records already are" is false.
- **Consumer-side survival is already solved.** `macula_station_outbound_link` replays
  every SUBSCRIBE on reconnect (`:86-87,527`). A seed replica adds nothing there except
  **buffering events for an absent consumer** — a store-and-forward mailbox with retention,
  addressing and the cursor, which the synthesis never admitted and which is its only real
  value.
- **"Seeds are naturally durable custodians" is a hand-wave.** Placement is XOR-closeness +
  diversity (`macula_dht_placement`: ≥8 ASN, ≥5 country, ≥3 tier, `degraded=true` escape),
  uncorrelated with reliability. Kademlia cannot produce "this leaf's two seeds" without
  special-casing seeds (the centralization the doc denies). And republish is a 24h **owner**
  obligation (`macula_dht_republish`, `DEFAULT_INTERVAL_MS = 86_400_000`); a dead leaf stops
  republishing, so "durable custody" has a 48h TTL on it and expires exactly when needed.
- **Refinement ≠ synthesis.** "Auto-form a group with 2 named seeds" is leaf-scoped,
  persistent, and needs a formation/assignment/replacement protocol; the synthesis is
  key-scoped stochastic custody with no named seeds. "Same design, two directions" is false
  — they are two incompatible topologies, and the anti-commons win belonged to the first
  while the "no formation protocol" claim belonged to the second.
- **It assumes the open multi-hop bug fixed, then adds nothing.** Re-routing publishes to a
  seed after churn IS the machinery the self-heal bug says is broken; and once fixed,
  bloom-fan + SUBSCRIBE-replay already give re-attach-with-gap. "Fast" is also false: bloom
  convergence is 2s debounce / 30s tick, ~6s multi-hop, nothing sub-second.
- **The seed tier is the new correlated-failure domain and scaling ceiling.** A churn wave
  focuses every leaf's failover on the same few seeds — the mesh-wide version of the
  incident's handshake storm, at a fixed address. Seed capacity must scale with mesh size,
  so growth is bounded by the steward's budget. SPOF moved up a level, passing only because
  the hardware is better.
- **"Signed records" is authn, not authz.** Sybil keys sign unbounded junk registrations;
  redirection (naming another party as sink) is a traffic-bombing primitive; quotas make
  seeds central gatekeepers — the authority the frame forbids.

## 8. What this whole thread actually concludes

Three foundational ideas (named cluster of 3; seed-backed cluster; k-replicated
subscriptions) all failed for the **same reason**: each tries to put a **strong delivery
guarantee inside the network**. The mesh is deliberately best-effort — bloom-routed,
eventually consistent, at-least-once-with-gaps — and every attempt to make a single node's
failure invisible at the mesh layer either re-invents consensus, breaks the commons trust
model, or moves the single point of failure up a tier.

The end-to-end principle is the resolution: **reliability beyond at-least-once-with-gaps
belongs at the endpoints, not in the mesh.** An application that needs to lose nothing
implements its own buffer + replay. And for the capture pipeline that already exists at the
application layer: `hecate-grid`'s overlapping-window polling is the replay, `hecate-archive`'s
append-only tape is the buffer, `{epoch, seq}` is the gap detector. The capture pipeline is
already correctly designed; the mesh stays best-effort and the app provides the guarantee.

So the honest output of the whole exploration, stripped of every architecture that failed:

1. **Fix the multi-hop pubsub self-heal bug** ([[project_multihop_pubsub_propagation_broken]]).
   The one real mesh defect the incident exposed.
2. **One station per box.** The operational rule the incident was actually about.
3. Keep the mesh **best-effort**. Do not build clustering, seed-custody, or
   subscription-replication into it.
4. If a use case needs zero loss, it implements **end-to-end buffer + replay** — and if a
   shared such buffer is ever wanted, propose it under its real name: a store-and-forward
   mailbox SERVICE with an operator, retention, target-consent authz, quotas, and a replay
   protocol. Not emergent Kademlia behavior, not a "cluster."

The reframe that survives from §0 is real and worth keeping: stop hardening the single
node. But the conclusion is not "make the mesh cover single-node failure." It is "**the mesh
does not have to** — the endpoints do, and for capture they already do."
