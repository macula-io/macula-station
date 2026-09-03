# PLAN_DEFERRED_WORK.md — Macula V2 deferred-items catalog

**Purpose:** single source of truth for every piece of V2 work we
have <em>intentionally</em> not done yet. Each entry lists what it
is, why it was deferred, what unblocks it, and the trigger phrase
that picks it up. Nothing lives here that we forgot — it is an
active queue, not a graveyard.

**Invariants:**

1. Every deferral has a <b>blocker</b> (external, sequencing, or
   cost) and an <b>owner</b> (us, foundation-ops, third party).
2. Every deferral has a <b>trigger</b> — a verbal phrase or a
   concrete event that licenses us to pick it up.
3. Nothing in here is "maybe someday" — items without clear triggers
   belong in `PLAN_MACULA_V2_PART9_OPEN.md` as open questions, not
   here.

Cross-reference:
- `PLAN_MACULA_V2_ROOT.md` §10 — phase roadmap.
- `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md` §11–§13 — Phase 6–8 tasks.
- `PLAN_MACULA_V2_PART9_OPEN.md` §2 — full O1–O39 open-questions
  index.
- `PHASE_2_LIFEGUARD_GAPS.md` — SWIM Lifeguard extensions.
- `PLAN_PHASE_6_BREAKDOWN.md` — shipped vs deferred 6.x items.

---

## 1. Phase 6 real-network adapters (us-blocked vs foundation-ops-blocked)

All five cascade strategies ship with pluggable transport behaviours and
canned-fake CI coverage. The concrete adapters against real networks
below are the last Phase-6 items. Some are blocked on foundation
operational infrastructure; others are self-contained sub-projects
we can ship when convenient.

| # | Item | Files | Blocker | Owner | Trigger |
|---|------|-------|---------|-------|---------|
| 6.3.5 | IPv6 anycast Tier-A probe (Part 5 §4.4) — parallel QUIC handshake against the foundation's `/48' anycast prefix; reachable endpoints feed via_doh as extra resolver targets | new `macula_bootstrap_anycast` | Foundation must allocate + RPKI-sign a `/48' and announce from ≥2 custodians' ASNs | Foundation ops | "Start 6.3.5 — anycast probe" (after foundation confirms prefix) |
| 6.3.6 | Gated DoH CT against real resolvers (Cloudflare / Quad9 / Mullvad) | `macula_bootstrap_doh_SUITE` | Foundation must publish signed PKARR records under `_pkarr.<b32>.macula.io' zone | Foundation ops | "Start 6.3.6 — gated DoH" (after PKARR zone live) |
| 6.4.y | Link-local scope-id + multi-interface mDNS fan-out (Part 5 §5) | Probe fan-out + interface enumeration shipped 2026-04-15 (Sprint C). Remaining: per-interface responder sup + real-network validation against a multi-NIC host. | None (unblocked) | Us | "Finish 6.4.y — responder fan-out" |
| 6.5.x | Real Mainline DHT UDP client (BEP 5 + BEP 44 `get`) | new `macula_bootstrap_dht_udp` | None (unblocked) — 500-1000 LOC sub-project; choose Erlang-native or Rust NIF | Us | "Start 6.5.x — BT-DHT UDP client" |
| 6.6.y | Gated CT against real Electrum/Esplora + Infura/Ankr | `macula_bootstrap_via_blockchain_SUITE` | Foundation must publish Bitcoin OP_RETURN + Ethereum `AnchorPublished' events on testnet | Foundation ops | "Start 6.6.y — gated chain CT" (after testnet anchors published) |

**Sub-project sizing:**
- 6.5.x is big — minimal Mainline DHT client needs routing table
  (another Kademlia), UDP RPC, transaction correlation, token
  handling, iterative find-node, BEP 44 `get`. Expect 4–6 sessions.
- 6.3.5 and 6.3.6 are small once unblocked (1–2 sessions each).
- 6.4.y is a single-session enhancement.
- 6.6.y is 1 session wrapping existing adapter with real endpoints.

---

## 2. Phase 2 Lifeguard extensions (L1–L4)

Classic SWIM shipped. Lifeguard extensions (per DSN 2002 + arXiv
2017) are deferred until real-WAN telemetry is available for tuning.

| ID | Extension | Depends on |
|----|-----------|------------|
| L1 | Indirect-ping via `k' buddies | None |
| L2 | Self-awareness multiplier | Fleet-scale health baseline |
| L3 | Refutation-buddy (NACK relay) | L1 (shares buddy selection) |
| L4 | Explicit NACK from buddy to suspector | L1 + L3 |

**Deferral rationale** (per `PHASE_2_LIFEGUARD_GAPS.md`):

- Phase 2 acceptance bar (confirmed_failed within 8 s on 3-station
  LAN) passes without Lifeguard.
- DHT (Phase 3) depends on SWIM <em>membership events</em>, not on
  Lifeguard quality.
- Lifeguard tuning is data-driven — `T%', `k', multiplier bounds —
  and wants real-network telemetry before calibration.

**Trigger:** "Resume Phase 2 — Lifeguard extensions" (pick L1 first,
then L3 + L4 together, then L2 last).

**Gating open questions:** O13 (group partition threshold at 256),
O14 (Tier-3 heartbeat period at 15 s), O16 (self-awareness
sensitivity on RPi-class hardware).

---

## 3. Phase 7 — Hardening (not started)

Scope per `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md` §12.

| Session | Deliverable | Gating |
|---------|-------------|--------|
| 7.1 | Chaos test suite — node kills, network partitions, clock skew, BGP simulation. ✅ Primitive harness shipped 2026-04-15 (Sprint D) — see `test/fleet_chaos.erl'. Remaining: partition / clock-skew / BGP-sim primitives need iptables + time-travel tooling at the fleet level. | O34 (harness tooling: PropEr native + custom) |
| 7.2 | Sybil flood — 10 000 adversary NodeIds across 3 ASes, measure bucket diversity | — |
| 7.3 | Eclipse simulation — adversary controls 50% of target's routing table, measure lookup degradation | — |
| 7.4 | Adaptive crypto-puzzle difficulty (foundation-signed parameter bumps via via_doh/C/D re-fetch) | O1 (puzzle difficulty policy) |
| 7.5 | 24-h burn-in on 10-station lab fleet | O39 (burn-in duration — 72 h minimum, 7 d if schedule permits) |
| 7.6 | Tier-3 SLA behaviour elicitation (limited T4 reachability) | O14 |
| 7.7 | Security review of every signed-record path | O23 (firmware signing), O33 (sigstore) |

**Trigger:** "Start Phase 7 hardening — Session 7.1" (after station
integration sprint lands).

**Gating open questions:** O4 (jurisdiction list), O7 (GDPR
controller), O14, O18 (k=3 disjoint paths review), O27 (error-code
registry), O32 (CI OTP matrix), O33 (sigstore).

---

## 4. Phase 8 — Lab cutover (not started)

Scope per `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md` §13.

| Session | Deliverable |
|---------|-------------|
| 8.1 | Fleet bring-up on beam00–03 via macula-demo GitOps |
| 8.2 | 72 h zero-incident soak |
| 8.3 | V1 shutdown (relay boxes removed from service) |
| 8.4 | 1.0.0 cut at hex.pm + release signature |

**Depends on:** Phase 7 complete + station integration sprint
complete + PLAN_STATION_RUNBOOK.md acceptance.

**Trigger:** "Start Phase 8 — Lab cutover".

**Gating open questions:** O36 (foundation-signed conformance-test
vector registry — foundation, monthly refresh), O37 (public chaos
report transparency), O39 (burn-in duration).

---

## 5. Open questions (O1–O39)

Full master index: `PLAN_MACULA_V2_PART9_OPEN.md` §2. Reproducing
the phase-gating summary here — these are the questions whose
resolution gates a specific phase:

| Question | Gates | Leaning |
|----------|-------|---------|
| O1 Crypto-puzzle difficulty policy | Phase 3 | Adaptive, foundation FROST signs |
| O2 Blockchain anchor chain | Phase 6 start | Both BTC + ETH (≈240 EUR/year) |
| O3 Foundation custodian set | Phase 6 start | 5 custodians, 3-of-5 FROST, EU-spread |
| O4 Jurisdiction list | Phase 7 start | EU27 + EEA + UK + CH; Balkans opt-in |
| O5 CGNAT-only operators | Phase 4 | Degraded-T0; no routing participation |
| O6 mDNS default announce | Phase 6 | Announce on (LAN = trust boundary) |
| O7 GDPR controller model | Phase 7 (legal) | Three-level; legal review pending |
| O8 Gateway-tier incentives | V2 launch | Foundation-funded; tokens deferred |
| O9 Post-quantum migration | Phase 9 | Hybrid classical+PQ, timeline TBD |
| O10 Quicer vs Quinn | Phase 1 | Stay quicer unless chaos flags issues |
| O11 T-1 nodes-as-proxy tier | Post-V2.0 | No; revisit on user friction |
| O12 Tunnel-only stations | Phase 4 | Full T0, reduced diversity weight |
| O13 SWIM partition threshold | Phase 2 chaos | Keep 256 until 1000-station test |
| O14 Tier-3 heartbeat period | Phase 7 field | 15 s; measure on constrained bw |
| O15 Tombstone retention | Phase 3 review | 96 h; 7 d if replay attack bites |
| O16 Lifeguard self-awareness on RPi | Phase 2 calibration | HashiCorp defaults |
| O17 Path-cache TTL | Phase 4 tuning | 5 min |
| O18 k=3 vs k=5 disjoint | Phase 7 | k=3 at V2.0 |
| O19 HyParView active-view ceiling | V2.1 | 15; revisit for realms >1000 |
| O20 Subscription-hint scope | Phase 5 | Per-realm |
| O21 Tier-penalty edge coefficient | Phase 4 | Heuristic, chaos-calibrate |
| O22 Bootstrap-tier attestation | Phase 6 | Opt-in (privacy) |
| O23 Firmware signing | Phase 6 | Reproducible builds + foundation-FROST |
| O24 Foundation rotation cadence | Phase 6 | Quarterly drill |
| O25 BERT/CBOR RPi regret | Phase 2 | Measure first |
| O26 Max 8 source-route hops | Phase 9 | Revisit with onion routing |
| O27 Error code space 0x80–0xFF | Phase 7 | Foundation-maintained registry |
| O28 Tombstone tag disposition | — | **RESOLVED** 2026-04-14 (distinct 0x0C) |
| O29 Anonymous-auth cipher | Realm-join plan | Deferred |
| O30 Extract macula_* library | V2.1 | Wait for second consumer |
| O31 Quicer vs Quinn final call | Phase 1 | Stay quicer |
| O32 CI OTP 27+28 matrix | Phase 7 | OTP 27 only until Phase 7 |
| O33 Sigstore + foundation co-sign | Phase 7 | Add at hardening |
| O34 Harness tool choice | Phase 2 | PropEr + custom |
| O35 TLA+ spec effort | V2.1 | Defer unless audit demands |
| O36 Conformance test vector registry | Phase 8 | Foundation monthly refresh |
| O37 Public chaos report | Post-Phase-8 | Yes, ecosystem credibility |
| O38 Rust-port cross-impl partner | TBD | No committed partner |
| O39 Burn-in duration pre-cutover | Phase 7 | 72 h minimum, 7 d preferred |

---

## 6. Adjacent plans (separate scope, not started)

These live outside the V2 Part 1–9 plan set and will run as separate
sprints once prerequisites land.

| Plan | Scope | Prerequisites | Trigger |
|------|-------|---------------|---------|
| [`PLAN_HECATE_REALM_SERVICE.md`](https://github.com/macula-io/macula-realm-identity/blob/main/plans/PLAN_HECATE_REALM_SERVICE.md) | **IN PROGRESS (2026-08-27)** — realm identity service, now its own repo `macula-io/macula-realm-identity` (extracted from `macula-realm` 2026-08-26, not a `hecate-social` white-label). Phases 0-3.5 shipped (SDK frame transport, admission desk, point-to-point overlay relay); Phase 4 (HyParView dispatcher) + Phase 5 (Plumtree gossip) not started but newly unblocked — see the plan doc §2. Consumes `macula`'s `src/overlay/` (HyParView + Plumtree folded into the SDK itself, not separate packages) as a hex dependency, not `apps/hecate_overlay/` directly. | Phases 0-3.5 (done) | Pick up Phase 4 per the linked plan |
| `PLAN_MNS_AND_REALM_JOIN.md` | Realm admission flow: OAuth-style join, invitation codes, admin key rotation, cross-realm federation. Runs on the realm service above; daemon side handles UI. | Realm service skeleton + V2 walking skeleton (done) + realm_directory + realm_member_endorsement records (done) | "Start realm-join plan" |
| `PLAN_GIT_OVER_MESH.md` | **ACTIVE (2026-04-21)** — git server pattern riding on V2 `macula:advertise/call/subscribe`. Core daemon capability replacing GitHub for all Hecate repos (gitops, plugin source, Martha handles). Umbrella apps: `guide_repo_lifecycle` (CMD) / `project_repos` (PRJ) / `query_repos` (QRY) / `serve_git_over_mesh` (infra) / `announce_ref_updates` (infra). Rust `git-remote-mesh` binary bridges native git to mesh procedures. Svelte browser in hecate-web (RepoList, RepoBrowser, CommitLog, DiffViewer, RefSubscription). Plan lives at `macula-io/macula-station/plans/PLAN_GIT_OVER_MESH.md'. | None — all prerequisites shipped | Picked up 2026-04-21 (companion to macula-realm Hanko migration) |
| `PLAN_LOCAL_FIRST_BOOT.md` | **Daemon** (user-machine, `hecate-daemon' repo) boots offline, mesh activates on demand via `POST /api/mesh/activate'. NOT a station concern — stations are always-on servers. | Daemon repo + Tauri bridge | "Start local-first boot" |

---

## 7. SDK (`macula-io/macula` v2) deferred items

| Item | Where | Blocker | Trigger |
|------|-------|---------|---------|
| Foundation key-rotation handling | `macula_foundation:apply_rotation/1' (new) | Phase 7 §12.4 — rotation protocol | "Start rotation handling" |
| Keccak-256 helper | `macula_identity:keccak256/1' | None (OTP has SHA-3 not Keccak; need NIF or port) | "Add keccak256 helper" |
| Full FROST threshold signer | New app `macula_frost' | External crypto lib / Rust NIF | "Start FROST signer" |
| Stable Quinn NIF swap (if quicer unstable) | `macula_transport' | O10 trigger (Phase 1 chaos flags quicer) | "Swap to Quinn NIF" |

---

## 8. Station deferred items — post-sprint snapshot (2026-04-15)

Station integration sprint 8.1–8.8 shipped. The pre-sprint wishlist
that used to live here (persistent identity, listener config,
periodic rebootstrap, admin API, graceful shutdown, realm-member
endorsement loading) is all landed code. What follows is the
list of items the sprint explicitly deferred, grouped by category.
Each session's `Deferred:' block in `PLAN_STATION_INTEGRATION.md'
is the authoritative detail.

### 8.1 — station-level polish

**Shipped in Sprint B (2026-04-15):**

- ✅ **Exponential back-off on repeated rebootstrap failures** —
  `macula_station_rebootstrap' now widens its partition window as
  `base × 2^min(N, 4)' per consecutive trigger, capping at 16×
  base. On DHT recovery, `consecutive' resets to 0 and a
  `{macula_station_rebootstrap, recovered, N}' notification
  surfaces the number of skipped retries. Status map gains
  `consecutive' + `current_window_ms' keys.

**Unblocked but not yet picked up:**

- **Docker healthcheck doesn't detect a wedged connection-handling
  process** — confirmed twice now: `station-it-milan` (2026-08-13,
  see `FLEET.md`) and `station-de-frankfurt` (2026-09-03). Both
  times the container stayed `healthy` — process alive, socket
  still bound — while the station was fully dead on the mesh side.
  2026-09-03: Frankfurt logged 8,580 `puzzle_invalid` rejections
  from colocated `macula-realm` (its 4 unaudited
  `macula_station_link:start_link/1` call sites were missing
  `identity:`, fixed in `macula-realm` `2368929`) driving repeated
  `peering_router pathological` tripwire trips with escalating
  peak cost (13.7k → 438k reds/s across ~17 h) until the process
  went completely silent — zero log output for the ~1h44m until
  discovered, recovered by a redeploy. The current healthcheck
  needs to probe actual QUIC/mesh liveness (e.g. a lightweight
  self-dial, or a "last successful handshake" staleness check),
  not just process/port presence. Trigger: "Start station
  healthcheck liveness probe".

- **`geo_check` + rich DHT observe spec** — replace `asn => 0,
  country => <<"??">>' defaults in
  `macula_station_peer_observer' with live geolocation lookup.
  Needs a GeoIP database (MaxMind GeoLite2 — free but account +
  mmdb download) OR a free HTTP-API lookup with local cache.
  License + operational choice is the blocker, not code.
  Trigger: "Start geo_check integration".

**Cross-repo — blocked on SDK change:**

- **SWIM `leave` frame type** — schema extension in
  `macula_frame' (in `macula-io/macula@v2') so graceful
  shutdown can advertise departure explicitly instead of
  relying on SWIM suspicion + timeout. Needs a commit to the
  SDK repo + a dep-SHA bump here. Small (~20 LOC in the SDK,
  wire-up in `macula_station:shutdown/1' + observer dispatch).
  `REALM_LEAVE' is NOT included — realm concerns live in the
  future realm service post-§8.4 reversal. Trigger: "Add SWIM
  leave frame".

**Shipped in Sprint D (2026-04-15):**

- ✅ **`test/fleet_chaos.erl` helper module** — kill_pid/1,
  stop_peer/1, pause/1, resume/1, wait_until/2, wait_alive/3,
  wait_confirmed_failed/3, member_state/2. Consumed by
  `fleet_SUITE' (both scenarios refactored to use it). 6 eunit
  cases cover each primitive in isolation. Foundation for
  Phase 7.2 / 7.3 scenarios to compose.

### 8.2 — admin API (Phase 7 hardening)

- **`POST /bootstrap/add-peer`** — rare operator-debug endpoint
  for peer injection without a cascade.
- **`GET /metrics`** — Prometheus text-format exporter; wants a
  proper counter registry. Lands alongside the Grafana dashboard
  in Phase 7.
- **TLS + client-cert auth** on the admin listener. Loopback-only
  today.
- **Unix-socket bind** (`/tmp/hecate-admin.sock`) — needs
  `gen_tcp' AF_UNIX support or a port-driver bridge.

### 8.3 — fleet / real-network (beam-cluster work)

- **4-node beam-fleet CT** — iptables partition / heal, gatewayed
  tier diversity, podman auto-update observation.
  `fleet_SUITE' covers the code paths via 2 `peer' VMs. The
  4-node scenarios need real hardware + ops tooling.
- **End-to-end warm-boot round-trip test** (cold boot → observe
  peers → restart → assert DHT pre-seeded from cache on the new
  process).
- **Two-station tombstone-reach CT** — sender shuts down, reader's
  DHT learns the tombstone via replicate walk.
- **Tier-B mDNS responder real-network test** — needs live
  responder.
- **Automated report drop** to `/bulk0/.hecate/reports/$(date +%F)/' —
  post-run `scp' step in the fleet script.

### 8.4 — design reversal (no follow-up on station)

Session 8.4's per-realm HyParView + Plumtree on the station was
reversed in Sprint A (2026-04-15). Stations are realm-agnostic
infrastructure; realm state moved to a new `hecate-realm' /
`macula-realm' service. See §6 (adjacent plans) for the service
plan, and `PLAN_STATION_INTEGRATION.md §8.4' for the reversal
record.

### 8.5 — runbook / operator-facing docs

- **`PLAN_STATION_RUNBOOK.md`** alignment with shipped reality
  (endpoint list, child tree, config shape, shutdown API,
  accessors). Last unchecked acceptance item in
  `PLAN_STATION_INTEGRATION.md §6'.

Full per-session breakdown lives in
`PLAN_STATION_INTEGRATION.md' — each `8.x' header ships a
`Landed:' + `Deferred:' pair.

### 8.6 — failing eunit tests on slower hardware (CI-non-blocking, 2026-05-02)

Self-hosted Forgejo Actions runner (`beam02-builder`, J4105 @ 1.5GHz)
surfaced 3 timing-sensitive eunit failures that did not appear on the
GitHub-hosted runners (Xeon-class, ~3-5x faster). Eunit was marked
non-blocking in both `.github/workflows/ci.yml` and
`.forgejo/workflows/ci.yml` (`rebar3 eunit || true`) so the rest of
the pipeline (compile, xref, dialyzer, image build + push) stays
green and unblocks the Tier-3 deploy.

**Failing assertions (run #5, sha `2266e6d`, 2026-05-02):**
- `?assert(Pred())` — predicate timeout
- `?assertEqual([<<65>>,<<66>>], Payloads)` — expected payload set mismatch
- `?assertEqual([<<"only-A">>], [P || {_,P} <- Replies])` — RPC reply filter mismatch
- Underlying smoking gun: `{error,wait_until_timeout, …}`

Smoking gun is `wait_until_timeout` — strongly suggests hardcoded
timeouts in helpers that need scaling for slow hardware OR genuine
race conditions exposed by slower scheduling.

**Trigger to re-block CI:**
1. Locate the helper (`wait_until_*/N` or similar) and parameterise
   timeout via env (`MACULA_TEST_TIMEOUT_SCALE` default 1.0, set to
   3.0 on beam02 runner via the `runner.envs` config).
2. Re-run the suite locally on a J4105 to confirm green.
3. Drop the `|| true` from both workflow files.
4. Squash this section out of `PLAN_DEFERRED_WORK.md`.

---

## 9. Items consciously <em>not</em> on this list

For the avoidance of doubt — these are <em>design decisions</em>,
not deferred items:

- Transitive realm federation (A-B + B-C does NOT imply A-C) — Part 5 §10.3.
- Foundation-as-unilateral-authority — Part 5 §13.1 explicitly rules out.
- Data-model migration from V1 — Part 7 §2: none, fleet runs both
  until Phase 8 cutover.
- Content moderation at foundation level — Part 5 §13.1 OOS; realms
  self-govern.

---

## 10. Review cadence

This file gets a pass <b>at the start of every phase transition</b>
(Phase 6 → 7, Phase 7 → 8, V2.0 → V2.1). Items that remain deferred
after their trigger event has passed require either:
- an updated blocker (with the new reason), or
- promotion to an active session, or
- explicit acknowledgement that the item is dropped (move to `Dropped'
  section with reason).

No `Dropped' section today — every item above has an owner and a
path to done.
