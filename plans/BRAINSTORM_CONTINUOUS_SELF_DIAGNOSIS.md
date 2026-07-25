# BRAINSTORM: continuous self-diagnosis (and the self-healing trap)

**Status:** foundational reflection. NOT a design. For adversary attack on the premise.
**Date:** 2026-07-25
**Provoked by:** the idea that a station should have a lightweight, permanent mechanism to
evaluate its own and its neighbours' health -> a self-diagnosing, maybe self-healing mesh.

> **CORRECTION (Fable, verified in code 2026-07-25):** §0's original claim — "the router's
> 1.17M reds/s WAS in this feed the whole time, nobody watched" — is FALSE. The beacon
> publishes `process_info(Pid, reductions)` RAW: the cumulative lifetime counter, not a
> rate (`health_publisher.erl:117`), from a STATELESS sampler (no previous sample kept). A
> monotonically-growing uninterpretable integer was on the wire, never a rate. The sole
> consumer OVERWRITES the last snapshot (`load_monitor.ex:82` `:ets.insert`), so rate is
> underivable there too, and NOTHING renders `reductions` anywhere in the realm web app
> (grep: zero hits). **The loop was not "open." The SENSOR is broken** — wrong units, and
> the one pathological field is unrendered. This reorders the whole brainstorm: the
> highest-value fix is to fix the sensor, not to add DIAGNOSE/NEIGHBOUR/ACT stages
> downstream of a beacon that emits garbage. See the full verdict in §7.

## 0. The beacon already exists, and that IS the lesson

The station already runs the lightweight permanent self-health mechanism being asked for:

- **`macula_station_health_publisher`** broadcasts, every 10s on `_mesh.health.v1`, the
  **mailbox + heap + reductions of its most load-bearing processes**. ~~The peering_router
  at 1.17M reds/s that melted the fleet for months WAS in this feed the whole time.~~
  (FALSE — see correction above: only a raw cumulative counter was, never a rate.)
- **SWIM** (`macula_swim`) gives neighbour LIVENESS (alive -> suspect -> confirmed_failed
  via signed ping/indirect-ping/gossip, 2s period).
- **relay_ping** gives neighbour RTT every 30s (the latency map).

So the mesh is not missing a health beacon. What actually happened:

- `_mesh.health.v1` is consumed by exactly ONE thing: a realm-side dashboard
  (`load_monitor_subscriber.ex` -> `load_monitor.ex` -> a LiveView). No station consumes
  another station's beacon. There is NO threshold, NO alert, NO anomaly detection, NO
  autonomic response anywhere in the codebase (verified).
- The router's pathological reduction rate was published every 10s for months and changed
  nothing, because nobody watched the dashboard and nothing acted on the number.

**The gap is not the beacon. The loop is open.** Published != observed != diagnosed !=
acted. This is the same disease as `/vigil`'s attacker count pinned at 400 and macula.io
melting under a metric that was on screen: a signal with no closed loop is theatre.

## 1. What self-diagnosis should actually add (the OBSERVE->DIAGNOSE->ACT loop)

Three missing stages, in order of safety:

1. **DIAGNOSE (missing).** Turn the raw beacon into a verdict. Local thresholds / simple
   anomaly on the signals the beacon already emits (per-process reductions, mailbox, heap),
   PLUS the ones that specifically predicted this session's failures and are not yet
   sampled: scheduler utilisation (`scheduler_wall_time`), subscription-set size (the
   unbounded-growth leak), and outstanding-timer / process counts (the timer ratchet). A
   station should be able to say "my router is pathological" from its own data.
2. **NEIGHBOUR-AWARENESS (missing).** Today only the realm consumes `_mesh.health.v1`. A
   station could subscribe to its DIRECT neighbours' beacons and build a local view: "the
   peer I route through is degrading." SWIM already gives liveness; this adds behavioural /
   resource health, which SWIM never sees (a box at load 20 still answers SWIM pings right
   up until it dies -- SWIM is a lagging binary indicator, resource health is the leading
   one).
3. **ACT (missing, and where the danger lives -- see 2).**

## 2. Self-HEALING is the trap this whole session was a graveyard of

The seductive half of the idea is "pre-emptive self-healing." The session's evidence is
that automated remediation under uncertain diagnosis is precisely where this system keeps
shooting itself:

- The **silence probe** (an app-level self-healing reflex: detect silence -> force
  reconnect) FALSE-POSITIVED under load and caused reconnect storms.
- **Reconnect backoff / crash-restart under starvation** amplified load: a starved node
  restarting is equally starved, and the restart is itself a load spike.
- Every automated remediation tried either did not help or made things worse.

The through-line: a remediation that cannot distinguish "I am slow" from "the peer is
dead", or "transient spike" from "real pathology", will act on noise and cause churn. The
mesh already has open bugs (multi-hop self-heal) that are exactly failed automated
convergence.

So **"pre-emptive self-healing" must be reframed**. The safe autonomic responses, in
increasing risk:

- **Alert (safe).** Fire a loud, signed `station_degraded` fact when local diagnosis trips.
  Close the loop that was open. A human or a very conservative controller acts. This alone
  would have caught the router leak months early.
- **Self-throttle / shed (safe-ish, IS graceful degradation).** When saturated, the station
  does LESS: shed sheddable work (the load-shedding from the earlier design), lengthen its
  own reconcile cadence, stop advertising non-essential capability. Doing less is the one
  autonomic response that cannot cause a storm.
- **Soft de-preference of a degrading neighbour (risky).** Lower a degrading peer's routing
  weight GRADUALLY; back off traffic toward it. Never a hard re-route (that is the churn
  trap). And only on a SUSTAINED, not transient, neighbour-degraded signal.
- **Automated reconnect / restart / re-route / teardown (FORBIDDEN by this session's
  evidence).** This is what "self-healing" usually means and it is exactly the reflex that
  caused every incident here. Do not build it.

The honest reframe: **self-DIAGNOSIS yes; self-THROTTLING yes; self-HEALING (automated
remediation) no.** Heal by telling someone, and by doing less -- not by tearing down and
rebuilding.

## 3. "Lightweight" is make-or-break, and self-referential

The mechanism must be genuinely cheap, because CPU burn is the disease it exists to catch.
A diagnosis loop that itself becomes a hot loop is the exquisite irony this codebase would
actually commit (see: the peering_router). Constraints:

- **Extend the existing 10s beacon, do not add a subsystem.** The sampler exists; add the
  missing signals (scheduler util, sub-set size, timer count) to `build_payload`, and add a
  cheap local threshold check on the same 10s tick.
- **No per-message work, ever.** Diagnosis samples on a timer; it never touches the hot
  path. `scheduler_wall_time` and a handful of `process_info` calls per 10s is free.
- **Neighbour consumption is bounded** to DIRECT peers (a station already knows them), not
  the whole mesh -- O(peers), not O(N).

## 4. The deeper question: in-mesh vs external observability

Standard practice is external: node_exporter + Prometheus + Alertmanager, battle-tested,
off-the-shelf. Why reinvent it in-mesh? The honest case for in-mesh:

- Macula is a federated commons: independent operators each run a node; there is no central
  Prometheus and mandating one re-centralises the thing. Each node self-diagnosing and
  gossiping health to neighbours matches the decentralised model.
- The richest signals (a specific gen_server's reduction rate) are BEAM-internal and
  already in-process; a station knows itself better than an external scraper.

But the honest case AGAINST: the DIAGNOSIS logic (what threshold means "pathological") is
the same hard problem either way, and it is unsolved -- 1.17M reds/s looks like "busy", not
"broken", without a baseline. In-mesh does not make the hard part easier; it just avoids a
central server. Maybe the right answer is external observability for the OPERATOR view PLUS
minimal in-mesh self-throttling for the autonomic view, and NOT a full in-mesh diagnosis
engine.

## 5. Hard questions for the adversary

1. Is thresholding even a valid diagnosis? The fleet melted and NO static threshold on
   reds/s would have cleanly separated "pathological router" from "legitimately busy relay"
   without a per-process baseline. Is the diagnosis problem actually tractable cheaply, or
   does credible anomaly detection need history/baselines the "lightweight" constraint
   forbids?
2. Neighbour-awareness feedback loops: if every station de-prefers degrading neighbours,
   does a transient spike on one hub cause a mesh-wide stampede of traffic AWAY from it
   (making healthy neighbours degrade, cascading)? Is neighbour-reactive routing a new
   correlated-failure mechanism, exactly like the ones we keep finding?
3. Is self-throttling actually safe, or does a station shedding under a transient spike drop
   traffic it could have served, and if many shed at once (correlated load), does the mesh
   brown out precisely when it is needed? (This echoes the earlier correlated-shed doubt.)
4. Given the beacon already exists and was ignored, what makes an ALERT any less ignorable
   than the dashboard? Does closing the loop just move the failure from "unwatched
   dashboard" to "unactioned alert" / "muted pager"? Is the real fix organisational, not
   technical?
5. Does in-mesh self-diagnosis earn its keep over external node_exporter+Prometheus, given
   the diagnosis logic is the hard part and is identical either way? Is the commons argument
   enough, or is it reinventing a solved wheel worse?
6. Is "signed station_degraded fact on the mesh" a new abuse surface (a malicious node
   spamming degraded facts about others, or lying about itself to shed responsibility)?

## 6. What I believe, to be attacked

The user's instinct is right that continuous self-diagnosis is missing -- but the beacon is
not; the CLOSED LOOP is. The highest-value, lowest-risk step is to close the loop
conservatively: add the leading-indicator signals to the existing beacon, add a cheap local
threshold that fires a `station_degraded` alert, and add self-throttling (do less when
saturated). Neighbour-reactive routing and anything resembling automated self-healing are
where the session's evidence says we cause our own incidents, and should be resisted until
the observe->diagnose->alert loop has proven itself and the diagnosis logic is trustworthy.
Heal by alerting and by doing less; never by tearing down.

## 7. Adversary verdict (Fable, 2026-07-25): premise falsified, idea shrunk to a tripwire

**Objection 0 (fatal to §0's premise, verified):** the sensor is broken, the loop was
never open. Beacon publishes RAW cumulative reductions from a stateless sampler
(`health_publisher.erl:117`); consumer overwrites last snapshot (`load_monitor.ex:82`);
nothing renders it. A valid signal never reached anyone, so "the beacon was ignored for
months" (the whole motivation) is unproven, and DIAGNOSE/NEIGHBOUR/ACT would be towers on
sand. (Number hygiene: router comment says 1.7M reds/s, this doc says 1.17M — pin it.)

**On the author's 6 doubts:**
- **Q2, Q3, Q6 are the RIGHT doubts** — confirmed. Jointly they kill neighbour
  de-preference AND the mesh-wide `station_degraded` fact.
- **Q1 (thresholding) right in spirit, wrong in detail.** Split by process class:
  control-plane procs (router, bloom_exchange, dedup, fanout) are near-idle at steady
  state; the leak was ~34,000x baseline; a static per-process RATE threshold ("sustained
  > 10k reds/s") catches it with ~zero false positives — IF the unit is fixed to a rate.
  Data-plane procs (dispatcher, dht) genuinely do millions of reds/s; there absolute
  thresholds are noise and only trend/derivative works. "Lightweight forbids history" is
  false — keeping the previous sample for 8 procs is a few dozen integers.
- **Q4 asks the right question from a false premise** (no valid signal was ever shown).
- **Q5 resolved:** per-operator external (node_exporter/Prometheus) is federation-COMPATIBLE
  (sovereignty = each operator watches their own node; nothing re-centralises). Reserve
  in-node for a default-on tripwire, not a full diagnosis engine.

**Refutations that shrink the idea:**
- **Neighbour de-preference (§2 rung 3): refuted twice.** (a) No substrate —
  `macula_routing`'s edge-weight model has ZERO callers outside its own app; it is not
  wired into any live forwarding path. Nothing to "softly lower." (b) Even if built:
  degree-~3 topology + all neighbours reacting to the same 10s beacon on the same tick =
  coupled-controller oscillation (the silence-probe failure shape). No neighbour-reactive
  routing is safe in a few-hub mesh. DROP this rung.
- **Self-throttle "cannot cause a storm" is false as written.** "Stop advertising
  non-essential capability" under a flapping threshold IS an advertise/unadvertise churn
  generator (the peer_observer 116k-mailbox amplifier). Shedding also confuses endogenous
  burn (this session's bug) with exogenous load — shedding sheds the WRONG work when the
  node is burning internally, cutting service while the bug runs. Defensible only with
  hysteresis, only for retry-free provably-sheddable work, only AFTER the alert loop is
  trusted. Not step one. Also collides with the separately-designed DHT shedder (two
  shedders, independent thresholds, oscillate).
- **§2's forbid-list is correct and hard-won** (reconnect/restart/reroute/teardown).
  SWIM-is-lagging-binary confirmed. Beacon wire cost is O(direct peers)/10s, dwarfed by
  SWIM — NOT a scaling worry (conceded to author).
- **Missing doubt:** self-throttle creates a DIAGNOSTIC BLIND SPOT — throttle, load drops,
  pathology hides, real bug never found. This session was saved precisely because load-20
  on an idle box kept screaming. Masking is a real cost of "do less."
- **The core session mistake, repeated:** the failures were plain BUGS (timer leak, sub
  leak). No health mechanism would have PREVENTED them, only narrated them — and the
  narrator already existed and failed at its one job because of a units bug. Fix the
  sensor, don't build three stages behind it.

**Smallest correct version (supersedes §6's plan):**
1. **Fix the sensor.** Publisher keeps the previous sample, publishes `reds_per_s` (delta
   over the 10s tick) alongside mbox/heap. ~15 lines in `build_payload`.
2. **Render it** in the load tab (the rate, per proc).
3. **One in-node tripwire**, same 10s tick: control-plane procs sustained above a
   per-process RATE threshold for N ticks, OR mbox monotonically growing across N ticks ->
   log ERROR loudly + optional operator webhook. **Local only.** No mesh fact, no neighbour
   consumption, no throttle, no de-preference.
4. Optionally expose the same numbers at `/metrics` for operators who run their own stack.

Everything beyond that (neighbour-awareness, ACT rungs) is DEFERRED until the tripwire has
caught or missed a real incident. The "watch for UNBOUNDED GROWTH" instinct is right for
the leak class; the first implementation is "fix units + rate threshold on control-plane
procs," not a growth-detector (timer-count has no public `process_info` API; sub-set-size
sampling hits the documented `sys:get_state` 700ms-under-load trap).
