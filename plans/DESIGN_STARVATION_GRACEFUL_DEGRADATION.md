# DESIGN: graceful degradation under CPU starvation

**Status:** DESIGN gate, pre-implementation. No code. For adversary review before any is written.
**Date:** 2026-07-25
**Supersedes:** a reverted `put_record` call→cast change that was wrong (broke the daemon
read-after-write ack, didn't fix the announcer crash, mis-stated the mechanism).

---

## 0. The incident, and the ONE claim that survived verification

2026-07-24: a ~20-minute mesh delivery gap, caught by a sequence-numbered capture pair.
Both endpoint stations sit on one physical box (Hetzner Nuremberg, **2 CPU cores, 3
stations**, macula 5.1.0). At 16:10:33 UTC both stations on that box had a synchronized
QUIC accept-handshake failure storm (25 + 48 failures, same second); a peering link
dropped; delivery resumed on its own ~20 min later. The Paris midpoint was unaffected.

What is **proven**: box-wide CPU starvation on a 2-core box.

What was **retracted** (do not rebuild these): that a synchronous DHT `put_record` stalled
a peer link's frame loop (it doesn't — the observer spawns a worker per CALL,
`macula_station_peer_observer.erl:~717`); that an announcer crash-loop tripped supervisor
restart intensity (arithmetically impossible: 5s/crash vs intensity-5-in-10s).

---

## 1. The mechanism, grounded in code this time

Two facts change the whole design:

**1a. QUIC keepalives are generated in the Rust/tokio NIF, not by a BEAM process.**
`macula_quic.erl:106-112`: `keep_alive=15s`, `idle_timeout=300s`. So a BEAM-side
`process_flag(priority, high)` does NOT protect keepalives — and on a 2-core box, boosting
BEAM priority would *starve the tokio threads further*. **Priority-boosting is the wrong
lever and is dropped.** (This was going to be my headline fix. It is a red herring.)

**1b. The link drop is a BEAM-side liveness probe firing on false silence.**
`macula_station_outbound_link.erl:63-64`: `SILENCE_THRESHOLD_MS = 300_000`,
`SILENCE_CHECK_MS = 60_000`. Every 60s the link checks how long since it *processed* an
inbound frame; past 5 min it force-closes and reconnects (`:322-324`,
`maybe_force_reconnect_on_silence`). The probe exists for a real bug: a half-open where
QUIC ACKs keepalives but the peer's app-level `peering_conn` worker is dead, silently
breaking cross-station pubsub for hours (the moduledoc at `:50-62`). Its own comment
concedes the false-positive: *"a peer briefly dropping all traffic (long GC, IO storm)
gets reconnected. Acceptable — reconnect is cheap."*

Under CPU starvation that "acceptable" assumption inverts three ways:

1. `last_frame_at` staleness now means **"I wasn't scheduled to process frames"**, not
   "the peer went quiet." The probe misreads our own starvation as the peer's death.
2. **Reconnect is not cheap during a handshake storm.** The force-close lands the link
   into exactly the "accept handshake failed: timed out" storm, so it can't re-establish.
   Backoff is 1s→60s exponential (`:641-642`), so a few failed attempts strand the link
   for many minutes — the ~20-min gap.
3. So the station, *because* it is overloaded, **manufactures more work for itself
   (teardown + reconnect) at the exact moment it can least afford it.**

**This is the core defect, and it generalises: under load the station AMPLIFIES its own
load.** The silence probe reconnects healthy links; the announcer crashes and restarts and
re-runs its DHT fanout; retries pile up. A transient CPU spike becomes a self-sustaining
outage. Graceful degradation is the opposite reflex: **under load, do LESS — shed, back
off, and never tear down what you cannot cheaply rebuild.**

---

## 2. The design principle

For the endgame (one station per box, thousands of tiny nodes each near its limit), a
starved node is the steady state. The property that matters is not "never starve" — it is
**"when starved, degrade to slower/staler, never to tearing yourself down."** Concretely,
a starved station should shed sheddable work and hold its peer links, ending up behind on
DHT freshness and presence, not disconnected.

Erlang supervision does not give this. Supervision recovers from *crashes*; starvation is
not a crash, and restarting a starved process yields an equally starved process. The
levers are flow-control and load-regulation, and every one must leave a **visible signal**
— "graceful degradation means the system knows it is degrading."

---

## 3. The changes, each targeting one self-amplification path

### A. Make the silence probe distinguish "peer dead" from "I was slow" (the direct fix)

The probe passively times out on *processed-frame* silence, which starvation forges. Make
it **actively confirm death before force-closing**: on silence, send an application-level
`_relay.ping` (the RPC already exists, `macula_station_relay_ping.erl`) and only
force-close if the ping *also* fails.

- Peer alive but link was quiet, or we were merely slow → ping succeeds → **do not
  tear down.** Kills the false-positive.
- Genuine half-open (peer's app worker dead) → ping fails → force-close. **Preserves the
  bug the probe exists to catch.**

Tension to resolve at review: under starvation the ping round-trip may itself time out
because *we* are slow to send/receive. So a ping timeout must be interpreted against a
load signal (§D): ping-timeout under high local load = inconclusive, defer; ping-timeout
under normal load = real dead peer, force-close. Without §D this fix trades one
false-positive for another.

### B. Shed sheddable DHT work under mailbox depth

`macula_dht_server` is the shared serialization point. Add a `message_queue_len` check
(`erlang:process_info(self(), message_queue_len)` or `process_flag(message_queue_data,
...)` sampling) on the ingress of **sheddable** work — inbound STORE frames and
replication puts — and drop it above a high-water mark. Records are TTL'd and reconciled
by the periodic replication tick (`macula_dht_replicate`, default 1h), so a shed store
re-lands later. This lowers total BEAM load, indirectly relieving tokio.

- Sheddable: inbound STORE frames, replication puts, presence re-puts.
- NOT sheddable: reads (`find_local_record`, `find_value`), the daemon's own
  `_dht.put_record` (see §C — it gets a `busy` reply, not a silent drop).
- Every shed increments a counter and is sampled to a log/gauge (§D).

### C. Stop the announcer amplifying, and keep the daemon ack honest

Two distinct paths, two different fixes:

- **Announcer self-maintenance** (`publish_node_record`, called from `init/1` and the
  refresh timer): today it makes an *uncaught* synchronous `find_local_record` per live
  peer (`macula_station_announcer.erl:404`, via `peer_observer_hosts`) plus the self-put.
  Both must be non-crashing best-effort: a timeout means "skip peer metadata / skip this
  refresh," not "crash init and re-run the fanout on restart." Catch, log, move on. The
  record self-heals at the next refresh (75% of TTL) well before it expires. (This is the
  path the reverted change *claimed* to fix and did not — it async'd the put and left the
  find crashing.)
- **Inbound `_dht.put_record` RPC** is the **daemon read-after-write path** (its own
  moduledoc: "returns the moment the local store has succeeded"; replication uses
  `send_store` wire frames, not this RPC). It must NOT become a silent optimistic ack
  (the reverted change's bug). If §B decides to shed here under extreme load, it returns a
  structured **`busy`** error so the daemon backs off and retries — never a false `ok`.

### D. Make degradation observable (prerequisite, not an add-on)

A cheap load signal, sampled every few seconds: DHT mailbox depth, and shed/deferred-probe
counters, published on a mesh-internal topic and/or logged. This is what §A needs to
interpret a ping timeout, and what turns "the node silently degraded" into "the node
reported it was shedding." Without it, A–C are invisible and unfalsifiable.

---

## 4. Explicit non-goals (each is a trap already hit)

- **No `process_flag(priority, high)`.** Keepalives are tokio-side (§1a); boosting BEAM on
  a 2-core box starves the transport it is trying to protect.
- **No silent optimistic ack on the daemon RPC.** Breaks read-after-write; a cast to a
  dead DHT pid returns `ok` and loses data forever. Shed → `busy`, never fake `ok`.
- **No unbounded cast as a "fix."** Converting a blocking call to an unbounded cast moves
  the failure from "caller times out (recoverable)" to "mailbox grows → OOM → whole
  station dies (unrecoverable)," and unplugs the only overload alarm.
- **This is not a substitute for right-sizing.** In the current 3-stations-on-2-cores
  deployment the fastest relief is fewer stations per box. These changes are for the
  *endgame* (a single small node degrading gracefully under its own limit) and for
  robustness, not a licence to oversubscribe.

---

## 5. Sequencing

D → A → B/C. D first because A cannot correctly interpret a ping timeout without it, and
because shipping any of B/C without a degradation signal is shipping silent behaviour
change. Each is independently revertible.

---

## 6. Questions for the adversary

1. Is §A's active-ping-before-force-close actually robust, or does it just move the
   false-positive from "silence timeout" to "ping timeout," given both fail under the same
   starvation? Is the load signal (§D) a sufficient discriminator, or is ANY BEAM-side
   liveness probe fundamentally unable to tell "peer dead" from "I'm starved," making the
   whole probe the wrong layer (should half-open detection live in the QUIC/tokio layer
   instead)?
2. §B sheds inbound STORE frames above a mailbox watermark. Does dropping replication
   stores risk DHT records never converging on a chronically-loaded node — i.e. does the
   "TTL + periodic reconcile" safety net actually hold when the node is *always* shedding,
   or does shedding become permanent data loss on exactly the small nodes the endgame is
   built from?
3. Is measuring `message_queue_len` a reliable load signal, or is it cheap-but-misleading
   (a deep mailbox that drains fast vs a shallow one behind a slow NIF call)? Is there a
   better starvation signal than mailbox depth — scheduler utilisation
   (`scheduler_wall_time`), run-queue length?
4. Does fixing the silence probe (§A) just re-expose the original half-open bug it was
   built to catch, in a new disguise, on exactly the loaded nodes where half-opens are
   most likely?
5. Is the whole framing — "under load, do less" — right for a DHT, where doing less
   (shedding stores, skipping refreshes) degrades the *shared* routing/record substrate
   that OTHER nodes depend on? Does one node's graceful self-degradation externalise cost
   onto the mesh?
6. Given the endgame is 1-station-per-box, is any of this worth building now against a
   3-on-2 test box, or does it risk over-fitting the software to a deployment artefact
   (oversubscription) that the target topology removes?
