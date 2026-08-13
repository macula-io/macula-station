# Wire-liveness tripwire — a station that has stopped moving packets must say so

**Status:** Planning
**Created:** 2026-08-13
**Last Updated:** 2026-08-13
**Classification:** BUILD, not CLAIM. It makes no assertion about the world; it
is plumbing. Tests and a commit, no adversarial gate.

> This exists so that the next station to die is noticed by a machine within
> five minutes instead of by a human three days later.

---

## 1. The incident this is answering

`station-it-milan`, 2026-08-13, served nothing for **30 hours** while every
signal it published read green.

| what was checked | what it said |
|---|---|
| container healthcheck (`curl -sf http://127.0.0.1:8443/status`) | **healthy**, every 30 s, for 30 h |
| `macula_station_listener:stats/1` | `connected => 54, handshaking => 0, rejected => 0, cap => 1000` |
| listener process | alive, mailbox 0, bound to the right address |
| `peer_observer:conns/1` | 54 entries |
| box | up 128 days, no firewall, `beam.smp` holding `[2600:3c0b::…]:4433` |
| **tcpdump, inbound** | **every QUIC Initial ARRIVING** |
| **tcpdump, outbound** | **zero packets in 25 s** — no reply, no keepalive to paris |
| `conns` entries with `outbound` set | **0 of 54** |
| `connected_hostnames()` | **`[]`** |

A reboot cleared it: 7 conns, 1 outbound, 1 verified peer, same node id.
Membership converged ~20 min later.

**Why nothing fired.** Every liveness signal a station publishes is derived from
BEAM state that a dead transport does not disturb, so no counter disagreed with
any other counter. The only two things that disagreed were **the station and the
network**, and nothing compares those two.

### Two facts that make this worse than a missing check

**The healthcheck could not have caught it, and nothing would have acted if it
had.** Verified live on the milan box:

```
Healthcheck: curl -sf http://127.0.0.1:8443/status   30s interval, 3 retries
RestartPolicy: unless-stopped
Watchtower Args: []
```

The check is a **loopback HTTP call to the station's own admin port** — it
proves the BEAM is alive and Cowboy is listening and asks nothing about QUIC.
And `unless-stopped` restarts on **exit**, not on **unhealthy**: Docker never
restarts an unhealthy container the way a Kubernetes liveness probe would.
Watchtower runs with no args and only chases registry digests. So turning the
healthcheck red is **necessary but not sufficient** — the plan must name who
acts (§5).

---

## 2. The finding that shapes everything: the wire is not observable today

**Every send-side quantity in this stack is a statement of intent, not of
transmission.** A check built on any of them would have been green for all 30
hours.

| candidate signal | why it is useless | verified |
|---|---|---|
| `macula_quic:getstat/2` | returns `{ok, [{S, 0} \|\| S <- Stats]}` — **hardcoded zeros** | `macula/src/peering/macula_quic.erl:316` |
| `macula_peering:send_frame/2` return | a `gen_statem:cast`; returns `ok` to a dead pid | — |
| `route_pubsub_frames` `forwarded` counter | increments on `count_send(ok)` where ok is that cast. **Climbed for the whole outage** | — |
| `macula_transport` | exposes no stats, counter or timestamp at all | `apps/macula_transport/src/macula_transport.erl` |
| only last-activity timestamp in the system | `last_inbound_at`, receive-only, private record field, per-link | `macula_station_outbound_link.erl:108` |

`getstat/2` is the dangerous one. Its own doc comment excuses the zeros as
"harmless (dist_util only uses these for liveness signals)" — which is exactly
the use case a hardcoded zero destroys. **It is a trap laid for precisely this
fix**: wire a monitor to it and you get a plausible, permanently-flat reading
forever, and "counters flat" becomes indistinguishable from "counters
unimplemented".

### What is observable

The kernel's own view of the one socket we own. Verified on live station
`station-se-stockholm`:

```
NetworkMode: host
  sl  local_address                     remote_address   st tx_queue rx_queue ... drops
  85: 093C…B6FE:1151 00000000…:0000 07 00000000:00000000 ...    0
```

One unambiguous row on port `0x1151` (4433), with `tx_queue:rx_queue` and
`drops` readable. This is **off-BEAM** — it is the kernel disagreeing with the
application, which is the comparison nothing was making.

---

## 3. The invariants

No single observable is both false at milan and true in every legitimate state,
so there are two rules on one shared tick with **disjoint escalation**.

### I1 — `ingress_stall` (primary, can act)

> While the kernel is holding undelivered datagrams on this station's own
> listener socket, the station's dispatched-frame counter must advance.

```
NOT ( rx_queue > 0
      AND rx_queue non-decreasing across k consecutive samples
      AND delta(frames dispatched) == 0 across the same window )
```

| state | I1 |
|---|---|
| **milan** | **FALSE for 30 h** — Initials arriving, `handshaking = 0`, no `{quic, new_conn}` ever delivered, queue filling, nothing dispatched |
| boot | queue drains from the first tick — antecedent false |
| idle | `rx_queue = 0` — antecedent false |
| inbound-only leaf | works fully; this is the only rule that covers a leaf |
| **partition** | **structurally silent** — no inbound traffic means `rx_queue = 0`. Load-bearing for §5 |
| CPU starvation | a starved-but-alive endpoint driver still drains, with millisecond lag, not 5 minutes of monotone backlog |

### I2 — `outbound_futility` (secondary, reports only, never acts)

> If `macula_station_outbound_links_sup` is alive with N≥1 children, at least
> one outbound link must reach verified (`peer_node_id` set) within any 300 s
> window.

False at milan (1 configured peer, `connected_hostnames() == []`, 30 h). Gate on
`whereis/1` so a leaf with no configured peers disarms it rather than trips it.

**I2 is exactly what a real partition produces fleet-wide**, which is why it
reports and never acts.

---

## 4. Where it lives

| piece | home | why |
|---|---|---|
| both rules | `macula_station_health_publisher`, **riding the existing `?TICK_MS` 10 s tick** | already the station's only shipped tripwire; already owns prev-sample deltas, per-label strike counters and the rising/falling edge machine. A new gen_server would be a horizontal monitor layer and would itself be a thing that can die silently |
| procfs parsing | **inline**, private, exported under `-ifdef(TEST)` | precedent: `macula_station_announcer:288` reads `/proc/meminfo` inline with a non-Linux fallback. A `macula_station_socket_stats` module reads as a `utils/` layer |
| dial counters | `macula_station_outbound_link` | vertical slicing — the counter lives with the probe that produces it |
| readout | `macula_station_admin` | new `GET /wire`; `wire` sub-map on `/status`; **`healthy` untouched** (it is the current HEALTHCHECK target and documented operator-tool compatible) |

---

## 5. What happens when it fires

| rung | action | honest value |
|---|---|---|
| 0 | always increment and expose `wire_checks_ran` | a detector with nothing to detect and a detector that cannot fire look identical from outside. Non-negotiable |
| 1 | edge-triggered `?LOG_ERROR` on the rising edge, `?LOG_NOTICE` on falling, plus `_macula.peering.wire_stalled` / `_macula.peering.outbound_futile` diagnostics | **close to zero on its own.** Nobody read milan's logs for 30 hours. This rung is for the post-mortem, not the fix |
| 2 | `GET /wire` returns 503 when stalled; Dockerfile HEALTHCHECK retargeted `/status` → `/wire` | flips a column in `docker ps` and makes the fault visible to fleet scripts. **Necessary, not sufficient** — see §1 |
| 3 | `erlang:halt(70)` on **I1 only**, after 30 consecutive stalled samples (5 min) | the only rung that actually restores service, given `unless-stopped` restarts on exit |

**`halt/1`, not `application:stop`.** `init:stop` runs `prep_stop/1` →
`macula_station:prepare_shutdown/1`, which publishes a **signed tombstone** for
this station's own node record. On a true positive that tombstone cannot leave
the box anyway; on a false positive it evicts a healthy station from the mesh
ahead of TTL — the false alarm causes the outage it was diagnosing.

**Guards on rung 3**

- config `wire_stall_action :: log | halt`, **default `log`**. Ship stages 1-2
  as log; flip per-station only after the tripwire has run on the fleet without
  a false rising edge. That answers the standing "defer until it has caught or
  missed a real incident" ruling with data instead of argument.
- at most one self-halt per 6 h, persisted to `<data_dir>/wire_stall_halt.stamp`
  and read at init. A restart that does not fix it must not loop.
- **precondition:** the restart policy lives in `macula-demo`, not here.
  Confirmed `unless-stopped` on milan. `halt` without it is just a dead station.

### The partition restart-storm objection, answered

**I1 cannot storm — structurally, not by tuning.** Its antecedent is "the kernel
is holding undelivered datagrams for MY socket". A transit or DNS partition
removes inbound traffic, `rx_queue` goes to 0, the antecedent is false, and I1
is silent on every box simultaneously. It is a local-fault detector by
construction and a network event cannot trigger it. That is precisely why I1 and
not I2 gets the halt: wiring halt to I2 would convert a self-healing network
event into seven simultaneous cold boots at 10-20 min of reconvergence each.

**This repo has already been burned twice by silence detectors**, and the
difference matters:

| prior detector | outcome |
|---|---|
| `peer_observer` conn-aging sweep | deleted — closed healthy conns, since the 15 s QUIC keepalive keeps app-idle conns fine |
| `outbound_link` `SILENCE_THRESHOLD_MS` probe | false-fired under CPU starvation 2026-07-24, tore down healthy links into a handshake storm, 20-min delivery gap |

Both measured **inbound silence**, which starvation forges. I1 measures a
different quantity: not "we saw nothing" but **"the kernel saw something and we
did not read it"**. Starvation does not forge that.

*Residual risk accepted:* a genuine 5-minute whole-VM scheduler pathology with a
full kernel queue and zero dispatched frames will halt. That station was not
serviceable either way.

### Thresholds

| define | value | derivation |
|---|---|---|
| `WIRE_STALL_SAMPLES` | 30 (5 min at the 10 s tick) | healthy stations drain sub-millisecond; milan held it for 10,800 samples — **360× headroom** |
| `WIRE_RX_FLOOR_BYTES` | 1 | any non-zero backlog counts, but only combined with non-decreasing across the window, so a draining queue never trips |
| `OUTBOUND_FUTILE_MS` | 300 000 | 30 s handshake timeout + 60 s `?MAX_BACKOFF` ≈ 90 s worst legitimate cycle; 300 s is 3.3 cycles. **Coupled to both** — raising either without this manufactures false futility |
| `HALT_COOLDOWN_MS` | 21 600 000 (6 h) | a cold station costs 10-20 min to reconverge (measured at milan), so 6 h caps self-inflicted downtime at ~5% even in a pathological loop |

---

## 6. Rejected signals, and why

Recorded so nobody re-proposes them.

| rejected | why |
|---|---|
| `/proc/net/snmp6` `Udp6OutDatagrams` delta | **the container runs `--network=host`** (verified). snmp6 counters are per-netns, so they count sshd, watchtower and DNS. Never flat → permanently green. **Worse than `getstat/2` because it looks measured** |
| `macula_quic:getstat/2` | hardcoded zeros (§2) |
| any send-site counter | counts casts, not transmissions. `forwarded` climbed through the whole outage |
| `macula_dht:size/1`, `min_viable_peers` | station entries are deliberately never forgotten on disconnect, so the routing table stays full through total isolation. Also `min_viable_peers = 8` is already below threshold on a 7-station fleet |
| `peer_links:connections/0` as the **trigger** | wraps `gen_server:call(_, _, 500)` in `catch _:_ -> []`, so a merely busy registry reads as zero peers. Corroboration only |

---

## 7. Testing

All decision logic pure and exported under `-ifdef(TEST)`; the gen_server
callbacks contain no decisions. This is `health_publisher`'s existing pattern
(17 pure tests, no mocks — there is no mocking library in this repo).

| id | case |
|---|---|
| T1 | `parse_udp6/2` against a **real kernel fixture** with a genuinely non-zero `rx_queue`. Three states: `{ok, …}`, `not_found`, `unavailable` — **`unavailable` must never collapse to 0** |
| **T2 RED** | **the milan replay, no network at all** — 30 synthetic samples `#{rx_queue => 8192, frames_total => 4711}`; asserts `ok` at 29 and `stalled` at 30. *This is the case that proves the check can fire* |
| T3-T5 | false positives: idle; busy with oscillating queue; slow drain with frames still advancing |
| T6 | `frames_total` goes backwards after `POST /telemetry/frames/reset` → clamp, no strike |
| T7 | procfs unavailable (macOS dev box) → verdict `unknown`, distinct from `ok`, no strike |
| T8 | `outbound_futility/2`: sup absent → ok (leaf); 290 s → ok; 310 s → futile; `stats/1` timeout → `unknown`, never `futile` |
| T9 | the ladder as a pure function, including the halt decision. **The halt path is never tested by actually halting** |
| T10 CT | **end-to-end RED without a fleet**: `gen_udp:open` with a tiny `{recbuf, 2048}`, `{active, false}`, blast it and never read, point the check at that port, freeze the telemetry stub, drive 30 ticks → asserts the rising edge and the `/wire` 503 |

**Hard requirement:** every test verified RED with its fix reverted before any
green is believed, and the count stated in the commit body.

---

## 8. Staging

| # | repo | title form | scope |
|---|---|---|---|
| **1** | macula-station | *A station that cannot read its own socket must not report itself healthy* | I1 + `/wire` + HEALTHCHECK retarget + `wire_checks_ran`. `wire_stall_action` defaults to `log`. **No halt.** Delivers essentially all the value; no NIF, no cross-repo |
| **2** | macula-station | *A dial that never succeeds must cost a counter, not silence* | `dial_failures`, `unverified_since`, `last_dial_error` + `stats/1`; `relay_ping`'s discarded `{error,_}` branch counts and publishes; I2 rule. **Observation only — no forced reconnect** |
| **3** | macula-station | *Enable the action* | `wire_stall_action => halt`, 6 h stamp, `halt(70)` on I1 only. **Gated on 1-2 running clean on the fleet** |
| **4** | macula-realm | *Absence is the only thing a mute station can say* | one ETS fold on `updated_at_ms` against a station roster → `stale`. The number that would have shown milan going dark **was already being computed and displayed for 30 hours with no threshold on it** |
| **5** | macula (SDK) | *Ask quinn what it actually sent* | `nif_connection_stats` → full `ConnectionStats`; make `getstat/2` return `{error, not_implemented}` or delete it |

### Why commit 5 is cheap, and optional

`nif_max_datagram_size` (`native/macula_quic/src/connection.rs:267`) **already
calls `conn.connection.stats()`** and throws away everything but
`path.current_mtu`. Surfacing the rest is an extension of a working function:
~25 lines in `connection.rs`, one in `rustler::init!`, ~5 each in
`macula_quic.erl` and `macula_transport.erl`. No new dependency, no new resource
type. It gives `udp_tx{datagrams,bytes}`, `udp_rx{…}`,
`path{rtt, lost_packets, black_holes_detected}` — and **`udp_tx.datagrams`
frozen while `udp_rx.datagrams` climbs is the milan signature exactly**, at
per-connection granularity.

It is optional because it is per-connection: it would have caught the 54 wedged
inbound conns and would **not** have caught the missing keepalive to paris.

---

## 9. What this does NOT cover

Stated plainly so the residual is owned rather than discovered.

1. **Egress-only blackhole with healthy ingress.** I1 is silent (the socket
   drains). I2 catches it only where outbound peers are configured, and only as
   a report. **A leaf with dead egress is covered by nothing here.**
2. **The outbound client endpoints.** `CLIENT_ENDPOINT_V6` / `_V4` are
   process-global Rust statics bound to `[::]:0` / `0.0.0.0:0` with **no Erlang
   handle and ephemeral ports**. The keepalive to paris that stopped is
   invisible to this design at every rung — not a missing counter, a structural
   blind spot.
3. **Non-Linux hosts.** Verdict `unknown`, never `stalled`. I1 does not exist on
   macOS; a dev box gets I2 only.
4. **A container not sharing the host netns.** `--network=host` is what makes
   the procfs row attributable. If that changes, I1 needs revisiting.
5. **A whole-VM freeze that stops the tick.** The check dies with it. Nothing
   in-node can cover this — only commit 4 can.
6. **Root cause.** This detects and does not explain. Why milan's tokio runtime
   stopped servicing both sockets is unanswered; commit 5's
   `nif_runtime_metrics` (tokio `num_alive_tasks`, `global_queue_depth`, both
   stable API) is the instrument that would confirm or refute an
   accept-task-leak.

---

## 10. Success criteria

- [ ] T2 and T10 both observed RED before their fix exists
- [ ] a synthetic milan (full kernel queue, frozen frame counter) trips `/wire`
      to 503 within 5 minutes, on a dev box, with no fleet
- [ ] the tripwire runs on all seven stations for **two weeks with zero false
      rising edges** before commit 3 is considered
- [ ] `wire_checks_ran` is non-zero on every station, so a silent detector is
      distinguishable from a working one
- [ ] `station-topology-audit.sh` and `macula-e2e/scripts/station-joined.sh`
      agree with `/wire` on a station deliberately wedged

---

## 11. Open questions for Raf

1. **Rung 3 at all?** A station that halts itself is a station that can
   flap. The alternative is leaving it at rung 2 and adding something that
   restarts on unhealthy (e.g. an autoheal sidecar) in `macula-demo` — which
   moves the decision off the station and out of this repo. My recommendation is
   rung 3 with the 6 h stamp, because it needs no new fleet component and I1
   provably cannot storm.
2. **Commit 4 crosses a standing ruling.** `BRAINSTORM_CONTINUOUS_SELF_DIAGNOSIS.md`
   §7 ruled **local-only** on 2026-07-25 — before the frankfurt (08-05) and
   milan (08-13) incidents. Milan is new evidence that a mute station cannot
   report its own muteness. I read that as a re-opening on evidence rather than
   a re-litigation, but it is your call.
3. **Commit 5 now or later?** It is the only path to a true send-side signal and
   it is genuinely cheap, but it touches the SDK and forces a station dep bump
   on top of the pending `~> 7.1` → `~> 8.0` move.
