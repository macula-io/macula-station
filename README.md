# Macula Station

**Macula V2 reference station.** Private repository. Design phase — not yet usable.

> **Renamed from `hecate-social/hecate-station`** on 2026-04-30. Repository transferred to `macula-io/macula-station`. Erlang application names follow the rename: `hecate_*` (eight in-scope sub-apps) became `macula_*`. Two transitional sub-apps (`hecate_overlay` and `hecate_realm`) keep the `hecate_` prefix until they migrate to the realm-service repository.

---

## Status

| Phase | State | Description |
|-------|-------|-------------|
| **Design** | ✅ Complete (2026-04-14) | ROOT + 9 Parts authored. 39 open questions tracked. |
| **Phase 0 — Repo bootstrap** | 🏗️ In progress | This repo. Empty skeleton that compiles. |
| **Phase 1 — Walking skeleton** | ⏳ Pending | Two stations, signed `node_record` exchange, tombstone on stop. |
| Phase 2–8 | ⏳ Pending | See `plans/PLAN_MACULA_V2_PART7_IMPLEMENTATION.md`. |

---

## What is this?

Macula Station is the reference implementation of a **Macula V2** node. Macula is a federated relay/mesh protocol for sovereign, end-to-end-encrypted application networks. A *station* is the unit of deployment — a single process that provides identity, peering, DHT participation, SWIM liveness, source-routing, bootstrap, and overlay services to consumers of the Macula SDK.

See the architecture plans for full context:

- [`plans/PLAN_MACULA_V2_ROOT.md`](plans/PLAN_MACULA_V2_ROOT.md) — index + overview
- [`plans/PLAN_MACULA_V2_PART1_FOUNDATIONS.md`](plans/PLAN_MACULA_V2_PART1_FOUNDATIONS.md) — goals, six pillars, terminology
- [`plans/PLAN_MACULA_V2_PART2_TOPOLOGY.md`](plans/PLAN_MACULA_V2_PART2_TOPOLOGY.md) — topology + roles
- [`plans/PLAN_MACULA_V2_PART3_DISCOVERY.md`](plans/PLAN_MACULA_V2_PART3_DISCOVERY.md) — DHT + PKARR records
- [`plans/PLAN_MACULA_V2_PART4_LIFECYCLE.md`](plans/PLAN_MACULA_V2_PART4_LIFECYCLE.md) — peer lifecycle + SWIM
- [`plans/PLAN_MACULA_V2_PART5_BOOTSTRAP.md`](plans/PLAN_MACULA_V2_PART5_BOOTSTRAP.md) — bootstrap cascade
- [`plans/PLAN_MACULA_V2_PART6_PROTOCOL.md`](plans/PLAN_MACULA_V2_PART6_PROTOCOL.md) — wire protocol
- [`plans/PLAN_MACULA_V2_PART7_IMPLEMENTATION.md`](plans/PLAN_MACULA_V2_PART7_IMPLEMENTATION.md) — this phase plan
- [`plans/PLAN_MACULA_V2_PART8_VERIFICATION.md`](plans/PLAN_MACULA_V2_PART8_VERIFICATION.md) — testing strategy
- [`plans/PLAN_MACULA_V2_PART9_OPEN.md`](plans/PLAN_MACULA_V2_PART9_OPEN.md) — 39 open questions (O1–O39)
- [`plans/THREAT_MODEL_MACULA.md`](plans/THREAT_MODEL_MACULA.md) — threat model

---

## Relationship to other repos

| Repo | Role |
|------|------|
| `macula-io/macula` | SDK consumed by applications. V1 on `v1.x`; V2 develops on `main`. |
| `macula-io/macula-station` (this) | Reference station server implementation. V2 only. |
| `macula-io/macula-relay` | **Archived.** V1 codebase. Superseded by this repo. |
| `hecate-social/hecate-daemon` | User-facing Hecate runtime. Consumes Macula SDK. |
| `macula-io/macula-realm` | Realm server. Will consume V2 from Phase 7+. |

---

## Build (skeleton)

```
rebar3 compile
```

Nothing runs yet. This is Phase 0 — acceptance is "empty umbrella compiles + CI green."

---

## Operational lessons

- [`docs/PUBSUB_RESIGN_LOOP_LESSON.md`](docs/PUBSUB_RESIGN_LOOP_LESSON.md) — why the `[peer_observer] pubsub frame verify failed: signature_invalid` warning is load-bearing, what four protocol-side fixes regressed, and what a Phase 2 publisher-end-to-end signature attempt needs to know. Read before touching the relay path.
- [`docs/CASCADE_INVESTIGATION.md`](docs/CASCADE_INVESTIGATION.md) — root-cause investigation of the e2e torture cascade. Confirmed mechanism (sync `gen_server:call` from `peer_observer` to `macula_dht:observe` times out under accumulated daemon-conn state, peer_observer dies, supervisor restart drops named ETS, fleet-wide cascade). Tactical fix shipped in commit `b0340b7`; conn-aging follow-up scoped from data.

---

## License

Apache-2.0 — see [`LICENSE`](LICENSE).
