# Phase 6 — Bootstrap Cascade: Session Breakdown

**Parent plan:** `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md` §11
**Spec:** `PLAN_MACULA_V2_PART5_BOOTSTRAP.md` §3–§8 (five-tier cascade),
          `PLAN_MACULA_V2_PART6_PROTOCOL.md` §9.14–§9.17 (foundation records).
**Status:** In progress — 6.1 + 6.2 shipped 2026-04-14.

## Session 6.1 — SDK foundation records + orchestrator + Tier E (shipped)

**Scope:** End-to-end cascade with the operator-paste tier wired,
plus the foundation trust anchor every other tier ultimately
verifies against.

**SDK side** — `macula-io/macula@25e5bbe` (branch v2):
- Four foundation record types in `macula_record`:
  - `0x0D foundation_seed_list` (§9.14)
  - `0x0E foundation_parameter` (§9.15)
  - `0x0F foundation_realm_trust_list` (§9.16)
  - `0x10 foundation_t3_attestation` (§9.17)
- `macula_foundation` trust anchor:
  - `pubkeys/0` resolves from `application:get_env(macula_record,
    foundation_pubkeys, _)`; falls back to five deterministic SHA-256
    placeholder pubkeys (no private key exists — production firmware
    MUST override before any Tier A record is consumed).
  - `verify_record/1` enforces `type ∈ {0x0D..0x10}` + signer-trust
    + envelope signature + expiry.
  - `live_pubkeys/0` distinguishes operator-supplied from placeholder.

**Station side** — `hecate-station@f83af56`:
- `hecate_bootstrap` cascade orchestrator: ordered tier list, per-tier
  stagger, deadline, first-tier-to-meet-`min_peers` wins, remaining
  workers cancelled.
- `hecate_bootstrap_tier` behaviour: `tier/0`, `stagger_ms/0`,
  `probe/1 -> {ok, [verified_peer()]} | {error, _}`.
- `hecate_bootstrap_peer_url` codec: `macula-peer:<base64url(cbor{r,a})>`
  — signed `node_record` + transport hints.
- `hecate_bootstrap_tier_e`: zero-stagger, decodes operator-pasted
  URLs, tolerant of bad URLs in a paste.

+CT suite `hecate_phase6_SUITE` covers Tier E happy path, cascade
fall-through to a working tier, and foundation trust-boundary
(trusted vs untrusted signer, wrong record type rejected).

## Session 6.2 — Tier A scaffold + DoH corroboration logic (shipped)

**Scope:** Tier A orchestration and signature verification, with
network I/O abstracted behind a behaviour so unit tests can drive
the corroboration logic without DoH endpoints.

**Files added:**
- `hecate_bootstrap_resolver` — behaviour with one callback,
  `resolve(Url, FoundationKey, Opts) -> {ok, RecordBytes} | {error, _}`.
  Resolvers MUST return raw bytes; the orchestrator owns decoding,
  signer-trust, expiry, and storage-key checks (a single trusted
  resolver would be a trust-anchor failure mode).
- `hecate_bootstrap_tier_a` — zero-stagger probe.
  - Spawns one worker per `(Pubkey, Resolver)` pair in parallel.
  - Tallies replies by `{Pubkey, raw bytes}`; any group reaching
    `corroboration` (default 2) is accepted.
  - Per-response verification chain: `macula_record:decode/1` →
    storage-key matches `SHA-256("foundation_seed_list" || Fk)` →
    `macula_foundation:verify_record/1`. Fail at any step ⇒ try the
    next eligible group.
  - Each surviving seed is emitted as a `verified_peer()` carrying
    the corroborated `foundation_seed_list` record as its anchor.

**Test side:**
- `hecate_bootstrap_tier_a_fake` — ETS-backed in-memory resolver;
  honours `{sleep, Ms, Reply}` so timeout behaviour can be exercised.
- `hecate_bootstrap_tier_a_tests` — 9 eunit cases:
  threshold met / unmet, single-hijack outvoted, untrusted signer
  rejected, wrong-storage-key rejected, no-resolvers, no-pubkeys,
  verified-peer shape, slow resolver doesn't block.
- `hecate_phase6_SUITE` adds two CT cases:
  Tier A wins on corroborated bytes; Tier A under-quorum cascades
  to Tier E.

**State of green** (2026-04-14, post-6.2):
- 437 station eunit / 25 station CT / xref / dialyzer all clean.

## Session 6.3 — Real DoH resolver + IPv6 anycast probe (planned)

- `hecate_bootstrap_doh_httpc` — `inets:httpc` backed implementation
  of `hecate_bootstrap_resolver`. Issues `application/dns-message`
  POST per RFC 8484; parses DNS response; extracts PKARR TXT/CBOR.
- `hecate_bootstrap_anycast` probe: parallel ICMPv6/QUIC ping at the
  foundation-published anycast prefix; surviving endpoints feed back
  into Tier A as additional resolver targets.
- Network-integrated CT suite (skipped without `MACULA_DOH_ENABLE=1`)
  exercises against Cloudflare 1.1.1.1 + Quad9 9.9.9.9 + Mullvad.

## Session 6.4 — Tier B mDNS (planned)

- `hecate_bootstrap_tier_b` advertises + listens on
  `_macula._udp.local` (IPv6 mDNS `ff02::fb`).
- TXT record carries `node_id`, `port`, `tier`; QUIC handshake then
  corroborates via signed `node_record`.
- Privacy switch: announce on/off (default on; O6 unresolved).

## Session 6.5 — Tier C Mainline DHT bridge (planned)

- `hecate_bootstrap_tier_c` queries Mainline DHT for foundation
  pubkeys; verifies returned PKARR records.
- Either: (a) embed minimal BT-DHT client in Erlang, or (b) bind to
  `libtorrent`/`mainline-dht-go` via NIF/port. Decision pending.

## Session 6.6 — Tier D blockchain anchor (planned)

- `hecate_bootstrap_tier_d` reads quarterly foundation-signed seed
  list from Bitcoin OP_RETURN and Ethereum contract event.
- Both chains queried; either suffices.
- Light-client libs vs block-explorer JSON APIs — decision pending
  (O2).

## Session 6.7 — Acceptance against §11.3 bars (planned)

- Cold-boot Tier A only ⇒ < 2 s to first DHT lookup.
- Cold-boot Tier B only ⇒ < 3 s.
- Cold-boot Tier C only ⇒ < 10 s.
- Cold-boot Tier D only ⇒ < 30 s.
- Adversarial drop scenario ⇒ full cascade in < 60 s.
