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

## Session 6.3 — DoH codec + concrete resolver (shipped 2026-04-15)

**Scope:** pure RFC 8484 codec, concrete `inets:httpc`-backed resolver
implementing `hecate_bootstrap_resolver`, and the base32 codec needed
to derive PKARR zone labels. Anycast probe split out to a later
session (6.3.5) to keep this scope tight.

**Files added:**
- `hecate_bootstrap_base32` — RFC 4648 canonical base32, lowercase
  unpadded output, case-insensitive decode. A 32-byte pubkey encodes
  to exactly 52 chars (fits the 63-char DNS-label limit); trailing
  sub-byte bit fragments on decode are discarded deterministically.
- `hecate_bootstrap_doh` — pure codec + resolver core.
  - `query_domain(Pubkey, ZoneBase)` → `"_pkarr.<52b32>.<zone>"`
    (lowercase Erlang string, what `inet_dns' wants).
  - `build_query/1,2` → DoH `application/dns-message` binary via
    `inet_dns:make_msg/encode`. `build_query/2` accepts an explicit
    transaction id for deterministic tests.
  - `parse_response/3` → verifies id, QR flag, RCODE, collects TXT
    answers matching the expected name (case-insensitive),
    concatenates character-strings + RRs into a single binary. Errors
    are typed atoms + `{rcode, N}` / `{http, Reason}`.
  - `resolve/4` composes the codec with a caller-supplied `SendFun`
    `(Url, Body) -> {ok, RespBody} | {error, _}`.
- `hecate_bootstrap_doh_http` — thin concrete resolver: reads
  `doh_zone_base` from app env (default `<<"macula.io">>`), delegates
  to `hecate_bootstrap_doh:resolve/4` with an `httpc`-backed send fun
  that POSTs `application/dns-message` and maps HTTP status to
  `{ok, Body} | {error, {http_status, Code, Phrase}} | {error, _}`.
- `inets` + `ssl` added to `hecate_bootstrap.app.src` applications.

**Test coverage:**
- `hecate_bootstrap_base32_tests` — RFC 4648 vectors, random-input
  round-trips, 52-char fits-in-label invariant, case-insensitive
  decode, bad-char rejection. 20 cases.
- `hecate_bootstrap_doh_tests` — 29 cases covering query_domain
  (zero-key, random-key round-trip, string zones), build_query
  (encoding shape, random-id range), parse_response (single-string,
  multi-string concat, multi-RR concat, case-insensitive name match,
  id mismatch, QR=false, NXDOMAIN, REFUSED, no_txt_answer, wrong
  domain, garbage bytes), and resolve/4 (happy path, http-error
  wrapping, zone_base override, parse-error propagation).
- `hecate_bootstrap_tier_a_doh_tests` — integration: a module that
  implements `hecate_bootstrap_resolver` by running the real codec
  with a canned HTTP transport, proving Tier A + DoH + foundation
  record pipeline all compose. Exercises multi-chunk TXT splitting
  (records split into 200-byte chars-strings).

**State of green** (2026-04-15, post-6.3):
- 487 station eunit / 25 station CT / xref / dialyzer all clean.

## Session 6.3.5 — IPv6 anycast probe (planned)

- `hecate_bootstrap_anycast` probe: parallel QUIC handshake against
  the foundation-published anycast prefix `2001:...:macula:a::/48';
  any reachable peer becomes an additional Tier A resolver target
  (corroboration prevents BGP-hijack).
- Requires RPKI verification of the announcing AS to be useful;
  validation path TBD (likely defer to a Part 2 §3.5 follow-up).

## Session 6.3.6 — Network-integrated gated CT (planned)

- `hecate_bootstrap_doh_SUITE` skipped without `MACULA_DOH_ENABLE=1`;
  exercises `hecate_bootstrap_doh_http' against Cloudflare 1.1.1.1,
  Quad9 9.9.9.9, Mullvad, with a test record actually published to
  a throwaway domain.

## Session 6.4 — Tier B mDNS probe (shipped 2026-04-15)

**Scope:** query side of link-local mDNS bootstrap. Publish/announce
side (running an mDNS responder for steady-state operation) splits
into a separate Phase 6.4.x follow-up — the probe path is the piece
needed for cold-boot cascade.

**Files added:**
- `hecate_bootstrap_mdns` — pure codec.
  - `service_name/0`, `multicast_group/0`, `multicast_port/0` —
    well-known `_macula._udp.local`, `ff02::fb`, `5353`.
  - `build_query/0,1,2` — DNS `any` query over standard DNS wire
    format via `inet_dns`; `/2` accepts explicit transaction id
    for deterministic tests.
  - `parse_response/1` → list of `#{name, type, data}` answer maps.
  - `extract_candidates/1` → filters TXT records at the service name
    (case-insensitive) and decodes TXT key-value pairs
    (`node_id=<64 hex>`, `port=<1..65535>`, `tier=<0..4>`). Drops
    malformed rows silently.
- `hecate_bootstrap_mdns_transport` behaviour — single callback
  `query(QueryBin, TimeoutMs) -> [{SrcAddr, PacketBin}]`.
- `hecate_bootstrap_mdns_udp` — concrete `gen_udp` implementation:
  inet6 socket, `multicast_ttl=1`, `multicast_loop=false`, sends to
  `[ff02::fb]:5353`, collects unicast replies until deadline.
  (Scope-id handling for link-local peers tracked as 6.4.x.)
- `hecate_bootstrap_tier_b` — probe module (200 ms stagger per
  Part 5 §3). Two pluggable dependencies:
  - `udp_transport` (module implementing the behaviour).
  - `handshake_fun(SrcAddr, Port, ExpectedNodeId)
      -> {ok, SignedRecord} | {error, _}` — the QUIC corroboration
    step (Part 5 §5.1). TXT alone is never trusted; the handshake
    proves possession of the advertised NodeId. Default refuses
    (`handshake_not_configured`) so a misconfigured station yields
    zero peers rather than trusting mDNS blindly.
  - Verify chain per candidate: handshake → `macula_record:verify/1`
    (signature + expiry) → `macula_record:key/1` equals the TXT
    NodeId. Dedup by NodeId (multiple advertisements of the same
    peer collapse).

**Test coverage:**
- `hecate_bootstrap_mdns_tests` — 23 eunit cases (constants, query
  shape, empty/garbage parse, answer map, TXT extraction,
  case-insensitive name match, drop missing/bad fields, range checks
  for port + tier).
- `hecate_bootstrap_tier_b_tests` — 8 eunit cases (happy path, no
  replies, handshake failure, identity mismatch, expired record,
  malformed TXT skipped alongside good one, dedup, default handshake
  rejects everything). Fake transport: ETS-backed
  `hecate_bootstrap_mdns_fake`.
- `hecate_phase6_SUITE` adds one CT case:
  tier_b wins cascade when tier_a has no resolvers (Tier B's 200 ms
  stagger does not block it; three handshake-corroborated peers
  pass the min_peers=3 threshold).

**State of green** (2026-04-15, post-6.4):
- 518 station eunit / 26 station CT / xref / dialyzer all clean.

## Session 6.4.x — mDNS responder + link-local scope-id (planned)

- Advertise `_macula._udp.local` TXT + AAAA for our own NodeId so
  other stations can find us on the LAN (steady-state, not cold
  boot).
- Scope-id plumbing for link-local addresses — `{multicast_if,
  IfAddr}` + per-interface query fan-out.
- Privacy switch (O6): default-on vs default-off mDNS advertise.

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
