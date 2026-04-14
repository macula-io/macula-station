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

## Session 6.4.x — mDNS responder (shipped 2026-04-15)

**Scope:** advertise side of Tier B — steady-state service so peers'
probes find us. Link-local scope-id plumbing split to 6.4.y
(multi-interface fan-out remains on the to-do list).

**Files added:**
- `hecate_bootstrap_mdns:build_advertisement/2` — pure fn mapping an
  incoming query + our `node_info` to response bytes or `ignore`.
  Filters: QR=1 (don't echo responses), non-service name, unsupported
  qtype, garbage bytes. TXT answer carries the exact same
  `node_id=hex/port/tier` triple that the probe side decodes.
  Transaction id is echoed.
- `hecate_bootstrap_mdns_responder` — gen_server owning the UDP
  socket. Pluggable `socket_opener` opt (tests bind an ephemeral
  loopback port; production joins `[ff02::fb]:5353`, which may
  conflict with avahi). `silent=true` keeps the socket bound but
  drops every query (Part 5 §5.3 privacy switch, O6). `set_silent/2`
  toggles at runtime; `port/1` exposes the bound port.

**Tests — 13 new eunit:** advertisement builder happy/response-echo/
other-service/unsupported-type/TXT/PTR/garbage/id-echo; responder
loopback round-trip, silent-drops, set_silent toggle, non-service
dropped, init failure (trap_exit + drain).

**State of green (post-6.4.x):** 531 station eunit / 26 station CT /
xref / dialyzer clean.

## Session 6.4.y — link-local scope-id + multi-interface (planned)

- Per-interface fan-out: one responder per interface, each bound
  with `{multicast_if, IfAddr}`.
- Scope-id plumbing through tier_b candidate records so link-local
  peer addresses remain reachable from the routing table.

## Session 6.5 — Tier C Mainline DHT bridge (shipped 2026-04-15)

**Scope:** pure codecs + Tier C probe with pluggable DHT transport.
The actual Kademlia-over-UDP BT-DHT client lives in 6.5.x — we ship
everything a production transport will need to plug in cleanly.

**Files added:**
- `hecate_bootstrap_bencode` — BEP 3 bencode codec. Integers, byte
  strings, lists, dicts (binary keys, sorted on encode).
  Deterministic: encoding two equal maps produces byte-identical
  output regardless of insertion order.
- `hecate_bootstrap_bep44` — mutable-item envelope.
  - `target_id/1,2` — SHA-1 of pubkey [+ salt] per BEP 44 §2.
  - `signed_payload/2,3` — exact BEP 44 §1 wire shape
    `3:seqi<seq>e1:v<bencoded-value>` (and the salt-prefixed
    variant). Wire shape verified against BEP 44's published
    example (`Hello World!` with seq=1234 and salt=`foobar`).
  - `sign/3,4` — convenience Ed25519 signer for tests + tooling
    (production uses FROST threshold).
  - `verify/1` — shape check (32-byte pubkey, 64-byte sig, seq≥0)
    then Ed25519 verify over the BEP 44 payload.
- `hecate_bootstrap_dht_transport` behaviour — single callback
  `get_mutable(TargetId, TimeoutMs) -> {ok, item()} | {error, _}`.
- `hecate_bootstrap_tier_c` — probe (500 ms stagger). For each
  foundation pubkey in parallel: derive target id → DHT get →
  verify item.pubkey matches → `hecate_bootstrap_bep44:verify/1` →
  decode inner DNS packet → concatenate every TXT character-string
  → `macula_record:decode/1` → `macula_foundation:verify_record/1`
  → emit seeds as verified peers. First successful pubkey wins;
  failures fall through. Missing transport is a hard error
  (`{error, no_transport}`).

**Tests — 27 new eunit + 1 CT:**
- `hecate_bootstrap_bencode_tests` (44 cases across encode/decode/
  round-trip/canonicalisation/error-paths).
- `hecate_bootstrap_bep44_tests` (13 cases: target_id vectors,
  signed_payload shape including BEP 44's published example, sign+
  verify round-trip with and without salt, tampered value/seq/pubkey
  rejected, salt mismatch rejected, malformed shape rejected).
- `hecate_bootstrap_tier_c_tests` (10 cases with ETS-backed
  `hecate_bootstrap_dht_fake`): happy path, no_transport,
  no_pubkeys, wrong pubkey in DHT item, tampered BEP 44 signature,
  non-DNS value, PKARR with no TXT, DHT get failure, record not
  signed by foundation, multi-pubkey first-success wins.
- `hecate_phase6_SUITE` gains tier_c cascade-winner case when Tiers
  A and B are effectively down.

**State of green (post-6.5):** 598 station eunit / 27 station CT /
xref / dialyzer clean.

## Session 6.5.x — Real Kademlia UDP DHT client (planned)

- Either minimal Erlang BT-DHT client or Rust NIF wrapping an
  existing Kademlia implementation.
- Multi-node redundancy: query ≥8 nodes, prefer highest seq.
- Rate limiting per Mainline DHT operator policy.

## Session 6.6 — Tier D blockchain anchor (planned)

- `hecate_bootstrap_tier_d` — fetch quarterly foundation seed list
  from Bitcoin OP_RETURN + Ethereum contract event.
- Light-client vs block-explorer-API path — decision pending (O2).

## Session 6.7 — Acceptance against §11.3 bars (planned)

- Cold-boot Tier A only ⇒ < 2 s to first DHT lookup.
- Cold-boot Tier B only ⇒ < 3 s.
- Cold-boot Tier C only ⇒ < 10 s.
- Cold-boot Tier D only ⇒ < 30 s.
- Adversarial drop scenario ⇒ full cascade in < 60 s.

