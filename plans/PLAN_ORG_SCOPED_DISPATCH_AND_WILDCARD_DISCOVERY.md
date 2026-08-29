# PLAN — Org-scoped dispatch and wildcard discovery

**Status:** ALL slices (1–11) **DONE and live-verified 2026-08-29**. Slices
3–5 (wildcards) needed the slice-3 ownership question answered first; 7–9
were cleanup, independently shippable. **Slice 10**: a real,
previously-unknown bug found while live-testing 1–2. **Slice 11**: build
version tracking, found and fixed while verifying the fleet rollout. Slice
5(b) (mesh-wide wildcard pubsub) found and fixed a second real bug live —
see its own section below.
**Created:** 2026-08-29
**Updated:** 2026-08-29 — all slices shipped same day.
**Repos touched:** `hecate-services/hecate-om` (primary), `macula-io/macula` (SDK,
possibly), `macula-io/macula-station` (pubsub only, slice 5) — verified below that
most of this needs **no macula-station DHT/dispatch changes at all**, which is a
smaller blast radius than assumed when this was scoped.
**One line:** so a consumer can call one org's capability (not whichever org's
provider happens to answer first) and browse or target capabilities/topics by
pattern, correctly, even when providers share a relay station.

---

## How to read this plan

Slices 1–2 and 6–9 are **BUILD**, not CLAIM — infrastructure and cleanup, no
assertion about the world, no adversarial gate needed. Slices 3–5 are also BUILD
in nature but slice 3 has one real design decision (where the shared
pattern-matching primitive lives) that should be settled before 4 and 5 start,
since both depend on it.

One-line checkpoint before starting any slice: which slice, its DONE-WHEN, rough
size. Cheap to veto.

---

## Phase map

| # | Slice | Depends on | Repo(s) | Status |
|---|-------|-----------|---------|--------|
| 1 | Org-qualified wire-level procedure names | — | hecate-om | **DONE 2026-08-29** |
| 2 | Thread key-format through resolve → dial | 1 | hecate-om | **DONE, live-verified** |
| 6 | Fold in the TTL fix while touching `advertise_one` | 1 | hecate-om | **DONE in code; live effect needs macula hex ≥10.11.1** |
| 3 | Shared segment/wildcard-matching primitive | — | macula (resolved: macula-station DOES dep on macula, `~> 10.5`) | **DONE — `macula_topic_pattern`, macula 10.12.0** |
| 4 | Org capability browse (redesigned — see below) | 3 | hecate-om | **DONE, live-verified 2026-08-29 — macula 10.13.1 published, hecate-om upgraded** |
| 5 | Wildcard pubsub topic **subscription** | 3 | macula (`hecate_pubsub`), macula-station | **PARTIALLY DONE — 2026-08-29. Scope (a) station-local: DONE, macula 10.13.0. Scope (b) mesh-wide: 1-hop DONE (macula 10.14.0 `patterns/1` + macula-station `df8f57f` fixed a real peer-subscription bug found live, cross-station delivery verified). 2+-hop confirmed NOT working despite converged gossip state — separate, not-yet-root-caused gap, see dedicated note below** |
| 7 | Fix `macula_dht_lookup.erl` — turned out to be a real bug, not just stale docs | — | macula-station | **DONE 2026-08-29** |
| 8 | Correct `read_model_services.md`'s discovery-ceiling claim | — | hecate-om | **DONE** |
| 9 | Correct the `kademlia_dht_architecture.html` hexdocs page | after 1–5 land | macula-station | **DONE 2026-08-29 — hexdocs page is frozen (hex 1-hour edit window, long since closed) and describes an architecture that has since moved out of macula entirely; superseded by a new, source-verified guide at `macula-station/docs/KADEMLIA_DHT_ARCHITECTURE.md` (commit `6eb874d`), not an edit to the old page** |
| 10 | **NEW** — second sequential `call_station` on one pool fails | — | macula (`macula_client`'s pool/link reuse) | **DONE, live-verified 2026-08-29** |

**Publish status, 2026-08-29, updated**: macula **10.13.1 published to hex.pm** (confirmed
via the hex API — top release). hecate-om's `macula` dep upgraded (`rebar3 upgrade macula`)
and re-verified: full unit suite + the live station suite (including the new
`list_org_capabilities` test) pass against the real hex-resolved package, no checkout
needed. macula-station continues to build via its own `_checkouts/macula` local-dev symlink
(pre-existing convention, untouched) — its `rebar.config` constraint (`~> 10.5`) already
permits 10.13.1 with no edit needed, and no Dockerfile/CI pins a narrower version.

**Still outstanding**: the demo fleet's actual RUNNING macula-station image does not yet
reflect this session's macula-station-side fix (slice 7's dial-injection fix) — that needs
a separate build + push to ghcr + watchtower-driven deploy, not something a hex publish
touches. Not done this session; flag before assuming slice 7 is live on the fleet, not just
correct in source.

Slices 1–2 are the direct fix for the bug the live test found and should go
first. 6 is a two-line addition to the same function 1 already touches — do it
in the same commit, not a separate trip through the code. 3–5 are the
newly-scoped wildcard work and can start independently of 1–2/6. 7–8 have zero
code dependency on anything else and can be done any time; 9 should wait until
the architecture it documents has actually stabilized, or it just needs
correcting twice.

---

## Background — what's verified true right now, not assumed

Everything below was read from source this session, not recalled from training
or trusted from a doc page. Citations are file paths, not vibes.

- **The bug**: `hecate-services/hecate-om/src/hecate_om_capabilities.erl` already
  has an uncommitted fix (`discovery_key_org/3`, `org_scoped_or_any/4`,
  `org_scoped_full_or_any/4`) that correctly resolves the DHT record for
  `call_capability(Org, ...)` under an org-qualified key. Proven correct at the
  DHT layer by `test/hecate_om_capabilities_tests.erl`'s
  `discovery_key_org_matches_what_gets_published_under_it_test` (73/73 suite
  passing). **Proven insufficient** by
  `test_live/hecate_om_capabilities_live_station_tests.erl`'s
  `org_scoped_call_reaches_only_the_targeted_org_test_`, which is currently
  **failing** live against `station-de-frankfurt.macula.io`: both an
  `<<"acme">>`-targeted and a `<<"contoso">>`-targeted call get answered by
  Contoso, because both test providers share one relay station.
- **Root cause, confirmed at the station**:
  `macula-station/apps/macula_station/src/macula_remote_advertise_registry.erl`
  keys its whole registry `{realm(), procedure()}` where `-type procedure() ::
  binary()` — a bare, opaque string, with an explicit documented invariant: *"A
  `(realm, procedure)` tuple has at most one advertiser at a time on this
  station. Re-advertising replaces the prior entry."* Two orgs calling
  `macula_response:advertise_direct(Pool, Realm, CapName, ...)` with the same
  bare `CapName` collide on this exact invariant — whoever's 30-second
  republish timer (`hecate_om_capabilities:?REPUBLISH_INTERVAL_MS`) lands last
  wins the slot, mesh-wide, per station.
- **The registry is genuinely opaque — verified, not assumed.** `register/4`'s
  only constraint on `Procedure` is `is_binary(Procedure)`. No parsing, no
  length limit, no delimiter assumption anywhere in the module. The wire
  `advertise/1` frame builder in `macula-io/macula/src/peering/macula_frame.erl`
  (`advertise(#{realm := R, procedure := Proc, advertiser := Adv})`) confirms
  `realm` and `procedure` are **separate fields on the frame** — the procedure
  string does not need to re-embed the realm. **This means slice 1 needs no
  macula-station code changes at all**: registering and calling under a longer,
  differently-shaped procedure string just works today.
- **The multi-hop routing problem ("consumer knows the key, my station doesn't
  hold it") is already solved, live, and better than earlier session notes
  claimed.** `macula-station/apps/macula_station/src/macula_station_dht_handlers.erl`'s
  `_dht.find_record`/`_dht.find_records` handlers check the local store first,
  then run a bounded multi-round iterative FIND_VALUE walk (`walk_find_value/5`,
  max 3 rounds, width 5, XOR-distance sorted) against progressively-discovered
  peers, dialing new candidates on demand via
  `macula_station_dht_dialer:ensure_dialed/3` — which verifies the
  handshake-proven NodeId against the one it asked for before trusting the
  connection. The "only sees what one relay locally holds" ceiling
  (`read_model_services.md`'s own wording, see slice 8) is real but applies
  specifically to `_dht.find_records_by_type` (a one-shot local-only listing),
  **not** to `find_records`/`find_record` (exact-key), which already walks.
  `macula:find_records/2` on the client SDK side
  (`macula-io/macula/src/macula_direct_dial.erl`'s `find_records_retry/3`) is a
  thin RPC to this handler; the "retry" loop rides out DHT-propagation lag, not
  topological unreachability.
- **Composite-key wildcard discovery needs no macula-station changes either.**
  DHT storage keys are `crypto:hash(sha256, <arbitrary string>)` — generic,
  confirmed by `hecate_om_capabilities:discovery_key/2` and `discovery_key_org/3`
  both routing through the same `macula_record:procedure_key/1`. Publishing a
  record under additional composite keys, and querying them via the existing
  `find_records`/`find_record` RPC, is exactly the same mechanism as slice 1 —
  no new station-side primitive required. Only wildcard **pubsub subscription**
  (live fan-out matching against a pattern, not a one-shot keyed lookup) is
  actually a different mechanism and does need relay-side changes (slice 5).

---

## Part A — Org-scoped wire dispatch (slices 1–2, 6) — DONE 2026-08-29

Implemented, unit-tested, dialyzer-clean, and **live-verified twice** against
`station-de-frankfurt.macula.io`: `org_scoped_call_reaches_only_the_targeted_org_test_`
passes — a call targeting `acme` is answered only by `acme`, a call targeting
`contoso` only by `contoso`, never crossed, with both providers sharing one
relay station.

Turned out simpler than scoped: the separate `advertise_record_only` DHT write
from the earlier (uncommitted) fix was dropped entirely — `advertise_direct`
called with `org_procedure(Org, Name)` as its `Procedure` argument already
publishes the DHT record at exactly `discovery_key_org/3`'s key on its own
(`macula_direct_dial:discovery_uri/2`'s `RealmHex/Procedure` and this module's
`procedure_uri/3`'s `RealmHex/Org/Name` are byte-identical when
`Procedure = org_procedure(Org, Name)` — verified directly in source, not
assumed). Two `advertise_direct` calls (bare + org-qualified), each
self-sufficient, replaced one `advertise_direct` + one hand-built record.

Also fixed along the way, not originally scoped as its own slice but found
necessary to get slice 2's live test genuinely green (not just "usually
green"):

- **`macula:adv_opts/1` (macula SDK) silently dropped `ttl_ms`** — a real bug
  in an owned library, fixed at the source per this org's own rule. macula
  bumped 10.11.0 → 10.11.1. hecate-om's `ttl_ms` fix (slice 6) has no live
  effect for the handler-bearing advertise path until hecate-om's `macula` dep
  (`~> 10.0`, hex) is bumped past 10.11.1 — not done this session (never
  publish to hex without the user firing it).
- **`hecate_om_capabilities:find/2` had zero retry margin** against
  `macula:find_records/2` — unlike `macula_direct_dial`'s own internal
  resolution (which retries 50× at 100ms specifically to ride out DHT-write
  propagation lag), this module's own `find/2` was a single shot. Found live:
  the second-published (Contoso's) org-qualified record briefly wasn't
  resolvable through this gap. Fixed by mirroring `macula_direct_dial`'s exact
  retry budget. This let two now-dead `_Other -> []` catch-all clauses
  (`resolve_records/1`, `resolve_full_records/1`) be removed — dialyzer
  correctly flagged them as unreachable once `find/2` stopped ever returning
  `{error, _}` to its callers.

The design detail below (slices 1/2/6 as originally scoped) is kept as the
historical record of what was planned; see the DONE summary above for what
actually shipped and where it differed.

### Slice 1 — Org-qualified wire-level procedure names

**What:** `hecate_om_capabilities:advertise_one/7` (handler-bearing clause)
currently calls `macula_response:advertise_direct(Pool, Realm, Name, Mod, Args,
KeyPair, Opts)` once, under the bare `Name`. Change it to advertise **twice**:
the existing bare-name registration (unchanged — this is what makes
`call_capability` work today for "any provider" callers and stays the
fallback), plus a second `advertise_direct` call under an org-qualified wire
name.

**Wire name format, decided:** `<<Org/binary, "/", Name/binary>>` — **not**
`procedure_uri/3`'s `RealmHex/Org/Name` form. Realm is already a separate field
on the ADVERTISE and CALL frames (confirmed above); re-embedding it in the
procedure string would be redundant on every wire message. This is a
**different string than the DHT discovery key** — the DHT key
(`discovery_key_org/3`, unchanged) still needs the realm prefix because a DHT
storage key has no separate realm field, it's one opaque hash. Two different
formats for two different purposes; don't conflate them.

**Reuse, don't duplicate:** both `advertise_direct` calls should go through the
same `reuse_sup` mechanism already in place (`reuse_sup_opts/1`) so a second
supervised responder process isn't leaked per republish tick. Investigate
whether `macula_response:advertise_direct` can register a second name against
an *existing* handler process cheaply, or whether it necessarily spins up a
second one — either is fine correctness-wise, but prefer the cheaper path if
available.

**DONE-WHEN:** `org_scoped_call_reaches_only_the_targeted_org_test_` in
`test_live/hecate_om_capabilities_live_station_tests.erl` still fails at this
point (slice 2 is what makes it pass) — but a manual live check confirms two
distinct entries now exist in the station's `macula_remote_advertise_registry`
for `{Realm, <<"acme/", CapName/binary>>}` and
`{Realm, <<"contoso/", CapName/binary>>}`, not one shared bare-name slot.

### Slice 2 — Thread the key-format through resolve → dial

**What:** `resolve_at/4` and `resolve_full/4` (via `org_scoped_or_any/4` /
`org_scoped_full_or_any/4`) currently return matching records but don't say
*which* key format matched. `dial_provider/9` always calls with the bare
`CapName`. Fix: have the org-scoped/any-org resolution functions tag their
result with which path won (`{org_scoped, Records}` vs `{any_org, Records}`),
and have `dial_provider` send the org-qualified wire name
(`<<Org/binary, "/", CapName/binary>>`) when the org-scoped path won, the bare
name otherwise.

**Why this has to be a tag, not a re-derivation at call time:** the caller's
`Org` argument to `call_capability/5` is the org they *want*, not necessarily
the org whose advertisement actually matched — under `org_scoped_or_any`'s
fallback, an any-org caller can still resolve to a *specific* provider's
record, and the wire CALL needs to name whichever registration that provider
actually holds. Deriving this from `Org` alone would silently target the wrong
registration on the fallback path.

**DONE-WHEN:** `org_scoped_call_reaches_only_the_targeted_org_test_` passes,
live, against `station-de-frankfurt.macula.io` — both a call targeting
`<<"acme">>` and a call targeting `<<"contoso">>` get answered by the org they
targeted, every time, never the other. Re-run the existing single-org live test
(`capability_with_handler_is_genuinely_callable_test_`) to confirm no
regression on the common case.

### Slice 6 — TTL fix, bundled into slice 1's commit

**What:** `advertise_one`'s handler-bearing clause calls
`macula_response:advertise_direct/7` on a 30-second republish timer
(`?REPUBLISH_INTERVAL_MS`) but never passes `ttl_ms`, so the underlying
`procedure_advertisement` falls back to the ~48-hour envelope default. A dead
service's advertisement (now **two** of them, bare and org-qualified, once
slice 1 lands) stays discoverable and callable-looking for up to two days.
Pass an explicit `ttl_ms` proportioned to the republish interval — 3–4× is the
margin `macula_station_announcer` already uses for stations (refreshes at 75%
of TTL). Already documented as a known gap in
`hecate-om/guides/read_model_services.md`'s last section; this slice is what
actually fixes it in code, since slice 1 is already touching this exact
function.

**DONE-WHEN:** a capability advertised via `hecate_om_capabilities:register/1`
carries a `ttl_ms` on its `procedure_advertisement` record proportioned to
30s (not the ~48h default) — check via `macula_record:expires_at/1` on the
record fetched right after a `register/1` call in a unit test.

---

## Part B — Wildcard matching (slices 3–5)

Two genuinely different mechanisms sharing one primitive. Don't conflate them:

- **B1, discovery** — a one-shot "resolve this pattern to matching providers
  right now" query. Reuses the existing multi-hop `find_records`/`find_record`
  walk; needs no relay-side changes, just a client-side (hecate-om or shared
  macula-SDK-helper) query-planning layer.
- **B2, subscription** — a live, ongoing "fan out this publish to every
  subscriber whose *pattern* matches, not just exact-string subscribers."
  This is genuinely relay-side work: it changes how the station's pubsub
  routing decides who to deliver a PUBLISH to.

Both need the same underlying question answered: given a fixed 5-segment
address `realm/org/app/domain/name_vN` (realm always concrete — it's bound to
the connection/session, never wildcarded), does a wildcard-bearing *pattern*
match a concrete address? `*` is a single-segment wildcard in exactly one or
more of `org`/`app`/`domain`/`name` — no `**`, no partial-segment globs, no
open-ended pattern language. This is consistent with, not in tension with,
`macula-io/CLAUDE.md`'s existing "Massive Scale Topic Design" rule (no
per-entity IDs in topics) — org/app/domain/name are bounded-cardinality
dimensions, not per-entity identifiers.

### Slice 3 — Shared segment/wildcard-matching primitive

**What:** one pure function, `matches(Pattern, Concrete) -> boolean()`, over
4-tuples (or `/`-split binaries) of org/app/domain/name, `*` matches anything
in that position, everything else is exact equality. Zero I/O, trivially
testable, no reason to write it twice.

**Investigate first, don't assume:** does `macula-station` depend on the
`macula` hex package as a library, or does it implement the wire protocol
independently (parallel codebases per `macula-io/CLAUDE.md`'s "SDK provides...
Relay provides (separate repo)" split)? If macula-station has no dependency
path to `macula`, the primitive can't live in one place and be `require`'d by
the other — it either needs to live in a new tiny shared dependency-free
library both already could pull in, or be independently (but identically)
implemented in both, with a shared test vector file to keep them honest. Check
`macula-station`'s `rebar.config`/`mix.exs`-equivalent deps before deciding.

**DONE-WHEN:** the primitive exists in exactly the number of places the
investigation above says it needs to, with a shared/mirrored test suite
covering: all-concrete (exact match only), single wildcard trailing, single
wildcard non-trailing, multiple wildcards, wildcard-vs-wildcard (pattern
matching pattern — needed for B2's subscription-registration-time dedup, not
just publish-time matching).

### Slice 4 — Org capability browse — DONE 2026-08-29, redesigned from the original

**Shipped as `hecate_om_capabilities:list_org_capabilities/1` /
`resolve_org_capabilities/3`** (hecate-om commit `02e313e`), live-verified
against `station-de-frankfurt.macula.io`. The original design below (publish
the same record under composite prefix/per-segment keys) turned out
infeasible once implementation started: `macula_record:storage_key/1`
derives a `procedure_advertisement`'s storage key from its own
`procedure_uri` PAYLOAD field — there is no way to store the record at an
independently-chosen key without that field misrepresenting what it
advertises. Also, `hecate_om_capabilities`'s actual address shape turned out
to be 2 dynamic segments (`Org`, `Name`), not the 4-segment
`org/app/domain/name` the design below assumed — and the only genuinely NEW
wildcard case in a 2-segment model is "browse everything under an Org"
(`Org/*`); "any org for a specific name" is already served by the existing
bare-key fallback, no wildcard machinery needed for that case at all.

Implemented instead as a client-side filter over
`macula:find_records_by_type/2` (matched via `macula_topic_pattern`) —
reusing the same local-relay-view, warm-start-only mechanism
`read_model_services.md` already documents, rather than building a second
DHT record type to solve a consistency problem (multiple advertisers
writing one shared org-index record) that a browse feature doesn't actually
need to solve.

### Slice 4, ORIGINAL DESIGN (superseded, kept for history) — Composite-key wildcard capability discovery (B1)

**What:** alongside the existing full-URI DHT record, publish the same
`procedure_advertisement` under a small, fixed set of additional keys so a
wildcard query resolves via the *existing* `find_records` walk with no new
station-side lookup machinery:

- **Trailing-wildcard (prefix) keys** — cheap, single lookup, no
  intersection needed: `hash(RealmHex/Org)`, `hash(RealmHex/Org/App)`,
  `hash(RealmHex/Org/App/Domain)`. Serves patterns like `Org/App/Domain/*`
  where every wildcard is a suffix.
- **Per-segment keys** — for a wildcard anywhere *before* a concrete segment:
  `hash(RealmHex/{app}/App)`, `hash(RealmHex/{domain}/Domain)`,
  `hash(RealmHex/{name}/Name)` (tag the segment position in the hash input so
  an App value and a Domain value with the same bytes don't collide). Serves
  `*/App/Domain/Name` (any org) and similar.
- **Storage choice:** publish the *full* advertisement record again under each
  composite key (not a pointer to the primary record). `find_records` already
  returns the whole bag at a key — this avoids a second resolve hop at the
  cost of a handful of extra small, TTL-bounded, self-expiring writes (at most
  7 extra keys per advertisement). Revisit only if replication cost becomes a
  real problem later; don't build pointer-indirection pre-emptively.
- **Query planning** (client-side, hecate-om or a shared macula-SDK helper):
  given a pattern, if every wildcard is trailing, issue one `find_records`
  against the matching prefix key. Otherwise, issue one `find_records` per
  concrete-enough per-segment index and intersect the results client-side by
  the primary record's own `procedure_uri` field.

**DONE-WHEN:** a live test (two providers, two orgs, same shape as the slice 2
test) resolves `realm/*/app/domain/name_v1` (any org) to *both* providers, and
`realm/acme/*` (org pinned) to every capability Acme has registered, without
issuing more than one `find_records` round-trip per concrete/prefix segment
combination actually present in the pattern.

### Slice 5 — Wildcard pubsub topic subscription (B2) — RESCOPED 2026-08-29 after reading the real fan-out engine

**Read in full**: `macula-station/apps/macula_station/src/macula_station_route_pubsub_frames.erl`
(835 lines). This is not a simple topic-string registry — it's a heavily
torture-tested (100k-event runs, per-branch loss counters added specifically
because "months of 'multi-hop feels flaky' produced no evidence") dispatcher
with two DISTINCT matching mechanisms:

1. **Local exact-match**: `hecate_pubsub_registry`/`hecate_pubsub_server` —
   subscribers directly connected to THIS station, matched by exact topic
   string.
2. **Cross-station transitive gossip**: `macula_station_bloom_exchange` — each
   station gossips a **Bloom filter** summarizing the topics its downstream
   subscribers (direct + further transitive) care about. A publish fans out
   to peer stations whose Bloom filter tests positive for the topic, without
   needing exact subscriber-list knowledge at every hop — this is what makes
   the mesh scale without O(mesh-size) traffic for every publish.

**The complication**: a Bloom filter tests exact-string membership via
k hash functions — the SAME fundamental incompatibility with wildcards that a
SHA-256 DHT key has (found in slice 4's own background research). There is no
single string to hash for a pattern like `realm/*/app/domain/name_v1`.
Making a wildcard subscription work **locally** (station the subscriber is
directly connected to) is the straightforward part already scoped below.
Making it work **mesh-wide** — a wildcard subscriber on station A correctly
receiving a publish that originates on station C, several hops away — needs
the Bloom-gossip layer itself to somehow represent "downstream wants anything
matching pattern P," which a standard Bloom filter cannot do. Two honest
options, not yet decided:

- **(a) Scope wildcard subscriptions to local-only for now**: a wildcard
  subscriber only ever sees publishes from publishers on the SAME station.
  Real, useful, honestly documented as a limitation — NOT silently shipped as
  if it were mesh-wide.
- **(b) Extend the gossip protocol** to carry registered patterns alongside
  (or instead of) the Bloom summary for topics a downstream wants via
  wildcard — bigger, touches `macula_station_bloom_exchange`'s wire format
  and gossip cadence, risks the performance properties that whole subsystem
  was hard-won for (the 100k-event torture numbers in its own comments).

**Recommendation**: (a) first, shipped and clearly labeled as station-local
only, with (b) as an explicit, separately-scoped follow-up if mesh-wide
wildcard subscription turns out to be needed in practice — don't build (b)
speculatively against a system this performance-sensitive without a real use
case driving it. This mirrors the plan's own smell test: ship the smaller
thing, see if anyone needs more.

**What (a) actually requires**: a subscriber registering a pattern (containing
`*`) needs the station's LOCAL topic registry (`hecate_pubsub_registry`/
`hecate_pubsub_server`) to hold it as a pattern, not an exact string; on a
LOCAL publish (`deliver_typed(publish, ...)`/`deliver_typed(event, ...)`), in
addition to the existing O(1) exact-match lookup, check registered patterns
against the concrete published topic using `macula_topic_pattern:matches/2`
(slice 3, already shipped in macula 10.12.0 — needs macula-station's own
`macula` dep bumped once published). Given the number of *distinct patterns*
actually registered is expected to be small relative to publish volume, a
linear scan of registered patterns is probably sufficient — don't build a
trie/index prematurely.

**DONE-WHEN (scope a)**: a subscriber registered on `realm/*/app/domain/name_v1`
receives a publish to `realm/acme/app/domain/name_v1` **and** one to
`realm/contoso/app/domain/name_v1` from a publisher on the SAME station, live,
and a subscriber on the exact string still only receives its own exact topic
(no regression on existing exact-match subscribers). Explicitly test and
document that a wildcard subscriber does NOT receive a publish that only
reaches this station via bloom-gossip from a peer — that gap is scope (a)'s
known, accepted boundary, not a bug to chase in this slice.

**Scope (b) — what actually shipped, 2026-08-29:**

Went with option (b) from the two above, decided by direct instruction rather
than re-litigating the tradeoff — the "needs a real use case" caveat had
already been satisfied by having this concrete slice in hand.

- **New `patterns/1` export** on `hecate_pubsub`/`hecate_pubsub_server`
  (macula 10.14.0, published to hex.pm) — the same list `subscribe/3` was
  already building locally for scope (a), just handed out separately from
  `topics/1`.
- **Patterns are NOT folded into the Bloom.** A Bloom filter can only test
  exact-string membership; a `*`-bearing pattern can't be usefully
  compressed into one. Instead gossiped as a raw set of pattern strings
  (`term_to_binary/1`) on a brand new `_mesh.patterns` topic — completely
  separate wire topic, state fields, ETS mirror table
  (`macula_station_peer_patterns`), and delivery counter
  (`?CTR_NO_PATTERN_MATCH`) from the existing Bloom machinery in
  `macula_station_bloom_exchange`. Reuses the SAME rebuild tick (30s),
  debounce (2s), and peer-staleness TTL (600s) — same `peer_seen` clock,
  since a peer that stopped gossiping is equally stale for both channels.
- **Matching is O(patterns) per publish** (`macula_topic_pattern:matches/2`
  against each gossiped pattern), not O(1) like the Bloom — accepted, since
  the number of distinct patterns actually registered mesh-wide is expected
  to stay small, same assumption the scope-(a) design already made about
  local pattern counts.
- **Fan-out**: `macula_station_route_pubsub_frames:bloom_fan_extras_for_topic/4`
  now unions candidates from both channels (bloom-fan ∪ pattern-fan),
  deduped, each independently counted so "no bloom match" and "no pattern
  match" never conflate into one signal — the module's own established
  discipline (see `?CTR_UNAUTH_EVENT`/`?CTR_UNAUTH_ORIGIN`'s comment for the
  precedent this follows).
- **Security**: the `_mesh.patterns` payload is attacker-influenceable wire
  data from a peer station, decoded with `binary_to_term(_, [safe])` (no new
  atoms, no funs/pids) inside try/catch, verified with a dedicated test that
  a malformed/hostile payload is dropped, not crashed, and never poisons
  `peer_patterns`.
- **Tests**: 10 new cases in `macula_station_bloom_exchange_tests.erl`
  (merge/union semantics, debounce scheduling on peer-pattern change and on
  inbound `_mesh.patterns` events, the malformed-payload safety property,
  `peer_patterns/1` + `pattern_matches_ets/1` end-to-end) and a new
  `macula_station_route_pubsub_frames_tests.erl` (this module previously had
  ZERO test coverage — scoped narrowly to the new bloom-fan/pattern-fan union
  and its independent counters, not a wholesale backfill). Full macula-station
  suite (1096 tests) and dialyzer both clean; macula SDK suite (1618 tests,
  one known-flaky `call_stream` timing test excluded, passes in isolation)
  and dialyzer both clean.
- **NOT done**: live multi-station verification (the fleet-level check this
  session used for the org-dispatch and connection-reuse fixes earlier) has
  not been run against the real demo fleet yet. Unit/property coverage only
  so far.

---

## Part C — Cleanup opportunities found while investigating this

### Slice 7 — `macula_dht_lookup.erl`: stale docs, likely-dead code

**What was found:** `macula-station/apps/macula_dht/src/macula_dht_lookup.erl`
implements a proper S/Kademlia disjoint-path iterative lookup (3 paths, α=3),
well-documented and unit-tested
(`macula_dht_lookup_tests.erl`) — but has **no production caller** found
outside `macula_dht.erl`'s thin `lookup_nodes/2,3` wrapper, which itself has no
caller found outside its own tests. The module's inline comments describe
`macula_dht_protocol:entry_to_station_ref/2` hardcoding `addresses => []` as a
**current** limitation ("⚠ WHAT THIS DOES NOT BUY... the entry cannot be
dialled") — but that was fixed 2026-07-27 (confirmed: the current
`entry_to_station_ref/2` publishes real endpoints via
`macula_dht_entry:endpoints/1`, guarded by
`macula_dht_endpoint_propagation_tests`). The comment is now describing a bug
that doesn't exist anymore, which will mislead the next person who reads it —
including a future session of this same assistant.

There are, in effect, **two parallel implementations of the same iterative
walk**: this one, and `macula_station_dht_handlers.erl`'s own hand-rolled
`walk_find_value/5`, which is the one actually wired to live client traffic
and already has the dial-fix proven in production use.

**What to do:** investigate whether `macula_dht_lookup:lookup_nodes/2,3` is
meant as a general-purpose utility for a not-yet-built future caller, or is
genuinely dead. If dead and nothing in a near-term plan needs it, delete it
and its wrapper per this org's own "no backward compatibility — if nothing is
in production, delete old code entirely" rule, rather than leaving two
implementations of the same idea to drift apart. If it's meant to stay (e.g.
because disjoint-path lookup is more Sybil-resistant than the hand-rolled
walk's single-path-per-round approach and that resistance matters somewhere),
at minimum fix the stale comments and add a moduledoc note pointing at
`macula_station_dht_handlers:walk_find_value/5` as the one actually live,
so the next reader isn't misled about which is real.

**DONE-WHEN:** either the module and its wrapper are gone, or its comments
accurately describe current reality and the "not reachable" claim is removed.

### Slice 8 — `read_model_services.md`'s discovery-ceiling claim, imprecise

**What was found:** the committed guide
(`hecate-services/hecate-om/guides/read_model_services.md`, commit `0bd4bfb`)
says *"A one-time `find_records_by_type` call only sees what one relay locally
holds — fine at ten entities, silently incomplete at thousands"* under a
heading, "Discovery that scales past a crawl," that reads as a general claim
about DHT discovery rather than specifically about the bulk-listing call. It's
correct about `find_records_by_type` (genuinely local-only, confirmed from its
own moduledoc comment in `macula_station_dht_handlers.erl`) but doesn't
distinguish it from `find_records`/`find_record` (exact-key), which — as
established in this plan's Background section — already does a real multi-hop
walk and does not share that ceiling.

**What to do:** edit the section to name `find_records_by_type` specifically
as the local-only call, and add a line noting exact-key resolution
(`find_records`/`find_record`, what `resolve_at`/`resolve_full` actually use)
already walks multi-hop station-side as of the current macula-station, so the
subscription pattern's real value is for *bulk browse* completeness and
staleness handling, not for basic exact-key reachability, which was never as
broken as the surrounding text implies.

**DONE-WHEN:** the guide accurately attributes the local-only ceiling to the
one call that actually has it.

### Slice 9 — `kademlia_dht_architecture.html` hexdocs page

**What was found:** `macula.hexdocs.pm/0.9.1/kademlia_dht_architecture.html`
claims a "✅ Production-ready DHT implementation" with specific bucket counts
and hop estimates, but its own version footer (0.6.0) doesn't match its header
(0.9.1), and its described mechanics (disjoint paths) match
`macula_dht_lookup.erl` — the module found likely-dead in slice 7 — not
`macula_station_dht_handlers.erl`'s actually-live walk. It also doesn't make
clear that this lives in `macula-station`, a separate repo from the `macula`
SDK the docs are published under.

**What to do:** correct this once slices 1–5 (and the slice-7 decision on
which implementation is canonical) have landed, so it documents the settled
architecture instead of a moving target. Sequencing this before then means
writing it twice.

**DONE-WHEN:** the page accurately names which repo/module implements the walk,
matches its own version header/footer, and doesn't claim more than what's
actually live.

### Slice 10 — NEW, found 2026-08-29: second sequential `call_station` fails

**What was found**, live, while getting slice 2's two-org test to a genuine
green (not investigation, not guesswork — reproduced deterministically 3
times with 3 different diagnostics): a `macula:call_station/7` call made on a
pool that already has a healthy, established connection to the SAME station
(from an earlier `call_station` call to that same station on that same pool)
fails with `{error, {disconnected, {peer_closed, "connection lost"}}}`.
**Confirmed to track POSITION, not org or procedure**: calling the exact same
org's exact same procedure twice in a row on one pool reproduces it
identically (1st call ok, 2nd fails the same way) — ruled out anything
specific to org-scoping, procedure-name shape, or this session's changes.
`capability_with_handler_is_genuinely_callable_test_` (the original,
pre-existing live test) never caught this because it only ever makes one
call per test run.

**Working theory, not yet confirmed**: the test's `ConsumerPool` already holds
a healthy link to the station from its own configured seed (`?SEED`,
established via `wait_healthy/2` before any `call_station` call). `call_station`
targets the SAME physical station but via a *different URL spelling*
(`quic://[ipv6]:port`, built by `station_url/2` from a resolved
`station_endpoint` record, vs. `?SEED`'s own `https://hostname:4433` form).
If the pool's "reuse an existing link, or dial a new one" logic
(`macula_client:call_station/8`'s own doc: *"The pool reuses an existing link
or dials and monitors a new one"*) treats these as two DIFFERENT targets
because the strings differ, it may open a genuinely second connection
alongside the pool's existing seed link — and the station may then be closing
one of the two as a duplicate from the same identity. Unconfirmed: needs
tracing inside `macula_client`'s pool gen_server (`{call_station, ...}`
handling) and checking whether macula-station enforces any single-connection-
per-NodeId policy.

**Why this matters beyond this test**: if the theory is right, this affects
ANY real caller making 2+ direct-dial calls over the lifetime of one pool —
which `hecate_om_capabilities:call_capability` explicitly supports and expects
services to do routinely (it's a library function meant to be called
repeatedly, not once). This is a bigger deal than a test-hygiene issue if
confirmed in production shape, not just this specific test's pool setup.

**Workaround shipped** (not a fix): the live test now uses a separate consumer
pool per call rather than one shared pool making two sequential calls,
sidestepping the issue for what that test is actually meant to prove (org
isolation, not connection-reuse robustness).

**DONE — 2026-08-29.** Root cause confirmed by reading `macula_client.erl`
directly: `ensure_link/3` keys its link table by the literal `Station` seed
STRING, not by the station's actual identity. Fixed via `expected_node_id`-
based reuse on a literal-key miss (macula 10.13.1, commit `bea89e7`) — see
its own CHANGELOG entry for the full writeup. Live-verified twice against
`station-de-frankfurt.macula.io` via a temporary local `_checkouts/macula`
symlink (removed after verification — hecate-om's real dependency is still
hex `~> 10.0`, unpublished past 10.11.0 as of this session): the org-scoped
live test now reliably passes with ONE shared consumer pool making two
sequential `call_capability` calls, reverted from the two-pool workaround
that shipped alongside slices 1–2.

---

## Files likely touched, by slice

| Slice | File | Change |
|---|---|---|
| 1, 6 | `hecate-om/src/hecate_om_capabilities.erl` | `advertise_one/7` dual-advertise + `ttl_ms` — **DONE** |
| 2 | `hecate-om/src/hecate_om_capabilities.erl` | tag + thread key-format through resolve/dial — **DONE** |
| 6 (found necessary) | `macula/src/macula_direct_dial.erl` | `adv_opts/1` ttl_ms-forwarding bug, fixed at source — **DONE**, macula 10.11.0→10.11.1 |
| 2 (found necessary) | `hecate-om/src/hecate_om_capabilities.erl` | `find/2` DHT-propagation retry (was single-shot) — **DONE** |
| 1–2, 6 | `hecate-om/test/hecate_om_capabilities_tests.erl` | unit coverage for wire-name selection, ttl, retry — **DONE** (27 tests) |
| 1–2 | `hecate-om/test_live/hecate_om_capabilities_live_station_tests.erl` | rewritten `advertise_org/6`, two-pool workaround for slice 10 — **DONE, live-green** |
| — | `macula/test/macula_direct_dial_adv_opts_tests.erl` | new, regression coverage for the adv_opts fix — **DONE** (7 tests) |
| 10 (new) | `macula/src/client/macula_client.erl` (likely) | second-call connection-reuse bug — **investigation not started** |
| 3 | TBD per investigation | new pure matching module |
| 4 | `hecate-om/src/hecate_om_capabilities.erl` (or new module) | composite-key publish + query planning |
| 5 | `macula-station` pubsub routing module(s) | pattern-aware fan-out |
| 5 | `macula` SDK pubsub client surface | pattern subscribe API, if not already generic |
| 7 | `macula-station/apps/macula_dht/src/macula_dht_lookup.erl` (+ wrapper, tests) | fix or delete |
| 8 | `hecate-om/guides/read_model_services.md` | precision edit |
| 9 | macula hexdocs source (wherever `kademlia_dht_architecture.md` source lives) | correction pass |

---

## Testing strategy

Matches this org's existing 3-tier convention (pure → zero-seed real pool →
live station in `test_live/`, excluded from the default CI gate):

- Slice 3's primitive: pure eunit, no mesh, exhaustive pattern-match cases.
- Slices 1–2, 6: unit tests for the pure decision logic (key-format tagging,
  ttl computation) plus the existing live test as the real proof — it already
  exists and already correctly fails before the fix.
- Slice 4: unit tests for query planning (which keys a pattern resolves to)
  plus a new live test mirroring slice 2's two-org shape, extended to a
  wildcard query.
- Slice 5: needs its own live test — two subscribers (one exact, one
  wildcard-pattern) against one publisher, confirming both correct delivery
  and no over-delivery to the exact-match subscriber.

## Slice 11 — NEW, found 2026-08-29: no way to verify a rollout — DONE

Trying to verify slice 7/10's actual fleet rollout surfaced a real, separate gap:
nothing about a station's mesh-facing identity revealed what code it was
actually running. `macula_station.app.src`'s `vsn` had never been bumped once
in this project's history (every build ever reported `"0.1.0"`); `macula_station:version/0`
was a hardcoded literal (`"0.1.0-phase1"`); and `macula_record:read_node_record/1`
silently dropped the `version` field the announcer's heartbeat already carried,
so no consumer — including `hecate-stations` — could ever read a station's own
reported build back out even if it had been real.

Fixed by baking the commit SHA into the Docker image at build time (CI already
computed it for cache-busting) and wiring it through the EXISTING heartbeat
field end to end: macula (`read_node_record/1` now extracts `version`,
10.13.2) → macula-station (`GIT_SHA` build-arg, `station_version()` +
`macula_station:version/0` both read it) → hecate-stations (`station_read_model`
captures it, `list_stations` needed no changes since it already passes the
whole doc through).

**Live-verified fleet-wide, 7/7 stations, all reporting the identical commit
SHA `24884c398594a204cf10c3e9ac19ae7ee8c388ef`**: 4 via `hecate_stations.list_stations`
over the mesh, 3 (falkenstein, helsinki, stockholm — absent from that
particular read-model snapshot, a separate node_record-merge gap, not a
rollout failure) via each station's own admin `/status` endpoint directly.
This is also now the standing mechanism for verifying any future
macula-station rollout — no more SSH-and-guess.

**Node_record-merge gap investigated and closed, 2026-08-29 — not a bug.**
Found `hecate-stations` actually running on `beam03.lab` (not on the demo
fleet itself; deployed separately, seeded via `station-de-frankfurt.macula.io`,
consumes mesh-wide DHT records regardless of which realm it authenticates
as, per `hecate_om/CLAUDE.md`'s "realm joins mesh, not stations"). Drove its
live node directly via `remote_console` (NOT `eval` — that boots a
throwaway instance against a release, see
`reference_elixir_release_eval_vs_rpc`) and confirmed: `find_records_by_type/2`
called fresh does find every station's `node_record`/`station_endpoint`,
including falkenstein/helsinki/stockholm; one caught a record mid-flight
right at its expiry boundary (`macula_record:verify/1` returned
`{error, expired}`), and a second query moments later found the same
station's record `valid` again. A follow-up read-model dump minutes later
showed all 3 present with correct hostnames. This is eventual-consistency
convergence in a snapshot-then-subscribe design (`ingest_node_records`'s own
moduledoc names the pattern) — the announcer refreshes at 75% of its 10-min
TTL, so worst case for a station to reappear after being caught mid-refresh
is one refresh interval, not a permanent gap. **Real, fixed defect found
along the way**: `ingest_node_records.erl` swallowed every `verify/1`
failure with zero logging, which is what made this benign race look like a
persistent, unexplained absence in the first place — commit `69e2abd`
(hecate-stations) adds a debug-level log line naming the dropped record's
key and reason.

## Slice 5b live verification, 2026-08-29 — real bug found and fixed

Live-testing scope (b) against the real fleet (helsinki/falkenstein/nuremberg)
initially found ZERO cross-station delivery, even between direct peers, even
with `local_patterns` confirmed correct on the subscribing station. Traced it
to `macula_station_bloom_exchange:sync_inbound_subs/1`: `subscribe_one/3`
only ever called `macula_station_link:subscribe/4` for `<<"_mesh.bloom">>` —
the NEW `?MESH_PATTERNS_TOPIC` (`_mesh.patterns`) was never subscribed to on
any peer link, on any station. `broadcast_patterns/1` was correctly
publishing on every rebuild (same shape as `broadcast_filter/1` always has),
but into a channel nobody had a listener for — so `peer_patterns` stayed
permanently empty everywhere, on every station, unconditionally. Fixed in
commit `df8f57f`: `subscribe_one/3` now subscribes to both topics on the
same link, storing both subrefs; an entry only lands in `Subs` when both
succeed. Full suite (1096 tests) + dialyzer clean; live-verified after
fleet redeploy (see success criteria below).

**Nuremberg's apparent restart-loop — resolved, self-inflicted, not a bug.**
Looked independent at the time (repeated clean `exitCode=0` die/start pairs
seconds apart, no CRASH REPORT, `RestartCount` climbing even on checks that
omitted the `remote_console` `q()` self-inflict below). Root cause found
after the fact by cross-referencing ghcr image push timestamps against
`docker events`: this session pushed 5 macula-station commits within about
20 minutes while chasing the pattern-gossip fix (`2e8ec35` 15:27,
`b5cc215`/`56c5785` 16:10, `df8f57f` 16:34 UTC). Each push triggers
watchtower to pull and recreate every station; nuremberg happened to catch
several of those recreates close enough together that some landed before
the previous one had cleared its 90s healthcheck `StartPeriod`, producing
what looked like an independent crash-loop. Confirmed resolved: the last
push of the session (`0e26caa`, 17:08:34 UTC) triggered one clean recreate
at 17:09:09, and nuremberg has been stable since (`RestartCount=0`, healthy,
1h36m+ uptime at last check) — no bug to fix, no dedicated look needed.

**Process note**: early attempts to diagnose the above were badly confounded
by a genuinely separate, self-inflicted mistake — every diagnostic script
piped into `bin/macula_station remote_console` ended with a trailing `q().`,
which (per Erlang's `-remsh` semantics) halts the REMOTE node, not the local
shell. This crash-restarted the very stations being inspected on every
single check, which is what made the initial "does the fix even work" signal
so noisy and contradictory before this was caught via `docker events`
timestamps. See memory `feedback_remote_console_q_kills_remote_node` —
never end a piped `remote_console` script with `q()`.

## Success criteria

- [x] `org_scoped_call_reaches_only_the_targeted_org_test_` passes live — **2026-08-29**
- [x] Existing single-org live test still passes (no regression) — **2026-08-29**
- [x] A capability's TTL is proportioned to its republish interval, not the
      envelope default — **DONE, live effect confirmed: macula 10.13.1 published,
      hecate-om upgraded, full suite + live suite pass — 2026-08-29**
- [x] Browsing every capability an org has advertised works live, without
      knowing any capability name in advance (redesigned from the original
      "wildcard query against two orgs sharing a station" framing — see
      slice 4's redesign note for why) — **2026-08-29**
- [~] A wildcard pubsub subscription receives publishes from every matching
      concrete topic, live, with no over-delivery to exact subscribers —
      **1-hop DONE, 2+-hop NOT working — 2026-08-29** against the real
      fleet: `macula-cli pubsub watch "acme/*"` on helsinki received a
      publish to `acme/svc.do` from falkenstein (a genuinely different
      station, direct peer), `delivered_via: "direct"`, correct payload;
      a publish to `other/thing` from the same station produced zero
      events (no over-delivery). Found and fixed a real bug on the way —
      see the dedicated note below. 1-hop delivery is live-verified and
      correct.
      **2+-hop delivery does NOT work, confirmed 2026-08-29 after the fleet
      settled** (nuremberg → falkenstein → helsinki, nuremberg is not a
      direct peer of helsinki): `peer_patterns` at nuremberg confirmed fully
      converged (7 matches) via direct query immediately before publishing,
      yet the watcher on helsinki received nothing. This is a SEPARATE gap
      from the peer-subscription bug fixed in `df8f57f` — gossip convergence
      itself works multi-hop (confirmed directly), so the break is somewhere
      in the intermediate hop's own forward-on-EVENT fan-out decision, not
      in gossip propagation. Not yet root-caused. Flagged for the user
      before spending more time on it.
- [x] `macula_dht_lookup.erl` corrected — turned out to be a real bug (not
      just stale docs), fixed with dependency-injected dialing — **2026-08-29**
- [x] `read_model_services.md` correctly attributes the discovery ceiling — **2026-08-29**
- [x] hexdocs Kademlia page matches the settled, live architecture — superseded
      by `macula-station/docs/KADEMLIA_DHT_ARCHITECTURE.md` (the old hex page
      is frozen, not editable — see slice 9) — **2026-08-29**
- [x] Second sequential `call_station` on one pool root-caused and fixed
      (slice 10) — **2026-08-29, live-verified against real hex 10.13.1, and
      again against the deployed fleet (all 7 stations) after the 10.13.2
      publish and macula-station rollout**
      fixed (slice 10)
