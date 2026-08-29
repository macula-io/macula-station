# PLAN — Org-scoped dispatch and wildcard discovery

**Status:** Slices 1, 2, 6 **DONE and live-verified 2026-08-29** (see below).
3–5 (wildcards) need the slice-3 ownership question answered first; 7–9 are
cleanup, independently shippable. **New slice 10 added**: a real,
previously-unknown bug found while live-testing 1–2 — tracked separately, not
blocking, but significant.
**Created:** 2026-08-29
**Updated:** 2026-08-29 — slices 1/2/6 shipped same day.
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

| # | Slice | Depends on | Repo(s) |
|---|-------|-----------|---------|
| 1 | Org-qualified wire-level procedure names | — | hecate-om |
| 2 | Thread key-format through resolve → dial | 1 | hecate-om |
| 6 | Fold in the TTL fix while touching `advertise_one` | 1 | hecate-om |
| 3 | Shared segment/wildcard-matching primitive | — | macula and/or macula-station (decide) |
| 4 | Composite-key wildcard capability **discovery** | 3 | hecate-om, macula (usage only) |
| 5 | Wildcard pubsub topic **subscription** | 3 | macula-station (+ macula SDK client surface) |
| 7 | Retire or fix `macula_dht_lookup.erl`'s stale docs | — | macula-station |
| 8 | Correct `read_model_services.md`'s discovery-ceiling claim | — | hecate-om |
| 9 | Correct the `kademlia_dht_architecture.html` hexdocs page | after 1–5 land | macula |
| 10 | **NEW** — second sequential `call_station` on one pool fails | — | macula (likely `macula_client`'s pool/link reuse) |

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

### Slice 4 — Composite-key wildcard capability discovery (B1)

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

### Slice 5 — Wildcard pubsub topic subscription (B2)

**What:** genuinely different from slice 4 — this is relay-side fan-out
matching, not DHT lookup. Needs investigation into the actual current
subscription/fan-out path (`macula-station`'s pubsub routing —
`macula_station_route_pubsub_frames` per prior-session notes, plus whatever
`hecate_pubsub`/`hecate_pubsub_server`/`hecate_pubsub_registry` in the `macula`
SDK, shipped 10.11.0 this session, actually do) before designing further. At
minimum: a subscriber registering a pattern (containing `*`) needs the
station's topic registry to hold it as a pattern, not an exact string; on
PUBLISH, in addition to the existing O(1) exact-match lookup, check registered
patterns against the concrete published topic using slice 3's primitive.
Given the number of *distinct patterns* actually registered is expected to be
small relative to publish volume, a linear scan of registered patterns
(bounded, not per-message-expensive at any realistic subscription count) is
probably sufficient — don't build a trie/index for this prematurely; revisit
only if a live test or real traffic shows it matters.

**DONE-WHEN:** a subscriber registered on `realm/*/app/domain/name_v1`
receives a publish to `realm/acme/app/domain/name_v1` **and** one to
`realm/contoso/app/domain/name_v1`, live, and a subscriber on the exact string
still only receives its own exact topic (no regression on existing exact-match
subscribers).

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

**DONE-WHEN**: root cause identified (macula pool link-reuse logic vs.
macula-station connection policy, or something else) and either fixed, or
determined to be intended behavior with `hecate_om_capabilities`'s own usage
pattern needing to change (e.g., cache/reuse the resolved `Url` per station
rather than re-resolving and re-dialing on every call). A NEW live test
(one pool, two sequential `call_capability` calls, no org-scoping involved at
all — the minimal repro) should be written first, to pin this down
independently of everything else in this plan.

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

## Success criteria

- [x] `org_scoped_call_reaches_only_the_targeted_org_test_` passes live — **2026-08-29**
- [x] Existing single-org live test still passes (no regression) — **2026-08-29**
- [x] A capability's TTL is proportioned to its republish interval, not the
      envelope default — **DONE in code; the handler-bearing path's DHT
      record has no LIVE effect yet, blocked on a macula hex release past
      10.11.1 (not published this session — user fires hex publishes)**
- [ ] A wildcard capability query resolves correctly against two orgs sharing
      a station, live
- [ ] A wildcard pubsub subscription receives publishes from every matching
      concrete topic, live, with no over-delivery to exact subscribers
- [ ] `macula_dht_lookup.erl` either removed or corrected — no comment in the
      tree describes a fixed bug as current
- [ ] `read_model_services.md` correctly attributes the discovery ceiling
- [ ] hexdocs Kademlia page matches the settled, live architecture
- [ ] **NEW** — second sequential `call_station` on one pool root-caused and
      fixed (slice 10)
