# Kademlia DHT architecture

Replaces `macula.hexdocs.pm/0.9.1/kademlia_dht_architecture.html`, a page
frozen inside an old macula SDK hex release. Hex releases are immutable past
one hour, so that page cannot be corrected — and there is nothing to correct
it *into* in the current `macula` repo either: DHT/Kademlia lives entirely in
this repo now (`macula-io/macula` is the client SDK; the relay, including the
DHT, is `macula-station` — see both repos' own `CLAUDE.md`). The old page
predates that split. This guide documents what is actually here, in
`apps/macula_dht/`, verified against source, not carried forward from a page
that had drifted (its own version header and footer didn't even agree with
each other).

`macula-station` is not published to hex, so this lives as a plain repo doc,
not a hexdocs page — the accurate, discoverable place for it now is here,
next to the code.

## Routing table

`macula_dht_routing_table` — 256 buckets, addressed by XOR common-prefix
length between a station's own 256-bit NodeId and a peer's. Bucket `i` holds
peers agreeing with `Self` on the first `(255 - i)` bits; bucket `255` (the
closest peers) differs only in the last bit. Bucket capacity (`k`) is 20 —
the standard Kademlia parameter, matching `macula_dht_lookup`'s own
`?DEFAULT_TARGET_COUNT`. The table is a pure value (insertions return a new
table); `macula_dht_bucket` owns per-bucket scoring/eviction, `macula_dht_server`
owns process-level state.

## Two lookup implementations exist. Only one is live.

**`macula_dht_lookup:lookup_nodes/2,3`** — a textbook S/Kademlia disjoint-path
walk: `d=3` parallel, non-overlapping paths, `alpha=3` concurrent queries per
path, terminates on deadline or all-paths-idle, returns the deduped top-N by
XOR distance. Its only production caller is `macula_station_bootstrap_runner`'s
self-lookup at boot (populate the routing table when a station joins).
Until 2026-08-29 this walk was effectively one-hop-wide in production: real
station addresses reached it (fixed 2026-07-27), but nothing dialed a peer
learned mid-walk before querying it — `macula_dht_server`'s injected
`send_frame` only resolves an EXISTING connection, never dials one. Fixed via
dependency injection (an optional `dial` callback in `lookup_nodes/3`'s opts,
since this app has no dependency on `macula_station`, home of the actual
dialer) — see this module's own moduledoc for the full history.

**`macula_station_dht_handlers`'s own `walk_find_value/5`** — a separate,
hand-rolled multi-round walk (bounded to 3 rounds, width 5 candidates/round),
NOT built on `macula_dht_lookup` at all. This is the one that actually answers
every client-facing `_dht.find_record`/`_dht.find_records` RPC call on a local
store miss — i.e., the thing `hecate_om_capabilities`'s whole resolution path
(`call_capability`, `list_org_capabilities`) rides on, end to end, live-verified
against the real fleet. It independently reinvents dial-on-demand
(`macula_station_dht_dialer:ensure_dialed/3`, called directly at this walk's
own call site) rather than sharing `macula_dht_lookup`'s dial-injection
mechanism.

**Two implementations of the same idea, only one exercised by real traffic.**
Flagged, not resolved, by the session that found it (2026-08-29) — worth
consolidating onto one, but which one absorbs the other is a real design
question (the disjoint-path walk is more principled/Sybil-resistant; the
hand-rolled one is the one already proven live), not a quick fix. See
`plans/PLAN_ORG_SCOPED_DISPATCH_AND_WILDCARD_DISCOVERY.md`'s slice 7 for the
investigation that found this.

## What a lookup actually does, end to end

A client's `macula:find_records/2` is one RPC to whichever station it's
connected to (`_dht.find_record`/`_dht.find_records`). That station:

1. Checks its own local store. Hit → return immediately.
2. Miss → `walk_find_value/5`: round 0 queries every peer already in this
   station's own k-closest-to-key routing-table entries (already connected,
   no dial needed) in parallel. Each reply is either the value (done) or a
   NODES list of that peer's own k-closest — folded into the next round's
   candidate pool, deduped against everyone already queried, dialing any
   genuinely new peer on demand and verifying its handshake-proven NodeId
   against the one the walk was chasing before trusting the connection.
3. Bounded three ways: round cap, a `Seen` set (no NodeId queried twice even
   across a NODES-reply cycle), and every round only ever walking toward the
   key (a station's own k-closest-to-key is by construction closer than
   whoever offered it).

This is what makes "the consumer knows the key, the directly-connected
station doesn't hold it" already solved today — not through gossip or hope,
through a real bounded multi-hop walk.

## Wildcard / prefix queries are a different, separate problem

A DHT key here is `crypto:hash(sha256, <the whole procedure/topic string>)` —
opaque by design. There is no way to route toward "a key with this prefix"
the way you can route toward an exact key: hashing is precisely what destroys
that structure. Extending discovery to wildcard/prefix patterns (`org/*`,
`*/app/domain/name`) does **not** need a new lookup algorithm — every such
query still resolves via the exact-key walk above, just against a small,
bounded set of *composite* keys (prefix keys, or per-segment secondary
indices) published alongside the primary record. See
`hecate_om_capabilities:list_org_capabilities/1`'s own design note for why
even that turned out unnecessary for capability records specifically
(a `procedure_advertisement`'s storage key is derived from its own payload,
so a client-side filter over `find_records_by_type` served the actual need
without new DHT machinery at all) — the option remains open for a case that
genuinely needs it.
