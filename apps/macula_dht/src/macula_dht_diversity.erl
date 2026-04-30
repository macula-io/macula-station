%% @doc Diversity primitives for tier-diverse Kademlia buckets.
%%
%% S/Kademlia resists eclipse + Sybil attacks by *preferring routing-table
%% entries that diversify ASN, country, and deployment tier* over entries
%% that bunch peers on a single uplink or provider. This module provides
%% the pure arithmetic of that preference:
%%
%% <ul>
%%   <li>Count distinct ASN/country/tier in a bucket.</li>
%%   <li>Check bucket-level soft constraints from Part 3 §4.3.</li>
%%   <li>Score how much *novelty* a candidate entry adds to a bucket
%%       (used by `macula_dht_bucket' when ranking admit vs. evict).</li>
%% </ul>
%%
%% This module owns *no* state and *no* thresholds for replica placement
%% — that is `macula_dht_placement' (Session 3.8).
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §4.3.
-module(macula_dht_diversity).

-export([
    distinct_asns/1,
    distinct_countries/1,
    distinct_tiers/1,
    counts/1,
    soft_constraints/0,
    bucket_constraints_met/1,
    bucket_constraints_met/2,
    novelty_score/2
]).

-export_type([constraints/0, counts/0, report/0]).

%% Soft constraint defaults from Part 3 §4.3. Buckets below the member
%% threshold (`tier_min_members') are exempt from the tier constraint.
-define(DEF_ASN_MIN,          5).
-define(DEF_COUNTRY_MIN,      3).
-define(DEF_TIER_MIN,         2).
-define(DEF_TIER_MIN_MEMBERS, 8).

%% Novelty weights. Per-dimension 1.0 keeps `novelty_score/2' in `[0, 3]'
%% and lets the bucket-ranking formula combine it with other signals
%% (uptime, latency, incumbency) without dominating them.
-define(W_ASN,     1.0).
-define(W_COUNTRY, 1.0).
-define(W_TIER,    1.0).

-type constraints() :: #{
    asn_min          := non_neg_integer(),
    country_min      := non_neg_integer(),
    tier_min         := non_neg_integer(),
    tier_min_members := non_neg_integer()
}.

-type counts() :: #{
    asns      := non_neg_integer(),
    countries := non_neg_integer(),
    tiers     := non_neg_integer()
}.

-type report() :: #{
    asn_ok     := boolean(),
    country_ok := boolean(),
    tier_ok    := boolean()
}.

%%=====================================================================
%% Distinct-value counters
%%=====================================================================

-spec distinct_asns([macula_dht_entry:entry()]) -> non_neg_integer().
distinct_asns(Entries) ->
    distinct(fun macula_dht_entry:asn/1, Entries).

-spec distinct_countries([macula_dht_entry:entry()]) -> non_neg_integer().
distinct_countries(Entries) ->
    distinct(fun macula_dht_entry:country/1, Entries).

-spec distinct_tiers([macula_dht_entry:entry()]) -> non_neg_integer().
distinct_tiers(Entries) ->
    distinct(fun macula_dht_entry:tier/1, Entries).

-spec counts([macula_dht_entry:entry()]) -> counts().
counts(Entries) ->
    #{
        asns      => distinct_asns(Entries),
        countries => distinct_countries(Entries),
        tiers     => distinct_tiers(Entries)
    }.

%%=====================================================================
%% Soft constraints
%%=====================================================================

-spec soft_constraints() -> constraints().
soft_constraints() ->
    #{
        asn_min          => ?DEF_ASN_MIN,
        country_min      => ?DEF_COUNTRY_MIN,
        tier_min         => ?DEF_TIER_MIN,
        tier_min_members => ?DEF_TIER_MIN_MEMBERS
    }.

-spec bucket_constraints_met([macula_dht_entry:entry()]) -> report().
bucket_constraints_met(Entries) ->
    bucket_constraints_met(Entries, soft_constraints()).

-spec bucket_constraints_met([macula_dht_entry:entry()], constraints()) -> report().
bucket_constraints_met(Entries, Cs) ->
    #{asns := A, countries := C, tiers := T} = counts(Entries),
    #{asn_min := Am, country_min := Cm,
      tier_min := Tm, tier_min_members := TmMembers} = Cs,
    #{
        asn_ok     => A >= Am,
        country_ok => C >= Cm,
        tier_ok    => tier_ok(length(Entries), T, Tm, TmMembers)
    }.

%% @doc Bounded novelty score for a candidate relative to a bucket.
%%
%% The result is in `[0, ?W_ASN + ?W_COUNTRY + ?W_TIER]'. One weight is
%% added for each dimension the candidate introduces to the bucket. An
%% entry that duplicates every dimension scores `0'; one that is new in
%% ASN, country, and tier scores the full sum.
-spec novelty_score(macula_dht_entry:entry(), [macula_dht_entry:entry()]) -> float().
novelty_score(Candidate, Entries) ->
    Asns      = set_of(fun macula_dht_entry:asn/1,     Entries),
    Countries = set_of(fun macula_dht_entry:country/1, Entries),
    Tiers     = set_of(fun macula_dht_entry:tier/1,    Entries),
    bonus(macula_dht_entry:asn(Candidate),     Asns,      ?W_ASN)
      + bonus(macula_dht_entry:country(Candidate), Countries, ?W_COUNTRY)
      + bonus(macula_dht_entry:tier(Candidate),    Tiers,     ?W_TIER).

%%=====================================================================
%% Internal
%%=====================================================================

-spec distinct(fun((macula_dht_entry:entry()) -> term()),
               [macula_dht_entry:entry()]) -> non_neg_integer().
distinct(F, Entries) ->
    sets:size(set_of(F, Entries)).

-spec set_of(fun((macula_dht_entry:entry()) -> T),
             [macula_dht_entry:entry()]) -> sets:set(T).
set_of(F, Entries) ->
    sets:from_list([F(E) || E <- Entries]).

-spec tier_ok(non_neg_integer(), non_neg_integer(),
              non_neg_integer(), non_neg_integer()) -> boolean().
tier_ok(Len, _Tiers, _Min, MinMembers) when Len < MinMembers -> true;
tier_ok(_Len, Tiers, Min, _MinMembers)                        -> Tiers >= Min.

-spec bonus(term(), sets:set(term()), float()) -> float().
bonus(Value, Set, Weight) ->
    is_novel(sets:is_element(Value, Set), Weight).

-spec is_novel(boolean(), float()) -> float().
is_novel(true,  _W) -> 0.0;
is_novel(false,  W) -> W.
