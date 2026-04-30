%% EUnit tests for macula_dht_diversity.
-module(macula_dht_diversity_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Distinct counters
%%---------------------------------------------------------------------

distinct_counts_on_empty_bucket_test() ->
    ?assertEqual(#{asns => 0, countries => 0, tiers => 0},
                 macula_dht_diversity:counts([])).

distinct_counts_unique_entries_test() ->
    Entries = [
        entry(1, 100, <<"BE">>, t0),
        entry(2, 200, <<"NL">>, t1),
        entry(3, 300, <<"FR">>, t2)
    ],
    ?assertEqual(#{asns => 3, countries => 3, tiers => 3},
                 macula_dht_diversity:counts(Entries)).

distinct_counts_duplicate_asns_test() ->
    Entries = [
        entry(1, 100, <<"BE">>, t0),
        entry(2, 100, <<"NL">>, t0),
        entry(3, 100, <<"FR">>, t0)
    ],
    ?assertEqual(#{asns => 1, countries => 3, tiers => 1},
                 macula_dht_diversity:counts(Entries)).

%%---------------------------------------------------------------------
%% Soft constraints
%%---------------------------------------------------------------------

empty_bucket_tier_exempt_test() ->
    #{tier_ok := Ok} = macula_dht_diversity:bucket_constraints_met([]),
    ?assert(Ok).

small_bucket_tier_exempt_test() ->
    Entries = [entry(N, 100, <<"BE">>, t0) || N <- lists:seq(1, 7)],
    #{tier_ok := Ok} = macula_dht_diversity:bucket_constraints_met(Entries),
    ?assert(Ok).

large_bucket_tier_checked_test() ->
    %% 8 entries, all tier t0 => fails tier_ok (needs ≥2 tiers).
    Entries = [entry(N, 100, <<"BE">>, t0) || N <- lists:seq(1, 8)],
    #{tier_ok := Ok} = macula_dht_diversity:bucket_constraints_met(Entries),
    ?assertNot(Ok).

large_bucket_two_tiers_passes_test() ->
    Base = [entry(N, 100, <<"BE">>, t0) || N <- lists:seq(1, 7)],
    Entries = [entry(8, 999, <<"DE">>, t1) | Base],
    Report = macula_dht_diversity:bucket_constraints_met(Entries),
    ?assertMatch(#{tier_ok := true}, Report).

asn_constraint_fails_below_min_test() ->
    Entries = [entry(N, 100, <<"BE">>, t0) || N <- lists:seq(1, 20)],
    Report = macula_dht_diversity:bucket_constraints_met(Entries),
    ?assertMatch(#{asn_ok := false}, Report).

country_constraint_fails_below_min_test() ->
    %% 20 entries, 5 ASNs, 2 countries — countries fail.
    Entries = mixed_bucket(),
    Report = macula_dht_diversity:bucket_constraints_met(Entries),
    ?assertMatch(#{country_ok := false}, Report).

all_constraints_pass_on_diverse_bucket_test() ->
    Entries = diverse_bucket(),
    Report = macula_dht_diversity:bucket_constraints_met(Entries),
    ?assertEqual(#{asn_ok => true, country_ok => true, tier_ok => true},
                 Report).

custom_constraints_honoured_test() ->
    Entries = [entry(N, 100, <<"BE">>, t0) || N <- lists:seq(1, 10)],
    Custom = #{asn_min => 0, country_min => 0,
               tier_min => 1, tier_min_members => 8},
    ?assertEqual(#{asn_ok => true, country_ok => true, tier_ok => true},
                 macula_dht_diversity:bucket_constraints_met(Entries, Custom)).

%%---------------------------------------------------------------------
%% Novelty score
%%---------------------------------------------------------------------

novelty_full_when_bucket_empty_test() ->
    Cand = entry(9, 999, <<"JP">>, t3),
    %% empty bucket sees every value as new
    ?assertEqual(3.0, macula_dht_diversity:novelty_score(Cand, [])).

novelty_zero_when_everything_duplicates_test() ->
    Entries = [entry(1, 100, <<"BE">>, t0),
               entry(2, 100, <<"BE">>, t0)],
    Cand = entry(3, 100, <<"BE">>, t0),
    ?assertEqual(0.0, macula_dht_diversity:novelty_score(Cand, Entries)).

novelty_partial_one_new_dimension_test() ->
    Entries = [entry(1, 100, <<"BE">>, t0)],
    %% new ASN, same country, same tier -> 1.0
    Cand = entry(2, 200, <<"BE">>, t0),
    ?assertEqual(1.0, macula_dht_diversity:novelty_score(Cand, Entries)).

novelty_partial_two_new_dimensions_test() ->
    Entries = [entry(1, 100, <<"BE">>, t0)],
    %% new ASN + new country, same tier -> 2.0
    Cand = entry(2, 200, <<"NL">>, t0),
    ?assertEqual(2.0, macula_dht_diversity:novelty_score(Cand, Entries)).

novelty_bounded_by_weights_test() ->
    Entries = [entry(1, 100, <<"BE">>, t0)],
    Cand    = entry(2, 999, <<"JP">>, t3),
    Score   = macula_dht_diversity:novelty_score(Cand, Entries),
    ?assert(Score >= 0.0 andalso Score =< 3.0),
    ?assertEqual(3.0, Score).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

entry(NodeIx, Asn, Country, Tier) ->
    macula_dht_entry:new(#{
        node_id => <<NodeIx:256>>,
        asn     => Asn,
        country => Country,
        tier    => Tier
    }, 0).

%% 20 entries: 5 ASNs, 2 countries, 2 tiers (so country_ok fails)
mixed_bucket() ->
    Asns      = [100, 200, 300, 400, 500],
    Countries = [<<"BE">>, <<"NL">>],
    Tiers     = [t0, t1],
    [entry(
         N,
         lists:nth(1 + (N rem 5), Asns),
         lists:nth(1 + (N rem 2), Countries),
         lists:nth(1 + (N rem 2), Tiers))
     || N <- lists:seq(1, 20)].

%% 20 entries passing all soft constraints (5 ASNs, 5 countries, 4 tiers)
diverse_bucket() ->
    Asns      = [100, 200, 300, 400, 500],
    Countries = [<<"BE">>, <<"NL">>, <<"FR">>, <<"DE">>, <<"ES">>],
    Tiers     = [t0, t1, t2, t3],
    [entry(
         N,
         lists:nth(1 + (N rem 5), Asns),
         lists:nth(1 + (N rem 5), Countries),
         lists:nth(1 + (N rem 4), Tiers))
     || N <- lists:seq(1, 20)].
