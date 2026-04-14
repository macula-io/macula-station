%% EUnit tests for hecate_dht_placement — the pure greedy algorithm.
-module(hecate_dht_placement_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Defaults + shape
%%---------------------------------------------------------------------

default_constraints_have_expected_fields_test() ->
    C = hecate_dht_placement:default_constraints(),
    ?assertEqual(20, maps:get(k, C)),
    ?assertEqual(8,  maps:get(asn_min, C)),
    ?assertEqual(5,  maps:get(country_min, C)),
    ?assertEqual(3,  maps:get(tier_min, C)),
    ?assertEqual(3,  maps:get(operator_max, C)).

empty_candidate_pool_returns_empty_chosen_test() ->
    R = hecate_dht_placement:place(<<0:256>>, []),
    ?assertEqual([], maps:get(chosen, R)),
    ?assertEqual(true, maps:get(degraded, R)).

%%---------------------------------------------------------------------
%% Candidates sorted by distance, truncated to K
%%---------------------------------------------------------------------

chosen_ordered_by_distance_and_capped_at_k_test() ->
    Key = <<0:256>>,
    Refs = [ref(I, 100, <<"BE">>, 0) || I <- lists:seq(1, 10)],
    R = hecate_dht_placement:place(Key, Refs, small_constraints(5)),
    Chosen = maps:get(chosen, R),
    ?assertEqual(5, length(Chosen)),
    Ids = [maps:get(node_id, X) || X <- Chosen],
    Distances = [hecate_dht_xor:distance_int(Key, Id) || Id <- Ids],
    ?assertEqual(Distances, lists:sort(Distances)).

%%---------------------------------------------------------------------
%% Operator cap caps per-ASN occupancy
%%---------------------------------------------------------------------

operator_cap_limits_same_asn_replicas_test() ->
    %% Ten peers all in ASN 100 — operator_max = 3 means only 3
    %% accepted even though K = 5 slots are available.
    Refs = [ref(I, 100, <<"BE">>, 0) || I <- lists:seq(1, 10)],
    R = hecate_dht_placement:place(<<0:256>>, Refs,
                                   small_constraints(5, 3)),
    ?assertEqual(3, length(maps:get(chosen, R))),
    ?assert(maps:get(degraded, R)).

%%---------------------------------------------------------------------
%% Diversity satisfied when pool permits
%%---------------------------------------------------------------------

diversity_constraints_met_when_pool_has_enough_variety_test() ->
    %% Pool of 20 peers: 4 ASNs × 5 countries, with tiers cycling 0..3.
    %% asn_min=2, country_min=3, tier_min=2 should be trivially met.
    Refs = [ref(I, 100 + (I rem 4), country(I rem 5), I rem 4)
            || I <- lists:seq(1, 20)],
    R = hecate_dht_placement:place(<<0:256>>, Refs,
                                   #{k => 5, asn_min => 2,
                                     country_min => 3, tier_min => 2,
                                     operator_max => 3}),
    ?assertEqual(5, length(maps:get(chosen, R))),
    ?assertNot(maps:get(degraded, R)),
    #{asns := A, countries := C, tiers := T} = maps:get(counts, R),
    ?assert(A >= 2),
    ?assert(C >= 3),
    ?assert(T >= 2).

%%---------------------------------------------------------------------
%% Greedy prefers diversity to satisfy deficit
%%---------------------------------------------------------------------

greedy_picks_diverse_when_pool_limited_test() ->
    %% K=4, asn_min=3. First 10 candidates (closest) are all ASN 100,
    %% followed by 3 peers in distinct ASNs. The algorithm must reach
    %% into the tail to satisfy asn_min.
    SameAsnClose = [ref(I, 100, <<"BE">>, 0) || I <- lists:seq(1, 10)],
    DiverseTail = [ref(1_000 + N, 100 + N, country(N), N)
                   || N <- lists:seq(1, 3)],
    Refs = SameAsnClose ++ DiverseTail,
    R = hecate_dht_placement:place(<<0:256>>, Refs,
                                   #{k => 4, asn_min => 3,
                                     country_min => 1, tier_min => 1,
                                     operator_max => 20}),
    Chosen = maps:get(chosen, R),
    ?assertEqual(4, length(Chosen)),
    %% Three ASNs must be present.
    Asns = [maps:get(asn, X) || X <- Chosen],
    ?assertEqual(3, length(lists:usort(Asns))),
    ?assertNot(maps:get(degraded, R)).

%%---------------------------------------------------------------------
%% Duplicate NodeId filtered
%%---------------------------------------------------------------------

duplicate_candidates_are_deduped_test() ->
    Dup = ref(1, 100, <<"BE">>, 0),
    Refs = [Dup, Dup, Dup, ref(2, 200, <<"NL">>, 1)],
    R = hecate_dht_placement:place(<<0:256>>, Refs,
                                   small_constraints(4, 20)),
    Chosen = maps:get(chosen, R),
    ?assertEqual(2, length(Chosen)),
    Ids = [maps:get(node_id, X) || X <- Chosen],
    ?assertEqual(2, length(lists:usort(Ids))).

%%---------------------------------------------------------------------
%% Degraded flag when constraints impossible
%%---------------------------------------------------------------------

degraded_when_asn_minimum_unreachable_test() ->
    %% 5 peers all in ASN 100; asn_min=3 — unreachable.
    Refs = [ref(I, 100, <<"BE">>, 0) || I <- lists:seq(1, 5)],
    R = hecate_dht_placement:place(<<0:256>>, Refs,
                                   #{k => 5, asn_min => 3,
                                     country_min => 1, tier_min => 1,
                                     operator_max => 10}),
    ?assert(maps:get(degraded, R)).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

ref(N, Asn, Country, Tier) ->
    #{node_id      => <<N:256>>,
      station_id   => <<N:256>>,
      addresses    => [],
      tier         => Tier,
      asn          => Asn,
      country      => Country,
      last_seen_at => 1}.

country(0) -> <<"BE">>;
country(1) -> <<"NL">>;
country(2) -> <<"FR">>;
country(3) -> <<"DE">>;
country(4) -> <<"JP">>.

small_constraints(K) ->
    small_constraints(K, 20).

small_constraints(K, OpMax) ->
    #{k => K, asn_min => 1, country_min => 1, tier_min => 1,
      operator_max => OpMax}.
