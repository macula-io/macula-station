%% EUnit tests for macula_dht_bucket.
-module(macula_dht_bucket_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Construction / inspection
%%---------------------------------------------------------------------

new_default_is_empty_k20_test() ->
    B = macula_dht_bucket:new(),
    ?assertEqual(20, macula_dht_bucket:capacity(B)),
    ?assertEqual(0,  macula_dht_bucket:size(B)),
    ?assertEqual([], macula_dht_bucket:members(B)).

new_with_custom_capacity_test() ->
    B = macula_dht_bucket:new(4),
    ?assertEqual(4, macula_dht_bucket:capacity(B)).

new_with_zero_capacity_rejected_test() ->
    ?assertError(function_clause, macula_dht_bucket:new(0)).

%%---------------------------------------------------------------------
%% Insert — space available
%%---------------------------------------------------------------------

insert_into_empty_admits_test() ->
    B = macula_dht_bucket:new(4),
    E = entry(1, 100, <<"BE">>, t0),
    {admitted, B1} = macula_dht_bucket:insert(E, B, 0),
    ?assertEqual(1, macula_dht_bucket:size(B1)),
    ?assert(macula_dht_bucket:contains(id(1), B1)).

insert_distinct_ids_all_admitted_test() ->
    B0 = macula_dht_bucket:new(4),
    B1 = admit_all(B0, [entry(N, 100 + N, <<"BE">>, t0) || N <- lists:seq(1, 4)]),
    ?assertEqual(4, macula_dht_bucket:size(B1)).

%%---------------------------------------------------------------------
%% Insert — duplicate NodeId
%%---------------------------------------------------------------------

insert_existing_touches_test() ->
    B0 = macula_dht_bucket:new(4),
    E  = macula_dht_entry:new(base_spec(1, 100, <<"BE">>, t0), 50),
    {admitted, B1} = macula_dht_bucket:insert(E, B0, 100),
    {touched,  B2} = macula_dht_bucket:insert(E, B1, 900),
    {ok, Found} = macula_dht_bucket:find(id(1), B2),
    ?assertEqual(900, macula_dht_entry:last_seen(Found)),
    ?assertEqual(50,  macula_dht_entry:first_seen(Found)),
    ?assertEqual(1,   macula_dht_bucket:size(B2)).

%%---------------------------------------------------------------------
%% Insert — full bucket, scoring
%%---------------------------------------------------------------------

insert_full_rejects_weaker_candidate_test() ->
    %% fill a 2-slot bucket with strong (uptime=1.0) incumbents
    B0 = macula_dht_bucket:new(2),
    Strong1 = strong(1, 100, <<"BE">>, t0),
    Strong2 = strong(2, 200, <<"NL">>, t1),
    {admitted, B1} = macula_dht_bucket:insert(Strong1, B0, 0),
    {admitted, B2} = macula_dht_bucket:insert(Strong2, B1, 0),
    Weak = weak(3, 300, <<"FR">>, t2),
    {rejected, B3} = macula_dht_bucket:insert(Weak, B2, 1_000),
    ?assertEqual(B2, B3).

insert_full_evicts_weakest_when_candidate_stronger_test() ->
    B0 = macula_dht_bucket:new(2),
    Weak   = weak(1, 100, <<"BE">>, t0),
    Strong = strong(2, 200, <<"BE">>, t0),
    {admitted, B1} = macula_dht_bucket:insert(Weak,   B0, 0),
    {admitted, B2} = macula_dht_bucket:insert(Strong, B1, 0),
    Cand = strong(3, 300, <<"JP">>, t3),
    {replaced, Evicted, B3} = macula_dht_bucket:insert(Cand, B2, 1_000),
    ?assertEqual(id(1), macula_dht_entry:node_id(Evicted)),
    ?assertEqual(2, macula_dht_bucket:size(B3)),
    ?assertNot(macula_dht_bucket:contains(id(1), B3)),
    ?assert(macula_dht_bucket:contains(id(3), B3)).

insert_full_prefers_diverse_candidate_over_duplicate_asn_test() ->
    %% bucket full of same-ASN same-country peers; diverse candidate
    %% with equivalent uptime should win on novelty.
    B0 = macula_dht_bucket:new(2),
    A  = strong(1, 100, <<"BE">>, t0),
    B  = strong(2, 100, <<"BE">>, t0),
    {admitted, B1} = macula_dht_bucket:insert(A, B0, 0),
    {admitted, B2} = macula_dht_bucket:insert(B, B1, 0),
    Diverse = strong(3, 999, <<"JP">>, t3),
    {replaced, _Evicted, _B3} = macula_dht_bucket:insert(Diverse, B2, 1_000),
    ok.

%%---------------------------------------------------------------------
%% Remove / touch
%%---------------------------------------------------------------------

remove_drops_by_id_test() ->
    B0 = admit_all(macula_dht_bucket:new(4),
                   [entry(N, 100, <<"BE">>, t0) || N <- [1, 2, 3]]),
    B1 = macula_dht_bucket:remove(id(2), B0),
    ?assertEqual(2, macula_dht_bucket:size(B1)),
    ?assertNot(macula_dht_bucket:contains(id(2), B1)).

remove_missing_is_noop_test() ->
    B0 = admit_all(macula_dht_bucket:new(4),
                   [entry(N, 100, <<"BE">>, t0) || N <- [1, 2]]),
    B1 = macula_dht_bucket:remove(id(99), B0),
    ?assertEqual(B0, B1).

touch_updates_last_seen_test() ->
    E = entry(1, 100, <<"BE">>, t0),
    {admitted, B0} = macula_dht_bucket:insert(E, macula_dht_bucket:new(), 100),
    B1 = macula_dht_bucket:touch(id(1), B0, 900),
    {ok, Found} = macula_dht_bucket:find(id(1), B1),
    ?assertEqual(900, macula_dht_entry:last_seen(Found)).

%%---------------------------------------------------------------------
%% Score (sanity)
%%---------------------------------------------------------------------

score_empty_bucket_is_finite_test() ->
    S = macula_dht_bucket:score(entry(1, 100, <<"BE">>, t0), [], 0),
    ?assert(is_float(S)).

score_strong_beats_weak_when_equal_context_test() ->
    W = weak(1, 100, <<"BE">>, t0),
    S = strong(1, 100, <<"BE">>, t0),
    ?assert(macula_dht_bucket:score(S, [], 0)
              > macula_dht_bucket:score(W, [], 0)).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

id(N) -> <<N:256>>.

entry(N, Asn, Country, Tier) ->
    macula_dht_entry:new(base_spec(N, Asn, Country, Tier), 0).

base_spec(N, Asn, Country, Tier) ->
    #{
        node_id => id(N),
        asn     => Asn,
        country => Country,
        tier    => Tier
    }.

strong(N, Asn, Country, Tier) ->
    E = entry(N, Asn, Country, Tier),
    macula_dht_entry:with_uptime(
      macula_dht_entry:with_latency(E, 30), 0.95).

weak(N, Asn, Country, Tier) ->
    E = entry(N, Asn, Country, Tier),
    macula_dht_entry:with_uptime(
      macula_dht_entry:with_latency(E, 450), 0.05).

admit_all(Bucket, Entries) ->
    lists:foldl(fun(E, B) ->
        {admitted, B1} = macula_dht_bucket:insert(E, B, 0),
        B1
    end, Bucket, Entries).
