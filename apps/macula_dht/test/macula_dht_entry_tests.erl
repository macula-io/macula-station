%% EUnit tests for macula_dht_entry.
-module(macula_dht_entry_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Construction
%%---------------------------------------------------------------------

new_defaults_test() ->
    E = mk_entry(),
    ?assert(macula_dht_entry:is_entry(E)),
    ?assertEqual([], macula_dht_entry:endpoints(E)),
    ?assertEqual(undefined, macula_dht_entry:latency(E)),
    ?assertEqual(0.0, macula_dht_entry:uptime(E)).

new_respects_injected_now_test() ->
    E = macula_dht_entry:new(base_spec(), 1_000),
    ?assertEqual(1_000, macula_dht_entry:first_seen(E)),
    ?assertEqual(1_000, macula_dht_entry:last_seen(E)).

new_accepts_all_four_tiers_test() ->
    [?assert(macula_dht_entry:is_entry(
                 macula_dht_entry:new(spec_with(tier, T), 0)))
     || T <- [t0, t1, t2, t3]].

new_rejects_bad_tier_test() ->
    ?assertError(_,
        macula_dht_entry:new(spec_with(tier, rogue), 0)).

new_rejects_bad_asn_test() ->
    ?assertError(_,
        macula_dht_entry:new(spec_with(asn, -1), 0)).

new_rejects_bad_country_test() ->
    ?assertError(_,
        macula_dht_entry:new(spec_with(country, <<"BEL">>), 0)).

new_rejects_bad_nodeid_length_test() ->
    ?assertError(_,
        macula_dht_entry:new(spec_with(node_id, <<1, 2, 3>>), 0)).

new_clamps_uptime_high_test() ->
    E = macula_dht_entry:new(spec_with(uptime_frac, 2.5), 0),
    ?assertEqual(1.0, macula_dht_entry:uptime(E)).

new_clamps_uptime_low_test() ->
    E = macula_dht_entry:new(spec_with(uptime_frac, -0.3), 0),
    ?assertEqual(0.0, macula_dht_entry:uptime(E)).

new_accepts_endpoints_list_test() ->
    Eps = [{quic, <<"1.2.3.4">>, 443}, {quic6, <<"::1">>, 443}],
    E = macula_dht_entry:new((base_spec())#{endpoints => Eps}, 0),
    ?assertEqual(Eps, macula_dht_entry:endpoints(E)).

%%---------------------------------------------------------------------
%% Accessors
%%---------------------------------------------------------------------

accessors_round_trip_test() ->
    E = macula_dht_entry:new(base_spec(), 500),
    ?assertEqual(<<0:256>>, macula_dht_entry:node_id(E)),
    ?assertEqual(20473,     macula_dht_entry:asn(E)),
    ?assertEqual(<<"BE">>,  macula_dht_entry:country(E)),
    ?assertEqual(t0,        macula_dht_entry:tier(E)).

%%---------------------------------------------------------------------
%% Mutators
%%---------------------------------------------------------------------

touch_updates_last_seen_only_test() ->
    E  = macula_dht_entry:new(base_spec(), 100),
    E1 = macula_dht_entry:touch(E, 900),
    ?assertEqual(100, macula_dht_entry:first_seen(E1)),
    ?assertEqual(900, macula_dht_entry:last_seen(E1)).

with_latency_sets_field_test() ->
    E  = mk_entry(),
    E1 = macula_dht_entry:with_latency(E, 42),
    ?assertEqual(42, macula_dht_entry:latency(E1)).

with_latency_rejects_negative_test() ->
    ?assertError(function_clause,
                 macula_dht_entry:with_latency(mk_entry(), -1)).

with_uptime_clamps_test() ->
    E = macula_dht_entry:with_uptime(mk_entry(), 5),
    ?assertEqual(1.0, macula_dht_entry:uptime(E)).

%%---------------------------------------------------------------------
%% Timings
%%---------------------------------------------------------------------

age_ms_is_now_minus_last_seen_test() ->
    E = macula_dht_entry:new(base_spec(), 100),
    ?assertEqual(400, macula_dht_entry:age_ms(E, 500)).

age_ms_not_negative_for_time_skew_test() ->
    E = macula_dht_entry:new(base_spec(), 500),
    ?assertEqual(0, macula_dht_entry:age_ms(E, 400)).

time_in_table_is_now_minus_first_seen_test() ->
    E  = macula_dht_entry:new(base_spec(), 100),
    E1 = macula_dht_entry:touch(E, 500),
    ?assertEqual(900, macula_dht_entry:time_in_table_ms(E1, 1_000)).

%%---------------------------------------------------------------------
%% Predicate
%%---------------------------------------------------------------------

is_entry_negative_cases_test() ->
    ?assertNot(macula_dht_entry:is_entry(#{})),
    ?assertNot(macula_dht_entry:is_entry(foo)),
    ?assertNot(macula_dht_entry:is_entry(#{node_id => <<0:8>>})).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

mk_entry() ->
    macula_dht_entry:new(base_spec(), 0).

base_spec() ->
    #{
        node_id => <<0:256>>,
        asn     => 20473,
        country => <<"BE">>,
        tier    => t0
    }.

spec_with(Key, Value) ->
    maps:put(Key, Value, base_spec()).
