-module(hecate_content_dht_tests).
-include_lib("eunit/include/eunit.hrl").

mcid()  -> <<1, 1, (crypto:strong_rand_bytes(32))/binary>>.
node_id() -> crypto:strong_rand_bytes(32).

dht_key_is_32_bytes_test() ->
    ?assertEqual(32, byte_size(hecate_content_dht:dht_key(mcid()))).

dht_key_is_deterministic_test() ->
    M = mcid(),
    ?assertEqual(hecate_content_dht:dht_key(M),
                 hecate_content_dht:dht_key(M)).

dht_key_differs_per_mcid_test() ->
    ?assertNotEqual(hecate_content_dht:dht_key(mcid()),
                    hecate_content_dht:dht_key(mcid())).

create_provider_info_required_fields_test() ->
    P = hecate_content_dht:create_provider_info(
          node_id(), <<"quic://host:4433">>, #{}),
    ?assert(maps:is_key(node_id, P)),
    ?assert(maps:is_key(endpoint, P)),
    ?assert(maps:is_key(metadata, P)),
    ?assert(maps:is_key(advertised_at, P)).

format_providers_handles_single_test() ->
    P = #{node_id => node_id(), endpoint => <<"e">>},
    [Norm] = hecate_content_dht:format_providers(P),
    ?assertEqual(<<"e">>, maps:get(endpoint, Norm)).

format_providers_handles_list_test() ->
    Ps = [#{node_id => node_id(), endpoint => <<"a">>},
          #{node_id => node_id(), endpoint => <<"b">>}],
    Norms = hecate_content_dht:format_providers(Ps),
    ?assertEqual(2, length(Norms)).

format_providers_handles_empty_test() ->
    ?assertEqual([], hecate_content_dht:format_providers([])).

create_announcement_returns_key_value_pair_test() ->
    {Key, Value} = hecate_content_dht:create_announcement(
                     mcid(), node_id(), <<"e">>, #{name => <<"x">>}),
    ?assertEqual(32, byte_size(Key)),
    ?assertEqual(300, maps:get(ttl, Value)).

create_removal_marks_removed_test() ->
    R = hecate_content_dht:create_removal(node_id()),
    ?assertEqual(true, maps:get(removed, R)).

default_ttl_is_300_test() ->
    ?assertEqual(300, hecate_content_dht:default_ttl()).

get_ttl_uses_opt_or_default_test() ->
    ?assertEqual(60,  hecate_content_dht:get_ttl(#{ttl => 60})),
    ?assertEqual(300, hecate_content_dht:get_ttl(#{})).

reannounce_interval_below_floor_test() ->
    ?assertEqual(30, hecate_content_dht:reannounce_interval(60)).

reannounce_interval_above_threshold_test() ->
    ?assertEqual(240, hecate_content_dht:reannounce_interval(300)).
