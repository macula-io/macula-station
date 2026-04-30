%% EUnit smoke tests for the macula_dht facade — every call MUST reach
%% the server. Behavioural details are in macula_dht_server_tests.
-module(macula_dht_tests).

-include_lib("eunit/include/eunit.hrl").

start() ->
    {ok, Dht} = macula_dht:start_link(#{self_id => <<0:256>>}),
    Dht.

stop(Dht) -> macula_dht:stop(Dht).

version_test() ->
    ?assertEqual(<<"0.1.0-phase3">>, macula_dht:version()).

facade_start_stop_test() ->
    Dht = start(),
    ?assert(is_pid(Dht)),
    ?assertEqual(ok, stop(Dht)).

facade_delegates_observe_test() ->
    Dht = start(),
    ?assertEqual(admitted,
                 macula_dht:observe(Dht, spec(<<1:8, 0:248>>))),
    stop(Dht).

facade_delegates_self_id_test() ->
    Dht = start(),
    ?assertEqual(<<0:256>>, macula_dht:self_id(Dht)),
    stop(Dht).

facade_delegates_find_and_contains_test() ->
    Dht = start(),
    PeerId = <<1:8, 0:248>>,
    admitted = macula_dht:observe(Dht, spec(PeerId)),
    ?assert(macula_dht:contains(Dht, PeerId)),
    {ok, _} = macula_dht:find(Dht, PeerId),
    ?assertEqual(error, macula_dht:find(Dht, <<2:8, 0:248>>)),
    stop(Dht).

facade_delegates_k_closest_test() ->
    Dht = start(),
    admitted = macula_dht:observe(Dht, spec(<<1:8, 0:248>>)),
    admitted = macula_dht:observe(Dht, spec(<<9:8, 0:248>>)),
    Result = macula_dht:k_closest(Dht, <<1:8, 0:248>>, 2),
    ?assertEqual(2, length(Result)),
    stop(Dht).

facade_delegates_siblings_and_ids_test() ->
    Dht = start(),
    admitted = macula_dht:observe(Dht, spec(<<1:8, 0:248>>)),
    ?assertEqual(1, length(macula_dht:siblings(Dht))),
    ?assertEqual([<<1:8, 0:248>>], macula_dht:sibling_ids(Dht)),
    stop(Dht).

facade_delegates_size_and_bucket_count_test() ->
    Dht = start(),
    admitted = macula_dht:observe(Dht, spec(<<1:8, 0:248>>)),
    ?assertEqual(1, macula_dht:size(Dht)),
    ?assertEqual(1, macula_dht:bucket_count(Dht)),
    stop(Dht).

facade_delegates_touch_and_forget_test() ->
    Dht = start(),
    PeerId = <<1:8, 0:248>>,
    admitted = macula_dht:observe(Dht, spec(PeerId)),
    ok = macula_dht:touch(Dht, PeerId),
    _ = macula_dht:stats(Dht),
    ?assert(macula_dht:contains(Dht, PeerId)),
    ok = macula_dht:forget(Dht, PeerId),
    _ = macula_dht:stats(Dht),
    ?assertNot(macula_dht:contains(Dht, PeerId)),
    stop(Dht).

facade_delegates_stats_test() ->
    Dht = start(),
    ?assertMatch(#{size := 0, bucket_count := 0,
                   sibling_count := 0, k := 20, s := 16},
                 macula_dht:stats(Dht)),
    stop(Dht).

%% Independent instances do not share state.
facade_supports_multiple_instances_test() ->
    {ok, A} = macula_dht:start_link(#{self_id => <<0:256>>}),
    {ok, B} = macula_dht:start_link(#{self_id => <<1:256>>}),
    ?assertNotEqual(macula_dht:self_id(A), macula_dht:self_id(B)),
    PeerId = <<7:8, 0:248>>,
    admitted = macula_dht:observe(A, spec(PeerId)),
    ?assert(macula_dht:contains(A, PeerId)),
    ?assertNot(macula_dht:contains(B, PeerId)),
    stop(A),
    stop(B).

spec(NodeId) ->
    #{node_id => NodeId, asn => 100, country => <<"BE">>, tier => t0}.
