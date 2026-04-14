%% EUnit smoke tests for the hecate_dht facade — every call MUST reach
%% the server. Behavioural details are in hecate_dht_server_tests.
-module(hecate_dht_tests).

-include_lib("eunit/include/eunit.hrl").

start() ->
    {ok, Dht} = hecate_dht:start_link(#{self_id => <<0:256>>}),
    Dht.

stop(Dht) -> hecate_dht:stop(Dht).

version_test() ->
    ?assertEqual(<<"0.1.0-phase3">>, hecate_dht:version()).

facade_start_stop_test() ->
    Dht = start(),
    ?assert(is_pid(Dht)),
    ?assertEqual(ok, stop(Dht)).

facade_delegates_observe_test() ->
    Dht = start(),
    ?assertEqual(admitted,
                 hecate_dht:observe(Dht, spec(<<1:8, 0:248>>))),
    stop(Dht).

facade_delegates_self_id_test() ->
    Dht = start(),
    ?assertEqual(<<0:256>>, hecate_dht:self_id(Dht)),
    stop(Dht).

facade_delegates_find_and_contains_test() ->
    Dht = start(),
    PeerId = <<1:8, 0:248>>,
    admitted = hecate_dht:observe(Dht, spec(PeerId)),
    ?assert(hecate_dht:contains(Dht, PeerId)),
    {ok, _} = hecate_dht:find(Dht, PeerId),
    ?assertEqual(error, hecate_dht:find(Dht, <<2:8, 0:248>>)),
    stop(Dht).

facade_delegates_k_closest_test() ->
    Dht = start(),
    admitted = hecate_dht:observe(Dht, spec(<<1:8, 0:248>>)),
    admitted = hecate_dht:observe(Dht, spec(<<9:8, 0:248>>)),
    Result = hecate_dht:k_closest(Dht, <<1:8, 0:248>>, 2),
    ?assertEqual(2, length(Result)),
    stop(Dht).

facade_delegates_siblings_and_ids_test() ->
    Dht = start(),
    admitted = hecate_dht:observe(Dht, spec(<<1:8, 0:248>>)),
    ?assertEqual(1, length(hecate_dht:siblings(Dht))),
    ?assertEqual([<<1:8, 0:248>>], hecate_dht:sibling_ids(Dht)),
    stop(Dht).

facade_delegates_size_and_bucket_count_test() ->
    Dht = start(),
    admitted = hecate_dht:observe(Dht, spec(<<1:8, 0:248>>)),
    ?assertEqual(1, hecate_dht:size(Dht)),
    ?assertEqual(1, hecate_dht:bucket_count(Dht)),
    stop(Dht).

facade_delegates_touch_and_forget_test() ->
    Dht = start(),
    PeerId = <<1:8, 0:248>>,
    admitted = hecate_dht:observe(Dht, spec(PeerId)),
    ok = hecate_dht:touch(Dht, PeerId),
    _ = hecate_dht:stats(Dht),
    ?assert(hecate_dht:contains(Dht, PeerId)),
    ok = hecate_dht:forget(Dht, PeerId),
    _ = hecate_dht:stats(Dht),
    ?assertNot(hecate_dht:contains(Dht, PeerId)),
    stop(Dht).

facade_delegates_stats_test() ->
    Dht = start(),
    ?assertMatch(#{size := 0, bucket_count := 0,
                   sibling_count := 0, k := 20, s := 16},
                 hecate_dht:stats(Dht)),
    stop(Dht).

%% Independent instances do not share state.
facade_supports_multiple_instances_test() ->
    {ok, A} = hecate_dht:start_link(#{self_id => <<0:256>>}),
    {ok, B} = hecate_dht:start_link(#{self_id => <<1:256>>}),
    ?assertNotEqual(hecate_dht:self_id(A), hecate_dht:self_id(B)),
    PeerId = <<7:8, 0:248>>,
    admitted = hecate_dht:observe(A, spec(PeerId)),
    ?assert(hecate_dht:contains(A, PeerId)),
    ?assertNot(hecate_dht:contains(B, PeerId)),
    stop(A),
    stop(B).

spec(NodeId) ->
    #{node_id => NodeId, asn => 100, country => <<"BE">>, tier => t0}.
