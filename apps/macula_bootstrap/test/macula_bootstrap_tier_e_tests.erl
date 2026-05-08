%% EUnit tests for macula_bootstrap_tier_e.
-module(macula_bootstrap_tier_e_tests).

-include_lib("eunit/include/eunit.hrl").

strategy_is_via_operator_paste_test() ->
    ?assertEqual(via_operator_paste, macula_bootstrap_tier_e:strategy()).

stagger_is_zero_test() ->
    ?assertEqual(0, macula_bootstrap_tier_e:stagger_ms()).

discover_without_urls_errors_test() ->
    ?assertEqual({error, no_urls},
                 macula_bootstrap_tier_e:discover(#{})).

discover_with_all_invalid_urls_errors_test() ->
    ?assertEqual({error, all_urls_invalid},
                 macula_bootstrap_tier_e:discover(
                   #{peer_urls => [<<"http://bad">>,
                                   <<"macula-peer:!!!">>]})).

discover_yields_single_peer_test() ->
    {Url, Kp} = build_url([]),
    {ok, [Peer]} = macula_bootstrap_tier_e:discover(
                     #{peer_urls => [Url]}),
    ?assertEqual(macula_identity:public(Kp), maps:get(node_id, Peer)),
    ?assertEqual(via_operator_paste, maps:get(strategy, Peer)),
    ?assertEqual(macula_bootstrap_tier_e, maps:get(via, Peer)),
    ?assertEqual([], maps:get(addresses, Peer)).

discover_yields_multiple_peers_test() ->
    {U1, _} = build_url([]),
    {U2, _} = build_url([]),
    {U3, _} = build_url([]),
    {ok, Peers} = macula_bootstrap_tier_e:discover(
                    #{peer_urls => [U1, U2, U3]}),
    ?assertEqual(3, length(Peers)),
    Ids = [maps:get(node_id, P) || P <- Peers],
    ?assertEqual(3, length(lists:usort(Ids))).

discover_tolerates_mixed_good_and_bad_urls_test() ->
    {U1, _} = build_url([]),
    {ok, Peers} = macula_bootstrap_tier_e:discover(
                    #{peer_urls => [<<"http://bad">>, U1, <<"macula-peer:XXX">>]}),
    ?assertEqual(1, length(Peers)).

discover_preserves_address_hints_test() ->
    Addrs = [#{{text, <<"v6">>}   => {text, <<"2a02::1">>},
               {text, <<"port">>} => 7000}],
    {Url, _} = build_url(Addrs),
    {ok, [Peer]} = macula_bootstrap_tier_e:discover(
                     #{peer_urls => [Url]}),
    ?assertEqual(1, length(maps:get(addresses, Peer))).

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

build_url(Addrs) ->
    Kp = macula_identity:generate(),
    Record = macula_record:sign(
        macula_record:node_record(macula_identity:public(Kp), [], 0),
        Kp),
    {macula_bootstrap_peer_url:encode(Record, Addrs), Kp}.
