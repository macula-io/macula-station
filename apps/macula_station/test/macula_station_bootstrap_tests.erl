-module(macula_station_bootstrap_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% to_entry_spec — pure
%%==================================================================

spec_defaults_for_unknown_metadata_test() ->
    Peer = peer(<<1:256>>, via_doh, macula_bootstrap_via_doh),
    Spec = macula_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(<<1:256>>, maps:get(node_id, Spec)),
    ?assertEqual(0, maps:get(asn, Spec)),
    ?assertEqual(<<"??">>, maps:get(country, Spec)),
    ?assertEqual(t0, maps:get(tier, Spec)),
    ?assertEqual([], maps:get(endpoints, Spec)).

spec_preserves_addresses_test() ->
    Addrs = [#{ {text, <<"ip">>}   => {text, <<"::1">>},
                {text, <<"port">>} => 7000 }],
    Peer = (peer(<<2:256>>, via_doh, macula_bootstrap_via_doh))#{addresses => Addrs},
    Spec = macula_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(Addrs, maps:get(endpoints, Spec)).

spec_gateway_tier_3_maps_to_t3_test() ->
    Peer = (peer(<<3:256>>, via_doh, macula_bootstrap_via_doh))#{gateway_tier => 3},
    Spec = macula_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(t3, maps:get(tier, Spec)).

spec_gateway_tier_4_maps_to_t3_test() ->
    Peer = (peer(<<4:256>>, via_mainline_dht, macula_bootstrap_via_mainline_dht))#{gateway_tier => 4},
    Spec = macula_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(t3, maps:get(tier, Spec)).

spec_unknown_gateway_tier_defaults_t0_test() ->
    Peer = (peer(<<5:256>>, via_mdns, macula_bootstrap_via_mdns))#{
        gateway_tier => undefined
    },
    Spec = macula_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(t0, maps:get(tier, Spec)).

spec_tier_b_peer_defaults_t0_test() ->
    Peer = peer(<<6:256>>, via_mdns, macula_bootstrap_via_mdns),
    Spec = macula_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(t0, maps:get(tier, Spec)).

spec_tier_e_peer_defaults_t0_test() ->
    Peer = peer(<<7:256>>, via_operator_paste, macula_bootstrap_via_operator_paste),
    Spec = macula_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(t0, maps:get(tier, Spec)).

%%==================================================================
%% ingest/2 — side-effectful
%%==================================================================

ingest_empty_list_test_() ->
    {setup,
     fun setup_dht/0,
     fun cleanup_dht/1,
     fun({Dht}) ->
         fun() ->
             Summary = macula_station_bootstrap:ingest(Dht, []),
             ?assertEqual(#{observed => 0, admitted => 0, touched => 0,
                            replaced => 0, rejected => 0}, Summary)
         end
     end}.

ingest_observes_new_peers_test_() ->
    {setup,
     fun setup_dht/0,
     fun cleanup_dht/1,
     fun({Dht}) ->
         fun() ->
             Peers = [peer(rand_id(), via_doh, macula_bootstrap_via_doh)
                      || _ <- lists:seq(1, 5)],
             Summary = macula_station_bootstrap:ingest(Dht, Peers),
             ?assertEqual(5, maps:get(observed, Summary)),
             ?assertEqual(5, maps:get(admitted, Summary)),
             ?assertEqual(0, maps:get(rejected, Summary)),
             ?assertEqual(5, macula_dht:size(Dht))
         end
     end}.

ingest_touches_on_repeat_test_() ->
    {setup,
     fun setup_dht/0,
     fun cleanup_dht/1,
     fun({Dht}) ->
         fun() ->
             Peer = peer(rand_id(), via_doh, macula_bootstrap_via_doh),
             _ = macula_station_bootstrap:ingest(Dht, [Peer]),
             Summary = macula_station_bootstrap:ingest(Dht, [Peer]),
             ?assertEqual(1, maps:get(observed, Summary)),
             ?assertEqual(1, maps:get(touched, Summary)),
             ?assertEqual(0, maps:get(admitted, Summary))
         end
     end}.

ingest_from_foundation_seeds_tags_t3_test_() ->
    {setup,
     fun setup_dht/0,
     fun cleanup_dht/1,
     fun({Dht}) ->
         fun() ->
             Id    = rand_id(),
             Peer  = (peer(Id, via_doh, macula_bootstrap_via_doh))#{gateway_tier => 4},
             _     = macula_station_bootstrap:ingest(Dht, [Peer]),
             {ok, Entry} = macula_dht:find(Dht, Id),
             ?assertEqual(t3, macula_dht_entry:tier(Entry))
         end
     end}.

ingest_preserves_count_across_cascade_tiers_test_() ->
    {setup,
     fun setup_dht/0,
     fun cleanup_dht/1,
     fun({Dht}) ->
         fun() ->
             Peers = [peer(rand_id(), via_doh, macula_bootstrap_via_doh),
                      peer(rand_id(), via_mdns, macula_bootstrap_via_mdns),
                      peer(rand_id(), via_mainline_dht, macula_bootstrap_via_mainline_dht),
                      peer(rand_id(), via_blockchain, macula_bootstrap_tier_d),
                      peer(rand_id(), via_operator_paste, macula_bootstrap_via_operator_paste)],
             Summary = macula_station_bootstrap:ingest(Dht, Peers),
             ?assertEqual(5, maps:get(observed, Summary)),
             ?assertEqual(5, maps:get(admitted, Summary))
         end
     end}.

%%==================================================================
%% Helpers
%%==================================================================

peer(NodeId, Strategy, Via) ->
    #{
        node_id   => NodeId,
        record    => fake_record(),
        addresses => [],
        strategy  => Strategy,
        via       => Via
    }.

fake_record() ->
    %% ingest never inspects the record; an opaque placeholder is
    %% sufficient for routing-table admission.
    #{type => 16#0D, key => <<0:256>>, version => <<0:128>>,
      created_at => 1, expires_at => 2, payload => #{}}.

rand_id() ->
    crypto:strong_rand_bytes(32).

setup_dht() ->
    application:ensure_all_started(crypto),
    {ok, Dht} = macula_dht:start_link(#{self_id => rand_id()}),
    {Dht}.

cleanup_dht({Dht}) ->
    macula_dht:stop(Dht),
    ok.
