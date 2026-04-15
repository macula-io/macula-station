-module(hecate_station_bootstrap_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% to_entry_spec — pure
%%==================================================================

spec_defaults_for_unknown_metadata_test() ->
    Peer = peer(<<1:256>>, a, hecate_bootstrap_tier_a),
    Spec = hecate_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(<<1:256>>, maps:get(node_id, Spec)),
    ?assertEqual(0, maps:get(asn, Spec)),
    ?assertEqual(<<"??">>, maps:get(country, Spec)),
    ?assertEqual(t0, maps:get(tier, Spec)),
    ?assertEqual([], maps:get(endpoints, Spec)).

spec_preserves_addresses_test() ->
    Addrs = [#{ {text, <<"ip">>}   => {text, <<"::1">>},
                {text, <<"port">>} => 7000 }],
    Peer = (peer(<<2:256>>, a, hecate_bootstrap_tier_a))#{addresses => Addrs},
    Spec = hecate_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(Addrs, maps:get(endpoints, Spec)).

spec_gateway_tier_3_maps_to_t3_test() ->
    Peer = (peer(<<3:256>>, a, hecate_bootstrap_tier_a))#{gateway_tier => 3},
    Spec = hecate_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(t3, maps:get(tier, Spec)).

spec_gateway_tier_4_maps_to_t3_test() ->
    Peer = (peer(<<4:256>>, c, hecate_bootstrap_tier_c))#{gateway_tier => 4},
    Spec = hecate_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(t3, maps:get(tier, Spec)).

spec_unknown_gateway_tier_defaults_t0_test() ->
    Peer = (peer(<<5:256>>, b, hecate_bootstrap_tier_b))#{
        gateway_tier => undefined
    },
    Spec = hecate_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(t0, maps:get(tier, Spec)).

spec_tier_b_peer_defaults_t0_test() ->
    Peer = peer(<<6:256>>, b, hecate_bootstrap_tier_b),
    Spec = hecate_station_bootstrap:to_entry_spec(Peer),
    ?assertEqual(t0, maps:get(tier, Spec)).

spec_tier_e_peer_defaults_t0_test() ->
    Peer = peer(<<7:256>>, e, hecate_bootstrap_tier_e),
    Spec = hecate_station_bootstrap:to_entry_spec(Peer),
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
             Summary = hecate_station_bootstrap:ingest(Dht, []),
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
             Peers = [peer(rand_id(), a, hecate_bootstrap_tier_a)
                      || _ <- lists:seq(1, 5)],
             Summary = hecate_station_bootstrap:ingest(Dht, Peers),
             ?assertEqual(5, maps:get(observed, Summary)),
             ?assertEqual(5, maps:get(admitted, Summary)),
             ?assertEqual(0, maps:get(rejected, Summary)),
             ?assertEqual(5, hecate_dht:size(Dht))
         end
     end}.

ingest_touches_on_repeat_test_() ->
    {setup,
     fun setup_dht/0,
     fun cleanup_dht/1,
     fun({Dht}) ->
         fun() ->
             Peer = peer(rand_id(), a, hecate_bootstrap_tier_a),
             _ = hecate_station_bootstrap:ingest(Dht, [Peer]),
             Summary = hecate_station_bootstrap:ingest(Dht, [Peer]),
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
             Peer  = (peer(Id, a, hecate_bootstrap_tier_a))#{gateway_tier => 4},
             _     = hecate_station_bootstrap:ingest(Dht, [Peer]),
             {ok, Entry} = hecate_dht:find(Dht, Id),
             ?assertEqual(t3, hecate_dht_entry:tier(Entry))
         end
     end}.

ingest_preserves_count_across_cascade_tiers_test_() ->
    {setup,
     fun setup_dht/0,
     fun cleanup_dht/1,
     fun({Dht}) ->
         fun() ->
             Peers = [peer(rand_id(), a, hecate_bootstrap_tier_a),
                      peer(rand_id(), b, hecate_bootstrap_tier_b),
                      peer(rand_id(), c, hecate_bootstrap_tier_c),
                      peer(rand_id(), d, hecate_bootstrap_tier_d),
                      peer(rand_id(), e, hecate_bootstrap_tier_e)],
             Summary = hecate_station_bootstrap:ingest(Dht, Peers),
             ?assertEqual(5, maps:get(observed, Summary)),
             ?assertEqual(5, maps:get(admitted, Summary))
         end
     end}.

%%==================================================================
%% Helpers
%%==================================================================

peer(NodeId, CascadeTier, Via) ->
    #{
        node_id   => NodeId,
        record    => fake_record(),
        addresses => [],
        tier      => CascadeTier,
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
    {ok, Dht} = hecate_dht:start_link(#{self_id => rand_id()}),
    {Dht}.

cleanup_dht({Dht}) ->
    hecate_dht:stop(Dht),
    ok.
