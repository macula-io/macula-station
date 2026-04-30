%% EUnit tests for macula_dht_protocol — pure frame builders and
%% entry↔station_ref conversion. No gen_server, no wire I/O.
-module(macula_dht_protocol_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Nonce
%%---------------------------------------------------------------------

new_nonce_is_16_bytes_test() ->
    N = macula_dht_protocol:new_nonce(),
    ?assertEqual(16, byte_size(N)).

new_nonce_is_not_deterministic_test() ->
    ?assertNotEqual(macula_dht_protocol:new_nonce(),
                    macula_dht_protocol:new_nonce()).

%%---------------------------------------------------------------------
%% PING / PONG
%%---------------------------------------------------------------------

build_ping_returns_signed_frame_and_nonce_test() ->
    Kp = macula_identity:generate(),
    {Frame, Nonce} = macula_dht_protocol:build_ping(Kp),
    ?assertEqual(ping, macula_frame:frame_type(Frame)),
    ?assertEqual(Nonce, maps:get(nonce, Frame)),
    ?assertEqual(64, byte_size(macula_frame:signature(Frame))),
    ?assertMatch({ok, _}, macula_frame:verify(Frame, macula_identity:public(Kp))).

build_pong_echoes_nonce_and_is_signed_test() ->
    Kp = macula_identity:generate(),
    {_, Nonce} = macula_dht_protocol:build_ping(Kp),
    Pong = macula_dht_protocol:build_pong(Nonce, Kp),
    ?assertEqual(pong, macula_frame:frame_type(Pong)),
    ?assertEqual(Nonce, maps:get(nonce, Pong)),
    ?assertMatch({ok, _},
                 macula_frame:verify(Pong, macula_identity:public(Kp))).

build_pong_rejects_bad_nonce_size_test() ->
    Kp = macula_identity:generate(),
    ?assertError(function_clause,
                 macula_dht_protocol:build_pong(<<0:64>>, Kp)).

%%---------------------------------------------------------------------
%% FIND_NODE / NODES
%%---------------------------------------------------------------------

build_find_node_shape_test() ->
    Kp = macula_identity:generate(),
    Self = macula_identity:public(Kp),
    Key  = crypto:strong_rand_bytes(32),
    F = macula_dht_protocol:build_find_node(Key, Self, 3, Kp),
    ?assertEqual(find_node, macula_frame:frame_type(F)),
    ?assertEqual(Key,  maps:get(key,    F)),
    ?assertEqual(Self, maps:get(origin, F)),
    ?assertEqual(3,    maps:get(depth,  F)),
    ?assertMatch({ok, _}, macula_frame:verify(F, Self)).

build_nodes_reply_converts_entries_test() ->
    Kp = macula_identity:generate(),
    Entries = [sample_entry(N) || N <- [1, 2, 3]],
    Key  = crypto:strong_rand_bytes(32),
    F = macula_dht_protocol:build_nodes_reply(Key, Entries, Kp),
    ?assertEqual(nodes, macula_frame:frame_type(F)),
    ?assertEqual(3, length(maps:get(nodes, F))),
    ?assertMatch({ok, _},
                 macula_frame:verify(F, macula_identity:public(Kp))).

build_nodes_reply_empty_list_test() ->
    Kp = macula_identity:generate(),
    F = macula_dht_protocol:build_nodes_reply(crypto:strong_rand_bytes(32),
                                              [], Kp),
    ?assertEqual([], maps:get(nodes, F)).

%%---------------------------------------------------------------------
%% verify/2
%%---------------------------------------------------------------------

verify_accepts_signed_frame_test() ->
    Kp = macula_identity:generate(),
    {Frame, _} = macula_dht_protocol:build_ping(Kp),
    ?assertMatch({ok, _},
                 macula_dht_protocol:verify(Frame, macula_identity:public(Kp))).

verify_rejects_tampered_frame_test() ->
    Kp = macula_identity:generate(),
    {Frame, _} = macula_dht_protocol:build_ping(Kp),
    Tampered = Frame#{nonce => crypto:strong_rand_bytes(16)},
    ?assertEqual({error, signature_invalid},
                 macula_dht_protocol:verify(Tampered,
                                            macula_identity:public(Kp))).

verify_rejects_wrong_key_test() ->
    Kp1 = macula_identity:generate(),
    Kp2 = macula_identity:generate(),
    {Frame, _} = macula_dht_protocol:build_ping(Kp1),
    ?assertEqual({error, signature_invalid},
                 macula_dht_protocol:verify(Frame,
                                            macula_identity:public(Kp2))).

%%---------------------------------------------------------------------
%% Entry → station_ref
%%---------------------------------------------------------------------

entry_to_station_ref_preserves_id_asn_country_test() ->
    Entry = sample_entry(7),
    Ref = macula_dht_protocol:entry_to_station_ref(Entry),
    ?assertEqual(macula_dht_entry:node_id(Entry),
                 maps:get(node_id, Ref)),
    ?assertEqual(macula_dht_entry:asn(Entry),
                 maps:get(asn, Ref)),
    ?assertEqual(macula_dht_entry:country(Entry),
                 maps:get(country, Ref)).

entry_to_station_ref_maps_tier_atom_to_int_test() ->
    E0 = sample_entry_with_tier(0, t0),
    E3 = sample_entry_with_tier(1, t3),
    ?assertEqual(0, maps:get(tier,
                    macula_dht_protocol:entry_to_station_ref(E0))),
    ?assertEqual(3, maps:get(tier,
                    macula_dht_protocol:entry_to_station_ref(E3))).

entry_to_station_ref_produces_positive_last_seen_at_test() ->
    Ref = macula_dht_protocol:entry_to_station_ref(sample_entry(1)),
    ?assert(maps:get(last_seen_at, Ref) > 0).

tier_to_int_round_trip_test() ->
    [?assertEqual(T, macula_dht_protocol:int_to_tier(
                       macula_dht_protocol:tier_to_int(T)))
     || T <- [t0, t1, t2, t3]].

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

sample_entry(N) ->
    sample_entry_with_tier(N, t0).

sample_entry_with_tier(N, Tier) ->
    macula_dht_entry:new(#{
        node_id => <<N:256>>,
        asn     => 64512 + N,
        country => <<"BE">>,
        tier    => Tier
    }).
