-module(macula_bootstrap_foundation_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% decode_record_bytes
%%==================================================================

decode_round_trips_valid_record_test() ->
    Record = sample_record(),
    Bytes  = macula_record:encode(Record),
    {ok, Decoded} = macula_bootstrap_foundation:decode_record_bytes(Bytes),
    ?assertEqual(macula_record:key(Record), macula_record:key(Decoded)).

decode_rejects_arbitrary_garbage_test_() ->
    [?_assertEqual({error, bad_record_bytes},
                   macula_bootstrap_foundation:decode_record_bytes(Bin))
     || Bin <- [<<>>,
                <<"not a macula record">>,
                <<0, 0, 0, 0>>,
                crypto:strong_rand_bytes(32)]].

decode_rejects_malformed_cbor_test() ->
    %% 0x9f starts an indefinite-length CBOR array; alone it never
    %% terminates and triggers a match error inside
    %% macula_record_cbor:decode/1. We must catch and normalise.
    ?assertEqual({error, bad_record_bytes},
                 macula_bootstrap_foundation:decode_record_bytes(<<16#9F>>)).

decode_truthfully_returns_record_errors_test() ->
    %% A decodable CBOR map that is not a valid macula envelope
    %% should come back as an `{error, _}' from macula_record rather
    %% than being eaten by our catch-all.
    NotAnEnvelope = macula_record_cbor:encode(
                      #{ {text, <<"hello">>} => {text, <<"world">>} }),
    ?assertMatch({error, _},
                 macula_bootstrap_foundation:decode_record_bytes(
                   NotAnEnvelope)).

%%==================================================================
%% peers_from_record
%%==================================================================

peers_from_seed_list_stamps_strategy_and_via_test() ->
    Record = sample_record(),
    Peers  = macula_bootstrap_foundation:peers_from_record(
               Record, via_mainline_dht, macula_bootstrap_via_mainline_dht),
    ?assert(length(Peers) > 0),
    [?assertEqual(via_mainline_dht, maps:get(strategy, P)) || P <- Peers],
    [?assertEqual(macula_bootstrap_via_mainline_dht, maps:get(via, P)) || P <- Peers],
    [?assertEqual(Record, maps:get(record, P)) || P <- Peers].

peers_from_record_preserves_seed_node_ids_test() ->
    Seeds = sample_seeds(4),
    Kp    = macula_identity:generate(),
    Fk    = macula_identity:public(Kp),
    Record = macula_record:sign(
               macula_record:foundation_seed_list(Fk, Seeds), Kp),
    Peers = macula_bootstrap_foundation:peers_from_record(
              Record, via_doh, macula_bootstrap_via_doh),
    Expected = lists:sort([maps:get(node_id, S) || S <- Seeds]),
    Got      = lists:sort([maps:get(node_id, P) || P <- Peers]),
    ?assertEqual(Expected, Got).

peers_from_record_empty_seed_list_test() ->
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    Record = macula_record:sign(
               macula_record:foundation_seed_list(Fk, []), Kp),
    ?assertEqual([], macula_bootstrap_foundation:peers_from_record(
                       Record, d, macula_bootstrap_tier_d)).

peers_from_non_seed_list_record_returns_empty_test() ->
    %% A record whose payload doesn't carry a `seeds' key — defensively
    %% yields the empty list rather than crashing. Foundation may
    %% someday publish foundation_parameter / trust_list records that
    %% happen to land here if a caller mis-routes; we degrade
    %% gracefully instead of failing loud.
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    Record = macula_record:sign(
               macula_record:foundation_parameter(
                 Fk, <<"puzzle_difficulty">>, 8), Kp),
    ?assertEqual([], macula_bootstrap_foundation:peers_from_record(
                       Record, a, macula_bootstrap_via_doh)).

peers_from_record_preserves_addresses_test() ->
    Kp    = macula_identity:generate(),
    Fk    = macula_identity:public(Kp),
    Seeds = [#{node_id   => crypto:strong_rand_bytes(32),
               addresses => [#{ {text, <<"ip">>}   => {text, <<"::1">>},
                                {text, <<"port">>} => 7000 }],
               tier      => 4}],
    Record = macula_record:sign(
               macula_record:foundation_seed_list(Fk, Seeds), Kp),
    [Peer] = macula_bootstrap_foundation:peers_from_record(
               Record, b, macula_bootstrap_via_mdns),
    ?assertMatch([#{ {text, <<"ip">>}   := _,
                     {text, <<"port">>} := 7000 }],
                 maps:get(addresses, Peer)).

%%==================================================================
%% Helpers
%%==================================================================

sample_record() ->
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    macula_record:sign(
      macula_record:foundation_seed_list(Fk, sample_seeds(3)), Kp).

sample_seeds(N) ->
    [#{node_id   => crypto:strong_rand_bytes(32),
       addresses => [], tier => 4} || _ <- lists:seq(1, N)].
