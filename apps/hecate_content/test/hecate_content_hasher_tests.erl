-module(hecate_content_hasher_tests).
-include_lib("eunit/include/eunit.hrl").

sha256_hash_matches_crypto_test() ->
    Data = <<"hello world">>,
    ?assertEqual(crypto:hash(sha256, Data),
                 hecate_content_hasher:hash(sha256, Data)).

blake3_hash_is_32_bytes_test() ->
    Hash = hecate_content_hasher:hash(blake3, <<"hello">>),
    ?assertEqual(32, byte_size(Hash)).

blake3_hash_streaming_matches_single_call_test() ->
    Chunks = [<<"hello ">>, <<"world">>],
    Whole  = iolist_to_binary(Chunks),
    ?assertEqual(hecate_content_hasher:hash(blake3, Whole),
                 hecate_content_hasher:hash_streaming(blake3, Chunks)).

sha256_streaming_matches_single_call_test() ->
    Chunks = [<<"foo ">>, <<"bar ">>, <<"baz">>],
    Whole  = iolist_to_binary(Chunks),
    ?assertEqual(hecate_content_hasher:hash(sha256, Whole),
                 hecate_content_hasher:hash_streaming(sha256, Chunks)).

verify_returns_true_for_correct_hash_test() ->
    Data = <<"verify-me">>,
    Hash = hecate_content_hasher:hash(blake3, Data),
    ?assert(hecate_content_hasher:verify(blake3, Data, Hash)).

verify_returns_false_for_wrong_hash_test() ->
    Wrong = <<0:256>>,
    ?assertNot(hecate_content_hasher:verify(blake3, <<"abc">>, Wrong)).

supported_algorithms_includes_both_test() ->
    Algos = hecate_content_hasher:supported_algorithms(),
    ?assert(lists:member(blake3, Algos)),
    ?assert(lists:member(sha256, Algos)).

is_supported_test() ->
    ?assert(hecate_content_hasher:is_supported(blake3)),
    ?assert(hecate_content_hasher:is_supported(sha256)),
    ?assertNot(hecate_content_hasher:is_supported(md5)).

hash_size_is_32_for_both_test() ->
    ?assertEqual(32, hecate_content_hasher:hash_size(blake3)),
    ?assertEqual(32, hecate_content_hasher:hash_size(sha256)).

default_algorithm_is_blake3_test() ->
    ?assertEqual(blake3, hecate_content_hasher:default_algorithm()).

hex_encode_known_input_test() ->
    ?assertEqual(<<"deadbeef">>,
                 hecate_content_hasher:hex_encode(<<16#de, 16#ad, 16#be, 16#ef>>)).

hex_decode_roundtrip_test() ->
    Bin = <<1, 2, 3, 4, 5>>,
    Hex = hecate_content_hasher:hex_encode(Bin),
    ?assertEqual({ok, Bin}, hecate_content_hasher:hex_decode(Hex)).

hex_decode_uppercase_works_test() ->
    ?assertEqual({ok, <<16#DE, 16#AD>>},
                 hecate_content_hasher:hex_decode(<<"DEAD">>)).

hex_decode_invalid_input_fails_test() ->
    ?assertEqual({error, invalid_hex},
                 hecate_content_hasher:hex_decode(<<"xyz!">>)),
    ?assertEqual({error, invalid_hex},
                 hecate_content_hasher:hex_decode(<<"abc">>)).
