-module(macula_content_manifest_tests).
-include_lib("eunit/include/eunit.hrl").

create_returns_manifest_with_mcid_test() ->
    {ok, M} = macula_content_manifest:create(<<"hello">>),
    ?assertMatch(<<_:272>>, maps:get(mcid, M)),
    ?assertEqual(<<"unnamed">>, maps:get(name, M)),
    ?assertEqual(5, maps:get(size, M)).

create_with_options_test() ->
    {ok, M} = macula_content_manifest:create(
                <<"data">>,
                #{name => <<"file.txt">>,
                  hash_algorithm => sha256,
                  chunk_size => 2}),
    ?assertEqual(<<"file.txt">>, maps:get(name, M)),
    ?assertEqual(sha256, maps:get(hash_algorithm, M)),
    ?assertEqual(2, maps:get(chunk_size, M)),
    ?assertEqual(2, maps:get(chunk_count, M)).

mcid_is_deterministic_for_same_data_test() ->
    {ok, A} = macula_content_manifest:create(
                <<"x">>, #{name => <<"n">>, chunk_size => 1024}),
    {ok, B} = macula_content_manifest:create(
                <<"x">>, #{name => <<"n">>, chunk_size => 1024}),
    ?assertEqual(maps:get(mcid, A), maps:get(mcid, B)).

mcid_changes_with_data_test() ->
    {ok, A} = macula_content_manifest:create(<<"a">>),
    {ok, B} = macula_content_manifest:create(<<"b">>),
    ?assertNotEqual(maps:get(mcid, A), maps:get(mcid, B)).

encode_decode_roundtrip_test() ->
    {ok, M} = macula_content_manifest:create(<<"hello world">>),
    {ok, Bin} = macula_content_manifest:encode(M),
    {ok, M2}  = macula_content_manifest:decode(Bin),
    ?assertEqual(maps:get(mcid, M),       maps:get(mcid, M2)),
    ?assertEqual(maps:get(size, M),       maps:get(size, M2)),
    ?assertEqual(maps:get(chunk_count, M),maps:get(chunk_count, M2)),
    ?assertEqual(maps:get(root_hash, M),  maps:get(root_hash, M2)).

decode_garbage_returns_error_test() ->
    ?assertEqual({error, invalid_manifest},
                 macula_content_manifest:decode(<<"not valid cbor">>)).

verify_returns_ok_for_matching_data_test() ->
    Data = <<"some content">>,
    {ok, M} = macula_content_manifest:create(Data),
    ?assertEqual(ok, macula_content_manifest:verify(M, Data)).

verify_returns_size_mismatch_test() ->
    {ok, M} = macula_content_manifest:create(<<"abc">>),
    ?assertEqual({error, size_mismatch},
                 macula_content_manifest:verify(M, <<"abcd">>)).

verify_returns_root_hash_mismatch_test() ->
    {ok, M} = macula_content_manifest:create(<<"abcde">>),
    %% Same size, different content.
    ?assertEqual({error, root_hash_mismatch},
                 macula_content_manifest:verify(M, <<"xyzab">>)).

mcid_to_string_format_test() ->
    {ok, M} = macula_content_manifest:create(<<"hi">>),
    Str = macula_content_manifest:mcid_to_string(maps:get(mcid, M)),
    ?assertMatch(<<"mcid1-manifest-blake3-", _/binary>>, Str).

mcid_string_roundtrip_test() ->
    {ok, M} = macula_content_manifest:create(<<"r">>),
    MCID = maps:get(mcid, M),
    Str  = macula_content_manifest:mcid_to_string(MCID),
    ?assertEqual({ok, MCID}, macula_content_manifest:mcid_from_string(Str)).

mcid_from_string_invalid_test() ->
    ?assertEqual({error, invalid_mcid},
                 macula_content_manifest:mcid_from_string(<<"not-an-mcid">>)).

get_chunk_mcid_returns_raw_codec_test() ->
    Data = iolist_to_binary([<<C:8>> || C <- lists:seq(1, 50)]),
    {ok, M} = macula_content_manifest:create(Data, #{chunk_size => 10}),
    {ok, ChunkMCID} = macula_content_manifest:get_chunk_mcid(M, 0),
    <<_:8, Codec:8, _:32/binary>> = ChunkMCID,
    ?assertEqual(16#55, Codec).

get_chunk_mcid_invalid_index_test() ->
    {ok, M} = macula_content_manifest:create(<<"x">>),
    ?assertEqual({error, invalid_index},
                 macula_content_manifest:get_chunk_mcid(M, 99)).
