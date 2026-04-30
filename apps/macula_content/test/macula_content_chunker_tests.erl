-module(macula_content_chunker_tests).
-include_lib("eunit/include/eunit.hrl").

default_chunk_size_test() ->
    ?assertEqual(262144, macula_content_chunker:default_chunk_size()).

chunk_empty_data_test() ->
    ?assertEqual({ok, []}, macula_content_chunker:chunk(<<>>, 1024)).

chunk_smaller_than_size_returns_one_chunk_test() ->
    {ok, Chunks} = macula_content_chunker:chunk(<<"hi">>, 1024),
    ?assertEqual([<<"hi">>], Chunks).

chunk_exact_multiple_test() ->
    Data = <<1:8, 2:8, 3:8, 4:8>>,
    {ok, Chunks} = macula_content_chunker:chunk(Data, 2),
    ?assertEqual([<<1, 2>>, <<3, 4>>], Chunks).

chunk_with_remainder_test() ->
    Data = <<"abcdefg">>,
    {ok, Chunks} = macula_content_chunker:chunk(Data, 3),
    ?assertEqual([<<"abc">>, <<"def">>, <<"g">>], Chunks).

reassemble_roundtrip_test() ->
    Data = crypto:strong_rand_bytes(100),
    {ok, Chunks} = macula_content_chunker:chunk(Data, 13),
    ?assertEqual(Data, macula_content_chunker:reassemble(Chunks)).

chunk_info_carries_index_offset_size_hash_test() ->
    Data = <<"abcdef">>,
    {ok, Chunks} = macula_content_chunker:chunk(Data, 2),
    [I0, I1, I2] = macula_content_chunker:chunk_info(Chunks, blake3),
    ?assertEqual(0, maps:get(index, I0)),
    ?assertEqual(0, maps:get(offset, I0)),
    ?assertEqual(2, maps:get(size, I0)),
    ?assertEqual(2, maps:get(index, I2)),
    ?assertEqual(4, maps:get(offset, I2)),
    ?assertEqual(2, maps:get(size, I2)),
    ?assertNotEqual(maps:get(hash, I0), maps:get(hash, I1)).

merkle_root_single_chunk_equals_chunk_hash_test() ->
    Data = <<"hello world">>,
    {ok, Chunks} = macula_content_chunker:chunk(Data, 4096),
    [Info] = macula_content_chunker:chunk_info(Chunks, blake3),
    ?assertEqual(maps:get(hash, Info),
                 macula_content_chunker:merkle_root([Info], blake3)).

merkle_root_empty_is_zero_data_hash_test() ->
    Empty = macula_content_hasher:hash(blake3, <<>>),
    ?assertEqual(Empty, macula_content_chunker:merkle_root([], blake3)).

merkle_root_changes_with_data_test() ->
    {ok, ChunksA} = macula_content_chunker:chunk(<<"AAAA">>, 1),
    {ok, ChunksB} = macula_content_chunker:chunk(<<"AAAB">>, 1),
    InfosA = macula_content_chunker:chunk_info(ChunksA, blake3),
    InfosB = macula_content_chunker:chunk_info(ChunksB, blake3),
    ?assertNotEqual(macula_content_chunker:merkle_root(InfosA, blake3),
                    macula_content_chunker:merkle_root(InfosB, blake3)).

verify_chunk_with_correct_hash_test() ->
    Chunk = <<"abc">>,
    Hash  = macula_content_hasher:hash(blake3, Chunk),
    ?assert(macula_content_chunker:verify_chunk(Chunk, Hash, blake3)).
