-module(hecate_content_tests).
-include_lib("eunit/include/eunit.hrl").

%%--- helpers ---

setup() ->
    Dir = filename:join("/tmp",
            "hecate-content-facade-"
            ++ integer_to_list(erlang:unique_integer([positive]))),
    {ok, Pid} = hecate_content_store:start_link(#{store_path => Dir}),
    unlink(Pid),
    {Pid, Dir}.

cleanup({Pid, Dir}) ->
    case is_process_alive(Pid) of
        true  -> catch hecate_content_store:stop();
        false -> ok
    end,
    catch del_recursive(Dir).

del_recursive(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            {ok, Names} = file:list_dir(Dir),
            lists:foreach(
                fun(N) -> del_recursive(filename:join(Dir, N)) end, Names),
            file:del_dir(Dir);
        false ->
            file:delete(Dir)
    end.

%%--- generator ---

facade_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun(_) -> ?_test(mcid_default_algorithm()) end,
         fun(_) -> ?_test(mcid_explicit_sha256()) end,
         fun(_) -> ?_test(mcid_string_roundtrip()) end,
         fun(_) -> ?_test(create_and_verify_manifest()) end,
         fun(_) -> ?_test(missing_chunks_for_unstored_data()) end,
         fun(_) -> ?_test(store_then_fetch_roundtrip()) end,
         fun(_) -> ?_test(store_with_options()) end,
         fun(_) -> ?_test(is_local_for_block_mcid()) end,
         fun(_) -> ?_test(is_local_for_manifest_mcid()) end,
         fun(_) -> ?_test(status_returns_stats_map()) end
     ]}.

%%--- tests ---

mcid_default_algorithm() ->
    <<_:8, Codec:8, _:32/binary>> = hecate_content:mcid(<<"x">>),
    ?assertEqual(16#55, Codec).

mcid_explicit_sha256() ->
    M = hecate_content:mcid(<<"x">>, sha256),
    ?assertEqual(34, byte_size(M)).

mcid_string_roundtrip() ->
    {ok, Manifest} = hecate_content:create_manifest(<<"hi">>),
    Mcid = maps:get(mcid, Manifest),
    Str  = hecate_content:mcid_to_string(Mcid),
    ?assertEqual({ok, Mcid}, hecate_content:mcid_from_string(Str)).

create_and_verify_manifest() ->
    Data = <<"verify">>,
    {ok, M} = hecate_content:create_manifest(Data),
    ?assert(hecate_content:verify_manifest(M, Data)),
    ?assertNot(hecate_content:verify_manifest(M, <<"changd">>)).

missing_chunks_for_unstored_data() ->
    Data = <<"chunked-data-here">>,
    {ok, M} = hecate_content:create_manifest(Data, #{chunk_size => 4}),
    %% Nothing in the store yet, so all chunks are missing.
    ?assertEqual(maps:get(chunks, M), hecate_content:missing_chunks(M)).

store_then_fetch_roundtrip() ->
    Data = <<"store-and-fetch-content">>,
    {ok, MCID} = hecate_content:store(Data),
    ?assertEqual({ok, Data}, hecate_content:fetch(MCID)).

store_with_options() ->
    {ok, MCID} = hecate_content:store(<<"x">>,
                                       #{name => <<"named.txt">>,
                                         chunk_size => 1}),
    {ok, M} = hecate_content_store:get_manifest(MCID),
    ?assertEqual(<<"named.txt">>, maps:get(name, M)).

is_local_for_block_mcid() ->
    Data = <<"block-data">>,
    Hash = hecate_content_hasher:hash(blake3, Data),
    BlockMcid = <<1, 16#55, Hash/binary>>,
    ?assertNot(hecate_content:is_local(BlockMcid)),
    ok = hecate_content_store:put_block(BlockMcid, Data),
    ?assert(hecate_content:is_local(BlockMcid)).

is_local_for_manifest_mcid() ->
    Data = <<"manifested">>,
    {ok, MCID} = hecate_content:store(Data),
    ?assert(hecate_content:is_local(MCID)).

status_returns_stats_map() ->
    Stats = hecate_content:status(),
    ?assert(maps:is_key(block_count, Stats)),
    ?assert(maps:is_key(manifest_count, Stats)),
    ?assert(maps:is_key(total_size, Stats)).
