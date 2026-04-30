-module(macula_content_store_tests).
-include_lib("eunit/include/eunit.hrl").

%%--- helpers ---

setup() ->
    Dir = test_dir(),
    {ok, Pid} = macula_content_store:start_link(#{store_path => Dir}),
    unlink(Pid),
    {Pid, Dir}.

cleanup({Pid, Dir}) ->
    case is_process_alive(Pid) of
        true  -> catch macula_content_store:stop();
        false -> ok
    end,
    %% Best-effort cleanup of test directory.
    catch del_recursive(Dir).

test_dir() ->
    Tag = integer_to_list(erlang:unique_integer([positive])),
    filename:join(["/tmp", "macula-content-store-" ++ Tag]).

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

block_mcid(Data) ->
    Hash = macula_content_hasher:hash(blake3, Data),
    <<1, 16#55, Hash/binary>>.

manifest() ->
    {ok, M} = macula_content_manifest:create(
                <<"hello">>, #{name => <<"x">>, chunk_size => 4096}),
    M.

%%--- generator ---

store_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun(_) -> ?_test(put_get_roundtrip()) end,
         fun(_) -> ?_test(put_rejects_bad_hash()) end,
         fun(_) -> ?_test(has_block_reflects_state()) end,
         fun(_) -> ?_test(delete_block_removes_it()) end,
         fun(_) -> ?_test(get_unknown_returns_not_found()) end,
         fun(_) -> ?_test(put_get_manifest_roundtrip()) end,
         fun(_) -> ?_test(list_manifests_returns_stored()) end,
         fun(_) -> ?_test(delete_manifest_removes_it()) end,
         fun(_) -> ?_test(stats_reflect_inserts()) end,
         fun(_) -> ?_test(verify_integrity_passes_for_clean_store()) end,
         fun(_) -> ?_test(gc_removes_orphan_blocks()) end
     ]}.

%%--- block tests ---

put_get_roundtrip() ->
    Data = <<"block-content">>,
    MCID = block_mcid(Data),
    ?assertEqual(ok, macula_content_store:put_block(MCID, Data)),
    ?assertEqual({ok, Data}, macula_content_store:get_block(MCID)).

put_rejects_bad_hash() ->
    %% MCID hash bytes don't match Data — should fail.
    BadMCID = <<1, 16#55, 0:256>>,
    ?assertEqual({error, hash_mismatch},
                 macula_content_store:put_block(BadMCID, <<"oops">>)).

has_block_reflects_state() ->
    Data = <<"x">>,
    MCID = block_mcid(Data),
    ?assertNot(macula_content_store:has_block(MCID)),
    ok = macula_content_store:put_block(MCID, Data),
    ?assert(macula_content_store:has_block(MCID)).

delete_block_removes_it() ->
    Data = <<"y">>,
    MCID = block_mcid(Data),
    ok = macula_content_store:put_block(MCID, Data),
    ok = macula_content_store:delete_block(MCID),
    ?assertEqual({error, not_found}, macula_content_store:get_block(MCID)).

get_unknown_returns_not_found() ->
    Unknown = <<1, 16#55, (crypto:strong_rand_bytes(32))/binary>>,
    ?assertEqual({error, not_found},
                 macula_content_store:get_block(Unknown)).

%%--- manifest tests ---

put_get_manifest_roundtrip() ->
    M = manifest(),
    ok = macula_content_store:put_manifest(M),
    {ok, Got} = macula_content_store:get_manifest(maps:get(mcid, M)),
    ?assertEqual(maps:get(mcid, M), maps:get(mcid, Got)).

list_manifests_returns_stored() ->
    M = manifest(),
    ok = macula_content_store:put_manifest(M),
    Stored = macula_content_store:list_manifests(),
    ?assert(lists:member(maps:get(mcid, M), Stored)).

delete_manifest_removes_it() ->
    M = manifest(),
    ok = macula_content_store:put_manifest(M),
    ok = macula_content_store:delete_manifest(maps:get(mcid, M)),
    ?assertEqual({error, not_found},
                 macula_content_store:get_manifest(maps:get(mcid, M))).

%%--- maintenance ---

stats_reflect_inserts() ->
    Data = <<"abc">>,
    MCID = block_mcid(Data),
    ok = macula_content_store:put_block(MCID, Data),
    Stats = macula_content_store:stats(),
    ?assert(maps:get(block_count, Stats) >= 1),
    ?assert(maps:get(total_size, Stats) >= byte_size(Data)).

verify_integrity_passes_for_clean_store() ->
    Data = <<"vi">>,
    MCID = block_mcid(Data),
    ok = macula_content_store:put_block(MCID, Data),
    ?assertMatch({ok, _}, macula_content_store:verify_integrity()).

gc_removes_orphan_blocks() ->
    %% Put a block but no manifest referencing it.
    Data = <<"orphan">>,
    MCID = block_mcid(Data),
    ok = macula_content_store:put_block(MCID, Data),
    {ok, #{removed := N}} = macula_content_store:gc(),
    ?assertEqual(1, N),
    ?assertNot(macula_content_store:has_block(MCID)).
