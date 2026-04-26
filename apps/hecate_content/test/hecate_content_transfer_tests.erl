-module(hecate_content_transfer_tests).
-include_lib("eunit/include/eunit.hrl").

%%--- helpers ---

mcid_for(Data) ->
    Hash = hecate_content_hasher:hash(blake3, Data),
    <<1, 16#55, Hash/binary>>.

setup() ->
    Dir = filename:join("/tmp",
            "hecate-content-transfer-"
            ++ integer_to_list(erlang:unique_integer([positive]))),
    {ok, Store} = hecate_content_store:start_link(#{store_path => Dir}),
    {ok, Trans} = hecate_content_transfer:start_link(),
    unlink(Store),
    unlink(Trans),
    {Store, Trans, Dir}.

cleanup({Store, Trans, Dir}) ->
    case is_process_alive(Trans) of
        true  -> catch hecate_content_transfer:stop();
        false -> ok
    end,
    case is_process_alive(Store) of
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

transfer_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun(_) -> ?_test(build_want_default_priority()) end,
         fun(_) -> ?_test(build_want_custom_priority()) end,
         fun(_) -> ?_test(build_have_carries_sizes()) end,
         fun(_) -> ?_test(build_manifest_req()) end,
         fun(_) -> ?_test(build_cancel()) end,
         fun(_) -> ?_test(request_blocks_returns_id()) end,
         fun(_) -> ?_test(pending_requests_lists_pending()) end,
         fun(_) -> ?_test(complete_request_clears()) end,
         fun(_) -> ?_test(cancel_request_clears()) end,
         fun(_) -> ?_test(request_info_known()) end,
         fun(_) -> ?_test(request_info_unknown()) end,
         fun(_) -> ?_test(process_inbound_want_returns_block_frame()) end,
         fun(_) -> ?_test(process_inbound_want_unknown_returns_error()) end,
         fun(_) -> ?_test(process_inbound_block_stores_data()) end,
         fun(_) -> ?_test(process_inbound_have_is_ok()) end,
         fun(_) -> ?_test(process_inbound_manifest_req_known()) end,
         fun(_) -> ?_test(process_inbound_manifest_req_unknown_returns_not_found_frame()) end,
         fun(_) -> ?_test(process_inbound_cancel_drops_matching_requests()) end
     ]}.

%%--- builders ---

build_want_default_priority() ->
    M = mcid_for(<<"a">>),
    F = hecate_content_transfer:build_want([M]),
    ?assertEqual(want, macula_frame:frame_type(F)),
    [B] = maps:get(blocks, F),
    ?assertEqual(128, maps:get(priority, B)).

build_want_custom_priority() ->
    M = mcid_for(<<"a">>),
    F = hecate_content_transfer:build_want([M], 200),
    [B] = maps:get(blocks, F),
    ?assertEqual(200, maps:get(priority, B)).

build_have_carries_sizes() ->
    M = mcid_for(<<"a">>),
    F = hecate_content_transfer:build_have([{M, 4096}]),
    ?assertEqual(have, macula_frame:frame_type(F)),
    [#{size := 4096}] = maps:get(blocks, F).

build_manifest_req() ->
    M = mcid_for(<<"a">>),
    F = hecate_content_transfer:build_manifest_req(M),
    ?assertEqual(manifest_req, macula_frame:frame_type(F)),
    ?assertEqual(M, maps:get(mcid, F)).

build_cancel() ->
    Ms = [mcid_for(<<"a">>), mcid_for(<<"b">>)],
    F = hecate_content_transfer:build_cancel(Ms),
    ?assertEqual(cancel, macula_frame:frame_type(F)),
    ?assertEqual(Ms, maps:get(blocks, F)).

%%--- request tracking ---

request_blocks_returns_id() ->
    {ok, Id} = hecate_content_transfer:request_blocks(
                 [mcid_for(<<"a">>)], <<"target">>),
    ?assertEqual(16, byte_size(Id)).

pending_requests_lists_pending() ->
    {ok, Id} = hecate_content_transfer:request_blocks(
                 [mcid_for(<<"a">>)], <<"target">>),
    ?assert(lists:member(Id, hecate_content_transfer:pending_requests())).

complete_request_clears() ->
    {ok, Id} = hecate_content_transfer:request_blocks(
                 [mcid_for(<<"a">>)], <<"target">>),
    ok = hecate_content_transfer:complete_request(Id),
    ?assertNot(lists:member(Id, hecate_content_transfer:pending_requests())).

cancel_request_clears() ->
    {ok, Id} = hecate_content_transfer:request_blocks(
                 [mcid_for(<<"a">>)], <<"target">>),
    ok = hecate_content_transfer:cancel_request(Id),
    ?assertNot(lists:member(Id, hecate_content_transfer:pending_requests())).

request_info_known() ->
    {ok, Id} = hecate_content_transfer:request_blocks(
                 [mcid_for(<<"x">>)], <<"t">>),
    {ok, Info} = hecate_content_transfer:request_info(Id),
    ?assertEqual(<<"t">>, maps:get(target_node, Info)),
    ?assertEqual(pending, maps:get(status, Info)).

request_info_unknown() ->
    ?assertEqual({error, not_found},
                 hecate_content_transfer:request_info(<<0:128>>)).

%%--- inbound dispatch ---

process_inbound_want_returns_block_frame() ->
    Data = <<"want-me">>,
    MCID = mcid_for(Data),
    ok = hecate_content_store:put_block(MCID, Data),
    Want = macula_frame:want(#{blocks => [#{mcid => MCID, priority => 128}]}),
    {ok, BlockFrame} =
        hecate_content_transfer:process_inbound(<<0:256>>, Want),
    ?assertEqual(block, macula_frame:frame_type(BlockFrame)),
    ?assertEqual(Data, maps:get(payload, BlockFrame)).

process_inbound_want_unknown_returns_error() ->
    Unknown = <<1, 16#55, (crypto:strong_rand_bytes(32))/binary>>,
    Want = macula_frame:want(#{blocks => [#{mcid => Unknown,
                                            priority => 128}]}),
    ?assertEqual({error, not_found},
                 hecate_content_transfer:process_inbound(<<0:256>>, Want)).

process_inbound_block_stores_data() ->
    Data = <<"block">>,
    MCID = mcid_for(Data),
    Frame = macula_frame:block(#{mcid => MCID, payload => Data}),
    ?assertEqual(ok,
                 hecate_content_transfer:process_inbound(<<0:256>>, Frame)),
    ?assertEqual({ok, Data}, hecate_content_store:get_block(MCID)).

process_inbound_have_is_ok() ->
    Frame = macula_frame:have(#{blocks => [#{mcid => mcid_for(<<"x">>),
                                              size => 1}]}),
    ?assertEqual(ok,
                 hecate_content_transfer:process_inbound(<<0:256>>, Frame)).

process_inbound_manifest_req_known() ->
    {ok, M} = hecate_content_manifest:create(<<"x">>),
    ok = hecate_content_store:put_manifest(M),
    Frame = macula_frame:manifest_req(#{mcid => maps:get(mcid, M)}),
    {ok, Res} = hecate_content_transfer:process_inbound(<<0:256>>, Frame),
    ?assertEqual(manifest_res, macula_frame:frame_type(Res)),
    ?assert(is_map(maps:get(manifest, Res))).

process_inbound_manifest_req_unknown_returns_not_found_frame() ->
    UnknownMcid = <<1, 16#56, (crypto:strong_rand_bytes(32))/binary>>,
    Frame = macula_frame:manifest_req(#{mcid => UnknownMcid}),
    {ok, Res} = hecate_content_transfer:process_inbound(<<0:256>>, Frame),
    ?assertEqual(not_found, maps:get(manifest, Res)).

process_inbound_cancel_drops_matching_requests() ->
    M  = mcid_for(<<"x">>),
    {ok, _Id} = hecate_content_transfer:request_blocks([M], <<"t">>),
    Frame = macula_frame:cancel(#{blocks => [M]}),
    ok = hecate_content_transfer:process_inbound(<<0:256>>, Frame),
    %% Cast — give it a moment to flush.
    timer:sleep(50),
    ?assertEqual([], hecate_content_transfer:pending_requests()).
