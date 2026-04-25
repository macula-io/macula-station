-module(hecate_content_announcer_tests).
-include_lib("eunit/include/eunit.hrl").

%%--- helpers ---

setup() ->
    Dir = filename:join("/tmp",
            "hecate-content-announcer-"
            ++ integer_to_list(erlang:unique_integer([positive]))),
    {ok, Store} = hecate_content_store:start_link(#{store_path => Dir}),
    Kp = macula_identity:generate(),
    {ok, Announcer} = hecate_content_announcer:start_link(#{
        dht        => undefined,
        identity   => Kp,
        station_id => macula_identity:public(Kp),
        endpoint   => <<"quic://test:4433">>}),
    unlink(Store),
    unlink(Announcer),
    {Store, Announcer, Kp, Dir}.

cleanup({Store, Announcer, _Kp, Dir}) ->
    catch_stop_alive(Announcer, fun(P) -> hecate_content_announcer:stop(P) end),
    catch_stop_alive(Store,     fun(_P) -> hecate_content_store:stop() end),
    catch del_recursive(Dir).

catch_stop_alive(Pid, StopFn) ->
    case is_process_alive(Pid) of
        true  -> catch StopFn(Pid);
        false -> ok
    end.

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

mcid_for(Data) ->
    Hash = hecate_content_hasher:hash(blake3, Data),
    <<1, 16#56, Hash/binary>>.

%%--- generator ---

announcer_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun({_S, A, _K, _D}) -> ?_test(announcer_joins_event_group(A)) end,
         fun({_S, A, _K, _D}) ->
             ?_test(announce_with_undefined_dht_returns_error(A)) end,
         fun({_S, A, _K, _D}) ->
             ?_test(manifest_stored_event_reaches_announcer(A)) end,
         fun({_S, A, _K, _D}) ->
             ?_test(announce_by_mcid_uses_local_manifest(A)) end,
         fun({_S, A, _K, _D}) ->
             ?_test(announce_by_unknown_mcid_returns_not_found(A)) end
     ]}.

%%--- tests ---

announcer_joins_event_group(Announcer) ->
    Members = pg:get_members(hecate_content_store:events_group()),
    ?assert(lists:member(Announcer, Members)).

announce_with_undefined_dht_returns_error(Announcer) ->
    {ok, M} = hecate_content_manifest:create(<<"x">>),
    ?assertEqual({error, dht_not_configured},
                 hecate_content_announcer:announce(
                   Announcer, maps:get(mcid, M), M)).

manifest_stored_event_reaches_announcer(Announcer) ->
    %% Storing a manifest should fire a pg broadcast that the
    %% announcer receives. With undefined DHT, the announcer drops
    %% the event without crashing — that's the contract we're
    %% asserting here (no exception, no exit).
    {ok, M} = hecate_content_manifest:create(<<"event-test">>),
    ok = hecate_content_store:put_manifest(M),
    %% Give the cast loop a moment to flush.
    timer:sleep(50),
    ?assert(is_process_alive(Announcer)).

announce_by_mcid_uses_local_manifest(Announcer) ->
    {ok, M} = hecate_content_manifest:create(<<"by-mcid">>),
    ok = hecate_content_store:put_manifest(M),
    %% With undefined DHT the call returns dht_not_configured, but
    %% it had to fetch the manifest from the store first — so a
    %% successful manifest fetch is implicit in any non-not_found
    %% answer.
    Result = hecate_content_announcer:announce(Announcer, maps:get(mcid, M)),
    ?assertEqual({error, dht_not_configured}, Result).

announce_by_unknown_mcid_returns_not_found(Announcer) ->
    Unknown = mcid_for(crypto:strong_rand_bytes(16)),
    ?assertEqual({error, not_found},
                 hecate_content_announcer:announce(Announcer, Unknown)).
