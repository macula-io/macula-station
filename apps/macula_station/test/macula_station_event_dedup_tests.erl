%% @doc Tests for the (publisher, seq) pubsub-event dedup cache.
%%
%% Drives the gen_server directly (it owns a named public ETS table);
%% no station boot needed.
-module(macula_station_event_dedup_tests).
-include_lib("eunit/include/eunit.hrl").

setup() ->
    {ok, Pid} = macula_station_event_dedup:start_link(),
    Pid.

cleanup(Pid) ->
    catch gen_server:stop(Pid),
    %% start_link/0 registers locally; make sure a stale name/table
    %% does not leak into the next test.
    catch persistent_term:erase({macula_station_event_dedup, dup_counter}),
    ok.

dedup_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(_Pid) ->
        [ ?_assertEqual(new,       macula_station_event_dedup:seen_or_record(pk(1), 0))
        , ?_assertEqual(duplicate, macula_station_event_dedup:seen_or_record(pk(1), 0))
        , ?_assertEqual(duplicate, macula_station_event_dedup:seen_or_record(pk(1), 0))
        , ?_assertEqual(new,       macula_station_event_dedup:seen_or_record(pk(1), 1))
        , ?_assertEqual(new,       macula_station_event_dedup:seen_or_record(pk(2), 0))
        , ?_assertEqual(duplicate, macula_station_event_dedup:seen_or_record(pk(2), 0))
        , ?_assert(macula_station_event_dedup:window_size() >= 3)
        , ?_assert(macula_station_event_dedup:dup_count() >= 3)
        %% Malformed keys are never deduped (and never crash).
        , ?_assertEqual(new, macula_station_event_dedup:seen_or_record(not_a_binary, 0))
        , ?_assertEqual(new, macula_station_event_dedup:seen_or_record(pk(1), -1))
        , ?_assertEqual(new, macula_station_event_dedup:seen_or_record(pk(1), not_an_int))
        ]
    end}.

%% No table → graceful `new', no crash.
no_table_is_graceful_test() ->
    %% No start_link/0 in this test, so the named table does not exist.
    ?assertEqual(undefined, ets:whereis(macula_station_event_dedup)),
    ?assertEqual(new, macula_station_event_dedup:seen_or_record(pk(9), 0)),
    ?assertEqual(0, macula_station_event_dedup:window_size()),
    ?assertEqual(0, macula_station_event_dedup:dup_count()).

pk(N) -> <<N:256>>.
