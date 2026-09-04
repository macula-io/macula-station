%% @doc Tests for the (publisher, seq) pubsub-event dedup cache,
%% including the arity-3 (publisher, seq, topic) form added 2026-09
%% (see `topic_scoped_dedup_test_/0' and
%% `macula_station_event_dedup:seen_or_record/3''s own doc).
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

%% The arity-3 (publisher, seq, topic) form: the actual bug this
%% shipped for (2026-09) was a bare (publisher, seq) key treating a
%% legitimate publish on a NEW topic as a repeat of some earlier
%% publish that happened to reuse the same seq value on a DIFFERENT
%% topic (macula-go's own RPC telemetry facts vs. an application's
%% business publish, both under one identity's counter space). Same
%% (publisher, seq) on two DIFFERENT topics must both be `new'; same
%% (publisher, seq, topic) repeated must still be `duplicate' -- the
%% loop-kill property topic-scoping must NOT weaken.
topic_scoped_dedup_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(_Pid) ->
        [ %% Same publisher+seq, different topics: both genuinely new.
          %% This is the exact shape of the bug -- an RPC fact and a
          %% business publish sharing one seq value must not collide.
          ?_assertEqual(new, macula_station_event_dedup:seen_or_record(pk(1), 0, <<"rpc.sent_v1">>))
        , ?_assertEqual(new, macula_station_event_dedup:seen_or_record(pk(1), 0, <<"agents.room.x">>))
          %% Loop-kill intact: repeating the SAME (publisher, seq, topic)
          %% triple is still caught as a duplicate.
        , ?_assertEqual(duplicate, macula_station_event_dedup:seen_or_record(pk(1), 0, <<"rpc.sent_v1">>))
        , ?_assertEqual(duplicate, macula_station_event_dedup:seen_or_record(pk(1), 0, <<"agents.room.x">>))
          %% A third, still-different topic under the same (publisher, seq)
          %% is also fresh.
        , ?_assertEqual(new, macula_station_event_dedup:seen_or_record(pk(1), 0, <<"agents.lobby">>))
          %% peek/3 mirrors seen_or_record/3 without writing or
          %% double-counting duplicates.
        , ?_assertEqual(seen,   macula_station_event_dedup:peek(pk(1), 0, <<"rpc.sent_v1">>))
        , ?_assertEqual(absent, macula_station_event_dedup:peek(pk(1), 0, <<"never_seen_topic">>))
          %% The arity-2 and arity-3 key spaces are disjoint -- a bare
          %% (publisher, seq) entry does not satisfy an arity-3 lookup
          %% for the "same" publisher/seq, and vice versa (different
          %% publisher here only to avoid the arity-2 dedup_test_'s own
          %% pk(1)/pk(2) writes bleeding into this independent setup).
        , ?_assertEqual(new,    macula_station_event_dedup:seen_or_record(pk(3), 0))
        , ?_assertEqual(absent, macula_station_event_dedup:peek(pk(3), 0, <<"anything">>))
          %% Malformed topic never dedups, same defensive contract as
          %% a malformed publisher/seq.
        , ?_assertEqual(new, macula_station_event_dedup:seen_or_record(pk(1), 0, not_a_binary))
        ]
    end}.

pk(N) -> <<N:256>>.
