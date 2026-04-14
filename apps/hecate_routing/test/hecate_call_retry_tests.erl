%% EUnit tests for hecate_call_retry.
-module(hecate_call_retry_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Defaults — Part 4 §6.4 presets
%%---------------------------------------------------------------------

interactive_defaults_match_spec_test() ->
    ?assertEqual(#{retry_budget => 2, overall_timeout_ms => 5_000},
                 hecate_call_retry:interactive_defaults()).

background_defaults_match_spec_test() ->
    ?assertEqual(#{retry_budget => 5, overall_timeout_ms => 30_000},
                 hecate_call_retry:background_defaults()).

bulk_defaults_provide_high_budget_test() ->
    #{retry_budget := Budget,
      overall_timeout_ms := Timeout} =
        hecate_call_retry:bulk_defaults(),
    ?assert(Budget >= 100),
    ?assert(Timeout >= 60_000).

%%---------------------------------------------------------------------
%% Happy path — first attempt succeeds
%%---------------------------------------------------------------------

first_attempt_success_terminates_immediately_test() ->
    Counter = spawn_counter(),
    Result = hecate_call_retry:execute(#{
        paths  => [path(p1), path(p2), path(p3)],
        try_fn => succeeding_fn(<<"hello">>, Counter)
    }),
    ?assertEqual({ok, <<"hello">>}, maps:get(outcome, Result)),
    ?assertEqual(1, length(maps:get(attempts, Result))),
    ?assertEqual(1, counter_value(Counter)),
    stop_counter(Counter).

%%---------------------------------------------------------------------
%% Retryable failure → rotates to next path
%%---------------------------------------------------------------------

retryable_failure_rotates_to_next_path_test() ->
    Counter = spawn_counter(),
    %% Fail on first 2 paths, succeed on third.
    TryFn = fun(P) ->
        bump(Counter),
        case P of
            [p3 | _] -> {ok, <<"third try">>};
            _        -> {error, unknown_next_peer, #{}}
        end
    end,
    Result = hecate_call_retry:execute(#{
        paths        => [path(p1), path(p2), path(p3)],
        try_fn       => TryFn,
        retry_budget => 2
    }),
    ?assertEqual({ok, <<"third try">>}, maps:get(outcome, Result)),
    ?assertEqual(3, length(maps:get(attempts, Result))),
    %% Each attempt's path is recorded.
    Paths = [maps:get(path, A) || A <- maps:get(attempts, Result)],
    ?assertEqual([path(p1), path(p2), path(p3)], Paths),
    stop_counter(Counter).

%%---------------------------------------------------------------------
%% Non-retryable codes never rotate
%%---------------------------------------------------------------------

target_realm_refused_does_not_retry_test() ->
    Counter = spawn_counter(),
    TryFn = fun(_) ->
        bump(Counter),
        {error, target_realm_refused, #{}}
    end,
    Result = hecate_call_retry:execute(#{
        paths        => [path(p1), path(p2), path(p3)],
        try_fn       => TryFn,
        retry_budget => 5
    }),
    ?assertMatch({error, target_realm_refused, _},
                 maps:get(outcome, Result)),
    ?assertEqual(1, counter_value(Counter)),
    stop_counter(Counter).

signature_invalid_does_not_retry_test() ->
    Counter = spawn_counter(),
    TryFn = fun(_) ->
        bump(Counter),
        {error, signature_invalid, #{}}
    end,
    Result = hecate_call_retry:execute(#{
        paths => [path(p1), path(p2)], try_fn => TryFn,
        retry_budget => 5
    }),
    ?assertMatch({error, signature_invalid, _},
                 maps:get(outcome, Result)),
    ?assertEqual(1, counter_value(Counter)),
    stop_counter(Counter).

tombstoned_does_not_retry_test() ->
    Counter = spawn_counter(),
    TryFn = fun(_) ->
        bump(Counter),
        {error, tombstoned, #{}}
    end,
    Result = hecate_call_retry:execute(#{
        paths => [path(p1), path(p2)], try_fn => TryFn,
        retry_budget => 5
    }),
    ?assertMatch({error, tombstoned, _}, maps:get(outcome, Result)),
    ?assertEqual(1, counter_value(Counter)),
    stop_counter(Counter).

%%---------------------------------------------------------------------
%% Budget exhaustion
%%---------------------------------------------------------------------

budget_zero_means_no_retries_test() ->
    Counter = spawn_counter(),
    TryFn = fun(_) ->
        bump(Counter),
        {error, unknown_next_peer, #{}}
    end,
    Result = hecate_call_retry:execute(#{
        paths => [path(p1), path(p2), path(p3)],
        try_fn => TryFn,
        retry_budget => 0
    }),
    ?assertMatch({error, unknown_next_peer, _}, maps:get(outcome, Result)),
    ?assertEqual(1, counter_value(Counter)),
    stop_counter(Counter).

budget_exhausted_returns_last_error_test() ->
    Counter = spawn_counter(),
    TryFn = fun(_) ->
        bump(Counter),
        {error, temporary_relay_failure, #{}}
    end,
    Result = hecate_call_retry:execute(#{
        paths => [path(p1), path(p2), path(p3), path(p4)],
        try_fn => TryFn,
        retry_budget => 2
    }),
    ?assertMatch({error, temporary_relay_failure, _},
                 maps:get(outcome, Result)),
    %% 1 initial + 2 retries = 3 attempts.
    ?assertEqual(3, counter_value(Counter)),
    stop_counter(Counter).

%%---------------------------------------------------------------------
%% Path exhaustion (fewer paths than budget allows)
%%---------------------------------------------------------------------

path_exhaustion_short_circuits_budget_test() ->
    Counter = spawn_counter(),
    TryFn = fun(_) ->
        bump(Counter),
        {error, unknown_next_peer, #{}}
    end,
    Result = hecate_call_retry:execute(#{
        paths => [path(p1)],   %% only one path
        try_fn => TryFn,
        retry_budget => 5
    }),
    ?assertMatch({error, unknown_next_peer, _}, maps:get(outcome, Result)),
    ?assertEqual(1, counter_value(Counter)),
    stop_counter(Counter).

%%---------------------------------------------------------------------
%% Overall deadline
%%---------------------------------------------------------------------

overall_deadline_truncates_attempts_test() ->
    Counter = spawn_counter(),
    %% Each attempt sleeps for 60 ms, so within a 150 ms total only
    %% a couple should fit.
    TryFn = fun(_) ->
        bump(Counter),
        timer:sleep(60),
        {error, unknown_next_peer, #{}}
    end,
    Result = hecate_call_retry:execute(#{
        paths => [path(p1), path(p2), path(p3),
                  path(p4), path(p5), path(p6)],
        try_fn => TryFn,
        retry_budget => 10,
        overall_timeout_ms => 150
    }),
    %% The deadline_exceeded synthesised error is returned.
    ?assertMatch({error, expiry_too_soon, #{phase := retry,
                                            reason := deadline}},
                 maps:get(outcome, Result)),
    %% Strictly fewer than 6 attempts due to deadline.
    ?assert(counter_value(Counter) < 6),
    stop_counter(Counter).

%%---------------------------------------------------------------------
%% Attempts log preserved
%%---------------------------------------------------------------------

attempts_log_records_each_path_and_outcome_test() ->
    TryFn = fun([First | _]) ->
        case First of
            p1 -> {error, unknown_next_peer, #{phase => p1}};
            p2 -> {error, temporary_relay_failure, #{phase => p2}};
            p3 -> {ok, <<"third">>}
        end
    end,
    Result = hecate_call_retry:execute(#{
        paths => [path(p1), path(p2), path(p3)],
        try_fn => TryFn,
        retry_budget => 2
    }),
    Attempts = maps:get(attempts, Result),
    ?assertEqual(3, length(Attempts)),
    [A1, A2, A3] = Attempts,
    ?assertMatch(#{path := [p1 | _],
                   outcome := {error, unknown_next_peer, _}}, A1),
    ?assertMatch(#{path := [p2 | _],
                   outcome := {error, temporary_relay_failure, _}}, A2),
    ?assertMatch(#{path := [p3 | _],
                   outcome := {ok, <<"third">>}}, A3).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

%% Build a fake "path" — content doesn't matter for these unit
%% tests, only the identity of each list. We use atoms so the
%% test output is readable.
path(Sym) -> [Sym, target].

succeeding_fn(Reply, Counter) ->
    fun(_) ->
        bump(Counter),
        {ok, Reply}
    end.

spawn_counter() ->
    spawn(fun() -> counter_loop(0) end).

counter_loop(N) ->
    receive
        bump          -> counter_loop(N + 1);
        {value, From} -> From ! {value, N}, counter_loop(N);
        stop          -> ok
    end.

bump(Counter) ->
    Counter ! bump.

counter_value(Counter) ->
    Counter ! {value, self()},
    receive {value, V} -> V after 1_000 -> exit(counter_timeout) end.

stop_counter(Counter) ->
    Counter ! stop.
