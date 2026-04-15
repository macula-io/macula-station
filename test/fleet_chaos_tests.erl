%% @doc Unit tests for `fleet_chaos' primitives.
%%
%% Exercises each primitive in isolation without peer nodes — just
%% a local gen_server as the probe target, or a bare spawned pid.
%% The cross-node + station-integration paths are covered by
%% `fleet_SUITE' (which now consumes these same primitives).
-module(fleet_chaos_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% kill_pid/1
%%==================================================================

kill_pid_on_dead_pid_returns_ok_test() ->
    Pid = spawn(fun() -> ok end),
    ok  = wait_dead(Pid, 200),
    ?assertEqual(ok, fleet_chaos:kill_pid(Pid)).

kill_pid_on_alive_pid_kills_it_test() ->
    Pid = spawn(fun() -> timer:sleep(60_000) end),
    ?assert(is_process_alive(Pid)),
    ?assertEqual(ok, fleet_chaos:kill_pid(Pid)),
    ?assertNot(is_process_alive(Pid)).

%%==================================================================
%% pause/1 + resume/1 — wrap sys:suspend / sys:resume.
%%==================================================================

pause_and_resume_gen_server_test() ->
    {ok, Pid} = echo_server:start_link(),
    try
        ?assertEqual(pong, echo_server:ping(Pid)),
        ok = fleet_chaos:pause(Pid),
        %% A plain call would block forever on a suspended gen_server;
        %% use a monitored short-timeout call to prove the process is
        %% still registered but unresponsive.
        ?assertExit({timeout, _}, gen_server:call(Pid, ping, 100)),
        ok = fleet_chaos:resume(Pid),
        ?assertEqual(pong, echo_server:ping(Pid))
    after
        echo_server:stop(Pid)
    end.

%%==================================================================
%% wait_until/2
%%==================================================================

wait_until_returns_ok_when_predicate_flips_test() ->
    Counter = counters:new(1, [atomics]),
    _ = spawn_link(fun() ->
        timer:sleep(100),
        counters:put(Counter, 1, 1)
    end),
    Pred = fun() -> counters:get(Counter, 1) > 0 end,
    ?assertEqual(ok, fleet_chaos:wait_until(Pred, 1_000)).

wait_until_times_out_test() ->
    Pred = fun() -> false end,
    ?assertEqual(timeout, fleet_chaos:wait_until(Pred, 50)).

wait_until_handles_raising_predicate_test() ->
    %% A predicate that raises (because a remote pid just died,
    %% say) should be treated as "not yet true" rather than
    %% aborting the wait.
    Counter = counters:new(1, [atomics]),
    Pred = fun() ->
        case counters:get(Counter, 1) of
            0 -> error(not_ready);
            _ -> true
        end
    end,
    _ = spawn_link(fun() ->
        timer:sleep(100),
        counters:put(Counter, 1, 1)
    end),
    ?assertEqual(ok, fleet_chaos:wait_until(Pred, 1_000)).

%%==================================================================
%% Helpers
%%==================================================================

wait_dead(Pid, 0)  -> is_process_alive(Pid) andalso error(still_alive), ok;
wait_dead(Pid, Ms) ->
    case is_process_alive(Pid) of
        false -> ok;
        true  -> timer:sleep(10), wait_dead(Pid, Ms - 10)
    end.
