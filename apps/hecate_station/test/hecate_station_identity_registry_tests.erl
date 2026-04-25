%% @doc Eunit suite for the per-BEAM identity registry.
%%
%% Covers the CRUD primitives + EXIT-driven cleanup. Phase 1
%% identity_sups come up empty, so the lifecycle is purely
%% supervisor scaffolding — that is exactly what we want to
%% exercise here, without dragging in the per-identity workers
%% Phase 2 + 3 add later.
-module(hecate_station_identity_registry_tests).
-include_lib("eunit/include/eunit.hrl").

-define(REG, hecate_station_identity_registry).

%%==================================================================
%% Fixture
%%==================================================================

with_registry(Body) ->
    {setup,
     fun start_registry/0,
     fun stop_registry/1,
     fun(_Pid) -> Body end}.

start_registry() ->
    _ = (catch gen_server:stop(?REG)),
    {ok, Pid} = ?REG:start_link(),
    Pid.

stop_registry(_Pid) ->
    _ = (catch gen_server:stop(?REG)),
    ok.

opts(Key) ->
    #{identity_key => Key}.

%%==================================================================
%% Empty registry
%%==================================================================

empty_registry_is_empty_test_() ->
    with_registry([
        ?_assertEqual([], ?REG:list()),
        ?_assertEqual({error, not_found}, ?REG:lookup(<<"identity-a">>))
    ]).

%%==================================================================
%% register / lookup
%%==================================================================

register_then_lookup_test_() ->
    with_registry(fun() ->
        {ok, A} = ?REG:register(<<"identity-a">>, opts(<<"identity-a">>)),
        ?assert(is_pid(A)),
        ?assert(is_process_alive(A)),
        ?assertEqual({ok, A}, ?REG:lookup(<<"identity-a">>))
    end).

register_distinct_keys_coexist_test_() ->
    with_registry(fun() ->
        {ok, A} = ?REG:register(<<"identity-a">>, opts(<<"identity-a">>)),
        {ok, B} = ?REG:register(<<"identity-b">>, opts(<<"identity-b">>)),
        {ok, C} = ?REG:register(<<"identity-c">>, opts(<<"identity-c">>)),
        ?assertEqual(3, length(lists:usort([A, B, C]))),
        [?assert(is_process_alive(P)) || P <- [A, B, C]],

        Listed = lists:sort(?REG:list()),
        ?assertEqual([{<<"identity-a">>, A},
                      {<<"identity-b">>, B},
                      {<<"identity-c">>, C}], Listed)
    end).

double_register_same_key_errors_test_() ->
    with_registry(fun() ->
        {ok, A} = ?REG:register(<<"identity-a">>, opts(<<"identity-a">>)),
        ?assertEqual({error, already_registered},
                     ?REG:register(<<"identity-a">>, opts(<<"identity-a">>))),
        ?assertEqual({ok, A}, ?REG:lookup(<<"identity-a">>))
    end).

%%==================================================================
%% terminate
%%==================================================================

terminate_kills_sup_and_drops_entry_test_() ->
    with_registry(fun() ->
        {ok, A} = ?REG:register(<<"identity-a">>, opts(<<"identity-a">>)),
        Ref     = erlang:monitor(process, A),
        ok      = ?REG:terminate(<<"identity-a">>),
        receive {'DOWN', Ref, process, A, _} -> ok
        after 1_000 -> ?assert(false) end,
        ?assertNot(is_process_alive(A)),
        ?assertEqual({error, not_found}, ?REG:lookup(<<"identity-a">>)),
        ?assertEqual([], ?REG:list())
    end).

terminate_unknown_key_errors_test_() ->
    with_registry([
        ?_assertEqual({error, not_found},
                      ?REG:terminate(<<"missing">>))
    ]).

terminate_one_leaves_others_alive_test_() ->
    with_registry(fun() ->
        {ok, A} = ?REG:register(<<"identity-a">>, opts(<<"identity-a">>)),
        {ok, B} = ?REG:register(<<"identity-b">>, opts(<<"identity-b">>)),
        ok      = ?REG:terminate(<<"identity-a">>),
        ?assertNot(is_process_alive(A)),
        ?assert(is_process_alive(B)),
        ?assertEqual({error, not_found}, ?REG:lookup(<<"identity-a">>)),
        ?assertEqual({ok, B}, ?REG:lookup(<<"identity-b">>))
    end).

%%==================================================================
%% Crash cleanup — EXIT-driven path
%%==================================================================

sup_crash_drops_entry_test_() ->
    with_registry(fun() ->
        {ok, A} = ?REG:register(<<"identity-a">>, opts(<<"identity-a">>)),
        Ref     = erlang:monitor(process, A),
        exit(A, kill),
        receive {'DOWN', Ref, process, A, _} -> ok
        after 1_000 -> ?assert(false) end,
        wait_until(fun() ->
            ?REG:lookup(<<"identity-a">>) =:= {error, not_found}
        end, 50, 20),
        ?assertEqual({error, not_found}, ?REG:lookup(<<"identity-a">>)),
        ?assertEqual([], ?REG:list())
    end).

%%==================================================================
%% Helpers
%%==================================================================

wait_until(_F, _Sleep, 0) ->
    timeout;
wait_until(F, Sleep, N) ->
    wait_until_step(F(), F, Sleep, N).

wait_until_step(true, _F, _Sleep, _N) ->
    ok;
wait_until_step(_, F, Sleep, N) ->
    timer:sleep(Sleep),
    wait_until(F, Sleep, N - 1).
