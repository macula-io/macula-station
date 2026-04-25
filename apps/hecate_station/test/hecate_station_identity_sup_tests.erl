%% @doc Eunit suite for the per-identity supervisor (Phase 1).
%%
%% Phase 1 deliverable: lay the OTP shape. The supervisor itself
%% has no children yet — Phase 2/3 wire the per-identity workers
%% in. The tests here verify:
%%
%% <ul>
%%   <li>start_link succeeds and returns a live, child-less
%%       supervisor pid;</li>
%%   <li>multiple identity_sup instances coexist in one BEAM (no
%%       `{local, _}' name registration);</li>
%%   <li>graceful shutdown via the parent's `exit(Sup, shutdown)'
%%       takes the sup down cleanly.</li>
%% </ul>
%%
%% End-to-end interaction with the registry — register, lookup,
%% terminate, crash cleanup — is covered by
%% `hecate_station_identity_registry_tests', which exercises the
%% supervisor as a real registry-owned child.
%%
%% Each test traps exits because `supervisor:start_link/2' links
%% the sup to the calling test process; an explicit
%% `exit(Sup, shutdown)' would otherwise propagate back and cancel
%% the test.
-module(hecate_station_identity_sup_tests).
-include_lib("eunit/include/eunit.hrl").

-define(SUP, hecate_station_identity_sup).

start_link_returns_live_supervisor_test() ->
    process_flag(trap_exit, true),
    {ok, Sup} = ?SUP:start_link(opts(<<"identity-a">>)),
    ?assert(is_pid(Sup)),
    ?assert(is_process_alive(Sup)),
    %% Phase 1 children list is empty — Phase 2/3 reparents the
    %% per-identity workers (station_server, swim, dht, overlay).
    ?assertEqual([], supervisor:which_children(Sup)),
    ok = shutdown(Sup).

multiple_identity_sups_coexist_test() ->
    process_flag(trap_exit, true),
    {ok, A} = ?SUP:start_link(opts(<<"identity-a">>)),
    {ok, B} = ?SUP:start_link(opts(<<"identity-b">>)),
    {ok, C} = ?SUP:start_link(opts(<<"identity-c">>)),
    [?assert(is_process_alive(P)) || P <- [A, B, C]],
    ?assertEqual(3, length(lists:usort([A, B, C]))),
    [ok = shutdown(P) || P <- [A, B, C]].

graceful_shutdown_takes_sup_down_test() ->
    process_flag(trap_exit, true),
    {ok, Sup} = ?SUP:start_link(opts(<<"identity-a">>)),
    Ref       = erlang:monitor(process, Sup),
    exit(Sup, shutdown),
    receive {'DOWN', Ref, process, Sup, _} -> ok
    after 1_000 -> ?assert(false) end,
    ?assertNot(is_process_alive(Sup)),
    flush_exit(Sup).

%%==================================================================
%% Helpers
%%==================================================================

opts(Key) ->
    #{identity_key => Key}.

shutdown(Sup) ->
    Ref = erlang:monitor(process, Sup),
    exit(Sup, shutdown),
    receive {'DOWN', Ref, process, Sup, _} -> ok
    after 1_000 -> exit(Sup, kill), ok end,
    flush_exit(Sup).

%% Drain any link-propagated EXIT signal the test process may still
%% have queued — `supervisor:start_link/2' links the sup to the
%% caller, so an explicit shutdown delivers `{'EXIT', Sup, shutdown}'
%% that would otherwise leak between tests.
flush_exit(Pid) ->
    receive {'EXIT', Pid, _} -> ok
    after 0 -> ok
    end.
