%% @doc Eunit suite for the per-identity supervisor.
%%
%% Verifies:
%%
%% <ul>
%%   <li>start_link succeeds and returns a live supervisor pid
%%       carrying the de-singletonised pubsub_registry as a
%%       per-identity child;</li>
%%   <li>passing the announcer prerequisites (identity / station_id
%%       / endpoint) brings up the content_announcer alongside the
%%       registry;</li>
%%   <li>multiple identity_sup instances coexist in one BEAM with
%%       disjoint pubsub_registry pids (no `{local, _}'
%%       collisions);</li>
%%   <li>graceful shutdown via the parent's `exit(Sup, shutdown)'
%%       takes the sup down cleanly.</li>
%% </ul>
%%
%% End-to-end interaction with the registry — register, lookup,
%% terminate, crash cleanup — is covered by
%% `macula_station_identity_registry_tests', which exercises the
%% supervisor as a real registry-owned child.
%%
%% Each test traps exits because `supervisor:start_link/2' links
%% the sup to the calling test process; an explicit
%% `exit(Sup, shutdown)' would otherwise propagate back and cancel
%% the test.
-module(macula_station_identity_sup_tests).
-include_lib("eunit/include/eunit.hrl").

-define(SUP, macula_station_identity_sup).

start_link_returns_live_supervisor_test() ->
    process_flag(trap_exit, true),
    {ok, Sup} = ?SUP:start_link(opts(<<"identity-a">>)),
    ?assert(is_pid(Sup)),
    ?assert(is_process_alive(Sup)),
    %% Phase 2 wires pubsub_registry as the always-on per-identity
    %% child. The content_announcer is opt-in; absent identity /
    %% station_id / endpoint it is omitted (this is the path the
    %% Phase 1 lifecycle tests still travel through `opts/1').
    Ids = lists:sort(
        [Id || {Id, _, _, _} <- supervisor:which_children(Sup)]),
    ?assertEqual([hecate_pubsub_registry], Ids),
    ok = shutdown(Sup).

start_link_with_announcer_opts_includes_announcer_test() ->
    process_flag(trap_exit, true),
    Kp = macula_identity:generate(),
    Opts = #{
        identity_key => <<"identity-a">>,
        identity     => Kp,
        station_id   => macula_identity:public(Kp),
        endpoint     => <<"quic://test:4433">>
    },
    {ok, Sup} = ?SUP:start_link(Opts),
    Ids = lists:sort(
        [Id || {Id, _, _, _} <- supervisor:which_children(Sup)]),
    ?assertEqual(lists:sort([hecate_pubsub_registry,
                             macula_content_announcer]),
                 Ids),
    ok = shutdown(Sup).

multiple_identity_sups_coexist_test() ->
    process_flag(trap_exit, true),
    {ok, A} = ?SUP:start_link(opts(<<"identity-a">>)),
    {ok, B} = ?SUP:start_link(opts(<<"identity-b">>)),
    {ok, C} = ?SUP:start_link(opts(<<"identity-c">>)),
    [?assert(is_process_alive(P)) || P <- [A, B, C]],
    ?assertEqual(3, length(lists:usort([A, B, C]))),
    [ok = shutdown(P) || P <- [A, B, C]].

multi_identity_sups_have_disjoint_registries_test() ->
    process_flag(trap_exit, true),
    {ok, A} = ?SUP:start_link(opts(<<"identity-a">>)),
    {ok, B} = ?SUP:start_link(opts(<<"identity-b">>)),
    RegA = registry_pid(A),
    RegB = registry_pid(B),
    ?assertNotEqual(RegA, RegB),
    [?assert(is_process_alive(P)) || P <- [RegA, RegB]],
    %% Subscribe to a realm under A; B's registry must not see it.
    R  = crypto:strong_rand_bytes(32),
    Kp = macula_identity:generate(),
    {ok, _ServerA} = hecate_pubsub_registry:register(RegA, R, Kp),
    ?assertEqual([R], hecate_pubsub_registry:list_realms(RegA)),
    ?assertEqual([], hecate_pubsub_registry:list_realms(RegB)),
    ok = shutdown(A),
    ok = shutdown(B).

registry_pid(Sup) ->
    [Pid] = [P || {hecate_pubsub_registry, P, _, _}
                  <- supervisor:which_children(Sup)],
    Pid.

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
