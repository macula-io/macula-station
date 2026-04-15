%% @doc Integration tests for the Session 8.2 boot pipeline.
%%
%% Exercises `hecate_station_app:start/2' end-to-end with a stub tier
%% and a freshly-generated identity on disk. We are NOT using
%% `application:ensure_all_started' here: it pulls in macula_peering
%% and friends which have OS-level side effects (QUIC listener sockets)
%% that are not needed for the Phase 2 boot sequence and conflict
%% with the walking-skeleton suites running in the same VM.
%%
%% Instead, each test wires the minimum: sets env, starts the sup,
%% calls `hecate_station_app:start/2' directly, asserts process tree,
%% then tears it all down.
-module(hecate_station_app_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Disabled env — sup comes up with no children.
%%==================================================================

disabled_env_yields_empty_sup_test_() ->
    {setup, fun reset_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            process_flag(trap_exit, true),
            {ok, Sup} = hecate_station_app:start(normal, []),
            ?assert(is_pid(Sup)),
            ?assertEqual([], supervisor:which_children(Sup)),
            ok = cleanup_sup(Sup)
        end
    end}.

%%==================================================================
%% Happy path — cascade seeds DHT, SWIM starts.
%%==================================================================

happy_path_boots_dht_and_swim_test_() ->
    {setup, fun reset_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir   = make_tmpdir(),
            Peers = hecate_station_stub_tier:stub_peers(3),
            try
                set_station_env(Dir),
                set_bootstrap_tiers(Peers),
                {ok, Sup} = hecate_station_app:start(normal, []),
                %% Both runtime children registered.
                {ok, DhtPid}  = hecate_station:dht(),
                {ok, SwimPid} = hecate_station:swim(),
                ?assert(is_process_alive(DhtPid)),
                ?assert(is_process_alive(SwimPid)),
                ?assertEqual(3, hecate_dht:size(DhtPid)),
                ?assertEqual([], hecate_swim:members(SwimPid)),
                %% Both registered under the expected names.
                ?assertEqual(DhtPid,  whereis(hecate_dht)),
                ?assertEqual(SwimPid, whereis(hecate_swim)),
                ok = cleanup_sup(Sup)
            after
                rm_rf(Dir)
            end
        end
    end}.

%%==================================================================
%% no_tiers — sup shut down cleanly, SWIM never started.
%%==================================================================

no_tiers_aborts_boot_test_() ->
    {setup, fun reset_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            process_flag(trap_exit, true),
            Dir = make_tmpdir(),
            try
                set_station_env(Dir),
                %% No tiers in bootstrap env.
                application:set_env(hecate_bootstrap, tiers, []),
                ?assertEqual({error, no_tiers},
                             hecate_station_app:start(normal, [])),
                drain_exit_signals(),
                ok = wait_gone(hecate_station_sup, 2000),
                %% SWIM never started — registered name is gone.
                ?assertEqual(undefined, whereis(hecate_swim)),
                %% Sup was shut down.
                ?assertEqual(undefined, whereis(hecate_station_sup))
            after
                rm_rf(Dir)
            end
        end
    end}.

%%==================================================================
%% Identity continuity — warm-boot after an orderly shutdown.
%%==================================================================

warm_boot_preserves_identity_test_() ->
    {setup, fun reset_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir   = make_tmpdir(),
            Peers = hecate_station_stub_tier:stub_peers(1),
            try
                set_station_env(Dir),
                set_bootstrap_tiers(Peers),
                {ok, Sup1}       = hecate_station_app:start(normal, []),
                {ok, DhtPid1}    = hecate_station:dht(),
                Self1            = hecate_dht:self_id(DhtPid1),
                ok = cleanup_sup(Sup1),
                ?assertEqual(undefined, whereis(hecate_dht)),
                {ok, Sup2}       = hecate_station_app:start(normal, []),
                {ok, DhtPid2}    = hecate_station:dht(),
                Self2            = hecate_dht:self_id(DhtPid2),
                ?assertEqual(Self1, Self2),
                ok = cleanup_sup(Sup2)
            after
                rm_rf(Dir)
            end
        end
    end}.

%%==================================================================
%% Helpers
%%==================================================================

reset_env() ->
    %% Trap exits — `hecate_station_app:start/2' links the sup to the
    %% test process (no application_master in this context). Shutdown
    %% signals must not kill the eunit runner.
    process_flag(trap_exit, true),
    Station = [data_dir, identity_file, bind, port, certfile, keyfile,
               realms, capabilities],
    Boot    = [tiers, cascade_opts],
    Saved = [{S, station, application:get_env(hecate_station, S)} || S <- Station]
          ++ [{B, bootstrap, application:get_env(hecate_bootstrap, B)} || B <- Boot],
    [application:unset_env(hecate_station, K) || K <- Station],
    [application:unset_env(hecate_bootstrap, K) || K <- Boot],
    Saved.

restore_env(Saved) ->
    drain_exit_signals(),
    [restore_one(App, K, V) || {K, App, V} <- Saved],
    ok.

drain_exit_signals() ->
    receive
        {'EXIT', _Pid, _Reason} -> drain_exit_signals()
    after 0 -> ok
    end.

restore_one(_App, _K, undefined)       -> ok;
restore_one(station,   K, {ok, V})     -> application:set_env(hecate_station, K, V);
restore_one(bootstrap, K, {ok, V})     -> application:set_env(hecate_bootstrap, K, V).

set_station_env(Dir) ->
    application:set_env(hecate_station, data_dir, Dir),
    application:set_env(hecate_station, bind,     "127.0.0.1"),
    application:set_env(hecate_station, port,     9999),
    application:set_env(hecate_station, certfile, "/tmp/unused-cert.pem"),
    application:set_env(hecate_station, keyfile,  "/tmp/unused-key.pem").

set_bootstrap_tiers(Peers) ->
    Tiers = [{hecate_station_stub_tier, #{peers => Peers}}],
    application:set_env(hecate_bootstrap, tiers, Tiers),
    application:set_env(hecate_bootstrap, cascade_opts,
                        #{min_peers => 1, timeout_ms => 2000}).

make_tmpdir() ->
    Base = filename:join(["/tmp", "hecate-station-app-test",
                          integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(Base, "placeholder")),
    Base.

rm_rf(Dir) -> _ = os:cmd("rm -rf " ++ Dir), ok.

cleanup_sup(Sup) ->
    true = unlink(Sup),
    Ref  = monitor(process, Sup),
    exit(Sup, shutdown),
    receive {'DOWN', Ref, process, Sup, _} -> ok after 5000 -> timeout end.

wait_gone(Name, 0) ->
    ?assertEqual(undefined, whereis(Name)),
    ok;
wait_gone(Name, Ms) ->
    case whereis(Name) of
        undefined -> ok;
        _Pid      -> timer:sleep(20), wait_gone(Name, max(Ms - 20, 0))
    end.
