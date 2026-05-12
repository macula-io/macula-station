%% @doc Integration tests for the Session 8.2 + 8.3 boot pipeline.
%%
%% Exercises `macula_station_app:start/2' end-to-end with a stub tier
%% and a freshly-generated identity on disk. Tests that only need the
%% process tree (8.2) skip QUIC; tests that exercise the listener +
%% peer observer (8.3) spin up a real TLS/QUIC listener on a free
%% loopback port and dial it from an independent `macula_peering'
%% client to assert the observer-driven DHT observe + SWIM add_peer
%% paths.
%%
%% Each test wires the minimum: sets env, starts the sup via
%% `macula_station_app:start/2', asserts outcomes, tears down.
-module(macula_station_app_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("macula_station/include/macula_station_cfg.hrl").

%%==================================================================
%% Disabled env — sup comes up with the always-on identity registry
%% but no boot-pipeline children.
%%==================================================================

disabled_env_yields_static_children_only_test_() ->
    {setup, fun reset_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            process_flag(trap_exit, true),
            {ok, Sup} = macula_station_app:start(normal, []),
            ?assert(is_pid(Sup)),
            %% Three always-on infrastructure children:
            %% event_dedup ((publisher,seq) pubsub-event dedup cache),
            %% record_fanout (singleton DHT-record fan-out),
            %% peer_links (outbound station_link registry).
            %% supervisor:which_children/1 returns them in
            %% reverse-start order, so the last-started (event_dedup)
            %% comes first.
            Children = supervisor:which_children(Sup),
            ?assertMatch([{macula_station_event_dedup,
                           _, worker,
                           [macula_station_event_dedup]},
                          {macula_station_record_fanout,
                           _, worker,
                           [macula_station_record_fanout]},
                          {macula_station_peer_links,
                           _, worker,
                           [macula_station_peer_links]}],
                         Children),
            ok = cleanup_sup(Sup)
        end
    end}.

%%==================================================================
%% Happy path — cascade seeds DHT, SWIM starts.
%%==================================================================

happy_path_boots_full_runtime_test_() ->
    {setup, fun reset_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            process_flag(trap_exit, true),
            Dir   = make_tmpdir(),
            Peers = macula_station_stub_tier:stub_peers(3),
            try
                set_station_env(Dir),
                set_discoverers(Peers),
                {ok, Sup} = macula_station_app:start(normal, []),
                {ok, DhtPid}      = macula_station:dht(),
                {ok, SwimPid}     = macula_station:swim(),
                {ok, ObserverPid} = macula_station:observer(),
                {ok, ListenerPid} = macula_station:listener(),
                [?assert(is_process_alive(P))
                 || P <- [DhtPid, SwimPid, ObserverPid, ListenerPid]],
                ?assertEqual(3, macula_dht:size(DhtPid)),
                ?assertEqual([], macula_swim:members(SwimPid)),
                ?assertMatch({"127.0.0.1", _Port},
                             macula_station:listen_addr()),
                ok = cleanup_sup(Sup)
            after
                rm_rf(Dir)
            end
        end
    end}.

cache_and_rebootstrap_children_start_when_configured_test_() ->
    {setup, fun reset_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            process_flag(trap_exit, true),
            Dir   = make_tmpdir(),
            Peers = macula_station_stub_tier:stub_peers(1),
            try
                set_station_env(Dir),
                set_discoverers(Peers),
                application:set_env(macula_station, cache, #{
                    path => filename:join(Dir, "cache"),
                    flush_period_ms => 60_000
                }),
                application:set_env(macula_station, rebootstrap, #{
                    min_viable_peers => 8,
                    check_period_ms => 60_000,
                    partition_window_ms => 60_000
                }),
                {ok, Sup} = macula_station_app:start(normal, []),
                {ok, CachePid} = macula_station:cache(),
                {ok, RbPid}    = macula_station:rebootstrap(),
                ?assert(is_process_alive(CachePid)),
                ?assert(is_process_alive(RbPid)),
                %% Force a flush; file should appear under the cache dir.
                ok = macula_station_cache:flush(CachePid),
                ?assert(filelib:is_regular(macula_station_cache:path(
                    filename:join(Dir, "cache")))),
                ok = cleanup_sup(Sup)
            after
                application:unset_env(macula_station, cache),
                application:unset_env(macula_station, rebootstrap),
                rm_rf(Dir)
            end
        end
    end}.

%%==================================================================
%% End-to-end — external peering client dials the station; observer
%% records the peer into the DHT with tier=t0 and SWIM learns it.
%%==================================================================

external_peer_dial_lands_in_dht_and_swim_test_() ->
    {setup, fun reset_env/0, fun restore_env/1, fun(_) ->
        {timeout, 30,
         fun() ->
             process_flag(trap_exit, true),
             Dir   = make_tmpdir(),
             Peers = macula_station_stub_tier:stub_peers(1),
             try
                 set_station_env(Dir),
                 set_discoverers(Peers),
                 {ok, Sup} = macula_station_app:start(normal, []),
                 {ok, Dht}  = macula_station:dht(),
                 {ok, Swim} = macula_station:swim(),
                 {_Bind, Port} = macula_station:listen_addr(),
                 %% External peer with its own identity dials our listener.
                 ExtKp = macula_identity:generate(),
                 ExtId = macula_identity:public(ExtKp),
                 {ok, _ClientPid} = macula_peering:connect(#{
                     role            => client,
                     identity        => ExtKp,
                     realms          => [],
                     capabilities    => 16#FF,
                     controlling_pid => self(),
                     target          => #{host => "127.0.0.1",
                                          port => Port,
                                          timeout_ms => 3_000}
                 }),
                 ok = wait_until(fun() ->
                     macula_dht:contains(Dht, ExtId) andalso
                     lists:any(fun(#{node_id := N}) -> N =:= ExtId end,
                               macula_swim:members(Swim))
                 end, 10_000),
                 %% Observer-admitted entries are tier t0.
                 {ok, Entry} = macula_dht:find(Dht, ExtId),
                 ?assertEqual(t0, macula_dht_entry:tier(Entry)),
                 %% Pre-existing stub-tier peer + external = 2 entries.
                 ?assertEqual(2, macula_dht:size(Dht)),
                 ok = cleanup_sup(Sup)
             after
                 rm_rf(Dir)
             end
         end}
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
                application:set_env(macula_bootstrap, discoverers, []),
                ?assertEqual({error, no_tiers},
                             macula_station_app:start(normal, [])),
                drain_exit_signals(),
                ok = wait_gone(macula_station_sup, 2000),
                %% SWIM never started — registered name is gone.
                ?assertEqual(undefined, whereis(macula_swim)),
                %% Sup was shut down.
                ?assertEqual(undefined, whereis(macula_station_sup))
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
            Peers = macula_station_stub_tier:stub_peers(1),
            try
                set_station_env(Dir),
                set_discoverers(Peers),
                {ok, Sup1}       = macula_station_app:start(normal, []),
                {ok, DhtPid1}    = macula_station:dht(),
                Self1            = macula_dht:self_id(DhtPid1),
                ok = cleanup_sup(Sup1),
                ?assertEqual(undefined, whereis(macula_dht)),
                {ok, Sup2}       = macula_station_app:start(normal, []),
                {ok, DhtPid2}    = macula_station:dht(),
                Self2            = macula_dht:self_id(DhtPid2),
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
    %% Trap exits — `macula_station_app:start/2' links the sup to the
    %% test process (no application_master in this context). Shutdown
    %% signals must not kill the eunit runner.
    process_flag(trap_exit, true),
    {ok, _} = application:ensure_all_started(macula),
    Station = [data_dir, identity_file, bind, port, certfile, keyfile,
               capabilities, cache, rebootstrap, admin],
    Boot    = [discoverers, cascade_opts],
    Saved = [{S, station, application:get_env(macula_station, S)} || S <- Station]
          ++ [{B, bootstrap, application:get_env(macula_bootstrap, B)} || B <- Boot],
    [application:unset_env(macula_station, K) || K <- Station],
    [application:unset_env(macula_bootstrap, K) || K <- Boot],
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
restore_one(station,   K, {ok, V})     -> application:set_env(macula_station, K, V);
restore_one(bootstrap, K, {ok, V})     -> application:set_env(macula_bootstrap, K, V).

set_station_env(Dir) ->
    {Cert, Key} = generate_test_cert(Dir),
    application:set_env(macula_station, data_dir, Dir),
    application:set_env(macula_station, bind,     "127.0.0.1"),
    application:set_env(macula_station, port,     free_port()),
    application:set_env(macula_station, certfile, Cert),
    application:set_env(macula_station, keyfile,  Key).

%% Self-signed cert + key into Dir. Requires openssl on PATH (CI has it).
generate_test_cert(Dir) ->
    ok       = filelib:ensure_dir(filename:join(Dir, "x")),
    CertPath = filename:join(Dir, "cert.pem"),
    KeyPath  = filename:join(Dir, "key.pem"),
    Cmd = lists:flatten(io_lib:format(
        "openssl req -x509 -newkey rsa:2048 -nodes "
        "-keyout ~s -out ~s -days 1 -subj /CN=localhost 2>&1",
        [KeyPath, CertPath])),
    Out = os:cmd(Cmd),
    true = filelib:is_regular(CertPath) orelse error({openssl_failed, Out}),
    {CertPath, KeyPath}.

free_port() ->
    {ok, S} = gen_udp:open(0, [{reuseaddr, true}]),
    {ok, Port} = inet:port(S),
    ok = gen_udp:close(S),
    Port.

set_discoverers(Peers) ->
    Tiers = [{macula_station_stub_tier, #{peers => Peers}}],
    application:set_env(macula_bootstrap, discoverers, Tiers),
    application:set_env(macula_bootstrap, cascade_opts,
                        #{min_peers => 1, timeout_ms => 2000}).

make_tmpdir() ->
    Base = filename:join(["/tmp", "macula-station-app-test",
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

wait_until(Pred, Ms) ->
    wait_until_step(Pred(), Pred, Ms).

wait_until_step(true,  _Pred, _Ms)              -> ok;
wait_until_step(false, Pred, Ms) when Ms =< 0   -> ?assert(Pred()), ok;
wait_until_step(false, Pred, Ms)                ->
    timer:sleep(50),
    wait_until_step(Pred(), Pred, Ms - 50).
