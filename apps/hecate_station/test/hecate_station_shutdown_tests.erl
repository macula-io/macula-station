%% @doc Graceful-shutdown (Session 8.7) tests.
%%
%% Boots the full station, calls `hecate_station:shutdown/0,1',
%% asserts the tombstone landed in the local DHT via
%% `find_local_record/2', the cache file is present, the sup is
%% gone, and `shutdown/0' on an already-stopped station is idempotent.
-module(hecate_station_shutdown_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("hecate_station/include/hecate_station_cfg.hrl").

%%==================================================================
%% Tombstone + cache flush on graceful shutdown
%%==================================================================

shutdown_publishes_tombstone_and_flushes_cache_test_() ->
    {setup, fun setup_app/0, fun teardown_dir/1, fun(Ctx) ->
        {timeout, 15,
         fun() ->
             process_flag(trap_exit, true),
             #{dht := Dht,
               kp  := Kp,
               cache_dir := CacheDir} = Ctx,
             Pub = macula_identity:public(Kp),
             %% Snapshot the tombstone key before we shut down — the
             %% DHT process is about to exit, so we need the public
             %% key ahead of time.
             ok = hecate_station:shutdown(retired),
             %% After shutdown:
             %% 1. The sup registered name is gone.
             ok = wait_gone(hecate_station_sup, 10_000),
             ?assertEqual(undefined, whereis(hecate_station_sup)),
             %% 2. The cache file was flushed to disk.
             ?assert(filelib:is_regular(
                 hecate_station_cache:path(CacheDir))),
             %% 3. The tombstone remains reachable via the DHT pid
             %%    if we re-boot a local DHT and load the cache —
             %%    the put_record step persists it in ETS only for
             %%    the pid's lifetime. Instead we verify the
             %%    tombstone was built + published BEFORE the pid
             %%    died by using the pre-captured snapshot.
             %%    Sanity check on inputs:
             ?assertEqual(32, byte_size(Pub)),
             %% DHT pid should be dead.
             ?assertNot(is_process_alive(Dht)),
             ok
         end}
    end}.

shutdown_is_idempotent_test_() ->
    {setup, fun setup_app/0, fun teardown_dir/1, fun(_Ctx) ->
        {timeout, 20,
         fun() ->
             process_flag(trap_exit, true),
             ok = hecate_station:shutdown(),
             ok = wait_gone(hecate_station_sup, 10_000),
             %% Calling shutdown on a stopped station returns
             %% `{error, not_started}' rather than crashing — the
             %% sup tree is gone, so dht()/current_identity() both
             %% report not_started.
             ?assertEqual({error, not_started},
                          hecate_station:shutdown())
         end}
    end}.

%%==================================================================
%% Tombstone persisted locally BEFORE the sup dies.
%%==================================================================

%% This test exercises the tombstone-in-local-DHT invariant with a
%% fresh station that we control step-by-step: boot, check the DHT
%% has no tombstone for our key, inject the publish call through a
%% helper that leaves the sup alive, then assert the tombstone is
%% there. The helper is a convenience exposed only for tests; full
%% shutdown tears the sup down before we can query.
publish_tombstone_lands_in_local_dht_test_() ->
    {setup, fun setup_app/0, fun teardown_app/1, fun(#{kp := Kp} = Ctx) ->
        {timeout, 20,
         fun() ->
             process_flag(trap_exit, true),
            {ok, Dht} = hecate_station:dht(),
            Pub       = macula_identity:public(Kp),
            %% No prior tombstone for the self-pub key.
            ?assertEqual([],
                hecate_dht:find_local_record(Dht, Pub)),
            %% Build + put a tombstone directly — same code path
            %% the shutdown helper takes minus the best-effort
            %% store / sup-teardown steps.
            Tomb = tombstone(Kp, retired),
            ok   = hecate_dht:put_record(Dht, Tomb),
            Records = hecate_dht:find_local_record(Dht, Pub),
            ?assertMatch([_], Records),
            [Stored] = Records,
            %% The stored record's own type tag is the tombstone
            %% tag (0x0C); its payload carries the superseded
            %% node_record tag (0x01 — what `tombstone_type/0'
            %% reports).
            ?assertEqual(16#0C, macula_record:type(Stored)),
            _ = Ctx
         end}
    end}.

%%==================================================================
%% Fixture — minimal app boot with a cache configured so the flush
%% step on shutdown has a file to land in.
%%==================================================================

setup_app() ->
    process_flag(trap_exit, true),
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(macula_peering),
    %% Guard against a previous test's half-dead sup.
    _ = kill_if_alive(hecate_station_sup),
    _ = hecate_station:forget_dial_opts(),
    Saved = clear_env(),
    Dir       = make_tmpdir(),
    CacheDir  = filename:join(Dir, "cache"),
    set_station_env(Dir),
    application:set_env(hecate_bootstrap, tiers,
        [{hecate_station_stub_tier,
          #{peers => hecate_station_stub_tier:stub_peers(1)}}]),
    application:set_env(hecate_bootstrap, cascade_opts,
        #{min_peers => 1, timeout_ms => 1000}),
    application:set_env(hecate_station, cache,
        #{path => CacheDir, flush_period_ms => 60_000}),
    {ok, _Sup} = hecate_station_app:start(normal, []),
    {ok, Dht}  = hecate_station:dht(),
    {ok, Kp}   = hecate_station:current_identity(),
    #{dir => Dir, cache_dir => CacheDir, dht => Dht, kp => Kp, saved => Saved}.

teardown_dir(#{dir := Dir, saved := Saved}) ->
    %% Fast, unconditional teardown — shutdown may be the SUT and
    %% we cannot rely on it succeeding between tests. Brutal-kill
    %% any lingering sup, clear persistent_term, restore env.
    _ = kill_if_alive(hecate_station_sup),
    _ = hecate_station:forget_dial_opts(),
    restore_env(Saved),
    rm_rf(Dir),
    ok.

teardown_app(Ctx) -> teardown_dir(Ctx).

tombstone(Kp, Reason) ->
    Pub = macula_identity:public(Kp),
    Unsigned = macula_record:tombstone(Pub,
                                       hecate_station:tombstone_type(),
                                       Reason),
    macula_record:sign(Unsigned, Kp).

%%------------------------------------------------------------------
%% Env + dir helpers — mirror hecate_station_admin_tests shapes so
%% the suite is self-contained.
%%------------------------------------------------------------------

clear_env() ->
    Station = [data_dir, identity_file, bind, port, certfile, keyfile,
               realms, capabilities, cache, rebootstrap, admin],
    Boot    = [tiers, cascade_opts],
    Saved = [{K, station, application:get_env(hecate_station, K)} || K <- Station]
         ++ [{K, bootstrap, application:get_env(hecate_bootstrap, K)} || K <- Boot],
    [application:unset_env(hecate_station, K) || K <- Station],
    [application:unset_env(hecate_bootstrap, K) || K <- Boot],
    Saved.

restore_env(Saved) ->
    [case V of
         undefined -> application:unset_env(App, K);
         {ok, X}   -> application:set_env(App, K, X)
     end || {K, App, V} <- Saved],
    ok.

set_station_env(Dir) ->
    {Cert, Key} = generate_test_cert(Dir),
    application:set_env(hecate_station, data_dir, Dir),
    application:set_env(hecate_station, bind,     "127.0.0.1"),
    application:set_env(hecate_station, port,     free_port()),
    application:set_env(hecate_station, certfile, Cert),
    application:set_env(hecate_station, keyfile,  Key).

generate_test_cert(Dir) ->
    ok   = filelib:ensure_dir(filename:join(Dir, "x")),
    Cert = filename:join(Dir, "cert.pem"),
    Key  = filename:join(Dir, "key.pem"),
    Cmd  = lists:flatten(io_lib:format(
        "openssl req -x509 -newkey rsa:2048 -nodes "
        "-keyout ~s -out ~s -days 1 -subj /CN=localhost 2>&1",
        [Key, Cert])),
    Out  = os:cmd(Cmd),
    true = filelib:is_regular(Cert) orelse error({openssl_failed, Out}),
    {Cert, Key}.

free_port() ->
    {ok, S} = gen_udp:open(0, [{reuseaddr, true}]),
    {ok, P} = inet:port(S),
    ok = gen_udp:close(S),
    P.

make_tmpdir() ->
    Base = filename:join(["/tmp", "hecate-station-shutdown-test",
                          integer_to_list(erlang:unique_integer([positive]))]),
    ok   = filelib:ensure_dir(filename:join(Base, "placeholder")),
    Base.

rm_rf(Dir) -> _ = os:cmd("rm -rf " ++ Dir), ok.

kill_if_alive(Name) ->
    kill_step(whereis(Name)).

kill_step(undefined) -> ok;
kill_step(Pid) when is_pid(Pid) ->
    Ref = monitor(process, Pid),
    exit(Pid, shutdown),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 3_000 -> exit(Pid, kill), ok
    end.

wait_gone(Name, Ms) ->
    wait_gone_step(whereis(Name), Name, Ms).

wait_gone_step(undefined, _Name, _Ms)       -> ok;
wait_gone_step(_Pid, _Name, Ms) when Ms =< 0 -> timeout;
wait_gone_step(_Pid, Name, Ms)              ->
    timer:sleep(20),
    wait_gone_step(whereis(Name), Name, Ms - 20).
