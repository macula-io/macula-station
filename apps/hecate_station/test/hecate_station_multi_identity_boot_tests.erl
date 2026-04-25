%% @doc Multi-identity boot test (PLAN_MULTI_IDENTITY_RELAY §Phase 4).
%%
%% Drives `hecate_station_app:start/2' through the multi-identity
%% branch: sets `MACULA_RELAY_IDENTITIES' to three entries on
%% distinct loopback IPs (127.0.0.{1,2,3}) sharing one port + cert,
%% verifies each identity is registered in
%% `hecate_station_identity_registry' and its listener is alive.
-module(hecate_station_multi_identity_boot_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Generator
%%==================================================================

multi_identity_boot_test_() ->
    {timeout, 60,
     {setup,
      fun reset_env/0,
      fun restore_env/1,
      fun(_) ->
         fun() -> three_identities_boot() end
      end}}.

three_identities_boot() ->
    process_flag(trap_exit, true),
    Dir = make_tmpdir(),
    try
        {Cert, Key} = generate_cert(Dir),
        BoxSecretPath = filename:join(Dir, "box-secret"),
        Port = free_port(),
        EnvValue =
            "relay-a.macula.io/A/AA/1.0/2.0/127.0.0.1,"
            "relay-b.macula.io/B/BB/3.0/4.0/127.0.0.2,"
            "relay-c.macula.io/C/CC/5.0/6.0/127.0.0.3",
        os:putenv("MACULA_RELAY_IDENTITIES", EnvValue),
        application:set_env(hecate_station, port, Port),
        application:set_env(hecate_station, certfile, Cert),
        application:set_env(hecate_station, keyfile, Key),
        application:set_env(hecate_station, box_secret_path, BoxSecretPath),

        {ok, Sup} = hecate_station_app:start(normal, []),

        Listed = lists:sort(
            [Hostname || {Hostname, _Pid}
                         <- hecate_station_identity_registry:list()]),
        ?assertEqual([<<"relay-a.macula.io">>,
                      <<"relay-b.macula.io">>,
                      <<"relay-c.macula.io">>],
                     Listed),

        %% Each identity_sup carries a live listener bound to its IP.
        [verify_identity_listener(H) || H <- Listed],

        ok = cleanup_sup(Sup)
    after
        os:unsetenv("MACULA_RELAY_IDENTITIES"),
        application:unset_env(hecate_station, port),
        application:unset_env(hecate_station, certfile),
        application:unset_env(hecate_station, keyfile),
        application:unset_env(hecate_station, box_secret_path),
        rm_rf(Dir)
    end.

verify_identity_listener(Hostname) ->
    {ok, IdSup} = hecate_station_identity_registry:lookup(Hostname),
    Children = supervisor:which_children(IdSup),
    Listeners = [P || {hecate_station_listener, P, _, _} <- Children],
    ?assertMatch([_], Listeners),
    [Pid] = Listeners,
    ?assert(is_process_alive(Pid)).

%%==================================================================
%% Per-identity isolation — distinct keys + distinct DHTs
%%==================================================================

three_identities_have_distinct_keys_test_() ->
    {timeout, 60,
     {setup,
      fun reset_env/0,
      fun restore_env/1,
      fun(_) ->
         fun() -> distinct_keys() end
      end}}.

distinct_keys() ->
    process_flag(trap_exit, true),
    Dir = make_tmpdir(),
    try
        {Cert, Key} = generate_cert(Dir),
        BoxSecretPath = filename:join(Dir, "box-secret"),
        Port = free_port(),
        os:putenv("MACULA_RELAY_IDENTITIES",
                  "x.macula.io/X/XX/1.0/2.0/127.0.0.1,"
                  "y.macula.io/Y/YY/3.0/4.0/127.0.0.2"),
        application:set_env(hecate_station, port, Port),
        application:set_env(hecate_station, certfile, Cert),
        application:set_env(hecate_station, keyfile, Key),
        application:set_env(hecate_station, box_secret_path, BoxSecretPath),

        {ok, Sup} = hecate_station_app:start(normal, []),
        {ok, SupX} = hecate_station_identity_registry:lookup(<<"x.macula.io">>),
        {ok, SupY} = hecate_station_identity_registry:lookup(<<"y.macula.io">>),

        DhtX  = child_pid(SupX, hecate_dht),
        DhtY  = child_pid(SupY, hecate_dht),
        SwimX = child_pid(SupX, hecate_swim),
        SwimY = child_pid(SupY, hecate_swim),

        ?assertNotEqual(DhtX, DhtY),
        ?assertNotEqual(SwimX, SwimY),

        ok = cleanup_sup(Sup)
    after
        os:unsetenv("MACULA_RELAY_IDENTITIES"),
        application:unset_env(hecate_station, port),
        application:unset_env(hecate_station, certfile),
        application:unset_env(hecate_station, keyfile),
        application:unset_env(hecate_station, box_secret_path),
        rm_rf(Dir)
    end.

%%==================================================================
%% Helpers
%%==================================================================

reset_env() ->
    %% Snapshot keys we touch.
    Keys = [port, certfile, keyfile, box_secret_path,
            data_dir, identity_file, bind, capabilities],
    Saved = [{K, application:get_env(hecate_station, K)} || K <- Keys],
    [application:unset_env(hecate_station, K) || K <- Keys],
    EnvVar = os:getenv("MACULA_RELAY_IDENTITIES"),
    os:unsetenv("MACULA_RELAY_IDENTITIES"),
    {Saved, EnvVar}.

restore_env({Saved, EnvVar}) ->
    drain_exit_signals(),
    [restore_one(K, V) || {K, V} <- Saved],
    case EnvVar of
        false -> os:unsetenv("MACULA_RELAY_IDENTITIES");
        Value -> os:putenv("MACULA_RELAY_IDENTITIES", Value)
    end,
    ok.

restore_one(_K, undefined) -> ok;
restore_one(K, {ok, V})    -> application:set_env(hecate_station, K, V).

drain_exit_signals() ->
    receive {'EXIT', _Pid, _Reason} -> drain_exit_signals()
    after 0 -> ok end.

cleanup_sup(Sup) ->
    Ref = erlang:monitor(process, Sup),
    _ = unlink(Sup),
    exit(Sup, shutdown),
    receive {'DOWN', Ref, process, Sup, _} -> ok
    after 5_000 -> exit(Sup, kill), ok end,
    drain_exit_signals().

make_tmpdir() ->
    Dir = filename:join("/tmp",
            "hecate-multi-id-boot-"
            ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

rm_rf(Path) ->
    case filelib:is_dir(Path) of
        true ->
            {ok, Names} = file:list_dir(Path),
            [rm_rf(filename:join(Path, N)) || N <- Names],
            file:del_dir(Path);
        false ->
            file:delete(Path)
    end.

generate_cert(Dir) ->
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

child_pid(Sup, Id) ->
    [Pid] = [P || {ChildId, P, _, _} <- supervisor:which_children(Sup),
                  ChildId =:= Id],
    Pid.
