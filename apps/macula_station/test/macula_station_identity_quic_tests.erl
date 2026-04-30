%% @doc Multi-identity QUIC cross-talk tests (PLAN_MULTI_IDENTITY_RELAY §Phase 3).
%%
%% Spawns two `macula_station_identity_sup' instances inside one
%% BEAM, each with its own per-identity DHT, SWIM, peer observer
%% and listener bound to a distinct loopback port. Then dials from
%% identity A's keypair to identity B's listener and verifies the
%% peer landed in B's DHT + SWIM via B's observer.
%%
%% This is the Phase 3 acceptance: per-identity isolation holds
%% under real QUIC, no `{local, _}' singletons collide, and each
%% identity's observer sees only its own inbound peers.
-module(macula_station_identity_quic_tests).
-include_lib("eunit/include/eunit.hrl").

-define(SUP, macula_station_identity_sup).

%%==================================================================
%% Generator
%%==================================================================

cross_talk_test_() ->
    {setup,
     fun setup_env/0,
     fun cleanup_env/1,
     fun({Dir, _StartedApps}) ->
        {timeout, 30,
         fun() -> two_identities_cross_dial(Dir) end}
     end}.

setup_env() ->
    Dir = filename:join("/tmp",
            "hecate-multi-identity-quic-"
            ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    %% macula_peering owns the per-connection supervisor pool that
    %% `macula_peering:connect/1' funnels children through. Start it
    %% (and any of its deps) so the dial path resolves.
    {ok, Started} = application:ensure_all_started(macula),
    {Dir, Started}.

cleanup_env({Dir, Started}) ->
    [application:stop(App) || App <- lists:reverse(Started)],
    rm_rf(Dir).

%%==================================================================
%% The test
%%==================================================================

two_identities_cross_dial(Dir) ->
    process_flag(trap_exit, true),
    {Cert, Key} = generate_cert(Dir),
    KpA = macula_identity:generate(),
    KpB = macula_identity:generate(),
    PortA = free_port(),
    PortB = free_port(),

    OptsA = identity_opts(<<"identity-a">>, KpA, PortA, Cert, Key),
    OptsB = identity_opts(<<"identity-b">>, KpB, PortB, Cert, Key),

    {ok, SupA} = ?SUP:start_link(OptsA),
    {ok, SupB} = ?SUP:start_link(OptsB),

    try
        DhtA  = child_pid(SupA, macula_dht),
        DhtB  = child_pid(SupB, macula_dht),
        SwimA = child_pid(SupA, macula_swim),
        SwimB = child_pid(SupB, macula_swim),

        %% Both per-identity DHTs are alive and disjoint.
        ?assertNotEqual(DhtA, DhtB),
        ?assertEqual(0, macula_dht:size(DhtA)),
        ?assertEqual(0, macula_dht:size(DhtB)),

        %% A dials B. The dial is initiated as if it came from a
        %% standalone peer carrying identity A — Phase 3 makes sure
        %% the peering pipeline can be parameterised per-identity
        %% without fixed-name collisions.
        IdA = macula_identity:public(KpA),
        IdB = macula_identity:public(KpB),
        {ok, _Conn} = macula_peering:connect(#{
            role            => client,
            identity        => KpA,
            realms          => [],
            capabilities    => 16#FF,
            controlling_pid => self(),
            target          => #{host       => "127.0.0.1",
                                 port       => PortB,
                                 timeout_ms => 3_000}
        }),

        %% B's observer must record A in B's DHT + SWIM. A's DHT
        %% must remain empty (the dial was outbound from A; A's
        %% observer would only see inbound peers).
        ok = wait_until(fun() ->
            macula_dht:contains(DhtB, IdA) andalso
            lists:any(fun(#{node_id := N}) -> N =:= IdA end,
                      macula_swim:members(SwimB))
        end, 10_000),

        ?assertEqual(0, macula_dht:size(DhtA)),
        ?assertEqual(1, macula_dht:size(DhtB)),
        %% IdB must NOT have leaked into A's DHT.
        ?assertNot(macula_dht:contains(DhtA, IdB)),
        %% A's SWIM must not have learnt anything either.
        ?assertEqual([], macula_swim:members(SwimA)),

        ok
    after
        shutdown(SupA),
        shutdown(SupB)
    end.

%%==================================================================
%% Helpers
%%==================================================================

identity_opts(Key, Kp, Port, Cert, KeyFile) ->
    #{
        identity_key => Key,
        identity     => Kp,
        bind         => "127.0.0.1",
        port         => Port,
        certfile     => Cert,
        keyfile      => KeyFile,
        capabilities => 16#FF,
        realms       => []
    }.

child_pid(Sup, ChildId) ->
    [Pid] = [P || {Id, P, _, _} <- supervisor:which_children(Sup),
                  Id =:= ChildId],
    Pid.

shutdown(Sup) ->
    Ref = erlang:monitor(process, Sup),
    _ = unlink(Sup),
    exit(Sup, shutdown),
    receive {'DOWN', Ref, process, Sup, _} -> ok
    after 5_000 -> exit(Sup, kill), ok end,
    flush_exit(Sup).

flush_exit(Pid) ->
    receive {'EXIT', Pid, _} -> ok after 0 -> ok end.

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

wait_until(_F, Budget) when Budget =< 0 ->
    erlang:error(wait_until_timeout);
wait_until(F, Budget) ->
    case F() of
        true  -> ok;
        false -> timer:sleep(100), wait_until(F, Budget - 100)
    end.

rm_rf(Path) ->
    case filelib:is_dir(Path) of
        true ->
            {ok, Names} = file:list_dir(Path),
            [rm_rf(filename:join(Path, N)) || N <- Names],
            file:del_dir(Path);
        false ->
            file:delete(Path)
    end.
