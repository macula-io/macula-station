%% @doc CT suite — multi-process integration tests for the test cluster
%% harness itself. Lives alongside the harness module; covers the parts
%% that spawn real BEAM peers (eunit's per-test process model races
%% with peer node teardown, see PLAN_PHASE_1_MULTI_PROCESS_CT_HARNESS
%% step 5 commit message for the diagnosis).
%%
%% Pure-function tests (allocate_data_dir, new_handle, accessors) stay
%% in the eunit suite `macula_station_test_cluster_tests'.
-module(macula_station_test_cluster_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    spawn_one_station_yields_well_formed_handle/1,
    stop_cluster_kills_peer_and_removes_data_dir/1,
    stop_cluster_is_idempotent_on_dead_handle/1,
    spawn_three_stations_yields_unique_handles/1,
    two_stations_dial_and_handshake/1
]).

%%==================================================================
%% CT plumbing
%%==================================================================

suite() ->
    %% 5 minutes per testcase. Per-station boot can take 30-60s on a
    %% busy box (cascade + listener + NIF load); 3-station test pays
    %% that 3x sequentially.
    [{timetrap, {minutes, 5}}].

all() ->
    [
        spawn_one_station_yields_well_formed_handle,
        stop_cluster_kills_peer_and_removes_data_dir,
        stop_cluster_is_idempotent_on_dead_handle,
        two_stations_dial_and_handshake,
        spawn_three_stations_yields_unique_handles
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Name, Config) ->
    %% Each testcase manages its own cluster lifecycle in the body
    %% (these tests verify spawn_cluster/stop_cluster behaviour, so
    %% fixture-mediated lifecycle would mask the thing under test).
    %% Provide a per-testcase base_dir under priv_dir so leaks are
    %% contained and CT's automatic cleanup catches them.
    PrivDir = ?config(priv_dir, Config),
    [{cluster_opts, #{base_dir => PrivDir}} | Config].

end_per_testcase(_Name, _Config) ->
    ok.

%%==================================================================
%% Test cases
%%==================================================================

spawn_one_station_yields_well_formed_handle(Config) ->
    Opts = ?config(cluster_opts, Config),
    Handles = macula_station_test_cluster:spawn_cluster(1, Opts),
    try
        ?assertMatch([_], Handles),
        [H] = Handles,
        Pub = macula_station_test_cluster:pubkey(H),
        ?assert(is_binary(Pub)),
        ?assertEqual(32, byte_size(Pub)),
        Node = macula_station_test_cluster:peer_node(H),
        ?assert(is_atom(Node)),
        {Host, Port} = macula_station_test_cluster:listen_addr(H),
        ?assertEqual({0,0,0,0,0,0,0,1}, Host),
        ?assert(Port > 1024),
        ?assert(is_process_alive(macula_station_test_cluster:peer_pid(H))),
        ?assert(filelib:is_dir(macula_station_test_cluster:data_dir(H)))
    after
        macula_station_test_cluster:stop_cluster(Handles)
    end.

stop_cluster_kills_peer_and_removes_data_dir(Config) ->
    Opts = ?config(cluster_opts, Config),
    [H] = macula_station_test_cluster:spawn_cluster(1, Opts),
    PeerPid = macula_station_test_cluster:peer_pid(H),
    DataDir = macula_station_test_cluster:data_dir(H),
    ?assert(is_process_alive(PeerPid)),
    ?assert(filelib:is_dir(DataDir)),
    ok = macula_station_test_cluster:stop_cluster([H]),
    wait_until(fun() -> not is_process_alive(PeerPid) end, 50, 100),
    ?assertNot(is_process_alive(PeerPid)),
    ?assertNot(filelib:is_dir(DataDir)).

stop_cluster_is_idempotent_on_dead_handle(Config) ->
    Opts = ?config(cluster_opts, Config),
    [H] = macula_station_test_cluster:spawn_cluster(1, Opts),
    ok  = macula_station_test_cluster:stop_cluster([H]),
    %% Second call must not crash.
    ok  = macula_station_test_cluster:stop_cluster([H]).

two_stations_dial_and_handshake(Config) ->
    Opts = ?config(cluster_opts, Config),
    Handles = macula_station_test_cluster:spawn_cluster(2, Opts),
    try
        ?assertEqual(2, length(Handles)),
        [A, B] = Handles,
        ok = macula_station_test_cluster:dial(A, B),
        ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 10_000),
        ok = macula_station_test_cluster:wait_for_handshakes(B, 1, 10_000)
    after
        macula_station_test_cluster:stop_cluster(Handles)
    end.

spawn_three_stations_yields_unique_handles(Config) ->
    Opts = ?config(cluster_opts, Config),
    Handles = macula_station_test_cluster:spawn_cluster(3, Opts),
    try
        ?assertEqual(3, length(Handles)),
        Pubkeys = [macula_station_test_cluster:pubkey(H)      || H <- Handles],
        Nodes   = [macula_station_test_cluster:peer_node(H)   || H <- Handles],
        Ports   = [element(2, macula_station_test_cluster:listen_addr(H))
                  || H <- Handles],
        Dirs    = [macula_station_test_cluster:data_dir(H)    || H <- Handles],
        ?assertEqual(3, length(lists:usort(Pubkeys))),
        ?assertEqual(3, length(lists:usort(Nodes))),
        ?assertEqual(3, length(lists:usort(Ports))),
        ?assertEqual(3, length(lists:usort(Dirs))),
        [?assertEqual({0,0,0,0,0,0,0,1},
                      element(1, macula_station_test_cluster:listen_addr(H)))
         || H <- Handles]
    after
        macula_station_test_cluster:stop_cluster(Handles)
    end.

%%==================================================================
%% Helpers
%%==================================================================

wait_until(_Pred, 0, _IntervalMs) -> timeout;
wait_until(Pred, N, IntervalMs) when N > 0 ->
    case Pred() of
        true  -> ok;
        false -> timer:sleep(IntervalMs), wait_until(Pred, N - 1, IntervalMs)
    end.
