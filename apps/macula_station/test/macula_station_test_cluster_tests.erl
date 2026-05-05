%% @doc eunit for macula_station_test_cluster — Phase 1 step 1.
%%
%% Covers helpers and accessors. spawn_cluster/2 + dial/2 +
%% wait_for_handshakes/3 are exercised in their own test files
%% (added in steps 2-7 of PLAN_PHASE_1_MULTI_PROCESS_CT_HARNESS.md).
-module(macula_station_test_cluster_tests).

-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% allocate_data_dir/1
%%==================================================================

allocate_data_dir_creates_unique_directory_test() ->
    Base = base_dir(),
    Dir1 = macula_station_test_cluster:allocate_data_dir(Base),
    Dir2 = macula_station_test_cluster:allocate_data_dir(Base),
    ?assertNotEqual(Dir1, Dir2),
    ?assert(filelib:is_dir(Dir1)),
    ?assert(filelib:is_dir(Dir2)),
    cleanup([Dir1, Dir2]).

allocate_data_dir_path_is_under_base_test() ->
    Base = base_dir(),
    Dir = macula_station_test_cluster:allocate_data_dir(Base),
    BaseAbs = filename:absname(Base),
    DirAbs = filename:absname(Dir),
    ?assertEqual(BaseAbs, filename:dirname(DirAbs)),
    cleanup([Dir]).

allocate_data_dir_path_carries_recognisable_prefix_test() ->
    Dir = macula_station_test_cluster:allocate_data_dir(base_dir()),
    Basename = filename:basename(Dir),
    ?assertMatch("macula_test_station_" ++ _, Basename),
    cleanup([Dir]).

%%==================================================================
%% new_handle/5 + accessors
%%==================================================================

new_handle_constructs_well_formed_handle_test() ->
    Handle = sample_handle(),
    ?assert(is_map(Handle)),
    ?assertEqual(test_node@nohost,
                 macula_station_test_cluster:peer_node(Handle)),
    ?assertEqual(<<0:256>>,
                 macula_station_test_cluster:pubkey(Handle)),
    ?assertEqual({{0,0,0,0,0,0,0,1}, 4433},
                 macula_station_test_cluster:listen_addr(Handle)),
    ?assertEqual("/tmp/test_station_data",
                 macula_station_test_cluster:data_dir(Handle)),
    ?assertEqual(self(),
                 macula_station_test_cluster:peer_pid(Handle)).

new_handle_rejects_short_pubkey_test() ->
    ?assertError(function_clause,
                 macula_station_test_cluster:new_handle(
                     test_node@nohost,
                     <<0:128>>,                        %% 16 bytes — wrong
                     {{0,0,0,0,0,0,0,1}, 4433},
                     "/tmp/x",
                     self())).

new_handle_rejects_non_atom_node_test() ->
    ?assertError(function_clause,
                 macula_station_test_cluster:new_handle(
                     "not_an_atom",
                     <<0:256>>,
                     {{0,0,0,0,0,0,0,1}, 4433},
                     "/tmp/x",
                     self())).

new_handle_rejects_non_pid_test() ->
    ?assertError(function_clause,
                 macula_station_test_cluster:new_handle(
                     test_node@nohost,
                     <<0:256>>,
                     {{0,0,0,0,0,0,0,1}, 4433},
                     "/tmp/x",
                     not_a_pid)).

%%==================================================================
%% spawn_cluster/2 (N=1) + stop_cluster/1
%%==================================================================

spawn_one_station_yields_well_formed_handle_test_() ->
    {timeout, 60, fun() ->
        process_flag(trap_exit, true),
        Handles = macula_station_test_cluster:spawn_cluster(1, #{}),
        try
            ?assertMatch([_], Handles),
            [H] = Handles,
            %% pubkey is a 32-byte Ed25519 public key
            Pub = macula_station_test_cluster:pubkey(H),
            ?assert(is_binary(Pub)),
            ?assertEqual(32, byte_size(Pub)),
            %% peer node atom is set; controller pid is alive
            Node = macula_station_test_cluster:peer_node(H),
            ?assert(is_atom(Node)),
            %% listen_addr is IPv6 loopback + a real port
            {Host, Port} = macula_station_test_cluster:listen_addr(H),
            ?assertEqual({0,0,0,0,0,0,0,1}, Host),
            ?assert(Port > 1024),
            %% peer pid is alive
            ?assert(is_process_alive(macula_station_test_cluster:peer_pid(H))),
            %% data_dir exists
            ?assert(filelib:is_dir(macula_station_test_cluster:data_dir(H)))
        after
            macula_station_test_cluster:stop_cluster(Handles)
        end
    end}.

stop_cluster_kills_peer_and_removes_data_dir_test_() ->
    {timeout, 60, fun() ->
        process_flag(trap_exit, true),
        [H] = macula_station_test_cluster:spawn_cluster(1, #{}),
        PeerPid = macula_station_test_cluster:peer_pid(H),
        DataDir = macula_station_test_cluster:data_dir(H),
        ?assert(is_process_alive(PeerPid)),
        ?assert(filelib:is_dir(DataDir)),
        ok = macula_station_test_cluster:stop_cluster([H]),
        %% Wait for the controller pid to actually exit.
        wait_until(fun() -> not is_process_alive(PeerPid) end, 50, 100),
        ?assertNot(is_process_alive(PeerPid)),
        %% Data dir is removed.
        ?assertNot(filelib:is_dir(DataDir))
    end}.

stop_cluster_is_idempotent_on_dead_handle_test_() ->
    {timeout, 60, fun() ->
        process_flag(trap_exit, true),
        [H] = macula_station_test_cluster:spawn_cluster(1, #{}),
        ok  = macula_station_test_cluster:stop_cluster([H]),
        %% Second call must not crash.
        ok  = macula_station_test_cluster:stop_cluster([H])
    end}.

stop_cluster_empty_list_is_noop_test() ->
    ?assertEqual(ok, macula_station_test_cluster:stop_cluster([])).

%%==================================================================
%% spawn_cluster/2 (N>1) — multiple isolated stations
%%==================================================================

spawn_three_stations_yields_unique_handles_test_() ->
    %% 240s — sequential boot of 3 stations on a busy CI box can
    %% take 30-60s each (cascade + listener init + NIF load).
    {timeout, 240, fun() ->
        process_flag(trap_exit, true),
        Handles = macula_station_test_cluster:spawn_cluster(3, #{}),
        try
            ?assertEqual(3, length(Handles)),
            Pubkeys = [macula_station_test_cluster:pubkey(H) || H <- Handles],
            Nodes   = [macula_station_test_cluster:peer_node(H) || H <- Handles],
            Ports   = [element(2, macula_station_test_cluster:listen_addr(H))
                      || H <- Handles],
            Dirs    = [macula_station_test_cluster:data_dir(H) || H <- Handles],
            %% Every station has unique resources.
            ?assertEqual(3, length(lists:usort(Pubkeys))),
            ?assertEqual(3, length(lists:usort(Nodes))),
            ?assertEqual(3, length(lists:usort(Ports))),
            ?assertEqual(3, length(lists:usort(Dirs))),
            %% All stations bind to ::1.
            [?assertEqual({0,0,0,0,0,0,0,1},
                          element(1, macula_station_test_cluster:listen_addr(H)))
             || H <- Handles]
        after
            macula_station_test_cluster:stop_cluster(Handles)
        end
    end}.

%%==================================================================
%% Helpers
%%==================================================================

base_dir() ->
    %% eunit doesn't supply priv_dir; use system temp.
    filename:join([os_temp(), "macula_station_test_cluster_eunit"]).

os_temp() ->
    case os:getenv("TMPDIR") of
        false -> "/tmp";
        Tmp   -> Tmp
    end.

cleanup(Dirs) ->
    [file:del_dir_r(D) || D <- Dirs],
    ok.

wait_until(_Pred, 0, _IntervalMs) -> timeout;
wait_until(Pred, N, IntervalMs) when N > 0 ->
    case Pred() of
        true  -> ok;
        false -> timer:sleep(IntervalMs), wait_until(Pred, N - 1, IntervalMs)
    end.

sample_handle() ->
    macula_station_test_cluster:new_handle(
        test_node@nohost,
        <<0:256>>,
        {{0,0,0,0,0,0,0,1}, 4433},
        "/tmp/test_station_data",
        self()).
