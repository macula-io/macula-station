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
%% stop_cluster/1 with empty list (pure-function — no spawn)
%%
%% spawn-related tests have moved to macula_station_test_cluster_SUITE
%% (CT). eunit's per-test process model races with peer node teardown;
%% CT's init_per_testcase / end_per_testcase fixtures are the proper
%% home for multi-process integration tests.
%%==================================================================

stop_cluster_empty_list_is_noop_test() ->
    ?assertEqual(ok, macula_station_test_cluster:stop_cluster([])).

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

sample_handle() ->
    macula_station_test_cluster:new_handle(
        test_node@nohost,
        <<0:256>>,
        {{0,0,0,0,0,0,0,1}, 4433},
        "/tmp/test_station_data",
        self()).
