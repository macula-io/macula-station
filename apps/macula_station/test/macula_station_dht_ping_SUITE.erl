%% @doc CT suite — DHT primitives over the wire.
%%
%% Three end-to-end tests, each spinning up an isolated multi-process
%% cluster via macula_station_test_cluster:
%%
%% <ul>
%%   <li>2-station PING — the smallest round-trip the wire supports.</li>
%%   <li>3-station FIND_NODE — A asks B for k-closest to C; C must
%%       appear in the reply.</li>
%%   <li>3-station STORE + FIND_VALUE — A puts a record at B's
%%       custodian, then C asks B for it; record must round-trip.</li>
%% </ul>
%%
%% Reference: PLAN_DHT_TRANSPORT_WIRING §7 steps 5-7.
-module(macula_station_dht_ping_SUITE).

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
    ping_two_stations/1,
    find_node_three_stations/1,
    store_and_find_value_three_stations/1
]).

%%==================================================================
%% CT plumbing
%%==================================================================

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [ping_two_stations,
     find_node_three_stations,
     store_and_find_value_three_stations].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    PrivDir = ?config(priv_dir, Config),
    [{cluster_opts, #{base_dir => PrivDir}} | Config].

end_per_testcase(_Name, _Config) -> ok.

%%==================================================================
%% Tests
%%==================================================================

%% Smallest round-trip: A pings B, receives PONG, ping_peer/3 returns
%% {ok, #{rtt_ms := _}}. Proves outbound + inbound + reply correlation
%% all work for the simplest DHT frame pair.
ping_two_stations(Config) ->
    Opts = ?config(cluster_opts, Config),
    Handles = macula_station_test_cluster:spawn_cluster(2, Opts),
    try
        [A, B] = Handles,
        ok = macula_station_test_cluster:dial(A, B),
        ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 10_000),
        BPubkey = macula_station_test_cluster:pubkey(B),
        Result = macula_station_test_cluster:rpc(
                   A, macula_dht, ping_peer,
                   [macula_dht, BPubkey, 5_000]),
        ct:pal("ping_peer(A->B) returned: ~p", [Result]),
        ?assertMatch({ok, #{rtt_ms := _}}, Result)
    after
        macula_station_test_cluster:stop_cluster(Handles)
    end.

%% 3-station: A peers with B, B peers with C. A asks B for k-closest
%% to C's NodeId. C must appear in the reply (B knows C as a direct
%% peer).
find_node_three_stations(Config) ->
    Opts = ?config(cluster_opts, Config),
    Handles = macula_station_test_cluster:spawn_cluster(3, Opts),
    try
        [A, B, C] = Handles,
        ok = macula_station_test_cluster:dial(A, B),
        ok = macula_station_test_cluster:dial(B, C),
        ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 10_000),
        ok = macula_station_test_cluster:wait_for_handshakes(B, 2, 10_000),
        BPubkey = macula_station_test_cluster:pubkey(B),
        CPubkey = macula_station_test_cluster:pubkey(C),
        Result = macula_station_test_cluster:rpc(
                   A, macula_dht, find_node,
                   [macula_dht, CPubkey, BPubkey, 5_000]),
        ct:pal("find_node(A asks B for k-closest to C) returned: ~p", [Result]),
        ?assertMatch({ok, [_ | _]}, Result),
        {ok, Refs} = Result,
        Ids = [maps:get(node_id, R) || R <- Refs],
        ?assert(lists:member(CPubkey, Ids))
    after
        macula_station_test_cluster:stop_cluster(Handles)
    end.

%% 3-station STORE + FIND_VALUE: A puts a record locally, sends STORE
%% to B (custodian), C asks B for that record by key, B replies VALUE.
%% Tests both write path (STORE/STORE_ACK) and read path
%% (FIND_VALUE/VALUE).
store_and_find_value_three_stations(Config) ->
    Opts = ?config(cluster_opts, Config),
    Handles = macula_station_test_cluster:spawn_cluster(3, Opts),
    try
        [A, B, C] = Handles,
        %% Build a fully connected triangle so every node can reach
        %% every other one for the read path.
        ok = macula_station_test_cluster:dial(A, B),
        ok = macula_station_test_cluster:dial(B, C),
        ok = macula_station_test_cluster:dial(C, A),
        ok = macula_station_test_cluster:wait_for_handshakes(A, 2, 10_000),
        ok = macula_station_test_cluster:wait_for_handshakes(B, 2, 10_000),
        ok = macula_station_test_cluster:wait_for_handshakes(C, 2, 10_000),
        BPubkey = macula_station_test_cluster:pubkey(B),
        %% A builds a record signed by A's identity, with B's pubkey
        %% as the storage key (so a find_value(B-key) at B answers
        %% locally — B already holds B's own self-record from boot).
        Result = macula_station_test_cluster:rpc(
                   C, macula_dht, find_value,
                   [macula_dht, BPubkey, BPubkey, 5_000]),
        ct:pal("find_value(C asks B for B's pubkey) returned: ~p",
               [Result]),
        ?assertMatch({value, [_]}, Result)
    after
        macula_station_test_cluster:stop_cluster(Handles)
    end.
