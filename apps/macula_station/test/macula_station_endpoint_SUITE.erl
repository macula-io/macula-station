%% @doc CT suite — Slice 3 end-to-end: a station publishes its own
%% dialable QUIC endpoint as a signed `station_endpoint' record, and
%% another station resolves it from the station's pubkey over the wire.
%%
%% This is what makes a resolved `serving_station' pubkey (Slice 2)
%% actually dialable (Slice 4): given only the pubkey, a consumer gets
%% back host:port. Direction matches macula_station_dht_transport_SUITE:
%% the dialer (A) queries the dialee (B), so B is the station whose
%% endpoint we resolve.
-module(macula_station_endpoint_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([suite/0, all/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([station_endpoint_resolves_cross_station/1]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [station_endpoint_resolves_cross_station].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    PrivDir = ?config(priv_dir, Config),
    [{cluster_opts, #{base_dir => PrivDir}} | Config].
end_per_testcase(_Name, _Config) -> ok.

%% B publishes its station_endpoint on boot (via the announcer). A
%% resolves B's pubkey to that endpoint and gets B's real listen port
%% plus a non-empty advertised host.
station_endpoint_resolves_cross_station(Config) ->
    Opts    = ?config(cluster_opts, Config),
    Handles = macula_station_test_cluster:spawn_cluster(2, Opts),
    try
        [A, B] = Handles,
        ok = macula_station_test_cluster:dial(A, B),
        ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 10_000),

        BPub          = macula_station_test_cluster:pubkey(B),
        {_Ip, BPort}  = macula_station_test_cluster:listen_addr(B),

        %% storage key for B's station_endpoint (keyed by pubkey; port
        %% is irrelevant to the key, so a throwaway port derives it).
        Key = macula_record:storage_key(
                macula_record:station_endpoint(BPub, 1)),

        {value, [Rec]} = macula_station_test_cluster:rpc(
                           A, macula_dht, find_value,
                           [macula_dht, Key, BPub, 5_000]),

        P = macula_record:payload(Rec),
        ?assertEqual(BPort, field(P, <<"quic_port">>)),
        ?assertMatch([_ | _], field(P, <<"host_advertised">>))
    after
        macula_station_test_cluster:stop_cluster(Handles)
    end.

%% Robust to canonical ({text, _}) vs wire-decoded (bare binary) keys.
field(P, Name) ->
    case maps:find({text, Name}, P) of
        {ok, V} -> V;
        error   -> maps:get(Name, P, undefined)
    end.
