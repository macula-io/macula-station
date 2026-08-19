%% @doc CT suite — Slice 4 end-to-end: an SDK pool dials a station that
%% is NOT in its seed set and issues a CALL there, in one hop.
%%
%% This is the direct-dial data path: a consumer that resolved a
%% serving_station (Slice 2) to its endpoint (Slice 3) reaches it via
%% `macula:call_station/6' without ever seeding to it and without a mesh
%% relay. Proves the happy path that the macula-side unit test (which
%% only exercises the unreachable-dial degradation) cannot.
-module(macula_station_call_station_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([suite/0, all/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([call_station_dials_outside_seed_set/1]).

-define(DHT_REALM, <<0:256>>).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [call_station_dials_outside_seed_set].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(macula),
    Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    PrivDir = ?config(priv_dir, Config),
    [{cluster_opts, #{base_dir => PrivDir}} | Config].
end_per_testcase(_Name, _Config) -> ok.

%% A pool seeded to NOTHING dials station B by URL and calls a procedure
%% B serves (`_dht.find_record'); a second call reuses the link.
call_station_dials_outside_seed_set(Config) ->
    Opts       = ?config(cluster_opts, Config),
    [B]        = macula_station_test_cluster:spawn_cluster(1, Opts),
    %% Test stations share a self-signed cert whose SPKI is not the
    %% station pubkey, so the pool dials with `verify => none' (loopback,
    %% same as station<->station in the harness). Production pins the
    %% station identity via `expected_node_id'.
    {ok, Pool} = macula:connect([], #{verify => none}),
    try
        Url = station_url(macula_station_test_cluster:listen_addr(B)),

        R1 = macula:call_station(Pool, Url, ?DHT_REALM,
                                 <<"_dht.find_record">>,
                                 #{key => <<0:256>>}, 5_000),
        ?assertMatch({ok, _}, R1),

        %% second call to the same station reuses the link
        R2 = macula:call_station(Pool, Url, ?DHT_REALM,
                                 <<"_dht.find_record">>,
                                 #{key => <<0:256>>}, 5_000),
        ?assertMatch({ok, _}, R2)
    after
        catch macula:close(Pool),
        macula_station_test_cluster:stop_cluster([B])
    end.

%% `quic://[<ip6>]:<port>' — the seed URL form macula:connect accepts.
station_url({Ip, Port}) ->
    Host = list_to_binary(inet:ntoa(Ip)),
    <<"quic://[", Host/binary, "]:", (integer_to_binary(Port))/binary>>.
