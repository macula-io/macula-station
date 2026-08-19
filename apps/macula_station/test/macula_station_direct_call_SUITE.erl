%% @doc CT suite — Slice 5 end-to-end: the full direct-dial data plane
%% composed over a real station. Resolve a capability's provider from its
%% `procedure_advertisement', resolve that serving station's
%% `station_endpoint' to a dialable URL, then `call_station' the raw
%% capability there. This is the exact sequence `hecate_om' composes in
%% `call_capability/5' (which macula-station can't call directly — no
%% hecate-om dep — so the suite drives macula, and hecate-om's decision
%% logic is unit-tested there).
-module(macula_station_direct_call_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([suite/0, all/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([resolve_dial_call_composes/1]).

-define(DHT_REALM, <<0:256>>).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [resolve_dial_call_composes].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(macula),
    Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    PrivDir = ?config(priv_dir, Config),
    [{cluster_opts, #{base_dir => PrivDir}} | Config].
end_per_testcase(_Name, _Config) -> ok.

resolve_dial_call_composes(Config) ->
    Opts       = ?config(cluster_opts, Config),
    [B]        = macula_station_test_cluster:spawn_cluster(1, Opts),
    BPub       = macula_station_test_cluster:pubkey(B),
    {_, BPort} = macula_station_test_cluster:listen_addr(B),
    SeedUrl    = station_url(macula_station_test_cluster:listen_addr(B)),
    {ok, Pool} = macula:connect([SeedUrl], #{verify => none}),
    try
        %% A provider advertises capability `_dht.find_record' (which B
        %% actually serves) with B as its serving station.
        Cap = <<"_dht.find_record">>,
        Uri = procedure_uri(?DHT_REALM, Cap),
        Kp  = macula_identity:generate(),
        Ad  = macula_record:sign(
                macula_record:procedure_advertisement(
                  macula_identity:public(Kp), Uri, BPub), Kp),
        ok  = macula_station_test_cluster:rpc(
                B, macula_dht, put_record, [macula_dht, Ad]),
        timer:sleep(300),  %% let B's own station_endpoint publish settle

        %% 1. RESOLVE providers of the capability
        {ok, Recs} = macula:find_records(
                       Pool, macula_record:procedure_key(Uri)),
        [#{serving_station := SS} | _] =
            [macula_record:read_procedure_advertisement(R) || R <- Recs],
        ?assertEqual(BPub, SS),

        %% 2. RESOLVE the serving station's endpoint -> dialable URL
        {ok, EpRec} = macula:find_record(
                        Pool, macula_record:station_endpoint_key(SS)),
        #{quic_port := EPort, host_advertised := [EHost | _]} =
            macula_record:read_station_endpoint(EpRec),
        ?assertEqual(BPort, EPort),
        DialUrl = build_url(EHost, EPort),

        %% 3. DIAL the resolved station and CALL the raw capability there
        R = macula:call_station(Pool, DialUrl, ?DHT_REALM, Cap,
                                #{key => <<0:256>>}, 5_000),
        ?assertMatch({ok, _}, R)
    after
        catch macula:close(Pool),
        macula_station_test_cluster:stop_cluster([B])
    end.

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

procedure_uri(Realm, Name) ->
    <<(binary:encode_hex(Realm))/binary, "/", Name/binary>>.

station_url({Ip, Port}) ->
    build_url(list_to_binary(inet:ntoa(Ip)), Port).

build_url(Host, Port) ->
    <<"quic://[", Host/binary, "]:", (integer_to_binary(Port))/binary>>.
