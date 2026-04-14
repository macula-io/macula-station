%% @doc Phase 1 walking skeleton — the end-to-end acceptance suite.
%%
%% Per `plans/PLAN_MACULA_V2_PART7_IMPLEMENTATION §6.3':
%%
%% > End-to-end test passes: two stations, signed `node_record` exchange,
%% > tombstone on stop.
%%
%% Each test case starts one or two stations on loopback with self-signed
%% certs, exercises the handshake / connect_to / stop flows, and asserts
%% the observable outcomes (peer list, tombstone log, identity match).
-module(phase1_walking_skeleton_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([
    single_station_starts_and_stops/1,
    two_stations_handshake/1,
    tombstone_published_on_stop/1,
    signed_node_record_roundtrip/1
]).

all() ->
    [
        single_station_starts_and_stops,
        two_stations_handshake,
        tombstone_published_on_stop,
        signed_node_record_roundtrip
    ].

%%------------------------------------------------------------------
%% Suite-level setup
%%------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(macula_peering),
    PrivDir = ?config(priv_dir, Config),
    {Cert, Key} = phase1_helper:generate_test_cert(PrivDir),
    [{certfile, Cert}, {keyfile, Key} | Config].

end_per_suite(_Config) ->
    ok = application:stop(macula_peering),
    ok.

init_per_testcase(_Name, Config) ->
    Config.

end_per_testcase(_Name, _Config) ->
    ok.

%%------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------

single_station_starts_and_stops(Config) ->
    {ok, S} = start_station(<<"alice">>, Config),
    Kp = hecate_station:identity(S),
    ?assert(is_map(Kp)),
    ?assertEqual(32, byte_size(macula_identity:public(Kp))),
    {ok, _Tombs} = hecate_station:stop(S),
    %% Server is gone — calling identity now fails.
    ?assertExit({noproc, _}, hecate_station:identity(S)).

two_stations_handshake(Config) ->
    {ok, A} = start_station(<<"alice">>, Config),
    {ok, B} = start_station(<<"bob">>, Config),
    {_BindA, PortA} = hecate_station:listen_addr(A),
    {ok, _PeerPid} = hecate_station:connect_to(B, #{
        host => "127.0.0.1", port => PortA, timeout_ms => 3_000
    }),
    %% Wait for both sides to register their peer.
    ok = phase1_helper:wait_for(
        fun() -> length(hecate_station:peers(A)) >= 1 andalso
                 length(hecate_station:peers(B)) >= 1
        end, 5_000),
    APeers = hecate_station:peers(A),
    BPeers = hecate_station:peers(B),
    ?assertMatch([{_, _}], APeers),
    ?assertMatch([{_, _}], BPeers),
    %% Each side learned the other's NodeId.
    APub = macula_identity:public(hecate_station:identity(A)),
    BPub = macula_identity:public(hecate_station:identity(B)),
    [{_APid, AEntry}] = APeers,
    [{_BPid, BEntry}] = BPeers,
    ?assertEqual(BPub, maps:get(peer_node_id, AEntry, undefined)),
    ?assertEqual(APub, maps:get(peer_node_id, BEntry, undefined)),
    {ok, _} = hecate_station:stop(A),
    {ok, _} = hecate_station:stop(B).

tombstone_published_on_stop(Config) ->
    {ok, S} = start_station(<<"alice">>, Config),
    Kp = hecate_station:identity(S),
    Pub = macula_identity:public(Kp),
    %% Before stop: no tombstones.
    ?assertEqual([], hecate_station:tombstones(S)),
    %% stop/2 builds + appends the tombstone and returns the final log.
    {ok, Tombs} = hecate_station:stop(S, retired),
    ?assertMatch([_], Tombs),
    [Tomb] = Tombs,
    ?assertEqual(16#0C, macula_record:type(Tomb)),
    ?assertEqual(Pub, macula_record:key(Tomb)),
    ?assertMatch({ok, _}, macula_record:verify(Tomb)).

signed_node_record_roundtrip(Config) ->
    {ok, S} = start_station(<<"alice">>, Config),
    Kp = hecate_station:identity(S),
    Pub = macula_identity:public(Kp),
    Realms = [crypto:strong_rand_bytes(32)],
    Unsigned = macula_record:node_record(Pub, Realms, 16#FF),
    Signed = macula_record:sign(Unsigned, Kp),
    Wire = macula_record:encode(Signed),
    {ok, Decoded} = macula_record:decode(Wire),
    ?assertMatch({ok, _}, macula_record:verify(Decoded)),
    ?assertEqual(Pub, macula_record:key(Decoded)),
    {ok, _} = hecate_station:stop(S).

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

start_station(_Label, Config) ->
    Cert = ?config(certfile, Config),
    Key  = ?config(keyfile, Config),
    Port = phase1_helper:free_port(),
    Identity = macula_identity:generate(),
    hecate_station:start_link(#{
        bind         => "127.0.0.1",
        port         => Port,
        certfile     => Cert,
        keyfile      => Key,
        identity     => Identity,
        realms       => [],
        capabilities => 16#FF
    }).
