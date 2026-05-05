%% @doc Phase 2 — SWIM-Lifeguard liveness acceptance suite.
%%
%% Per `plans/PLAN_MACULA_V2_PART7_IMPLEMENTATION §7.3':
%%
%% > 3-station test: kill one, peers detect within 10 s (2 s SWIM period × 6).
%%
%% Phase 2 first pass uses direct-only probes (no indirect ping yet).
%% Lifeguard extensions land in a follow-up session.
%%
%% Test configuration tightens the SWIM timings to keep CT runs short
%% (period 500 ms, ping_timeout 200 ms, suspect_timeout 2 s ⇒ ~3 s
%% expected detection).
-module(phase2_swim_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([
    swim_keeps_peers_alive_under_traffic/1,
    swim_detects_paused_peer/1
]).

all() ->
    [
        swim_keeps_peers_alive_under_traffic,
        swim_detects_paused_peer
    ].

%%------------------------------------------------------------------
%% Suite-level setup
%%------------------------------------------------------------------

%% Suite is legacy V2 phase-buildout testbed for SWIM convergence.
%% Skipped in CI until repaired or migrated to the new
%% macula_station_test_cluster harness. See
%% PLAN_PHASE_1_MULTI_PROCESS_CT_HARNESS.md.
init_per_suite(_Config) ->
    {skip, "legacy phase2 SWIM suite; awaiting harness migration"}.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Name, Config) ->
    Config.

end_per_testcase(_Name, _Config) ->
    %% Tests stop their own stations; nothing to do here.
    ok.

%%------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------

swim_keeps_peers_alive_under_traffic(Config) ->
    {A, B, C} = start_three_station_mesh(Config),
    %% Let several SWIM rounds run; all peers should remain alive.
    timer:sleep(2_500),
    assert_state_for_all(A, [B, C], alive),
    assert_state_for_all(B, [A, C], alive),
    assert_state_for_all(C, [A, B], alive),
    cleanup([A, B, C]).

swim_detects_paused_peer(Config) ->
    {A, B, C} = start_three_station_mesh(Config),
    %% Snapshot C's NodeId before suspending — sys:suspend blocks all calls.
    CPub = macula_identity:public(macula_station:identity(C)),
    %% Wait until the mesh is fully marked alive.
    ok = phase1_helper:wait_for(
        fun() ->
            length(alive_peers_of(A)) >= 2 andalso
            length(alive_peers_of(B)) >= 2
        end, 5_000),
    %% Suspend C — its gen_server stops processing, so it can no longer
    %% reply to SWIM pings (peering frames queue but ACKs are never built).
    ok = sys:suspend(C),
    %% Detection should land within ping_timeout_ms + suspect_timeout_ms
    %% plus a couple of period_ms slots for the round to actually target C.
    ok = phase1_helper:wait_for(
        fun() ->
            is_confirmed(A, CPub) andalso is_confirmed(B, CPub)
        end, 8_000),
    %% Tear down — unlink before brutal-killing C so the kill signal does
    %% not propagate to the test process. A and B then stop cleanly.
    true = unlink(C),
    exit(C, kill),
    cleanup([A, B]).

%%------------------------------------------------------------------
%% Mesh setup
%%------------------------------------------------------------------

start_three_station_mesh(Config) ->
    {ok, A} = start_station(Config),
    {ok, B} = start_station(Config),
    {ok, C} = start_station(Config),
    %% Mesh: B→A, C→A, C→B  (each pair has at least one direction).
    {_, AP} = macula_station:listen_addr(A),
    {_, BP} = macula_station:listen_addr(B),
    {ok, _} = macula_station:connect_to(B, target(AP)),
    {ok, _} = macula_station:connect_to(C, target(AP)),
    {ok, _} = macula_station:connect_to(C, target(BP)),
    %% Wait until each station has 2 peer entries (handshake complete).
    ok = phase1_helper:wait_for(
        fun() ->
            length(macula_station:peers(A)) >= 2 andalso
            length(macula_station:peers(B)) >= 2 andalso
            length(macula_station:peers(C)) >= 2
        end, 5_000),
    {A, B, C}.

start_station(Config) ->
    Cert = ?config(certfile, Config),
    Key  = ?config(keyfile, Config),
    Port = phase1_helper:free_port(),
    Identity = macula_identity:generate(),
    macula_station:start_link(#{
        bind         => "127.0.0.1",
        port         => Port,
        certfile     => Cert,
        keyfile      => Key,
        identity     => Identity,
        realms       => [],
        capabilities => 16#FF,
        period_ms          => 500,
        ping_timeout_ms    => 200,
        suspect_timeout_ms => 2_000
    }).

target(Port) ->
    #{host => "127.0.0.1", port => Port, timeout_ms => 3_000}.

cleanup(Stations) ->
    [catch macula_station:stop(S) || S <- Stations, is_pid(S), is_process_alive(S)],
    ok.

%%------------------------------------------------------------------
%% Membership inspection
%%------------------------------------------------------------------

alive_peers_of(Station) ->
    [M || #{state := alive} = M <- macula_station:swim_members(Station)].

is_confirmed(Station, NodeId) ->
    member_state(Station, NodeId) =:= confirmed_failed.

member_state(Station, NodeId) ->
    case [M || #{node_id := N} = M <- macula_station:swim_members(Station),
               N =:= NodeId] of
        [#{state := S}] -> S;
        []              -> undefined
    end.

assert_state_for_all(Station, Identities, ExpectedState) ->
    [begin
         Pub = macula_identity:public(macula_station:identity(Other)),
         ?assertEqual(ExpectedState, member_state(Station, Pub))
     end
     || Other <- Identities].
