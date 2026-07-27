%% One direction of a mutual pair dies, end to end over real QUIC.
%%
%% WHY THIS ONE AND NOT A KILL/RESTART ARC. Killing a whole station takes BOTH
%% conn directions down, so the peer is isolated and removed, which is the path
%% `macula_station_peer_observer_tests:disconnected_removes_from_swim' already
%% pins deterministically in milliseconds. The only thing real QUIC adds there
%% is WHEN the survivors notice, which is a property of MsQuic's idle timers and
%% `peer:stop's shutdown mode, not of this codebase. Asserting on it buys CI
%% flakiness to measure someone else's config.
%%
%% Restart-and-rejoin is worse: `macula_station:connect_to/1' is a bare
%% `macula_peering:connect' with no `outbound_link', so no reconnect-with-backoff
%% loop exists for any edge this harness builds. After a respawn nothing redials
%% the victim, nobody can learn its new ephemeral port, and the only way to get
%% the mesh back is for the test driver to dial it. A green result there would
%% read as "the mesh healed" when it means "the test script healed the mesh".
%%
%% What is NOT covered anywhere else is this: `macula_swim_three_arm_tests'
%% proves the mechanism by calling `macula_swim:add_peer/3' itself, so it never
%% exercises the real chain. This suite drives a genuine QUIC conn death into a
%% real `disconnected' notification, through `resync_swim_after_conn_loss/4'
%% with its is_station gate populated by the real capability probe, through
%% `drop_probes_for/2', and out into signed PING/ACK over the surviving conn
%% with real signature verification.
%%
%% ⚠ THE LOAD-BEARING ASSERTION IS `acks_matched', NOT "stays alive".
%%
%% With the resync reverted, the peer ALSO stays alive: the `is_process_alive'
%% filter simply stops selecting the dead pid, so nothing is probed and nothing
%% is suspected. That is the blind-spot mode measured in the three-arm harness
%% (arm B, 9 of 10). A suite asserting only "never leaves alive" passes on the
%% reverted build and discriminates nothing. Verified liveness means ACKs still
%% arriving after the kill.
-module(macula_station_asymmetric_loss_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([conn_death_on_a_mutual_pair_keeps_the_peer_verified/1,
         conn_death_repoints_swim_at_the_survivor/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% Three full suspect cycles at production defaults (2s period, 6s suspect).
-define(OBSERVE_MS, 25_000).
-define(HANDSHAKE_MS, 15_000).

all() ->
    [conn_death_on_a_mutual_pair_keeps_the_peer_verified,
     conn_death_repoints_swim_at_the_survivor].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Case, Config) ->
    %% Two stations, because the regime needs exactly one mutual pair.
    Stations = macula_station_test_cluster:spawn_cluster(
                 2, #{base_dir => ?config(priv_dir, Config)}),
    [{stations, Stations} | Config].

end_per_testcase(_Case, Config) ->
    macula_station_test_cluster:stop_cluster(?config(stations, Config)),
    ok.

%%===================================================================
%% Cases
%%===================================================================

%% The pre-registered claim. Kill the conn SWIM actually holds and the peer
%% must remain VERIFIED alive, not merely un-condemned.
conn_death_on_a_mutual_pair_keeps_the_peer_verified(Config) ->
    {A, B} = mutual_pair(Config),
    BId = macula_station_test_cluster:pubkey(B),

    %% Read which pid SWIM holds rather than assuming a direction:
    %% `primary_conn_lookup/1' prefers inbound, but which conn won the race to
    %% populate the slot is not something this test should depend on.
    Held = swim_conn_pid(A, BId),
    ?assert(is_pid(Held)),
    Acks0 = swim_stat(A, acks_matched),

    kill_on(A, Held),

    %% Never leaves alive, across three full suspect cycles.
    ok = hold_state(A, BId, alive, ?OBSERVE_MS),

    %% ⚠ The assertion that separates a working resync from the blind spot.
    Acks1 = swim_stat(A, acks_matched),
    ct:pal("acks_matched ~p -> ~p over ~pms", [Acks0, Acks1, ?OBSERVE_MS]),
    ?assert(Acks1 - Acks0 >= 5),

    %% And the mechanism counter proves the fixed path is what did it.
    ?assert(verdict(A, conn_resynced) >= 1).

%% SWIM must end up holding a DIFFERENT, live pid: the survivor.
conn_death_repoints_swim_at_the_survivor(Config) ->
    {A, B} = mutual_pair(Config),
    BId = macula_station_test_cluster:pubkey(B),
    Held = swim_conn_pid(A, BId),

    kill_on(A, Held),
    ok = wait_until(fun() ->
        Now = swim_conn_pid(A, BId),
        is_pid(Now) andalso Now =/= Held
    end, ?HANDSHAKE_MS),

    Survivor = swim_conn_pid(A, BId),
    ?assertNotEqual(Held, Survivor),
    ?assert(alive_on(A, Survivor)).

%%===================================================================
%% Helpers
%%===================================================================

%% ⚠ TWO DIFFERENT DIAL MECHANISMS, DELIBERATELY.
%%
%% `dial/2' is a bare connect, and the observer records the resulting event as
%% INBOUND. Calling it twice does NOT build a mutual pair: both conns land in
%% the inbound slot and `purge_stale_slot/4' evicts the first, leaving ONE conn.
%% Killing that is an isolation, not an asymmetric loss, and the peer is
%% correctly removed -- which is exactly how the first version of this suite
%% failed, with the member `absent' rather than `alive'.
%%
%% Only `macula_station_outbound_link' re-tags its event as
%% `connected_outbound' and so populates the outbound slot. So: B dials A the
%% cheap way for the inbound side, and A runs a REAL outbound link to B.
mutual_pair(Config) ->
    [A, B] = ?config(stations, Config),
    ok = macula_station_test_cluster:dial(B, A),
    {ok, _Link} = macula_station_test_cluster:dial_outbound(A, B),
    BId = macula_station_test_cluster:pubkey(B),
    %% Wait for BOTH slots, not merely for SWIM to know the peer: a single
    %% direction would satisfy a naive is_pid check and silently retire the
    %% regime under test.
    ok = wait_until(fun() -> mutual(A, BId) end, ?HANDSHAKE_MS),
    {A, B}.

%% Both directions present for this NodeId.
mutual(Station, NodeId) ->
    case maps:get(NodeId, conns(Station), undefined) of
        #{inbound := In, outbound := Out} -> is_pid(In) andalso is_pid(Out);
        _Other                            -> false
    end.

conns(Station) ->
    macula_station_test_cluster:rpc(
      Station, macula_station_test_cluster, on_peer_conns, []).

%% Member list is fetched WHOLE and filtered here, rather than sending a fun to
%% the peer: a fun closes over its defining module, and this suite's module is
%% not guaranteed loadable on a spawned node.
members(Station) ->
    macula_station_test_cluster:rpc(
      Station, macula_station_test_cluster, on_peer_swim_members, []).

%% The conn pid SWIM holds for NodeId, or undefined.
swim_conn_pid(Station, NodeId) ->
    first([C || #{node_id := N, conn_pid := C} <- members(Station),
                N =:= NodeId], undefined).

state_on(Station, NodeId) ->
    first([S || #{node_id := N, state := S} <- members(Station),
                N =:= NodeId], absent).

first([X | _], _Default) -> X;
first([], Default)       -> Default.

swim_stat(Station, Key) ->
    maps:get(Key, macula_station_test_cluster:rpc(
                    Station, macula_station_test_cluster,
                    on_peer_swim_stats, []), 0).

verdict(Station, Key) ->
    maps:get(Key, macula_station_test_cluster:rpc(
                    Station, macula_station_test_cluster,
                    on_peer_swim_verdicts, []), 0).

alive_on(Station, Pid) ->
    macula_station_test_cluster:rpc(
      Station, erlang, is_process_alive, [Pid]).

kill_on(Station, Pid) ->
    macula_station_test_cluster:rpc(Station, erlang, exit, [Pid, kill]),
    ok.

%% Assert a state HOLDS for the whole budget, rather than merely being reached
%% once. A member that flaps alive -> suspect -> alive would pass a
%% wait-for-alive check while demonstrating the exact defect under test.
hold_state(Station, NodeId, Want, Budget) when Budget =< 0 ->
    ?assertEqual(Want, state_on(Station, NodeId)),
    ok;
hold_state(Station, NodeId, Want, Budget) ->
    ?assertEqual(Want, state_on(Station, NodeId)),
    timer:sleep(500),
    hold_state(Station, NodeId, Want, Budget - 500).

wait_until(Pred, Budget) when Budget =< 0 ->
    ?assert(Pred()),
    ok;
wait_until(Pred, Budget) ->
    step_until(Pred(), Pred, Budget).

step_until(true, _Pred, _Budget) -> ok;
step_until(false, Pred, Budget) ->
    timer:sleep(250),
    wait_until(Pred, Budget - 250).
