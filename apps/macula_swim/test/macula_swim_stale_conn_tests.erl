%% A dead conn pid must not be probed, and a late ACK must refute.
%%
%% Both defects share one root: SWIM holds a single `conn_pid' per member,
%% handed to it once, and never re-validated. `is_pid/1' is TRUE for a dead
%% pid, so a member whose conn died stayed selectable as a probe target
%% forever, and every ping into that void timed out into a `confirmed_failed'
%% verdict about a station that was reachable the whole time.
%%
%% This is the same pathogen as the 2026-07-26 multihop pubsub root cause. It
%% is pinned here so its third recurrence is a test failure rather than
%% another investigation.
-module(macula_swim_stale_conn_tests).

-include_lib("eunit/include/eunit.hrl").

-define(PERIOD_MS,          20).
-define(PING_TIMEOUT_MS,    30).
-define(SUSPECT_TIMEOUT_MS, 60).

%%---------------------------------------------------------------------
%% A dead pid is not a probe target
%%---------------------------------------------------------------------

%% Without the liveness filter this member is picked, pinged into the void,
%% and confirmed dead. The peer never existed as a live process at all.
dead_conn_pid_is_never_probed_test() ->
    {ok, Swim} = start_swim(),
    Dead = spawn(fun() -> ok end),
    ok = wait_until_dead(Dead),
    PeerId = macula_identity:public(macula_identity:generate()),
    ok = macula_swim:add_peer(Swim, PeerId, Dead),
    %% Long enough for several probe periods AND the suspect window.
    timer:sleep(?SUSPECT_TIMEOUT_MS * 5),
    ?assertEqual(alive, state_of(Swim, PeerId)),
    macula_swim:stop(Swim).

%% A LIVE conn that simply never answers must still convert — the fix must not
%% turn the detector off.
live_but_silent_conn_still_converts_test() ->
    {ok, Swim} = start_swim(),
    Silent = spawn(fun Loop() -> receive _ -> Loop() end end),
    PeerId = macula_identity:public(macula_identity:generate()),
    ok = macula_swim:add_peer(Swim, PeerId, Silent),
    ok = wait_for_state(Swim, PeerId, confirmed_failed, 3000),
    exit(Silent, kill),
    macula_swim:stop(Swim).

%%---------------------------------------------------------------------
%% A late ACK refutes
%%---------------------------------------------------------------------

%% The suspect window is the ONLY chance: `pick_alive_target/1' never
%% re-probes a suspect, so before the fix this ACK matched no pending round
%% and was dropped, discarding the one frame proving the peer was alive.
late_ack_from_a_suspect_refutes_test() ->
    {ok, Swim} = start_swim(),
    Silent = spawn(fun Loop() -> receive _ -> Loop() end end),
    PeerId = macula_identity:public(macula_identity:generate()),
    ok = macula_swim:add_peer(Swim, PeerId, Silent),
    ok = wait_for_state(Swim, PeerId, suspect, 2000),
    %% Round 999999 matches nothing in `probes'. That is the point.
    macula_swim:handle_frame(Swim, PeerId, ack_frame(999999)),
    ok = wait_for_state(Swim, PeerId, alive, 1000),
    exit(Silent, kill),
    macula_swim:stop(Swim).

%% An ACK from a CONFIRMED peer resurrects it, and must.
%%
%% This test asserted the opposite until 2026-07-27. The claim was that reviving
%% a published verdict would be invisible to the consumer; it is not, since
%% `touch_alive/3' notifies on every non-alive transition. Meanwhile `on_ping/3'
%% resurrected confirmed members all along, so the two entry points disagreed.
%% Refusing here also made a false confirmation PERMANENT: nothing re-admits a
%% still-connected peer, so it stayed dead until the next restart.
late_ack_resurrects_a_confirmed_peer_test() ->
    {ok, Swim} = start_swim(),
    Silent = spawn(fun Loop() -> receive _ -> Loop() end end),
    PeerId = macula_identity:public(macula_identity:generate()),
    ok = macula_swim:add_peer(Swim, PeerId, Silent),
    ok = wait_for_state(Swim, PeerId, confirmed_failed, 3000),
    macula_swim:handle_frame(Swim, PeerId, ack_frame(999999)),
    ok = wait_for_state(Swim, PeerId, alive, 1000),
    ?assertEqual(alive, state_of(Swim, PeerId)),
    exit(Silent, kill),
    macula_swim:stop(Swim).

%%---------------------------------------------------------------------
%% Mechanism counters
%%---------------------------------------------------------------------

%% These exist so a QUIET FLEET IS READABLE. Post-capability-gate the fleet
%% emits ~0 suspicions per hour, and from outside "nothing to detect" and
%% "detector cannot fire" are indistinguishable. `probes_sent' separates them.
%% `refuted' answers the separate question of whether the late-ACK branch has
%% ever executed in production at all, which no conversion count can.
probes_are_counted_even_when_nothing_is_suspected_test() ->
    {ok, Swim} = start_swim(),
    Live = spawn(fun Loop() -> receive _ -> Loop() end end),
    PeerId = macula_identity:public(macula_identity:generate()),
    ok = macula_swim:add_peer(Swim, PeerId, Live),
    timer:sleep(?PERIOD_MS * 5),
    #{probes_sent := Sent} = macula_swim:stats(Swim),
    ?assert(Sent > 0),
    exit(Live, kill),
    macula_swim:stop(Swim).

%% A dead conn must NOT be probed, so the counter must stay at zero. This is
%% the pair to the test above: together they show the difference between a
%% detector that is idle and one that is firing into the void.
dead_conn_produces_no_probes_test() ->
    {ok, Swim} = start_swim(),
    Dead = spawn(fun() -> ok end),
    ok = wait_until_dead(Dead),
    PeerId = macula_identity:public(macula_identity:generate()),
    ok = macula_swim:add_peer(Swim, PeerId, Dead),
    timer:sleep(?PERIOD_MS * 5),
    ?assertEqual(0, maps:get(probes_sent, macula_swim:stats(Swim))),
    macula_swim:stop(Swim).

refutation_is_counted_test() ->
    {ok, Swim} = start_swim(),
    Silent = spawn(fun Loop() -> receive _ -> Loop() end end),
    PeerId = macula_identity:public(macula_identity:generate()),
    ok = macula_swim:add_peer(Swim, PeerId, Silent),
    ok = wait_for_state(Swim, PeerId, suspect, 2000),
    ?assertEqual(0, maps:get(refuted, macula_swim:stats(Swim))),
    macula_swim:handle_frame(Swim, PeerId, ack_frame(999999)),
    ok = wait_for_state(Swim, PeerId, alive, 1000),
    #{refuted := R, suspected := Su} = macula_swim:stats(Swim),
    ?assertEqual(1, R),
    ?assert(Su >= 1),
    exit(Silent, kill),
    macula_swim:stop(Swim).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

start_swim() ->
    Kp = macula_identity:generate(),
    macula_swim:start_link(
      #{self_node_id       => macula_identity:public(Kp),
        identity           => Kp,
        controlling_pid    => self(),
        period_ms          => ?PERIOD_MS,
        ping_timeout_ms    => ?PING_TIMEOUT_MS,
        suspect_timeout_ms => ?SUSPECT_TIMEOUT_MS}).

ack_frame(Round) ->
    macula_frame:swim_ack(#{round       => Round,
                            responder   => <<0:256>>,
                            incarnation => 0,
                            piggyback   => []}).

state_of(Swim, NodeId) ->
    case [maps:get(state, M)
          || M <- macula_swim:members(Swim), maps:get(node_id, M) =:= NodeId] of
        [St] -> St;
        []   -> absent
    end.

wait_for_state(_Swim, _NodeId, _Want, Budget) when Budget =< 0 ->
    {error, timeout};
wait_for_state(Swim, NodeId, Want, Budget) ->
    check_state(state_of(Swim, NodeId) =:= Want, Swim, NodeId, Want, Budget).

check_state(true, _Swim, _NodeId, _Want, _Budget) ->
    ok;
check_state(false, Swim, NodeId, Want, Budget) ->
    timer:sleep(20),
    wait_for_state(Swim, NodeId, Want, Budget - 20).

wait_until_dead(Pid) ->
    check_dead(erlang:is_process_alive(Pid), Pid).

check_dead(false, _Pid) -> ok;
check_dead(true,   Pid) -> timer:sleep(5), wait_until_dead(Pid).
