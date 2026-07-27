%% Three-arm differential harness for the two SWIM false-confirm fixes.
%%
%% WHY. The fleet cannot answer this. The regime the stale-pid fix addresses is
%% asymmetric conn loss where the SURVIVING conn outlives the suspicion window;
%% a container roll kills both directions within seconds, so the survivor never
%% persists and burst conversion is identical with and without the fix. The
%% pre-gate 100% conversion figure is also the wrong comparator, being the
%% daemon population that the capability gate had already removed. And
%% production can never host the reverted arm, so a fleet reading has no floor.
%%
%% A floor is the point. A fix that passes against a failure that never
%% reproduced has been tested against nothing, so every arm here must show its
%% REVERTED counterpart failing. If a reverted arm does not fail, the production
%% false-confirms had a cause other than the one being claimed.
%%
%% == How the reverted arms are built ==
%%
%% Arm B withholds the observer's resync, which lives outside macula_swim and
%% is simply not performed. Faithful by construction.
%%
%% Arm C withholds the late ACK. This is EXACTLY the pre-fix behaviour, not an
%% approximation: the old `on_ack/3' fall-through was `_ -> S', which has no
%% side effects whatsoever, so a discarded ACK is observationally identical to
%% an ACK that never arrived. Reverting by omission needs no duplicate module.
%%
%% Arm B_pre emulates the pre-fix `pick_alive_target/1', which guarded on
%% `is_pid/1' alone. A cast to a DEAD pid silently succeeds and is never
%% answered, so a live process that ignores every frame reproduces the old
%% behaviour precisely, and does so through the current code.
%%
%% ⚠ ARMS B' AND C ARE THE SAME EXPERIMENT UNDER TWO NAMES. Both spawn a mute
%% process, add it, and settle. The equivalence arguments above are each
%% defensible, but this harness does NOT give the two fixes independent floors:
%% it demonstrates ONE floor, that a member which never answers converts, and
%% that floor is compatible with either mechanism. What separates them is the
%% A2-versus-C contrast, where the only difference is whether a late ACK ever
%% arrives. Do not read the two 10/10 rows as two pieces of evidence.
%%
%% == The outcome that is not binary ==
%%
%% ⚠ THREE OUTCOMES, NOT TWO. The `is_process_alive/1' filter means a member
%% whose conn pid is dead is no longer SELECTED, so it is never probed, never
%% suspected, and never converted. That is not success: it converts a
%% false-POSITIVE failure mode into a BLIND-SPOT one, where SWIM reports a
%% member `alive' while having verified nothing about it. Only the observer's
%% resync restores actual verification. A harness scoring converted-vs-not
%% would call arm B a pass and miss this entirely, so trials are scored
%% `converted', `alive_probed' or `alive_unprobed'.
-module(macula_swim_three_arm_tests).

-include_lib("eunit/include/eunit.hrl").

-define(PERIOD_MS,           40).
-define(PING_TIMEOUT_MS,     20).
-define(SUSPECT_TIMEOUT_MS, 120).
%% The survivor must outlive at least three suspicion windows, which is what
%% makes this the in-regime event rather than a roll.
-define(TRIAL_MS,           (?SUSPECT_TIMEOUT_MS * 5)).
-define(N,                   10).

%%---------------------------------------------------------------------
%% Arms
%%---------------------------------------------------------------------

three_arm_differential_test_() ->
    {timeout, 300, fun() ->
        Sched = kill_schedule(?N),
        A1 = run_arm(resync_applied,  Sched),
        B  = run_arm(resync_withheld, Sched),
        Bp = run_arm(prefix_dead_pid, Sched),
        A2 = run_arm(late_ack_delivered, Sched),
        C  = run_arm(late_ack_withheld, Sched),
        [report(N, R) || {N, R} <- [{"A1 resync applied     ", A1},
                                    {"B  resync withheld    ", B},
                                    {"B' pre-fix dead pid   ", Bp},
                                    {"A2 late ACK delivered ", A2},
                                    {"C  late ACK withheld  ", C}]],

        %% Measured 2026-07-27, n=10 per arm, one shared kill schedule:
        %%
        %%   A1 resync applied      converted= 0 probed=10 unprobed= 0
        %%   B  resync withheld     converted= 1 probed= 0 unprobed= 9
        %%   B' pre-fix dead pid    converted=10 probed= 0 unprobed= 0
        %%   A2 late ACK delivered  converted= 0 probed=10 unprobed= 0
        %%   C  late ACK withheld   converted=10 probed= 0 unprobed= 0
        %%
        %% FLOORS FIRST. If the reverted arms do not reproduce the failure,
        %% nothing below this line means anything.
        ?assert(conv(Bp) >= 9),
        ?assert(conv(C)  >= 9),

        %% The fixes hold against the failure their floors just demonstrated.
        ?assert(conv(A1) =< 1),
        ?assert(conv(A2) =< 1),

        %% Arm A1 must also actually VERIFY the peer, not merely refrain from
        %% condemning it. This is the assertion that separates the fix from the
        %% blind spot below.
        ?assert(outcome(A1, alive_probed) >= 9),

        %% ⚠ THE BLIND SPOT. Withholding the resync does not convert any more,
        %% because the liveness filter stops selecting a dead pid. It goes
        %% silent instead: alive, unprobed, unverified. Pinned so that a future
        %% reading of "arm B does not convert" is never mistaken for health.
        ?assert(conv(B) =< 1),
        ?assert(outcome(B, alive_unprobed) >= 9)
    end}.

%%---------------------------------------------------------------------
%% Harness
%%---------------------------------------------------------------------

%% One deterministic schedule, reused across every arm, so arms differ only in
%% the mechanism under test and never in when the conn died.
kill_schedule(N) ->
    {Delays, _} = lists:foldl(fun(_, {Acc, St}) ->
        {D, St1} = rand:uniform_s(?PERIOD_MS * 3, St),
        {[D | Acc], St1}
    end, {[], rand:seed_s(exsss, {42, 17, 99})}, lists:seq(1, N)),
    lists:reverse(Delays).

run_arm(Arm, Sched) ->
    [trial(Arm, D) || D <- Sched].

conv(Results)          -> outcome(Results, converted).
outcome(Results, Want) -> length([x || R <- Results, R =:= Want]).

report(Name, R) ->
    io:format(user, "~n  ~s converted=~2B alive_probed=~2B alive_unprobed=~2B",
              [Name, outcome(R, converted), outcome(R, alive_probed),
               outcome(R, alive_unprobed)]).

%%---------------------------------------------------------------------
%% One trial
%%---------------------------------------------------------------------

%% Exactly ONE member, so the global `probes_sent' counter is unambiguously
%% about that member and "was it verified" needs no per-peer accounting.
trial(Arm, KillDelay) ->
    {ok, Swim} = start_swim(),
    BId = macula_identity:public(macula_identity:generate()),
    Outcome = play(Arm, Swim, BId, KillDelay),
    macula_swim:stop(Swim),
    Outcome.

%% P1, asymmetric conn loss. SWIM was handed X. X dies; Y survives and answers.
%%
%% ⚠ X MUST ANSWER UNTIL IT IS KILLED. The first version of this harness used a
%% silent process for X, so the detector correctly began suspecting BEFORE the
%% kill and arm A1 converted 4 of 10 -- an artefact of measuring "a conn that
%% never worked" instead of "a working conn that died". The regime under test
%% is a HEALTHY mutual pair losing one direction, so X is a live responder up
%% to the moment it dies. Same error class as dropping ACKs alongside PINGs in
%% macula_swim_conversion_tests: the perturbation did more than the one thing
%% it claimed to do.
play(resync_applied, Swim, BId, KillDelay) ->
    X = responder(Swim, BId, 0),
    Y = responder(Swim, BId, 0),
    ok = macula_swim:add_peer(Swim, BId, X),
    timer:sleep(KillDelay),
    kill_and_wait(X),
    %% What macula_station_peer_observer:resync_swim_after_conn_loss/4 does.
    ok = macula_swim:add_peer(Swim, BId, Y),
    settle(Swim, BId, [Y], probes(Swim));

play(resync_withheld, Swim, BId, KillDelay) ->
    X = responder(Swim, BId, 0),
    Y = responder(Swim, BId, 0),
    ok = macula_swim:add_peer(Swim, BId, X),
    timer:sleep(KillDelay),
    kill_and_wait(X),
    settle(Swim, BId, [Y], probes(Swim));

%% B', the pre-fix world: the pid passes `is_pid/1' and is selected, but no
%% answer ever comes back. A live process that ignores frames IS a dead pid as
%% far as a `gen_statem:cast' is concerned.
play(prefix_dead_pid, Swim, BId, KillDelay) ->
    Silent = spawn(fun Loop() -> receive _ -> Loop() end end),
    ok = macula_swim:add_peer(Swim, BId, Silent),
    timer:sleep(KillDelay),
    settle(Swim, BId, [Silent], 0);

%% P2, late ACK. The conn is alive and answers, but after ping_timeout.
play(late_ack_delivered, Swim, BId, KillDelay) ->
    Late = responder(Swim, BId, ?PING_TIMEOUT_MS * 2),
    ok = macula_swim:add_peer(Swim, BId, Late),
    timer:sleep(KillDelay),
    settle(Swim, BId, [Late], 0);

%% C: the ACK is withheld, which is precisely what the old fall-through did
%% with it. Same perturbation, same timings, one mechanism removed.
play(late_ack_withheld, Swim, BId, KillDelay) ->
    Mute = spawn(fun Loop() -> receive _ -> Loop() end end),
    ok = macula_swim:add_peer(Swim, BId, Mute),
    timer:sleep(KillDelay),
    settle(Swim, BId, [Mute], 0).

%% Let the trial run its course, then score it.
%%
%% ⚠ PROBES ARE COUNTED FROM THE PERTURBATION, NOT FROM BOOT. `probes_sent' is
%% cumulative, and in the P1 arms SWIM probed X happily for up to 120ms before
%% the kill. Scoring on the raw total therefore reported arm B as
%% `alive_probed' 6 times out of 10 on the strength of probes that predated the
%% event under test, which is precisely the blind spot this harness exists to
%% expose. The baseline is taken the instant the perturbation completes.
settle(Swim, BId, Procs, Baseline) ->
    timer:sleep(?TRIAL_MS),
    St = state_of(Swim, BId),
    Sent = probes(Swim) - Baseline,
    [exit(P, kill) || P <- Procs],
    score(St, Sent).

probes(Swim) ->
    maps:get(probes_sent, macula_swim:stats(Swim)).

score(confirmed_failed, _Sent)       -> converted;
score(_Alive, Sent) when Sent > 0    -> alive_probed;
score(_Alive, _Zero)                 -> alive_unprobed.

%% `alive_unprobed' is the finding, not a pass: SWIM reports the member alive
%% while having sent it nothing since the conn died. It has verified nothing.

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

%% Stands in for a peering conn that answers PINGs, optionally after a delay.
%% `macula_peering:send_frame/2' is a plain `gen_statem:cast', so receiving
%% `{'$gen_cast', {send_frame, Frame}}' is the whole contract.
responder(Swim, BId, DelayMs) ->
    spawn(fun Loop() ->
        receive
            {'$gen_cast', {send_frame, #{frame_type := swim_ping,
                                         round := Round}}} ->
                timer:sleep(DelayMs),
                macula_swim:handle_frame(Swim, BId, ack(Round)),
                Loop();
            _Other ->
                Loop()
        end
    end).

ack(Round) ->
    macula_frame:swim_ack(#{round       => Round,
                            responder   => <<0:256>>,
                            incarnation => 0,
                            piggyback   => []}).

kill_and_wait(Pid) ->
    exit(Pid, kill),
    wait_dead(Pid).

wait_dead(Pid) ->
    check_dead(erlang:is_process_alive(Pid), Pid).

check_dead(false, _Pid) -> ok;
check_dead(true,   Pid) -> timer:sleep(2), wait_dead(Pid).

state_of(Swim, NodeId) ->
    case [maps:get(state, M)
          || M <- macula_swim:members(Swim), maps:get(node_id, M) =:= NodeId] of
        [St] -> St;
        []   -> absent
    end.
