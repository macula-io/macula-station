%% Does a suspected member convert to confirmed_failed, and how often?
%%
%% WHY THIS EXISTS. On the live fleet, 142 of 142 confirmed_failed verdicts
%% were contradicted by a live connection. That turned out to be population
%% pollution: SWIM was fed every connecting peer including daemons, which
%% cannot answer a probe. After gating membership on station capability the
%% observed conversion fell to 3 of 8 suspects. But 8 events from a single
%% post-boot window is not a rate, and inferring a mechanism from it is exactly
%% the mistake this codebase keeps making.
%%
%% So measure the mechanism directly instead of waiting for the fleet.
%%
%% THE MODEL UNDER TEST. Once A marks B suspect, A can never re-probe it:
%% `pick_alive_target/1' only selects members in state `alive', and a suspect
%% is not. A late ACK cannot rescue either, because `on_ping_timeout/3' has
%% already taken the pending round out of the map, so the arriving ACK matches
%% nothing. The ONLY rescue path is B independently PINGing A, which lands in
%% `on_ping/3' and reaches `touch_alive/3'.
%%
%% B probes one random alive member per period, and the suspect window is three
%% periods, so
%%
%%     P(rescue)     = 1 - (1 - 1/M)^3
%%     P(conversion) =     (1 - 1/M)^3
%%
%% where M is the suspect's member count. That predicts 0.30 at M=3, 0.42 at
%% M=4, 0.51 at M=5, 0.73 at M=10 -- i.e. conversion RISES with mesh size,
%% which is the counter-intuitive part worth pinning.
%%
%% HOW. K real `macula_swim' gen_servers in a full mesh. `macula_peering:
%% send_frame/2' is a plain `gen_statem:cast', so a spawned loop process serves
%% as a conn: it receives the cast and calls `macula_swim:handle_frame/3' on
%% the far side, or drops when told to.
%%
%% ⚠ DROP A's PINGS ONLY, NOT ALL A->B TRAFFIC. The first version of this
%% harness dropped every frame on the A->B link, which also swallowed A's ACKs
%% to B. B therefore stopped hearing from A, suspected A in turn, and -- since
%% `pick_alive_target/1' only selects `alive' members -- STOPPED PROBING A.
%% That severs the very rescue path under measurement, and it inflated
%% conversion to 75/67/89% against a predicted 30/51/73%. The harness was
%% measuring a mutual blackout, not a missed probe.
%%
%% Dropping only A's PING leaves B fully served: B still hears A's ACKs, keeps
%% A `alive', and keeps probing it. That is a single missed probe target and no
%% other perturbation, which is the question actually being asked -- a live
%% peer that misses one ACK under load, not a partition.
-module(macula_swim_conversion_tests).

-include_lib("eunit/include/eunit.hrl").

%% Fast but keeping the ratio the production defaults use: the suspect window
%% is exactly three probe periods, which is what makes the exponent 3.
-define(PERIOD_MS,          20).
-define(PING_TIMEOUT_MS,     8).
-define(SUSPECT_TIMEOUT_MS, 60).
-define(TRIALS,             60).

%%---------------------------------------------------------------------
%% The curve
%%---------------------------------------------------------------------

conversion_follows_the_mutual_ping_model_test_() ->
    {timeout, 300, fun() ->
        Results = [{M, measure(M, ?TRIALS)} || M <- [3, 5, 10]],
        [report(M, R) || {M, R} <- Results],
        %% The model's central claim is monotonic: a BIGGER mesh converts MORE,
        %% because the suspect is less likely to happen to probe its accuser.
        %% Measured 2026-07-27, 60 trials per point, against a predicted
        %% 29.6 / 51.2 / 72.9:
        %%
        %%   run 1   28.3 / 48.2 / 75.5   deltas -1.3, -3.0, +2.6
        %%   run 2   23.3 / 55.2 / 78.2   deltas -6.3, +4.0, +5.3
        %%
        %% The band is 0.15 because binomial noise at n=60 and p~0.5 is about
        %% 6.5 points, which is the whole of the run-to-run spread above. It is
        %% not slack for a loose fit.
        [begin
             Pred = math:pow(1 - 1/M, 3),
             #{conversion := Got, suspects := S} = R,
             ?assert(S > 0),
             ?assert(abs(Got - Pred) < 0.15)
         end || {M, R} <- Results],
        [{_, #{conversion := C3}}, {_, #{conversion := C5}},
         {_, #{conversion := C10}}] = Results,
        %% Strictly monotonic: the bigger the mesh, the MORE often a suspicion
        %% converts, because the suspect is less likely to happen to probe the
        %% node accusing it. That is the scaling defect this pins.
        ?assert(C10 > C5),
        ?assert(C5 > C3)
    end}.

%% The rescue path must actually fire. Zero rescues at small M would mean the
%% mutual-ping mechanism does not work and conversion is unconditional --
%% the "independently mistuned" hypothesis.
rescue_path_fires_at_small_mesh_test_() ->
    {timeout, 120, fun() ->
        #{rescued := Rescued, suspects := S} = measure(3, ?TRIALS),
        ?assert(S > 0),
        ?assert(Rescued > 0)
    end}.

%%---------------------------------------------------------------------
%% Harness
%%---------------------------------------------------------------------

report(M, #{suspects := S, converted := C, rescued := R,
            conversion := Conv}) ->
    io:format(user,
              "~n  M=~2B  suspects=~3B converted=~3B rescued=~3B  "
              "conversion=~5.1f%  predicted=~5.1f%",
              [M, S, C, R, Conv * 100, math:pow(1 - 1/M, 3) * 100]).

measure(M, Trials) ->
    Outcomes = [trial(M) || _ <- lists:seq(1, Trials)],
    S = length([x || O <- Outcomes, O =/= not_suspected]),
    C = length([x || converted <- Outcomes]),
    R = length([x || rescued <- Outcomes]),
    #{suspects => S, converted => C, rescued => R,
      conversion => conversion(C, S)}.

conversion(_C, 0) -> 0.0;
conversion(C, S)  -> C / S.

%% One trial: K = M+1 nodes so every node holds exactly M members. Drop all
%% A->B traffic, then read what A concluded about B.
trial(M) ->
    K = M + 1,
    Collector = self(),
    process_flag(trap_exit, true),
    Nodes = [start_node(Collector) || _ <- lists:seq(1, K)],
    Links = wire(Nodes),
    [{ASwim, AId} | _] = Nodes,
    {BSwim, BId} = lists:nth(2, Nodes),
    _ = ASwim, _ = BSwim,
    %% Starve A of B's ACKs by dropping A's PINGS only. B keeps receiving A's
    %% ACKs, so B keeps A `alive' and keeps probing it -- the rescue path stays
    %% intact and is what we are measuring.
    set_drop(maps:get({AId, BId}, Links), true),
    Outcome = await_verdict(BId, ?SUSPECT_TIMEOUT_MS * 6),
    [macula_swim:stop(P) || {P, _} <- Nodes],
    [exit(L, kill) || L <- maps:values(Links)],
    flush(),
    Outcome.

start_node(Collector) ->
    Kp = macula_identity:generate(),
    {ok, Pid} = macula_swim:start_link(
                  #{self_node_id       => macula_identity:public(Kp),
                    identity           => Kp,
                    controlling_pid    => Collector,
                    period_ms          => ?PERIOD_MS,
                    ping_timeout_ms    => ?PING_TIMEOUT_MS,
                    suspect_timeout_ms => ?SUSPECT_TIMEOUT_MS}),
    {Pid, macula_identity:public(Kp)}.

%% One link process per ORDERED pair. It stands in for a peering conn: SWIM
%% casts {send_frame, Frame} at it, and it hands the frame to the far side as
%% having come from the near side.
wire(Nodes) ->
    maps:from_list(
      [begin
           %% UNLINKED: these are torn down with exit/2 at the end of every
           %% trial, and a linked kill would take the test process with it.
           L = spawn(fun() -> link_loop(ToSwim, FromId, false) end),
           ok = macula_swim:add_peer(FromSwim, ToId, L),
           {{FromId, ToId}, L}
       end
       || {FromSwim, FromId} <- Nodes, {ToSwim, ToId} <- Nodes,
          FromId =/= ToId]).

link_loop(ToSwim, FromId, DropPings) ->
    receive
        {set_drop, D} ->
            link_loop(ToSwim, FromId, D);
        {'$gen_cast', {send_frame, Frame}} ->
            maybe_deliver(DropPings andalso is_ping(Frame),
                          ToSwim, FromId, Frame),
            link_loop(ToSwim, FromId, DropPings);
        _Ignored ->
            link_loop(ToSwim, FromId, DropPings)
    end.

is_ping(Frame) -> macula_frame:frame_type(Frame) =:= swim_ping.

maybe_deliver(true, _ToSwim, _FromId, _Frame) ->
    ok;
maybe_deliver(false, ToSwim, FromId, Frame) ->
    macula_swim:handle_frame(ToSwim, FromId, Frame).

set_drop(Link, D) -> Link ! {set_drop, D}, ok.

%% Read A's conclusion about B off the controlling-pid notifications. A trial
%% where B is never picked at all is `not_suspected' and is excluded from the
%% denominator rather than counted as a rescue.
await_verdict(BId, Timeout) ->
    receive
        {macula_swim, member_state, BId, suspect} ->
            await_resolution(BId, Timeout)
    after Timeout ->
        not_suspected
    end.

await_resolution(BId, Timeout) ->
    receive
        {macula_swim, member_state, BId, confirmed_failed} -> converted;
        {macula_swim, member_state, BId, alive}            -> rescued
    after Timeout ->
        not_suspected
    end.

flush() ->
    receive _ -> flush() after 0 -> ok end.
