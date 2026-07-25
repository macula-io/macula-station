%%% @doc Tripwire transition logic for the health beacon. Covers the
%%% risky pure functions: reds/s derivation, strike accumulation per
%%% class, mailbox-growth detection, edge-triggered alarm, and the
%%% incarnation-bridging guard (dead-proc pruning).
-module(macula_station_health_publisher_tests).
-include_lib("eunit/include/eunit.hrl").

-define(M, macula_station_health_publisher).

%%====================================================================
%% rate/4 — reds/s over the tick
%%====================================================================

rate_no_prior_sample_is_zero_test() ->
    ?assertEqual(0, ?M:rate(<<"x">>, 5000, #{}, 10_000)).

rate_normal_delta_test() ->
    %% 20000 reds over 10s => 2000 reds/s.
    Prev = #{<<"x">> => {10000, 0}},
    ?assertEqual(2000, ?M:rate(<<"x">>, 30000, Prev, 10_000)).

rate_restart_clamps_to_zero_test() ->
    %% Fresh pid after supervised restart: lifetime counter reset near 0,
    %% below the previous total => must clamp to 0, never report negative.
    Prev = #{<<"x">> => {1_000_000, 0}},
    ?assertEqual(0, ?M:rate(<<"x">>, 42, Prev, 10_000)).

rate_zero_dt_is_zero_test() ->
    %% Guard: Now not strictly after PrevTs => 0 (no div-by-zero).
    Prev = #{<<"x">> => {0, 10_000}},
    ?assertEqual(0, ?M:rate(<<"x">>, 50000, Prev, 10_000)).

%%====================================================================
%% rate_strikes/3 — control-plane only
%%====================================================================

rate_strikes_control_over_limit_accumulates_test() ->
    ?assertEqual(3, ?M:rate_strikes(control, 20_000, 2)).

rate_strikes_control_under_limit_resets_test() ->
    ?assertEqual(0, ?M:rate_strikes(control, 9_000, 5)).

rate_strikes_data_never_accumulates_test() ->
    %% Data-plane procs burn legitimately: the rate rule must not apply.
    ?assertEqual(0, ?M:rate_strikes(data, 5_000_000, 2)).

%%====================================================================
%% mbox_strikes/3 — monotone growth only, above floor
%%====================================================================

mbox_strikes_growth_accumulates_test() ->
    ?assertEqual(4, ?M:mbox_strikes(120, 100, 3)).

mbox_strikes_steady_does_not_accumulate_test() ->
    %% Constant deep mailbox is not a leak — must reset.
    ?assertEqual(0, ?M:mbox_strikes(500, 500, 5)).

mbox_strikes_below_floor_ignored_test() ->
    ?assertEqual(0, ?M:mbox_strikes(40, 10, 2)).

mbox_strikes_shrink_resets_test() ->
    ?assertEqual(0, ?M:mbox_strikes(80, 200, 5)).

%%====================================================================
%% eval_one/3 — edge-triggered alarm state machine
%%====================================================================

%% A control proc held hot fires alarm on the 3rd strike and stays
%% alarmed (single edge), then clears when it cools.
control_rate_alarm_edge_test() ->
    Hot = #{label => <<"peering_router">>, class => control, mbox => 0},
    S0 = ?M:new_strike(),
    S1 = ?M:eval_one(Hot, 50_000, S0),
    ?assertMatch(#{rate := 1, alarmed := false}, S1),
    S2 = ?M:eval_one(Hot, 50_000, S1),
    ?assertMatch(#{rate := 2, alarmed := false}, S2),
    S3 = ?M:eval_one(Hot, 50_000, S2),
    ?assertMatch(#{rate := 3, alarmed := true}, S3),
    S4 = ?M:eval_one(Hot, 50_000, S3),
    ?assertMatch(#{rate := 4, alarmed := true}, S4),
    Cool = #{label => <<"peering_router">>, class => control, mbox => 0},
    S5 = ?M:eval_one(Cool, 10, S4),
    ?assertMatch(#{rate := 0, alarmed := false}, S5).

%% mbox growth on a data proc alarms via the all-class mailbox rule,
%% never via the (control-only) rate rule.
data_mbox_growth_alarms_test() ->
    S = lists:foldl(
          fun(N, Acc) ->
                  Sample = #{label => <<"peer_observer">>, class => data,
                             mbox => 100 + N * 10},
                  ?M:eval_one(Sample, 5_000_000, Acc)
          end, ?M:new_strike(), lists:seq(1, 6)),
    ?assertMatch(#{mbox := 6, alarmed := true}, S).

%%====================================================================
%% evaluate/3 — pruning guards incarnation bridging
%%====================================================================

evaluate_prunes_absent_proc_test() ->
    %% A proc alarmed last tick that is absent this tick must be dropped,
    %% so its strike count cannot bridge into a restarted incarnation.
    Stale = #{<<"peering_router">> => #{rate => 4, mbox => 0,
                                        last_mbox => 0, alarmed => true}},
    Samples = [#{label => <<"dht">>, class => data, mbox => 0,
                 heap_w => 0, reds => 0}],
    Out = ?M:evaluate(Samples, #{<<"dht">> => 0}, Stale),
    ?assertEqual(false, maps:is_key(<<"peering_router">>, Out)),
    ?assert(maps:is_key(<<"dht">>, Out)).

evaluate_keeps_present_proc_counters_test() ->
    Prior = #{<<"peering_router">> => #{rate => 2, mbox => 0,
                                        last_mbox => 0, alarmed => false}},
    Samples = [#{label => <<"peering_router">>, class => control, mbox => 0,
                 heap_w => 0, reds => 0}],
    %% Still hot => strike advances from the retained count to 3 => alarm.
    Out = ?M:evaluate(Samples, #{<<"peering_router">> => 50_000}, Prior),
    ?assertMatch(#{<<"peering_router">> := #{rate := 3, alarmed := true}}, Out).
