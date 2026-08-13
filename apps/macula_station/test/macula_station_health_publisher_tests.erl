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

%%====================================================================
%% Wire tripwire — the milan 2026-08-13 failure
%%
%% Both /proc/net/udp6 rows below are REAL kernel output, not invented.
%% Row 1 was captured on this workstation by opening a UDP6 socket with
%% {recbuf, 2048}, blasting 40 x 1200 bytes at it and never reading:
%% rx_queue 0x900 = 2304 bytes backed up, 39 drops. Row 2 is the live
%% listener of station-se-stockholm on port 4433 (0x1151), idle.
%%
%% An invented line would prove nothing about the parser, and the parser
%% is the part that stands between this rule and a permanently-green
%% alarm.
%%====================================================================

-define(UDP6, <<
  "  sl  local_address                         remote_address                    "
  "    st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops\n"
  "10417: 00000000000000000000000000000000:9D3E 00000000000000000000000000000000:0000 "
  "07 00000000:00000900 00:00000000 00000000  1000        0 5529288 2 0000000035a448ac 39\n"
  "   85: 093C002600000000FF6E00205450B6FE:1151 00000000000000000000000000000000:0000 "
  "07 00000000:00000000 00:00000000 00000000     0        0 2299435 2 ffff8c3183be1500 0\n"
>>).

port_hex_pads_to_four_test() ->
    ?assertEqual(<<"1151">>, ?M:port_hex(4433)),
    ?assertEqual(<<"9D3E">>, ?M:port_hex(40254)),
    ?assertEqual(<<"0035">>, ?M:port_hex(53)).

%%--- T1: parse_udp6/2 -------------------------------------------------

parse_udp6_reads_backlogged_row_test() ->
    ?assertEqual({ok, #{tx_queue => 0, rx_queue => 2304, drops => 39}},
                 ?M:parse_udp6({ok, ?UDP6}, 40254)).

parse_udp6_reads_idle_station_row_test() ->
    ?assertEqual({ok, #{tx_queue => 0, rx_queue => 0, drops => 0}},
                 ?M:parse_udp6({ok, ?UDP6}, 4433)).

parse_udp6_absent_port_is_not_found_test() ->
    %% A real answer: the socket is not bound.
    ?assertEqual(not_found, ?M:parse_udp6({ok, ?UDP6}, 9999)).

parse_udp6_unreadable_is_unavailable_not_zero_test() ->
    %% THE load-bearing case. A non-Linux box, or an unreadable procfs,
    %% must never look like an empty queue — that would make the rule
    %% permanently green everywhere it cannot see.
    ?assertEqual(unavailable, ?M:parse_udp6({error, enoent}, 4433)).

%%--- T2: the milan replay, no network at all --------------------------

%% Drive N identical stalled samples through the pure state machine.
run_wire(Samples) ->
    lists:foldl(fun(S, W) -> ?M:wire_step(S, W) end, ?M:new_wire(), Samples).

stalled_sample() -> #{rx_queue => 8192, frames => 4711}.

wire_milan_replay_fires_at_thirty_strikes_test() ->
    %% Backlog non-empty and never shrinking, dispatched frames frozen.
    %% This is exactly what milan held for 10,800 samples.
    %%
    %% A strike is an INTERVAL, not a sample: the first sample only
    %% establishes the baseline, so N+1 samples yield N strikes. 30
    %% strikes is 30 ticks is 5 minutes of observed stall.
    ?assertMatch(#{verdict := ok, strikes := 29},
                 run_wire(lists:duplicate(30, stalled_sample()))),
    ?assertMatch(#{verdict := stalled, strikes := 30, alarmed := true},
                 run_wire(lists:duplicate(31, stalled_sample()))).

wire_first_sample_never_strikes_test() ->
    %% No prior sample means no delta to judge.
    ?assertMatch(#{verdict := ok, strikes := 0},
                 run_wire([stalled_sample()])).

%%--- T3-T5: the states that must stay green ---------------------------

wire_idle_station_is_ok_test() ->
    %% Empty queue, frames flat. Nothing is arriving; nothing is owed.
    Idle = #{rx_queue => 0, frames => 100},
    ?assertMatch(#{verdict := ok, strikes := 0},
                 run_wire(lists:duplicate(50, Idle))).

wire_busy_station_with_oscillating_queue_is_ok_test() ->
    Samples = [#{rx_queue => Q, frames => 1000 + N}
               || {Q, N} <- lists:zip(
                              lists:flatten(lists:duplicate(10, [0, 65536, 128, 4096, 0])),
                              lists:seq(1, 50))],
    ?assertMatch(#{verdict := ok}, run_wire(Samples)).

wire_slow_drain_with_advancing_frames_is_ok_test() ->
    %% Deep AND non-decreasing backlog, but the station is dispatching.
    %% The consequent holds, so the invariant holds. This is the clause
    %% that separates "overloaded" from "wedged", and getting it wrong
    %% would halt the busiest station on the fleet.
    Samples = [#{rx_queue => 65536, frames => N} || N <- lists:seq(1, 60)],
    ?assertMatch(#{verdict := ok, strikes := 0}, run_wire(Samples)).

wire_draining_queue_never_strikes_test() ->
    Samples = [#{rx_queue => Q, frames => 500}
               || Q <- lists:seq(60, 1, -1)],
    ?assertMatch(#{verdict := ok, strikes := 0}, run_wire(Samples)).

%%--- T6: counter reset ------------------------------------------------

wire_frame_counter_reset_does_not_strike_test() ->
    %% POST /telemetry/frames/reset sends the total backwards. A reset is
    %% not evidence of a stall; clamp exactly as rate/4 does.
    Before = lists:duplicate(20, stalled_sample()),
    After  = [#{rx_queue => 8192, frames => 0}],
    W = lists:foldl(fun(S, Acc) -> ?M:wire_step(S, Acc) end,
                    ?M:new_wire(), Before ++ After),
    ?assertMatch(#{verdict := ok, strikes := 0}, W).

%%--- T7: unavailable is a third state ---------------------------------

wire_unavailable_is_unknown_and_resets_strikes_test() ->
    %% A macOS dev box, or a station that cannot read its own socket.
    %% Must not accumulate toward a verdict it cannot justify.
    Primed = run_wire(lists:duplicate(30, stalled_sample())),
    W = ?M:wire_step(unavailable, Primed),
    ?assertMatch(#{verdict := unknown, strikes := 0, prev := undefined}, W).

wire_checks_advance_even_when_unavailable_test() ->
    %% A detector with nothing to detect and a detector that cannot run
    %% look identical from outside unless it counts its own passes.
    W = lists:foldl(fun(S, Acc) -> ?M:wire_step(S, Acc) end,
                    ?M:new_wire(), lists:duplicate(7, unavailable)),
    ?assertMatch(#{checks := 7, verdict := unknown}, W).

%%--- recovery ---------------------------------------------------------

wire_recovers_on_falling_edge_test() ->
    Stalled = run_wire(lists:duplicate(31, stalled_sample())),
    ?assertMatch(#{alarmed := true}, Stalled),
    Recovered = ?M:wire_step(#{rx_queue => 0, frames => 4712}, Stalled),
    ?assertMatch(#{verdict := ok, alarmed := false, strikes := 0}, Recovered).

%%--- the dispatch-phase fold -----------------------------------------

sum_dispatch_counts_only_dispatch_self_test() ->
    %% forward_send times a gen_statem:cast that returns ok to a dead
    %% pid — it climbed for all thirty hours of the milan outage. Folding
    %% it here would rebuild the blindness this rule exists to remove.
    Rows = [{{publish, dispatch_self, below_1ms}, 10},
            {{publish, forward_send,  below_1ms}, 9999},
            {{call,    dispatch_self, below_5ms}, 5},
            {{call,    recv_to_dispatch, below_1ms}, 777}],
    ?assertEqual(15, lists:foldl(fun ?M:sum_dispatch/2, 0, Rows)).

%%====================================================================
%% End to end against a REAL backlogged socket
%%
%% The fixture above proves the parser against known bytes. It cannot
%% prove that the port match finds OUR row in a live kernel's table,
%% among every other socket on the machine — and that match is the one
%% step between this rule and watching the wrong socket forever.
%%
%% So: open a UDP6 socket with a deliberately tiny receive buffer, blast
%% it, never read it, and require the parser to find that exact port
%% carrying a non-zero backlog. Skips on any host without procfs rather
%% than failing, since the rule itself degrades to `unknown' there.
%%====================================================================

live_backlogged_socket_is_observed_test_() ->
    {timeout, 30, fun live_backlogged_socket/0}.

live_backlogged_socket() ->
    maybe_run_live(file:read_file("/proc/net/udp6")).

maybe_run_live({error, _NoProcfs}) ->
    %% Not Linux. The rule reports `unknown' here by design.
    ok;
maybe_run_live({ok, _}) ->
    {ok, S} = gen_udp:open(0, [inet6, binary, {active, false}, {recbuf, 2048}]),
    {ok, Port} = inet:port(S),
    ok = blast(Port, 40),
    timer:sleep(300),
    Parsed = ?M:parse_udp6(file:read_file("/proc/net/udp6"), Port),
    gen_udp:close(S),
    assert_backlog(Parsed, Port).

blast(Port, N) ->
    {ok, T} = gen_udp:open(0, [inet6, binary, {active, false}]),
    Payload = binary:copy(<<"x">>, 1200),
    [ok = gen_udp:send(T, {0,0,0,0,0,0,0,1}, Port, Payload)
     || _ <- lists:seq(1, N)],
    gen_udp:close(T).

assert_backlog({ok, #{rx_queue := Rx}}, _Port) when Rx > 0 ->
    ok;
assert_backlog(Other, Port) ->
    %% Loud on purpose: a silent pass here would mean the tripwire is
    %% watching a socket that is not ours.
    erlang:error({expected_backlog_on_port, Port, got, Other}).
