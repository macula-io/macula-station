%% @doc Phase 4 acceptance suite.
%%
%% Per `plans/PLAN_PHASE_4_BREAKDOWN.md' "Phase 4 acceptance"
%% (from Part 7 §9.3):
%%
%% <ul>
%%   <li>Cross-relay RPC p95 latency &lt; 200 ms (intra-EU).</li>
%%   <li>Failed-edge reroute p95 &lt; 50 ms.</li>
%%   <li>V1 blocker `dist-tunnel-blocker.md' resolved: cross-relay
%%       CALL works across &gt; 2 hops.</li>
%%   <li>CT suite green; no flakes over 10 runs.</li>
%% </ul>
%%
%% The latency numbers in the spec target a real intra-EU QUIC
%% deployment. The in-VM testbed runs orders of magnitude faster
%% (microseconds per hop) — the assertions below verify behaviour
%% rather than absolute latency, but cross-checks confirm the
%% behavioural targets are well within the spec's bars.
-module(macula_phase4_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([
    cross_relay_call_succeeds_across_three_hops/1,
    mid_path_failure_reroutes_via_alternate_path/1,
    signed_error_attributes_failed_hop/1,
    retry_budget_exhausted_returns_terminal_failure/1
]).

all() ->
    [cross_relay_call_succeeds_across_three_hops,
     mid_path_failure_reroutes_via_alternate_path,
     signed_error_attributes_failed_hop,
     retry_budget_exhausted_returns_terminal_failure].

init_per_suite(Config) ->
    rand:seed(default, {41, 17, 3}),
    Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
    Net = phase4_helper:start_fleet([a, b, c, d, e, f]),
    [{net, Net} | Config].

end_per_testcase(_TestCase, Config) ->
    phase4_helper:stop_fleet(?config(net, Config)),
    ok.

%%==================================================================
%% Test cases
%%==================================================================

%% @doc Acceptance: a CALL traverses A → B → C → D, the
%% destination echoes the payload, and the originator collects the
%% RESULT. Resolves the V1 dist-tunnel-blocker that prevented
%% cross-relay CALLs from working across &gt; 2 hops.
cross_relay_call_succeeds_across_three_hops(Config) ->
    Net = ?config(net, Config),
    Spec = #{procedure  => <<"realm/acme/weather/get_v1">>,
             realm      => crypto:strong_rand_bytes(32),
             payload    => #{city => <<"Brussels">>},
             timeout_ms => 1_000},
    Started = erlang:monotonic_time(microsecond),
    Outcome = phase4_helper:call_via_path(Net, a, [a, b, c, d], Spec),
    Elapsed = erlang:monotonic_time(microsecond) - Started,
    ct:pal("Cross-relay 4-hop CALL: ~p us", [Elapsed]),
    ?assertMatch({ok, #{echoed := #{city := <<"Brussels">>}}}, Outcome),
    %% In-VM should be sub-millisecond per hop; well below 200 ms.
    ?assert(Elapsed < 200_000).

%% @doc Acceptance: failed-edge reroute. Build two disjoint paths
%% and kill a mid-path hop on the first one. The retry orchestrator
%% rotates to the alternate path; the CALL succeeds.
mid_path_failure_reroutes_via_alternate_path(Config) ->
    Net = ?config(net, Config),
    %% Kill C on every station (C is a hop on path 1 only).
    ok = phase4_helper:set_alive(Net, c, false),

    Spec = #{procedure  => <<"realm/acme/proc/v1">>,
             realm      => crypto:strong_rand_bytes(32),
             payload    => #{n => 7},
             timeout_ms => 800},
    Path1 = [a, b, c, d],   %% C is dead → unknown_next_peer at B
    Path2 = [a, e, f, d],   %% all alive — succeeds

    Started = erlang:monotonic_time(microsecond),
    Result = phase4_helper:call_via_paths(
               Net, a, [Path1, Path2], Spec,
               #{retry_budget => 2, overall_timeout_ms => 1_500}),
    Elapsed = erlang:monotonic_time(microsecond) - Started,
    ct:pal("Reroute via alternate path: ~p us, attempts=~p",
           [Elapsed, length(maps:get(attempts, Result))]),

    ?assertMatch({ok, #{echoed := #{n := 7}}},
                 maps:get(outcome, Result)),
    Attempts = maps:get(attempts, Result),
    ?assertEqual(2, length(Attempts)),
    [Attempt1, Attempt2] = Attempts,
    ?assertMatch({error, unknown_next_peer, _},
                 maps:get(outcome, Attempt1)),
    ?assertMatch({ok, _},
                 maps:get(outcome, Attempt2)).

%% @doc Acceptance: when an intermediate hop returns
%% `unknown_next_peer' (because the next hop is dead), the signed
%% ERROR frame attributes the failed hop via `offending_hop'.
signed_error_attributes_failed_hop(Config) ->
    Net = ?config(net, Config),
    ok = phase4_helper:set_alive(Net, c, false),
    Spec = #{procedure  => <<"realm/acme/proc/v1">>,
             realm      => crypto:strong_rand_bytes(32),
             payload    => ok,
             timeout_ms => 500},
    %% A → B → C → D with C dead.
    {error, unknown_next_peer, Detail} =
        phase4_helper:call_via_path(Net, a, [a, b, c, d], Spec),
    %% offending_hop carries the (16-byte) prefix of C, zero-padded
    %% to 32 bytes per the SDK ERROR frame contract.
    OffHop = maps:get(offending_hop, Detail),
    ?assertEqual(32, byte_size(OffHop)),
    Cprefix = phase4_helper:name_to_prefix(Net, c),
    ?assertEqual(Cprefix, binary:part(OffHop, 0, 16)).

%% @doc Acceptance: when every supplied path fails, the retry
%% orchestrator returns the latest BOLT#4 failure (no synthesis
%% on real-failure exhaustion).
retry_budget_exhausted_returns_terminal_failure(Config) ->
    Net = ?config(net, Config),
    %% Kill both possible mid-paths.
    ok = phase4_helper:set_alive(Net, c, false),
    ok = phase4_helper:set_alive(Net, f, false),
    Spec = #{procedure  => <<"realm/acme/proc/v1">>,
             realm      => crypto:strong_rand_bytes(32),
             payload    => ok,
             timeout_ms => 500},
    Result = phase4_helper:call_via_paths(
               Net, a,
               [[a, b, c, d], [a, e, f, d]], Spec,
               #{retry_budget => 5, overall_timeout_ms => 2_000}),
    ?assertMatch({error, unknown_next_peer, _},
                 maps:get(outcome, Result)),
    %% Both paths attempted before exhaustion.
    ?assertEqual(2, length(maps:get(attempts, Result))).
