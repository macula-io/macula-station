%%% @doc Every branch that ends an EVENT's journey must be counted.
%%%
%%% Before this, three of them returned `ok' with no log at all (empty target
%%% list, target with no live worker, registry-refused PUBLISH) and the two
%%% duplicate-drop branches logged at `debug', off in production. So the
%%% branches that LOSE a message were the only ones producing no evidence,
%%% which is why repeated "multi-hop feels flaky" investigations could not
%%% distinguish a routing fault from a dedup drop from a stale conn.
%%%
%%% Counters are cumulative and global (`persistent_term' + `counters', the
%%% idiom `macula_station_event_dedup' already uses) and deliberately survive a
%%% restart of the module, so these tests assert DELTAS rather than absolutes.
-module(macula_station_delivery_outcomes_tests).

-include_lib("eunit/include/eunit.hrl").

-define(CT, macula_station_delivery_outcomes_tests_conns).
-define(NODE, <<7:256>>).

%%====================================================================
%% Fixture
%%====================================================================

outcomes_test_() ->
    {foreach,
     fun setup/0,
     fun teardown/1,
     [
      fun stats_expose_every_slot/1,
      fun empty_target_list_counts_no_targets/1,
      fun successful_fan_does_not_count_no_targets/1,
      fun dead_target_counts_no_live_conn/1,
      fun live_target_counts_forwarded/1
     ]}.

setup() ->
    macula_station_route_pubsub_frames:install_counters(),
    ets:new(?CT, [named_table, public, set]).

teardown(_Tab) ->
    catch ets:delete(?CT),
    ok.

%%====================================================================
%% Tests
%%====================================================================

stats_expose_every_slot(_) ->
    fun() ->
        Stats = stats(),
        Expected = [dup_dropped, dup_dropped_pre, forwarded, no_bloom_match,
                    no_live_conn, no_targets, relay_publish_err,
                    unauth_publisher],
        ?assertEqual(Expected, lists:sort(maps:keys(Stats))),
        ?assert(lists:all(fun(V) -> is_integer(V) andalso V >= 0 end,
                          maps:values(Stats)))
    end.

%% Nothing to fan to at all.
empty_target_list_counts_no_targets(_) ->
    fun() ->
        Before = stats(),
        macula_station_route_pubsub_frames:fan_out_event(frame(), [], ?CT),
        ?assertEqual(1, delta(no_targets, Before, stats()))
    end.

%% The counter must NOT fire at the end of a successful fan. It used to be
%% tempting to count in the recursion's base clause, which fires every time
%% the list is exhausted and would have reported every delivery as a loss.
successful_fan_does_not_count_no_targets(_) ->
    fun() ->
        Pid = live_pid(),
        put_conns(Pid, undefined),
        Before = stats(),
        macula_station_route_pubsub_frames:fan_out_event(frame(), [?NODE], ?CT),
        ?assertEqual(0, delta(no_targets, Before, stats())),
        stop_pid(Pid)
    end.

%% Believed reachable, no live worker: a lost event, previously silent.
dead_target_counts_no_live_conn(_) ->
    fun() ->
        put_conns(dead_pid(), undefined),
        Before = stats(),
        macula_station_route_pubsub_frames:fan_out_event(frame(), [?NODE], ?CT),
        After = stats(),
        ?assertEqual(1, delta(no_live_conn, Before, After)),
        ?assertEqual(0, delta(forwarded, Before, After))
    end.

live_target_counts_forwarded(_) ->
    fun() ->
        Pid = live_pid(),
        put_conns(Pid, undefined),
        Before = stats(),
        macula_station_route_pubsub_frames:fan_out_event(frame(), [?NODE], ?CT),
        After = stats(),
        ?assertEqual(1, delta(forwarded, Before, After)),
        ?assertEqual(0, delta(no_live_conn, Before, After)),
        stop_pid(Pid)
    end.

%%====================================================================
%% Helpers
%%====================================================================

stats() ->
    macula_station_route_pubsub_frames:delivery_stats().

delta(Key, Before, After) ->
    maps:get(Key, After) - maps:get(Key, Before).

%% Only `topic' is read on this path (bloom-fan skips `_mesh.*'); a plain map
%% is enough and avoids signing a real frame.
frame() ->
    #{topic => <<"io.macula/test/delivery_outcomes">>}.

put_conns(In, Out) ->
    true = ets:insert(?CT, {?NODE, #{inbound => In, outbound => Out}}),
    ok.

live_pid() ->
    spawn(fun idle/0).

idle() ->
    receive stop -> ok end.

stop_pid(Pid) ->
    Pid ! stop,
    ok.

dead_pid() ->
    Pid = spawn(fun() -> ok end),
    Ref = erlang:monitor(process, Pid),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 1000 -> erlang:demonitor(Ref, [flush]), exit({pid_not_dying, Pid})
    end,
    Pid.
