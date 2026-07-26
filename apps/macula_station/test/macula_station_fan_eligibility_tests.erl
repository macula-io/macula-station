%%% @doc Fan-out eligibility must track conn worker LIVENESS, not merely the
%%% presence of a conns-table entry.
%%%
%%% Regression cover. `has_live_conn/2' used to answer `true' for any entry at
%%% all (`[{_, _PeerConns}] -> true'). Two states defeat that:
%%%
%%% 1. `peer_observer:empty_peer_conns/0' installs
%%%    `#{inbound => undefined, outbound => undefined}' while a peer is
%%%    (re)connecting. The peer was judged fan-eligible with no worker to send
%%%    through, and `send_event_to_sub/2' then no-opped. The EVENT counted as
%%%    fanned and went nowhere.
%%%
%%% 2. A recorded pid can already be dead. Delivery preferred `inbound' on
%%%    `is_pid/1' alone, so a dead inbound worker was used and a LIVE outbound
%%%    one never tried.
%%%
%%% Both are the normal state during a link flap, not edge cases, which is why
%%% they showed up as correlated bursts of silent loss rather than a steady
%%% leak.
-module(macula_station_fan_eligibility_tests).

-include_lib("eunit/include/eunit.hrl").

-define(CT, macula_station_fan_eligibility_tests_conns).
-define(NODE, <<1:256>>).

%%====================================================================
%% Fixture
%%====================================================================

eligibility_test_() ->
    {foreach,
     fun setup/0,
     fun teardown/1,
     [
      fun absent_entry_is_not_eligible/1,
      fun both_undefined_is_not_eligible/1,
      fun both_dead_is_not_eligible/1,
      fun live_inbound_is_eligible/1,
      fun live_inbound_is_preferred/1,
      fun dead_inbound_falls_back_to_live_outbound/1,
      fun outbound_only_is_eligible/1,
      fun missing_table_is_not_eligible/1
     ]}.

setup() ->
    ets:new(?CT, [named_table, public, set]).

teardown(_Tab) ->
    catch ets:delete(?CT),
    ok.

%%====================================================================
%% Not eligible
%%====================================================================

absent_entry_is_not_eligible(_) ->
    fun() ->
        ?assertNot(macula_station_route_pubsub_frames:fan_eligible(?CT, ?NODE))
    end.

%% THE defect: an entry exists, both directions are undefined.
both_undefined_is_not_eligible(_) ->
    fun() ->
        put_conns(undefined, undefined),
        ?assertNot(macula_station_route_pubsub_frames:fan_eligible(?CT, ?NODE)),
        ?assertEqual(undefined, resolve())
    end.

both_dead_is_not_eligible(_) ->
    fun() ->
        Dead1 = dead_pid(),
        Dead2 = dead_pid(),
        put_conns(Dead1, Dead2),
        ?assert(is_pid(Dead1)),
        ?assertNot(macula_station_route_pubsub_frames:fan_eligible(?CT, ?NODE)),
        ?assertEqual(undefined, resolve())
    end.

missing_table_is_not_eligible(_) ->
    fun() ->
        ?assertNot(macula_station_route_pubsub_frames:fan_eligible(
                     macula_station_fan_eligibility_tests_no_such_table, ?NODE))
    end.

%%====================================================================
%% Eligible
%%====================================================================

live_inbound_is_eligible(_) ->
    fun() ->
        In = live_pid(),
        put_conns(In, undefined),
        ?assert(macula_station_route_pubsub_frames:fan_eligible(?CT, ?NODE)),
        ?assertEqual(In, resolve()),
        stop_pid(In)
    end.

live_inbound_is_preferred(_) ->
    fun() ->
        In  = live_pid(),
        Out = live_pid(),
        put_conns(In, Out),
        ?assertEqual(In, resolve()),
        stop_pid(In),
        stop_pid(Out)
    end.

%% The improvement: a dead inbound no longer shadows a usable outbound.
dead_inbound_falls_back_to_live_outbound(_) ->
    fun() ->
        Out = live_pid(),
        put_conns(dead_pid(), Out),
        ?assert(macula_station_route_pubsub_frames:fan_eligible(?CT, ?NODE)),
        ?assertEqual(Out, resolve()),
        stop_pid(Out)
    end.

outbound_only_is_eligible(_) ->
    fun() ->
        Out = live_pid(),
        put_conns(undefined, Out),
        ?assert(macula_station_route_pubsub_frames:fan_eligible(?CT, ?NODE)),
        ?assertEqual(Out, resolve()),
        stop_pid(Out)
    end.

%%====================================================================
%% Helpers
%%====================================================================

put_conns(In, Out) ->
    true = ets:insert(?CT, {?NODE, #{inbound => In, outbound => Out}}),
    ok.

resolve() ->
    macula_station_route_pubsub_frames:conn_pid(
      {ok, conns_of(ets:lookup(?CT, ?NODE))}).

conns_of([{_, Conns}]) -> Conns;
conns_of([])           -> #{inbound => undefined, outbound => undefined}.

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
