%%% @doc Station wiring must hand consumers a REGISTERED NAME for the
%%% singleton registries, never a pid captured at child-spec time.
%%%
%%% The bug this covers. `macula_station_app' built child-spec options with
%%% `whereis(hecate_pubsub_registry)' (and the handler / remote-advertise
%%% registries) in eleven places. That captures a pid ONCE, at boot. The
%%% registries are siblings under a `one_for_one' supervisor, so any of them can
%%% restart alone, and `macula_station_sup:ensure_registered/2' then re-points
%%% the NAME at the new pid. But every consumer still held the old pid, and a
%%% supervisor reuses the ORIGINAL child spec on restart, so not even restarting
%%% the consumer would pick up the new pid. The station would keep calling a
%%% dead process for the rest of its life.
%%%
%%% This is the same defect class as the SDK's captured `pubsub_recipient'
%%% (fixed in macula 7.1.0), one layer further in.
-module(macula_station_registry_handle_tests).

-include_lib("eunit/include/eunit.hrl").

-define(NAME, hecate_pubsub_registry).

%%====================================================================
%% The regression
%%====================================================================

name_survives_a_registry_restart_test() ->
    Old = start_registry(),
    ?assertEqual([], hecate_pubsub_registry:list_realms(?NAME)),

    stop_registry(Old),
    New = start_registry(),
    ?assertNotEqual(Old, New),

    %% The NAME still answers, now on the new pid. This is what the fix buys.
    ?assertEqual([], hecate_pubsub_registry:list_realms(?NAME)),
    ?assertEqual(New, whereis(?NAME)),

    %% The captured pid is the pre-fix behaviour and is permanently dead. Not
    %% "slow" or "degraded": every call exits, forever, with no recovery path.
    ?assertExit({noproc, _}, hecate_pubsub_registry:list_realms(Old)),

    stop_registry(New).

%% A name that is momentarily unregistered must fail loudly at the call site
%% rather than silently, so a boot race is visible instead of dropping traffic.
unregistered_name_exits_test() ->
    ?assertEqual(undefined, whereis(?NAME)),
    ?assertExit({noproc, _}, hecate_pubsub_registry:list_realms(?NAME)).

%%====================================================================
%% Helpers
%%====================================================================

%% Mirrors macula_station_sup: start unregistered, then bind the name, so a
%% restart re-points the name exactly as ensure_registered/2 does.
start_registry() ->
    {ok, Pid} = hecate_pubsub_registry:start_link(#{}),
    _ = catch unregister(?NAME),
    true = register(?NAME, Pid),
    Pid.

stop_registry(Pid) ->
    Ref = erlang:monitor(process, Pid),
    _ = catch unregister(?NAME),
    unlink(Pid),
    exit(Pid, shutdown),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 2000 -> erlang:demonitor(Ref, [flush]), exit({registry_not_stopping, Pid})
    end.
