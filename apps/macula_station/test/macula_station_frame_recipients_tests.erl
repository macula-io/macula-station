%%% @doc The station must hand its peering workers REGISTERED NAMES for the
%%% DHT and pubsub frame-bypass recipients, never pids.
%%%
%%% Regression cover. Both call sites used to pass `whereis(Name)', which
%%% captures a pid once for the life of the peering connection. Two failures
%%% followed from that, and both were silent:
%%%
%%% 1. A recipient crash-restart stranded the connection. The SDK guarded the
%%%    bypass with `is_pid/1', which is true for a DEAD pid, so every frame was
%%%    posted into a dead mailbox and discarded by the VM. No error at either
%%%    end, no `disconnected' event, no reconnect — the connection stayed
%%%    pubsub- and DHT-silent until it was torn down.
%%%
%%% 2. The bypass depended on a boot race. A connection accepted or dialled
%%%    before the recipient registered got no bypass at all, for its whole life.
%%%
%%% macula >= 7.1.0 re-resolves a registered name on every frame, so passing
%%% the name fixes both. These tests assert the name is what we pass; the SDK's
%%% `macula_peering_recipient_tests' covers the resolution behaviour itself.
-module(macula_station_frame_recipients_tests).

-include_lib("eunit/include/eunit.hrl").

-define(DHT_NAME,    macula_dht).
-define(PUBSUB_NAME, macula_station_route_pubsub_frames).

%%====================================================================
%% Accept side
%%====================================================================

accept_side_passes_registered_names_test() ->
    Opts = macula_station_listener:peering_opts(listener_opts()),
    assert_named_recipients(Opts).

%%====================================================================
%% Dial side
%%====================================================================

dial_side_passes_registered_names_test() ->
    {ok, Opts} = macula_station:compose_dial({ok, self()}, dial_template()),
    assert_named_recipients(Opts).

%%====================================================================
%% Shared assertions
%%====================================================================

%% Both recipients must be the registered names, and must be atoms. The
%% `is_atom' assertions are the load-bearing ones: a pid here reintroduces
%% the stranded-connection bug even though the map key is still present.
assert_named_recipients(Opts) ->
    ?assertEqual(?DHT_NAME,    maps:get(dht_recipient, Opts)),
    ?assertEqual(?PUBSUB_NAME, maps:get(pubsub_recipient, Opts)),
    ?assert(is_atom(maps:get(dht_recipient, Opts))),
    ?assert(is_atom(maps:get(pubsub_recipient, Opts))),
    ?assertNot(is_pid(maps:get(dht_recipient, Opts))),
    ?assertNot(is_pid(maps:get(pubsub_recipient, Opts))).

%% Names must be passed unconditionally — not gated on the recipient being
%% registered right now. That gate was failure mode 2 above.
names_do_not_depend_on_registration_test() ->
    ?assertEqual(undefined, whereis(?PUBSUB_NAME)),
    ?assertEqual(undefined, whereis(?DHT_NAME)),
    Accept = macula_station_listener:peering_opts(listener_opts()),
    {ok, Dial} = macula_station:compose_dial({ok, self()}, dial_template()),
    ?assertEqual(?PUBSUB_NAME, maps:get(pubsub_recipient, Accept)),
    ?assertEqual(?PUBSUB_NAME, maps:get(pubsub_recipient, Dial)).

%%====================================================================
%% Helpers
%%====================================================================

listener_opts() ->
    #{identity     => macula_identity:generate(),
      realms       => [],
      capabilities => 0,
      observer     => self()}.

dial_template() ->
    #{identity => macula_identity:generate()}.

%%====================================================================
%% ⚠ THE OBSERVER, WHICH WAS THE ONE RECIPIENT LEFT AS A CAPTURED PID
%%====================================================================
%%
%% `dht_recipient' and `pubsub_recipient' became registered names on
%% 2026-07-26 because a captured pid is a black hole after the recipient
%% restarts: `Pid ! Msg' to a dead pid succeeds and the VM discards it, with
%% no error at either end. `controlling_pid' kept the bug.
%%
%% On 2026-08-05 station-de-frankfurt's peer_observer died at 06:30:54. The
%% listener's opts map, built once at its own init, went on handing the dead
%% pid to every accept. Those connections announced themselves to nobody,
%% never entered the conns ETS mirror, and pubsub fan-out — which resolves
%% subscribers through that mirror — delivered nothing to any of them for
%% hours, while `/health' reported healthy.

the_observer_is_resolved_at_accept_not_captured_at_init_test() ->
    Dead = dead_pid(),
    Live = spawn(fun() -> receive stop -> ok end end),
    true = register(macula_station_peer_observer, Live),
    try
        Opts = macula_station_listener:peering_opts(
                 maps:put(observer, Dead, listener_opts())),
        ?assertEqual(Live, maps:get(controlling_pid, Opts))
    after
        catch unregister(macula_station_peer_observer),
        Live ! stop
    end.

%% The restart window: for the instant in which no observer is registered,
%% the configured value is still handed over rather than `undefined', which
%% would crash the peering worker's init. A connection accepted in that
%% window is recovered by the observer's own `reconcile_inbound_accepts/1'.
the_configured_observer_is_the_fallback_when_the_name_is_free_test() ->
    catch unregister(macula_station_peer_observer),
    Configured = self(),
    Opts = macula_station_listener:peering_opts(
             maps:put(observer, Configured, listener_opts())),
    ?assertEqual(Configured, maps:get(controlling_pid, Opts)).

dead_pid() ->
    P = spawn(fun() -> ok end),
    wait_gone(P),
    P.

wait_gone(P) ->
    case is_process_alive(P) of
        false -> ok;
        true  -> timer:sleep(5), wait_gone(P)
    end.
