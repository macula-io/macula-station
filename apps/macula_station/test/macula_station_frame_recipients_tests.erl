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
