%% EUnit tests for `macula_station_record_fanout'.
%%
%% Mirrors the classification surface previously covered by
%% `macula_station_fact_publisher_tests'. The fan-out replaces the
%% per-identity publisher with a node-singleton that publishes onto
%% the station's `hecate_pubsub_server' for the mesh-realm.
%%
%% We drive the fan-out in test mode: `start_link/1' takes the wiring
%% map directly so the test never needs the supervisor's
%% name-registered singleton.
-module(macula_station_record_fanout_tests).

-include_lib("eunit/include/eunit.hrl").

-define(MESH_REALM, <<0:256>>).

%%====================================================================
%% Fixture
%%====================================================================

setup() ->
    process_flag(trap_exit, true),
    Identity  = macula_identity:generate(),
    {ok, Reg} = hecate_pubsub_registry:start_link(#{identity => Identity}),
    unlink(Reg),
    {ok, Obs} = stub_observer:start_link(),
    unlink(Obs),
    Wiring = #{
        pubsub_registry => Reg,
        peer_observer   => Obs,
        identity        => Identity
    },
    {ok, Pub} = macula_station_record_fanout:start_link(Wiring),
    unlink(Pub),
    #{publisher => Pub, registry => Reg, observer => Obs,
      identity  => Identity}.

cleanup(#{publisher := Pub, registry := Reg, observer := Obs}) ->
    catch macula_station_record_fanout:stop(Pub),
    catch hecate_pubsub_registry:stop(Reg),
    catch stub_observer:stop(Obs),
    ok.

%%====================================================================
%% Generators
%%====================================================================

classification_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun(Ctx) ->
             {timeout, 30,
              fun() -> station_record_lands_on_station_topic(Ctx) end}
         end,
         fun(Ctx) ->
             {timeout, 30,
              fun() -> daemon_record_lands_on_daemon_topic(Ctx) end}
         end,
         fun(Ctx) ->
             {timeout, 30,
              fun() -> wire_decoded_daemon_record_lands_on_daemon_topic(Ctx) end}
         end,
         fun(Ctx) ->
             {timeout, 30,
              fun() -> unknown_record_type_is_ignored(Ctx) end}
         end,
         fun(Ctx) ->
             {timeout, 30,
              fun() -> unlabeled_record_defaults_to_daemon_topic(Ctx) end}
         end,
         fun(Ctx) ->
             {timeout, 30,
              fun() -> test_daemon_record_is_never_announced(Ctx) end}
         end,
         fun(Ctx) ->
             {timeout, 30,
              fun() -> test_station_record_is_never_announced(Ctx) end}
         end
     ]}.

%%====================================================================
%% Cases — classification surface
%%====================================================================

station_record_lands_on_station_topic(Ctx) ->
    assert_publish_lands(Ctx,
                        station_record(),
                        <<"_mesh.station.announced_v1">>).

daemon_record_lands_on_daemon_topic(Ctx) ->
    assert_publish_lands(Ctx,
                        daemon_record(),
                        <<"_mesh.daemon.announced_v1">>).

%% Same regression covered by the legacy fact_publisher tests:
%% wire-decoded daemon records have atomized keys but tagged-text
%% values. `payload_kind/1' must accept all three variants.
wire_decoded_daemon_record_lands_on_daemon_topic(Ctx) ->
    assert_publish_lands(Ctx,
                        wire_atomized_daemon_record(),
                        <<"_mesh.daemon.announced_v1">>).

%% Regression for the inverted-default bug (2026-08-20): a record with no
%% `kind' field at all — the SDK's own peer default — must land on the
%% DAEMON topic, not the station one. Real stations always stamp an
%% explicit `kind = station' (`macula_station_announcer:node_record_opts/1');
%% anything unlabeled is an ordinary daemon.
unlabeled_record_defaults_to_daemon_topic(Ctx) ->
    assert_publish_lands(Ctx,
                        unlabeled_record(),
                        <<"_mesh.daemon.announced_v1">>).

%% `kind = test_daemon' / `test_station' must never reach either public
%% presence topic — see `maybe_publish_presence/3'. A realm dashboard's
%% "N nodes dialled in" counter was mostly counting these before the fix
%% (macula-e2e's own throwaway DHT round-trip fixtures, tagged this way
%% 2026-08-21 for exactly this reason).
test_daemon_record_is_never_announced(Ctx) ->
    assert_publish_does_not_land(Ctx, test_daemon_record(),
                                 [<<"_mesh.station.announced_v1">>,
                                  <<"_mesh.daemon.announced_v1">>]).

test_station_record_is_never_announced(Ctx) ->
    assert_publish_does_not_land(Ctx, test_station_record(),
                                 [<<"_mesh.station.announced_v1">>,
                                  <<"_mesh.daemon.announced_v1">>]).

unknown_record_type_is_ignored(#{publisher := Pub, registry := Reg,
                                 identity := PubIdentity}) ->
    {ok, Server} = hecate_pubsub_registry:register(Reg, ?MESH_REALM, PubIdentity),
    SubKp        = macula_identity:generate(),
    SubId        = macula_identity:public(SubKp),
    ok = hecate_pubsub_server:subscribe(Server, <<"_mesh.station.announced_v1">>, SubId),
    ok = hecate_pubsub_server:subscribe(Server, <<"_mesh.daemon.announced_v1">>,  SubId),
    FakeRecord = #{type => 16#33, payload => #{},
                   signature => <<0:512>>, key => <<0:256>>},
    macula_station_record_fanout:on_record(Pub, FakeRecord),
    receive
        {'$gen_cast', {send_frame, _}} ->
            erlang:error(unknown_type_fired_event)
    after 200 -> ok
    end.

%%====================================================================
%% Helpers
%%====================================================================

assert_publish_lands(#{publisher := Pub, registry := Reg, observer := Obs,
                       identity := PubIdentity},
                     Record, ExpectedTopic) ->
    {ok, Server} = hecate_pubsub_registry:register(Reg, ?MESH_REALM, PubIdentity),
    SubKp        = macula_identity:generate(),
    SubId        = macula_identity:public(SubKp),
    ok = hecate_pubsub_server:subscribe(Server, ExpectedTopic, SubId),
    ok = stub_observer:register_conn(Obs, SubId, self()),
    macula_station_record_fanout:on_record(Pub, Record),
    Got = receive_event(ExpectedTopic, 5_000),
    case Got of
        ExpectedTopic -> ok;
        Other ->
            Diagnostic = #{
                expected_topic => ExpectedTopic,
                got            => Other,
                record_type    => maps:get(type, Record, undefined),
                payload        => maps:get(payload, Record, undefined),
                server_topics  => hecate_pubsub_server:topics(Server),
                server_subs    =>
                    hecate_pubsub_server:subscribers(Server, ExpectedTopic)
            },
            erlang:error({event_not_fanned_out, Diagnostic})
    end.

receive_event(_Expected, Timeout) ->
    receive
        {'$gen_cast', {send_frame, #{frame_type := event, topic := T}}} -> T
    after Timeout -> timeout
    end.

%% Subscribes to every topic in `ForbiddenTopics', fires the record, and
%% fails if ANY of them ever receives an event. 300ms, not 5s: this
%% asserts an absence, so it only needs to be long enough that a real
%% publish (which lands well under 200ms in `assert_publish_lands/3'
%% above) would have shown up by now.
assert_publish_does_not_land(#{publisher := Pub, registry := Reg,
                               observer := Obs, identity := PubIdentity},
                             Record, ForbiddenTopics) ->
    {ok, Server} = hecate_pubsub_registry:register(Reg, ?MESH_REALM, PubIdentity),
    SubKp        = macula_identity:generate(),
    SubId        = macula_identity:public(SubKp),
    [ok = hecate_pubsub_server:subscribe(Server, T, SubId)
     || T <- ForbiddenTopics],
    ok = stub_observer:register_conn(Obs, SubId, self()),
    macula_station_record_fanout:on_record(Pub, Record),
    case receive_event(ForbiddenTopics, 300) of
        timeout -> ok;
        Topic   -> erlang:error({test_record_leaked_onto_presence_topic, Topic})
    end.

test_daemon_record() ->
    OwnerKp = macula_identity:generate(),
    NodeId  = macula_identity:public(OwnerKp),
    macula_record:sign(
      macula_record:node_record(NodeId, [], 0, #{kind => <<"test_daemon">>}),
      OwnerKp).

test_station_record() ->
    OwnerKp = macula_identity:generate(),
    NodeId  = macula_identity:public(OwnerKp),
    macula_record:sign(
      macula_record:node_record(NodeId, [], 0, #{kind => <<"test_station">>}),
      OwnerKp).

%% A real station always stamps `kind = station' explicitly
%% (`macula_station_announcer:node_record_opts/1') — mirror that here
%% rather than relying on the record's default, which is now `daemon'.
station_record() ->
    OwnerKp = macula_identity:generate(),
    NodeId  = macula_identity:public(OwnerKp),
    macula_record:sign(
      macula_record:node_record(NodeId, [], 0, #{kind => <<"station">>}),
      OwnerKp).

%% No `kind' field at all — what the SDK's own peer default produces.
unlabeled_record() ->
    OwnerKp = macula_identity:generate(),
    NodeId  = macula_identity:public(OwnerKp),
    macula_record:sign(macula_record:node_record(NodeId, [], 0), OwnerKp).

daemon_record() ->
    OwnerKp = macula_identity:generate(),
    NodeId  = macula_identity:public(OwnerKp),
    macula_record:sign(
      macula_record:node_record(
        NodeId, [], 0,
        #{ttl_ms => 600_000, kind => <<"daemon">>}),
      OwnerKp).

wire_atomized_daemon_record() ->
    OwnerKp = macula_identity:generate(),
    NodeId  = macula_identity:public(OwnerKp),
    Signed  = macula_record:sign(
                macula_record:node_record(
                  NodeId, [], 0,
                  #{ttl_ms => 600_000, kind => <<"daemon">>}),
                OwnerKp),
    OldPayload = maps:get(payload, Signed),
    Atomised   = maps:from_list(
                     [{atomise_key(K), V} || {K, V} <- maps:to_list(OldPayload)]),
    Signed#{payload => Atomised}.

atomise_key({text, B}) when is_binary(B) ->
    try binary_to_existing_atom(B, utf8)
    catch error:badarg -> {text, B}
    end;
atomise_key(K) -> K.
