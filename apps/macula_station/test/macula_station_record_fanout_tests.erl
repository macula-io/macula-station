%% EUnit tests for `macula_station_record_fanout'.
%%
%% Mirrors the classification surface previously covered by
%% `macula_station_fact_publisher_tests'. The fan-out replaces the
%% per-identity publisher with a node-singleton that walks every
%% identity's `pubsub_server' on each `record_stored' cast.
%%
%% We drive the fan-out in test mode (`start_link/1' with
%% `initial_identities') so the tests never need a real
%% `macula_station_identity_registry'. Each test fixture wires:
%%
%%   * one real `hecate_pubsub_registry' per identity (bound to the
%%     generated keypair),
%%   * one `stub_observer' per identity (so subscribers can resolve
%%     to the test process pid for receive-based assertions),
%%   * a fan-out instance with the identities preloaded.
%%
%% Multi-identity fan-out is exercised by spinning up two identities
%% in one fixture and asserting the EVENT lands in BOTH subscribers'
%% mailboxes. This is the regression that originally drove the
%% `pg'-based broadcast in `fact_publisher' — keeping it green is the
%% acceptance criterion for the refactor.
-module(macula_station_record_fanout_tests).

-include_lib("eunit/include/eunit.hrl").

-define(MESH_REALM, <<0:256>>).

%%====================================================================
%% Fixture — single identity (basic classification cases)
%%====================================================================

setup_single() ->
    process_flag(trap_exit, true),
    Identity  = macula_identity:generate(),
    {ok, Reg} = hecate_pubsub_registry:start_link(#{identity => Identity}),
    unlink(Reg),
    {ok, Obs} = stub_observer:start_link(),
    unlink(Obs),
    Phase3 = #{
        pubsub_registry => Reg,
        peer_observer   => Obs,
        identity        => Identity
    },
    {ok, Pub} = macula_station_record_fanout:start_link(#{
        initial_identities => #{<<"test">> => Phase3}
    }),
    unlink(Pub),
    #{publisher => Pub, registry => Reg, observer => Obs,
      identity  => Identity, identity_key => <<"test">>}.

cleanup_single(#{publisher := Pub, registry := Reg, observer := Obs}) ->
    catch macula_station_record_fanout:stop(Pub),
    catch hecate_pubsub_registry:stop(Reg),
    catch stub_observer:stop(Obs),
    ok.

%%====================================================================
%% Fixture — two identities (cross-identity fan-out regression)
%%====================================================================

setup_double() ->
    process_flag(trap_exit, true),
    Id1 = macula_identity:generate(),
    Id2 = macula_identity:generate(),
    {ok, R1} = hecate_pubsub_registry:start_link(#{identity => Id1}),
    {ok, R2} = hecate_pubsub_registry:start_link(#{identity => Id2}),
    unlink(R1), unlink(R2),
    {ok, O1} = stub_observer:start_link(),
    {ok, O2} = stub_observer:start_link(),
    unlink(O1), unlink(O2),
    Initial = #{
        <<"id1">> => #{pubsub_registry => R1,
                       peer_observer   => O1,
                       identity        => Id1},
        <<"id2">> => #{pubsub_registry => R2,
                       peer_observer   => O2,
                       identity        => Id2}
    },
    {ok, Pub} = macula_station_record_fanout:start_link(#{
        initial_identities => Initial
    }),
    unlink(Pub),
    #{publisher => Pub,
      id1 => #{registry => R1, observer => O1, identity => Id1},
      id2 => #{registry => R2, observer => O2, identity => Id2}}.

cleanup_double(#{publisher := Pub, id1 := A, id2 := B}) ->
    catch macula_station_record_fanout:stop(Pub),
    catch hecate_pubsub_registry:stop(maps:get(registry, A)),
    catch hecate_pubsub_registry:stop(maps:get(registry, B)),
    catch stub_observer:stop(maps:get(observer, A)),
    catch stub_observer:stop(maps:get(observer, B)),
    ok.

%%====================================================================
%% Generators
%%====================================================================

single_identity_test_() ->
    {foreach,
     fun setup_single/0,
     fun cleanup_single/1,
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
         end
     ]}.

cross_identity_test_() ->
    {foreach,
     fun setup_double/0,
     fun cleanup_double/1,
     [
         fun(Ctx) ->
             {timeout, 30,
              fun() -> record_fans_to_both_identities(Ctx) end}
         end
     ]}.

%%====================================================================
%% Single-identity cases — classification surface
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

unknown_record_type_is_ignored(#{publisher := Pub, registry := Reg,
                                 identity := PubIdentity,
                                 identity_key := IdKey}) ->
    {ok, Server} = hecate_pubsub_registry:register(Reg, ?MESH_REALM, PubIdentity),
    SubKp        = macula_identity:generate(),
    SubId        = macula_identity:public(SubKp),
    ok = hecate_pubsub_server:subscribe(Server, <<"_mesh.station.announced_v1">>, SubId),
    ok = hecate_pubsub_server:subscribe(Server, <<"_mesh.daemon.announced_v1">>,  SubId),
    FakeRecord = #{type => 16#33, payload => #{},
                   signature => <<0:512>>, key => <<0:256>>},
    macula_station_record_fanout:on_record(Pub, IdKey, FakeRecord),
    receive
        {'$gen_cast', {send_frame, _}} ->
            erlang:error(unknown_type_fired_event)
    after 200 -> ok
    end.

%%====================================================================
%% Cross-identity case — the cross_publish replacement
%%
%% A subscriber attached to identity 1 must receive an EVENT for a
%% record that originated at identity 2. This is the regression that
%% justified the previous pg-based broadcast and is now satisfied by
%% the fan-out walking every identity in its identities map.
%%====================================================================

record_fans_to_both_identities(#{publisher := Pub,
                                 id1 := #{registry := R1, observer := O1,
                                          identity := Id1},
                                 id2 := #{registry := R2, observer := O2,
                                          identity := Id2}}) ->
    %% Pre-create both pubsub_servers so we can subscribe before the
    %% record cast lands.
    {ok, S1} = hecate_pubsub_registry:register(R1, ?MESH_REALM, Id1),
    {ok, S2} = hecate_pubsub_registry:register(R2, ?MESH_REALM, Id2),
    %% Two distinct subscribers, one per identity. Both register
    %% themselves with their identity's stub_observer so the fan-out
    %% can resolve the conn pid back to `self()' and deliver the
    %% EVENT frame as a `{$gen_cast, {send_frame, ...}}' message we
    %% receive in the test process.
    Sub1Id = macula_identity:public(macula_identity:generate()),
    Sub2Id = macula_identity:public(macula_identity:generate()),
    ok = hecate_pubsub_server:subscribe(S1, <<"_mesh.station.announced_v1">>, Sub1Id),
    ok = hecate_pubsub_server:subscribe(S2, <<"_mesh.station.announced_v1">>, Sub2Id),
    ok = stub_observer:register_conn(O1, Sub1Id, self()),
    ok = stub_observer:register_conn(O2, Sub2Id, self()),
    %% Cast a record originated at id1. Both subscribers must hear it.
    macula_station_record_fanout:on_record(Pub, <<"id1">>, station_record()),
    Got1 = receive_event(<<"_mesh.station.announced_v1">>, 5_000),
    Got2 = receive_event(<<"_mesh.station.announced_v1">>, 5_000),
    ?assertEqual(<<"_mesh.station.announced_v1">>, Got1),
    ?assertEqual(<<"_mesh.station.announced_v1">>, Got2).

%%====================================================================
%% Helpers
%%====================================================================

assert_publish_lands(#{publisher := Pub, registry := Reg, observer := Obs,
                       identity := PubIdentity, identity_key := IdKey},
                     Record, ExpectedTopic) ->
    {ok, Server} = hecate_pubsub_registry:register(Reg, ?MESH_REALM, PubIdentity),
    SubKp        = macula_identity:generate(),
    SubId        = macula_identity:public(SubKp),
    ok = hecate_pubsub_server:subscribe(Server, ExpectedTopic, SubId),
    ok = stub_observer:register_conn(Obs, SubId, self()),
    macula_station_record_fanout:on_record(Pub, IdKey, Record),
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

station_record() ->
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
