%% EUnit tests for `hecate_station_fact_publisher'.
%%
%% The publisher classifies records by `(type, payload.kind)' and
%% pushes EVENT frames out every subscribed peering connection. The
%% tests exercise the classification + fan-out by:
%%
%%   * starting a real `hecate_pubsub_registry'
%%   * stubbing the peer_observer with a tiny gen_server that tracks
%%     `conn_for/2' lookups so we can assert which subscribers were
%%     fanned out to
%%   * calling `on_record_stored/2' directly with crafted records
%%   * inspecting which topic ended up on the registry's pubsub_server
%%
%% No QUIC, no real peering — those land in CT integration tests.
-module(hecate_station_fact_publisher_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Fixture
%%====================================================================

setup() ->
    process_flag(trap_exit, true),
    Identity     = macula_identity:generate(),
    {ok, Reg}    = hecate_pubsub_registry:start_link(#{identity => Identity}),
    unlink(Reg),
    {ok, Obs}    = stub_observer:start_link(),
    unlink(Obs),
    {ok, Pub}    = hecate_station_fact_publisher:start_link(#{
                      pubsub_registry => Reg,
                      peer_observer   => Obs,
                      identity        => Identity,
                      identity_key    => <<"test">>
                  }),
    unlink(Pub),
    #{publisher => Pub, registry => Reg, observer => Obs,
      identity  => Identity}.

cleanup(#{publisher := Pub, registry := Reg, observer := Obs}) ->
    catch hecate_station_fact_publisher:stop(Pub),
    catch hecate_pubsub_registry:stop(Reg),
    catch stub_observer:stop(Obs),
    ok.

%%====================================================================
%% Generator
%%====================================================================

fact_publisher_test_() ->
    %% 30s eunit timeout per test — leaves headroom for the 5s
    %% receive timeout in `assert_publish_lands' to fire and dump
    %% its diagnostic map. Default 5s eunit timeout would race
    %% the receive timer.
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
         end
     ]}.

%%====================================================================
%% Cases
%%====================================================================

station_record_lands_on_station_topic(Ctx) ->
    assert_publish_lands(Ctx,
                        station_record(),
                        <<"_mesh.station.announced_v1">>).

daemon_record_lands_on_daemon_topic(Ctx) ->
    assert_publish_lands(Ctx,
                        daemon_record(),
                        <<"_mesh.daemon.announced_v1">>).

%% Regression: macula_frame:from_wire_envelope/1 atomizes recognised
%% binary keys/values via binary_to_existing_atom/1. After wire round-trip
%% the daemon record's payload has `#{kind => daemon}' (atom key, atom
%% value) instead of `#{{text, <<"kind">>} => {text, <<"daemon">>}}'.
%% Pre-fix `payload_kind/1' guards required is_binary(K), so the atomized
%% form fell through to the `<<"station">>' default, misclassifying every
%% daemon-presence record as a station event.
wire_decoded_daemon_record_lands_on_daemon_topic(Ctx) ->
    assert_publish_lands(Ctx,
                        atomized_daemon_record(),
                        <<"_mesh.daemon.announced_v1">>).

unknown_record_type_is_ignored(#{publisher := Pub, registry := Reg,
                                 identity := PubIdentity}) ->
    %% Subscribe a fake subscriber to BOTH known mesh topics, then
    %% send an unknown-type record and assert no event arrives. The
    %% mesh-realm pubsub_server is eagerly materialised by init/1,
    %% so we cannot assert on registry emptiness — the contract is
    %% "unknown types fire no events", verified by the subscriber's
    %% mailbox being silent.
    {ok, Server} = hecate_pubsub_registry:register(Reg, <<0:256>>, PubIdentity),
    SubKp        = macula_identity:generate(),
    SubId        = macula_identity:public(SubKp),
    ok = hecate_pubsub_server:subscribe(Server, <<"_mesh.station.announced_v1">>, SubId),
    ok = hecate_pubsub_server:subscribe(Server, <<"_mesh.daemon.announced_v1">>,  SubId),
    FakeRecord = #{type => 16#33, payload => #{},
                   signature => <<0:512>>, key => <<0:256>>},
    hecate_station_fact_publisher:on_record_stored(Pub, FakeRecord),
    receive
        {'$gen_cast', {send_frame, _}} ->
            erlang:error(unknown_type_fired_event)
    after 200 -> ok
    end.

%%====================================================================
%% Common: pre-subscribe a fake subscriber, then assert the publish
%% reaches it as an EVENT frame on the expected topic.
%%====================================================================

assert_publish_lands(#{publisher := Pub, registry := Reg, observer := Obs,
                       identity := PubIdentity}, Record, ExpectedTopic) ->
    %% Pre-create the pubsub_server for the mesh realm so we can
    %% subscribe before the publish call materialises it.
    {ok, Server} = hecate_pubsub_registry:register(Reg, <<0:256>>, PubIdentity),
    SubKp        = macula_identity:generate(),
    SubId        = macula_identity:public(SubKp),
    ok = hecate_pubsub_server:subscribe(Server, ExpectedTopic, SubId),
    ok = stub_observer:register_conn(Obs, SubId, self()),
    hecate_station_fact_publisher:on_record_stored(Pub, Record),
    receive
        {'$gen_cast', {send_frame, #{frame_type := event,
                                     topic := T} = _Frame}} ->
            ?assertEqual(ExpectedTopic, T)
    after 5_000 ->
        Diagnostic = #{
            expected_topic => ExpectedTopic,
            record_type    => maps:get(type, Record, undefined),
            payload        => maps:get(payload, Record, undefined),
            server_topics  => hecate_pubsub_server:topics(Server),
            server_subs    => hecate_pubsub_server:subscribers(Server, ExpectedTopic)
        },
        erlang:error({event_not_fanned_out, Diagnostic})
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

%% Build a daemon record with the same shape `macula_frame:from_wire_envelope/1'
%% produces for an inbound `_dht.put_record' call: text-tagged keys/values
%% whose binaries match existing atoms get atomized. For node_record
%% payloads, `kind' itself is a known atom, and the value `<<"daemon">>'
%% matches the `daemon' atom — so both end up atomized.
atomized_daemon_record() ->
    OwnerKp = macula_identity:generate(),
    NodeId  = macula_identity:public(OwnerKp),
    Signed  = macula_record:sign(
                macula_record:node_record(
                  NodeId, [], 0,
                  #{ttl_ms => 600_000, kind => <<"daemon">>}),
                OwnerKp),
    OldPayload = maps:get(payload, Signed),
    NewPayload = OldPayload#{
        %% Drop the {text, <<"kind">>} key/value, replace with atom forms
        %% as if `from_wire_envelope/1' had recognised them.
        kind => daemon
    },
    NewPayload2 = maps:remove({text, <<"kind">>}, NewPayload),
    Signed#{payload => NewPayload2}.

