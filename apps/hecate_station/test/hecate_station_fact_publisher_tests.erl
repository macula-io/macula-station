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
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun(Ctx) ->
             ?_test(station_record_lands_on_station_topic(Ctx))
         end,
         fun(Ctx) ->
             ?_test(daemon_record_lands_on_daemon_topic(Ctx))
         end,
         fun(Ctx) ->
             ?_test(unknown_record_type_is_ignored(Ctx))
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

unknown_record_type_is_ignored(#{publisher := Pub, registry := Reg}) ->
    %% Fake record of an unknown type. The publisher must ignore it
    %% without crashing or materialising a server.
    FakeRecord = #{type => 16#33, payload => #{},
                   signature => <<0:512>>, key => <<0:256>>},
    hecate_station_fact_publisher:on_record_stored(Pub, FakeRecord),
    timer:sleep(100),
    ?assertEqual([], hecate_pubsub_registry:list_realms(Reg)).

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
    after 1_000 ->
        erlang:error({event_not_fanned_out, ExpectedTopic})
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

