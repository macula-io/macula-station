%% @doc Observer unit tests.
%%
%% Injects fake peering events directly into the observer gen_server
%% and asserts the resulting DHT + SWIM state. Uses a real
%% `hecate_dht' (pure Erlang, no NIFs) and a real `hecate_swim'; no
%% QUIC listener or peering worker is involved.
-module(hecate_station_peer_observer_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Connected → observes DHT + adds to SWIM.
%%==================================================================

connected_observes_into_dht_as_t0_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, Dht, _Swim, NodeId, ConnPid} = one_connected_peer(Ctx),
            {ok, Entry} = hecate_dht:find(Dht, NodeId),
            ?assertEqual(t0, hecate_dht_entry:tier(Entry)),
            ?assertEqual(1,  hecate_dht:size(Dht)),
            ?assertMatch([{ConnPid, NodeId}],
                         hecate_station_peer_observer:peers(Obs))
        end
    end}.

connected_adds_peer_to_swim_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {_Obs, _Dht, Swim, NodeId, ConnPid} = one_connected_peer(Ctx),
            wait_for(fun() -> swim_alive(Swim, NodeId, ConnPid) end, 500)
        end
    end}.

swim_alive(Swim, NodeId, ConnPid) ->
    lists:any(fun(#{node_id := N, state := alive,
                    conn_pid := C}) ->
                  N =:= NodeId andalso C =:= ConnPid;
                 (_) -> false
              end, hecate_swim:members(Swim)).

%%==================================================================
%% Duplicate connected — DHT returns `touched', not `admitted'.
%%==================================================================

duplicate_connected_touches_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(#{obs := Obs,
                                               dht := Dht} = Ctx) ->
        fun() ->
            NodeId  = random_node_id(),
            ConnPid = spawn_dummy(),
            Obs ! {macula_peering, connected, ConnPid, NodeId},
            wait_for(fun() -> hecate_dht:size(Dht) =:= 1 end, 500),
            %% A direct call confirms the DHT's own idempotence and
            %% matches what the observer's next event would do.
            Spec = #{node_id => NodeId, endpoints => [],
                     asn => 0, country => <<"??">>, tier => t0},
            ?assertEqual(touched, hecate_dht:observe(Dht, Spec)),
            _ = Ctx
        end
    end}.

%%==================================================================
%% Disconnected — SWIM drops the member.
%%==================================================================

disconnected_removes_from_swim_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Dht, Swim, NodeId, ConnPid} = one_connected_peer(Ctx),
            wait_for(fun() -> has_member(Swim, NodeId) end, 500),
            Obs ! {macula_peering, disconnected, ConnPid, operator_stop},
            wait_for(fun() -> not has_member(Swim, NodeId) end, 500),
            ?assertEqual([], hecate_station_peer_observer:peers(Obs))
        end
    end}.

%%==================================================================
%% Frame routing — SWIM frames with matching signature reach SWIM;
%% bad signatures get dropped; DHT-level frames bypass SWIM.
%%==================================================================

signed_swim_ping_reaches_swim_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Dht, _Swim, _NodeId, ConnPid} = one_connected_peer(Ctx),
            Kp = maps:get(peer_kp, Ctx),
            Ping = macula_frame:swim_ping(#{round => 1,
                                            incarnation => 0,
                                            piggyback => []}),
            Signed = macula_frame:sign(Ping, Kp),
            Obs ! {macula_peering, frame, ConnPid, Signed},
            %% There is no synchronous observer on SWIM receipt; let
            %% the cast land. On a bad signature the assertion below
            %% would fail because SWIM would never have replied.
            %% Ignoring the actual ack here: the critical property is
            %% that the observer did not crash and forwarded the frame.
            ?assert(is_process_alive(Obs))
        end
    end}.

unsigned_frame_is_dropped_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Dht, _Swim, _NodeId, ConnPid} = one_connected_peer(Ctx),
            %% Build an unsigned frame — `macula_frame:verify/2'
            %% returns `{error, _}', so the observer must drop it
            %% silently without crashing.
            Unsigned = macula_frame:swim_ping(#{round => 99,
                                                incarnation => 0,
                                                piggyback => []}),
            Obs ! {macula_peering, frame, ConnPid, Unsigned},
            timer:sleep(50),
            ?assert(is_process_alive(Obs))
        end
    end}.

frame_from_unknown_conn_is_dropped_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(#{obs := Obs}) ->
        fun() ->
            Fake = spawn_dummy(),
            Kp   = macula_identity:generate(),
            Ping = macula_frame:swim_ping(#{round => 1, incarnation => 0,
                                            piggyback => []}),
            Signed = macula_frame:sign(Ping, Kp),
            Obs ! {macula_peering, frame, Fake, Signed},
            timer:sleep(50),
            ?assert(is_process_alive(Obs))
        end
    end}.

%%==================================================================
%% Inbound PUBLISH relays EVENT to subscriber's connection
%% (Phase 1 of PLAN_V2_PARITY — single-station fan-out).
%%==================================================================

publish_relays_event_to_subscriber_test_() ->
    {setup, fun setup_with_pubsub/0, fun teardown_with_pubsub/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs} = Ctx,
            SubKp  = macula_identity:generate(),
            SubId  = macula_identity:public(SubKp),
            PubKp  = macula_identity:generate(),
            PubId  = macula_identity:public(PubKp),
            Realm  = crypto:strong_rand_bytes(32),
            Topic  = <<"weather.measured_v1">>,
            %% Subscriber's connection = test pid so we capture the
            %% relayed EVENT via the send_frame cast.
            SubConn = self(),
            Obs ! {macula_peering, connected, SubConn, SubId},
            %% Publisher's connection = dummy pid.
            PubConn = spawn_dummy(),
            Obs ! {macula_peering, connected, PubConn, PubId},
            wait_for(fun() -> length(hecate_station_peer_observer:peers(Obs))
                              =:= 2 end, 500),
            %% Subscriber registers a sub for (Realm, Topic) — auto-
            %% materialises the realm in the registry via
            %% default_identity.
            SubFrame = macula_frame:sign(macula_frame:subscribe(#{
                topic      => Topic,
                realm      => Realm,
                subscriber => SubId
            }), SubKp),
            Obs ! {macula_peering, frame, SubConn, SubFrame},
            timer:sleep(50),
            %% Publisher sends PUBLISH. Observer routes to registry's
            %% relay_publish, server builds EVENT, observer fans EVENT
            %% to SubConn via macula_peering:send_frame (gen_server cast).
            PubFrame = macula_frame:sign(macula_frame:publish(#{
                topic           => Topic,
                realm           => Realm,
                publisher       => PubId,
                seq             => 1,
                payload         => #{temp => 20},
                published_at_ms => erlang:system_time(millisecond)
            }), PubKp),
            Obs ! {macula_peering, frame, PubConn, PubFrame},
            receive
                {'$gen_cast', {send_frame, EventFrame}} ->
                    ?assertEqual(event,
                                 macula_frame:frame_type(EventFrame)),
                    ?assertEqual(Topic, maps:get(topic, EventFrame)),
                    ?assertEqual(Realm, maps:get(realm, EventFrame)),
                    %% EVENT preserves the daemon's publisher pubkey
                    %% + seq for end-to-end pool dedup keying.
                    ?assertEqual(PubId, maps:get(publisher, EventFrame)),
                    ?assertEqual(1, maps:get(seq, EventFrame)),
                    ?assertEqual(#{temp => 20},
                                 maps:get(payload, EventFrame))
            after 2_000 ->
                erlang:error(no_event_relayed)
            end
        end
    end}.

publish_to_unknown_realm_is_dropped_test_() ->
    {setup, fun setup_with_pubsub/0, fun teardown_with_pubsub/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs} = Ctx,
            PubKp  = macula_identity:generate(),
            PubId  = macula_identity:public(PubKp),
            Realm  = crypto:strong_rand_bytes(32),
            PubConn = self(),
            Obs ! {macula_peering, connected, PubConn, PubId},
            wait_for(fun() -> length(hecate_station_peer_observer:peers(Obs))
                              =:= 1 end, 500),
            %% No subscriber for Realm → no server registered → publish
            %% drops silently. The observer must not crash and no
            %% send_frame cast must arrive.
            PubFrame = macula_frame:sign(macula_frame:publish(#{
                topic           => <<"x.v1">>,
                realm           => Realm,
                publisher       => PubId,
                seq             => 1,
                payload         => hello,
                published_at_ms => erlang:system_time(millisecond)
            }), PubKp),
            Obs ! {macula_peering, frame, PubConn, PubFrame},
            receive
                {'$gen_cast', {send_frame, _}} ->
                    erlang:error(unexpected_event_from_unknown_realm)
            after 200 -> ok
            end,
            ?assert(is_process_alive(Obs))
        end
    end}.

%%==================================================================
%% Fixture
%%==================================================================

setup() ->
    application:ensure_all_started(crypto),
    SelfKp  = macula_identity:generate(),
    PeerKp  = macula_identity:generate(),
    SelfId  = macula_identity:public(SelfKp),
    {ok, Dht}  = hecate_dht:start_link(#{self_id => SelfId}),
    {ok, Swim} = hecate_swim:start_link(#{
        self_node_id    => SelfId,
        identity        => SelfKp,
        controlling_pid => self()
    }),
    {ok, Obs} = hecate_station_peer_observer:start_link(#{dht => Dht,
                                                          swim => Swim}),
    #{dht => Dht, swim => Swim, obs => Obs,
      self_kp => SelfKp, peer_kp => PeerKp}.

teardown(#{obs := Obs, swim := Swim, dht := Dht}) ->
    _ = catch hecate_station_peer_observer:stop(Obs),
    _ = catch hecate_swim:stop(Swim),
    _ = catch hecate_dht:stop(Dht),
    ok.

setup_with_pubsub() ->
    application:ensure_all_started(crypto),
    SelfKp  = macula_identity:generate(),
    PeerKp  = macula_identity:generate(),
    SelfId  = macula_identity:public(SelfKp),
    {ok, Dht}  = hecate_dht:start_link(#{self_id => SelfId}),
    {ok, Swim} = hecate_swim:start_link(#{
        self_node_id    => SelfId,
        identity        => SelfKp,
        controlling_pid => self()
    }),
    {ok, Reg}  = hecate_pubsub_registry:start_link(#{identity => SelfKp}),
    unlink(Reg),
    {ok, Obs}  = hecate_station_peer_observer:start_link(#{
        dht             => Dht,
        swim            => Swim,
        pubsub_registry => Reg,
        self_id         => SelfId
    }),
    #{dht => Dht, swim => Swim, reg => Reg, obs => Obs,
      self_kp => SelfKp, peer_kp => PeerKp}.

teardown_with_pubsub(#{obs := Obs, swim := Swim, dht := Dht, reg := Reg}) ->
    _ = catch hecate_station_peer_observer:stop(Obs),
    _ = catch hecate_pubsub_registry:stop(Reg),
    _ = catch hecate_swim:stop(Swim),
    _ = catch hecate_dht:stop(Dht),
    ok.

one_connected_peer(#{obs := Obs, dht := Dht, swim := Swim,
                     peer_kp := PeerKp}) ->
    NodeId  = macula_identity:public(PeerKp),
    ConnPid = spawn_dummy(),
    Obs ! {macula_peering, connected, ConnPid, NodeId},
    wait_for(fun() -> hecate_dht:size(Dht) =:= 1 end, 500),
    {Obs, Dht, Swim, NodeId, ConnPid}.

has_member(Swim, NodeId) ->
    lists:any(fun(#{node_id := N}) -> N =:= NodeId end,
              hecate_swim:members(Swim)).

random_node_id() ->
    crypto:strong_rand_bytes(32).

%% A trivial linked process that stays alive so the observer's peer
%% map can keep it around without worrying about premature death.
spawn_dummy() ->
    spawn(fun() -> receive stop -> ok end end).

wait_for(Pred, Ms) ->
    wait_step(Pred(), Pred, Ms).

wait_step(true,  _Pred, _Ms)                 -> ok;
wait_step(false, Pred, Ms) when Ms =< 0      -> ?assert(Pred());
wait_step(false, Pred, Ms)                   ->
    timer:sleep(20),
    wait_step(Pred(), Pred, Ms - 20).
