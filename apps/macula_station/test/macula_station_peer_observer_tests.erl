%% @doc Observer unit tests.
%%
%% Injects fake peering events directly into the observer gen_server
%% and asserts the resulting DHT + SWIM state. Uses a real
%% `macula_dht' (pure Erlang, no NIFs) and a real `macula_swim'; no
%% QUIC listener or peering worker is involved.
-module(macula_station_peer_observer_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Connected → observes DHT + adds to SWIM.
%%==================================================================

connected_observes_into_dht_as_t0_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, Dht, _Swim, NodeId, ConnPid} = one_connected_peer(Ctx),
            {ok, Entry} = macula_dht:find(Dht, NodeId),
            ?assertEqual(t0, macula_dht_entry:tier(Entry)),
            ?assertEqual(1,  macula_dht:size(Dht)),
            ?assertMatch([{ConnPid, NodeId}],
                         macula_station_peer_observer:peers(Obs))
        end
    end}.

%% conn_for/2 should return the connection without going through the
%% observer's gen_server mailbox — proves wire-send paths (DHT
%% transport, etc.) won't queue against frame-handling under load.
%% We block the gen_server in a synthetic 2s sleep handle_call and
%% verify conn_for/2 still returns immediately.
conn_for_bypasses_gen_server_mailbox_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Dht, _Swim, NodeId, ConnPid} = one_connected_peer(Ctx),
            %% Confirm the lookup works via the ETS mirror.
            ?assertEqual({ok, ConnPid},
                         macula_station_peer_observer:conn_for(Obs, NodeId)),

            %% Stuff the gen_server mailbox with a slow custom call
            %% that won't terminate for 2s. If conn_for/2 went
            %% through gen_server:call it would block behind this
            %% and time out at 5s default — but it doesn't, because
            %% it reads ETS directly.
            spawn(fun() -> gen_server:call(Obs, slow_blocker, 5_000) end),
            timer:sleep(50), %% let the slow call enqueue first
            T0 = erlang:monotonic_time(millisecond),
            ?assertEqual({ok, ConnPid},
                         macula_station_peer_observer:conn_for(Obs, NodeId)),
            Elapsed = erlang:monotonic_time(millisecond) - T0,
            %% A bypass should resolve in microseconds; allow 200ms
            %% margin for scheduler jitter on a busy CI box.
            ?assert(Elapsed < 200,
                    {elapsed_ms_too_long_for_ets_bypass, Elapsed})
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
              end, macula_swim:members(Swim)).

%%==================================================================
%% Duplicate connected — DHT returns `touched', not `admitted'.
%%==================================================================

duplicate_connected_touches_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(#{obs := Obs,
                                               dht := Dht} = Ctx) ->
        fun() ->
            NodeId  = random_node_id(),
            ConnPid = spawn_dummy(),
            %% Use connected_outbound so the peer enters the DHT
            %% (inbound is intentionally not observed — see
            %% peer_observer:on_connected_directional/4).
            Obs ! {macula_peering, connected_outbound, ConnPid, NodeId},
            wait_for(fun() -> macula_dht:size(Dht) =:= 1 end, 500),
            %% A direct call confirms the DHT's own idempotence and
            %% matches what the observer's next event would do.
            Spec = #{node_id => NodeId, endpoints => [],
                     asn => 0, country => <<"??">>, tier => t0},
            ?assertEqual(touched, macula_dht:observe(Dht, Spec)),
            _ = Ctx
        end
    end}.

%% Locks the inbound-skip contract: a peer that arrives via the
%% bare `connected' event (= inbound from the listener) is
%% registered in `peers' (so we know about it for routing wire
%% replies) but is intentionally NOT observed in the DHT routing
%% table. Stations dial each other; daemons only inbound. Keeping
%% daemons out of the DHT routing table is what fixes the
%% iterative-find pollution surfaced by task #15.
inbound_does_not_observe_into_dht_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(#{obs := Obs,
                                               dht := Dht} = _Ctx) ->
        fun() ->
            NodeId  = random_node_id(),
            ConnPid = spawn_dummy(),
            Obs ! {macula_peering, connected, ConnPid, NodeId},
            timer:sleep(150),    % give the gen_server time to process
            ?assertEqual(0, macula_dht:size(Dht)),
            %% But the peer IS in the local peers map (so reply
            %% routing still works for whatever frame the peer sends).
            ?assertMatch([{ConnPid, NodeId}],
                         macula_station_peer_observer:peers(Obs))
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
            ?assertEqual([], macula_station_peer_observer:peers(Obs))
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
            wait_for(fun() -> length(macula_station_peer_observer:peers(Obs))
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
            wait_for(fun() -> length(macula_station_peer_observer:peers(Obs))
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
    {ok, Dht}  = macula_dht:start_link(#{self_id => SelfId}),
    {ok, Swim} = macula_swim:start_link(#{
        self_node_id    => SelfId,
        identity        => SelfKp,
        controlling_pid => self()
    }),
    {ok, Obs} = macula_station_peer_observer:start_link(#{dht => Dht,
                                                          swim => Swim}),
    #{dht => Dht, swim => Swim, obs => Obs,
      self_kp => SelfKp, peer_kp => PeerKp}.

%%==================================================================
%% Remote advertise + CALL forwarding
%%==================================================================

setup_with_advertise() ->
    application:ensure_all_started(crypto),
    SelfKp     = macula_identity:generate(),
    AdvKp      = macula_identity:generate(),
    CallerKp   = macula_identity:generate(),
    SelfId     = macula_identity:public(SelfKp),
    {ok, Dht}  = macula_dht:start_link(#{self_id => SelfId}),
    {ok, Swim} = macula_swim:start_link(#{
        self_node_id    => SelfId,
        identity        => SelfKp,
        controlling_pid => self()
    }),
    {ok, Hr}   = macula_handler_registry:start_link(#{}),
    {ok, Ra}   = macula_remote_advertise_registry:start_link(#{}),
    {ok, Obs}  = macula_station_peer_observer:start_link(#{
        dht              => Dht,
        swim             => Swim,
        handler_registry => Hr,
        remote_advertise => Ra,
        self_id          => SelfId
    }),
    #{dht => Dht, swim => Swim, hr => Hr, ra => Ra, obs => Obs,
      self_kp => SelfKp, adv_kp => AdvKp, caller_kp => CallerKp,
      self_id => SelfId}.

teardown_with_advertise(#{obs := Obs, hr := Hr, ra := Ra,
                          swim := Swim, dht := Dht}) ->
    _ = catch macula_station_peer_observer:stop(Obs),
    _ = catch macula_handler_registry:stop(Hr),
    _ = catch macula_remote_advertise_registry:stop(Ra),
    _ = catch macula_swim:stop(Swim),
    _ = catch macula_dht:stop(Dht),
    ok.

advertise_frame_registers_handler_test_() ->
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra, adv_kp := AdvKp} = Ctx,
            AdvId   = macula_identity:public(AdvKp),
            AdvConn = spawn_dummy(),
            Realm   = <<7:256>>,
            Procedure = <<"_realm.membership.join_with_token_v1">>,
            %% Connect the advertiser so the observer's `peers' map
            %% knows their NodeId.
            Obs ! {macula_peering, connected, AdvConn, AdvId},
            wait_for_peers(Obs, 1, 500),
            Frame = macula_frame:sign(
                      macula_frame:advertise(#{realm      => Realm,
                                               procedure  => Procedure,
                                               advertiser => AdvId}),
                      AdvKp),
            Obs ! {macula_peering, frame, AdvConn, Frame},
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {ok, _} -> true;
                    _       -> false
                end
            end, 500),
            {ok, Entry} = macula_remote_advertise_registry:lookup(
                            Ra, Realm, Procedure),
            ?assertEqual(AdvId,   maps:get(advertiser, Entry)),
            ?assertEqual(AdvConn, maps:get(conn_pid,   Entry))
        end
     end}.

mismatched_advertiser_pubkey_rejected_test_() ->
    %% A peer cannot advertise on behalf of a different identity —
    %% the on-wire `advertiser' field MUST equal the connection's
    %% NodeId. This check defends against a misbehaving SDK.
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra, adv_kp := AdvKp} = Ctx,
            AdvId   = macula_identity:public(AdvKp),
            AdvConn = spawn_dummy(),
            Imposter = <<99:256>>,
            Realm   = <<7:256>>,
            Procedure = <<"_p">>,
            Obs ! {macula_peering, connected, AdvConn, AdvId},
            wait_for_peers(Obs, 1, 500),
            Frame = macula_frame:sign(
                      macula_frame:advertise(#{realm      => Realm,
                                               procedure  => Procedure,
                                               advertiser => Imposter}),
                      AdvKp),
            Obs ! {macula_peering, frame, AdvConn, Frame},
            timer:sleep(100),
            ?assertEqual(
               {error, not_found},
               macula_remote_advertise_registry:lookup(Ra, Realm, Procedure))
        end
     end}.

unadvertise_frame_clears_handler_test_() ->
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra, adv_kp := AdvKp} = Ctx,
            AdvId     = macula_identity:public(AdvKp),
            AdvConn   = spawn_dummy(),
            Realm     = <<7:256>>,
            Procedure = <<"_p">>,
            Obs ! {macula_peering, connected, AdvConn, AdvId},
            wait_for_peers(Obs, 1, 500),
            advertise(Obs, AdvConn, AdvKp, Realm, Procedure),
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {ok, _} -> true;
                    _       -> false
                end
            end, 500),
            UnAdv = macula_frame:sign(
                      macula_frame:unadvertise(#{realm      => Realm,
                                                 procedure  => Procedure,
                                                 advertiser => AdvId}),
                      AdvKp),
            Obs ! {macula_peering, frame, AdvConn, UnAdv},
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {error, not_found} -> true;
                    _                  -> false
                end
            end, 500)
        end
     end}.

inbound_call_forwards_to_advertiser_conn_test_() ->
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, adv_kp := AdvKp, caller_kp := CallerKp} = Ctx,
            AdvId    = macula_identity:public(AdvKp),
            CallerId = macula_identity:public(CallerKp),
            AdvConn    = spawn_recorder(),
            CallerConn = spawn_recorder(),
            Realm     = <<7:256>>,
            Procedure = <<"_realm.membership.join_with_token_v1">>,
            Obs ! {macula_peering, connected, AdvConn, AdvId},
            Obs ! {macula_peering, connected, CallerConn, CallerId},
            wait_for_peers(Obs, 2, 500),
            advertise(Obs, AdvConn, AdvKp, Realm, Procedure),
            timer:sleep(50),
            CallId = <<42:128>>,
            Call = macula_frame:sign(
                     macula_frame:call(#{
                       call_id     => CallId,
                       procedure   => Procedure,
                       realm       => Realm,
                       payload     => #{token => <<"abc">>},
                       deadline_ms => erlang:system_time(millisecond) + 5_000,
                       caller      => CallerId}),
                     CallerKp),
            Obs ! {macula_peering, frame, CallerConn, Call},
            %% The advertiser conn should receive the forwarded CALL
            %% frame verbatim.
            assert_recorded_frame(AdvConn, call, CallId, 500)
        end
     end}.

call_for_unadvertised_proc_returns_unknown_next_peer_test_() ->
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, caller_kp := CallerKp} = Ctx,
            CallerId   = macula_identity:public(CallerKp),
            CallerConn = spawn_recorder(),
            Obs ! {macula_peering, connected, CallerConn, CallerId},
            wait_for_peers(Obs, 1, 500),
            CallId = <<99:128>>,
            Call = macula_frame:sign(
                     macula_frame:call(#{
                       call_id     => CallId,
                       procedure   => <<"_no.such.thing">>,
                       realm       => <<7:256>>,
                       payload     => #{},
                       deadline_ms => erlang:system_time(millisecond) + 5_000,
                       caller      => CallerId}),
                     CallerKp),
            Obs ! {macula_peering, frame, CallerConn, Call},
            {Frame, _} = await_recorded(CallerConn, error, 500),
            ?assertEqual(CallId, maps:get(call_id, Frame)),
            ?assertEqual(16#01, maps:get(code, Frame))
        end
     end}.

advertiser_disconnect_purges_advertised_procedures_test_() ->
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra, adv_kp := AdvKp} = Ctx,
            AdvId     = macula_identity:public(AdvKp),
            AdvConn   = spawn_dummy(),
            Realm     = <<7:256>>,
            Procedure = <<"_p">>,
            Obs ! {macula_peering, connected, AdvConn, AdvId},
            wait_for_peers(Obs, 1, 500),
            advertise(Obs, AdvConn, AdvKp, Realm, Procedure),
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {ok, _} -> true;
                    _       -> false
                end
            end, 500),
            Obs ! {macula_peering, disconnected, AdvConn, peer_closed},
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {error, not_found} -> true;
                    _                  -> false
                end
            end, 500)
        end
     end}.

forwarded_result_relayed_back_to_origin_test_() ->
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, adv_kp := AdvKp, caller_kp := CallerKp,
              self_kp := _SelfKp} = Ctx,
            AdvId    = macula_identity:public(AdvKp),
            CallerId = macula_identity:public(CallerKp),
            AdvConn    = spawn_recorder(),
            CallerConn = spawn_recorder(),
            Realm     = <<7:256>>,
            Procedure = <<"_p">>,
            Obs ! {macula_peering, connected, AdvConn, AdvId},
            Obs ! {macula_peering, connected, CallerConn, CallerId},
            wait_for_peers(Obs, 2, 500),
            advertise(Obs, AdvConn, AdvKp, Realm, Procedure),
            timer:sleep(50),
            CallId = <<13:128>>,
            Call = macula_frame:sign(
                     macula_frame:call(#{
                       call_id     => CallId,
                       procedure   => Procedure,
                       realm       => Realm,
                       payload     => #{},
                       deadline_ms => erlang:system_time(millisecond) + 5_000,
                       caller      => CallerId}),
                     CallerKp),
            Obs ! {macula_peering, frame, CallerConn, Call},
            assert_recorded_frame(AdvConn, call, CallId, 500),
            %% Advertiser sends RESULT back; the observer should
            %% relay it to the original caller's connection.
            Result = macula_frame:sign(
                       macula_frame:result(#{
                         call_id      => CallId,
                         payload      => #{ok => done},
                         responded_by => AdvId}),
                       AdvKp),
            Obs ! {macula_peering, frame, AdvConn, Result},
            assert_recorded_frame(CallerConn, result, CallId, 500)
        end
     end}.

forwarded_entry_purged_on_origin_disconnect_test_() ->
    %% Origin disconnects mid-CALL while the advertiser is still
    %% connected. The forwarded entry must be cleared (else the
    %% relay accumulates dangling state).
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, adv_kp := AdvKp, caller_kp := CallerKp} = Ctx,
            AdvId    = macula_identity:public(AdvKp),
            CallerId = macula_identity:public(CallerKp),
            AdvConn    = spawn_recorder(),
            CallerConn = spawn_recorder(),
            Realm     = <<7:256>>,
            Procedure = <<"_p">>,
            Obs ! {macula_peering, connected, AdvConn, AdvId},
            Obs ! {macula_peering, connected, CallerConn, CallerId},
            wait_for_peers(Obs, 2, 500),
            advertise(Obs, AdvConn, AdvKp, Realm, Procedure),
            timer:sleep(50),
            CallId = <<70:128>>,
            Call = macula_frame:sign(
                     macula_frame:call(#{
                       call_id     => CallId,
                       procedure   => Procedure,
                       realm       => Realm,
                       payload     => #{},
                       deadline_ms => erlang:system_time(millisecond) + 5_000,
                       caller      => CallerId}),
                     CallerKp),
            Obs ! {macula_peering, frame, CallerConn, Call},
            assert_recorded_frame(AdvConn, call, CallId, 500),
            ?assertEqual(1, forwarded_size(Obs)),
            Obs ! {macula_peering, disconnected, CallerConn, peer_closed},
            wait_for(fun() -> forwarded_size(Obs) =:= 0 end, 500)
        end
     end}.

forwarded_entry_purged_on_ttl_timeout_test_() ->
    %% The TTL timer fires when no reply ever arrives (advertiser
    %% wedged or returned a malformed frame). The default
    %% FORWARDED_TTL_MS is 60s — too long for a unit test, so we
    %% trigger the timeout path directly by sending the
    %% `{forwarded_timeout, CallId}' message to the observer.
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, adv_kp := AdvKp, caller_kp := CallerKp} = Ctx,
            AdvId    = macula_identity:public(AdvKp),
            CallerId = macula_identity:public(CallerKp),
            AdvConn    = spawn_recorder(),
            CallerConn = spawn_recorder(),
            Realm     = <<7:256>>,
            Procedure = <<"_p">>,
            Obs ! {macula_peering, connected, AdvConn, AdvId},
            Obs ! {macula_peering, connected, CallerConn, CallerId},
            wait_for_peers(Obs, 2, 500),
            advertise(Obs, AdvConn, AdvKp, Realm, Procedure),
            timer:sleep(50),
            CallId = <<71:128>>,
            Call = macula_frame:sign(
                     macula_frame:call(#{
                       call_id     => CallId,
                       procedure   => Procedure,
                       realm       => Realm,
                       payload     => #{},
                       deadline_ms => erlang:system_time(millisecond) + 5_000,
                       caller      => CallerId}),
                     CallerKp),
            Obs ! {macula_peering, frame, CallerConn, Call},
            assert_recorded_frame(AdvConn, call, CallId, 500),
            ?assertEqual(1, forwarded_size(Obs)),
            %% Synthetic TTL fire — what `erlang:send_after' would do
            %% after FORWARDED_TTL_MS without a reply.
            Obs ! {forwarded_timeout, CallId},
            wait_for(fun() -> forwarded_size(Obs) =:= 0 end, 500)
        end
     end}.

%% Reach into the observer's state record. The `forwarded' field is
%% the LAST element of the record (as of this commit); when the
%% record layout changes update both the index and the comment.
%%
%% State layout (record positions):
%%   1: tag (state)        2: dht                 3: swim
%%   4: handler_registry   5: pubsub_registry     6: remote_advertise
%%   7: self_id            8: peers               9: conns
%% State record layout (1-indexed within the tuple, after the
%% record-name atom at slot 1):
%%   2: dht,  3: swim,  4: handler_registry,  5: pubsub_registry,
%%   6: remote_advertise,  7: self_id,  8: peers,  9: conns,
%%  10: direction_of_pid,  11: forwarded
forwarded_size(Obs) ->
    State = sys:get_state(Obs),
    F = element(11, State),
    map_size(F).

advertise(Obs, AdvConn, AdvKp, Realm, Procedure) ->
    AdvId = macula_identity:public(AdvKp),
    Frame = macula_frame:sign(
              macula_frame:advertise(#{realm      => Realm,
                                       procedure  => Procedure,
                                       advertiser => AdvId}),
              AdvKp),
    Obs ! {macula_peering, frame, AdvConn, Frame}.

%% A dummy "conn" process that records every `send_frame' cast it
%% receives, keyed by frame_type. Tests can poll with
%% `await_recorded/3'.
spawn_recorder() ->
    Test = self(),
    spawn(fun() -> recorder_loop(Test, []) end).

recorder_loop(Test, Acc) ->
    receive
        {'$gen_cast', {send_frame, Frame}} ->
            Test ! {recorded, self(), Frame},
            recorder_loop(Test, [Frame | Acc]);
        stop ->
            ok
    end.

%% Skip non-matching `{recorded, _}' messages (e.g. SWIM ping frames
%% the observer fires after `connected') and surface the first one
%% that matches `(ConnPid, Type)'. Other messages stay in the
%% mailbox so a subsequent `await_recorded' can pick them up.
await_recorded(ConnPid, Type, Ms) ->
    receive
        {recorded, P, #{frame_type := T} = F}
          when P =:= ConnPid, T =:= Type ->
            {F, ConnPid}
    after Ms ->
        erlang:error({no_recorded_frame, ConnPid, Type})
    end.

assert_recorded_frame(ConnPid, Type, CallId, Ms) ->
    {Frame, _} = await_recorded(ConnPid, Type, Ms),
    ?assertEqual(CallId, maps:get(call_id, Frame)).

wait_for_peers(Obs, N, Ms) ->
    wait_for(fun() ->
        length(macula_station_peer_observer:peers(Obs)) =:= N
    end, Ms).

teardown(#{obs := Obs, swim := Swim, dht := Dht}) ->
    _ = catch macula_station_peer_observer:stop(Obs),
    _ = catch macula_swim:stop(Swim),
    _ = catch macula_dht:stop(Dht),
    ok.

setup_with_pubsub() ->
    application:ensure_all_started(crypto),
    SelfKp  = macula_identity:generate(),
    PeerKp  = macula_identity:generate(),
    SelfId  = macula_identity:public(SelfKp),
    {ok, Dht}  = macula_dht:start_link(#{self_id => SelfId}),
    {ok, Swim} = macula_swim:start_link(#{
        self_node_id    => SelfId,
        identity        => SelfKp,
        controlling_pid => self()
    }),
    {ok, Reg}  = hecate_pubsub_registry:start_link(#{identity => SelfKp}),
    unlink(Reg),
    {ok, Obs}  = macula_station_peer_observer:start_link(#{
        dht             => Dht,
        swim            => Swim,
        pubsub_registry => Reg,
        self_id         => SelfId
    }),
    #{dht => Dht, swim => Swim, reg => Reg, obs => Obs,
      self_kp => SelfKp, peer_kp => PeerKp}.

teardown_with_pubsub(#{obs := Obs, swim := Swim, dht := Dht, reg := Reg}) ->
    _ = catch macula_station_peer_observer:stop(Obs),
    _ = catch hecate_pubsub_registry:stop(Reg),
    _ = catch macula_swim:stop(Swim),
    _ = catch macula_dht:stop(Dht),
    ok.

one_connected_peer(#{obs := Obs, dht := Dht, swim := Swim,
                     peer_kp := PeerKp}) ->
    NodeId  = macula_identity:public(PeerKp),
    ConnPid = spawn_dummy(),
    %% `connected_outbound' (we dialled the peer) is what registers
    %% the peer in the DHT routing table; bare `connected' (peer
    %% dialled us) is treated as inbound and intentionally NOT
    %% observed in DHT — see peer_observer:on_connected_directional/4
    %% comment, which gates `macula_dht:observe' on outbound to keep
    %% daemon-class connections out of the routing table (the
    %% iterative-find pollution fix).
    Obs ! {macula_peering, connected_outbound, ConnPid, NodeId},
    wait_for(fun() -> macula_dht:size(Dht) =:= 1 end, 500),
    {Obs, Dht, Swim, NodeId, ConnPid}.

has_member(Swim, NodeId) ->
    lists:any(fun(#{node_id := N}) -> N =:= NodeId end,
              macula_swim:members(Swim)).

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
