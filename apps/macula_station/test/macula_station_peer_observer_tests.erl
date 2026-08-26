%% @doc Observer unit tests.
%%
%% Injects fake peering events directly into the observer gen_server
%% and asserts the resulting DHT + SWIM state. Uses a real
%% `macula_dht' (pure Erlang, no NIFs) and a real `macula_swim'; no
%% QUIC listener or peering worker is involved.
-module(macula_station_peer_observer_tests).
-include_lib("eunit/include/eunit.hrl").

%% Offset of #state.is_station inside the observer's gen_server state
%% tuple, for tests that inject the peer-station flag via
%% sys:replace_state. Counted from the record def (tag=1, dht=2, ..,
%% is_station=17 as of the `identity' / `stream_route' / `stream_bufs'
%% fields added for dedicated-stream relay). Update if the record
%% order changes.
-define(IS_STATION_INDEX, 17).

%%==================================================================
%% Connected → observes DHT + adds to SWIM.
%%==================================================================

connected_observes_into_dht_as_t0_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, Dht, _Swim, NodeId, ConnPid} = one_connected_station(Ctx),
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
            {Obs, _Dht, _Swim, NodeId, ConnPid} = one_connected_station(Ctx),
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

%% SWIM membership now requires a RESOLVED station capability, not merely a
%% connection. Connecting alone must NOT add the peer: daemons connect too, and
%% they cannot answer a SWIM probe, so every one of them timed out into suspect
%% and then confirmed_failed. Measured on the fleet before this change: 142 of
%% 142 confirmed_failed verdicts were contradicted by a live conn.
connected_alone_does_not_add_to_swim_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {_Obs, _Dht, Swim, NodeId, _ConnPid} = one_connected_peer(Ctx),
            %% The dummy conn cannot answer peer_capabilities, so resolution
            %% stays `unknown' and the peer never joins.
            timer:sleep(150),
            ?assertNot(has_member(Swim, NodeId))
        end
    end}.

%% ...and it IS added once the capability probe resolves it as a station.
resolved_station_is_added_to_swim_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Dht, Swim, NodeId, ConnPid} = one_connected_peer(Ctx),
            Obs ! {is_station_resolved, NodeId, 0, {ok, true}},
            wait_for(fun() -> swim_alive(Swim, NodeId, ConnPid) end, 500),
            ?assert(swim_alive(Swim, NodeId, ConnPid))
        end
    end}.

%% A resolved DAEMON stays out. This is the whole point of the gate.
resolved_daemon_is_not_added_to_swim_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Dht, Swim, NodeId, _ConnPid} = one_connected_peer(Ctx),
            Obs ! {is_station_resolved, NodeId, 0, {ok, false}},
            timer:sleep(150),
            ?assertNot(has_member(Swim, NodeId))
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
            Obs ! {macula_peering, connected, ConnPid, NodeId},
            resolve_as_station(Obs, NodeId),
            wait_for(fun() -> macula_dht:size(Dht) =:= 1 end, 500),
            %% A direct call confirms the DHT's own idempotence and
            %% matches what the observer's next event would do.
            Spec = #{node_id => NodeId, endpoints => [],
                     asn => 0, country => <<"??">>, tier => t0},
            ?assertEqual(touched, macula_dht:observe(Dht, Spec)),
            _ = Ctx
        end
    end}.

%%==================================================================
%% Disconnected — SWIM drops the member.
%%==================================================================

disconnected_removes_from_swim_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Dht, Swim, NodeId, ConnPid} = one_connected_station(Ctx),
            wait_for(fun() -> has_member(Swim, NodeId) end, 500),
            Obs ! {macula_peering, disconnected, ConnPid, operator_stop},
            wait_for(fun() -> not has_member(Swim, NodeId) end, 500),
            ?assertEqual([], macula_station_peer_observer:peers(Obs))
        end
    end}.

%%==================================================================
%% Mutual peers: ONE direction down is not a disconnect
%%==================================================================
%%
%% A mutual station pair holds TWO conns, but SWIM stores exactly ONE
%% `conn_pid'. These cover the wiring that `macula_swim_three_arm_tests'
%% deliberately stubs out: that harness proves the MECHANISM by calling
%% `macula_swim:add_peer/3' itself, so it says nothing about whether
%% `on_disconnected/2' actually reaches `resync_swim_after_conn_loss/4' with
%% the surviving conn.
%%
%% ⚠ A STATION RESTART CANNOT COVER THIS. Restarting a station kills BOTH
%% directions, so the peer becomes isolated and is removed — that is the
%% `disconnected_removes_from_swim' path above, not this one. The regime here
%% needs one direction to die while the other keeps living, which no
%% up/down test of a whole station can produce.

%% The load-bearing case. Before the fix SWIM kept the dead pid forever,
%% `is_pid/1' being true for a dead pid, and every probe timed out into a
%% `confirmed_failed' about a station that was reachable the whole time.
one_direction_down_repoints_swim_at_the_survivor_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, Swim, NodeId, InPid, OutPid} = mutual_station(Ctx),
            %% `primary_conn_lookup/1' prefers inbound, so that is what SWIM
            %% was handed.
            ?assert(swim_alive(Swim, NodeId, InPid)),
            Obs ! {macula_peering, disconnected, InPid, quic_timeout},
            wait_for(fun() -> swim_alive(Swim, NodeId, OutPid) end, 500),
            ?assert(swim_alive(Swim, NodeId, OutPid)),
            ?assert(has_member(Swim, NodeId))
        end
    end}.

%% ⚠ THE GATE MUST SURVIVE THE RESYNC. `macula_swim:add_peer/3' upserts, so
%% re-pointing a DAEMON would add a member the capability gate exists to keep
%% out, silently re-polluting the membership that took a whole investigation to
%% drain. A daemon that was never in SWIM must stay out.
one_direction_down_for_a_daemon_stays_out_of_swim_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, Swim, NodeId, InPid, _OutPid} = mutual_peer(Ctx),
            Obs ! {is_station_resolved, NodeId, 0, {ok, false}},
            timer:sleep(150),
            ?assertNot(has_member(Swim, NodeId)),
            Obs ! {macula_peering, disconnected, InPid, quic_timeout},
            timer:sleep(200),
            ?assertNot(has_member(Swim, NodeId))
        end
    end}.

%% Isolation still removes. The resync must not have turned removal off.
both_directions_down_removes_a_mutual_station_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, Swim, NodeId, InPid, OutPid} = mutual_station(Ctx),
            Obs ! {macula_peering, disconnected, InPid, quic_timeout},
            wait_for(fun() -> swim_alive(Swim, NodeId, OutPid) end, 500),
            Obs ! {macula_peering, disconnected, OutPid, quic_timeout},
            wait_for(fun() -> not has_member(Swim, NodeId) end, 500),
            ?assertNot(has_member(Swim, NodeId))
        end
    end}.

%% The counter that makes a quiet fleet readable: if `conn_resynced' never
%% moves in production, the regime never occurred there and no amount of
%% steady-state observation has validated the fix.
resync_is_counted_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Swim, _NodeId, InPid, _OutPid} = mutual_station(Ctx),
            V0 = macula_station_peer_observer:swim_verdicts(Obs),
            ?assertEqual(0, maps:get(conn_resynced, V0, 0)),
            Obs ! {macula_peering, disconnected, InPid, quic_timeout},
            wait_for(fun() ->
                maps:get(conn_resynced,
                         macula_station_peer_observer:swim_verdicts(Obs), 0) > 0
            end, 500),
            V1 = macula_station_peer_observer:swim_verdicts(Obs),
            ?assertEqual(1, maps:get(conn_resynced, V1, 0))
        end
    end}.

%%==================================================================
%% Frame routing — SWIM frames with matching signature reach SWIM;
%% bad signatures get dropped; DHT-level frames bypass SWIM.
%%==================================================================

signed_swim_ping_reaches_swim_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Dht, _Swim, _NodeId, ConnPid} = one_connected_station(Ctx),
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
            {Obs, _Dht, _Swim, _NodeId, ConnPid} = one_connected_station(Ctx),
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

advertise_frame_on_a_dead_conn_is_not_registered_test_() ->
    %% Defensive: `route/4' already refuses a frame once
    %% `on_disconnected' has processed its ConnPid's death (it's no
    %% longer in `peers'), and empirically (10 runs, both message
    %% orderings tried) that guard alone seems to already be
    %% sufficient — a manufactured "frame arrives while ConnPid is
    %% dead but not yet reaped" scenario could not be made to reach
    %% `live_advertise_match(true, ...)` in this test harness. This
    %% test exercises the extra `is_process_alive' guard directly
    %% rather than trying to race it via message ordering: kept as a
    %% correctness floor (a dead pid must never be write-eligible,
    %% however that state is reached) even though the specific live
    %% race it was added for was never conclusively reproduced — see
    %% `conn_sweep_purges_dead_advertisers_test_' for the fix that
    %% *is* confirmed to close the actual observed bug (a stale entry
    %% sitting in the registry with an already-dead `conn_pid').
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra, adv_kp := AdvKp} = Ctx,
            AdvId   = macula_identity:public(AdvKp),
            AdvConn = spawn_dummy(),
            Realm   = <<7:256>>,
            Procedure = <<"_realm.membership.join_with_token_v1">>,
            Obs ! {macula_peering, connected, AdvConn, AdvId},
            wait_for_peers(Obs, 1, 500),
            exit(AdvConn, kill),
            wait_for(fun() -> not is_process_alive(AdvConn) end, 500),
            Frame = macula_frame:sign(
                      macula_frame:advertise(#{realm      => Realm,
                                               procedure  => Procedure,
                                               advertiser => AdvId}),
                      AdvKp),
            Obs ! {macula_peering, frame, AdvConn, Frame},
            timer:sleep(100),
            ?assertEqual({error, not_found},
                         macula_remote_advertise_registry:lookup(
                           Ra, Realm, Procedure))
        end
     end}.

conn_sweep_purges_dead_advertisers_test_() ->
    %% The actual, confirmed-live bug: a station's advertise registry
    %% held an entry whose `conn_pid' was already dead
    %% (`is_process_alive' => `false'), with nothing to ever notice or
    %% correct it — no process re-validates an existing entry's
    %% liveness on its own. `conn_sweep' already existed for the exact
    %% same class of problem in `last_frame_at' ("defensive, in case a
    %% DOWN message was lost") — this extends that same sweep to the
    %% advertise registry rather than depending on diagnosing exactly
    %% how the DOWN got lost. Registers directly against the registry
    %% (bypassing the observer's own connect/advertise message flow
    %% entirely) so the dead `conn_pid' is deterministic, not raced.
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra, adv_kp := AdvKp} = Ctx,
            AdvId = macula_identity:public(AdvKp),
            DeadConn = spawn(fun() -> ok end),
            wait_for(fun() -> not is_process_alive(DeadConn) end, 500),
            Realm     = <<7:256>>,
            Procedure = <<"_realm.membership.join_with_token_v1">>,
            ok = macula_remote_advertise_registry:register(
                   Ra, Realm, Procedure,
                   #{advertiser => AdvId, conn_pid => DeadConn, source => direct}),
            {ok, _} = macula_remote_advertise_registry:lookup(Ra, Realm, Procedure),
            Obs ! conn_sweep,
            wait_for(fun() ->
                {error, not_found} =:=
                    macula_remote_advertise_registry:lookup(Ra, Realm, Procedure)
            end, 500),
            ?assertEqual({error, not_found},
                         macula_remote_advertise_registry:lookup(Ra, Realm, Procedure))
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

direct_advertise_replaces_gossip_entry_test_() ->
    %% A station-flagged peer's ADVERTISE is gossip; if no entry
    %% exists it registers (source=gossip). When the actual daemon
    %% then connects directly and ADVERTISEs the same (Realm, Proc),
    %% the direct entry MUST replace the gossip entry (so CALL
    %% forwarding goes daemon-direct instead of via the relay).
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra, adv_kp := AdvKp} = Ctx,
            StationKp = macula_identity:generate(),
            StationId = macula_identity:public(StationKp),
            DaemonId  = macula_identity:public(AdvKp),
            StationConn = spawn_dummy(),
            DaemonConn  = spawn_dummy(),
            Realm    = <<7:256>>,
            Procedure = <<"_p.gossip_then_direct">>,
            %% Inject StationId → true into observer state BEFORE the
            %% connected notify, so the gossip path picks it up.
            Obs ! {macula_peering, connected, StationConn, StationId},
            wait_for_peers(Obs, 1, 500),
            sys:replace_state(Obs, fun(S) ->
                IsStMap = element(?IS_STATION_INDEX, S),
                setelement(?IS_STATION_INDEX, S, IsStMap#{StationId => true})
            end),
            %% Station gossips an ADVERTISE on behalf of itself (the
            %% relayer's advertiser field is its own NodeId, per the
            %% real router behaviour).
            advertise(Obs, StationConn, StationKp, Realm, Procedure),
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {ok, #{conn_pid := StationConn, source := gossip}} -> true;
                    _ -> false
                end
            end, 500),
            %% Now daemon connects directly and ADVERTISEs same key.
            Obs ! {macula_peering, connected, DaemonConn, DaemonId},
            wait_for_peers(Obs, 2, 500),
            advertise(Obs, DaemonConn, AdvKp, Realm, Procedure),
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {ok, #{conn_pid := DaemonConn, source := direct}} -> true;
                    _ -> false
                end
            end, 500),
            {ok, Entry} = macula_remote_advertise_registry:lookup(
                            Ra, Realm, Procedure),
            ?assertEqual(DaemonConn, maps:get(conn_pid, Entry)),
            ?assertEqual(DaemonId,   maps:get(advertiser, Entry)),
            ?assertEqual(direct,     maps:get(source, Entry))
        end
     end}.

gossip_does_not_replace_direct_entry_test_() ->
    %% Opposite of the above: with a direct entry already in place,
    %% a gossip ADVERTISE for the same key must NOT overwrite it
    %% (first-write-wins for gossip preserves the optimal route).
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra, adv_kp := AdvKp} = Ctx,
            StationKp = macula_identity:generate(),
            StationId = macula_identity:public(StationKp),
            DaemonId  = macula_identity:public(AdvKp),
            DaemonConn  = spawn_dummy(),
            StationConn = spawn_dummy(),
            Realm    = <<7:256>>,
            Procedure = <<"_p.direct_then_gossip">>,
            Obs ! {macula_peering, connected, DaemonConn, DaemonId},
            wait_for_peers(Obs, 1, 500),
            %% Direct first.
            advertise(Obs, DaemonConn, AdvKp, Realm, Procedure),
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {ok, #{conn_pid := DaemonConn, source := direct}} -> true;
                    _ -> false
                end
            end, 500),
            %% Station connects, flagged as station.
            Obs ! {macula_peering, connected, StationConn, StationId},
            wait_for_peers(Obs, 2, 500),
            sys:replace_state(Obs, fun(S) ->
                IsStMap = element(?IS_STATION_INDEX, S),
                setelement(?IS_STATION_INDEX, S, IsStMap#{StationId => true})
            end),
            %% Gossip ADVERTISE for the same key — must NOT win.
            advertise(Obs, StationConn, StationKp, Realm, Procedure),
            timer:sleep(100),
            {ok, Entry} = macula_remote_advertise_registry:lookup(
                            Ra, Realm, Procedure),
            ?assertEqual(DaemonConn, maps:get(conn_pid, Entry)),
            ?assertEqual(DaemonId,   maps:get(advertiser, Entry)),
            ?assertEqual(direct,     maps:get(source, Entry))
        end
     end}.

gossip_unadvertise_does_not_remove_direct_entry_test_() ->
    %% The unadvertise-side counterpart of gossip_does_not_replace_
    %% direct_entry_test_ above. A station-flagged peer's UNADVERTISE
    %% for a key we hold as a DIRECT (daemon-owned) entry must NOT
    %% remove it -- exactly the same protection ADVERTISE already had.
    %% Real scenario this guards: macula_station_peering_router's
    %% distance-vector gossip re-attributes every relayed frame to the
    %% relaying station's own SelfId, so a peer that transiently loses
    %% its OWN gossip-learned copy of an entry computes an honest
    %% UNADVERTISE diff and sends it back over the connection that
    %% ALSO carries our direct daemon's registration. Before this fix
    %% that echo passed `Adv =:= NodeId' exactly like a real direct
    %% unadvertise would and erased the daemon's entry outright.
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra, adv_kp := AdvKp} = Ctx,
            StationKp = macula_identity:generate(),
            StationId = macula_identity:public(StationKp),
            DaemonId  = macula_identity:public(AdvKp),
            DaemonConn  = spawn_dummy(),
            StationConn = spawn_dummy(),
            Realm     = <<7:256>>,
            Procedure = <<"_p.gossip_unadvertise_vs_direct">>,
            Obs ! {macula_peering, connected, DaemonConn, DaemonId},
            wait_for_peers(Obs, 1, 500),
            advertise(Obs, DaemonConn, AdvKp, Realm, Procedure),
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {ok, #{conn_pid := DaemonConn, source := direct}} -> true;
                    _ -> false
                end
            end, 500),
            Obs ! {macula_peering, connected, StationConn, StationId},
            wait_for_peers(Obs, 2, 500),
            sys:replace_state(Obs, fun(S) ->
                IsStMap = element(?IS_STATION_INDEX, S),
                setelement(?IS_STATION_INDEX, S, IsStMap#{StationId => true})
            end),
            %% Station "echoes" an UNADVERTISE claiming itself -- the
            %% same shape a real distance-vector gossip drop takes.
            unadvertise(Obs, StationConn, StationKp, Realm, Procedure),
            timer:sleep(100),
            {ok, Entry} = macula_remote_advertise_registry:lookup(
                            Ra, Realm, Procedure),
            ?assertEqual(DaemonConn, maps:get(conn_pid, Entry)),
            ?assertEqual(direct,     maps:get(source, Entry))
        end
     end}.

gossip_unadvertise_removes_gossip_entry_test_() ->
    %% The gate must not become a one-way valve: a gossip-sourced entry
    %% still needs to be retractable by a gossip UNADVERTISE from the
    %% SAME relaying station, or a legitimate upstream removal would
    %% never propagate.
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra} = Ctx,
            StationKp = macula_identity:generate(),
            StationId = macula_identity:public(StationKp),
            StationConn = spawn_dummy(),
            Realm     = <<7:256>>,
            Procedure = <<"_p.gossip_unadvertise_removes_gossip">>,
            Obs ! {macula_peering, connected, StationConn, StationId},
            wait_for_peers(Obs, 1, 500),
            sys:replace_state(Obs, fun(S) ->
                IsStMap = element(?IS_STATION_INDEX, S),
                setelement(?IS_STATION_INDEX, S, IsStMap#{StationId => true})
            end),
            advertise(Obs, StationConn, StationKp, Realm, Procedure),
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {ok, #{source := gossip}} -> true;
                    _ -> false
                end
            end, 500),
            unadvertise(Obs, StationConn, StationKp, Realm, Procedure),
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {error, not_found} -> true;
                    _ -> false
                end
            end, 500)
        end
     end}.

reconnect_same_nodeid_purges_stale_advertise_test_() ->
    %% Same-NodeId reconnect with a different ConnPid must evict the
    %% old pid's remote-advertise entries synchronously, so a CALL
    %% arriving immediately after the new `connected' notify finds an
    %% empty registry instead of forwarding to the dead OldConnPid.
    %% Closes the "watchtower restart → harness needs stub bounce"
    %% regression.
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra, adv_kp := AdvKp} = Ctx,
            AdvId    = macula_identity:public(AdvKp),
            OldConn  = spawn_dummy(),
            Realm    = <<7:256>>,
            Procedure = <<"_p.reconnect">>,
            %% Round 1: connect + advertise.
            Obs ! {macula_peering, connected, OldConn, AdvId},
            wait_for_peers(Obs, 1, 500),
            advertise(Obs, OldConn, AdvKp, Realm, Procedure),
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {ok, #{conn_pid := OldConn}} -> true;
                    _ -> false
                end
            end, 500),
            %% Round 2: SAME NodeId reconnects on a fresh ConnPid
            %% WITHOUT a `disconnected' for OldConn ever firing.
            %% Without the purge fix, the registry entry survives
            %% pointing at OldConn — any inbound CALL would forward
            %% to a Pid the SDK already considers dead.
            NewConn = spawn_dummy(),
            Obs ! {macula_peering, connected, NewConn, AdvId},
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {error, not_found} -> true;
                    _ -> false
                end
            end, 500),
            ?assertEqual(
               {error, not_found},
               macula_remote_advertise_registry:lookup(Ra, Realm, Procedure))
        end
     end}.

reconnect_same_pid_is_idempotent_test_() ->
    %% Refiring `connected' for the SAME (NodeId, ConnPid) must not
    %% purge the live advertise entries — the purge is gated on
    %% pid-mismatch.
    {setup, fun setup_with_advertise/0, fun teardown_with_advertise/1,
     fun(Ctx) ->
        fun() ->
            #{obs := Obs, ra := Ra, adv_kp := AdvKp} = Ctx,
            AdvId    = macula_identity:public(AdvKp),
            Conn     = spawn_dummy(),
            Realm    = <<7:256>>,
            Procedure = <<"_p.idempotent">>,
            Obs ! {macula_peering, connected, Conn, AdvId},
            wait_for_peers(Obs, 1, 500),
            advertise(Obs, Conn, AdvKp, Realm, Procedure),
            wait_for(fun() ->
                case macula_remote_advertise_registry:lookup(
                       Ra, Realm, Procedure) of
                    {ok, _} -> true;
                    _ -> false
                end
            end, 500),
            %% Refire same (Conn, AdvId) — entry must survive.
            Obs ! {macula_peering, connected, Conn, AdvId},
            timer:sleep(50),
            ?assertMatch(
               {ok, #{conn_pid := Conn}},
               macula_remote_advertise_registry:lookup(Ra, Realm, Procedure))
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

%% Reach into the observer's state record.
%%
%% State record layout (1-indexed within the tuple, after the
%% record-name atom at slot 1):
%%   2: dht,  3: swim,  4: handler_registry,  5: pubsub_registry,
%%   6: remote_advertise,  7: self_id,  8: identity,  9: peers,
%%  10: conns,  11: direction_of_pid,  12: forwarded
%% Update this index (and `?IS_STATION_INDEX' above) if the record
%% layout changes.
forwarded_size(Obs) ->
    State = sys:get_state(Obs),
    F = element(12, State),
    map_size(F).

advertise(Obs, AdvConn, AdvKp, Realm, Procedure) ->
    AdvId = macula_identity:public(AdvKp),
    Frame = macula_frame:sign(
              macula_frame:advertise(#{realm      => Realm,
                                       procedure  => Procedure,
                                       advertiser => AdvId}),
              AdvKp),
    Obs ! {macula_peering, frame, AdvConn, Frame}.

unadvertise(Obs, AdvConn, AdvKp, Realm, Procedure) ->
    AdvId = macula_identity:public(AdvKp),
    Frame = macula_frame:sign(
              macula_frame:unadvertise(#{realm      => Realm,
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

%% Connect only. Since the capability gate, an INBOUND peer is not observed
%% into the DHT and not added to SWIM until its capability probe resolves it as
%% a station, so waiting on `macula_dht:size/1' here would hang forever. Wait on
%% the conns map, which the connect path still populates. Tests that need the
%% peer to BE a station call `resolve_as_station/2'.
one_connected_peer(#{obs := Obs, dht := Dht, swim := Swim,
                     peer_kp := PeerKp}) ->
    NodeId  = macula_identity:public(PeerKp),
    ConnPid = spawn_dummy(),
    Obs ! {macula_peering, connected, ConnPid, NodeId},
    wait_for(fun() -> macula_station_peer_observer:peers(Obs) =/= [] end, 500),
    {Obs, Dht, Swim, NodeId, ConnPid}.

%% Drive the deferred capability answer, as the resolver would.
resolve_as_station(Obs, NodeId) ->
    Obs ! {is_station_resolved, NodeId, 0, {ok, true}},
    ok.

%% Connect AND resolve as a station: the pre-gate meaning of "connected".
one_connected_station(Ctx) ->
    {Obs, Dht, Swim, NodeId, ConnPid} = one_connected_peer(Ctx),
    resolve_as_station(Obs, NodeId),
    wait_for(fun() -> macula_dht:size(Dht) =:= 1 end, 500),
    {Obs, Dht, Swim, NodeId, ConnPid}.

%% A mutual pair: the peer dialled us AND we dialled it, so both slots are
%% populated for one NodeId. Not resolved as a station — callers decide.
mutual_peer(#{obs := Obs, swim := Swim, peer_kp := PeerKp}) ->
    NodeId = macula_identity:public(PeerKp),
    InPid  = spawn_dummy(),
    OutPid = spawn_dummy(),
    Obs ! {macula_peering, connected, InPid, NodeId},
    Obs ! {macula_peering, connected_outbound, OutPid, NodeId, []},
    wait_for(fun() -> macula_station_peer_observer:peers(Obs) =/= [] end, 500),
    {Obs, Swim, NodeId, InPid, OutPid}.

mutual_station(Ctx) ->
    {Obs, Swim, NodeId, InPid, OutPid} = mutual_peer(Ctx),
    resolve_as_station(Obs, NodeId),
    wait_for(fun() -> swim_alive(Swim, NodeId, InPid) end, 500),
    {Obs, Swim, NodeId, InPid, OutPid}.

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

%%==================================================================
%% Isolation eviction is DAEMON-ONLY.
%%
%% Delete-on-disconnect was shipped to stop daemon entries leaking and
%% growing macula_dht's mailbox without bound. Applied to a STATION it
%% is a ratchet instead: station entries are expensive to rebuild and
%% almost nothing on this fleet rebuilds one, so every disconnect
%% shrinks the table permanently. It becomes actively harmful the
%% moment anything dials, because a successful-then-dropped dial would
%% delete the entry the dial just created.
%%==================================================================

%% Since the capability gate this is stronger than "forgotten on isolation":
%% a daemon never ENTERS the routing table at all, so there is nothing to
%% forget. The daemon-only clause in `maybe_forget_if_isolated/4' is now
%% belt-and-braces rather than the thing doing the work.
isolated_daemon_never_enters_the_dht_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, Dht, _Swim, NodeId, ConnPid} = one_connected_peer(Ctx),
            Obs ! {is_station_resolved, NodeId, 0, {ok, false}},
            timer:sleep(150),
            ?assertEqual(error, macula_dht:find(Dht, NodeId)),
            ?assertEqual(0, macula_dht:size(Dht)),
            %% and disconnecting it is still harmless
            Obs ! {macula_peering, disconnected, ConnPid, peer_closed},
            wait_for(fun() ->
                macula_station_peer_observer:peers(Obs) =:= [] end, 500),
            ?assertEqual(error, macula_dht:find(Dht, NodeId))
        end
    end}.

isolated_station_is_kept_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, Dht, _Swim, NodeId, ConnPid} = one_connected_station(Ctx),
            sys:replace_state(Obs, fun(S) ->
                IsStMap = element(?IS_STATION_INDEX, S),
                setelement(?IS_STATION_INDEX, S, IsStMap#{NodeId => true})
            end),
            Obs ! {macula_peering, disconnected, ConnPid, peer_closed},
            %% Wait on an observable the disconnect DOES change, so this
            %% cannot pass merely by racing ahead of the handler.
            wait_for(fun() ->
                macula_station_peer_observer:peers(Obs) =:= []
            end, 500),
            ?assertMatch({ok, _}, macula_dht:find(Dht, NodeId)),
            ?assertEqual(1, macula_dht:size(Dht))
        end
    end}.

%%==================================================================
%% SWIM verdicts are TALLIED, and corroborated against our own conns.
%%
%% macula_swim pushed alive/suspect/confirmed_failed transitions to its
%% controlling pid, which is this process, and until 2026-07-27 there
%% was no clause for the message: the whole output of an adaptive
%% failure detector fell into the catch-all handle_info. These tests
%% pin that it is now consumed, and that a confirmed_failed for a peer
%% we still hold a live conn to is counted separately as a suspected
%% false positive. Nothing here asserts an ACTION, deliberately: the
%% accuracy of the verdicts is unknown and acting on a signal of
%% unknown accuracy turns a failure detector into an amplifier.
%%==================================================================

swim_verdict_is_tallied_not_dropped_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(#{obs := Obs}) ->
        fun() ->
            ?assertEqual(#{}, macula_station_peer_observer:swim_verdicts(Obs)),
            Obs ! {macula_swim, member_state, random_node_id(), suspect},
            wait_for(fun() ->
                maps:get(suspect,
                         macula_station_peer_observer:swim_verdicts(Obs), 0) =:= 1
            end, 500),
            ?assertEqual(1, maps:get(suspect,
                macula_station_peer_observer:swim_verdicts(Obs), 0))
        end
    end}.

%% The measurement that matters: SWIM says dead, we still hold the conn.
swim_confirmed_failed_with_live_conn_is_flagged_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Dht, _Swim, NodeId, _ConnPid} = one_connected_station(Ctx),
            Obs ! {macula_swim, member_state, NodeId, confirmed_failed},
            wait_for(fun() ->
                maps:get(confirmed_live_conn,
                         macula_station_peer_observer:swim_verdicts(Obs), 0) =:= 1
            end, 500),
            V = macula_station_peer_observer:swim_verdicts(Obs),
            ?assertEqual(1, maps:get(confirmed_live_conn, V, 0)),
            ?assertEqual(0, maps:get(confirmed_no_conn, V, 0)),
            %% and the peer is STILL connected — observation only.
            ?assertMatch({ok, _},
                         macula_station_peer_observer:conn_for(Obs, NodeId))
        end
    end}.

swim_confirmed_failed_without_conn_is_corroborated_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(#{obs := Obs}) ->
        fun() ->
            Unknown = random_node_id(),
            Obs ! {macula_swim, member_state, Unknown, confirmed_failed},
            wait_for(fun() ->
                maps:get(confirmed_no_conn,
                         macula_station_peer_observer:swim_verdicts(Obs), 0) =:= 1
            end, 500),
            V = macula_station_peer_observer:swim_verdicts(Obs),
            ?assertEqual(1, maps:get(confirmed_no_conn, V, 0)),
            ?assertEqual(0, maps:get(confirmed_live_conn, V, 0))
        end
    end}.

%%==================================================================
%% ⚠ AN OBSERVER RESTART MUST RECOVER THE CONNECTIONS THAT NEVER DIED
%%==================================================================
%%
%% On 2026-08-05 station-de-frankfurt's observer died on a timed-out
%% `gen_server:call' into `macula_handler_registry'. The supervisor restarted
%% it; `init/1' recreated the public conns ETS mirror EMPTY; and pubsub
%% fan-out, which resolves subscriber connections through that mirror, was
%% blind to every already-connected client for the rest of the node's life.
%% Publishes were accepted and delivered to nobody. `/health' said healthy.
%%
%% The observer already rebuilt its OUTBOUND view from `macula_station_peer_links'
%% and its own comment called restart "a non-issue". It was a non-issue only for
%% the peers this station dials. Nothing remembered the peers that dialled US.
%% The listener does, and is now asked.

observer_restart_recovery_test_() ->
    {foreach,
     fun() -> application:ensure_all_started(crypto), ok end,
     fun(_) -> catch gen_server:stop(macula_station_listener), ok end,
     [fun a_restarted_observer_absorbs_live_inbound_accepts/0,
      fun a_dead_inbound_worker_is_not_absorbed/0,
      fun no_listener_is_not_a_crash/0]}.

%% The one that would have caught the outage.
a_restarted_observer_absorbs_live_inbound_accepts() ->
    NodeId  = peer_node_id(),
    ConnPid = spawn(fun() -> receive stop -> ok end end),
    {ok, _L} = stub_listener:start_link([{NodeId, ConnPid}]),
    Obs = start_bare_observer(),
    ?assertMatch([{NodeId, #{inbound := ConnPid}}],
                 ets:lookup(macula_station_peer_observer_conns, NodeId)),
    ConnPid ! stop,
    gen_server:stop(Obs).

%% A worker that died between the listener answering and the fold running is
%% skipped rather than installed, so fan-out never resolves to a dead pid.
a_dead_inbound_worker_is_not_absorbed() ->
    NodeId  = peer_node_id(),
    ConnPid = spawn(fun() -> ok end),
    _ = wait_dead(ConnPid),
    {ok, _L} = stub_listener:start_link([{NodeId, ConnPid}]),
    Obs = start_bare_observer(),
    ?assertEqual([], ets:lookup(macula_station_peer_observer_conns, NodeId)),
    gen_server:stop(Obs).

%% First boot: the observer starts before the listener exists. Nothing to
%% reconcile is the right answer, and it must not be a crash.
no_listener_is_not_a_crash() ->
    Obs = start_bare_observer(),
    ?assertEqual(0, ets:info(macula_station_peer_observer_conns, size)),
    gen_server:stop(Obs).

start_bare_observer() ->
    SelfKp = macula_identity:generate(),
    SelfId = maps:get(public, SelfKp),
    {ok, Dht}  = macula_dht:start_link(#{self_id => SelfId}),
    {ok, Swim} = macula_swim:start_link(#{self_node_id    => SelfId,
                                          identity        => SelfKp,
                                          controlling_pid => self()}),
    {ok, Obs}  = macula_station_peer_observer:start_link(#{dht => Dht, swim => Swim}),
    Obs.

peer_node_id() -> maps:get(public, macula_identity:generate()).

wait_dead(Pid) ->
    case is_process_alive(Pid) of
        false -> ok;
        true  -> timer:sleep(5), wait_dead(Pid)
    end.

%%==================================================================
%% Phase 3.5 overlay_relay instrumentation.
%%
%% Both branches below were completely unobservable before this: a real
%% incident (overlay_relay silently failing on the live fleet while CI's
%% local test-cluster suite stayed green) could not be diagnosed because
%% nothing distinguished "bad signature" from "target not connected"
%% from outside a live trace. These tests are the regression coverage
%% for the fix, driven the same way the rest of this file drives
%% dispatch: inject a real `{macula_peering, frame, ConnPid, Frame}'
%% message into a real observer with a genuinely connected peer, no
%% QUIC involved.
%%==================================================================

overlay_relay_bad_signature_is_counted_and_logged_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Dht, _Swim, _NodeId, ConnPid} = one_connected_peer(Ctx),
            %% Signed with a THIRD identity, not the one that connected
            %% ConnPid — `macula_frame:verify/2' checks the envelope
            %% against the connection's own authenticated NodeId, so
            %% this is a genuine signature mismatch, not a forged frame
            %% shape.
            Target = peer_node_id(),
            Impostor = macula_identity:generate(),
            Frame = macula_frame:sign(
                      macula_frame:overlay_relay(
                        #{peer => Target, payload => <<"irrelevant">>}),
                      Impostor),
            Before = macula_station_peer_observer:overlay_relay_stats(),
            Obs ! {macula_peering, frame, ConnPid, Frame},
            wait_for(fun() ->
                maps:get(verify_failed,
                         macula_station_peer_observer:overlay_relay_stats())
                  =:= maps:get(verify_failed, Before) + 1
            end, 500),
            After = macula_station_peer_observer:overlay_relay_stats(),
            ?assertEqual(maps:get(relayed, Before), maps:get(relayed, After)),
            ?assertEqual(maps:get(target_not_connected, Before),
                         maps:get(target_not_connected, After))
        end
    end}.

overlay_relay_target_not_connected_is_counted_and_logged_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        fun() ->
            {Obs, _Dht, _Swim, _NodeId, ConnPid} = one_connected_peer(Ctx),
            PeerKp = maps:get(peer_kp, Ctx),
            %% Signed by the SAME identity that connected ConnPid, so
            %% this verifies cleanly — the drop is purely because
            %% `Target' has no entry in `conns' at all.
            Target = peer_node_id(),
            Frame = macula_frame:sign(
                      macula_frame:overlay_relay(
                        #{peer => Target, payload => <<"irrelevant">>}),
                      PeerKp),
            Before = macula_station_peer_observer:overlay_relay_stats(),
            Obs ! {macula_peering, frame, ConnPid, Frame},
            wait_for(fun() ->
                maps:get(target_not_connected,
                         macula_station_peer_observer:overlay_relay_stats())
                  =:= maps:get(target_not_connected, Before) + 1
            end, 500),
            After = macula_station_peer_observer:overlay_relay_stats(),
            ?assertEqual(maps:get(relayed, Before), maps:get(relayed, After)),
            ?assertEqual(maps:get(verify_failed, Before),
                         maps:get(verify_failed, After))
        end
    end}.
