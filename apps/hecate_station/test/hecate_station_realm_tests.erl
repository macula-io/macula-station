%% @doc Per-realm gen_server unit tests.
%%
%% Uses a capture closure as `send_fun' so each outbound
%% `{send, NodeId, Frame}' action produced by the overlay / plumtree
%% modules lands in an ETS table the test can inspect. No QUIC, no
%% peering workers — this exercises the gen_server wiring + the
%% HyParView view + Plumtree state transitions end-to-end.
%%
%% Each test is self-contained: the realm gen_server is started + torn
%% down in the test body so `notify => self()' refers to the
%% executing test process (eunit setup/instantiator and test bodies
%% run in different processes).
-module(hecate_station_realm_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("hecate_station/include/hecate_station_cfg.hrl").

%%==================================================================
%% Membership
%%==================================================================

add_peer_populates_active_view_test() ->
    Ctx  = fixture(),
    Peer = rand_id(),
    ok   = hecate_station_realm:add_peer(realm(Ctx), Peer),
    ?assertEqual([Peer], hecate_station_realm:active_peers(realm(Ctx))),
    ?assert(hecate_station_realm:is_active(realm(Ctx), Peer)),
    teardown(Ctx).

remove_peer_drops_from_view_test() ->
    Ctx  = fixture(),
    Peer = rand_id(),
    ok = hecate_station_realm:add_peer(realm(Ctx), Peer),
    ok = hecate_station_realm:remove_peer(realm(Ctx), Peer),
    ?assertNot(hecate_station_realm:is_active(realm(Ctx), Peer)),
    ?assertEqual([], hecate_station_realm:active_peers(realm(Ctx))),
    teardown(Ctx).

%%==================================================================
%% Plumtree — local publish pushes GOSSIP to every eager peer.
%%==================================================================

publish_gossips_to_eager_peers_test() ->
    Ctx = fixture(),
    P1  = rand_id(),
    P2  = rand_id(),
    ok  = hecate_station_realm:add_peer(realm(Ctx), P1),
    ok  = hecate_station_realm:add_peer(realm(Ctx), P2),
    MsgId = <<1:128>>,
    ok = hecate_station_realm:publish(realm(Ctx), MsgId, <<"hello">>),
    Sent    = wait_sent(Ctx, 2, 500),
    Targets = lists:usort([T || {_Seq, T, _F} <- Sent]),
    ?assertEqual(lists:usort([P1, P2]), Targets),
    [?assertEqual(plumtree_gossip, macula_frame:frame_type(F))
     || {_, _, F} <- Sent],
    teardown(Ctx).

%%==================================================================
%% HyParView — inbound JOIN promotes the sender to the active view.
%%==================================================================

inbound_join_admits_sender_into_active_view_test() ->
    Ctx      = fixture(),
    JoinerKp = macula_identity:generate(),
    Joiner   = macula_identity:public(JoinerKp),
    R        = realm_id(Ctx),
    JoinFrame = hecate_overlay_proto:build_join(#{
        self_id  => Joiner,
        realm    => R,
        identity => JoinerKp
    }),
    ok = hecate_station_realm:handle_frame(realm(Ctx), Joiner, JoinFrame),
    wait_until(fun() ->
        hecate_station_realm:is_active(realm(Ctx), Joiner)
    end, 500),
    ?assert(hecate_station_realm:is_active(realm(Ctx), Joiner)),
    teardown(Ctx).

%%==================================================================
%% Plumtree — inbound GOSSIP delivers locally exactly once.
%%==================================================================

inbound_gossip_delivers_once_test() ->
    Ctx      = fixture(),
    R        = realm_id(Ctx),
    SenderKp = macula_identity:generate(),
    Sender   = macula_identity:public(SenderKp),
    MsgId    = <<42:128>>,
    Gossip0  = macula_frame:plumtree_gossip(#{
        realm   => R,
        msg_id  => MsgId,
        round   => 0,
        payload => <<"ping">>
    }),
    Gossip   = macula_frame:sign(Gossip0, SenderKp),
    ok = hecate_station_realm:handle_frame(realm(Ctx), Sender, Gossip),
    ?assertMatch({realm, delivery, MsgId, <<"ping">>},
                 wait_delivery(MsgId, 500)),
    teardown(Ctx).

%%==================================================================
%% Fixture
%%==================================================================

fixture() ->
    _       = application:ensure_all_started(crypto),
    Tab     = ets:new(?MODULE, [set, public]),
    SelfKp  = macula_identity:generate(),
    RealmId = <<99:256>>,
    Notify  = self(),
    SendFun = fun(NodeId, Frame) ->
                  ets:insert(Tab, {erlang:unique_integer([monotonic]),
                                   NodeId, Frame}),
                  ok
              end,
    RealmCfg = #realm_cfg{realm_id = RealmId,
                          active_view_size = 5,
                          passive_view_size = 20,
                          plumtree_fanout = 3},
    {ok, Realm} = hecate_station_realm:start_link(#{
        realm_cfg => RealmCfg,
        identity  => SelfKp,
        send_fun  => SendFun,
        notify    => Notify
    }),
    #{realm    => Realm,
      realm_id => RealmId,
      tab      => Tab,
      identity => SelfKp}.

teardown(#{realm := Realm, tab := Tab}) ->
    _ = catch hecate_station_realm:stop(Realm),
    _ = catch ets:delete(Tab),
    ok.

realm(#{realm := Realm})         -> Realm.
realm_id(#{realm_id := R})       -> R.

wait_sent(#{tab := Tab}, N, Ms) ->
    wait_sent_step(ets:info(Tab, size), Tab, N, Ms).

wait_sent_step(Size, Tab, N, _Ms) when Size >= N ->
    ets:tab2list(Tab);
wait_sent_step(_Size, _Tab, _N, Ms) when Ms =< 0 ->
    error(sent_timeout);
wait_sent_step(_Size, Tab, N, Ms) ->
    timer:sleep(20),
    wait_sent_step(ets:info(Tab, size), Tab, N, Ms - 20).

%% Wait for a specific MsgId — earlier tests in the same eunit
%% process may have left other deliveries + membership events in
%% our mailbox.
wait_delivery(MsgId, Ms) ->
    receive
        {hecate_station_realm, {realm, delivery, MsgId, _} = Msg} -> Msg;
        {hecate_station_realm, _Other}                            -> wait_delivery(MsgId, Ms)
    after Ms ->
        error(delivery_timeout)
    end.

wait_until(Pred, Ms) ->
    wait_until_step(Pred(), Pred, Ms).

wait_until_step(true,  _Pred, _Ms)             -> ok;
wait_until_step(false, Pred, Ms) when Ms =< 0  -> ?assert(Pred());
wait_until_step(false, Pred, Ms)               ->
    timer:sleep(20),
    wait_until_step(Pred(), Pred, Ms - 20).

rand_id() -> crypto:strong_rand_bytes(32).
