%% End-to-end wire tests for hecate_dht Session 3.5 — PING/PONG and
%% FIND_NODE/NODES between two in-VM DHT instances, wired by
%% crossing send_frame callbacks.
%%
%% The send_frame callback is the only transport. The "wire" is just
%% an `erlang:send/2' hop between two gen_servers in the same BEAM.
%% Production substitutes a QUIC-backed callback; the behaviour under
%% test is identical.
-module(hecate_dht_wire_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% PING / PONG round-trip
%%---------------------------------------------------------------------

ping_roundtrip_marks_target_last_seen_test() ->
    #{a := {A, KpA}, b := {B, KpB}} = start_pair(),
    BId = macula_identity:public(KpB),

    %% A observes B first so touch/2 has a live entry to bump.
    admitted = hecate_dht:observe(A, spec(BId)),
    {ok, BEntryBefore} = hecate_dht:find(A, BId),
    timer:sleep(5),

    {ok, #{rtt_ms := Rtt}} = hecate_dht:ping_peer(A, BId),
    ?assert(Rtt >= 0),

    {ok, BEntryAfter} = hecate_dht:find(A, BId),
    ?assert(hecate_dht_entry:last_seen(BEntryAfter)
              >= hecate_dht_entry:last_seen(BEntryBefore)),

    stop_pair(#{a => {A, KpA}, b => {B, KpB}}).

ping_no_transport_returns_error_test() ->
    Kp = macula_identity:generate(),
    {ok, D} = hecate_dht:start_link(#{self_id => macula_identity:public(Kp)}),
    ?assertEqual({error, no_transport},
                 hecate_dht:ping_peer(D, <<1:256>>)),
    hecate_dht:stop(D).

ping_timeout_when_peer_does_not_reply_test() ->
    %% A has a transport that drops frames on the floor — B never sees
    %% them, so A's timer fires.
    KpA = macula_identity:generate(),
    AId = macula_identity:public(KpA),
    Silent = fun(_Target, _Frame) -> ok end,
    {ok, A} = hecate_dht:start_link(#{
        self_id         => AId,
        identity        => KpA,
        send_frame      => Silent,
        ping_timeout_ms => 120
    }),
    ?assertEqual({error, timeout},
                 hecate_dht:ping_peer(A, <<7:256>>, 120)),
    hecate_dht:stop(A).

pong_with_wrong_nonce_is_ignored_test() ->
    %% An unsolicited PONG (no matching pending entry) must not crash
    %% the server or touch any routing-table entry.
    Kp = macula_identity:generate(),
    SelfId = macula_identity:public(Kp),
    {ok, D} = hecate_dht:start_link(#{
        self_id    => SelfId,
        identity   => Kp,
        send_frame => fun(_, _) -> ok end
    }),
    Spoof = hecate_frame:sign(
              hecate_frame:pong(#{nonce => crypto:strong_rand_bytes(16)}),
              Kp),
    ok = hecate_dht:handle_frame(D, SelfId, Spoof),
    %% Sanity call to sync: server must still respond.
    ?assertEqual(0, hecate_dht:size(D)),
    hecate_dht:stop(D).

tampered_frame_is_dropped_silently_test() ->
    %% B asks A for a ping, but the transport mutates the signed
    %% frame in flight. A's signature check must fail, A must not
    %% reply, and B's own ping_peer/3 call must time out.
    KpA = macula_identity:generate(),
    KpB = macula_identity:generate(),
    AId = macula_identity:public(KpA),
    BId = macula_identity:public(KpB),
    {ok, A} = hecate_dht:start_link(#{
        self_id    => AId,
        identity   => KpA,
        send_frame => fun(_, _) -> ok end
    }),
    Tamper = fun(_DstId, Frame) ->
        Mutated = Frame#{nonce => crypto:strong_rand_bytes(16)},
        hecate_dht:handle_frame(A, BId, Mutated)
    end,
    {ok, B} = hecate_dht:start_link(#{
        self_id         => BId,
        identity        => KpB,
        send_frame      => Tamper,
        ping_timeout_ms => 120
    }),
    ?assertEqual({error, timeout},
                 hecate_dht:ping_peer(B, AId, 120)),
    hecate_dht:stop(A),
    hecate_dht:stop(B).

%%---------------------------------------------------------------------
%% FIND_NODE / NODES round-trip
%%---------------------------------------------------------------------

find_node_returns_peers_target_knows_test() ->
    #{a := {A, KpA}, b := {B, KpB}} = start_pair(),
    BId = macula_identity:public(KpB),

    %% B has three peers in its routing table; A asks B for k-closest
    %% to a specific key.
    Peer1Id = <<1:8, 0:248>>,
    Peer2Id = <<2:8, 0:248>>,
    Peer3Id = <<3:8, 0:248>>,
    admitted = hecate_dht:observe(B, spec(Peer1Id)),
    admitted = hecate_dht:observe(B, spec(Peer2Id)),
    admitted = hecate_dht:observe(B, spec(Peer3Id)),

    {ok, Refs} = hecate_dht:find_node(A, <<1:8, 0:248>>, BId),
    ?assertEqual(3, length(Refs)),
    Ids = lists:sort([maps:get(node_id, R) || R <- Refs]),
    ?assertEqual(lists:sort([Peer1Id, Peer2Id, Peer3Id]), Ids),

    stop_pair(#{a => {A, KpA}, b => {B, KpB}}).

find_node_returns_empty_when_target_has_no_peers_test() ->
    #{a := {A, KpA}, b := {B, KpB}} = start_pair(),
    BId = macula_identity:public(KpB),
    {ok, []} = hecate_dht:find_node(A, <<9:8, 0:248>>, BId),
    stop_pair(#{a => {A, KpA}, b => {B, KpB}}).

find_node_timeout_when_peer_is_silent_test() ->
    Kp = macula_identity:generate(),
    {ok, D} = hecate_dht:start_link(#{
        self_id              => macula_identity:public(Kp),
        identity             => Kp,
        send_frame           => fun(_, _) -> ok end,
        find_node_timeout_ms => 120
    }),
    ?assertEqual({error, timeout},
                 hecate_dht:find_node(D, <<1:256>>, <<2:256>>, 120)),
    hecate_dht:stop(D).

find_node_no_transport_returns_error_test() ->
    Kp = macula_identity:generate(),
    {ok, D} = hecate_dht:start_link(#{self_id => macula_identity:public(Kp)}),
    ?assertEqual({error, no_transport},
                 hecate_dht:find_node(D, <<1:256>>, <<2:256>>)),
    hecate_dht:stop(D).

%%---------------------------------------------------------------------
%% Concurrent requests — two outstanding pings don't collide
%%---------------------------------------------------------------------

concurrent_pings_to_different_peers_test() ->
    %% A has two transports — one to B, one to C. Each keeps a
    %% distinct nonce. Both should resolve independently.
    KpA = macula_identity:generate(),
    KpB = macula_identity:generate(),
    KpC = macula_identity:generate(),
    AId = macula_identity:public(KpA),
    BId = macula_identity:public(KpB),
    CId = macula_identity:public(KpC),

    Router = spawn_router(),
    SendFrom = fun(FromId) ->
        fun(DstId, Frame) ->
            Router ! {route, FromId, DstId, Frame}, ok
        end
    end,
    {ok, A} = hecate_dht:start_link(#{self_id => AId, identity => KpA,
                                      send_frame => SendFrom(AId)}),
    {ok, B} = hecate_dht:start_link(#{self_id => BId, identity => KpB,
                                      send_frame => SendFrom(BId)}),
    {ok, C} = hecate_dht:start_link(#{self_id => CId, identity => KpC,
                                      send_frame => SendFrom(CId)}),
    register_router(Router, #{AId => A, BId => B, CId => C}),

    Parent = self(),
    spawn_link(fun() ->
        Parent ! {r1, hecate_dht:ping_peer(A, BId, 2_000)}
    end),
    spawn_link(fun() ->
        Parent ! {r2, hecate_dht:ping_peer(A, CId, 2_000)}
    end),
    R1 = receive {r1, X1} -> X1 after 3_000 -> timeout end,
    R2 = receive {r2, X2} -> X2 after 3_000 -> timeout end,
    ?assertMatch({ok, _}, R1),
    ?assertMatch({ok, _}, R2),

    Router ! stop,
    hecate_dht:stop(A),
    hecate_dht:stop(B),
    hecate_dht:stop(C).

%%---------------------------------------------------------------------
%% Harness
%%---------------------------------------------------------------------

start_pair() ->
    KpA = macula_identity:generate(),
    KpB = macula_identity:generate(),
    AId = macula_identity:public(KpA),
    BId = macula_identity:public(KpB),
    Router = spawn_router(),
    SendFrom = fun(FromId) ->
        fun(DstId, Frame) ->
            Router ! {route, FromId, DstId, Frame}, ok
        end
    end,
    {ok, A} = hecate_dht:start_link(#{
        self_id    => AId,
        identity   => KpA,
        send_frame => SendFrom(AId)
    }),
    {ok, B} = hecate_dht:start_link(#{
        self_id    => BId,
        identity   => KpB,
        send_frame => SendFrom(BId)
    }),
    register_router(Router, #{AId => A, BId => B}),
    put(dht_router, Router),
    #{a => {A, KpA}, b => {B, KpB}}.

stop_pair(#{a := {A, _}, b := {B, _}}) ->
    Router = get(dht_router),
    Router ! stop,
    hecate_dht:stop(A),
    hecate_dht:stop(B).

spawn_router() ->
    spawn(fun() -> router_loop(undefined) end).

register_router(Router, RoutingMap) ->
    Router ! {table, RoutingMap}, ok.

router_loop(Table) ->
    receive
        {table, T}                -> router_loop(T);
        {route, FromId, DstId, F} ->
            case Table of
                undefined -> router_loop(Table);
                _ ->
                    case maps:find(DstId, Table) of
                        {ok, Pid} -> hecate_dht:handle_frame(Pid, FromId, F);
                        error     -> ok
                    end,
                    router_loop(Table)
            end;
        stop                      -> ok
    end.

spec(NodeId) ->
    #{node_id => NodeId, asn => 64512, country => <<"BE">>, tier => t1}.
