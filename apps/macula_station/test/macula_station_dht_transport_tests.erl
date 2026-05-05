%% EUnit tests for macula_station_dht_transport.
%%
%% Stateless adapter: feed it a fake observer (registered under the
%% expected name) plus a fake conn pid, assert send_frame routes
%% correctly. No real DHT or peering started.
-module(macula_station_dht_transport_tests).

-include_lib("eunit/include/eunit.hrl").

-define(OBS, macula_station_peer_observer).

%%---------------------------------------------------------------------
%% Fake observer: a tiny gen_server-like process that answers
%% conn_for/2 and remembers what it was asked.
%%---------------------------------------------------------------------

start_fake_observer(Conns) ->
    Self = self(),
    Pid = spawn(fun() -> fake_observer_loop(Conns, Self) end),
    true = register(?OBS, Pid),
    Pid.

stop_fake_observer(Pid) ->
    case whereis(?OBS) of
        Pid -> unregister(?OBS);
        _   -> ok
    end,
    exit(Pid, normal).

fake_observer_loop(Conns, Reporter) ->
    receive
        {'$gen_call', From, {conn_for, NodeId}} ->
            Reporter ! {observer_lookup, NodeId},
            Reply = case maps:find(NodeId, Conns) of
                        {ok, P} -> {ok, P};
                        error   -> error
                    end,
            gen:reply(From, Reply),
            fake_observer_loop(Conns, Reporter);
        stop ->
            ok;
        _ ->
            fake_observer_loop(Conns, Reporter)
    end.

%%---------------------------------------------------------------------
%% Fake conn pid: drains macula_peering:send_frame casts and reports
%% them back to the test driver.
%%---------------------------------------------------------------------

start_fake_conn() ->
    Self = self(),
    spawn(fun() -> fake_conn_loop(Self) end).

fake_conn_loop(Reporter) ->
    receive
        {'$gen_cast', {send_frame, Frame}} ->
            Reporter ! {conn_received, self(), Frame},
            fake_conn_loop(Reporter);
        stop ->
            ok;
        _ ->
            fake_conn_loop(Reporter)
    end.

%%---------------------------------------------------------------------
%% Tests
%%---------------------------------------------------------------------

no_observer_returns_no_observer_test() ->
    %% Make sure name is free.
    ?assertEqual(undefined, whereis(?OBS)),
    NodeId = id(1),
    Frame  = #{type => ping, nonce => <<0:64>>},
    ?assertEqual({error, no_observer},
                 macula_station_dht_transport:send_frame(NodeId, Frame)).

unknown_node_returns_no_route_test() ->
    Obs = start_fake_observer(#{}),
    try
        NodeId = id(2),
        Frame  = #{type => ping, nonce => <<0:64>>},
        ?assertEqual({error, no_route},
                     macula_station_dht_transport:send_frame(NodeId, Frame)),
        receive {observer_lookup, NodeId} -> ok
        after 200 -> ?assert(observer_was_not_consulted)
        end
    after stop_fake_observer(Obs)
    end.

known_node_routes_to_conn_test() ->
    Conn = start_fake_conn(),
    NodeId = id(3),
    Obs = start_fake_observer(#{NodeId => Conn}),
    try
        Frame = #{type => ping, nonce => <<7:64>>},
        ?assertEqual(ok,
                     macula_station_dht_transport:send_frame(NodeId, Frame)),
        receive {conn_received, Conn, Frame} -> ok
        after 200 -> ?assert(conn_did_not_receive_frame)
        end
    after
        stop_fake_observer(Obs),
        Conn ! stop
    end.

bad_nodeid_arity_rejects_test() ->
    %% Only 256-bit binaries pass the head guard.
    Frame = #{type => ping},
    ?assertError(function_clause,
                 macula_station_dht_transport:send_frame(<<1,2,3>>, Frame)).

%%---------------------------------------------------------------------
%% helpers
%%---------------------------------------------------------------------

id(N) -> <<N:256>>.
