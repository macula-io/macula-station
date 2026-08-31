-module(macula_station_peer_links_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Lifecycle
%%==================================================================

empty_registry_returns_empty_lists_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            ?assertEqual([], macula_station_peer_links:connections()),
            ?assertEqual([], macula_station_peer_links:verified_peers()),
            ?assertEqual([], macula_station_peer_links:connected_hostnames())
        end
    end}.

%%==================================================================
%% register / set_peer_node_id / verified_peers — happy path
%%==================================================================

register_then_set_node_id_makes_peer_verified_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            Url    = <<"quic://station-be-brussels.macula.io:4433">>,
            NodeId = crypto:strong_rand_bytes(32),
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(Url, LinkPid),
            sync(),
            %% Pre-handshake: link is in connections but NOT verified.
            ?assertEqual([{Url, LinkPid}],
                         macula_station_peer_links:connections()),
            ?assertEqual([], macula_station_peer_links:verified_peers()),

            ok = macula_station_peer_links:set_peer_node_id(Url, NodeId),
            sync(),
            ?assertMatch([#{url := Url, node_id := NodeId,
                            host := <<"station-be-brussels.macula.io">>,
                            port := 4433}],
                         macula_station_peer_links:verified_peers()),
            ?assertEqual([<<"station-be-brussels.macula.io">>],
                         macula_station_peer_links:connected_hostnames()),
            stop_dummy(LinkPid)
        end
    end}.

%%==================================================================
%% IPv6-bracketed URLs — `[::1]:PORT' must not crash the registry.
%% `binary:split/2' on `":"' without recognising the brackets first
%% cuts inside the address, and `binary_to_integer/1' on the garbage
%% "port" half crashed this whole gen_server (every other registered
%% entry lost too, not just this one) until fixed. Test-harness
%% loopback dials (`macula_station_test_cluster:dial_outbound/2')
%% are exactly this shape; production `outbound_peers' config is
%% hostnames, never IPv6 literals, which is why this went unnoticed.
%%==================================================================

register_accepts_bracketed_ipv6_url_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            Url     = <<"quic://[::1]:36422">>,
            NodeId  = crypto:strong_rand_bytes(32),
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(Url, LinkPid),
            sync(),
            ?assertEqual([{Url, LinkPid}],
                         macula_station_peer_links:connections()),

            ok = macula_station_peer_links:set_peer_node_id(Url, NodeId),
            sync(),
            ?assertMatch([#{url := Url, node_id := NodeId,
                            host := <<"::1">>, port := 36422}],
                         macula_station_peer_links:verified_peers()),
            stop_dummy(LinkPid)
        end
    end}.

%% Bracketed IPv6 with no port suffix defaults the same way the
%% plain-host path does.
register_accepts_bare_bracketed_ipv6_url_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            Url     = <<"quic://[::1]">>,
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(Url, LinkPid),
            sync(),
            ok = macula_station_peer_links:set_peer_node_id(
                   Url, crypto:strong_rand_bytes(32)),
            sync(),
            ?assertMatch([#{host := <<"::1">>, port := 4433}],
                         macula_station_peer_links:verified_peers()),
            stop_dummy(LinkPid)
        end
    end}.

%%==================================================================
%% clear_peer_node_id — drops out of verified_peers but keeps the link
%%==================================================================

clear_node_id_drops_from_verified_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            Url     = <<"quic://station-be-ghent.macula.io:4433">>,
            NodeId  = crypto:strong_rand_bytes(32),
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(Url, LinkPid),
            ok = macula_station_peer_links:set_peer_node_id(Url, NodeId),
            sync(),
            ?assertMatch([_], macula_station_peer_links:verified_peers()),

            ok = macula_station_peer_links:clear_peer_node_id(Url),
            sync(),
            ?assertEqual([], macula_station_peer_links:verified_peers()),
            ?assertEqual([{Url, LinkPid}],
                         macula_station_peer_links:connections()),
            stop_dummy(LinkPid)
        end
    end}.

%%==================================================================
%% Monitor cleanup — link death drops the entry
%%==================================================================

dead_link_is_unregistered_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            Url     = <<"quic://station-be-antwerp.macula.io:4433">>,
            NodeId  = crypto:strong_rand_bytes(32),
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(Url, LinkPid),
            ok = macula_station_peer_links:set_peer_node_id(Url, NodeId),
            sync(),
            ?assertMatch([_], macula_station_peer_links:verified_peers()),

            stop_dummy(LinkPid),
            wait_until_empty(),
            ?assertEqual([], macula_station_peer_links:connections()),
            ?assertEqual([], macula_station_peer_links:verified_peers())
        end
    end}.

%%==================================================================
%% explicit unregister
%%==================================================================

unregister_drops_the_entry_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            Url     = <<"quic://station-be-leuven.macula.io:4433">>,
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(Url, LinkPid),
            sync(),
            ?assertEqual(1, length(macula_station_peer_links:connections())),
            ok = macula_station_peer_links:unregister(Url),
            sync(),
            ?assertEqual([], macula_station_peer_links:connections()),
            stop_dummy(LinkPid)
        end
    end}.

%%==================================================================
%% Peering-router kicks — register/unregister/DOWN all change what
%% `connections/0' returns (the peer half of the router's desired
%% (Realm, Topic, Peer) cross-product), which used to only surface on
%% the router's next 2s poll. Now they kick it directly, same as the
%% pubsub-subscription side already does -- see
%% `macula_station_peering_router''s moduledoc. `set_peer_node_id'/
%% `clear_peer_node_id' must NOT kick: they only affect
%% `verified_peers/0', which the router doesn't consult.
%%==================================================================

register_kicks_the_peering_router_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            RouterStub = register_router_stub(),
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(
                   <<"quic://station-be-mons.macula.io:4433">>, LinkPid),
            assert_kicked(RouterStub),
            stop_dummy(LinkPid),
            unregister_router_stub()
        end
    end}.

unregister_kicks_the_peering_router_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            Url = <<"quic://station-be-namur.macula.io:4433">>,
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(Url, LinkPid),
            sync(),
            RouterStub = register_router_stub(),
            ok = macula_station_peer_links:unregister(Url),
            assert_kicked(RouterStub),
            stop_dummy(LinkPid),
            unregister_router_stub()
        end
    end}.

dead_link_kicks_the_peering_router_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            Url = <<"quic://station-be-liege.macula.io:4433">>,
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(Url, LinkPid),
            sync(),
            RouterStub = register_router_stub(),
            stop_dummy(LinkPid),
            wait_until_empty(),
            assert_kicked(RouterStub),
            unregister_router_stub()
        end
    end}.

node_id_changes_do_not_kick_the_peering_router_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            Url = <<"quic://station-be-mechelen.macula.io:4433">>,
            NodeId = crypto:strong_rand_bytes(32),
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(Url, LinkPid),
            sync(),
            RouterStub = register_router_stub(),
            ok = macula_station_peer_links:set_peer_node_id(Url, NodeId),
            ok = macula_station_peer_links:clear_peer_node_id(Url),
            sync(),
            assert_not_kicked(RouterStub),
            stop_dummy(LinkPid),
            unregister_router_stub()
        end
    end}.

%%==================================================================
%% Helpers
%%==================================================================

setup() ->
    catch macula_station_peer_links:stop(),
    {ok, Pid} = macula_station_peer_links:start_link(),
    Pid.

teardown(_Pid) ->
    catch macula_station_peer_links:stop(),
    ok.

%% Force a sync round-trip through the gen_server so any pending
%% casts have been processed before we observe state.
sync() ->
    _ = macula_station_peer_links:connections(),
    ok.

spawn_dummy() ->
    spawn(fun() ->
        receive stop -> ok end
    end).

stop_dummy(Pid) ->
    catch (Pid ! stop),
    ok.

wait_until_empty() -> wait_until_empty(50).
wait_until_empty(0) -> ok;
wait_until_empty(N) ->
    case macula_station_peer_links:connections() of
        [] -> ok;
        _  -> timer:sleep(20), wait_until_empty(N - 1)
    end.

%% Registers THIS (the test) process under the peering router's own
%% name -- `notify_router_change/0' addresses it by that name
%% (`whereis(macula_station_peering_router) ! tick'), not by pid -- so
%% the kick lands directly in the test process's own mailbox and a
%% plain `receive' observes it. Returns unused; kept for symmetry with
%% `unregister_router_stub/0' at call sites.
register_router_stub() ->
    catch unregister(macula_station_peering_router),
    true = register(macula_station_peering_router, self()),
    self().

unregister_router_stub() ->
    catch unregister(macula_station_peering_router),
    ok.

assert_kicked(_Self) ->
    receive tick -> ok
    after 500 -> ?assert(false)
    end.

assert_not_kicked(_Self) ->
    receive tick -> ?assert(kicked_but_should_not_have_been)
    after 200 -> ok
    end.
