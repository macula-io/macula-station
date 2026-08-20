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
