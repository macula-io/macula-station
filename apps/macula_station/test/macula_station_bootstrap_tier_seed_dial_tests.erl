-module(macula_station_bootstrap_tier_seed_dial_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TIER, macula_station_bootstrap_tier_seed_dial).

%%==================================================================
%% Probe surface
%%==================================================================

tier_metadata_test() ->
    ?assertEqual(seed_dial, ?TIER:tier()),
    ?assertEqual(0,         ?TIER:stagger_ms()).

empty_registry_yields_no_verified_peers_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            ?assertEqual({error, no_verified_peers},
                         ?TIER:probe(#{wait_ms => 0}))
        end
    end}.

registry_with_one_peer_yields_one_verified_peer_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            Url     = <<"quic://station-be-brussels.macula.io:4433">>,
            NodeId  = crypto:strong_rand_bytes(32),
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(Url, LinkPid),
            ok = macula_station_peer_links:set_peer_node_id(Url, NodeId),
            sync(),

            {ok, [Peer]} = ?TIER:probe(#{wait_ms => 0}),
            ?assertMatch(#{node_id   := NodeId,
                           record    := undefined,
                           tier      := seed_dial,
                           via       := ?TIER},
                         Peer),
            ?assertMatch([#{host := <<"station-be-brussels.macula.io">>,
                            port := 4433,
                            transport := quic}],
                         maps:get(addresses, Peer)),

            stop_dummy(LinkPid)
        end
    end}.

unverified_peer_is_excluded_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            %% Register a link but never set node_id — should not show up.
            Url     = <<"quic://station-be-ghent.macula.io:4433">>,
            LinkPid = spawn_dummy(),
            ok = macula_station_peer_links:register(Url, LinkPid),
            sync(),
            ?assertEqual({error, no_verified_peers},
                         ?TIER:probe(#{wait_ms => 0})),
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

sync() ->
    _ = macula_station_peer_links:connections(),
    ok.

spawn_dummy() ->
    spawn(fun() -> receive stop -> ok end end).

stop_dummy(Pid) ->
    catch (Pid ! stop),
    ok.
