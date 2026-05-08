-module(macula_station_via_seed_dial_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TIER, macula_station_via_seed_dial).

%%==================================================================
%% Probe surface
%%==================================================================

strategy_metadata_test() ->
    ?assertEqual(via_seed_dial, ?TIER:strategy()),
    ?assertEqual(0,             ?TIER:stagger_ms()).

empty_registry_yields_ok_empty_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        fun() ->
            %% Empty registry must NOT be an error: fresh-fleet
            %% boot needs the listener to come up so siblings can
            %% dial in. Cascade accepts {ok, []} when min_peers=0.
            ?assertEqual({ok, []},
                         ?TIER:discover(#{wait_ms => 0}))
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

            {ok, [Peer]} = ?TIER:discover(#{wait_ms => 0}),
            ?assertMatch(#{node_id   := NodeId,
                           record    := undefined,
                           strategy  := via_seed_dial,
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
            ?assertEqual({ok, []}, ?TIER:discover(#{wait_ms => 0})),
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
