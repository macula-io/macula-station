-module(macula_station_peering_redundancy_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("macula_station/include/macula_station_cfg.hrl").

-define(M, macula_station_peering_redundancy).

%%==================================================================
%% Pure helpers — under_target/2, on_cooldown/3, eligible/3, rank/2,
%% pick_endpoint/1. No process, no DHT, no supervisor needed.
%%==================================================================

under_target_true_when_below_test() ->
    ?assert(?M:under_target(1, #peering_redundancy_cfg{min_station_peers = 3})).

under_target_false_when_at_or_above_test() ->
    ?assertNot(?M:under_target(3, #peering_redundancy_cfg{min_station_peers = 3})),
    ?assertNot(?M:under_target(5, #peering_redundancy_cfg{min_station_peers = 3})).

on_cooldown_false_when_never_dialled_test() ->
    ?assertNot(?M:on_cooldown(node_id(1), #{}, 1_000)).

on_cooldown_true_before_expiry_test() ->
    Cooldown = #{node_id(1) => 5_000},
    ?assert(?M:on_cooldown(node_id(1), Cooldown, 1_000)).

on_cooldown_false_after_expiry_test() ->
    Cooldown = #{node_id(1) => 5_000},
    ?assertNot(?M:on_cooldown(node_id(1), Cooldown, 5_001)).

eligible_excludes_already_connected_test() ->
    Candidates = [entry(1), entry(2)],
    Eligible   = ?M:eligible(Candidates, [node_id(1)], #{}),
    ?assertEqual([node_id(2)], [maps:get(node_id, C) || C <- Eligible]).

eligible_excludes_on_cooldown_test() ->
    Candidates = [entry(1), entry(2)],
    Cooldown   = #{node_id(1) => 9_999_999_999_999},
    Eligible   = ?M:eligible(Candidates, [], Cooldown),
    ?assertEqual([node_id(2)], [maps:get(node_id, C) || C <- Eligible]).

%% Diversity ranking: a candidate whose asn/country/tier are all new
%% relative to the current peer set outranks one that duplicates them.
rank_prefers_the_more_diverse_candidate_test() ->
    Current   = [entry(1, 100, <<"BE">>, t0)],
    Duplicate = entry(2, 100, <<"BE">>, t0),
    Diverse   = entry(3, 200, <<"FR">>, t1),
    [{TopScore, Top} | _] = ?M:rank([Duplicate, Diverse], Current),
    ?assertEqual(node_id(3), maps:get(node_id, Top)),
    ?assert(TopScore > 0).

pick_endpoint_takes_first_test() ->
    ?assertEqual(#{host => <<"a">>, port => 1},
                 ?M:pick_endpoint([#{host => <<"a">>, port => 1, transport => quic},
                                   #{host => <<"b">>, port => 2, transport => quic}])).

pick_endpoint_undefined_when_empty_test() ->
    ?assertEqual(undefined, ?M:pick_endpoint([])).

%%==================================================================
%% Integration — force_tick/1 through a real DHT + a real
%% outbound_links_sup, against a lightweight stub observer this test
%% controls directly (so "how many stations am I connected to" is a
%% fixed input, not something requiring real QUIC connections).
%%==================================================================

healthy_no_dial_test() ->
    {Rp, Dht, Obs} = fixture(#{min => 1}, _ObservedStations = 1, _Candidates = 0),
    try
        Status = macula_station_peering_redundancy:force_tick(Rp),
        ?assertEqual(0, maps:get(dials, Status))
    after
        teardown(Rp, Dht, Obs)
    end.

under_target_dials_one_candidate_test() ->
    {Rp, Dht, Obs} = fixture(#{min => 3}, _ObservedStations = 1, _Candidates = 2),
    try
        _ = macula_station_peering_redundancy:force_tick(Rp),
        ok = receive_dialed(1_000),
        Status = macula_station_peering_redundancy:state(Rp),
        ?assertEqual(1, maps:get(dials, Status))
    after
        teardown(Rp, Dht, Obs)
    end.

%% At most one dial per tick, even with several under-target slots
%% open and several candidates available.
at_most_one_dial_per_tick_test() ->
    {Rp, Dht, Obs} = fixture(#{min => 5}, _ObservedStations = 0, _Candidates = 4),
    try
        _ = macula_station_peering_redundancy:force_tick(Rp),
        ok = receive_dialed(1_000),
        %% Drain any extra messages that would indicate a second fire.
        ?assertEqual(no_more, drain(50)),
        Status = macula_station_peering_redundancy:state(Rp),
        ?assertEqual(1, maps:get(dials, Status))
    after
        teardown(Rp, Dht, Obs)
    end.

%% A dialled candidate is on cooldown and is not immediately
%% redialled by the very next tick even though it still is not
%% counted among "connected" stations by the stub observer (the
%% stub dial_fun doesn't register anything with the observer either).
dialled_candidate_goes_on_cooldown_test() ->
    {Rp, Dht, Obs} = fixture(#{min => 3}, _ObservedStations = 1, _Candidates = 1),
    try
        _ = macula_station_peering_redundancy:force_tick(Rp),
        ok = receive_dialed(1_000),
        _ = macula_station_peering_redundancy:force_tick(Rp),
        Status = macula_station_peering_redundancy:state(Rp),
        %% Still just the one dial -- the only candidate is on cooldown.
        ?assertEqual(1, maps:get(dials, Status)),
        ?assertEqual(1, maps:get(on_cooldown, Status))
    after
        teardown(Rp, Dht, Obs)
    end.

%%==================================================================
%% Fixture
%%==================================================================

%% `dial_fun' is stubbed rather than routed through a real
%% `macula_station_outbound_links_sup' + `macula_station_outbound_link':
%% a real dial needs the whole `macula'/`macula_transport' QUIC stack
%% (`macula_peering_conn_sup' et al) running, which is exactly the
%% weight `macula_station_rebootstrap''s own tests avoid by injecting
%% a stub peer-discoverer instead of a real bootstrap tier. The stub
%% here just records every dial attempt back to the test process.
fixture(#{min := Min}, ObservedStations, CandidateCount) ->
    _ = application:ensure_all_started(crypto),
    SelfKp = macula_identity:generate(),
    SelfId = macula_identity:node_id(SelfKp),
    {ok, Dht} = macula_dht:start_link(#{self_id => SelfId}),
    ObservedIds = [seed_station(Dht) || _ <- lists:seq(1, ObservedStations)],
    _ = [seed_station(Dht) || _ <- lists:seq(1, CandidateCount)],
    {ok, Obs} = stub_station_view_observer:start_link(
                    #{stations => ObservedStations, station_ids => ObservedIds}),
    Test = self(),
    DialFun = fun(Peer, _Kp, _Caps) ->
        Test ! {dial_attempted, Peer},
        {ok, self()}
    end,
    PrCfg = #peering_redundancy_cfg{
        min_station_peers = Min,
        check_period_ms   = 60_000,
        cooldown_ms       = 300_000,
        candidate_pool    = 32
    },
    {ok, Rp} = macula_station_peering_redundancy:start_link(#{
        dht                => Dht,
        observer           => Obs,
        identity           => SelfKp,
        peering_redundancy => PrCfg,
        notify             => self(),
        dial_fun           => DialFun
    }),
    {Rp, Dht, Obs}.

%% Seeds a station-shaped entry into the DHT routing table.
seed_station(Dht) ->
    NodeId = crypto:strong_rand_bytes(32),
    _ = macula_dht:observe(Dht, #{
        node_id   => NodeId,
        endpoints => [#{host => <<"127.0.0.1">>, port => 1, transport => quic}],
        asn       => 42,
        country   => <<"BE">>,
        tier      => t0
    }),
    NodeId.

teardown(Rp, Dht, Obs) ->
    _ = catch macula_station_peering_redundancy:stop(Rp),
    _ = catch macula_dht:stop(Dht),
    _ = catch stub_station_view_observer:stop(Obs),
    ok.

receive_dialed(Ms) ->
    receive
        {macula_station_peering_redundancy, dialed, _NodeId, _Result} -> ok
    after Ms ->
        error(dial_timeout)
    end.

drain(Ms) ->
    receive
        {macula_station_peering_redundancy, dialed, _, _} -> unexpected_extra_dial
    after Ms ->
        no_more
    end.

node_id(N) -> <<N:256>>.

entry(N) -> entry(N, 42, <<"BE">>, t0).

entry(N, Asn, Country, Tier) ->
    #{node_id   => node_id(N),
      endpoints => [#{host => <<"127.0.0.1">>, port => 1, transport => quic}],
      asn       => Asn,
      country   => Country,
      tier      => Tier}.
