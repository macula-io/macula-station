-module(hecate_station_rebootstrap_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("hecate_station/include/hecate_station_cfg.hrl").

%%==================================================================
%% Healthy DHT — no trigger.
%%==================================================================

healthy_dht_no_trigger_test() ->
    {Rb, Dht} = fixture(#{min => 1, window => 1000}, 2),
    try
        Status = hecate_station_rebootstrap:force_tick(Rb),
        ?assertEqual(0, maps:get(triggers, Status)),
        ?assertEqual(undefined, maps:get(low_since_ms, Status))
    after
        teardown(Rb, Dht)
    end.

%%==================================================================
%% DHT below threshold briefly — no trigger (window not elapsed).
%%==================================================================

brief_dip_does_not_trigger_test() ->
    {Rb, Dht} = fixture(#{min => 5, window => 10_000}, 0),
    try
        %% Two ticks in rapid succession — far less than the 10 s
        %% partition window.
        hecate_station_rebootstrap:force_tick(Rb),
        hecate_station_rebootstrap:force_tick(Rb),
        Status = hecate_station_rebootstrap:force_tick(Rb),
        ?assertEqual(0, maps:get(triggers, Status)),
        ?assertNotEqual(undefined, maps:get(low_since_ms, Status))
    after
        teardown(Rb, Dht)
    end.

%%==================================================================
%% Sustained low — triggers exactly once; resets low_since.
%%==================================================================

sustained_low_triggers_once_test() ->
    {Rb, Dht} = fixture(#{min => 5, window => 50}, 0),
    try
        %% First tick: low_since set.
        _ = hecate_station_rebootstrap:force_tick(Rb),
        timer:sleep(80),
        %% Second tick: window elapsed → trigger.
        _ = hecate_station_rebootstrap:force_tick(Rb),
        %% Wait for the async runner to notify back.
        ok = receive_triggered(500),
        S1 = hecate_station_rebootstrap:state(Rb),
        ?assertEqual(1, maps:get(triggers, S1)),
        %% Immediate follow-up tick: DHT still empty, but low_since
        %% was reset → watchdog restarts the countdown, no fresh fire.
        _ = hecate_station_rebootstrap:force_tick(Rb),
        S2 = hecate_station_rebootstrap:state(Rb),
        ?assertEqual(1, maps:get(triggers, S2)),
        ?assertNotEqual(undefined, maps:get(low_since_ms, S2))
    after
        teardown(Rb, Dht)
    end.

%%==================================================================
%% Recovery clears low_since.
%%==================================================================

recovery_clears_low_since_test() ->
    {Rb, Dht} = fixture(#{min => 2, window => 10_000}, 0),
    try
        _ = hecate_station_rebootstrap:force_tick(Rb),
        ?assertNotEqual(undefined,
            maps:get(low_since_ms, hecate_station_rebootstrap:state(Rb))),
        %% Fill the DHT above threshold.
        hecate_dht:observe(Dht, spec(rand_id())),
        hecate_dht:observe(Dht, spec(rand_id())),
        _ = hecate_station_rebootstrap:force_tick(Rb),
        Status = hecate_station_rebootstrap:state(Rb),
        ?assertEqual(undefined, maps:get(low_since_ms, Status)),
        ?assertEqual(0, maps:get(triggers, Status))
    after
        teardown(Rb, Dht)
    end.

%%==================================================================
%% Exponential back-off — Sprint B.
%%
%% After N consecutive triggers without recovery, the partition
%% window grows as `base × 2^min(N, 4)'. Recovery resets N to 0.
%%==================================================================

backoff_grows_the_window_under_sustained_partition_test() ->
    Base      = 60,
    {Rb, Dht} = fixture(#{min => 5, window => Base}, 0),
    try
        %% Initial window (consecutive=0) is the base.
        S0 = hecate_station_rebootstrap:state(Rb),
        ?assertEqual(0,    maps:get(consecutive, S0)),
        ?assertEqual(Base, maps:get(current_window_ms, S0)),
        %% Trigger #1: first tick sets low_since (no fire yet),
        %% wait past base window, second tick fires.
        _  = hecate_station_rebootstrap:force_tick(Rb),
        timer:sleep(Base + 20),
        _  = hecate_station_rebootstrap:force_tick(Rb),
        ok = receive_triggered(500),
        S1 = hecate_station_rebootstrap:state(Rb),
        ?assertEqual(1,        maps:get(triggers, S1)),
        ?assertEqual(1,        maps:get(consecutive, S1)),
        ?assertEqual(Base * 2, maps:get(current_window_ms, S1)),
        %% Trigger #2 flow: fresh tick sets a new low_since clock
        %% (first trigger reset it to undefined). Then wait past 2×
        %% window and tick again — should fire.
        _  = hecate_station_rebootstrap:force_tick(Rb),
        %% A tick at +80ms is inside the 2× window (=120ms) — no fire.
        timer:sleep(Base + 20),
        _  = hecate_station_rebootstrap:force_tick(Rb),
        S2 = hecate_station_rebootstrap:state(Rb),
        ?assertEqual(1, maps:get(triggers, S2)),
        %% Now wait the rest: total ≥ 2×Base past the fresh low_since.
        timer:sleep(Base + 40),
        _  = hecate_station_rebootstrap:force_tick(Rb),
        ok = receive_triggered(500),
        S3 = hecate_station_rebootstrap:state(Rb),
        ?assertEqual(2,        maps:get(triggers, S3)),
        ?assertEqual(2,        maps:get(consecutive, S3)),
        ?assertEqual(Base * 4, maps:get(current_window_ms, S3))
    after
        teardown(Rb, Dht)
    end.

backoff_caps_at_max_shift_test() ->
    Base      = 1,
    {Rb, Dht} = fixture(#{min => 5, window => Base}, 0),
    try
        %% Drive the state's `consecutive' counter past the cap by
        %% repeatedly forcing a tick + sleeping long enough for the
        %% growing window (well-capped by Base=1ms).
        drive_triggers(Rb, 6, Base),
        Status = hecate_station_rebootstrap:state(Rb),
        %% At N=6 (>4), the window sits at `Base × 2^4' = 16×.
        ?assert(maps:get(consecutive, Status) >= 5),
        ?assertEqual(Base * 16, maps:get(current_window_ms, Status))
    after
        teardown(Rb, Dht)
    end.

recovery_resets_consecutive_counter_test() ->
    Base      = 30,
    {Rb, Dht} = fixture(#{min => 2, window => Base}, 0),
    try
        %% Fire once under partition.
        _  = hecate_station_rebootstrap:force_tick(Rb),
        timer:sleep(Base + 20),
        _  = hecate_station_rebootstrap:force_tick(Rb),
        ok = receive_triggered(500),
        ?assertEqual(1, maps:get(consecutive,
                                  hecate_station_rebootstrap:state(Rb))),
        %% Recovery: DHT fills above threshold.
        hecate_dht:observe(Dht, spec(rand_id())),
        hecate_dht:observe(Dht, spec(rand_id())),
        _ = hecate_station_rebootstrap:force_tick(Rb),
        ok = receive_recovered(500),
        SR = hecate_station_rebootstrap:state(Rb),
        ?assertEqual(0,    maps:get(consecutive, SR)),
        ?assertEqual(Base, maps:get(current_window_ms, SR))
    after
        teardown(Rb, Dht)
    end.

drive_triggers(_Rb, 0, _Base)   -> ok;
drive_triggers(Rb, N, Base) ->
    _ = hecate_station_rebootstrap:force_tick(Rb),
    S = hecate_station_rebootstrap:state(Rb),
    timer:sleep(maps:get(current_window_ms, S) + 10),
    _ = hecate_station_rebootstrap:force_tick(Rb),
    _ = receive_triggered(500),
    drive_triggers(Rb, N - 1, Base).

receive_recovered(Ms) ->
    receive
        {hecate_station_rebootstrap, recovered, _N} -> ok
    after Ms ->
        error(recovery_timeout)
    end.

%%==================================================================
%% Fixture
%%==================================================================

fixture(#{min := Min, window := Window}, InitialPeers) ->
    _ = application:ensure_all_started(crypto),
    {ok, Dht} = hecate_dht:start_link(#{self_id => rand_id()}),
    [hecate_dht:observe(Dht, spec(rand_id()))
     || _ <- lists:seq(1, InitialPeers)],
    RbCfg = #rebootstrap_cfg{min_viable_peers = Min,
                             check_period_ms  = 60_000,
                             partition_window_ms = Window},
    Cfg   = #{tiers => [{hecate_station_stub_tier,
                         #{peers => hecate_station_stub_tier:stub_peers(1)}}],
              cascade_opts => #{min_peers => 1, timeout_ms => 500}},
    {ok, Rb} = hecate_station_rebootstrap:start_link(#{
        dht           => Dht,
        rebootstrap   => RbCfg,
        bootstrap_cfg => Cfg,
        notify        => self()
    }),
    {Rb, Dht}.

teardown(Rb, Dht) ->
    _ = catch hecate_station_rebootstrap:stop(Rb),
    _ = catch hecate_dht:stop(Dht),
    ok.

receive_triggered(Ms) ->
    receive
        {hecate_station_rebootstrap, rebootstrapped, _} -> ok
    after Ms ->
        error(rebootstrap_timeout)
    end.

spec(NodeId) ->
    #{node_id   => NodeId,
      endpoints => [],
      asn       => 42,
      country   => <<"BE">>,
      tier      => t0}.

rand_id() -> crypto:strong_rand_bytes(32).
