-module(hecate_station_cache_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("hecate_station/include/hecate_station_cfg.hrl").

%%==================================================================
%% dump/2 + load/2 round-trip
%%==================================================================

dump_and_load_round_trip_test() ->
    Dir        = make_tmpdir(),
    DhtA       = start_dht_with_peers(3),
    try
        ok = hecate_station_cache:dump(DhtA, Dir),
        ?assert(filelib:is_regular(hecate_station_cache:path(Dir))),
        DhtB = fresh_dht(),
        try
            {ok, #{loaded := 3, skipped := 0}} =
                hecate_station_cache:load(DhtB, Dir),
            ?assertEqual(3, hecate_dht:size(DhtB))
        after
            hecate_dht:stop(DhtB)
        end
    after
        hecate_dht:stop(DhtA),
        rm_rf(Dir)
    end.

load_of_missing_file_is_zero_test() ->
    Dir = make_tmpdir(),
    Dht = fresh_dht(),
    try
        {ok, #{loaded := 0, skipped := 0}} =
            hecate_station_cache:load(Dht, Dir),
        ?assertEqual(0, hecate_dht:size(Dht))
    after
        hecate_dht:stop(Dht),
        rm_rf(Dir)
    end.

load_of_corrupt_file_returns_error_test() ->
    Dir = make_tmpdir(),
    Dht = fresh_dht(),
    try
        ok = filelib:ensure_dir(hecate_station_cache:path(Dir)),
        ok = file:write_file(hecate_station_cache:path(Dir),
                             <<"not an erlang term">>),
        ?assertMatch({error, _}, hecate_station_cache:load(Dht, Dir)),
        ?assertEqual(0, hecate_dht:size(Dht))
    after
        hecate_dht:stop(Dht),
        rm_rf(Dir)
    end.

load_of_wrong_version_file_returns_error_test() ->
    Dir = make_tmpdir(),
    Dht = fresh_dht(),
    try
        ok = filelib:ensure_dir(hecate_station_cache:path(Dir)),
        Blob = term_to_binary({hecate_station_cache, 99, []}),
        ok = file:write_file(hecate_station_cache:path(Dir), Blob),
        ?assertEqual({error, bad_format},
                     hecate_station_cache:load(Dht, Dir))
    after
        hecate_dht:stop(Dht),
        rm_rf(Dir)
    end.

%%==================================================================
%% Periodic flush gen_server
%%==================================================================

periodic_gen_server_flushes_on_interval_test() ->
    Dir = make_tmpdir(),
    Dht = start_dht_with_peers(2),
    try
        Cfg = #cache_cfg{path = Dir, flush_period_ms = 50},
        {ok, Pid} = hecate_station_cache:start_link(#{dht => Dht,
                                                      cache => Cfg}),
        %% Give at least one tick to fire.
        wait_until(fun() ->
            filelib:is_regular(hecate_station_cache:path(Dir))
        end, 1000),
        ?assert(filelib:is_regular(hecate_station_cache:path(Dir))),
        ok = hecate_station_cache:stop(Pid)
    after
        hecate_dht:stop(Dht),
        rm_rf(Dir)
    end.

explicit_flush_writes_immediately_test() ->
    Dir = make_tmpdir(),
    Dht = start_dht_with_peers(1),
    try
        Cfg = #cache_cfg{path = Dir, flush_period_ms = 60_000},
        {ok, Pid} = hecate_station_cache:start_link(#{dht => Dht,
                                                      cache => Cfg}),
        ok = hecate_station_cache:flush(Pid),
        ?assert(filelib:is_regular(hecate_station_cache:path(Dir))),
        ok = hecate_station_cache:stop(Pid)
    after
        hecate_dht:stop(Dht),
        rm_rf(Dir)
    end.

%%==================================================================
%% Helpers
%%==================================================================

fresh_dht() ->
    _ = application:ensure_all_started(crypto),
    {ok, Dht} = hecate_dht:start_link(#{self_id => rand_id()}),
    Dht.

start_dht_with_peers(N) ->
    Dht = fresh_dht(),
    [hecate_dht:observe(Dht, spec(rand_id())) || _ <- lists:seq(1, N)],
    Dht.

spec(NodeId) ->
    #{node_id   => NodeId,
      endpoints => [],
      asn       => 42,
      country   => <<"BE">>,
      tier      => t0}.

rand_id() -> crypto:strong_rand_bytes(32).

make_tmpdir() ->
    Base = filename:join(["/tmp", "hecate-station-cache-test",
                          integer_to_list(erlang:unique_integer([positive]))]),
    ok   = filelib:ensure_dir(filename:join(Base, "placeholder")),
    Base.

rm_rf(Dir) -> _ = os:cmd("rm -rf " ++ Dir), ok.

wait_until(Pred, Ms) -> wait_step(Pred(), Pred, Ms).

wait_step(true,  _Pred, _Ms)             -> ok;
wait_step(false, Pred, Ms) when Ms =< 0  -> ?assert(Pred());
wait_step(false, Pred, Ms)               ->
    timer:sleep(20),
    wait_step(Pred(), Pred, Ms - 20).
