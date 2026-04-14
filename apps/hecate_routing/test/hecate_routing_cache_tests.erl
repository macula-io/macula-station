%% EUnit tests for hecate_routing_cache.
-module(hecate_routing_cache_tests).

-include_lib("eunit/include/eunit.hrl").

start() ->
    {ok, P} = hecate_routing_cache:start_link(#{ttl_ms => 60_000,
                                                sweep_interval_ms => 60_000}),
    P.

start(Opts) ->
    {ok, P} = hecate_routing_cache:start_link(Opts),
    P.

stop(P) -> hecate_routing_cache:stop(P).

%%---------------------------------------------------------------------
%% Empty state
%%---------------------------------------------------------------------

lookup_on_empty_returns_miss_test() ->
    P = start(),
    ?assertEqual(miss, hecate_routing_cache:lookup(P, dest1)),
    ?assertEqual(0, hecate_routing_cache:size(P)),
    stop(P).

%%---------------------------------------------------------------------
%% Store + lookup
%%---------------------------------------------------------------------

store_then_lookup_returns_paths_test() ->
    P = start(),
    Paths = [[a, b, c], [a, d, c]],
    ok = hecate_routing_cache:store(P, c, Paths),
    ?assertEqual({ok, Paths}, hecate_routing_cache:lookup(P, c)),
    ?assertEqual(1, hecate_routing_cache:size(P)),
    stop(P).

store_overwrites_existing_test() ->
    P = start(),
    ok = hecate_routing_cache:store(P, dest, [[a, b, dest]]),
    ok = hecate_routing_cache:store(P, dest, [[a, c, dest]]),
    ?assertEqual({ok, [[a, c, dest]]},
                 hecate_routing_cache:lookup(P, dest)),
    stop(P).

all_destinations_lists_keys_test() ->
    P = start(),
    ok = hecate_routing_cache:store(P, x, [[a, x]]),
    ok = hecate_routing_cache:store(P, y, [[a, y]]),
    ?assertEqual([x, y], lists:sort(hecate_routing_cache:all_destinations(P))),
    stop(P).

%%---------------------------------------------------------------------
%% TTL expiry
%%---------------------------------------------------------------------

lookup_after_ttl_returns_miss_and_drops_entry_test() ->
    P = start(#{ttl_ms => 30, sweep_interval_ms => 60_000}),
    ok = hecate_routing_cache:store(P, dest, [[a, dest]]),
    timer:sleep(60),
    ?assertEqual(miss, hecate_routing_cache:lookup(P, dest)),
    %% The expired entry was dropped on lookup.
    ?assertEqual(0, hecate_routing_cache:size(P)),
    stop(P).

evict_expired_returns_evicted_destinations_test() ->
    P = start(#{ttl_ms => 30, sweep_interval_ms => 60_000}),
    ok = hecate_routing_cache:store(P, x, [[a, x]]),
    ok = hecate_routing_cache:store(P, y, [[a, y]]),
    timer:sleep(60),
    Removed = hecate_routing_cache:evict_expired(P),
    ?assertEqual([x, y], lists:sort(Removed)),
    ?assertEqual(0, hecate_routing_cache:size(P)),
    stop(P).

evict_expired_keeps_fresh_entries_test() ->
    P = start(#{ttl_ms => 200, sweep_interval_ms => 60_000}),
    ok = hecate_routing_cache:store(P, dest, [[a, dest]]),
    Removed = hecate_routing_cache:evict_expired(P),
    ?assertEqual([], Removed),
    ?assertEqual(1, hecate_routing_cache:size(P)),
    stop(P).

%%---------------------------------------------------------------------
%% Explicit invalidation
%%---------------------------------------------------------------------

invalidate_drops_specific_destination_test() ->
    P = start(),
    ok = hecate_routing_cache:store(P, x, [[a, x]]),
    ok = hecate_routing_cache:store(P, y, [[a, y]]),
    ok = hecate_routing_cache:invalidate(P, x),
    ?assertEqual(miss, hecate_routing_cache:lookup(P, x)),
    ?assertEqual({ok, [[a, y]]}, hecate_routing_cache:lookup(P, y)),
    stop(P).

invalidate_unknown_destination_is_noop_test() ->
    P = start(),
    ok = hecate_routing_cache:invalidate(P, never_stored),
    ?assertEqual(0, hecate_routing_cache:size(P)),
    stop(P).

%%---------------------------------------------------------------------
%% SWIM-event invalidation
%%---------------------------------------------------------------------

invalidate_via_hop_drops_paths_through_failed_hop_test() ->
    P = start(),
    %% dest1 routes via hop_b; dest2 via hop_c. Failing hop_b must
    %% only evict dest1.
    ok = hecate_routing_cache:store(P, dest1, [[a, hop_b, dest1]]),
    ok = hecate_routing_cache:store(P, dest2, [[a, hop_c, dest2]]),
    Removed = hecate_routing_cache:invalidate_via_hop(P, hop_b),
    ?assertEqual([dest1], Removed),
    ?assertEqual(miss, hecate_routing_cache:lookup(P, dest1)),
    ?assertMatch({ok, _}, hecate_routing_cache:lookup(P, dest2)),
    stop(P).

invalidate_via_hop_evicts_when_any_disjoint_path_uses_hop_test() ->
    %% dest cached with two disjoint paths; hop_x appears in only
    %% one of them. Cache evicts the whole entry — fresh paths
    %% must be recomputed since the cached set is no longer
    %% trustworthy.
    P = start(),
    ok = hecate_routing_cache:store(P, dest,
                                    [[a, hop_x, dest], [a, hop_y, dest]]),
    Removed = hecate_routing_cache:invalidate_via_hop(P, hop_x),
    ?assertEqual([dest], Removed),
    ?assertEqual(miss, hecate_routing_cache:lookup(P, dest)),
    stop(P).

invalidate_via_hop_unknown_hop_is_noop_test() ->
    P = start(),
    ok = hecate_routing_cache:store(P, dest, [[a, b, dest]]),
    ?assertEqual([], hecate_routing_cache:invalidate_via_hop(P, unrelated)),
    ?assertEqual(1, hecate_routing_cache:size(P)),
    stop(P).

invalidate_via_dest_node_evicts_test() ->
    %% If the destination itself fails, the cache entry is stale.
    P = start(),
    ok = hecate_routing_cache:store(P, dest, [[a, b, dest]]),
    Removed = hecate_routing_cache:invalidate_via_hop(P, dest),
    ?assertEqual([dest], Removed),
    stop(P).

%%---------------------------------------------------------------------
%% Auto-sweep timer
%%---------------------------------------------------------------------

auto_sweep_evicts_in_background_test() ->
    P = start(#{ttl_ms => 30, sweep_interval_ms => 60}),
    ok = hecate_routing_cache:store(P, dest, [[a, dest]]),
    timer:sleep(180),  %% 3 sweeps; entry expires after 30ms
    ?assertEqual(0, hecate_routing_cache:size(P)),
    Stats = hecate_routing_cache:stats(P),
    ?assert(is_integer(maps:get(last_sweep, Stats))),
    stop(P).

%%---------------------------------------------------------------------
%% Stats
%%---------------------------------------------------------------------

stats_reports_configured_values_test() ->
    P = start(#{ttl_ms => 12_345, sweep_interval_ms => 67_890}),
    Stats = hecate_routing_cache:stats(P),
    ?assertEqual(0,       maps:get(size, Stats)),
    ?assertEqual(12_345,  maps:get(ttl_ms, Stats)),
    ?assertEqual(67_890,  maps:get(sweep_interval_ms, Stats)),
    ?assertEqual(undefined, maps:get(last_sweep, Stats)),
    stop(P).
