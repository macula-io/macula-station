%% EUnit tests for hecate_dht_monitor — bucket + replica diversity
%% reports and hecate_diagnostics gauge emission.
-module(hecate_dht_monitor_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Empty DHT — every report field is zero
%%---------------------------------------------------------------------

empty_dht_report_is_all_zero_test() ->
    {Dht, _Kp} = start_dht(),
    Mon = start_monitor(Dht),
    R = hecate_dht_monitor:report(Mon),
    ?assertEqual(0, maps:get(rt_size, R)),
    ?assertEqual(0, maps:get(bucket_count, R)),
    ?assertEqual(0, maps:get(record_count, R)),
    Buckets = maps:get(buckets, R),
    ?assertEqual(0, maps:get(populated, Buckets)),
    ?assertEqual(0, maps:get(asn_violations, Buckets)),
    Replicas = maps:get(replicas, R),
    ?assertEqual(0, maps:get(owned_records, Replicas)),
    ?assertEqual([], maps:get(placement_summaries, Replicas)),
    stop_monitor(Mon),
    stop_dht(Dht).

%%---------------------------------------------------------------------
%% Bucket diversity — concentrated peers flagged
%%---------------------------------------------------------------------

bucket_with_no_diversity_flags_violations_test() ->
    {Dht, _Kp} = start_dht(),
    %% Five peers all in the same ASN/country/tier — fails ≥5 ASN /
    %% ≥3 country soft constraints once populated.
    [admitted = hecate_dht:observe(Dht, peer_spec(<<N:256>>, 100, <<"BE">>, t0))
     || N <- lists:seq(1, 5)],
    Mon = start_monitor(Dht),
    R = hecate_dht_monitor:report(Mon),
    Buckets = maps:get(buckets, R),
    %% At least one populated bucket and at least one violation.
    ?assert(maps:get(populated, Buckets) >= 1),
    ?assert(maps:get(asn_violations, Buckets) >= 1
              orelse maps:get(country_violations, Buckets) >= 1),
    stop_monitor(Mon),
    stop_dht(Dht).

bucket_below_member_threshold_skips_tier_constraint_test() ->
    %% Buckets with < 8 entries are exempt from the tier constraint
    %% per Part 3 §4.3 — verify tier_violations stays zero in that
    %% small-sample regime.
    {Dht, _Kp} = start_dht(),
    [admitted = hecate_dht:observe(Dht, peer_spec(<<N:256>>, 100, <<"BE">>, t0))
     || N <- lists:seq(1, 3)],
    Mon = start_monitor(Dht),
    Buckets = maps:get(buckets, hecate_dht_monitor:report(Mon)),
    ?assertEqual(0, maps:get(tier_violations, Buckets)),
    stop_monitor(Mon),
    stop_dht(Dht).

%%---------------------------------------------------------------------
%% Replica diversity — owned record summary
%%---------------------------------------------------------------------

replica_summary_emitted_for_owned_records_test() ->
    {Dht, Kp} = start_dht(),
    SelfId = hecate_identity:public(Kp),
    %% Owner-signed record (envelope key matches self_id).
    Record = hecate_record:sign(hecate_record:node_record(SelfId, [], 0), Kp),
    ok = hecate_dht:put_record(Dht, Record),
    Mon = start_monitor(Dht, #{self_id => SelfId}),
    Replicas = maps:get(replicas, hecate_dht_monitor:report(Mon)),
    ?assertEqual(1, maps:get(owned_records, Replicas)),
    [Summary] = maps:get(placement_summaries, Replicas),
    ?assertEqual(hecate_record:storage_key(Record),
                 maps:get(storage_key, Summary)),
    %% No peers in the RT → degraded.
    ?assert(maps:get(degraded, Summary)),
    stop_monitor(Mon),
    stop_dht(Dht).

replica_summary_skips_records_owned_by_others_test() ->
    {Dht, Kp} = start_dht(),
    SelfId = hecate_identity:public(Kp),
    %% Stranger-signed record.
    KpStranger = hecate_identity:generate(),
    Record = hecate_record:sign(
               hecate_record:node_record(hecate_identity:public(KpStranger),
                                         [], 0),
               KpStranger),
    ok = hecate_dht:put_record(Dht, Record),
    Mon = start_monitor(Dht, #{self_id => SelfId}),
    Replicas = maps:get(replicas, hecate_dht_monitor:report(Mon)),
    ?assertEqual(0, maps:get(owned_records, Replicas)),
    stop_monitor(Mon),
    stop_dht(Dht).

%%---------------------------------------------------------------------
%% Tick: emits diagnostics event + populates gauges
%%---------------------------------------------------------------------

tick_populates_gauges_test() ->
    {Dht, _Kp} = start_dht(),
    [admitted = hecate_dht:observe(Dht, peer_spec(<<N:256>>, 100, <<"BE">>, t0))
     || N <- lists:seq(1, 2)],
    Mon = start_monitor(Dht),
    _ = hecate_dht_monitor:tick(Mon),
    Gauges = hecate_dht_monitor:gauges(Mon),
    GaugeNames = [Name || {Name, gauge, _} <- Gauges],
    ?assert(lists:member(<<"hecate.dht.rt_size">>, GaugeNames)),
    ?assert(lists:member(<<"hecate.dht.bucket_count">>, GaugeNames)),
    ?assert(lists:member(<<"hecate.dht.buckets.populated">>, GaugeNames)),
    %% Verify rt_size gauge matches actual RT size.
    {value, {_, gauge, RtSize}} =
        lists:keysearch(<<"hecate.dht.rt_size">>, 1, Gauges),
    ?assertEqual(2, RtSize),
    stop_monitor(Mon),
    stop_dht(Dht).

%%---------------------------------------------------------------------
%% Cumulative tick stats
%%---------------------------------------------------------------------

stats_track_ticks_test() ->
    {Dht, _Kp} = start_dht(),
    Mon = start_monitor(Dht),
    [_ = hecate_dht_monitor:tick(Mon) || _ <- lists:seq(1, 5)],
    #{ticks := T, last_tick := L} = hecate_dht_monitor:stats(Mon),
    ?assertEqual(5, T),
    ?assert(is_integer(L)),
    stop_monitor(Mon),
    stop_dht(Dht).

%%---------------------------------------------------------------------
%% Auto-firing on the configured interval
%%---------------------------------------------------------------------

interval_drives_auto_ticks_test() ->
    {Dht, _Kp} = start_dht(),
    Mon = start_monitor(Dht, #{interval_ms => 60}),
    timer:sleep(200),
    #{ticks := T} = hecate_dht_monitor:stats(Mon),
    ?assert(T >= 2),
    stop_monitor(Mon),
    stop_dht(Dht).

%%=====================================================================
%% Harness
%%=====================================================================

start_dht() ->
    Kp = hecate_identity:generate(),
    {ok, D} = hecate_dht:start_link(#{self_id  => hecate_identity:public(Kp),
                                      identity => Kp}),
    {D, Kp}.

stop_dht(D) ->
    hecate_dht:stop(D).

start_monitor(Dht) ->
    start_monitor(Dht, #{}).

start_monitor(Dht, Extra) ->
    Base = #{dht => Dht, interval_ms => 3_600_000},
    {ok, M} = hecate_dht_monitor:start_link(maps:merge(Base, Extra)),
    M.

stop_monitor(M) ->
    hecate_dht_monitor:stop(M).

peer_spec(NodeId, Asn, Country, Tier) ->
    #{node_id => NodeId, asn => Asn, country => Country, tier => Tier}.
