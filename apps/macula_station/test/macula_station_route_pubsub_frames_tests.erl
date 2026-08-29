%% @doc Focused tests for `macula_station_route_pubsub_frames'.
%%
%% This module otherwise has NO test coverage — it's exercised only
%% indirectly, through e2e/torture probes. Backfilling the whole
%% dispatcher is out of scope here; these tests cover exactly the new
%% surface added for mesh-wide wildcard pubsub (2026-08-29):
%% `bloom_fan_extras/4''s union of the Bloom channel and the new
%% pattern channel, their independent counters, and the shared
%% "already matched / excluded source / no live conn" filtering both
%% channels go through via `filter_fan_candidates/4'.
-module(macula_station_route_pubsub_frames_tests).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------
%% Setup
%%------------------------------------------------------------------

%% A real `bloom_exchange' process, so `peer_matches_ets/1' and
%% `pattern_matches_ets/1' read genuine state rather than requiring
%% this test to know the ETS table's internal row shape.
start_exchange() ->
    Kp = macula_identity:generate(),
    {ok, Pid} = macula_station_bloom_exchange:start_link(
                  #{pubsub_registry => self(), identity => Kp}),
    Pid.

stop_exchange(Pid) ->
    catch macula_station_bloom_exchange:stop(Pid).

%% A local, unnamed conns table in the exact `{NodeId, #{inbound =>,
%% outbound =>}}' shape `ets_lookup_conn/2' expects. Public so the
%% module-under-test (a different process only for the gen_server
%% cast/info paths — not exercised here) could read it too, though
%% these tests call the pure functions directly from this process.
new_conns_table() ->
    ets:new(conns, [set, public]).

%% A conn row backed by a genuinely alive, local pid — `fan_eligible/2'
%% requires `node(Pid) =:= node()' and `is_process_alive/1'.
live_conn_row(NodeId, CT) ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    ets:insert(CT, {NodeId, #{inbound => Pid, outbound => undefined}}),
    Pid.

node_id(N) -> <<N:256>>.

realm() -> crypto:strong_rand_bytes(32).

event_frame(Topic) ->
    #{frame_type => event, topic => Topic, realm => realm(),
      publisher => node_id(99), seq => 1, payload => <<>>}.

%%------------------------------------------------------------------
%% bloom_fan_extras/4 — union of the two channels
%%------------------------------------------------------------------

bloom_fan_extras_unions_bloom_and_pattern_candidates_test() ->
    Ex = start_exchange(),
    CT = new_conns_table(),
    try
        NodeA = node_id(1), % found via bloom
        NodeB = node_id(2), % found via pattern
        _ = live_conn_row(NodeA, CT),
        _ = live_conn_row(NodeB, CT),
        BloomA = bloom_with([<<"acme/svc.do">>]),
        ok = macula_station_bloom_exchange:receive_peer_bloom(Ex, NodeA, BloomA),
        ok = macula_station_bloom_exchange:receive_peer_patterns(
               Ex, NodeB, [<<"acme/*">>]),
        _ = sys:get_state(Ex),

        Result = macula_station_route_pubsub_frames:bloom_fan_extras(
                   event_frame(<<"acme/svc.do">>), [], [], CT),
        ?assertEqual(lists:sort([NodeA, NodeB]), lists:sort(Result))
    after
        stop_exchange(Ex)
    end.

bloom_fan_extras_dedupes_candidate_present_in_both_channels_test() ->
    Ex = start_exchange(),
    CT = new_conns_table(),
    try
        NodeA = node_id(1),
        _ = live_conn_row(NodeA, CT),
        Bloom = bloom_with([<<"acme/svc.do">>]),
        ok = macula_station_bloom_exchange:receive_peer_bloom(Ex, NodeA, Bloom),
        ok = macula_station_bloom_exchange:receive_peer_patterns(
               Ex, NodeA, [<<"acme/*">>]),
        _ = sys:get_state(Ex),

        Result = macula_station_route_pubsub_frames:bloom_fan_extras(
                   event_frame(<<"acme/svc.do">>), [], [], CT),
        ?assertEqual([NodeA], Result)
    after
        stop_exchange(Ex)
    end.

bloom_fan_extras_pattern_candidate_excluded_by_source_test() ->
    Ex = start_exchange(),
    CT = new_conns_table(),
    try
        Source = node_id(7),
        _ = live_conn_row(Source, CT),
        ok = macula_station_bloom_exchange:receive_peer_patterns(
               Ex, Source, [<<"acme/*">>]),
        _ = sys:get_state(Ex),

        %% Source excluded — the relay-hop case, avoiding echo back to
        %% the peer we received this EVENT from.
        Result = macula_station_route_pubsub_frames:bloom_fan_extras(
                   event_frame(<<"acme/svc.do">>), [], [Source], CT),
        ?assertEqual([], Result)
    after
        stop_exchange(Ex)
    end.

bloom_fan_extras_pattern_candidate_with_no_live_conn_is_dropped_test() ->
    Ex = start_exchange(),
    CT = new_conns_table(),
    try
        %% Named by the pattern gossip, but no row in the conns table
        %% at all — not a direct neighbour.
        Stranger = node_id(3),
        ok = macula_station_bloom_exchange:receive_peer_patterns(
               Ex, Stranger, [<<"acme/*">>]),
        _ = sys:get_state(Ex),

        Result = macula_station_route_pubsub_frames:bloom_fan_extras(
                   event_frame(<<"acme/svc.do">>), [], [], CT),
        ?assertEqual([], Result)
    after
        stop_exchange(Ex)
    end.

bloom_fan_extras_mesh_topic_skips_both_channels_test() ->
    Ex = start_exchange(),
    CT = new_conns_table(),
    try
        NodeA = node_id(1),
        _ = live_conn_row(NodeA, CT),
        Bloom = bloom_with([<<"_mesh.bloom">>]),
        ok = macula_station_bloom_exchange:receive_peer_bloom(Ex, NodeA, Bloom),
        ok = macula_station_bloom_exchange:receive_peer_patterns(
               Ex, NodeA, [<<"_mesh/*">>]),
        _ = sys:get_state(Ex),

        Result = macula_station_route_pubsub_frames:bloom_fan_extras(
                   event_frame(<<"_mesh.bloom">>), [], [], CT),
        ?assertEqual([], Result)
    after
        stop_exchange(Ex)
    end.

%%------------------------------------------------------------------
%% Independent counters — the two channels must never conflate their
%% "found nothing" signal (see `?CTR_NO_PATTERN_MATCH''s own comment
%% in the module under test).
%%------------------------------------------------------------------

no_pattern_match_bumps_only_the_pattern_counter_test() ->
    Ex = start_exchange(),
    CT = new_conns_table(),
    try
        macula_station_route_pubsub_frames:install_counters(),
        NodeA = node_id(1),
        _ = live_conn_row(NodeA, CT),
        Bloom = bloom_with([<<"acme/svc.do">>]),
        ok = macula_station_bloom_exchange:receive_peer_bloom(Ex, NodeA, Bloom),
        _ = sys:get_state(Ex),
        %% No patterns gossiped at all — pattern channel finds nothing.

        Before = macula_station_route_pubsub_frames:delivery_stats(),
        _ = macula_station_route_pubsub_frames:bloom_fan_extras(
              event_frame(<<"acme/svc.do">>), [], [], CT),
        After = macula_station_route_pubsub_frames:delivery_stats(),

        ?assertEqual(maps:get(no_pattern_match, Before) + 1,
                     maps:get(no_pattern_match, After)),
        ?assertEqual(maps:get(no_bloom_match, Before),
                     maps:get(no_bloom_match, After))
    after
        stop_exchange(Ex)
    end.

no_bloom_match_bumps_only_the_bloom_counter_test() ->
    Ex = start_exchange(),
    CT = new_conns_table(),
    try
        macula_station_route_pubsub_frames:install_counters(),
        NodeB = node_id(2),
        _ = live_conn_row(NodeB, CT),
        ok = macula_station_bloom_exchange:receive_peer_patterns(
               Ex, NodeB, [<<"acme/*">>]),
        _ = sys:get_state(Ex),
        %% No bloom gossiped at all — bloom channel finds nothing.

        Before = macula_station_route_pubsub_frames:delivery_stats(),
        _ = macula_station_route_pubsub_frames:bloom_fan_extras(
              event_frame(<<"acme/svc.do">>), [], [], CT),
        After = macula_station_route_pubsub_frames:delivery_stats(),

        ?assertEqual(maps:get(no_bloom_match, Before) + 1,
                     maps:get(no_bloom_match, After)),
        ?assertEqual(maps:get(no_pattern_match, Before),
                     maps:get(no_pattern_match, After))
    after
        stop_exchange(Ex)
    end.

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

bloom_with(Topics) ->
    BF = lists:foldl(fun macula_station_bloom:add/2,
                     macula_station_bloom:new(), Topics),
    macula_station_bloom:to_binary(BF).
