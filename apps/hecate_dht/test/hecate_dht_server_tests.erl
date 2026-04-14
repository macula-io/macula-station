%% EUnit tests for hecate_dht_server (the stateful core).
-module(hecate_dht_server_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Fixture
%%---------------------------------------------------------------------

start_with_opts(Opts) ->
    {ok, Pid} = hecate_dht_server:start_link(Opts),
    Pid.

start(SelfId) ->
    start_with_opts(#{self_id => SelfId}).

stop(Pid) ->
    hecate_dht_server:stop(Pid).

%%---------------------------------------------------------------------
%% start_link / init
%%---------------------------------------------------------------------

start_link_returns_pid_test() ->
    {ok, Pid} = hecate_dht_server:start_link(#{self_id => id(0)}),
    ?assert(is_pid(Pid)),
    stop(Pid).

start_link_rejects_non_256_bit_self_id_test() ->
    ?assertError(function_clause,
                 hecate_dht_server:start_link(#{self_id => <<1,2,3>>})).

start_link_honours_custom_k_and_s_test() ->
    Pid = start_with_opts(#{self_id => id(0), k => 5, s => 3}),
    #{k := 5, s := 3, size := 0,
      sibling_count := 0, bucket_count := 0} =
        hecate_dht_server:stats(Pid),
    stop(Pid).

init_sets_self_id_test() ->
    Pid = start(id(42)),
    ?assertEqual(id(42), hecate_dht_server:self_id(Pid)),
    stop(Pid).

%%---------------------------------------------------------------------
%% observe — admission
%%---------------------------------------------------------------------

observe_admits_fresh_peer_test() ->
    Pid = start(id(0)),
    Result = hecate_dht_server:observe(Pid, spec(<<1:1, 0:255>>)),
    ?assertEqual(admitted, Result),
    ?assertEqual(1, hecate_dht_server:size(Pid)),
    ?assert(hecate_dht_server:contains(Pid, <<1:1, 0:255>>)),
    stop(Pid).

observe_same_peer_twice_touches_test() ->
    Pid = start(id(0)),
    admitted = hecate_dht_server:observe(Pid, spec(<<1:1, 0:255>>)),
    touched  = hecate_dht_server:observe(Pid, spec(<<1:1, 0:255>>)),
    ?assertEqual(1, hecate_dht_server:size(Pid)),
    stop(Pid).

observe_self_is_rejected_test() ->
    Self = id(99),
    Pid  = start(Self),
    ?assertEqual(rejected, hecate_dht_server:observe(Pid, spec(Self))),
    ?assertEqual(0, hecate_dht_server:size(Pid)),
    stop(Pid).

observe_replaces_on_full_bucket_with_stronger_candidate_test() ->
    %% Fill bucket 255 (K=2) with weak peers, then force replacement.
    Pid = start_with_opts(#{self_id => id(0), k => 2, s => 16}),
    W1 = weak_spec(<<1:1, 0:255>>, 101, <<"BE">>, t0),
    W2 = weak_spec(<<1:1, 1:1, 0:254>>, 102, <<"BE">>, t0),
    admitted = hecate_dht_server:observe(Pid, W1),
    admitted = hecate_dht_server:observe(Pid, W2),
    Strong = strong_spec(<<1:1, 0:254, 1:1>>, 999, <<"JP">>, t3),
    {replaced, Evicted} = hecate_dht_server:observe(Pid, Strong),
    ?assert(hecate_dht_entry:is_entry(Evicted)),
    ?assertEqual(2, hecate_dht_server:size(Pid)),
    stop(Pid).

observe_rejects_when_bucket_full_of_stronger_peers_test() ->
    Pid = start_with_opts(#{self_id => id(0), k => 2, s => 16}),
    S1 = strong_spec(<<1:1, 0:255>>, 101, <<"BE">>, t0),
    S2 = strong_spec(<<1:1, 1:1, 0:254>>, 102, <<"NL">>, t1),
    admitted = hecate_dht_server:observe(Pid, S1),
    admitted = hecate_dht_server:observe(Pid, S2),
    Weak = weak_spec(<<1:1, 0:254, 1:1>>, 103, <<"FR">>, t2),
    ?assertEqual(rejected, hecate_dht_server:observe(Pid, Weak)),
    ?assertEqual(2, hecate_dht_server:size(Pid)),
    stop(Pid).

%%---------------------------------------------------------------------
%% find / contains / k_closest
%%---------------------------------------------------------------------

find_returns_entry_when_present_test() ->
    Pid = start(id(0)),
    PeerId = <<1:8, 0:248>>,
    admitted = hecate_dht_server:observe(Pid, spec(PeerId)),
    {ok, E} = hecate_dht_server:find(Pid, PeerId),
    ?assertEqual(PeerId, hecate_dht_entry:node_id(E)),
    stop(Pid).

find_returns_error_when_missing_test() ->
    Pid = start(id(0)),
    ?assertEqual(error, hecate_dht_server:find(Pid, <<7:8, 0:248>>)),
    stop(Pid).

contains_tracks_presence_test() ->
    Pid = start(id(0)),
    PeerId = <<1:8, 0:248>>,
    ?assertNot(hecate_dht_server:contains(Pid, PeerId)),
    admitted = hecate_dht_server:observe(Pid, spec(PeerId)),
    ?assert(hecate_dht_server:contains(Pid, PeerId)),
    stop(Pid).

k_closest_returns_ordered_by_distance_test() ->
    Pid = start(id(0)),
    Target = <<1:8, 0:248>>,
    Near   = <<1:8, 0:248>>,
    Mid    = <<1:8, 0:247, 1:1>>,
    Far    = <<1:8, 0:240, 255:8>>,
    admitted = hecate_dht_server:observe(Pid, spec(Far)),
    admitted = hecate_dht_server:observe(Pid, spec(Near)),
    admitted = hecate_dht_server:observe(Pid, spec(Mid)),
    Results = hecate_dht_server:k_closest(Pid, Target, 3),
    Ids = [hecate_dht_entry:node_id(E) || E <- Results],
    ?assertEqual([Near, Mid, Far], Ids),
    stop(Pid).

k_closest_on_empty_table_is_empty_test() ->
    Pid = start(id(0)),
    ?assertEqual([], hecate_dht_server:k_closest(Pid, id(99), 20)),
    stop(Pid).

%%---------------------------------------------------------------------
%% Siblings
%%---------------------------------------------------------------------

siblings_start_empty_test() ->
    Pid = start(id(0)),
    ?assertEqual([], hecate_dht_server:siblings(Pid)),
    ?assertEqual([], hecate_dht_server:sibling_ids(Pid)),
    stop(Pid).

observe_populates_sibling_list_test() ->
    Pid = start(id(0)),
    PeerId = <<1:8, 0:248>>,
    admitted = hecate_dht_server:observe(Pid, spec(PeerId)),
    ?assertEqual([PeerId], hecate_dht_server:sibling_ids(Pid)),
    stop(Pid).

siblings_ordered_by_distance_across_observations_test() ->
    Pid  = start_with_opts(#{self_id => id(0), k => 20, s => 4}),
    Far  = <<128:8, 0:248>>,
    Mid  = <<16:8,  0:248>>,
    Near = <<1:8,   0:248>>,
    admitted = hecate_dht_server:observe(Pid, spec(Far)),
    admitted = hecate_dht_server:observe(Pid, spec(Near)),
    admitted = hecate_dht_server:observe(Pid, spec(Mid)),
    ?assertEqual([Near, Mid, Far], hecate_dht_server:sibling_ids(Pid)),
    stop(Pid).

siblings_displace_farthest_when_closer_observed_test() ->
    Pid = start_with_opts(#{self_id => id(0), k => 20, s => 2}),
    Far  = <<128:8, 0:248>>,
    Near = <<1:8,   0:248>>,
    Mid  = <<4:8,   0:248>>,
    admitted = hecate_dht_server:observe(Pid, spec(Far)),
    admitted = hecate_dht_server:observe(Pid, spec(Near)),
    %% sibling set now full ({Near, Far}); Mid is closer than Far
    admitted = hecate_dht_server:observe(Pid, spec(Mid)),
    ?assertEqual([Near, Mid], hecate_dht_server:sibling_ids(Pid)),
    stop(Pid).

%%---------------------------------------------------------------------
%% touch / forget
%%---------------------------------------------------------------------

touch_bumps_last_seen_in_rt_and_siblings_test() ->
    Pid = start(id(0)),
    PeerId = <<1:8, 0:248>>,
    admitted = hecate_dht_server:observe(Pid, spec(PeerId)),
    {ok, Before} = hecate_dht_server:find(Pid, PeerId),
    timer:sleep(5),
    hecate_dht_server:touch(Pid, PeerId),
    %% touch is a cast — synchronise by issuing a call.
    _ = hecate_dht_server:stats(Pid),
    {ok, After} = hecate_dht_server:find(Pid, PeerId),
    ?assert(hecate_dht_entry:last_seen(After)
              >= hecate_dht_entry:last_seen(Before)),
    stop(Pid).

touch_missing_peer_is_noop_test() ->
    Pid = start(id(0)),
    ok = hecate_dht_server:touch(Pid, <<9:8, 0:248>>),
    _ = hecate_dht_server:stats(Pid),
    ?assertEqual(0, hecate_dht_server:size(Pid)),
    stop(Pid).

forget_removes_from_rt_and_siblings_test() ->
    Pid = start(id(0)),
    PeerId = <<1:8, 0:248>>,
    admitted = hecate_dht_server:observe(Pid, spec(PeerId)),
    ?assert(hecate_dht_server:contains(Pid, PeerId)),
    ?assertEqual([PeerId], hecate_dht_server:sibling_ids(Pid)),
    ok = hecate_dht_server:forget(Pid, PeerId),
    _ = hecate_dht_server:stats(Pid),
    ?assertNot(hecate_dht_server:contains(Pid, PeerId)),
    ?assertEqual([], hecate_dht_server:sibling_ids(Pid)),
    stop(Pid).

forget_missing_is_noop_test() ->
    Pid = start(id(0)),
    ok = hecate_dht_server:forget(Pid, <<9:8, 0:248>>),
    _ = hecate_dht_server:stats(Pid),
    ?assertEqual(0, hecate_dht_server:size(Pid)),
    stop(Pid).

%%---------------------------------------------------------------------
%% stats
%%---------------------------------------------------------------------

stats_reports_all_fields_test() ->
    Pid = start(id(0)),
    admitted = hecate_dht_server:observe(Pid, spec(<<1:8, 0:248>>)),
    Stats = hecate_dht_server:stats(Pid),
    ?assertMatch(#{self_id := _, size := 1, bucket_count := 1,
                   sibling_count := 1, k := 20, s := 16}, Stats),
    stop(Pid).

%%---------------------------------------------------------------------
%% Unknown call → explicit error
%%---------------------------------------------------------------------

unknown_call_returns_error_test() ->
    Pid = start(id(0)),
    ?assertEqual({error, unknown_call},
                 gen_server:call(Pid, {no_such_op, foo})),
    stop(Pid).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

id(N) -> <<N:256>>.

spec(NodeId) ->
    #{node_id => NodeId, asn => 100, country => <<"BE">>, tier => t0}.

weak_spec(NodeId, Asn, Country, Tier) ->
    #{node_id             => NodeId,
      asn                 => Asn,
      country             => Country,
      tier                => Tier,
      observed_latency_ms => 450,
      uptime_frac         => 0.05}.

strong_spec(NodeId, Asn, Country, Tier) ->
    #{node_id             => NodeId,
      asn                 => Asn,
      country             => Country,
      tier                => Tier,
      observed_latency_ms => 30,
      uptime_frac         => 0.95}.
