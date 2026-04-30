%% EUnit tests for macula_dht_routing_table.
-module(macula_dht_routing_table_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Construction
%%---------------------------------------------------------------------

new_default_k20_empty_test() ->
    T = macula_dht_routing_table:new(id(1)),
    ?assertEqual(id(1), macula_dht_routing_table:self_id(T)),
    ?assertEqual(20, macula_dht_routing_table:capacity(T)),
    ?assertEqual(0, macula_dht_routing_table:size(T)),
    ?assertEqual(0, macula_dht_routing_table:bucket_count(T)),
    ?assertEqual([], macula_dht_routing_table:all_entries(T)).

new_with_custom_capacity_test() ->
    T = macula_dht_routing_table:new(id(1), 4),
    ?assertEqual(4, macula_dht_routing_table:capacity(T)).

new_with_zero_capacity_rejected_test() ->
    ?assertError(function_clause, macula_dht_routing_table:new(id(1), 0)).

%%---------------------------------------------------------------------
%% Bucket dispatch
%%---------------------------------------------------------------------

insert_places_peer_in_correct_bucket_test() ->
    %% Self = 0, Other = 2^255 → differs in bit 0 → bucket 255
    Self  = id(0),
    Other = <<1:1, 0:255>>,
    T0 = macula_dht_routing_table:new(Self),
    {admitted, T1} = macula_dht_routing_table:insert(entry(Other), T0, 0),
    ?assertEqual(1, macula_dht_routing_table:size(T1)),
    ?assertEqual(1, macula_dht_routing_table:bucket_count(T1)),
    ?assert(macula_dht_routing_table:contains(Other, T1)).

insert_peers_in_different_buckets_test() ->
    Self = id(0),
    %% differs in bit 0 → bucket 255
    A = <<1:1, 0:255>>,
    %% differs in bit 1 → bucket 254
    B = <<0:1, 1:1, 0:254>>,
    T0 = macula_dht_routing_table:new(Self),
    {admitted, T1} = macula_dht_routing_table:insert(entry(A), T0, 0),
    {admitted, T2} = macula_dht_routing_table:insert(entry(B), T1, 0),
    ?assertEqual(2, macula_dht_routing_table:size(T2)),
    ?assertEqual(2, macula_dht_routing_table:bucket_count(T2)).

insert_self_is_rejected_test() ->
    Self = id(42),
    T0 = macula_dht_routing_table:new(Self),
    {rejected, T1} = macula_dht_routing_table:insert(entry(Self), T0, 0),
    ?assertEqual(0, macula_dht_routing_table:size(T1)).

%%---------------------------------------------------------------------
%% Duplicate insert → touched
%%---------------------------------------------------------------------

insert_existing_id_touches_test() ->
    Self = id(0),
    Other = <<1:1, 0:255>>,
    E  = macula_dht_entry:new(base_spec(Other), 50),
    T0 = macula_dht_routing_table:new(Self),
    {admitted, T1} = macula_dht_routing_table:insert(E, T0, 100),
    {touched,  T2} = macula_dht_routing_table:insert(E, T1, 900),
    {ok, Found} = macula_dht_routing_table:find(Other, T2),
    ?assertEqual(50,  macula_dht_entry:first_seen(Found)),
    ?assertEqual(900, macula_dht_entry:last_seen(Found)),
    ?assertEqual(1, macula_dht_routing_table:size(T2)).

%%---------------------------------------------------------------------
%% Find / contains — empty bucket
%%---------------------------------------------------------------------

find_in_empty_table_returns_error_test() ->
    T = macula_dht_routing_table:new(id(0)),
    ?assertEqual(error, macula_dht_routing_table:find(id(123), T)).

contains_false_when_absent_test() ->
    T = macula_dht_routing_table:new(id(0)),
    ?assertNot(macula_dht_routing_table:contains(id(123), T)).

find_self_returns_error_test() ->
    T = macula_dht_routing_table:new(id(42)),
    ?assertEqual(error, macula_dht_routing_table:find(id(42), T)).

%%---------------------------------------------------------------------
%% Capacity respected per bucket
%%---------------------------------------------------------------------

bucket_capacity_respected_per_index_test() ->
    %% Fill bucket 255 (peers differing in bit 0) to K=2, verify a weak
    %% extra candidate is rejected and strong candidate in another
    %% bucket (bit 1) still admits.
    Self = id(0),
    T0 = macula_dht_routing_table:new(Self, 2),
    S1 = strong_entry(<<1:1, 0:255>>, 101, <<"BE">>, t0),
    S2 = strong_entry(<<1:1, 1:1, 0:254>>, 102, <<"NL">>, t1),
    {admitted, T1} = macula_dht_routing_table:insert(S1, T0, 0),
    {admitted, T2} = macula_dht_routing_table:insert(S2, T1, 0),
    ?assertEqual(2, macula_dht_bucket:size(
                     macula_dht_routing_table:bucket_at(255, T2))),
    Weak = weak_entry(<<1:1, 0:254, 1:1>>, 103, <<"FR">>, t2),
    {rejected, T3} = macula_dht_routing_table:insert(Weak, T2, 1_000),
    ?assertEqual(T2, T3),
    %% bucket 254 still empty
    ?assertEqual(0, macula_dht_bucket:size(
                     macula_dht_routing_table:bucket_at(254, T3))).

bucket_eviction_propagates_test() ->
    Self = id(0),
    T0 = macula_dht_routing_table:new(Self, 2),
    %% both weak peers in same bucket (bit 0 differs)
    W1 = weak_entry(<<1:1, 0:255>>, 101, <<"BE">>, t0),
    W2 = weak_entry(<<1:1, 1:1, 0:253, 1:1>>, 102, <<"BE">>, t0),
    {admitted, T1} = macula_dht_routing_table:insert(W1, T0, 0),
    {admitted, T2} = macula_dht_routing_table:insert(W2, T1, 0),
    %% both are bucket 255 (common prefix zero bits)? Actually W2 has a
    %% leading 1 → bucket 255 same as W1.
    Strong = strong_entry(<<1:1, 0:254, 1:1>>, 999, <<"JP">>, t3),
    {replaced, Evicted, T3} =
        macula_dht_routing_table:insert(Strong, T2, 1_000),
    ?assert(macula_dht_routing_table:contains(
              macula_dht_entry:node_id(Strong), T3)),
    ?assertNot(macula_dht_routing_table:contains(
                 macula_dht_entry:node_id(Evicted), T3)),
    ?assertEqual(2, macula_dht_routing_table:size(T3)).

%%---------------------------------------------------------------------
%% Remove / touch
%%---------------------------------------------------------------------

remove_drops_entry_test() ->
    Self = id(0),
    Other = <<1:1, 0:255>>,
    T0 = macula_dht_routing_table:new(Self),
    {admitted, T1} = macula_dht_routing_table:insert(entry(Other), T0, 0),
    T2 = macula_dht_routing_table:remove(Other, T1),
    ?assertEqual(0, macula_dht_routing_table:size(T2)),
    %% Bucket should be collapsed from the map when it becomes empty.
    ?assertEqual(0, macula_dht_routing_table:bucket_count(T2)).

remove_missing_is_noop_test() ->
    T = macula_dht_routing_table:new(id(0)),
    ?assertEqual(T, macula_dht_routing_table:remove(<<1:1, 0:255>>, T)).

remove_self_is_noop_test() ->
    T = macula_dht_routing_table:new(id(42)),
    ?assertEqual(T, macula_dht_routing_table:remove(id(42), T)).

touch_updates_last_seen_test() ->
    Self = id(0),
    Other = <<1:1, 0:255>>,
    E = macula_dht_entry:new(base_spec(Other), 100),
    T0 = macula_dht_routing_table:new(Self),
    {admitted, T1} = macula_dht_routing_table:insert(E, T0, 100),
    T2 = macula_dht_routing_table:touch(Other, T1, 777),
    {ok, Found} = macula_dht_routing_table:find(Other, T2),
    ?assertEqual(777, macula_dht_entry:last_seen(Found)).

touch_missing_is_noop_test() ->
    T = macula_dht_routing_table:new(id(0)),
    ?assertEqual(T, macula_dht_routing_table:touch(<<1:1, 0:255>>, T, 1000)).

%%---------------------------------------------------------------------
%% bucket_at
%%---------------------------------------------------------------------

bucket_at_unknown_returns_empty_bucket_of_capacity_test() ->
    T = macula_dht_routing_table:new(id(0), 7),
    B = macula_dht_routing_table:bucket_at(42, T),
    ?assertEqual(7, macula_dht_bucket:capacity(B)),
    ?assertEqual(0, macula_dht_bucket:size(B)).

bucket_at_populated_returns_filled_bucket_test() ->
    Self = id(0),
    Other = <<1:1, 0:255>>, %% bucket 255
    T0 = macula_dht_routing_table:new(Self),
    {admitted, T1} = macula_dht_routing_table:insert(entry(Other), T0, 0),
    B = macula_dht_routing_table:bucket_at(255, T1),
    ?assertEqual(1, macula_dht_bucket:size(B)).

%%---------------------------------------------------------------------
%% k_closest
%%---------------------------------------------------------------------

k_closest_zero_returns_empty_test() ->
    Self = id(0),
    Other = <<1:1, 0:255>>,
    T0 = macula_dht_routing_table:new(Self),
    {admitted, T1} = macula_dht_routing_table:insert(entry(Other), T0, 0),
    ?assertEqual([], macula_dht_routing_table:k_closest(id(99), 0, T1)).

k_closest_on_empty_returns_empty_test() ->
    T = macula_dht_routing_table:new(id(0)),
    ?assertEqual([], macula_dht_routing_table:k_closest(id(99), 20, T)).

k_closest_returns_ordered_by_distance_test() ->
    Self = id(0),
    %% three peers with varying closeness to target
    Target = <<1:8, 0:248>>,
    PeerA  = <<1:8, 0:248>>,                  %% zero distance to target
    PeerB  = <<1:8, 0:247, 1:1>>,             %% distance 1
    PeerC  = <<1:8, 0:240, 255:8>>,           %% distance 255
    T0 = macula_dht_routing_table:new(Self),
    {admitted, T1} = macula_dht_routing_table:insert(entry(PeerA), T0, 0),
    {admitted, T2} = macula_dht_routing_table:insert(entry(PeerC), T1, 0),
    {admitted, T3} = macula_dht_routing_table:insert(entry(PeerB), T2, 0),
    Result = macula_dht_routing_table:k_closest(Target, 3, T3),
    Ids = [macula_dht_entry:node_id(E) || E <- Result],
    ?assertEqual([PeerA, PeerB, PeerC], Ids).

k_closest_truncates_to_k_test() ->
    Self = id(0),
    T0 = macula_dht_routing_table:new(Self),
    Peers = [<<1:8, N:8, 0:240>> || N <- lists:seq(1, 10)],
    T1 = lists:foldl(fun(P, Acc) ->
        {admitted, Acc1} = macula_dht_routing_table:insert(entry(P), Acc, 0),
        Acc1
    end, T0, Peers),
    Result = macula_dht_routing_table:k_closest(<<1:8, 0:248>>, 3, T1),
    ?assertEqual(3, length(Result)).

%%---------------------------------------------------------------------
%% all_entries
%%---------------------------------------------------------------------

all_entries_collects_across_buckets_test() ->
    Self = id(0),
    A = <<1:1, 0:255>>,              %% bucket 255
    B = <<0:1, 1:1, 0:254>>,         %% bucket 254
    C = <<0:2, 1:1, 0:253>>,         %% bucket 253
    T0 = macula_dht_routing_table:new(Self),
    {admitted, T1} = macula_dht_routing_table:insert(entry(A), T0, 0),
    {admitted, T2} = macula_dht_routing_table:insert(entry(B), T1, 0),
    {admitted, T3} = macula_dht_routing_table:insert(entry(C), T2, 0),
    All = macula_dht_routing_table:all_entries(T3),
    ?assertEqual(3, length(All)),
    Ids = lists:sort([macula_dht_entry:node_id(E) || E <- All]),
    ?assertEqual(lists:sort([A, B, C]), Ids).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

id(N) -> <<N:256>>.

base_spec(NodeId) ->
    #{node_id => NodeId, asn => 100, country => <<"BE">>, tier => t0}.

base_spec(NodeId, Asn, Country, Tier) ->
    #{node_id => NodeId, asn => Asn, country => Country, tier => Tier}.

entry(NodeId) ->
    macula_dht_entry:new(base_spec(NodeId), 0).

strong_entry(NodeId, Asn, Country, Tier) ->
    E = macula_dht_entry:new(base_spec(NodeId, Asn, Country, Tier), 0),
    macula_dht_entry:with_uptime(
      macula_dht_entry:with_latency(E, 30), 0.95).

weak_entry(NodeId, Asn, Country, Tier) ->
    E = macula_dht_entry:new(base_spec(NodeId, Asn, Country, Tier), 0),
    macula_dht_entry:with_uptime(
      macula_dht_entry:with_latency(E, 450), 0.05).
