%% EUnit tests for macula_dht_siblings.
-module(macula_dht_siblings_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Construction
%%---------------------------------------------------------------------

new_default_s16_empty_test() ->
    S = macula_dht_siblings:new(id(0)),
    ?assertEqual(id(0), macula_dht_siblings:self_id(S)),
    ?assertEqual(16, macula_dht_siblings:capacity(S)),
    ?assertEqual(0, macula_dht_siblings:size(S)),
    ?assertEqual([], macula_dht_siblings:members(S)).

new_with_custom_capacity_test() ->
    S = macula_dht_siblings:new(id(0), 4),
    ?assertEqual(4, macula_dht_siblings:capacity(S)).

new_with_zero_capacity_rejected_test() ->
    ?assertError(function_clause, macula_dht_siblings:new(id(0), 0)).

%%---------------------------------------------------------------------
%% Insert — space available
%%---------------------------------------------------------------------

insert_into_empty_admits_test() ->
    S0 = macula_dht_siblings:new(id(0), 4),
    E  = entry(<<1:1, 0:255>>),
    {admitted, S1} = macula_dht_siblings:insert(E, S0, 0),
    ?assertEqual(1, macula_dht_siblings:size(S1)),
    ?assert(macula_dht_siblings:contains(
              macula_dht_entry:node_id(E), S1)).

insert_distinct_admits_all_within_capacity_test() ->
    Self = id(0),
    S0 = macula_dht_siblings:new(Self, 4),
    Peers = [<<N:8, 0:248>> || N <- lists:seq(1, 4)],
    S1 = admit_all(S0, [entry(P) || P <- Peers]),
    ?assertEqual(4, macula_dht_siblings:size(S1)).

%%---------------------------------------------------------------------
%% Members returned in ascending distance order
%%---------------------------------------------------------------------

members_sorted_by_distance_ascending_test() ->
    Self = id(0),
    %% inserted in non-closest-first order
    Far  = <<255:8, 0:248>>,
    Mid  = <<16:8,  0:248>>,
    Near = <<1:8,   0:248>>,
    S0 = macula_dht_siblings:new(Self, 4),
    {admitted, S1} = macula_dht_siblings:insert(entry(Far),  S0, 0),
    {admitted, S2} = macula_dht_siblings:insert(entry(Near), S1, 0),
    {admitted, S3} = macula_dht_siblings:insert(entry(Mid),  S2, 0),
    Ids = [macula_dht_entry:node_id(E)
             || E <- macula_dht_siblings:members(S3)],
    ?assertEqual([Near, Mid, Far], Ids).

%%---------------------------------------------------------------------
%% Self is not admitted
%%---------------------------------------------------------------------

insert_self_is_rejected_test() ->
    Self = id(42),
    S0 = macula_dht_siblings:new(Self, 4),
    {rejected, S1} = macula_dht_siblings:insert(entry(Self), S0, 0),
    ?assertEqual(0, macula_dht_siblings:size(S1)).

%%---------------------------------------------------------------------
%% Duplicate ID → touched
%%---------------------------------------------------------------------

insert_existing_id_touches_test() ->
    Self = id(0),
    Other = <<1:1, 0:255>>,
    E = macula_dht_entry:new(base_spec(Other), 50),
    S0 = macula_dht_siblings:new(Self, 4),
    {admitted, S1} = macula_dht_siblings:insert(E, S0, 100),
    {touched,  S2} = macula_dht_siblings:insert(E, S1, 900),
    {ok, Found} = macula_dht_siblings:find(Other, S2),
    ?assertEqual(50, macula_dht_entry:first_seen(Found)),
    ?assertEqual(900, macula_dht_entry:last_seen(Found)),
    ?assertEqual(1, macula_dht_siblings:size(S2)).

%%---------------------------------------------------------------------
%% Full set — admission by distance only
%%---------------------------------------------------------------------

insert_full_closer_candidate_replaces_farthest_test() ->
    Self = id(0),
    S0 = macula_dht_siblings:new(Self, 2),
    Far  = <<128:8, 0:248>>,
    Near = <<1:8,   0:248>>,
    Mid  = <<4:8,   0:248>>,
    {admitted, S1} = macula_dht_siblings:insert(entry(Far),  S0, 0),
    {admitted, S2} = macula_dht_siblings:insert(entry(Near), S1, 0),
    {replaced, Evicted, S3} =
        macula_dht_siblings:insert(entry(Mid), S2, 1000),
    ?assertEqual(Far, macula_dht_entry:node_id(Evicted)),
    ?assertEqual(2, macula_dht_siblings:size(S3)),
    ?assertNot(macula_dht_siblings:contains(Far, S3)),
    ?assert(macula_dht_siblings:contains(Mid, S3)),
    ?assert(macula_dht_siblings:contains(Near, S3)).

insert_full_farther_candidate_rejected_test() ->
    Self = id(0),
    S0 = macula_dht_siblings:new(Self, 2),
    Near1 = <<1:8, 0:248>>,
    Near2 = <<2:8, 0:248>>,
    Far   = <<255:8, 0:248>>,
    {admitted, S1} = macula_dht_siblings:insert(entry(Near1), S0, 0),
    {admitted, S2} = macula_dht_siblings:insert(entry(Near2), S1, 0),
    {rejected, S3} = macula_dht_siblings:insert(entry(Far), S2, 1000),
    ?assertEqual(S2, S3).

%%---------------------------------------------------------------------
%% Farthest accessor
%%---------------------------------------------------------------------

farthest_on_empty_returns_error_test() ->
    S = macula_dht_siblings:new(id(0)),
    ?assertEqual(error, macula_dht_siblings:farthest(S)).

farthest_returns_last_by_distance_test() ->
    Self = id(0),
    Near = <<1:8, 0:248>>,
    Far  = <<255:8, 0:248>>,
    S0 = macula_dht_siblings:new(Self, 4),
    {admitted, S1} = macula_dht_siblings:insert(entry(Far),  S0, 0),
    {admitted, S2} = macula_dht_siblings:insert(entry(Near), S1, 0),
    {ok, E} = macula_dht_siblings:farthest(S2),
    ?assertEqual(Far, macula_dht_entry:node_id(E)).

%%---------------------------------------------------------------------
%% Remove / touch
%%---------------------------------------------------------------------

remove_drops_entry_test() ->
    Self = id(0),
    Other = <<1:8, 0:248>>,
    S0 = macula_dht_siblings:new(Self),
    {admitted, S1} = macula_dht_siblings:insert(entry(Other), S0, 0),
    S2 = macula_dht_siblings:remove(Other, S1),
    ?assertEqual(0, macula_dht_siblings:size(S2)),
    ?assertNot(macula_dht_siblings:contains(Other, S2)).

remove_missing_is_noop_test() ->
    S = macula_dht_siblings:new(id(0)),
    ?assertEqual(S, macula_dht_siblings:remove(<<1:8, 0:248>>, S)).

touch_updates_last_seen_test() ->
    Self = id(0),
    Other = <<1:8, 0:248>>,
    E = macula_dht_entry:new(base_spec(Other), 100),
    S0 = macula_dht_siblings:new(Self),
    {admitted, S1} = macula_dht_siblings:insert(E, S0, 100),
    S2 = macula_dht_siblings:touch(Other, S1, 999),
    {ok, Found} = macula_dht_siblings:find(Other, S2),
    ?assertEqual(999, macula_dht_entry:last_seen(Found)).

touch_missing_is_noop_test() ->
    S = macula_dht_siblings:new(id(0)),
    ?assertEqual(S, macula_dht_siblings:touch(<<1:8, 0:248>>, S, 999)).

%%---------------------------------------------------------------------
%% node_ids accessor
%%---------------------------------------------------------------------

node_ids_returned_by_distance_test() ->
    Self = id(0),
    Far  = <<64:8, 0:248>>,
    Near = <<1:8,  0:248>>,
    S0 = macula_dht_siblings:new(Self, 4),
    {admitted, S1} = macula_dht_siblings:insert(entry(Far),  S0, 0),
    {admitted, S2} = macula_dht_siblings:insert(entry(Near), S1, 0),
    ?assertEqual([Near, Far], macula_dht_siblings:node_ids(S2)).

%%---------------------------------------------------------------------
%% Find / contains on empty
%%---------------------------------------------------------------------

find_in_empty_returns_error_test() ->
    S = macula_dht_siblings:new(id(0)),
    ?assertEqual(error, macula_dht_siblings:find(id(99), S)).

contains_false_when_absent_test() ->
    S = macula_dht_siblings:new(id(0)),
    ?assertNot(macula_dht_siblings:contains(id(99), S)).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

id(N) -> <<N:256>>.

base_spec(NodeId) ->
    #{node_id => NodeId, asn => 100, country => <<"BE">>, tier => t0}.

entry(NodeId) ->
    macula_dht_entry:new(base_spec(NodeId), 0).

admit_all(Siblings, Entries) ->
    lists:foldl(fun(E, S) ->
        {admitted, S1} = macula_dht_siblings:insert(E, S, 0),
        S1
    end, Siblings, Entries).
