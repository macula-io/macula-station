%% @doc S/Kademlia sibling list — the `S=16' peers closest-by-XOR to
%% the station's own NodeId, maintained irrespective of bucket
%% boundaries.
%%
%% Siblings serve three purposes (Part 3 §4.4):
%%
%% <ul>
%%   <li>Fast rebalancing after local churn — the sibling set is the
%%       first place a republish runs.</li>
%%   <li>SWIM probe preference — siblings get probed more often.</li>
%%   <li>Realm-directory coherence — in-realm siblings gossip via
%%       HyParView first.</li>
%% </ul>
%%
%% Admission is *pure XOR distance* from `Self' — unlike buckets,
%% there is no uptime / novelty scoring. Siblings are by definition
%% "the closest peers we know about"; any peer closer than the
%% current farthest sibling displaces it.
%%
%% This module is a pure value. State ownership lives in
%% `macula_dht_server'.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §4.1 (S/Kademlia
%% extensions), §4.4 (sibling composition).
-module(macula_dht_siblings).

-export([
    new/1,
    new/2,
    self_id/1,
    capacity/1,
    size/1,
    members/1,
    node_ids/1,
    find/2,
    contains/2,
    farthest/1,
    insert/2,
    insert/3,
    remove/2,
    touch/3
]).

-export_type([siblings/0, insert_result/0]).

-define(DEFAULT_S, 16).

-type siblings() :: #{
    self_id  := macula_dht_xor:id(),
    capacity := pos_integer(),
    %% entries are stored as {DistanceInt, Entry} pairs, ordered by
    %% ascending distance (closest first). Distance is cached because
    %% it is needed on every insert comparison.
    entries  := [{non_neg_integer(), macula_dht_entry:entry()}]
}.

-type insert_result() ::
      {admitted, siblings()}
    | {touched,  siblings()}
    | {replaced, Evicted :: macula_dht_entry:entry(), siblings()}
    | {rejected, siblings()}.

%%=====================================================================
%% Construction
%%=====================================================================

-spec new(macula_dht_xor:id()) -> siblings().
new(Self) ->
    new(Self, ?DEFAULT_S).

-spec new(macula_dht_xor:id(), pos_integer()) -> siblings().
new(<<_:256>> = Self, S) when is_integer(S), S >= 1 ->
    #{self_id => Self, capacity => S, entries => []}.

%%=====================================================================
%% Inspection
%%=====================================================================

-spec self_id(siblings())  -> macula_dht_xor:id().
self_id(#{self_id := V})     -> V.

-spec capacity(siblings()) -> pos_integer().
capacity(#{capacity := S})   -> S.

-spec size(siblings()) -> non_neg_integer().
size(#{entries := Es}) -> length(Es).

-spec members(siblings()) -> [macula_dht_entry:entry()].
members(#{entries := Es}) -> [E || {_, E} <- Es].

-spec node_ids(siblings()) -> [macula_dht_xor:id()].
node_ids(S) -> [macula_dht_entry:node_id(E) || E <- members(S)].

-spec find(macula_dht_xor:id(), siblings()) ->
        {ok, macula_dht_entry:entry()} | error.
find(Id, #{entries := Es}) ->
    find_by_id(Id, Es).

-spec contains(macula_dht_xor:id(), siblings()) -> boolean().
contains(Id, S) ->
    case find(Id, S) of
        {ok, _} -> true;
        error   -> false
    end.

-spec farthest(siblings()) -> {ok, macula_dht_entry:entry()} | error.
farthest(#{entries := []})      -> error;
farthest(#{entries := Es})      -> {ok, element(2, lists:last(Es))}.

%%=====================================================================
%% Insertion / touch / removal
%%=====================================================================

-spec insert(macula_dht_entry:entry(), siblings()) -> insert_result().
insert(Cand, S) ->
    insert(Cand, S, erlang:monotonic_time(millisecond)).

-spec insert(macula_dht_entry:entry(), siblings(), integer()) -> insert_result().
insert(Cand, #{self_id := Self} = S, Now) ->
    Id = macula_dht_entry:node_id(Cand),
    dispatch_insert(id_equals(Id, Self), Cand, Id, S, Now).

-spec dispatch_insert(boolean(), macula_dht_entry:entry(),
                      macula_dht_xor:id(), siblings(), integer()) ->
          insert_result().
dispatch_insert(true, _Cand, _Id, S, _Now) ->
    {rejected, S};
dispatch_insert(false, Cand, Id, S, Now) ->
    consider(find_entry(Id, S), Cand, S, Now).

-spec consider({ok, macula_dht_entry:entry()} | error,
               macula_dht_entry:entry(), siblings(), integer()) ->
          insert_result().
consider({ok, _Existing}, Cand, S, Now) ->
    Id = macula_dht_entry:node_id(Cand),
    {touched, map_entries(fun(Es) -> touch_in(Id, Es, Now) end, S)};
consider(error, Cand, #{self_id := Self, capacity := Cap, entries := Es} = S,
         _Now) ->
    Dist = macula_dht_xor:distance_int(Self, macula_dht_entry:node_id(Cand)),
    admit_or_replace(length(Es) < Cap, Dist, Cand, S).

-spec admit_or_replace(boolean(), non_neg_integer(),
                       macula_dht_entry:entry(), siblings()) ->
          insert_result().
admit_or_replace(true, Dist, Cand, #{entries := Es} = S) ->
    {admitted, S#{entries := insert_sorted({Dist, Cand}, Es)}};
admit_or_replace(false, Dist, Cand, #{entries := Es} = S) ->
    {FarDist, FarEntry} = lists:last(Es),
    decide_full(Dist < FarDist, {FarDist, FarEntry}, Dist, Cand, S).

-spec decide_full(boolean(),
                  {non_neg_integer(), macula_dht_entry:entry()},
                  non_neg_integer(), macula_dht_entry:entry(),
                  siblings()) -> insert_result().
decide_full(false, _Far, _Dist, _Cand, S) ->
    {rejected, S};
decide_full(true, {_FarDist, FarEntry}, Dist, Cand, #{entries := Es} = S) ->
    Trimmed = lists:droplast(Es),
    {replaced, FarEntry, S#{entries := insert_sorted({Dist, Cand}, Trimmed)}}.

-spec remove(macula_dht_xor:id(), siblings()) -> siblings().
remove(<<_:256>> = Id, S) ->
    map_entries(fun(Es) -> drop_id(Id, Es) end, S).

-spec touch(macula_dht_xor:id(), siblings(), integer()) -> siblings().
touch(<<_:256>> = Id, S, Now) ->
    map_entries(fun(Es) -> touch_in(Id, Es, Now) end, S).

%%=====================================================================
%% Internal
%%=====================================================================

-spec id_equals(macula_dht_xor:id(), macula_dht_xor:id()) -> boolean().
id_equals(X, X) -> true;
id_equals(_, _) -> false.

-spec find_entry(macula_dht_xor:id(), siblings()) ->
          {ok, macula_dht_entry:entry()} | error.
find_entry(Id, #{entries := Es}) ->
    find_by_id(Id, Es).

-spec find_by_id(macula_dht_xor:id(),
                 [{non_neg_integer(), macula_dht_entry:entry()}]) ->
          {ok, macula_dht_entry:entry()} | error.
find_by_id(_Id, []) -> error;
find_by_id(Id, [{_, E} | Rest]) ->
    find_or_continue(macula_dht_entry:node_id(E) =:= Id, E, Id, Rest).

-spec find_or_continue(boolean(), macula_dht_entry:entry(),
                       macula_dht_xor:id(),
                       [{non_neg_integer(), macula_dht_entry:entry()}]) ->
          {ok, macula_dht_entry:entry()} | error.
find_or_continue(true,  E, _Id, _Rest) -> {ok, E};
find_or_continue(false, _, Id, Rest)   -> find_by_id(Id, Rest).

-spec insert_sorted({non_neg_integer(), macula_dht_entry:entry()},
                    [{non_neg_integer(), macula_dht_entry:entry()}]) ->
          [{non_neg_integer(), macula_dht_entry:entry()}].
insert_sorted(Pair, Es) ->
    lists:keymerge(1, [Pair], Es).

-spec drop_id(macula_dht_xor:id(),
              [{non_neg_integer(), macula_dht_entry:entry()}]) ->
          [{non_neg_integer(), macula_dht_entry:entry()}].
drop_id(Id, Es) ->
    [Pair || {_, E} = Pair <- Es, macula_dht_entry:node_id(E) =/= Id].

-spec touch_in(macula_dht_xor:id(),
               [{non_neg_integer(), macula_dht_entry:entry()}],
               integer()) ->
          [{non_neg_integer(), macula_dht_entry:entry()}].
touch_in(Id, Es, Now) ->
    [maybe_touch(Id, Pair, Now) || Pair <- Es].

-spec maybe_touch(macula_dht_xor:id(),
                  {non_neg_integer(), macula_dht_entry:entry()},
                  integer()) ->
          {non_neg_integer(), macula_dht_entry:entry()}.
maybe_touch(Id, {D, E}, Now) ->
    apply_touch(macula_dht_entry:node_id(E) =:= Id, D, E, Now).

-spec apply_touch(boolean(), non_neg_integer(),
                  macula_dht_entry:entry(), integer()) ->
          {non_neg_integer(), macula_dht_entry:entry()}.
apply_touch(true,  D, E, Now) -> {D, macula_dht_entry:touch(E, Now)};
apply_touch(false, D, E, _Now) -> {D, E}.

-spec map_entries(fun(([{non_neg_integer(), macula_dht_entry:entry()}]) ->
                          [{non_neg_integer(), macula_dht_entry:entry()}]),
                  siblings()) -> siblings().
map_entries(F, #{entries := Es} = S) ->
    S#{entries := F(Es)}.
