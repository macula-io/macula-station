%% @doc Kademlia routing table — 256 buckets addressed by XOR
%% common-prefix length with the station's own NodeId.
%%
%% Bucket `i' holds peers `Other' such that
%% `hecate_dht_xor:bucket_index(Self, Other) == i', i.e. peers that
%% agree with `Self' on the first `(255 - i)' bits and differ on the
%% `(255 - i + 1)'-th. Bucket `0' therefore holds the most distant
%% peers (top-bit differs), bucket `255' holds the closest (only the
%% last bit differs).
%%
%% The table is a pure value: insertions return a new table, never
%% mutate. Process ownership lives one level up in
%% `hecate_dht_server'. Buckets below `hecate_dht_bucket' keep all
%% scoring / eviction logic; this module only dispatches to the
%% correct bucket by index.
%%
%% Storage is sparse: only buckets that have ever held an entry are
%% materialised. Buckets are stored in a `#{BucketIx => bucket()}'
%% map; unknown indices are treated as empty buckets of the table's
%% configured capacity.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §3.2 (bucket
%% indexing), §4.3 (admission), §11 (maintenance).
-module(hecate_dht_routing_table).

-export([
    new/1,
    new/2,
    self_id/1,
    capacity/1,
    size/1,
    bucket_count/1,
    bucket_at/2,
    insert/2,
    insert/3,
    remove/2,
    touch/3,
    find/2,
    contains/2,
    all_entries/1,
    k_closest/3
]).

-export_type([table/0, insert_result/0]).

-define(DEFAULT_K, 20).

-type table() :: #{
    self_id  := hecate_dht_xor:id(),
    capacity := pos_integer(),
    buckets  := #{hecate_dht_xor:bucket_ix() => hecate_dht_bucket:bucket()}
}.

-type insert_result() ::
      {admitted, table()}
    | {touched,  table()}
    | {replaced, Evicted :: hecate_dht_entry:entry(), table()}
    | {rejected, table()}.

%%=====================================================================
%% Construction
%%=====================================================================

-spec new(hecate_dht_xor:id()) -> table().
new(Self) ->
    new(Self, ?DEFAULT_K).

-spec new(hecate_dht_xor:id(), pos_integer()) -> table().
new(<<_:256>> = Self, K) when is_integer(K), K >= 1 ->
    #{self_id => Self, capacity => K, buckets => #{}}.

%%=====================================================================
%% Inspection
%%=====================================================================

-spec self_id(table())  -> hecate_dht_xor:id().
self_id(#{self_id := V})  -> V.

-spec capacity(table()) -> pos_integer().
capacity(#{capacity := K}) -> K.

-spec size(table()) -> non_neg_integer().
size(#{buckets := Bs}) ->
    maps:fold(fun(_, B, Acc) -> Acc + hecate_dht_bucket:size(B) end, 0, Bs).

-spec bucket_count(table()) -> non_neg_integer().
bucket_count(#{buckets := Bs}) -> map_size(Bs).

-spec bucket_at(hecate_dht_xor:bucket_ix(), table()) -> hecate_dht_bucket:bucket().
bucket_at(Ix, #{capacity := K, buckets := Bs}) ->
    maps:get(Ix, Bs, hecate_dht_bucket:new(K)).

-spec find(hecate_dht_xor:id(), table()) ->
        {ok, hecate_dht_entry:entry()} | error.
find(Id, #{self_id := Self} = T) ->
    find_at(hecate_dht_xor:bucket_index(Self, Id), Id, T).

-spec contains(hecate_dht_xor:id(), table()) -> boolean().
contains(Id, T) ->
    case find(Id, T) of
        {ok, _} -> true;
        error   -> false
    end.

-spec all_entries(table()) -> [hecate_dht_entry:entry()].
all_entries(#{buckets := Bs}) ->
    maps:fold(fun(_, B, Acc) -> hecate_dht_bucket:members(B) ++ Acc end, [], Bs).

%% @doc K entries closest-by-XOR to `Target' from the whole table.
%% Entries are returned ordered by ascending distance to `Target'.
-spec k_closest(hecate_dht_xor:id(), non_neg_integer(), table()) ->
          [hecate_dht_entry:entry()].
k_closest(_Target, 0, _T) ->
    [];
k_closest(<<_:256>> = Target, K, T) when is_integer(K), K >= 0 ->
    Keyed = [{hecate_dht_xor:distance_int(Target, hecate_dht_entry:node_id(E)), E}
             || E <- all_entries(T)],
    Sorted = lists:keysort(1, Keyed),
    [E || {_, E} <- lists:sublist(Sorted, K)].

%%=====================================================================
%% Insertion / touch / removal
%%=====================================================================

-spec insert(hecate_dht_entry:entry(), table()) -> insert_result().
insert(Cand, T) ->
    insert(Cand, T, erlang:monotonic_time(millisecond)).

-spec insert(hecate_dht_entry:entry(), table(), integer()) -> insert_result().
insert(Cand, #{self_id := Self} = T, Now) ->
    Id = hecate_dht_entry:node_id(Cand),
    dispatch_insert(hecate_dht_xor:bucket_index(Self, Id), Cand, T, Now).

-spec dispatch_insert(hecate_dht_xor:bucket_ix(),
                      hecate_dht_entry:entry(), table(), integer()) ->
          insert_result().
dispatch_insert(-1, _Cand, T, _Now) ->
    {rejected, T};
dispatch_insert(Ix, Cand, T, Now) ->
    Bucket = bucket_at(Ix, T),
    relay_bucket_result(hecate_dht_bucket:insert(Cand, Bucket, Now), Ix, T).

-spec relay_bucket_result(hecate_dht_bucket:insert_result(),
                          hecate_dht_xor:bucket_ix(), table()) ->
          insert_result().
relay_bucket_result({admitted, B}, Ix, T) ->
    {admitted, put_bucket(Ix, B, T)};
relay_bucket_result({touched, B}, Ix, T) ->
    {touched, put_bucket(Ix, B, T)};
relay_bucket_result({replaced, Ev, B}, Ix, T) ->
    {replaced, Ev, put_bucket(Ix, B, T)};
relay_bucket_result({rejected, _B}, _Ix, T) ->
    {rejected, T}.

-spec remove(hecate_dht_xor:id(), table()) -> table().
remove(<<_:256>> = Id, #{self_id := Self} = T) ->
    remove_at(hecate_dht_xor:bucket_index(Self, Id), Id, T).

-spec remove_at(hecate_dht_xor:bucket_ix(), hecate_dht_xor:id(), table()) ->
          table().
remove_at(-1, _Id, T) -> T;
remove_at(Ix, Id, #{buckets := Bs} = T) ->
    update_existing(maps:find(Ix, Bs), Ix, Id, T).

-spec update_existing({ok, hecate_dht_bucket:bucket()} | error,
                      hecate_dht_xor:bucket_ix(), hecate_dht_xor:id(),
                      table()) -> table().
update_existing(error, _Ix, _Id, T) ->
    T;
update_existing({ok, B}, Ix, Id, T) ->
    put_bucket(Ix, hecate_dht_bucket:remove(Id, B), T).

-spec touch(hecate_dht_xor:id(), table(), integer()) -> table().
touch(<<_:256>> = Id, #{self_id := Self} = T, Now) ->
    touch_at(hecate_dht_xor:bucket_index(Self, Id), Id, T, Now).

-spec touch_at(hecate_dht_xor:bucket_ix(), hecate_dht_xor:id(),
               table(), integer()) -> table().
touch_at(-1, _Id, T, _Now) -> T;
touch_at(Ix, Id, #{buckets := Bs} = T, Now) ->
    touch_existing(maps:find(Ix, Bs), Ix, Id, T, Now).

-spec touch_existing({ok, hecate_dht_bucket:bucket()} | error,
                     hecate_dht_xor:bucket_ix(), hecate_dht_xor:id(),
                     table(), integer()) -> table().
touch_existing(error, _Ix, _Id, T, _Now) ->
    T;
touch_existing({ok, B}, Ix, Id, T, Now) ->
    put_bucket(Ix, hecate_dht_bucket:touch(Id, B, Now), T).

%%=====================================================================
%% Internal
%%=====================================================================

-spec find_at(hecate_dht_xor:bucket_ix(), hecate_dht_xor:id(), table()) ->
          {ok, hecate_dht_entry:entry()} | error.
find_at(-1, _Id, _T) ->
    error;
find_at(Ix, Id, #{buckets := Bs}) ->
    find_in(maps:find(Ix, Bs), Id).

-spec find_in({ok, hecate_dht_bucket:bucket()} | error,
              hecate_dht_xor:id()) ->
          {ok, hecate_dht_entry:entry()} | error.
find_in(error, _Id)   -> error;
find_in({ok, B}, Id)  -> hecate_dht_bucket:find(Id, B).

-spec put_bucket(hecate_dht_xor:bucket_ix(), hecate_dht_bucket:bucket(),
                 table()) -> table().
put_bucket(Ix, B, #{buckets := Bs} = T) ->
    T#{buckets := store_or_drop(hecate_dht_bucket:size(B), Ix, B, Bs)}.

-spec store_or_drop(non_neg_integer(), hecate_dht_xor:bucket_ix(),
                    hecate_dht_bucket:bucket(),
                    #{hecate_dht_xor:bucket_ix() => hecate_dht_bucket:bucket()}) ->
          #{hecate_dht_xor:bucket_ix() => hecate_dht_bucket:bucket()}.
store_or_drop(0, Ix, _B, Bs) -> maps:remove(Ix, Bs);
store_or_drop(_, Ix, B, Bs)  -> Bs#{Ix => B}.
