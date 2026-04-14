%% @doc XOR metric for S/Kademlia routing over 256-bit NodeIds.
%%
%% All Macula NodeIds are Ed25519 public keys (32 bytes / 256 bits). The
%% Kademlia routing space is the metric defined by bitwise XOR:
%% `d(A, B) = A xor B'. Smaller distance = "closer".
%%
%% This module is a pure library — no processes, no ETS, deterministic.
%% Everything it offers is a folding block for the routing table
%% (`hecate_dht_routing_table'), the lookup algorithm
%% (`hecate_dht_lookup'), and the replica-placement algorithm
%% (`hecate_dht_placement').
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §4.1-4.2.
-module(hecate_dht_xor).

-export([
    distance/2,
    distance_int/2,
    common_prefix_bits/2,
    bucket_index/2,
    closer/3,
    sort_by_distance/2,
    k_closest/3
]).

-export_type([id/0, distance/0, bucket_ix/0]).

-define(BITS, 256).

-type id()        :: <<_:?BITS>>.
-type distance()  :: <<_:?BITS>>.
-type bucket_ix() :: -1 | 0..255.

%%=====================================================================
%% Public API
%%=====================================================================

%% @doc Full XOR distance as a 256-bit binary.
-spec distance(id(), id()) -> distance().
distance(<<A:?BITS>>, <<B:?BITS>>) ->
    X = A bxor B,
    <<X:?BITS>>.

%% @doc XOR distance as a non-negative integer for comparison.
-spec distance_int(id(), id()) -> non_neg_integer().
distance_int(<<A:?BITS>>, <<B:?BITS>>) ->
    A bxor B.

%% @doc Count of leading bits where `A' and `B' agree. Range `0..256'.
%%
%% `common_prefix_bits(X, X) == 256'.
-spec common_prefix_bits(id(), id()) -> 0..?BITS.
common_prefix_bits(Id, Id) ->
    ?BITS;
common_prefix_bits(<<_:?BITS/bits>> = A, <<_:?BITS/bits>> = B) ->
    count_prefix(A, B, 0).

%% @doc Kademlia bucket index for `Other' relative to `Self'.
%%
%% The bucket holding `Other' is `255 - common_prefix_bits(Self, Other)'.
%% A node never belongs in its own routing table, so `Self = Other'
%% yields `-1'.
-spec bucket_index(id(), id()) -> bucket_ix().
bucket_index(Id, Id) ->
    -1;
bucket_index(<<_:?BITS/bits>> = Self, <<_:?BITS/bits>> = Other) ->
    (?BITS - 1) - common_prefix_bits(Self, Other).

%% @doc Three-way comparator: which of `A' or `B' is closer to `Target'?
%% Returns `a', `b', or `eq' if both are the same distance.
-spec closer(id(), id(), id()) -> a | b | eq.
closer(Target, A, B) ->
    compare(distance_int(Target, A), distance_int(Target, B)).

%% @doc Sort IDs by ascending XOR distance from `Target'.
-spec sort_by_distance(id(), [id()]) -> [id()].
sort_by_distance(Target, Ids) ->
    Keyed = [{distance_int(Target, Id), Id} || Id <- Ids],
    [Id || {_, Id} <- lists:sort(Keyed)].

%% @doc Top-K closest IDs to `Target' from `Ids', ascending distance.
-spec k_closest(id(), [id()], non_neg_integer()) -> [id()].
k_closest(Target, Ids, K) ->
    lists:sublist(sort_by_distance(Target, Ids), K).

%%=====================================================================
%% Internal
%%=====================================================================

-spec count_prefix(binary(), binary(), non_neg_integer()) -> 0..?BITS.
count_prefix(<<Byte:8, RestA/bits>>, <<Byte:8, RestB/bits>>, Acc) ->
    count_prefix(RestA, RestB, Acc + 8);
count_prefix(<<ByteA:8, _/bits>>, <<ByteB:8, _/bits>>, Acc) ->
    Acc + leading_zeros_byte(ByteA bxor ByteB);
count_prefix(<<>>, <<>>, Acc) ->
    Acc.

-spec leading_zeros_byte(0..255) -> 0..8.
leading_zeros_byte(N) when N >= 128 -> 0;
leading_zeros_byte(N) when N >=  64 -> 1;
leading_zeros_byte(N) when N >=  32 -> 2;
leading_zeros_byte(N) when N >=  16 -> 3;
leading_zeros_byte(N) when N >=   8 -> 4;
leading_zeros_byte(N) when N >=   4 -> 5;
leading_zeros_byte(N) when N >=   2 -> 6;
leading_zeros_byte(1)               -> 7;
leading_zeros_byte(0)               -> 8.

-spec compare(integer(), integer()) -> a | b | eq.
compare(D, D)              -> eq;
compare(A, B) when A < B   -> a;
compare(_, _)              -> b.
