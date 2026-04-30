%%% @doc Bloom filter for cross-relay subscription routing.
%%%
%%% Ported from V1 `macula_relay_bloom'. Each station computes a
%%% filter of its locally-subscribed topics. Stations exchange filters
%%% via gossip. On publish, check peer filters — forward only to
%%% relays whose filter matches.
%%%
%%% Parameters tuned for relay use:
%%%   - 8192 bits (1KB) per filter
%%%   - 7 hash functions
%%%   - ~1% false-positive rate at 1000 topics
%%%   - Zero false negatives (mathematical guarantee)
%%%
%%% Filters serialise to a 1KB binary for `_mesh.bloom' gossip.
-module(macula_station_bloom).

-export([new/0, add/2, check/2, merge/2]).
-export([to_binary/1, from_binary/1]).
-export([count_bits_set/1, estimated_elements/1]).

-define(BITS,  8192).         %% 1KB filter
-define(HASHES, 7).           %% Optimal for ~1000 elements at 1% FP
-define(BYTES, (?BITS div 8)).

-record(bloom, {
    bits       :: binary(),
    num_hashes :: pos_integer()
}).

-opaque bloom() :: #bloom{}.
-export_type([bloom/0]).

%%====================================================================
%% API
%%====================================================================

-spec new() -> bloom().
new() ->
    #bloom{bits = <<0:?BITS>>, num_hashes = ?HASHES}.

-spec add(binary(), bloom()) -> bloom().
add(Element, #bloom{bits = Bits, num_hashes = K} = BF) ->
    Positions = hash_positions(Element, K),
    BF#bloom{bits = set_bits(Positions, Bits)}.

-spec check(binary(), bloom()) -> boolean().
check(Element, #bloom{bits = Bits, num_hashes = K}) ->
    all_bits_set(hash_positions(Element, K), Bits).

-spec merge(bloom(), bloom()) -> bloom().
merge(#bloom{bits = A}, #bloom{bits = B}) ->
    #bloom{bits = bin_or(A, B), num_hashes = ?HASHES}.

-spec to_binary(bloom()) -> binary().
to_binary(#bloom{bits = Bits}) -> Bits.

-spec from_binary(binary()) -> bloom().
from_binary(Bits) when byte_size(Bits) =:= ?BYTES ->
    #bloom{bits = Bits, num_hashes = ?HASHES}.

-spec count_bits_set(bloom()) -> non_neg_integer().
count_bits_set(#bloom{bits = Bits}) -> popcount(Bits).

-spec estimated_elements(bloom()) -> float() | infinity.
estimated_elements(BF) ->
    X = count_bits_set(BF),
    case X of
        0       -> 0.0;
        ?BITS   -> infinity;
        _       -> -(?BITS / ?HASHES) * math:log(1 - X / ?BITS)
    end.

%%====================================================================
%% Internal
%%====================================================================

%% Double hashing — `position_i = (h1 + i * h2) mod m'.
hash_positions(Element, K) ->
    H1 = erlang:phash2(Element, ?BITS),
    H2 = erlang:phash2({Element, salt}, ?BITS),
    [((H1 + I * H2) rem ?BITS) || I <- lists:seq(0, K - 1)].

set_bits([], Bits) -> Bits;
set_bits([Pos | Rest], Bits) ->
    ByteIdx = Pos div 8,
    BitIdx  = Pos rem 8,
    <<Before:ByteIdx/binary, Byte:8, After/binary>> = Bits,
    NewByte = Byte bor (1 bsl BitIdx),
    set_bits(Rest, <<Before/binary, NewByte:8, After/binary>>).

all_bits_set([], _Bits) -> true;
all_bits_set([Pos | Rest], Bits) ->
    ByteIdx = Pos div 8,
    BitIdx  = Pos rem 8,
    <<_:ByteIdx/binary, Byte:8, _/binary>> = Bits,
    case Byte band (1 bsl BitIdx) of
        0 -> false;
        _ -> all_bits_set(Rest, Bits)
    end.

bin_or(<<>>, <<>>) -> <<>>;
bin_or(<<A:8, RestA/binary>>, <<B:8, RestB/binary>>) ->
    <<(A bor B):8, (bin_or(RestA, RestB))/binary>>.

popcount(Bin) -> popcount(Bin, 0).

popcount(<<>>,                 Acc) -> Acc;
popcount(<<Byte:8, Rest/binary>>, Acc) ->
    popcount(Rest, Acc + popcount_byte(Byte)).

popcount_byte(0) -> 0;
popcount_byte(B) -> (B band 1) + popcount_byte(B bsr 1).
