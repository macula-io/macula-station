%% EUnit tests for macula_dht_xor.
-module(macula_dht_xor_tests).

-include_lib("eunit/include/eunit.hrl").

-define(ZERO, <<0:256>>).
-define(ONE,  <<1:256>>).
-define(MAX,  <<-1:256>>). %% all-ones

%%---------------------------------------------------------------------
%% distance / distance_int
%%---------------------------------------------------------------------

distance_self_is_zero_test() ->
    Id = id("aa"),
    ?assertEqual(?ZERO, macula_dht_xor:distance(Id, Id)),
    ?assertEqual(0,      macula_dht_xor:distance_int(Id, Id)).

distance_is_symmetric_test() ->
    A = id("aa"), B = id("bb"),
    ?assertEqual(macula_dht_xor:distance(A, B),
                 macula_dht_xor:distance(B, A)).

distance_int_is_xor_test() ->
    ?assertEqual(1, macula_dht_xor:distance_int(?ZERO, ?ONE)).

distance_to_max_is_max_test() ->
    Int = macula_dht_xor:distance_int(?ZERO, ?MAX),
    ?assertEqual((1 bsl 256) - 1, Int).

%%---------------------------------------------------------------------
%% common_prefix_bits
%%---------------------------------------------------------------------

common_prefix_all_match_test() ->
    Id = id("aa"),
    ?assertEqual(256, macula_dht_xor:common_prefix_bits(Id, Id)).

common_prefix_zero_when_msb_differs_test() ->
    A = <<0:1, 0:255>>,
    B = <<1:1, 0:255>>,
    ?assertEqual(0, macula_dht_xor:common_prefix_bits(A, B)).

common_prefix_255_when_only_lsb_differs_test() ->
    A = <<0:255, 0:1>>,
    B = <<0:255, 1:1>>,
    ?assertEqual(255, macula_dht_xor:common_prefix_bits(A, B)).

common_prefix_across_byte_boundary_test() ->
    %% bits 0..9 agree, bit 10 differs
    A = <<0:10, 0:1, 0:245>>,
    B = <<0:10, 1:1, 0:245>>,
    ?assertEqual(10, macula_dht_xor:common_prefix_bits(A, B)).

common_prefix_symmetric_test() ->
    A = id("aa"), B = id("bb"),
    ?assertEqual(macula_dht_xor:common_prefix_bits(A, B),
                 macula_dht_xor:common_prefix_bits(B, A)).

%%---------------------------------------------------------------------
%% bucket_index
%%---------------------------------------------------------------------

bucket_index_self_is_minus_one_test() ->
    Id = id("self"),
    ?assertEqual(-1, macula_dht_xor:bucket_index(Id, Id)).

bucket_index_range_test() ->
    Self = <<0:256>>,
    %% LSB differs → highest-matching prefix → bucket 0
    Lsb  = <<0:255, 1:1>>,
    ?assertEqual(0, macula_dht_xor:bucket_index(Self, Lsb)),
    %% MSB differs → zero prefix → bucket 255
    Msb  = <<1:1, 0:255>>,
    ?assertEqual(255, macula_dht_xor:bucket_index(Self, Msb)).

bucket_index_in_range_for_random_peers_test() ->
    Self = id("self"),
    [begin
         Peer = rand_id(),
         Ix = macula_dht_xor:bucket_index(Self, Peer),
         ?assert(Ix >= 0 andalso Ix =< 255)
     end || _ <- lists:seq(1, 100)],
    ok.

%%---------------------------------------------------------------------
%% closer / sort_by_distance / k_closest
%%---------------------------------------------------------------------

closer_target_beats_farther_test() ->
    T = <<0:256>>,
    Near = <<0:255, 1:1>>,
    Far  = <<1:1, 0:255>>,
    ?assertEqual(a, macula_dht_xor:closer(T, Near, Far)),
    ?assertEqual(b, macula_dht_xor:closer(T, Far, Near)).

closer_equal_when_same_distance_test() ->
    T = <<0:256>>,
    A = <<0:255, 1:1>>,
    ?assertEqual(eq, macula_dht_xor:closer(T, A, A)).

sort_by_distance_orders_ascending_test() ->
    T = <<0:256>>,
    Ids = [<<N:256>> || N <- [7, 1, 42, 3, 0]],
    Sorted = macula_dht_xor:sort_by_distance(T, Ids),
    Expected = [<<N:256>> || N <- [0, 1, 3, 7, 42]],
    ?assertEqual(Expected, Sorted).

sort_empty_test() ->
    ?assertEqual([], macula_dht_xor:sort_by_distance(id("t"), [])).

k_closest_truncates_test() ->
    T = <<0:256>>,
    Ids = [<<N:256>> || N <- [7, 1, 42, 3, 0]],
    Top3 = macula_dht_xor:k_closest(T, Ids, 3),
    ?assertEqual([<<0:256>>, <<1:256>>, <<3:256>>], Top3).

k_closest_zero_returns_empty_test() ->
    ?assertEqual([], macula_dht_xor:k_closest(id("t"), [id("a")], 0)).

k_closest_larger_than_input_returns_all_test() ->
    T = <<0:256>>,
    Ids = [<<2:256>>, <<1:256>>],
    ?assertEqual([<<1:256>>, <<2:256>>],
                 macula_dht_xor:k_closest(T, Ids, 99)).

%%---------------------------------------------------------------------
%% helpers
%%---------------------------------------------------------------------

id(Tag) ->
    crypto:hash(sha256, Tag).

rand_id() ->
    crypto:strong_rand_bytes(32).
