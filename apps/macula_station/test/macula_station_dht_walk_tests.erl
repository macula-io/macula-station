%% @doc Unit tests for the pure helpers behind the multi-round Kademlia
%% FIND_VALUE walk in `macula_station_dht_handlers' — `next_candidates/3'
%% (dedupe + seen-filter + dead-end-filter + distance-sort + width-cap)
%% and the defensive `node_id_of/1' / `addresses_of/1' extractors that
%% guard against CBOR handing back `{text, Bin}' keys instead of atoms
%% (the same trap `macula_dht_endpoint_propagation_tests' exists for on
%% the producer side).
%%
%% Deliberately does NOT exercise `walk/5' itself or
%% `macula_station_dht_dialer' — that needs a real multi-station wire
%% round-trip, covered live against the fleet rather than here.
-module(macula_station_dht_walk_tests).
-include_lib("eunit/include/eunit.hrl").

-define(KEY, <<0:256>>).

ref(Id, Addrs) ->
    #{node_id => Id, addresses => Addrs}.

%%%===================================================================
%%% node_id_of / addresses_of — CBOR key-shape tolerance
%%%===================================================================

node_id_of_atom_key_test() ->
    ?assertEqual(<<1:256>>,
                 macula_station_dht_handlers:node_id_of(#{node_id => <<1:256>>})).

node_id_of_text_tuple_key_test() ->
    ?assertEqual(<<1:256>>,
                 macula_station_dht_handlers:node_id_of(
                   #{{text, <<"node_id">>} => <<1:256>>})).

node_id_of_binary_key_test() ->
    ?assertEqual(<<1:256>>,
                 macula_station_dht_handlers:node_id_of(
                   #{<<"node_id">> => <<1:256>>})).

addresses_of_atom_key_test() ->
    ?assertEqual([#{host => <<"h">>}],
                 macula_station_dht_handlers:addresses_of(
                   #{addresses => [#{host => <<"h">>}]})).

addresses_of_text_tuple_key_test() ->
    ?assertEqual([#{host => <<"h">>}],
                 macula_station_dht_handlers:addresses_of(
                   #{{text, <<"addresses">>} => [#{host => <<"h">>}]})).

addresses_of_binary_key_test() ->
    ?assertEqual([#{host => <<"h">>}],
                 macula_station_dht_handlers:addresses_of(
                   #{<<"addresses">> => [#{host => <<"h">>}]})).

addresses_of_missing_key_defaults_empty_test() ->
    ?assertEqual([], macula_station_dht_handlers:addresses_of(#{node_id => <<1:256>>})).

%%%===================================================================
%%% next_candidates/3
%%%===================================================================

next_candidates_drops_dead_ends_test() ->
    %% An entry with no address is a peer we cannot dial — an honest
    %% dead end, not a next hop.
    Refs = [ref(<<1:256>>, []), ref(<<2:256>>, [#{host => <<"h2">>}])],
    Got  = macula_station_dht_handlers:next_candidates(?KEY, Refs, sets:new()),
    ?assertEqual([{<<2:256>>, [#{host => <<"h2">>}]}], Got).

next_candidates_drops_already_seen_test() ->
    Refs = [ref(<<1:256>>, [#{host => <<"h1">>}]),
            ref(<<2:256>>, [#{host => <<"h2">>}])],
    Seen = sets:add_element(<<1:256>>, sets:new()),
    Got  = macula_station_dht_handlers:next_candidates(?KEY, Refs, Seen),
    ?assertEqual([{<<2:256>>, [#{host => <<"h2">>}]}], Got).

%% The SAME node_id reachable via two different peers' NODES replies
%% (both closer to it in the mesh than we are) must collapse to ONE
%% candidate, not be queried twice in the same round.
next_candidates_dedupes_by_node_id_test() ->
    Refs = [ref(<<3:256>>, [#{host => <<"first">>}]),
            ref(<<3:256>>, [#{host => <<"second">>}])],
    Got  = macula_station_dht_handlers:next_candidates(?KEY, Refs, sets:new()),
    ?assertEqual(1, length(Got)),
    [{Id, _Addrs}] = Got,
    ?assertEqual(<<3:256>>, Id).

next_candidates_sorts_by_distance_to_key_test() ->
    %% XOR-distance to <<0:256>> is just the id's own integer value —
    %% smaller id, closer. Deliberately supplied out of order.
    Refs = [ref(<<16#F0:256>>, [#{host => <<"far">>}]),
            ref(<<16#01:256>>, [#{host => <<"near">>}]),
            ref(<<16#10:256>>, [#{host => <<"mid">>}])],
    Got  = macula_station_dht_handlers:next_candidates(?KEY, Refs, sets:new()),
    ?assertEqual([<<16#01:256>>, <<16#10:256>>, <<16#F0:256>>],
                 [Id || {Id, _} <- Got]).

next_candidates_caps_at_round_width_test() ->
    %% ?WALK_ROUND_WIDTH is 5 — six reachable, distinct candidates must
    %% still yield only the five closest.
    Refs = [ref(<<N:256>>, [#{host => <<"h">>}]) || N <- lists:seq(1, 6)],
    Got  = macula_station_dht_handlers:next_candidates(?KEY, Refs, sets:new()),
    ?assertEqual(5, length(Got)),
    ?assertEqual([<<1:256>>, <<2:256>>, <<3:256>>, <<4:256>>, <<5:256>>],
                 [Id || {Id, _} <- Got]).

next_candidates_empty_refs_yields_empty_test() ->
    ?assertEqual([], macula_station_dht_handlers:next_candidates(?KEY, [], sets:new())).
