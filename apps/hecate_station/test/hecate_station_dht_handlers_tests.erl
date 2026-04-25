%% @doc Round-trip tests for the per-identity DHT procedure
%% handlers. Spins up a real `hecate_dht' + `hecate_handler_registry'
%% pair, exercises put/find/find-by-type against the advertised
%% closures the way the CALL frame dispatcher would.
-module(hecate_station_dht_handlers_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TYPE_NODE, 16#01).

%%%===================================================================
%%% Fixture
%%%===================================================================

setup() ->
    Kp = macula_identity:generate(),
    {ok, Dht} = hecate_dht:start_link(
                  #{self_id => macula_identity:public(Kp)}),
    {ok, Reg} = hecate_handler_registry:start_link(),
    {ok, _Hd} = hecate_station_dht_handlers:start_link(
                  #{handler_registry => Reg, dht => Dht}),
    #{kp => Kp, dht => Dht, registry => Reg}.

cleanup(#{dht := Dht, registry := Reg}) ->
    catch hecate_handler_registry:stop(Reg),
    catch hecate_dht:stop(Dht),
    ok.

%%%===================================================================
%%% Generator
%%%===================================================================

handlers_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [
        fun put_record_advertised/1,
        fun put_record_round_trip/1,
        fun put_record_rejects_bad_signature/1,
        fun find_record_returns_not_found_for_unknown_key/1,
        fun find_records_by_type_filters_to_type/1,
        fun find_records_by_type_returns_empty_when_none/1
     ]}.

%%%===================================================================
%%% Tests
%%%===================================================================

put_record_advertised(#{registry := Reg}) ->
    ?_test(begin
        Procs = lists:sort(hecate_handler_registry:list(Reg)),
        ?assertEqual(lists:sort([<<"_dht.put_record">>,
                                 <<"_dht.find_record">>,
                                 <<"_dht.find_records_by_type">>]),
                     Procs)
    end).

put_record_round_trip(#{kp := Kp, registry := Reg}) ->
    ?_test(begin
        Pub    = macula_identity:public(Kp),
        Record = signed_node_record(Pub, Kp),
        {ok, PutHandler}  = hecate_handler_registry:lookup(
                              Reg, <<"_dht.put_record">>),
        ?assertEqual({ok, ok}, PutHandler(Record)),

        {ok, FindHandler} = hecate_handler_registry:lookup(
                              Reg, <<"_dht.find_record">>),
        Key = macula_record:storage_key(Record),
        ?assertEqual({ok, Record},
                     FindHandler(#{key => Key}))
    end).

put_record_rejects_bad_signature(#{kp := Kp, registry := Reg}) ->
    ?_test(begin
        Pub    = macula_identity:public(Kp),
        Record = signed_node_record(Pub, Kp),
        Tampered = Record#{signature := <<0:512>>},
        {ok, PutHandler} = hecate_handler_registry:lookup(
                             Reg, <<"_dht.put_record">>),
        ?assertEqual({error, bad_signature}, PutHandler(Tampered))
    end).

find_record_returns_not_found_for_unknown_key(#{registry := Reg}) ->
    ?_test(begin
        {ok, FindHandler} = hecate_handler_registry:lookup(
                              Reg, <<"_dht.find_record">>),
        ?assertEqual({ok, not_found},
                     FindHandler(#{key => <<0:256>>}))
    end).

find_records_by_type_filters_to_type(#{kp := Kp, registry := Reg}) ->
    ?_test(begin
        Pub = macula_identity:public(Kp),
        R   = signed_node_record(Pub, Kp),

        {ok, PutHandler} = hecate_handler_registry:lookup(
                             Reg, <<"_dht.put_record">>),
        {ok, ok} = PutHandler(R),

        {ok, ListHandler} = hecate_handler_registry:lookup(
                              Reg, <<"_dht.find_records_by_type">>),

        {ok, NodeRecs} = ListHandler(#{type => ?TYPE_NODE}),
        ?assertEqual([R], NodeRecs),

        %% A type the DHT does not contain returns an empty list,
        %% not an error.
        {ok, OtherTypeRecs} = ListHandler(#{type => 16#FF}),
        ?assertEqual([], OtherTypeRecs)
    end).

find_records_by_type_returns_empty_when_none(#{registry := Reg}) ->
    ?_test(begin
        {ok, ListHandler} = hecate_handler_registry:lookup(
                              Reg, <<"_dht.find_records_by_type">>),
        ?assertEqual({ok, []},
                     ListHandler(#{type => ?TYPE_NODE}))
    end).

%%%===================================================================
%%% Helpers
%%%===================================================================

signed_node_record(Pub, Kp) ->
    Unsigned = macula_record:node_record(Pub, _Realms = [], _Caps = 0,
                                          #{display_name => <<"test">>}),
    macula_record:sign(Unsigned, Kp).
