%% EUnit tests for the in-server ETS record store exposed via
%% hecate_dht:put_record/2, find_local_record/2, record_count/1.
-module(hecate_dht_record_store_tests).

-include_lib("eunit/include/eunit.hrl").

start() ->
    Kp = macula_identity:generate(),
    {ok, D} = hecate_dht:start_link(#{self_id  => macula_identity:public(Kp),
                                      identity => Kp}),
    {D, Kp}.

%%---------------------------------------------------------------------
%% put_record / find_local_record roundtrip
%%---------------------------------------------------------------------

put_node_record_indexed_by_node_id_test() ->
    {D, _} = start(),
    OwnerKp = macula_identity:generate(),
    NodeId = macula_identity:public(OwnerKp),
    R = macula_record:sign(
          macula_record:node_record(NodeId, [], 0), OwnerKp),
    ok = hecate_dht:put_record(D, R),
    ?assertEqual([R], hecate_dht:find_local_record(D, NodeId)),
    ?assertEqual(1, hecate_dht:record_count(D)),
    hecate_dht:stop(D).

put_procedure_advertisement_indexed_by_uri_hash_test() ->
    {D, _} = start(),
    AdvKp = macula_identity:generate(),
    Advertiser = macula_identity:public(AdvKp),
    Uri = <<"mcp://weather/forecast">>,
    R = macula_record:sign(
          macula_record:procedure_advertisement(
              Advertiser, Uri, crypto:strong_rand_bytes(32)),
          AdvKp),
    ok = hecate_dht:put_record(D, R),
    UriHash = crypto:hash(sha256, Uri),
    ?assertEqual([R], hecate_dht:find_local_record(D, UriHash)),
    %% Lookup by advertiser's NodeId must NOT return the record —
    %% it's stored under SHA-256(uri), not the envelope key.
    ?assertEqual([], hecate_dht:find_local_record(D, Advertiser)),
    hecate_dht:stop(D).

put_realm_stations_indexed_by_hashed_station_set_test() ->
    {D, _} = start(),
    RealmKp = macula_identity:generate(),
    RealmId = macula_identity:public(RealmKp),
    R = macula_record:sign(
          macula_record:realm_stations(RealmId, []), RealmKp),
    ok = hecate_dht:put_record(D, R),
    HashedKey = crypto:hash(sha256, <<"station_set", RealmId/binary>>),
    ?assertEqual([R], hecate_dht:find_local_record(D, HashedKey)),
    hecate_dht:stop(D).

%%---------------------------------------------------------------------
%% Multi-owner: two advertisers of the same procedure both stored
%%---------------------------------------------------------------------

two_advertisers_same_procedure_both_stored_test() ->
    {D, _} = start(),
    Uri = <<"mcp://news/headlines">>,
    StationA = crypto:strong_rand_bytes(32),
    StationB = crypto:strong_rand_bytes(32),
    KpA = macula_identity:generate(),
    KpB = macula_identity:generate(),
    Ra = macula_record:sign(
           macula_record:procedure_advertisement(
               macula_identity:public(KpA), Uri, StationA), KpA),
    Rb = macula_record:sign(
           macula_record:procedure_advertisement(
               macula_identity:public(KpB), Uri, StationB), KpB),
    ok = hecate_dht:put_record(D, Ra),
    ok = hecate_dht:put_record(D, Rb),
    Stored = hecate_dht:find_local_record(D, crypto:hash(sha256, Uri)),
    ?assertEqual(2, length(Stored)),
    Owners = lists:sort([macula_record:key(R) || R <- Stored]),
    ?assertEqual(lists:sort([macula_identity:public(KpA),
                             macula_identity:public(KpB)]),
                 Owners),
    hecate_dht:stop(D).

%%---------------------------------------------------------------------
%% put_record replaces same-owner record (no version accumulation)
%%---------------------------------------------------------------------

put_record_twice_same_owner_replaces_test() ->
    {D, _} = start(),
    OwnerKp = macula_identity:generate(),
    NodeId = macula_identity:public(OwnerKp),
    R1 = macula_record:sign(
           macula_record:node_record(NodeId, [], 0), OwnerKp),
    ok = hecate_dht:put_record(D, R1),
    timer:sleep(2),
    R2 = macula_record:sign(
           macula_record:node_record(NodeId, [], 1), OwnerKp),
    ok = hecate_dht:put_record(D, R2),
    ?assertEqual([R2], hecate_dht:find_local_record(D, NodeId)),
    ?assertEqual(1, hecate_dht:record_count(D)),
    hecate_dht:stop(D).

%%---------------------------------------------------------------------
%% Missing keys
%%---------------------------------------------------------------------

find_local_record_missing_returns_empty_list_test() ->
    {D, _} = start(),
    ?assertEqual([], hecate_dht:find_local_record(D, <<0:256>>)),
    hecate_dht:stop(D).

fresh_server_record_count_is_zero_test() ->
    {D, _} = start(),
    ?assertEqual(0, hecate_dht:record_count(D)),
    hecate_dht:stop(D).

%%---------------------------------------------------------------------
%% on_record_stored callback fires after every store_put
%%---------------------------------------------------------------------

on_record_stored_callback_fires_for_put_record_test() ->
    {D, _} = start(),
    OwnerKp  = macula_identity:generate(),
    NodeId   = macula_identity:public(OwnerKp),
    Test     = self(),
    Ref      = make_ref(),
    ok = hecate_dht:set_on_record_stored(
           D, fun(R) -> Test ! {Ref, R} end),
    Record = macula_record:sign(
               macula_record:node_record(NodeId, [], 0), OwnerKp),
    ok = hecate_dht:put_record(D, Record),
    receive
        {Ref, Got} -> ?assertEqual(Record, Got)
    after 1_000 ->
        erlang:error(callback_did_not_fire)
    end,
    hecate_dht:stop(D).

on_record_stored_callback_can_be_replaced_test() ->
    {D, _} = start(),
    OwnerKp = macula_identity:generate(),
    Test    = self(),
    R1Ref   = make_ref(),
    R2Ref   = make_ref(),
    ok = hecate_dht:set_on_record_stored(
           D, fun(_) -> Test ! {R1Ref, fired} end),
    ok = hecate_dht:set_on_record_stored(
           D, fun(_) -> Test ! {R2Ref, fired} end),
    Record = macula_record:sign(
               macula_record:node_record(
                 macula_identity:public(OwnerKp), [], 0), OwnerKp),
    ok = hecate_dht:put_record(D, Record),
    receive
        {R1Ref, fired} -> erlang:error(stale_callback_fired);
        {R2Ref, fired} -> ok
    after 1_000 ->
        erlang:error(replacement_callback_did_not_fire)
    end,
    hecate_dht:stop(D).

on_record_stored_callback_can_be_cleared_test() ->
    {D, _} = start(),
    OwnerKp = macula_identity:generate(),
    Test    = self(),
    Ref     = make_ref(),
    ok = hecate_dht:set_on_record_stored(
           D, fun(_) -> Test ! {Ref, fired} end),
    ok = hecate_dht:set_on_record_stored(D, undefined),
    Record = macula_record:sign(
               macula_record:node_record(
                 macula_identity:public(OwnerKp), [], 0), OwnerKp),
    ok = hecate_dht:put_record(D, Record),
    receive
        {Ref, fired} -> erlang:error(cleared_callback_fired)
    after 200 -> ok
    end,
    hecate_dht:stop(D).

on_record_stored_callback_crash_does_not_kill_dht_test() ->
    {D, _} = start(),
    OwnerKp = macula_identity:generate(),
    ok = hecate_dht:set_on_record_stored(
           D, fun(_) -> erlang:error(boom) end),
    Record = macula_record:sign(
               macula_record:node_record(
                 macula_identity:public(OwnerKp), [], 0), OwnerKp),
    ok = hecate_dht:put_record(D, Record),
    %% DHT is still alive after callback crash.
    ?assertEqual(1, hecate_dht:record_count(D)),
    hecate_dht:stop(D).
