%% EUnit tests for the in-server ETS record store exposed via
%% hecate_dht:put_record/2, find_local_record/2, record_count/1.
-module(hecate_dht_record_store_tests).

-include_lib("eunit/include/eunit.hrl").

start() ->
    Kp = hecate_identity:generate(),
    {ok, D} = hecate_dht:start_link(#{self_id  => hecate_identity:public(Kp),
                                      identity => Kp}),
    {D, Kp}.

%%---------------------------------------------------------------------
%% put_record / find_local_record roundtrip
%%---------------------------------------------------------------------

put_node_record_indexed_by_node_id_test() ->
    {D, _} = start(),
    OwnerKp = hecate_identity:generate(),
    NodeId = hecate_identity:public(OwnerKp),
    R = hecate_record:sign(
          hecate_record:node_record(NodeId, [], 0), OwnerKp),
    ok = hecate_dht:put_record(D, R),
    ?assertEqual([R], hecate_dht:find_local_record(D, NodeId)),
    ?assertEqual(1, hecate_dht:record_count(D)),
    hecate_dht:stop(D).

put_procedure_advertisement_indexed_by_uri_hash_test() ->
    {D, _} = start(),
    AdvKp = hecate_identity:generate(),
    Advertiser = hecate_identity:public(AdvKp),
    Uri = <<"mcp://weather/forecast">>,
    R = hecate_record:sign(
          hecate_record:procedure_advertisement(
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
    RealmKp = hecate_identity:generate(),
    RealmId = hecate_identity:public(RealmKp),
    R = hecate_record:sign(
          hecate_record:realm_stations(RealmId, []), RealmKp),
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
    KpA = hecate_identity:generate(),
    KpB = hecate_identity:generate(),
    Ra = hecate_record:sign(
           hecate_record:procedure_advertisement(
               hecate_identity:public(KpA), Uri, StationA), KpA),
    Rb = hecate_record:sign(
           hecate_record:procedure_advertisement(
               hecate_identity:public(KpB), Uri, StationB), KpB),
    ok = hecate_dht:put_record(D, Ra),
    ok = hecate_dht:put_record(D, Rb),
    Stored = hecate_dht:find_local_record(D, crypto:hash(sha256, Uri)),
    ?assertEqual(2, length(Stored)),
    Owners = lists:sort([hecate_record:key(R) || R <- Stored]),
    ?assertEqual(lists:sort([hecate_identity:public(KpA),
                             hecate_identity:public(KpB)]),
                 Owners),
    hecate_dht:stop(D).

%%---------------------------------------------------------------------
%% put_record replaces same-owner record (no version accumulation)
%%---------------------------------------------------------------------

put_record_twice_same_owner_replaces_test() ->
    {D, _} = start(),
    OwnerKp = hecate_identity:generate(),
    NodeId = hecate_identity:public(OwnerKp),
    R1 = hecate_record:sign(
           hecate_record:node_record(NodeId, [], 0), OwnerKp),
    ok = hecate_dht:put_record(D, R1),
    timer:sleep(2),
    R2 = hecate_record:sign(
           hecate_record:node_record(NodeId, [], 1), OwnerKp),
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
