%% @doc CT suite — the Slice 2 end-to-end: a procedure_advertisement
%% written on one station is resolvable from another over real QUIC,
%% and decodes back to the right provider.
%%
%% This is the DONE-WHEN for direct-dial discovery Slice 2 that unit
%% tests could not cover: a record put on the PROVIDER station is
%% fetched by the CONSUMER station via the DHT wire, then read with
%% `macula_record:read_procedure_advertisement/1'. Because the record
%% makes a real CBOR round-trip, this is also the only test that proves
%% the reader handles the wire-decoded payload shape (not just the
%% locally-built one).
%%
%% Direction matches macula_station_dht_transport_SUITE: the dialer
%% (A) queries the dialee (B) over the A->B connection. So A is the
%% consumer, B is the provider.
-module(macula_station_procedure_advertisement_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([suite/0, all/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([advertisement_resolves_cross_station/1,
         two_providers_both_resolve_cross_station/1]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [advertisement_resolves_cross_station,
     two_providers_both_resolve_cross_station].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    PrivDir = ?config(priv_dir, Config),
    [{cluster_opts, #{base_dir => PrivDir}} | Config].
end_per_testcase(_Name, _Config) -> ok.

%%==================================================================
%% Tests
%%==================================================================

%% One provider (B) advertises a procedure; the consumer (A) resolves
%% it over the wire and the record decodes to B's serving station.
advertisement_resolves_cross_station(Config) ->
    with_dialed_pair(Config, fun(A, B) ->
        Kp   = macula_identity:generate(),
        Adv  = macula_identity:public(Kp),
        BSt  = macula_station_test_cluster:pubkey(B),
        Uri  = <<"realm42/org/app/checkout_v1">>,
        R    = macula_record:sign(
                 macula_record:procedure_advertisement(Adv, Uri, BSt), Kp),

        ok  = put_on(B, R),
        Key = macula_record:procedure_key(Uri),

        {value, [Got]} = find_on(A, Key, B),
        ?assertEqual(#{procedure_uri   => Uri,
                       advertiser_node => Adv,
                       serving_station => BSt},
                     macula_record:read_procedure_advertisement(Got))
    end).

%% Two providers advertise the SAME procedure on B; both records live
%% under one key (signer-deduped) and the consumer gets both back.
two_providers_both_resolve_cross_station(Config) ->
    with_dialed_pair(Config, fun(A, B) ->
        BSt = macula_station_test_cluster:pubkey(B),
        Uri = <<"realm42/org/app/checkout_v1">>,
        Key = macula_record:procedure_key(Uri),
        KpX = macula_identity:generate(),
        KpY = macula_identity:generate(),
        RX  = advert(KpX, Uri, BSt),
        RY  = advert(KpY, Uri, BSt),

        ok = put_on(B, RX),
        ok = put_on(B, RY),

        {value, Records} = find_on(A, Key, B),
        Advs = [maps:get(advertiser_node,
                         macula_record:read_procedure_advertisement(Rec))
                || Rec <- Records],
        ?assertEqual(lists:sort([macula_identity:public(KpX),
                                 macula_identity:public(KpY)]),
                     lists:sort(Advs))
    end).

%%==================================================================
%% Helpers
%%==================================================================

with_dialed_pair(Config, Fun) ->
    Opts    = ?config(cluster_opts, Config),
    Handles = macula_station_test_cluster:spawn_cluster(2, Opts),
    try
        [A, B] = Handles,
        ok = macula_station_test_cluster:dial(A, B),
        ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 10_000),
        Fun(A, B)
    after
        macula_station_test_cluster:stop_cluster(Handles)
    end.

advert(Kp, Uri, Station) ->
    macula_record:sign(
      macula_record:procedure_advertisement(
        macula_identity:public(Kp), Uri, Station), Kp).

put_on(Station, Record) ->
    macula_station_test_cluster:rpc(
      Station, macula_dht, put_record, [macula_dht, Record]).

%% Consumer queries the provider directly for the key over the wire.
find_on(Consumer, Key, Provider) ->
    ProviderPubkey = macula_station_test_cluster:pubkey(Provider),
    macula_station_test_cluster:rpc(
      Consumer, macula_dht, find_value,
      [macula_dht, Key, ProviderPubkey, 5_000]).
