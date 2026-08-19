%% @doc CT suite — Slice 7c end-to-end: resolve the realm -> org -> server
%% delegation chain over the DHT and verify it, on records fetched via the
%% SDK (so payloads arrive wire-decoded, the shape that broke Slice 2's
%% readers). A legit advertisement's chain verifies; a squatter with no
%% delegation is rejected because its delegation lookup misses.
-module(macula_station_delegation_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([suite/0, all/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([chain_resolves_and_verifies/1, squatter_has_no_delegation/1]).

-define(ORG, <<"acme">>).

suite() -> [{timetrap, {minutes, 5}}].
all()   -> [chain_resolves_and_verifies, squatter_has_no_delegation].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(macula),
    Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    [{cluster_opts, #{base_dir => ?config(priv_dir, Config)}} | Config].
end_per_testcase(_Name, _Config) -> ok.

chain_resolves_and_verifies(Config) ->
    with_station(Config, fun(Pool, BPub) ->
        {RealmId, RealmKp} = kp(),
        {OrgKey, OrgKp}    = kp(),
        {Adv, AdvKp}       = kp(),
        Uri = <<"realm/acme/checkout_v1">>,

        publish(Pool, macula_record:sign(
                  macula_record:org_directory(RealmId, ?ORG, OrgKey), RealmKp)),
        publish(Pool, macula_record:sign(
                  macula_record:procedure_delegation(OrgKey, Adv), OrgKp)),
        publish(Pool, macula_record:sign(
                  macula_record:procedure_advertisement(Adv, Uri, BPub), AdvKp)),
        timer:sleep(200),

        %% resolve the advertisement -> advertiser
        {ok, [AdRec]} = macula:find_records(
                          Pool, macula_record:procedure_key(Uri)),
        #{advertiser_node := A} = macula_record:read_procedure_advertisement(AdRec),
        ?assertEqual(Adv, A),

        %% resolve the chain records and verify (on wire-decoded records)
        {ok, OrgDir} = macula:find_record(
                         Pool, macula_record:org_directory_key(RealmId, ?ORG)),
        {ok, Del}    = macula:find_record(
                         Pool, macula_record:procedure_delegation_key(OrgKey, A)),
        ?assertEqual(ok,
                     macula_record:verify_delegation_chain(RealmId, OrgDir, Del, A))
    end).

squatter_has_no_delegation(Config) ->
    with_station(Config, fun(Pool, BPub) ->
        {OrgKey, _OrgKp}  = kp(),
        {Squat, SquatKp}  = kp(),
        Uri = <<"realm/acme/checkout_v1">>,

        %% squatter advertises but has no procedure_delegation from the org
        publish(Pool, macula_record:sign(
                  macula_record:procedure_advertisement(Squat, Uri, BPub), SquatKp)),
        timer:sleep(200),

        {ok, [AdRec]} = macula:find_records(
                          Pool, macula_record:procedure_key(Uri)),
        #{advertiser_node := A} = macula_record:read_procedure_advertisement(AdRec),
        ?assertEqual(Squat, A),

        %% no delegation exists for the squatter -> lookup misses -> dropped
        ?assertEqual({error, not_found},
                     macula:find_record(
                       Pool, macula_record:procedure_delegation_key(OrgKey, A)))
    end).

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

with_station(Config, Fun) ->
    Opts       = ?config(cluster_opts, Config),
    [B]        = macula_station_test_cluster:spawn_cluster(1, Opts),
    BPub       = macula_station_test_cluster:pubkey(B),
    Url        = station_url(macula_station_test_cluster:listen_addr(B)),
    {ok, Pool} = macula:connect([Url], #{verify => none}),
    try
        ok = wait_healthy(Pool),
        Fun(Pool, BPub)
    after
        catch macula:close(Pool),
        macula_station_test_cluster:stop_cluster([B])
    end.

publish(Pool, Record) ->
    ok = macula:put_record(Pool, Record).

kp() ->
    Kp = macula_identity:generate(),
    {macula_identity:public(Kp), Kp}.

wait_healthy(Pool) -> wait_healthy(Pool, 100).
wait_healthy(_Pool, 0) -> {error, no_healthy_link};
wait_healthy(Pool, N) ->
    healthy_or_retry(macula:status(Pool), Pool, N).

healthy_or_retry({ok, #{healthy_links := H}}, _Pool, _N) when H >= 1 -> ok;
healthy_or_retry(_Other, Pool, N) ->
    timer:sleep(100),
    wait_healthy(Pool, N - 1).

station_url({Ip, Port}) ->
    Host = list_to_binary(inet:ntoa(Ip)),
    <<"quic://[", Host/binary, "]:", (integer_to_binary(Port))/binary>>.
