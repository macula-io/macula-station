%% @doc CT suite — Slice 7b end-to-end: provider-side UCAN authorization.
%% A provider advertises a GATED procedure (`{ucan_required, Issuer}');
%% a consumer without a token is refused with BOLT#4 `unauthorized', a
%% consumer presenting a valid token is served. An open procedure serves
%% anyone. Real station over QUIC; provider + consumer are SDK pools.
-module(macula_station_gated_call_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([suite/0, all/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([gated_requires_ucan/1, open_serves_anyone/1]).

-define(REALM, <<7:256>>).

suite() -> [{timetrap, {minutes, 5}}].
all()   -> [gated_requires_ucan, open_serves_anyone].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(macula),
    Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    [{cluster_opts, #{base_dir => ?config(priv_dir, Config)}} | Config].
end_per_testcase(_Name, _Config) -> ok.

gated_requires_ucan(Config) ->
    with_station_and_pools(Config, fun(Url, Provider, Consumer) ->
        {ok, {IPub, IPriv}} = macula_crypto_nif:generate_keypair(),
        Proc = <<"test.echo">>,
        ok = advertise_when_ready(Provider, Proc,
                                  #{auth => {ucan_required, IPub}}),

        %% no token -> refused
        ?assertMatch({error, _},
                     macula:call_station(Consumer, Url, ?REALM, Proc,
                                         <<"hi">>, 5_000)),

        %% valid token -> served (echo returns the payload)
        Token = mint(IPriv),
        ?assertMatch({ok, _},
                     macula:call_station(Consumer, Url, ?REALM, Proc,
                                         <<"hi">>, 5_000,
                                         #{ucan_token => Token}))
    end).

open_serves_anyone(Config) ->
    with_station_and_pools(Config, fun(Url, Provider, Consumer) ->
        Proc = <<"test.open">>,
        ok = advertise_when_ready(Provider, Proc, #{}),
        ?assertMatch({ok, _},
                     macula:call_station(Consumer, Url, ?REALM, Proc,
                                         <<"hi">>, 5_000))
    end).

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

with_station_and_pools(Config, Fun) ->
    Opts       = ?config(cluster_opts, Config),
    [B]        = macula_station_test_cluster:spawn_cluster(1, Opts),
    Url        = station_url(macula_station_test_cluster:listen_addr(B)),
    {ok, Prov} = macula:connect([Url], #{verify => none}),
    {ok, Cons} = macula:connect([Url], #{verify => none}),
    try
        ok = wait_healthy(Prov),
        ok = wait_healthy(Cons),
        Fun(Url, Prov, Cons)
    after
        catch macula:close(Prov),
        catch macula:close(Cons),
        macula_station_test_cluster:stop_cluster([B])
    end.

%% Advertise once the provider's link to the station is up, then give the
%% wire ADVERTISE a moment to register on the station.
advertise_when_ready(Pool, Proc, AuthOpts) ->
    Handler = fun(Payload) -> {ok, Payload} end,
    ok = macula:advertise(Pool, ?REALM, Proc, Handler, AuthOpts),
    timer:sleep(500),
    ok.

wait_healthy(Pool) -> wait_healthy(Pool, 100).
wait_healthy(_Pool, 0) -> {error, no_healthy_link};
wait_healthy(Pool, N) ->
    healthy_or_retry(macula:status(Pool), Pool, N).

healthy_or_retry({ok, #{healthy_links := H}}, _Pool, _N) when H >= 1 -> ok;
healthy_or_retry(_Other, Pool, N) ->
    timer:sleep(100),
    wait_healthy(Pool, N - 1).

mint(IPriv) ->
    {ok, Token} = macula_ucan_nif:create(
                    <<"did:macula:consumer">>, <<"did:macula:provider">>,
                    [#{<<"with">> => <<"proc:test.echo">>,
                       <<"can">> => <<"call">>}],
                    IPriv),
    Token.

station_url({Ip, Port}) ->
    Host = list_to_binary(inet:ntoa(Ip)),
    <<"quic://[", Host/binary, "]:", (integer_to_binary(Port))/binary>>.
