%% @doc CT suite — streaming direct-dial end-to-end: a consumer opens a
%% streaming RPC by DIALING the serving station directly (outside its seed
%% set), rather than routing through an existing pool link. The streaming
%% analogue of macula_station_call_station_SUITE.
%%
%% A provider pool connected to station B advertises a `server_stream'
%% procedure; a consumer pool seeded to NOTHING dials B by URL via
%% `macula:call_stream_station/6' and reads the pushed chunks to `eof'.
%% Proves streaming reaches a provider in one hop, the same way unary
%% `call_station' does.
%%
%% A companion control case proves the same thing over the EXISTING
%% station-routed `call_stream/5' (both endpoints seeded to B) — this
%% isolates whether cross-connection stream relay works at all from
%% whether the NEW direct-dial entry point works.
-module(macula_station_call_stream_station_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([suite/0, all/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([stream_station_routed_control/1,
         stream_dials_outside_seed_set/1]).

-define(REALM, <<0:256>>).
-define(PROC, <<"echo.stream">>).

suite() -> [{timetrap, {minutes, 2}}].
all()   -> [stream_station_routed_control, stream_dials_outside_seed_set].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(macula),
    Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    [{cluster_opts, #{base_dir => ?config(priv_dir, Config)}} | Config].
end_per_testcase(_Name, _Config) -> ok.

%% CONTROL: both provider and consumer seed to B; the consumer uses the
%% existing station-routed `call_stream/5' (not direct-dial). Isolates
%% whether cross-connection streaming works at all on one station.
stream_station_routed_control(Config) ->
    Opts = ?config(cluster_opts, Config),
    [B]  = macula_station_test_cluster:spawn_cluster(1, Opts),
    Url  = station_url(macula_station_test_cluster:listen_addr(B)),
    {ok, Provider} = macula:connect([Url], #{verify => none}),
    {ok, Consumer} = macula:connect([Url], #{verify => none}),
    try
        ok = wait_healthy(Provider),
        ok = wait_healthy(Consumer),
        ok = macula:advertise_stream(Provider, ?REALM, ?PROC, server_stream,
                                     fun push_three_chunks/2),
        timer:sleep(300),
        {ok, Stream} = macula:call_stream(Consumer, ?REALM, ?PROC, #{}, #{}),
        ?assertEqual([<<"chunk-1">>, <<"chunk-2">>, <<"chunk-3">>],
                     recv_all(Stream))
    after
        catch macula:close(Consumer),
        catch macula:close(Provider),
        macula_station_test_cluster:stop_cluster([B])
    end.

%% A pool seeded to NOTHING dials station B by URL directly and opens the
%% stream there, exactly as `call_station_dials_outside_seed_set' does
%% for unary RPC.
stream_dials_outside_seed_set(Config) ->
    Opts = ?config(cluster_opts, Config),
    [B]  = macula_station_test_cluster:spawn_cluster(1, Opts),
    Url  = station_url(macula_station_test_cluster:listen_addr(B)),
    {ok, Provider} = macula:connect([Url], #{verify => none}),
    {ok, Consumer} = macula:connect([], #{verify => none}),
    try
        ok = wait_healthy(Provider),
        ok = macula:advertise_stream(Provider, ?REALM, ?PROC, server_stream,
                                     fun push_three_chunks/2),
        timer:sleep(300),
        {ok, Stream} = macula:call_stream_station(Consumer, Url, ?REALM,
                                                  ?PROC, #{}, #{}),
        ?assertEqual([<<"chunk-1">>, <<"chunk-2">>, <<"chunk-3">>],
                     recv_all(Stream))
    after
        catch macula:close(Consumer),
        catch macula:close(Provider),
        macula_station_test_cluster:stop_cluster([B])
    end.

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

%% The idiomatic server_stream handler shape (mirrors macula's own
%% `server_stream_test_/0'): push chunks, then `close_stream/1' — the
%% signal that produces `eof' on the consumer's `recv'. A handler that
%% only calls `set_reply' does not close the stream; `recv' keeps
%% waiting and only `await_reply' resolves.
push_three_chunks(Stream, _Args) ->
    macula:send(Stream, <<"chunk-1">>),
    macula:send(Stream, <<"chunk-2">>),
    macula:send(Stream, <<"chunk-3">>),
    macula:close_stream(Stream).

recv_all(Stream) -> recv_all(Stream, []).
recv_all(Stream, Acc) ->
    case macula:recv(Stream, 5_000) of
        {chunk, Bin} -> recv_all(Stream, [Bin | Acc]);
        {data, Term} -> recv_all(Stream, [Term | Acc]);
        eof          -> lists:reverse(Acc);
        Other        -> {stuck, Other, lists:reverse(Acc)}
    end.

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
