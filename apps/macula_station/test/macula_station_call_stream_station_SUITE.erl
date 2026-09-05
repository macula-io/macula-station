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
         stream_dials_outside_seed_set/1,
         stream_relays_through_outbound_dialled_hop/1,
         client_stream_half_close_still_gets_its_reply/1]).

-define(REALM, <<0:256>>).
-define(PROC, <<"echo.stream">>).
-define(SUM_PROC, <<"sum.stream">>).

suite() -> [{timetrap, {minutes, 2}}].
all()   -> [stream_station_routed_control, stream_dials_outside_seed_set,
            stream_relays_through_outbound_dialled_hop,
            client_stream_half_close_still_gets_its_reply].

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

%% Chain A - B - C (A/C not peered), C DIALS B (the direction that
%% matters — see below). Provider connects to C, consumer to A. The
%% consumer's `call_stream/5' resolves the procedure via A's gossip-
%% propagated remote_advertise entry (pointing at B), so A relays
%% STREAM_OPEN onto B; B's own entry (gossip-learned from C) points at
%% its connection to C, so B relays again onto C, which finally
%% dispatches to the real provider.
%%
%% `dial_outbound(C, B)' — not `dial_outbound(B, C)' — is the one
%% detail that makes this test exercise the bug it exists for: it
%% makes C the DIALER of its connection to B, so C's local process for
%% that connection is `macula_station_outbound_link', not
%% `macula_station_peer_observer' directly. B, relaying, opens a FRESH
%% dedicated stream on ITS handle to that same connection to forward
%% STREAM_OPEN onward — which C receives as an unprompted inbound
%% stream on the outbound_link-owned side. Before the fix,
%% outbound_link had no `new_dedicated_stream' handling at all: the
%% stream's ownership transfer happened (via macula_peering_conn) but
%% the notification silently hit outbound_link's catch-all, and every
%% subsequent byte on that stream failed outbound_link's
%% `is_map_key(Stream, ContentBufs)' guard too (content buffers only
%% track streams outbound_link itself opened) — also dropped. The
%% consumer would then time out on `recv/2' with nothing to show for
%% it. `dial_outbound(A, B)' direction does not matter for reproducing
%% this — A only ever SENDS on its handle to B, which needs no special
%% receive-side handling on either end.
stream_relays_through_outbound_dialled_hop(Config) ->
    Opts = ?config(cluster_opts, Config),
    [A, B, C] = macula_station_test_cluster:spawn_cluster(3, Opts),
    try
        {ok, _LinkAB} = macula_station_test_cluster:dial_outbound(A, B),
        {ok, _LinkCB} = macula_station_test_cluster:dial_outbound(C, B),
        ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 10_000),
        ok = macula_station_test_cluster:wait_for_handshakes(C, 1, 10_000),
        UrlA = station_url(macula_station_test_cluster:listen_addr(A)),
        UrlC = station_url(macula_station_test_cluster:listen_addr(C)),
        {ok, Provider} = macula:connect([UrlC], #{verify => none}),
        {ok, Consumer} = macula:connect([UrlA], #{verify => none}),
        try
            ok = wait_healthy(Provider),
            ok = wait_healthy(Consumer),
            ok = macula:advertise_stream(Provider, ?REALM, ?PROC, server_stream,
                                         fun push_three_chunks/2),
            %% Advertise gossip needs to reach A via B before the
            %% consumer's call can resolve a route at all.
            timer:sleep(3_000),
            {ok, Stream} = macula:call_stream(Consumer, ?REALM, ?PROC, #{}, #{}),
            ?assertEqual([<<"chunk-1">>, <<"chunk-2">>, <<"chunk-3">>],
                         recv_all(Stream))
        after
            catch macula:close(Consumer),
            catch macula:close(Provider)
        end
    after
        macula_station_test_cluster:stop_cluster([A, B, C])
    end.

%% Regression test for the mode-aware stream-route half-close fix
%% (macula_station_peer_observer.erl's maybe_close_stream_route/5).
%% Before that fix, ANY STREAM_END(role=send) -- from either side --
%% dropped the WHOLE bidirectional route immediately. That is correct
%% for `server_stream' (only the provider's own half-close ends the
%% exchange -- see the three cases above, all of which end via
%% `close_stream/1', a full role=both close, so none of them actually
%% exercised the role=send path the fleet's real server_stream
%% providers use, `macula_streamer:close/1'). It is WRONG for
%% `client_stream': a caller's own half-close (issued here via
%% `close_send/1') dropped the route before the provider's `set_reply/2'
%% could ever be relayed back -- the provider's call raised nothing
%% locally, but `await_reply/2' on the consumer side would time out.
%% This is client_stream/bidi's analogue of the `server_stream' cases
%% above: same one-station cross-connection relay, opposite data
%% direction, and a role=send half-close that must NOT be terminal by
%% itself in this mode.
client_stream_half_close_still_gets_its_reply(Config) ->
    Opts = ?config(cluster_opts, Config),
    [B]  = macula_station_test_cluster:spawn_cluster(1, Opts),
    Url  = station_url(macula_station_test_cluster:listen_addr(B)),
    {ok, Provider} = macula:connect([Url], #{verify => none}),
    {ok, Consumer} = macula:connect([Url], #{verify => none}),
    try
        ok = wait_healthy(Provider),
        ok = wait_healthy(Consumer),
        ok = macula:advertise_stream(Provider, ?REALM, ?SUM_PROC,
                                     client_stream, fun sum_then_reply/2),
        timer:sleep(300),
        {ok, Stream} = macula:call_stream(Consumer, ?REALM, ?SUM_PROC, #{},
                                          #{mode => client_stream}),
        macula:send(Stream, <<"1">>),
        macula:send(Stream, <<"2">>),
        macula:send(Stream, <<"3">>),
        ok = macula:close_send(Stream),
        ?assertEqual({ok, 6}, macula:await_reply(Stream, 5_000))
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

%% client_stream provider shape: recv chunks to eof, then set_reply
%% with their sum. Deliberately does NOT call close_stream/1 -- the
%% caller's own half-close (`close_send/1', role=send) is what should
%% end the recv side here, and `set_reply/2' is the terminal frame
%% that should still make it back despite that half-close, per the fix
%% this test exists for.
sum_then_reply(Stream, _Args) ->
    Total = recv_and_sum(Stream, 0),
    macula:set_reply(Stream, Total).

recv_and_sum(Stream, Acc) ->
    sum_step(macula:recv(Stream, 5_000), Stream, Acc).

sum_step({chunk, Bin}, Stream, Acc) ->
    recv_and_sum(Stream, Acc + binary_to_integer(Bin));
sum_step(eof, _Stream, Acc) ->
    Acc.

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
