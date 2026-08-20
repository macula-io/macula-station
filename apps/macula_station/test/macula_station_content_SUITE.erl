%% @doc CT suite — content sharing end-to-end over a real station:
%% single-block (regression, unchanged since v4.2.7) and the new
%% chunked-manifest path (macula 8.10+) that closes the "content
%% sharing" gap — blobs bigger than one 256 KiB block.
%%
%% The test cluster harness deliberately does not start `macula_content'
%% (it defaults its store dir to `/var/lib/hecate/content', unwritable
%% in a sandbox) — see `macula_station_test_cluster:boot_station_on_peer/6'.
%% `with_station/2' additionally starts it on the spawned peer with a
%% writable `store_dir' under the CT `priv_dir'.
-module(macula_station_content_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([suite/0, all/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([single_block_put_get_round_trips/1,
         chunked_put_get_round_trips/1,
         chunked_content_reassembles_in_order/1,
         empty_content_round_trips/1,
         chunked_content_gets_announced_and_resolves/1,
         eager_replication_lands_on_peer_station/1,
         iterative_get_reaches_two_hop_peer/1]).

suite() -> [{timetrap, {minutes, 3}}].

all() ->
    [single_block_put_get_round_trips,
     chunked_put_get_round_trips,
     chunked_content_reassembles_in_order,
     empty_content_round_trips,
     chunked_content_gets_announced_and_resolves,
     eager_replication_lands_on_peer_station,
     iterative_get_reaches_two_hop_peer].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(macula),
    Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    [{cluster_opts, #{base_dir => ?config(priv_dir, Config)}} | Config].
end_per_testcase(_Name, _Config) -> ok.

%% Unchanged since v4.2.7: a blob that fits in one block gets the
%% single-block MCID (<<1,16#55,BLAKE3(Bytes)>>) and round-trips via
%% `_content.put_block' / `_content.get_block' alone — no manifest.
single_block_put_get_round_trips(Config) ->
    with_station(Config, fun(Pool) ->
        Bytes = <<"small content, fits in one block">>,
        {ok, MCID} = macula:put_content(Pool, Bytes),
        ?assertMatch(<<1, 16#55, _:32/binary>>, MCID),
        ?assertEqual({ok, Bytes}, macula:get_content(Pool, MCID))
    end).

%% The gap this closes: a blob LARGER than one 256 KiB block. Split
%% client-side into chunks, each block uploaded, a manifest published,
%% then resolved + reassembled + Merkle-verified on get.
chunked_put_get_round_trips(Config) ->
    with_station(Config, fun(Pool) ->
        %% 3.5 blocks at the default 256 KiB chunk size.
        Bytes = crypto:strong_rand_bytes(3 * 262144 + 100000),
        {ok, MCID} = macula:put_content(Pool, Bytes),
        ?assertMatch(<<1, 16#56, _:32/binary>>, MCID),
        ?assertEqual({ok, Bytes}, macula:get_content(Pool, MCID))
    end).

%% Distinct chunk contents in a known order prove reassembly is not
%% accidentally correct via all-identical bytes.
chunked_content_reassembles_in_order(Config) ->
    with_station(Config, fun(Pool) ->
        ChunkSize = 262144,
        C1 = binary:copy(<<"A">>, ChunkSize),
        C2 = binary:copy(<<"B">>, ChunkSize),
        C3 = <<"tail">>,
        Bytes = <<C1/binary, C2/binary, C3/binary>>,
        {ok, MCID} = macula:put_content(Pool, Bytes),
        {ok, Got} = macula:get_content(Pool, MCID),
        ?assertEqual(Bytes, Got),
        ?assertEqual(C1, binary:part(Got, 0, ChunkSize)),
        ?assertEqual(C2, binary:part(Got, ChunkSize, ChunkSize)),
        ?assertEqual(C3, binary:part(Got, 2 * ChunkSize, byte_size(C3)))
    end).

empty_content_round_trips(Config) ->
    with_station(Config, fun(Pool) ->
        {ok, MCID} = macula:put_content(Pool, <<>>),
        ?assertMatch(<<1, 16#55, _:32/binary>>, MCID),
        ?assertEqual({ok, <<>>}, macula:get_content(Pool, MCID))
    end).

%% The station's macula_content_announcer auto-publishes a
%% content_announcement on every manifest_stored event (Slice: content
%% sharing discovery). Proves the storage_key/1 fix (macula 8.10) makes
%% that publish actually land (it was a function_clause crash before),
%% and that a consumer resolves it back via find_content_providers/2.
chunked_content_gets_announced_and_resolves(Config) ->
    with_station(Config, fun(Pool) ->
        Bytes = crypto:strong_rand_bytes(3 * 262144 + 100000),
        {ok, MCID} = macula:put_content(Pool, Bytes),
        {ok, Providers} = wait_providers(Pool, MCID, 50),
        ?assertMatch([#{announcer_node := _, endpoint := _}], Providers),
        %% Single-block content is not announced (no manifest_stored
        %% event) — resolving its MCID returns no providers, not an error.
        {ok, SmallMCID} = macula:put_content(Pool, <<"tiny">>),
        ?assertEqual({ok, []}, macula:find_content_providers(Pool, SmallMCID))
    end).

%% PLAN_PER_STREAM_QUIC_ISOLATION.md Phase 2 — the station-to-station
%% leg of content transfer (`macula_station_content_handlers:
%% safe_replicate/3') moved onto a dedicated QUIC stream, same as the
%% daemon-facing leg above. A daemon connected ONLY to A puts a
%% single-block blob; A's `handle_put_block' eager-replicates to its
%% peer B over the new dedicated-stream primitive. Verified from the
%% outside: a SEPARATE daemon pool connected ONLY to B can
%% `get_content' the same MCID straight from B's own local store — no
%% further network hop needed if replication actually landed.
eager_replication_lands_on_peer_station(Config) ->
    Opts = ?config(cluster_opts, Config),
    [A, B] = macula_station_test_cluster:spawn_cluster(2, Opts),
    try
        ok = boot_content_store(A, ?config(priv_dir, Config)),
        ok = boot_content_store(B, ?config(priv_dir, Config)),
        %% `dial_outbound/2', not `dial/2': the eager-replication path
        %% (`macula_station_content_handlers:safe_peer_connections/0')
        %% reads `macula_station_peer_links:connections/0', which is
        %% populated ONLY by a real `macula_station_outbound_link'
        %% worker (see its moduledoc) — `dial/2' is a raw ad-hoc
        %% `macula_station:connect_to/1' that bypasses `outbound_link'
        %% entirely and leaves `peer_links' permanently empty for the
        %% pairing, however the handshake itself succeeds fine.
        {ok, _Link} = macula_station_test_cluster:dial_outbound(A, B),
        ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 10_000),
        UrlA = station_url(macula_station_test_cluster:listen_addr(A)),
        UrlB = station_url(macula_station_test_cluster:listen_addr(B)),
        {ok, PoolA} = macula:connect([UrlA], #{verify => none}),
        {ok, PoolB} = macula:connect([UrlB], #{verify => none}),
        try
            ok = wait_healthy(PoolA),
            ok = wait_healthy(PoolB),
            Bytes = <<"replicate me to the peer station">>,
            {ok, MCID} = macula:put_content(PoolA, Bytes),
            ?assertMatch(<<1, 16#55, _:32/binary>>, MCID),
            ?assertEqual({ok, Bytes}, wait_get_content(PoolB, MCID, 50))
        after
            catch macula:close(PoolA),
            catch macula:close(PoolB)
        end
    after
        macula_station_test_cluster:stop_cluster([A, B])
    end.

%% Chain A - B - C (A/C not peered). Content put via A eager-replicates
%% to B (same mechanism the test above verifies). A daemon connected
%% ONLY to C has no direct route to A's copy — C's local
%% `_content.get_block' misses, and `?DEFAULT_HOPS' = 1 iterative
%% fanout to C's own peer (B) — over the SAME new dedicated-stream
%% primitive, this time the `_content.get_block' direction
%% (`macula_station_content_handlers:safe_call/2') — finds B's
%% replicated copy and returns it.
iterative_get_reaches_two_hop_peer(Config) ->
    Opts = ?config(cluster_opts, Config),
    [A, B, C] = macula_station_test_cluster:spawn_cluster(3, Opts),
    try
        [ok = boot_content_store(S, ?config(priv_dir, Config)) || S <- [A, B, C]],
        %% `dial_outbound/2' on both legs — see the comment in
        %% `eager_replication_lands_on_peer_station' above.
        {ok, _LinkAB} = macula_station_test_cluster:dial_outbound(A, B),
        {ok, _LinkCB} = macula_station_test_cluster:dial_outbound(C, B),
        ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 10_000),
        ok = macula_station_test_cluster:wait_for_handshakes(C, 1, 10_000),
        UrlA = station_url(macula_station_test_cluster:listen_addr(A)),
        UrlC = station_url(macula_station_test_cluster:listen_addr(C)),
        {ok, PoolA} = macula:connect([UrlA], #{verify => none}),
        {ok, PoolC} = macula:connect([UrlC], #{verify => none}),
        try
            ok = wait_healthy(PoolA),
            ok = wait_healthy(PoolC),
            Bytes = <<"two hops away via iterative fanout">>,
            {ok, MCID} = macula:put_content(PoolA, Bytes),
            %% Give eager replication (A -> B) time to land before C's
            %% iterative fanout (C -> B) can find it.
            ?assertEqual({ok, Bytes}, wait_get_content(PoolC, MCID, 50))
        after
            catch macula:close(PoolA),
            catch macula:close(PoolC)
        end
    after
        macula_station_test_cluster:stop_cluster([A, B, C])
    end.

wait_get_content(_Pool, _MCID, 0) -> {error, not_found};
wait_get_content(Pool, MCID, N) ->
    get_content_or_retry(macula:get_content(Pool, MCID), Pool, MCID, N).

get_content_or_retry({ok, _Bin} = Result, _Pool, _MCID, _N) -> Result;
get_content_or_retry(_Other, Pool, MCID, N) ->
    timer:sleep(100),
    wait_get_content(Pool, MCID, N - 1).

wait_providers(_Pool, _MCID, 0) -> {ok, []};
wait_providers(Pool, MCID, N) ->
    providers_or_retry(macula:find_content_providers(Pool, MCID), Pool, MCID, N).

providers_or_retry({ok, [_ | _]} = Result, _Pool, _MCID, _N) -> Result;
providers_or_retry(_Other, Pool, MCID, N) ->
    timer:sleep(100),
    wait_providers(Pool, MCID, N - 1).

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

with_station(Config, Fun) ->
    Opts       = ?config(cluster_opts, Config),
    [B]        = macula_station_test_cluster:spawn_cluster(1, Opts),
    ok         = boot_content_store(B, ?config(priv_dir, Config)),
    Url        = station_url(macula_station_test_cluster:listen_addr(B)),
    {ok, Pool} = macula:connect([Url], #{verify => none}),
    try
        ok = wait_healthy(Pool),
        Fun(Pool)
    after
        catch macula:close(Pool),
        macula_station_test_cluster:stop_cluster([B])
    end.

%% The harness boots the station WITHOUT `macula_content' (its default
%% store_dir, /var/lib/hecate/content, is unwritable in a sandbox) —
%% start it on the peer with a writable dir under this test's priv_dir.
%% `macula_station_content_handlers' / `macula_content_announcer' are
%% already running (children of macula_station_sup); only the block
%% store itself is missing.
boot_content_store(Station, PrivDir) ->
    StoreDir = filename:join(PrivDir, "content_store"),
    ok = macula_station_test_cluster:rpc(
           Station, application, set_env,
           [macula_content, store_dir, StoreDir]),
    {ok, _Started} = macula_station_test_cluster:rpc(
                       Station, application, ensure_all_started,
                       [macula_content]),
    ok.

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
