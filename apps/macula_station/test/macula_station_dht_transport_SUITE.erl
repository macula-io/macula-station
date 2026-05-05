%% @doc CT suite — the C1 reproducer for the no_transport DHT bug.
%%
%% Documents the regression discovered live on the BE station fleet
%% on 2026-05-05: `macula_dht:find_value/4' returns
%% `{error, no_transport}' immediately because the DHT module's
%% outbound transport callback is never installed in production
%% boot. See PLAN_DHT_TRANSPORT_WIRING.md for the analysis and the
%% 12-step fix.
%%
%% This suite EXPECTS THE TEST TO FAIL until PLAN_DHT_TRANSPORT_WIRING
%% lands. Once the SDK fix ships, `find_value` walks the network and
%% returns either `{value, [Record]}' (B's record found at custodian)
%% or `{nodes, [_|_]}' (B's record cache-missed, k-closest returned).
%% Either result confirms the wire works; `{error, no_transport}' is
%% the bug being tracked.
%%
%% Per Phase 1 plan (sequencing decision in
%% PLAN_PHASE_1_MULTI_PROCESS_CT_HARNESS.md): the test stays in the
%% suite as the regression-reproducer for C1. CI gating to block on
%% this test happens in Phase 2 (after Phase 4 closes C1). Until
%% then, the test running red is intentional — the failure message
%% itself is documentation of the open bug.
-module(macula_station_dht_transport_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([find_value_walks_the_network/1]).

%%==================================================================
%% CT plumbing
%%==================================================================

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [find_value_walks_the_network].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    PrivDir = ?config(priv_dir, Config),
    [{cluster_opts, #{base_dir => PrivDir}} | Config].

end_per_testcase(_Name, _Config) -> ok.

%%==================================================================
%% Tests
%%==================================================================

%% Boot 2 stations, dial A→B, ask A's DHT to find B's pubkey.
%%
%% Pre-PLAN_DHT_TRANSPORT_WIRING:
%%   Result =:= {error, no_transport}        — test FAILS, by design.
%%
%% Post-PLAN_DHT_TRANSPORT_WIRING:
%%   Result =:= {value, [_]}                 — record found at custodian
%%   OR Result =:= {nodes, [_|_]}            — k-closest returned (then
%%                                             A would do another hop)
%%
%% Either of the post-fix shapes is acceptable; both prove the wire
%% works end-to-end. `{error, no_transport}' is the only result
%% indicating C1 is still open.
find_value_walks_the_network(Config) ->
    Opts = ?config(cluster_opts, Config),
    Handles = macula_station_test_cluster:spawn_cluster(2, Opts),
    try
        [A, B] = Handles,
        ok = macula_station_test_cluster:dial(A, B),
        ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 10_000),
        BPubkey = macula_station_test_cluster:pubkey(B),
        %% Single-hop FIND_VALUE: A queries peer B for the value at
        %% B's own NodeId. Pre-fix this returned {error, no_transport}
        %% in 0ms because the DHT had no outbound callback. Post-fix
        %% the frame travels A→B over QUIC, B's peer_observer routes
        %% it into its DHT, the DHT either replies VALUE (B's record
        %% is locally stored) or NODES (cache miss).
        Result  = macula_station_test_cluster:rpc(
                    A, macula_dht, find_value,
                    [macula_dht, BPubkey, BPubkey, 5_000]),
        ct:pal("find_value(A, B's pubkey via B) returned: ~p", [Result]),
        case Result of
            {value, [_]} -> ok;
            {nodes, [_|_]} -> ok;
            {error, no_transport} ->
                ct:fail("C1 still open: macula_dht:find_value returned "
                        "{error, no_transport}. The DHT module's outbound "
                        "transport callback is not installed in production "
                        "boot. See PLAN_DHT_TRANSPORT_WIRING.md.");
            Other ->
                ct:fail({unexpected_find_value_result, Other})
        end
    after
        macula_station_test_cluster:stop_cluster(Handles)
    end.
