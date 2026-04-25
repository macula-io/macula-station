%% EUnit tests for hecate_dht_lookup — disjoint-path iterative
%% FIND_NODE against a small in-VM DHT network.
%%
%% Each test builds its own network of N DHT gen_servers plus a
%% single router pid that routes frames between them by NodeId.
-module(hecate_dht_lookup_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Empty DHT
%%---------------------------------------------------------------------

lookup_on_empty_dht_returns_empty_test() ->
    Net = start_network([a]),
    #{a := {A, _}} = Net,
    {ok, []} = hecate_dht:lookup_nodes(A, <<1:256>>, short_opts()),
    stop_network(Net).

%%---------------------------------------------------------------------
%% Single hop — A knows B, looks up for key close to B
%%---------------------------------------------------------------------

lookup_finds_refs_from_single_peer_test() ->
    Net = start_network([a, b]),
    #{a := {A, KpA}, b := {B, KpB}} = Net,
    BId = hecate_identity:public(KpB),

    %% A knows B.
    admitted = hecate_dht:observe(A, spec(BId)),
    %% B has three peers of its own that A has never seen.
    P1 = <<1:256>>, P2 = <<2:256>>, P3 = <<3:256>>,
    admitted = hecate_dht:observe(B, spec(P1)),
    admitted = hecate_dht:observe(B, spec(P2)),
    admitted = hecate_dht:observe(B, spec(P3)),

    {ok, Refs} = hecate_dht:lookup_nodes(A, BId, short_opts()),
    Ids = lists:sort([maps:get(node_id, R) || R <- Refs]),
    %% A's final shortlist should include B (from seed) and the three
    %% peers learned via FIND_NODE to B.
    ?assert(lists:member(BId, Ids)),
    ?assert(lists:member(P1, Ids)),
    ?assert(lists:member(P2, Ids)),
    ?assert(lists:member(P3, Ids)),

    _ = KpA,
    stop_network(Net).

%%---------------------------------------------------------------------
%% Multi-hop chain — A→B→C→D, lookup from A for D
%%---------------------------------------------------------------------

lookup_traverses_multi_hop_chain_test() ->
    Net = start_network([a, b, c, d]),
    #{a := {A, _}, b := {B, KpB}, c := {C, KpC}, d := {D, KpD}} = Net,
    BId = hecate_identity:public(KpB),
    CId = hecate_identity:public(KpC),
    DId = hecate_identity:public(KpD),

    %% A only knows B; B only knows C; C only knows D.
    admitted = hecate_dht:observe(A, spec(BId)),
    admitted = hecate_dht:observe(B, spec(CId)),
    admitted = hecate_dht:observe(C, spec(DId)),

    {ok, Refs} = hecate_dht:lookup_nodes(A, DId, short_opts()),
    Ids = [maps:get(node_id, R) || R <- Refs],
    %% The lookup must surface D itself — the chain worked end-to-end.
    ?assert(lists:member(DId, Ids)),
    %% And the intermediate peers must also appear.
    ?assert(lists:member(CId, Ids)),
    ?assert(lists:member(BId, Ids)),
    %% Closest-to-D first: D wins by XOR distance 0.
    [FirstId | _] = Ids,
    ?assertEqual(DId, FirstId),

    _ = D,
    stop_network(Net).

%%---------------------------------------------------------------------
%% Target-count caps the result
%%---------------------------------------------------------------------

lookup_respects_target_count_test() ->
    Net = start_network([a, b]),
    #{a := {A, _}, b := {B, KpB}} = Net,
    BId = hecate_identity:public(KpB),
    admitted = hecate_dht:observe(A, spec(BId)),
    [admitted = hecate_dht:observe(B, spec(<<N:256>>)) || N <- lists:seq(1, 10)],
    {ok, Refs} = hecate_dht:lookup_nodes(A, <<1:256>>,
                                         short_opts(#{target_count => 3})),
    ?assertEqual(3, length(Refs)),
    stop_network(Net).

%%---------------------------------------------------------------------
%% Result is unique by NodeId and sorted by distance
%%---------------------------------------------------------------------

lookup_result_is_deduped_and_ordered_by_distance_test() ->
    Net = start_network([a, b, c]),
    #{a := {A, _}, b := {B, KpB}, c := {C, KpC}} = Net,
    BId = hecate_identity:public(KpB),
    CId = hecate_identity:public(KpC),
    %% Both B and C share two common peers — dedup must not double-count.
    Shared = <<42:256>>,
    admitted = hecate_dht:observe(A, spec(BId)),
    admitted = hecate_dht:observe(A, spec(CId)),
    admitted = hecate_dht:observe(B, spec(Shared)),
    admitted = hecate_dht:observe(C, spec(Shared)),

    {ok, Refs} = hecate_dht:lookup_nodes(A, <<42:256>>, short_opts()),
    Ids = [maps:get(node_id, R) || R <- Refs],
    %% Dedup — Shared appears at most once.
    ?assertEqual(length(Ids), length(lists:usort(Ids))),
    %% Sorted ascending by XOR distance to the target key.
    Dists = [hecate_dht_xor:distance_int(<<42:256>>, Id) || Id <- Ids],
    ?assertEqual(Dists, lists:sort(Dists)),
    stop_network(Net).

%%---------------------------------------------------------------------
%% Overall-timeout terminates the lookup cleanly
%%---------------------------------------------------------------------

lookup_honours_overall_timeout_test() ->
    %% A has a silent transport — find_node calls will time out.
    Kp = hecate_identity:generate(),
    {ok, A} = hecate_dht:start_link(#{
        self_id         => hecate_identity:public(Kp),
        identity        => Kp,
        send_frame      => fun(_, _) -> ok end
    }),
    %% Seed A with a peer that will never respond.
    admitted = hecate_dht:observe(A, spec(<<99:256>>)),
    Start = erlang:monotonic_time(millisecond),
    {ok, Refs} = hecate_dht:lookup_nodes(A, <<99:256>>,
                                         #{overall_timeout_ms => 150,
                                           per_request_timeout_ms => 100}),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    %% Lookup must terminate within a small envelope around the
    %% configured overall timeout.
    ?assert(Elapsed < 500),
    %% The seeded peer is still reported (it's in A's shortlist even
    %% though it never responded).
    Ids = [maps:get(node_id, R) || R <- Refs],
    ?assert(lists:member(<<99:256>>, Ids)),
    hecate_dht:stop(A).

%%---------------------------------------------------------------------
%% Disjointness — a peer reached via two paths is queried only once
%%---------------------------------------------------------------------

disjoint_paths_do_not_double_query_test() ->
    Net = start_network([a, b, c, d]),
    #{a := {A, _}, b := {B, KpB}, c := {C, KpC}, d := {_D, KpD}} = Net,
    BId = hecate_identity:public(KpB),
    CId = hecate_identity:public(KpC),
    DId = hecate_identity:public(KpD),
    %% A knows B and C (two initial seeds — round-robin across 2 paths).
    admitted = hecate_dht:observe(A, spec(BId)),
    admitted = hecate_dht:observe(A, spec(CId)),
    %% Both B and C know D. Without disjointness D could be queried
    %% by both paths; with disjointness exactly one path claims it.
    admitted = hecate_dht:observe(B, spec(DId)),
    admitted = hecate_dht:observe(C, spec(DId)),

    {ok, _} = hecate_dht:lookup_nodes(A, DId,
                                      short_opts(#{paths => 2})),
    Stats = router_stats(),
    FindNodesToD = maps:get({DId, find_node}, Stats, 0),
    ?assertEqual(1, FindNodesToD),
    stop_network(Net).

%%=====================================================================
%% Router harness — one router per network routes signed frames
%% between DHT servers by NodeId.
%%=====================================================================

start_network(Names) ->
    Router = spawn_router(),
    Kps = [{N, hecate_identity:generate()} || N <- Names],
    Pairs = [{N, make_server(Kp, Router)} || {N, Kp} <- Kps],
    Net = maps:from_list([{N, {P, Kp}} || {N, {P, Kp}} <- Pairs]),
    RouteMap = maps:from_list([{hecate_identity:public(Kp), P}
                               || {_, {P, Kp}} <- Pairs]),
    Router ! {table, RouteMap},
    put(lookup_router, Router),
    Net.

make_server(Kp, Router) ->
    SelfId = hecate_identity:public(Kp),
    Send = fun(DstId, Frame) ->
        Router ! {route, SelfId, DstId, Frame}, ok
    end,
    {ok, P} = hecate_dht:start_link(#{
        self_id              => SelfId,
        identity             => Kp,
        send_frame           => Send,
        ping_timeout_ms      => 500,
        find_node_timeout_ms => 500
    }),
    {P, Kp}.

stop_network(Net) ->
    maps:foreach(fun(_, {P, _}) -> hecate_dht:stop(P) end, Net),
    Router = get(lookup_router),
    Router ! stop.

spawn_router() ->
    spawn(fun() -> router_loop(undefined, #{}) end).

router_loop(Table, Stats) ->
    receive
        {table, T}                -> router_loop(T, Stats);
        {route, From, Dst, Frame} ->
            route_frame(Table, From, Dst, Frame),
            router_loop(Table, bump_stat(Stats, Dst, Frame));
        {stats, Caller} ->
            Caller ! {stats, Stats},
            router_loop(Table, Stats);
        stop -> ok
    end.

route_frame(undefined, _From, _Dst, _Frame) -> ok;
route_frame(Table, From, Dst, Frame) ->
    case maps:find(Dst, Table) of
        {ok, Pid} -> hecate_dht:handle_frame(Pid, From, Frame);
        error     -> ok
    end.

bump_stat(Stats, Dst, Frame) ->
    Key = {Dst, hecate_frame:frame_type(Frame)},
    maps:update_with(Key, fun(N) -> N + 1 end, 1, Stats).

router_stats() ->
    Router = get(lookup_router),
    Router ! {stats, self()},
    receive {stats, S} -> S after 1_000 -> exit(stats_timeout) end.

spec(NodeId) ->
    #{node_id => NodeId, asn => 64512, country => <<"BE">>, tier => t1}.

%% Tight timeouts so eunit's default 5-second per-test ceiling is
%% never in play. 200 ms per request × up to 3 parallel requests =
%% one round of ~200 ms; overall 1500 ms is plenty for 2-3 rounds.
short_opts() -> short_opts(#{}).

short_opts(Extra) ->
    maps:merge(#{per_request_timeout_ms => 200,
                 overall_timeout_ms     => 1_500}, Extra).
