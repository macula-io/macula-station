%% EUnit tests for macula_dht_lookup — disjoint-path iterative
%% FIND_NODE against a small in-VM DHT network.
%%
%% Each test builds its own network of N DHT gen_servers plus a
%% single router pid that routes frames between them by NodeId.
-module(macula_dht_lookup_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Empty DHT
%%---------------------------------------------------------------------

lookup_on_empty_dht_returns_empty_test() ->
    Net = start_network([a]),
    #{a := {A, _}} = Net,
    {ok, []} = macula_dht:lookup_nodes(A, <<1:256>>, short_opts()),
    stop_network(Net).

%%---------------------------------------------------------------------
%% Single hop — A knows B, looks up for key close to B
%%---------------------------------------------------------------------

lookup_finds_refs_from_single_peer_test() ->
    Net = start_network([a, b]),
    #{a := {A, KpA}, b := {B, KpB}} = Net,
    BId = macula_identity:public(KpB),

    %% A knows B.
    admitted = macula_dht:observe(A, spec(BId)),
    %% B has three peers of its own that A has never seen.
    P1 = <<1:256>>, P2 = <<2:256>>, P3 = <<3:256>>,
    admitted = macula_dht:observe(B, spec(P1)),
    admitted = macula_dht:observe(B, spec(P2)),
    admitted = macula_dht:observe(B, spec(P3)),

    {ok, Refs} = macula_dht:lookup_nodes(A, BId, short_opts()),
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
    BId = macula_identity:public(KpB),
    CId = macula_identity:public(KpC),
    DId = macula_identity:public(KpD),

    %% A only knows B; B only knows C; C only knows D.
    admitted = macula_dht:observe(A, spec(BId)),
    admitted = macula_dht:observe(B, spec(CId)),
    admitted = macula_dht:observe(C, spec(DId)),

    {ok, Refs} = macula_dht:lookup_nodes(A, DId, short_opts()),
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
    BId = macula_identity:public(KpB),
    admitted = macula_dht:observe(A, spec(BId)),
    [admitted = macula_dht:observe(B, spec(<<N:256>>)) || N <- lists:seq(1, 10)],
    {ok, Refs} = macula_dht:lookup_nodes(A, <<1:256>>,
                                         short_opts(#{target_count => 3})),
    ?assertEqual(3, length(Refs)),
    stop_network(Net).

%%---------------------------------------------------------------------
%% Result is unique by NodeId and sorted by distance
%%---------------------------------------------------------------------

lookup_result_is_deduped_and_ordered_by_distance_test() ->
    Net = start_network([a, b, c]),
    #{a := {A, _}, b := {B, KpB}, c := {C, KpC}} = Net,
    BId = macula_identity:public(KpB),
    CId = macula_identity:public(KpC),
    %% Both B and C share two common peers — dedup must not double-count.
    Shared = <<42:256>>,
    admitted = macula_dht:observe(A, spec(BId)),
    admitted = macula_dht:observe(A, spec(CId)),
    admitted = macula_dht:observe(B, spec(Shared)),
    admitted = macula_dht:observe(C, spec(Shared)),

    {ok, Refs} = macula_dht:lookup_nodes(A, <<42:256>>, short_opts()),
    Ids = [maps:get(node_id, R) || R <- Refs],
    %% Dedup — Shared appears at most once.
    ?assertEqual(length(Ids), length(lists:usort(Ids))),
    %% Sorted ascending by XOR distance to the target key.
    Dists = [macula_dht_xor:distance_int(<<42:256>>, Id) || Id <- Ids],
    ?assertEqual(Dists, lists:sort(Dists)),
    stop_network(Net).

%%---------------------------------------------------------------------
%% Overall-timeout terminates the lookup cleanly
%%---------------------------------------------------------------------

lookup_honours_overall_timeout_test() ->
    %% A has a silent transport — find_node calls will time out.
    Kp = macula_identity:generate(),
    {ok, A} = macula_dht:start_link(#{
        self_id         => macula_identity:public(Kp),
        identity        => Kp,
        send_frame      => fun(_, _) -> ok end
    }),
    %% Seed A with a peer that will never respond.
    admitted = macula_dht:observe(A, spec(<<99:256>>)),
    Start = erlang:monotonic_time(millisecond),
    {ok, Refs} = macula_dht:lookup_nodes(A, <<99:256>>,
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
    macula_dht:stop(A).

%%---------------------------------------------------------------------
%% Disjointness — a peer reached via two paths is queried only once
%%---------------------------------------------------------------------

disjoint_paths_do_not_double_query_test() ->
    Net = start_network([a, b, c, d]),
    #{a := {A, _}, b := {B, KpB}, c := {C, KpC}, d := {_D, KpD}} = Net,
    BId = macula_identity:public(KpB),
    CId = macula_identity:public(KpC),
    DId = macula_identity:public(KpD),
    %% A knows B and C (two initial seeds — round-robin across 2 paths).
    admitted = macula_dht:observe(A, spec(BId)),
    admitted = macula_dht:observe(A, spec(CId)),
    %% Both B and C know D. Without disjointness D could be queried
    %% by both paths; with disjointness exactly one path claims it.
    admitted = macula_dht:observe(B, spec(DId)),
    admitted = macula_dht:observe(C, spec(DId)),

    {ok, _} = macula_dht:lookup_nodes(A, DId,
                                      short_opts(#{paths => 2})),
    Stats = router_stats(),
    FindNodesToD = maps:get({DId, find_node}, Stats, 0),
    ?assertEqual(1, FindNodesToD),
    stop_network(Net).

%%---------------------------------------------------------------------
%% Dial injection (2026-08-29) — a peer learned mid-walk (only ever
%% named in a NODES reply, never a round-0 seed) is dialed via the
%% injected `dial' option, with its real addresses, before being
%% queried. Proven directly: the router above reaches every network
%% member unconditionally regardless of dial state (it is a routing
%% table, not a connection-aware transport), so the tests above cannot
%% exercise this wiring on their own — absence of regression there
%% proves the DEFAULT (`no_dial') is unchanged, not that `dial' fires
%% correctly when supplied.
%%---------------------------------------------------------------------

dial_is_invoked_for_peers_learned_mid_walk_test() ->
    Net = start_network([a, b]),
    #{a := {A, _}, b := {B, KpB}} = Net,
    BId = macula_identity:public(KpB),
    CId = <<77:256>>,
    CEndpoints = [#{host => <<"c.example">>, port => 4433}],

    %% A knows B (round-0 seed). B knows C, with real endpoints -- A
    %% only ever learns of C via B's NODES reply, mid-walk.
    admitted = macula_dht:observe(A, spec(BId)),
    admitted = macula_dht:observe(B, spec(CId, CEndpoints)),

    Collector = spawn_dial_collector(),
    Dial = fun(PeerId, Addresses, _TimeoutMs) ->
        Collector ! {dialed, PeerId, Addresses},
        ok
    end,

    {ok, Refs} = macula_dht:lookup_nodes(A, CId,
                                         short_opts(#{dial => Dial})),
    Ids = [maps:get(node_id, R) || R <- Refs],
    ?assert(lists:member(CId, Ids)),

    %% C was actually dialed, with its real endpoints -- not just
    %% discovered and silently never reached.
    ?assert(lists:member({CId, CEndpoints}, collected(Collector))),
    stop_network(Net).

%% A `dial' that always refuses still lets the walk finish cleanly
%% (dial_then_find_node/5's `{error, _}' clause) -- it just cannot
%% actually QUERY anything beyond round-0 seeds, proving the gate is
%% genuinely a gate, not decorative. NOT provable via "is C in the
%% final result": C is DISCOVERED (added to the shortlist) the moment
%% B's NODES reply names it, regardless of whether A ever successfully
%% dials and queries C directly -- `finalise/1' returns shortlist
%% membership, not query success (see `lookup_honours_overall_timeout_test'
%% above, whose own seeded-but-never-responding peer is "still reported").
%% The only way to observe a blocked QUERY is a peer known ONLY to C:
%% D is discoverable at all solely via a NODES reply FROM C, which
%% requires A to have actually reached C -- refusing that dial must
%% therefore keep D undiscovered even though C itself still shows up.
dial_refusal_blocks_querying_the_refused_peer_test() ->
    Net = start_network([a, b, c]),
    #{a := {A, _}, b := {B, KpB}, c := {C, KpC}} = Net,
    BId = macula_identity:public(KpB),
    CId = macula_identity:public(KpC),
    DId = <<88:256>>,
    admitted = macula_dht:observe(A, spec(BId)),
    admitted = macula_dht:observe(B, spec(CId, [#{host => <<"c.example">>,
                                                  port => 4433}])),
    admitted = macula_dht:observe(C, spec(DId, [#{host => <<"d.example">>,
                                                  port => 4433}])),

    %% Refuses ONLY C -- B must still succeed (in production, `dial'
    %% is a no-op for an already-connected round-0 seed like B; a
    %% blanket refusal would also block A's very first query to B,
    %% and then B's own NODES reply naming C would never arrive
    %% either, which was this test's first, wrong version).
    RefuseOnly = fun(PeerId, _Addresses, _TimeoutMs) ->
        refuse_if(PeerId =:= CId)
    end,
    {ok, Refs} = macula_dht:lookup_nodes(A, DId,
                                         short_opts(#{dial => RefuseOnly})),
    Ids = [maps:get(node_id, R) || R <- Refs],
    %% C is discovered (named in B's NODES reply) even though dialing
    %% it was refused -- discovery and query success are different
    %% things, by design (see the comment above).
    ?assert(lists:member(CId, Ids)),
    %% D is known ONLY to C. Reaching it requires A to have actually
    %% QUERIED C, which the refused dial made impossible -- so D is
    %% never discovered at all.
    ?assertNot(lists:member(DId, Ids)),
    stop_network(Net).

refuse_if(true)  -> {error, refused};
refuse_if(false) -> ok.

spawn_dial_collector() ->
    spawn(fun() -> dial_collector_loop([]) end).

dial_collector_loop(Acc) ->
    receive
        {dialed, PeerId, Addresses} ->
            dial_collector_loop([{PeerId, Addresses} | Acc]);
        {get, Caller} ->
            Caller ! {dials, Acc}
    end.

collected(Collector) ->
    Collector ! {get, self()},
    receive {dials, D} -> D after 1_000 -> exit(dial_collector_timeout) end.

%%=====================================================================
%% Router harness — one router per network routes signed frames
%% between DHT servers by NodeId.
%%=====================================================================

start_network(Names) ->
    Router = spawn_router(),
    Kps = [{N, macula_identity:generate()} || N <- Names],
    Pairs = [{N, make_server(Kp, Router)} || {N, Kp} <- Kps],
    Net = maps:from_list([{N, {P, Kp}} || {N, {P, Kp}} <- Pairs]),
    RouteMap = maps:from_list([{macula_identity:public(Kp), P}
                               || {_, {P, Kp}} <- Pairs]),
    Router ! {table, RouteMap},
    put(lookup_router, Router),
    Net.

make_server(Kp, Router) ->
    SelfId = macula_identity:public(Kp),
    Send = fun(DstId, Frame) ->
        Router ! {route, SelfId, DstId, Frame}, ok
    end,
    {ok, P} = macula_dht:start_link(#{
        self_id              => SelfId,
        identity             => Kp,
        send_frame           => Send,
        ping_timeout_ms      => 500,
        find_node_timeout_ms => 500
    }),
    {P, Kp}.

stop_network(Net) ->
    maps:foreach(fun(_, {P, _}) -> macula_dht:stop(P) end, Net),
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
        {ok, Pid} -> macula_dht:handle_frame(Pid, From, Frame);
        error     -> ok
    end.

bump_stat(Stats, Dst, Frame) ->
    Key = {Dst, macula_frame:frame_type(Frame)},
    maps:update_with(Key, fun(N) -> N + 1 end, 1, Stats).

router_stats() ->
    Router = get(lookup_router),
    Router ! {stats, self()},
    receive {stats, S} -> S after 1_000 -> exit(stats_timeout) end.

spec(NodeId) ->
    #{node_id => NodeId, asn => 64512, country => <<"BE">>, tier => t1}.

%% As spec/1, with real endpoints -- the dial-injection tests need a
%% learned peer's `station_ref' to carry non-empty `addresses' (what
%% `entry_to_station_ref/2' actually propagates since the 2026-07-27
%% fix), otherwise `send_query/3' would call `Dial' with `[]' regardless
%% of what is being tested here.
spec(NodeId, Endpoints) ->
    (spec(NodeId))#{endpoints => Endpoints}.

%% Tight timeouts so eunit's default 5-second per-test ceiling is
%% never in play. 200 ms per request × up to 3 parallel requests =
%% one round of ~200 ms; overall 1500 ms is plenty for 2-3 rounds.
short_opts() -> short_opts(#{}).

short_opts(Extra) ->
    maps:merge(#{per_request_timeout_ms => 200,
                 overall_timeout_ms     => 1_500}, Extra).
