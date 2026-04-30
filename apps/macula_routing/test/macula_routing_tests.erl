%% EUnit tests for macula_routing.
-module(macula_routing_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Construction
%%---------------------------------------------------------------------

new_is_empty_test() ->
    G = macula_routing:new(),
    ?assertEqual([], macula_routing:vertices(G)).

add_vertex_idempotent_test() ->
    G  = macula_routing:add_vertex(a, macula_routing:new()),
    G2 = macula_routing:add_vertex(a, G),
    ?assertEqual([a], macula_routing:vertices(G2)).

add_edge_creates_endpoints_and_weight_test() ->
    G = macula_routing:add_edge(a, b, 5, macula_routing:new()),
    ?assertEqual(lists:sort([a, b]),
                 lists:sort(macula_routing:vertices(G))),
    ?assertEqual({ok, 5}, macula_routing:weight(a, b, G)).

add_edge_self_loop_rejected_test() ->
    ?assertError(function_clause,
                 macula_routing:add_edge(a, a, 1, macula_routing:new())).

add_edge_negative_weight_rejected_test() ->
    ?assertError(function_clause,
                 macula_routing:add_edge(a, b, -1, macula_routing:new())).

add_edges_bulk_inserts_test() ->
    G = macula_routing:add_edges(macula_routing:new(),
                                 [{a, b, 1}, {b, c, 2}, {c, d, 3}]),
    ?assertEqual(4, length(macula_routing:vertices(G))).

weight_unknown_returns_error_test() ->
    G = macula_routing:new(),
    ?assertEqual(error, macula_routing:weight(a, b, G)).

%%---------------------------------------------------------------------
%% Weight model (Part 3 §6.3)
%%---------------------------------------------------------------------

tier_penalty_orders_t0_above_t3_test() ->
    ?assert(macula_routing:tier_penalty(t0)
              > macula_routing:tier_penalty(t1)),
    ?assert(macula_routing:tier_penalty(t1)
              > macula_routing:tier_penalty(t2)),
    ?assert(macula_routing:tier_penalty(t2)
              > macula_routing:tier_penalty(t3)).

edge_weight_with_known_latency_test() ->
    %% 30 + 10 (default bw) + 5 (t1) = 45
    ?assertEqual(45, macula_routing:edge_weight(t1, 30)).

edge_weight_with_undefined_latency_uses_default_test() ->
    %% 100 + 10 + 5 = 115
    ?assertEqual(115, macula_routing:edge_weight(t1, undefined)).

edge_weight_custom_bandwidth_term_test() ->
    %% 30 + 25 + 1 (t3) = 56
    ?assertEqual(56, macula_routing:edge_weight(t3, 30, 25)).

%%---------------------------------------------------------------------
%% Dijkstra
%%---------------------------------------------------------------------

shortest_path_in_diamond_picks_lower_cost_branch_test() ->
    %% a→b→d (1+2=3) vs a→c→d (2+3=5). Expect [a,b,d].
    G = build([{a, b, 1}, {b, d, 2}, {a, c, 2}, {c, d, 3}]),
    {ok, Path, Cost} = macula_routing:shortest_path(G, a, d),
    ?assertEqual([a, b, d], Path),
    ?assertEqual(3, Cost).

shortest_path_uses_relaxation_through_indirect_route_test() ->
    %% Direct a→d cost 100; indirect a→b→d cost 3.
    G = build([{a, d, 100}, {a, b, 1}, {b, d, 2}]),
    {ok, Path, _} = macula_routing:shortest_path(G, a, d),
    ?assertEqual([a, b, d], Path).

shortest_path_unreachable_returns_none_test() ->
    G = macula_routing:add_vertex(c,
                                  macula_routing:add_edge(a, b, 1,
                                                          macula_routing:new())),
    ?assertEqual(none, macula_routing:shortest_path(G, a, c)).

shortest_path_unknown_source_returns_none_test() ->
    G = build([{a, b, 1}]),
    ?assertEqual(none, macula_routing:shortest_path(G, x, b)).

shortest_path_src_equals_tgt_test() ->
    G = macula_routing:add_vertex(a, macula_routing:new()),
    {ok, [a], 0} = macula_routing:shortest_path(G, a, a).

%%---------------------------------------------------------------------
%% Vertex-disjoint paths
%%---------------------------------------------------------------------

disjoint_paths_yields_two_in_diamond_test() ->
    %% a→b→e and a→c→e are vertex-disjoint.
    G = build([{a, b, 1}, {b, e, 1}, {a, c, 1}, {c, e, 1}]),
    Paths = macula_routing:disjoint_paths(G, a, e, 2),
    ?assertEqual(2, length(Paths)),
    %% Intermediate vertices b and c each appear in exactly one path.
    Mid = lists:sort([V || P <- Paths, V <- (P -- [a, e])]),
    ?assertEqual([b, c], Mid).

disjoint_paths_yields_three_when_three_routes_exist_test() ->
    G = build([{a, b, 1}, {b, e, 1},
               {a, c, 1}, {c, e, 1},
               {a, d, 1}, {d, e, 1}]),
    Paths = macula_routing:disjoint_paths(G, a, e, 3),
    ?assertEqual(3, length(Paths)),
    Mid = lists:sort([V || P <- Paths, V <- (P -- [a, e])]),
    ?assertEqual([b, c, d], Mid).

disjoint_paths_returns_fewer_when_graph_is_sparse_test() ->
    G = build([{a, b, 1}, {b, e, 1}]),
    Paths = macula_routing:disjoint_paths(G, a, e, 3),
    ?assertEqual(1, length(Paths)),
    ?assertEqual([[a, b, e]], Paths).

disjoint_paths_returns_empty_when_unreachable_test() ->
    G = macula_routing:add_vertex(e,
                                  macula_routing:add_edge(a, b, 1,
                                                          macula_routing:new())),
    Paths = macula_routing:disjoint_paths(G, a, e, 3),
    ?assertEqual([], Paths).

disjoint_paths_intermediate_vertices_are_globally_unique_test() ->
    %% Build a graph where path 1 uses {b, c}, path 2 must use
    %% different intermediates {d, f}.
    G = build([{a, b, 1}, {b, c, 1}, {c, e, 1},
               {a, d, 5}, {d, f, 5}, {f, e, 5}]),
    Paths = macula_routing:disjoint_paths(G, a, e, 2),
    ?assertEqual(2, length(Paths)),
    AllMid = [V || P <- Paths, V <- (P -- [a, e])],
    ?assertEqual(length(AllMid), length(lists:usort(AllMid))).

disjoint_paths_first_path_is_shortest_test() ->
    %% Two parallel paths, one cheaper. First disjoint should be
    %% the cheaper one.
    G = build([{a, b, 10}, {b, e, 10},
               {a, c, 1},  {c, e, 1}]),
    [First, Second] = macula_routing:disjoint_paths(G, a, e, 2),
    ?assertEqual([a, c, e], First),
    ?assertEqual([a, b, e], Second).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

build(Edges) ->
    macula_routing:add_edges(macula_routing:new(), Edges).
