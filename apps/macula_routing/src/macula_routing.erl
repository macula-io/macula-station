%% @doc Path computation over the routing-table graph (Part 3 §6).
%%
%% Pure functional graph + Dijkstra + iterative-greedy
%% vertex-disjoint paths. The graph is opaque: callers build it
%% from whatever edge sources they have (local routing table,
%% lookup observations, peer-supplied connectivity hints) and
%% compute paths against it.
%%
%% == Edge weight model (§6.3) ==
%%
%% <pre>
%%   edge_weight = latency_ms + bandwidth_term + tier_penalty(dst_tier)
%%   tier_penalty(t0) = 10  (residential — costly)
%%   tier_penalty(t1) =  5  (business)
%%   tier_penalty(t2) =  2  (datacenter)
%%   tier_penalty(t3) =  1  (foundation — cheapest, prefer for cross-country)
%% </pre>
%%
%% The bandwidth term defaults to `10' (which corresponds to
%% `1 / 100Mbps × 1000') because we don't yet have observed
%% bandwidth measurements; latency defaults to 100 ms when the
%% peer has no observed RTT yet. Both can be overridden by
%% callers with concrete data.
%%
%% == Disjoint-path algorithm ==
%%
%% Phase 4.3 ships an iterative-greedy variant: find the shortest
%% path, remove its intermediate vertices, repeat until `K' paths
%% are found (or no further path exists). The plan-of-record names
%% Suurballe's algorithm; the iterative-greedy form is correct
%% (paths are vertex-disjoint and each is the shortest among
%% remaining alternatives) but not strictly cost-optimal across
%% the full set. A Suurballe upgrade lands in Phase 7 hardening
%% if measured path cost matters at scale.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §6;
%% plans/PLAN_PHASE_4_BREAKDOWN.md Session 4.3.
-module(macula_routing).

-export([
    %% Graph construction
    new/0,
    add_vertex/2,
    add_edge/4,
    add_edges/2,

    %% Graph inspection
    vertices/1,
    has_vertex/2,
    out_neighbors/2,
    weight/3,

    %% Weight model
    edge_weight/2,
    edge_weight/3,
    tier_penalty/1,

    %% Pathfinding
    dijkstra/2,
    shortest_path/3,
    disjoint_paths/3,
    disjoint_paths/4
]).

-export_type([graph/0, vertex/0, weight/0, path/0, tier/0]).

-define(DEFAULT_LATENCY_MS,     100).
-define(DEFAULT_BANDWIDTH_TERM,  10).

-type vertex() :: term().
-type weight() :: number().
-type path()   :: [vertex(), ...].
-type tier()   :: t0 | t1 | t2 | t3.

-type graph() :: #{
    vertices := sets:set(vertex()),
    out      := #{vertex() => #{vertex() => weight()}}
}.

%%=====================================================================
%% Construction
%%=====================================================================

-spec new() -> graph().
new() ->
    #{vertices => sets:new(), out => #{}}.

-spec add_vertex(vertex(), graph()) -> graph().
add_vertex(V, #{vertices := Vs, out := Out} = G) ->
    G#{
        vertices := sets:add_element(V, Vs),
        out      := maps:put(V, maps:get(V, Out, #{}), Out)
    }.

-spec add_edge(vertex(), vertex(), weight(), graph()) -> graph().
add_edge(From, To, W, G) when is_number(W), W >= 0, From =/= To ->
    G1 = add_vertex(To, add_vertex(From, G)),
    Out1 = maps:update_with(From,
                            fun(M) -> M#{To => W} end,
                            maps:get(out, G1)),
    G1#{out := Out1}.

-spec add_edges(graph(), [{vertex(), vertex(), weight()}]) -> graph().
add_edges(G, Edges) ->
    lists:foldl(fun({F, T, W}, A) -> add_edge(F, T, W, A) end, G, Edges).

%%=====================================================================
%% Inspection
%%=====================================================================

-spec vertices(graph()) -> [vertex()].
vertices(#{vertices := Vs}) ->
    sets:to_list(Vs).

-spec has_vertex(vertex(), graph()) -> boolean().
has_vertex(V, #{vertices := Vs}) ->
    sets:is_element(V, Vs).

-spec out_neighbors(vertex(), graph()) -> #{vertex() => weight()}.
out_neighbors(V, #{out := Out}) ->
    maps:get(V, Out, #{}).

-spec weight(vertex(), vertex(), graph()) -> {ok, weight()} | error.
weight(From, To, G) ->
    Neighbors = out_neighbors(From, G),
    case maps:find(To, Neighbors) of
        {ok, W} -> {ok, W};
        error   -> error
    end.

%%=====================================================================
%% Weight model
%%=====================================================================

-spec edge_weight(tier(), non_neg_integer() | undefined) -> weight().
edge_weight(Tier, LatencyMs) ->
    edge_weight(Tier, LatencyMs, ?DEFAULT_BANDWIDTH_TERM).

-spec edge_weight(tier(), non_neg_integer() | undefined, number()) ->
        weight().
edge_weight(Tier, undefined, Bw) ->
    edge_weight(Tier, ?DEFAULT_LATENCY_MS, Bw);
edge_weight(Tier, LatencyMs, Bw)
  when is_integer(LatencyMs), LatencyMs >= 0,
       is_number(Bw), Bw >= 0 ->
    LatencyMs + Bw + tier_penalty(Tier).

-spec tier_penalty(tier()) -> non_neg_integer().
tier_penalty(t0) -> 10;
tier_penalty(t1) ->  5;
tier_penalty(t2) ->  2;
tier_penalty(t3) ->  1.

%%=====================================================================
%% Dijkstra
%%
%% Single-source shortest paths from `Src' over `Graph'. Returns the
%% partial distance + parent maps; only vertices reachable from
%% `Src' appear. `infinity' is the implicit default for missing
%% entries.
%%=====================================================================

-spec dijkstra(graph(), vertex()) ->
        {#{vertex() => weight()}, #{vertex() => vertex()}}.
dijkstra(Graph, Src) ->
    dispatch_dijkstra(has_vertex(Src, Graph), Graph, Src).

-spec dispatch_dijkstra(boolean(), graph(), vertex()) ->
        {#{vertex() => weight()}, #{vertex() => vertex()}}.
dispatch_dijkstra(false, _Graph, _Src) ->
    {#{}, #{}};
dispatch_dijkstra(true, Graph, Src) ->
    Pq = gb_sets:singleton({0, Src}),
    loop_dijkstra(Graph, Pq, #{Src => 0}, #{}, sets:new()).

-spec loop_dijkstra(graph(), gb_sets:set(),
                    #{vertex() => weight()},
                    #{vertex() => vertex()},
                    sets:set(vertex())) ->
        {#{vertex() => weight()}, #{vertex() => vertex()}}.
loop_dijkstra(Graph, Pq, Dist, Parent, Visited) ->
    case gb_sets:is_empty(Pq) of
        true  -> {Dist, Parent};
        false -> step_dijkstra(Graph, Pq, Dist, Parent, Visited)
    end.

-spec step_dijkstra(graph(), gb_sets:set(),
                    #{vertex() => weight()},
                    #{vertex() => vertex()},
                    sets:set(vertex())) ->
        {#{vertex() => weight()}, #{vertex() => vertex()}}.
step_dijkstra(Graph, Pq, Dist, Parent, Visited) ->
    {{D, V}, Pq1} = gb_sets:take_smallest(Pq),
    advance_dijkstra(sets:is_element(V, Visited),
                     V, D, Graph, Pq1, Dist, Parent, Visited).

-spec advance_dijkstra(boolean(), vertex(), weight(), graph(),
                       gb_sets:set(),
                       #{vertex() => weight()},
                       #{vertex() => vertex()},
                       sets:set(vertex())) ->
        {#{vertex() => weight()}, #{vertex() => vertex()}}.
advance_dijkstra(true, _V, _D, Graph, Pq, Dist, Parent, Visited) ->
    loop_dijkstra(Graph, Pq, Dist, Parent, Visited);
advance_dijkstra(false, V, D, Graph, Pq, Dist, Parent, Visited) ->
    Visited1 = sets:add_element(V, Visited),
    {Dist1, Parent1, Pq1} = relax_neighbors(V, D, Graph, Dist, Parent, Pq),
    loop_dijkstra(Graph, Pq1, Dist1, Parent1, Visited1).

-spec relax_neighbors(vertex(), weight(), graph(),
                      #{vertex() => weight()},
                      #{vertex() => vertex()},
                      gb_sets:set()) ->
        {#{vertex() => weight()},
         #{vertex() => vertex()},
         gb_sets:set()}.
relax_neighbors(V, D, Graph, Dist, Parent, Pq) ->
    Neighbors = out_neighbors(V, Graph),
    maps:fold(fun(U, W, Acc) -> maybe_relax(U, V, D + W, Acc) end,
              {Dist, Parent, Pq}, Neighbors).

-spec maybe_relax(vertex(), vertex(), weight(),
                  {#{vertex() => weight()},
                   #{vertex() => vertex()},
                   gb_sets:set()}) ->
        {#{vertex() => weight()},
         #{vertex() => vertex()},
         gb_sets:set()}.
maybe_relax(U, V, ND, {Dist, Parent, Pq}) ->
    Cur = maps:get(U, Dist, infinity),
    update_if_better(ND < Cur, U, V, ND, Dist, Parent, Pq).

-spec update_if_better(boolean(), vertex(), vertex(), weight(),
                       #{vertex() => weight()},
                       #{vertex() => vertex()},
                       gb_sets:set()) ->
        {#{vertex() => weight()},
         #{vertex() => vertex()},
         gb_sets:set()}.
update_if_better(false, _U, _V, _ND, Dist, Parent, Pq) ->
    {Dist, Parent, Pq};
update_if_better(true, U, V, ND, Dist, Parent, Pq) ->
    {Dist#{U => ND}, Parent#{U => V}, gb_sets:add({ND, U}, Pq)}.

%%=====================================================================
%% Path extraction
%%=====================================================================

-spec shortest_path(graph(), vertex(), vertex()) ->
        {ok, path(), weight()} | none.
shortest_path(Graph, Src, Tgt) ->
    {Dist, Parent} = dijkstra(Graph, Src),
    extract_path(maps:is_key(Tgt, Dist), Dist, Parent, Src, Tgt).

-spec extract_path(boolean(),
                   #{vertex() => weight()},
                   #{vertex() => vertex()},
                   vertex(), vertex()) ->
        {ok, path(), weight()} | none.
extract_path(false, _Dist, _Parent, _Src, _Tgt) ->
    none;
extract_path(true, Dist, Parent, Src, Tgt) ->
    {ok, reconstruct(Parent, Src, Tgt, [Tgt]), maps:get(Tgt, Dist)}.

-spec reconstruct(#{vertex() => vertex()}, vertex(), vertex(),
                  [vertex()]) -> path().
reconstruct(_Parent, Src, Src, Acc) ->
    Acc;
reconstruct(Parent, Src, Curr, Acc) ->
    Pred = maps:get(Curr, Parent),
    reconstruct(Parent, Src, Pred, [Pred | Acc]).

%%=====================================================================
%% Disjoint paths
%%=====================================================================

-spec disjoint_paths(graph(), vertex(), vertex()) -> [path()].
disjoint_paths(Graph, Src, Tgt) ->
    disjoint_paths(Graph, Src, Tgt, 3).

-spec disjoint_paths(graph(), vertex(), vertex(), pos_integer()) ->
        [path()].
disjoint_paths(Graph, Src, Tgt, K)
  when is_integer(K), K >= 1, Src =/= Tgt ->
    walk_paths(Graph, Src, Tgt, K, []).

-spec walk_paths(graph(), vertex(), vertex(), non_neg_integer(),
                 [path()]) -> [path()].
walk_paths(_Graph, _Src, _Tgt, 0, Acc) ->
    lists:reverse(Acc);
walk_paths(Graph, Src, Tgt, K, Acc) ->
    consider_next(shortest_path(Graph, Src, Tgt), Graph, Src, Tgt, K, Acc).

-spec consider_next({ok, path(), weight()} | none,
                    graph(), vertex(), vertex(),
                    non_neg_integer(), [path()]) -> [path()].
consider_next(none, _Graph, _Src, _Tgt, _K, Acc) ->
    lists:reverse(Acc);
consider_next({ok, Path, _Cost}, Graph, Src, Tgt, K, Acc) ->
    Intermediate = [V || V <- Path, V =/= Src, V =/= Tgt],
    Pruned = remove_vertices(Graph, Intermediate),
    walk_paths(Pruned, Src, Tgt, K - 1, [Path | Acc]).

-spec remove_vertices(graph(), [vertex()]) -> graph().
remove_vertices(Graph, Vs) ->
    lists:foldl(fun remove_vertex/2, Graph, Vs).

-spec remove_vertex(vertex(), graph()) -> graph().
remove_vertex(V, #{vertices := Vs, out := Out} = G) ->
    Out1 = maps:remove(V, Out),
    Out2 = maps:map(fun(_, M) -> maps:remove(V, M) end, Out1),
    G#{vertices := sets:del_element(V, Vs), out := Out2}.
