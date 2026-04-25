%% @doc S/Kademlia iterative FIND_NODE lookup with disjoint paths.
%%
%% Implements the algorithm from Part 3 §4.5:
%%
%% <pre>
%%   LOOKUP(key K, target_count n, paths d=3):
%%     for path p in 1..d, in parallel but path-disjoint:
%%       shortlist[p] = α closest-to-K known by self (no overlap with
%%                     shortlists of other paths)
%%       loop:
%%         pick α not-yet-queried peers from shortlist[p]
%%         send FIND_NODE(K) to each
%%         await replies; add returned peers to shortlist[p]
%%         terminate when shortlist[p] contains n peers closest-to-K
%%                   that have responded
%%     return union of top-n from all d paths, deduped
%% </pre>
%%
%% == Disjointness ==
%%
%% The module tracks a `claimed' set of NodeIds. Once a peer is added
%% to any path's shortlist it cannot enter another path's shortlist.
%% This is the conservative ("claimed") interpretation of the Part 3
%% §4.5 wording; a more permissive ("queried") interpretation would
%% allow shared shortlists until query-time. The conservative variant
%% is simpler and only differs from the permissive one when an early
%% response would have filled other paths' deficits, which is rare.
%%
%% == Termination ==
%%
%% Two independent signals terminate the lookup:
%%
%% <ul>
%%   <li>All paths are idle — no in-flight queries and every peer in
%%       the shortlist has been queried.</li>
%%   <li>The overall deadline expires.</li>
%% </ul>
%%
%% On termination, the lookup returns the top-N peers across all
%% paths, deduped by NodeId and ordered by ascending XOR distance to
%% the lookup key.
%%
%% == Concurrency ==
%%
%% Each outgoing FIND_NODE runs in a short-lived, unlinked worker
%% process that blocks on `hecate_dht:find_node/4' and sends its
%% result back to the orchestrator. Workers cannot outlive the
%% orchestrator meaningfully — they send into a (possibly dead)
%% mailbox and exit. Up to `d * α' (=9 by default) workers run in
%% parallel.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §4.5;
%% plans/PLAN_PHASE_3_BREAKDOWN.md Session 3.6.
-module(hecate_dht_lookup).

-export([lookup_nodes/2, lookup_nodes/3]).

-export_type([opts/0, result/0]).

-define(DEFAULT_TARGET_COUNT,            20).
-define(DEFAULT_PATHS,                    3).
-define(DEFAULT_ALPHA,                    3).
-define(DEFAULT_PER_REQUEST_TIMEOUT_MS, 5_000).
-define(DEFAULT_OVERALL_TIMEOUT_MS,    15_000).

-type opts() :: #{
    target_count           => pos_integer(),
    paths                  => pos_integer(),
    alpha                  => pos_integer(),
    per_request_timeout_ms => pos_integer(),
    overall_timeout_ms     => pos_integer()
}.

-type result() :: {ok, [hecate_frame:station_ref()]}.

-type path_id() :: non_neg_integer().

-type path() :: #{
    id        := path_id(),
    shortlist := [hecate_frame:station_ref()],
    queried   := sets:set(macula_identity:pubkey()),
    in_flight := sets:set(macula_identity:pubkey())
}.

-type state() :: #{
    dht          := hecate_dht:dht(),
    key          := hecate_dht_xor:id(),
    paths        := [path()],
    claimed      := sets:set(macula_identity:pubkey()),
    target_count := pos_integer(),
    alpha        := pos_integer(),
    per_req      := pos_integer(),
    deadline     := integer()
}.

%%=====================================================================
%% Public API
%%=====================================================================

-spec lookup_nodes(hecate_dht:dht(), hecate_dht_xor:id()) -> result().
lookup_nodes(Dht, Key) ->
    lookup_nodes(Dht, Key, #{}).

-spec lookup_nodes(hecate_dht:dht(), hecate_dht_xor:id(), opts()) -> result().
lookup_nodes(Dht, <<_:256>> = Key, Opts) when is_map(Opts) ->
    State = build_state(Dht, Key, Opts),
    run(State).

%%=====================================================================
%% State construction
%%=====================================================================

-spec build_state(hecate_dht:dht(), hecate_dht_xor:id(), opts()) -> state().
build_state(Dht, Key, Opts) ->
    N       = maps:get(target_count,           Opts, ?DEFAULT_TARGET_COUNT),
    D       = maps:get(paths,                  Opts, ?DEFAULT_PATHS),
    A       = maps:get(alpha,                  Opts, ?DEFAULT_ALPHA),
    PerReq  = maps:get(per_request_timeout_ms, Opts, ?DEFAULT_PER_REQUEST_TIMEOUT_MS),
    Overall = maps:get(overall_timeout_ms,     Opts, ?DEFAULT_OVERALL_TIMEOUT_MS),
    Self    = hecate_dht:self_id(Dht),
    Seeds   = seed_refs(Dht, Key, N * D, Self),
    Claimed = sets:from_list([Self | [id_of(R) || R <- Seeds]]),
    Paths   = distribute(Seeds, D, Key),
    #{
        dht          => Dht,
        key          => Key,
        paths        => Paths,
        claimed      => Claimed,
        target_count => N,
        alpha        => A,
        per_req      => PerReq,
        deadline     => erlang:monotonic_time(millisecond) + Overall
    }.

-spec seed_refs(hecate_dht:dht(), hecate_dht_xor:id(), pos_integer(),
                macula_identity:pubkey()) ->
          [hecate_frame:station_ref()].
seed_refs(Dht, Key, MaxCount, Self) ->
    Entries = hecate_dht:k_closest(Dht, Key, MaxCount),
    [hecate_dht_protocol:entry_to_station_ref(E)
     || E <- Entries, hecate_dht_entry:node_id(E) =/= Self].

-spec distribute([hecate_frame:station_ref()], pos_integer(),
                 hecate_dht_xor:id()) -> [path()].
distribute(Refs, D, Key) ->
    Sorted = sort_by_distance(Refs, Key),
    Buckets = split_round_robin(Sorted, D),
    [#{id        => I,
       shortlist => Lst,
       queried   => sets:new(),
       in_flight => sets:new()}
     || {I, Lst} <- lists:zip(lists:seq(0, D - 1), Buckets)].

-spec split_round_robin([T], pos_integer()) -> [[T]].
split_round_robin(List, D) ->
    Indexed = lists:zip(lists:seq(0, length(List) - 1), List),
    [[R || {I, R} <- Indexed, (I rem D) =:= B]
     || B <- lists:seq(0, D - 1)].

%%=====================================================================
%% Main loop
%%=====================================================================

-spec run(state()) -> result().
run(State) ->
    State1 = dispatch_all(State),
    loop_or_finalise(terminated(State1), State1).

-spec loop_or_finalise(boolean(), state()) -> result().
loop_or_finalise(true,  State) -> finalise(State);
loop_or_finalise(false, State) -> run(await_result(State)).

%%---------------------------------------------------------------------
%% Dispatch — top-up each path to α in-flight queries
%%---------------------------------------------------------------------

-spec dispatch_all(state()) -> state().
dispatch_all(State) ->
    Paths = [dispatch_path(P, State) || P <- maps:get(paths, State)],
    State#{paths := Paths}.

-spec dispatch_path(path(), state()) -> path().
dispatch_path(Path, State) ->
    Alpha = maps:get(alpha, State),
    Slots = max(0, Alpha - sets:size(maps:get(in_flight, Path))),
    dispatch_slots(Path, State, Slots).

-spec dispatch_slots(path(), state(), non_neg_integer()) -> path().
dispatch_slots(Path, _State, 0) ->
    Path;
dispatch_slots(Path, State, Remaining) ->
    advance(next_unqueried(Path), Path, State, Remaining).

-spec advance({ok, hecate_frame:station_ref()} | none, path(),
              state(), pos_integer()) -> path().
advance(none, Path, _State, _Remaining) ->
    Path;
advance({ok, Ref}, Path, State, Remaining) ->
    Path1 = send_query(Ref, Path, State),
    dispatch_slots(Path1, State, Remaining - 1).

-spec next_unqueried(path()) -> {ok, hecate_frame:station_ref()} | none.
next_unqueried(#{shortlist := Short, queried := Q}) ->
    scan_unqueried(Short, Q).

-spec scan_unqueried([hecate_frame:station_ref()],
                     sets:set(macula_identity:pubkey())) ->
          {ok, hecate_frame:station_ref()} | none.
scan_unqueried([], _Q) -> none;
scan_unqueried([Ref | Rest], Q) ->
    resume_scan(sets:is_element(id_of(Ref), Q), Ref, Rest, Q).

-spec resume_scan(boolean(), hecate_frame:station_ref(),
                  [hecate_frame:station_ref()],
                  sets:set(macula_identity:pubkey())) ->
          {ok, hecate_frame:station_ref()} | none.
resume_scan(true,  _Ref, Rest, Q) -> scan_unqueried(Rest, Q);
resume_scan(false, Ref,  _Rest, _Q) -> {ok, Ref}.

-spec send_query(hecate_frame:station_ref(), path(), state()) -> path().
send_query(Ref, Path, State) ->
    PathId = maps:get(id, Path),
    PeerId = id_of(Ref),
    Parent = self(),
    Dht    = maps:get(dht, State),
    Key    = maps:get(key, State),
    PerReq = maps:get(per_req, State),
    spawn(fun() ->
        Result = hecate_dht:find_node(Dht, Key, PeerId, PerReq),
        Parent ! {lookup_result, PathId, PeerId, Result}
    end),
    Path#{
        queried   := sets:add_element(PeerId, maps:get(queried, Path)),
        in_flight := sets:add_element(PeerId, maps:get(in_flight, Path))
    }.

%%---------------------------------------------------------------------
%% Termination check
%%---------------------------------------------------------------------

-spec terminated(state()) -> boolean().
terminated(State) ->
    past_deadline(State) orelse all_paths_idle(State).

-spec past_deadline(state()) -> boolean().
past_deadline(#{deadline := D}) ->
    erlang:monotonic_time(millisecond) >= D.

-spec all_paths_idle(state()) -> boolean().
all_paths_idle(#{paths := Paths}) ->
    lists:all(fun path_idle/1, Paths).

-spec path_idle(path()) -> boolean().
path_idle(#{in_flight := F, shortlist := Short, queried := Q}) ->
    sets:size(F) =:= 0
        andalso lists:all(fun(R) -> sets:is_element(id_of(R), Q) end, Short).

%%---------------------------------------------------------------------
%% Await worker result
%%---------------------------------------------------------------------

-spec await_result(state()) -> state().
await_result(State) ->
    TimeLeft = max(0, maps:get(deadline, State)
                      - erlang:monotonic_time(millisecond)),
    receive
        {lookup_result, PathId, PeerId, Result} ->
            absorb(State, PathId, PeerId, Result)
    after TimeLeft ->
        State
    end.

-spec absorb(state(), path_id(), macula_identity:pubkey(),
             hecate_dht:find_node_result()) -> state().
absorb(State, PathId, PeerId, {ok, Refs}) ->
    State1 = clear_in_flight(State, PathId, PeerId),
    merge_fresh(State1, PathId, Refs);
absorb(State, PathId, PeerId, {error, _}) ->
    clear_in_flight(State, PathId, PeerId).

-spec clear_in_flight(state(), path_id(), macula_identity:pubkey()) ->
          state().
clear_in_flight(State, PathId, PeerId) ->
    Paths = update_path(maps:get(paths, State), PathId,
                        fun(P) ->
                            F = sets:del_element(PeerId,
                                                 maps:get(in_flight, P)),
                            P#{in_flight := F}
                        end),
    State#{paths := Paths}.

-spec merge_fresh(state(), path_id(), [hecate_frame:station_ref()]) ->
          state().
merge_fresh(State, PathId, Refs) ->
    Claimed = maps:get(claimed, State),
    Fresh = [R || R <- Refs, not sets:is_element(id_of(R), Claimed)],
    Paths = update_path(maps:get(paths, State), PathId,
                        fun(P) -> add_to_shortlist(P, Fresh,
                                                    maps:get(key, State))
                         end),
    Claimed1 = lists:foldl(fun sets:add_element/2, Claimed,
                           [id_of(R) || R <- Fresh]),
    State#{paths := Paths, claimed := Claimed1}.

-spec add_to_shortlist(path(), [hecate_frame:station_ref()],
                       hecate_dht_xor:id()) -> path().
add_to_shortlist(#{shortlist := Short} = Path, Fresh, Key) ->
    Path#{shortlist := sort_by_distance(Short ++ Fresh, Key)}.

-spec update_path([path()], path_id(), fun((path()) -> path())) -> [path()].
update_path(Paths, PathId, F) ->
    [maybe_update(P, PathId, F) || P <- Paths].

-spec maybe_update(path(), path_id(), fun((path()) -> path())) -> path().
maybe_update(#{id := PathId} = P, PathId, F) -> F(P);
maybe_update(P, _Other, _F) -> P.

%%---------------------------------------------------------------------
%% Finalisation
%%---------------------------------------------------------------------

-spec finalise(state()) -> result().
finalise(State) ->
    N   = maps:get(target_count, State),
    Key = maps:get(key, State),
    All = lists:flatmap(fun(P) -> maps:get(shortlist, P) end,
                        maps:get(paths, State)),
    Sorted = sort_by_distance(dedup_by_id(All), Key),
    {ok, lists:sublist(Sorted, N)}.

%%=====================================================================
%% Small helpers
%%=====================================================================

-spec id_of(hecate_frame:station_ref()) -> macula_identity:pubkey().
id_of(#{node_id := Id}) -> Id.

-spec sort_by_distance([hecate_frame:station_ref()], hecate_dht_xor:id()) ->
          [hecate_frame:station_ref()].
sort_by_distance(Refs, Key) ->
    Keyed = [{hecate_dht_xor:distance_int(Key, id_of(R)), R} || R <- Refs],
    [R || {_, R} <- lists:keysort(1, Keyed)].

-spec dedup_by_id([hecate_frame:station_ref()]) ->
          [hecate_frame:station_ref()].
dedup_by_id(Refs) ->
    Map = lists:foldl(fun(R, Acc) -> Acc#{id_of(R) => R} end, #{}, Refs),
    maps:values(Map).
