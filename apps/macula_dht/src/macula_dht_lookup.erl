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
%% process that blocks on `macula_dht:find_node/4' and sends its
%% result back to the orchestrator. Workers cannot outlive the
%% orchestrator meaningfully — they send into a (possibly dead)
%% mailbox and exit. Up to `d * α' (=9 by default) workers run in
%% parallel.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §4.5;
%% plans/PLAN_PHASE_3_BREAKDOWN.md Session 3.6.
-module(macula_dht_lookup).

-export([lookup_nodes/2, lookup_nodes/3]).

-export_type([opts/0, result/0]).

-define(DEFAULT_TARGET_COUNT,            20).
-define(DEFAULT_PATHS,                    3).
-define(DEFAULT_ALPHA,                    3).
-define(DEFAULT_PER_REQUEST_TIMEOUT_MS, 5_000).
-define(DEFAULT_OVERALL_TIMEOUT_MS,    15_000).
-define(DEFAULT_DIAL_TIMEOUT_MS,        3_000).

%% Dial a peer learned mid-walk before querying it. `macula_dht' (this
%% app) has no dependency on `macula_station' (the app that actually
%% knows how to dial — `macula_station_dht_dialer'), so the walk cannot
%% call it directly without a circular app dependency; this is the same
%% dependency-inversion shape `macula_dht_server' already uses for
%% `send_frame' (injected by `macula_station_app' at boot, not called by
%% name from inside `macula_dht'). Default is a no-op — see `no_dial/3'
%% — so every existing caller (including this module's own tests) keeps
%% its current behaviour unless it opts in.
-type dial_fun() :: fun((macula_identity:pubkey(), [map()], pos_integer()) ->
                        ok | {error, term()}).

-type opts() :: #{
    target_count           => pos_integer(),
    paths                  => pos_integer(),
    alpha                  => pos_integer(),
    per_request_timeout_ms => pos_integer(),
    overall_timeout_ms     => pos_integer(),
    dial_timeout_ms        => pos_integer(),
    dial                   => dial_fun()
}.

-type result() :: {ok, [macula_frame:station_ref()]}.

-type path_id() :: non_neg_integer().

-type path() :: #{
    id        := path_id(),
    shortlist := [macula_frame:station_ref()],
    queried   := sets:set(macula_identity:pubkey()),
    in_flight := sets:set(macula_identity:pubkey())
}.

-type state() :: #{
    dht          := macula_dht:dht(),
    key          := macula_dht_xor:id(),
    paths        := [path()],
    claimed      := sets:set(macula_identity:pubkey()),
    target_count := pos_integer(),
    alpha        := pos_integer(),
    per_req      := pos_integer(),
    dial         := dial_fun(),
    dial_timeout := pos_integer(),
    deadline     := integer()
}.

%%=====================================================================
%% Public API
%%=====================================================================

-spec lookup_nodes(macula_dht:dht(), macula_dht_xor:id()) -> result().
lookup_nodes(Dht, Key) ->
    lookup_nodes(Dht, Key, #{}).

-spec lookup_nodes(macula_dht:dht(), macula_dht_xor:id(), opts()) -> result().
lookup_nodes(Dht, <<_:256>> = Key, Opts) when is_map(Opts) ->
    State = build_state(Dht, Key, Opts),
    run(State).

%%=====================================================================
%% State construction
%%=====================================================================

-spec build_state(macula_dht:dht(), macula_dht_xor:id(), opts()) -> state().
build_state(Dht, Key, Opts) ->
    N       = maps:get(target_count,           Opts, ?DEFAULT_TARGET_COUNT),
    D       = maps:get(paths,                  Opts, ?DEFAULT_PATHS),
    A       = maps:get(alpha,                  Opts, ?DEFAULT_ALPHA),
    PerReq  = maps:get(per_request_timeout_ms, Opts, ?DEFAULT_PER_REQUEST_TIMEOUT_MS),
    Overall = maps:get(overall_timeout_ms,     Opts, ?DEFAULT_OVERALL_TIMEOUT_MS),
    DialMs  = maps:get(dial_timeout_ms,        Opts, ?DEFAULT_DIAL_TIMEOUT_MS),
    Dial    = maps:get(dial,                   Opts, fun no_dial/3),
    Self    = macula_dht:self_id(Dht),
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
        dial         => Dial,
        dial_timeout => DialMs,
        deadline     => erlang:monotonic_time(millisecond) + Overall
    }.

%% Default: unchanged current behaviour — no dial, rely solely on
%% whatever connections already exist (round-0 seeds only). A caller
%% that wants a genuinely multi-hop walk supplies `dial =>
%% fun macula_station_dht_dialer:ensure_dialed/3' (or equivalent).
-spec no_dial(macula_identity:pubkey(), [map()], pos_integer()) -> ok.
no_dial(_PeerId, _Addresses, _TimeoutMs) -> ok.

-spec seed_refs(macula_dht:dht(), macula_dht_xor:id(), pos_integer(),
                macula_identity:pubkey()) ->
          [macula_frame:station_ref()].
seed_refs(Dht, Key, MaxCount, Self) ->
    Entries = macula_dht:k_closest(Dht, Key, MaxCount),
    [macula_dht_protocol:entry_to_station_ref(E)
     || E <- Entries, macula_dht_entry:node_id(E) =/= Self].

-spec distribute([macula_frame:station_ref()], pos_integer(),
                 macula_dht_xor:id()) -> [path()].
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

-spec advance({ok, macula_frame:station_ref()} | none, path(),
              state(), pos_integer()) -> path().
advance(none, Path, _State, _Remaining) ->
    Path;
advance({ok, Ref}, Path, State, Remaining) ->
    Path1 = send_query(Ref, Path, State),
    dispatch_slots(Path1, State, Remaining - 1).

-spec next_unqueried(path()) -> {ok, macula_frame:station_ref()} | none.
next_unqueried(#{shortlist := Short, queried := Q}) ->
    scan_unqueried(Short, Q).

-spec scan_unqueried([macula_frame:station_ref()],
                     sets:set(macula_identity:pubkey())) ->
          {ok, macula_frame:station_ref()} | none.
scan_unqueried([], _Q) -> none;
scan_unqueried([Ref | Rest], Q) ->
    resume_scan(sets:is_element(id_of(Ref), Q), Ref, Rest, Q).

-spec resume_scan(boolean(), macula_frame:station_ref(),
                  [macula_frame:station_ref()],
                  sets:set(macula_identity:pubkey())) ->
          {ok, macula_frame:station_ref()} | none.
resume_scan(true,  _Ref, Rest, Q) -> scan_unqueried(Rest, Q);
resume_scan(false, Ref,  _Rest, _Q) -> {ok, Ref}.

%% Dials `Ref' first (a no-op for an already-connected round-0 seed, and
%% for any caller that left `dial' at its default `no_dial/3' — see
%% moduledoc) before querying it. This is what makes a peer LEARNED
%% mid-walk (round >= 1, arriving only in a NODES reply, never a round-0
%% seed) actually reachable: `macula_dht_server''s injected `send_frame'
%% resolves a NodeId only through an EXISTING connection and never dials
%% on its own, so an undialed newly-learned peer would answer with
%% `no_route' no matter how correct its `station_ref' addresses are.
-spec send_query(macula_frame:station_ref(), path(), state()) -> path().
send_query(Ref, Path, State) ->
    PathId    = maps:get(id, Path),
    PeerId    = id_of(Ref),
    Addresses = maps:get(addresses, Ref, []),
    Parent    = self(),
    Dht       = maps:get(dht, State),
    Key       = maps:get(key, State),
    PerReq    = maps:get(per_req, State),
    Dial      = maps:get(dial, State),
    DialMs    = maps:get(dial_timeout, State),
    spawn(fun() ->
        Result = dial_then_find_node(Dial(PeerId, Addresses, DialMs),
                                     Dht, Key, PeerId, PerReq),
        Parent ! {lookup_result, PathId, PeerId, Result}
    end),
    Path#{
        queried   := sets:add_element(PeerId, maps:get(queried, Path)),
        in_flight := sets:add_element(PeerId, maps:get(in_flight, Path))
    }.

dial_then_find_node(ok, Dht, Key, PeerId, PerReq) ->
    macula_dht:find_node(Dht, Key, PeerId, PerReq);
dial_then_find_node({error, Reason}, _Dht, _Key, _PeerId, _PerReq) ->
    {error, {no_route, Reason}}.

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
             macula_dht:find_node_result()) -> state().
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

-spec merge_fresh(state(), path_id(), [macula_frame:station_ref()]) ->
          state().
merge_fresh(State, PathId, Refs) ->
    Claimed = maps:get(claimed, State),
    Fresh = [R || R <- Refs, not sets:is_element(id_of(R), Claimed)],
    ok = observe_fresh(maps:get(dht, State), Fresh),
    Paths = update_path(maps:get(paths, State), PathId,
                        fun(P) -> add_to_shortlist(P, Fresh,
                                                    maps:get(key, State))
                         end),
    Claimed1 = lists:foldl(fun sets:add_element/2, Claimed,
                           [id_of(R) || R <- Fresh]),
    State#{paths := Paths, claimed := Claimed1}.

%% Feed every newly-learned peer into the routing table.
%%
%% THIS IS HOW A KADEMLIA TABLE FILLS. Without it the lookup discovers
%% peers, hands them to its caller, and forgets them: the walk is the
%% only mechanism that learns nodes we were never told about, so a node
%% that does not observe here can never know more than it was seeded
%% with.
%%
%% It went unnoticed because every station in a mutually-dialled mesh is
%% fed by the CONNECT path instead: `macula_station_peer_observer'
%% observes each peer on handshake, in either direction. That is the only
%% other production fill path — an inbound FIND_NODE does NOT observe its
%% sender (`macula_dht_server:on_find_node/3' replies and returns state
%% unchanged), so do not reason as though it does.
%%
%% The first degree-1 LEAF exposed the gap: nothing dials a leaf, so it
%% gets exactly one handshake, and its table stayed pinned at that single
%% bootstrap seed while a self-lookup happily returned 20 peers it then
%% discarded.
%%
%% ⚠ WHAT THIS USED TO NOT BUY, fixed 2026-08-29. Two separate gaps had
%% to close for a learned entry to become genuinely reachable, not just
%% bookkeeping raising `dht:size/1':
%%
%%   1. `macula_dht_protocol:entry_to_station_ref/2' used to hardcode
%%      `addresses => []', so every ref arriving in a NODES reply was
%%      ADDRESS-LESS. Fixed 2026-07-27 (see that function's own comment;
%%      guarded by `macula_dht_endpoint_propagation_tests').
%%   2. Real addresses alone were still not enough: `macula_dht_server''s
%%      injected `send_frame' (`macula_station_dht_transport:send_frame/2')
%%      resolves a node only through an EXISTING connection and never
%%      dials, so a newly-learned peer answered `no_route' regardless of
%%      whether its addresses were populated. `macula_dht' (this app)
%%      has no dependency on `macula_station' (home of the actual
%%      dialer, `macula_station_dht_dialer'), so this walk could not call
%%      it directly. Fixed via dependency injection — an optional `dial'
%%      callback in `opts()' (default a no-op, so any existing caller
%%      that does not pass one keeps exactly its old one-hop-wide
%%      behaviour) — the same shape `send_frame' itself is injected with
%%      at `macula_station_app' boot. `macula_station_bootstrap_runner'
%%      (the one production caller, via a station's own self-lookup at
%%      boot) now passes `dial => fun macula_station_dht_dialer:ensure_dialed/3'.
%%
%% `observe_async' deliberately, not `observe': this runs on the lookup
%% coordinator, which may be the DHT process itself, and a synchronous
%% call would deadlock. A cast also keeps the walk off the routing
%% table's critical path.
-spec observe_fresh(macula_dht:dht(), [macula_frame:station_ref()]) -> ok.
observe_fresh(_Dht, []) ->
    ok;
observe_fresh(Dht, Fresh) ->
    lists:foreach(fun(R) -> macula_dht:observe_async(Dht, ref_to_spec(R)) end,
                  Fresh).

%% The inverse the codebase was missing: a received `station_ref' back
%% into a routing-table `spec'.
%%
%% The ref does carry the sender's `asn' and `country', so those two
%% fields are not the fabricated `asn => 0' that bootstrap-ingested
%% entries get. Do NOT read that as "the entry is fully populated":
%% `addresses' is hardcoded empty by
%% `macula_dht_protocol:entry_to_station_ref/2', so `endpoints' below is
%% always `[]' and the entry cannot be dialled. Real metadata, no route.
-spec ref_to_spec(macula_frame:station_ref()) -> macula_dht_entry:spec().
ref_to_spec(#{node_id := Id} = Ref) ->
    #{
        node_id   => Id,
        endpoints => maps:get(addresses, Ref, []),
        asn       => ref_asn(Ref),
        country   => maps:get(country, Ref, <<"??">>),
        tier      => ref_tier(Ref)
    }.

%% `asn' is `non_neg_integer() | undefined' on the wire; the routing
%% table requires an integer.
ref_asn(#{asn := A}) when is_integer(A) -> A;
ref_asn(_)                              -> 0.

%% Wire tier is 0..4; the routing table's is t0..t3, so 4 caps at t3 —
%% the same cap `macula_station_bootstrap:dht_tier/1' already applies.
ref_tier(#{tier := 0}) -> t0;
ref_tier(#{tier := 1}) -> t1;
ref_tier(#{tier := 2}) -> t2;
ref_tier(#{tier := 3}) -> t3;
ref_tier(#{tier := 4}) -> t3;
ref_tier(_)            -> t0.

-spec add_to_shortlist(path(), [macula_frame:station_ref()],
                       macula_dht_xor:id()) -> path().
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

-spec id_of(macula_frame:station_ref()) -> macula_identity:pubkey().
id_of(#{node_id := Id}) -> Id.

-spec sort_by_distance([macula_frame:station_ref()], macula_dht_xor:id()) ->
          [macula_frame:station_ref()].
sort_by_distance(Refs, Key) ->
    Keyed = [{macula_dht_xor:distance_int(Key, id_of(R)), R} || R <- Refs],
    [R || {_, R} <- lists:keysort(1, Keyed)].

-spec dedup_by_id([macula_frame:station_ref()]) ->
          [macula_frame:station_ref()].
dedup_by_id(Refs) ->
    Map = lists:foldl(fun(R, Acc) -> Acc#{id_of(R) => R} end, #{}, Refs),
    maps:values(Map).
