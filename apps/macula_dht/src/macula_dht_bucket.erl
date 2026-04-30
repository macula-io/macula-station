%% @doc A single Kademlia `k'-bucket — a bounded set of routing-table
%% entries that share a common XOR-prefix with the local node.
%%
%% Buckets are pure values: operations return a new bucket, never
%% mutate. State (ETS, gen_server) lives higher up in
%% `macula_dht_routing_table' / `macula_dht_server'.
%%
%% == Admission algorithm ==
%%
%% When a fresh candidate arrives at a full bucket, every member and the
%% candidate are *scored*. The lowest-scoring member is evicted iff the
%% candidate out-scores it. Score combines four signals:
%%
%% <ul>
%%   <li>`W_UPTIME' × `uptime_frac' — long-term reliability</li>
%%   <li>`W_NOVELTY' × `novelty/3' — ASN/country/tier diversification</li>
%%   <li>`W_INCUMBENCY' × `age_in_table' — small tenure bonus</li>
%%   <li>`-W_LATENCY' × `latency_frac' — latency penalty</li>
%% </ul>
%%
%% Weights are chosen so `novelty' never dominates `uptime'. A peer on a
%% unique ASN + country + tier beats a duplicate only when both have
%% comparable uptime — this prevents Sybils from eclipsing stable peers
%% merely by being on new ASNs.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §4.3 (tier-diverse
%% bucket admission).
-module(macula_dht_bucket).

-export([
    new/0,
    new/1,
    capacity/1,
    size/1,
    members/1,
    find/2,
    contains/2,
    insert/2,
    insert/3,
    remove/2,
    touch/3,
    score/3
]).

-export_type([bucket/0, insert_result/0]).

-define(DEFAULT_K,          20).
-define(W_UPTIME,          1.0).
-define(W_NOVELTY,         0.8).
-define(W_INCUMBENCY,      0.3).
-define(W_LATENCY,         0.5).
-define(LATENCY_FLOOR_MS,   20).
-define(LATENCY_CEIL_MS,   500).
-define(INCUMBENCY_HORIZON_MS, 86_400_000). %% 24h

-type bucket() :: #{
    capacity := pos_integer(),
    entries  := [macula_dht_entry:entry()]
}.

-type insert_result() ::
      {admitted, bucket()}
    | {touched,  bucket()}
    | {replaced, Evicted :: macula_dht_entry:entry(), bucket()}
    | {rejected, bucket()}.

%%=====================================================================
%% Construction
%%=====================================================================

-spec new() -> bucket().
new() ->
    new(?DEFAULT_K).

-spec new(pos_integer()) -> bucket().
new(K) when is_integer(K), K >= 1 ->
    #{capacity => K, entries => []}.

%%=====================================================================
%% Inspection
%%=====================================================================

-spec capacity(bucket()) -> pos_integer().
capacity(#{capacity := K}) -> K.

-spec size(bucket()) -> non_neg_integer().
size(#{entries := Es}) -> length(Es).

-spec members(bucket()) -> [macula_dht_entry:entry()].
members(#{entries := Es}) -> Es.

-spec find(macula_dht_xor:id(), bucket()) ->
        {ok, macula_dht_entry:entry()} | error.
find(Id, #{entries := Es}) ->
    find_by_id(Id, Es).

-spec contains(macula_dht_xor:id(), bucket()) -> boolean().
contains(Id, Bucket) ->
    case find(Id, Bucket) of
        {ok, _} -> true;
        error   -> false
    end.

%%=====================================================================
%% Insertion
%%=====================================================================

-spec insert(macula_dht_entry:entry(), bucket()) -> insert_result().
insert(Cand, Bucket) ->
    insert(Cand, Bucket, erlang:monotonic_time(millisecond)).

-spec insert(macula_dht_entry:entry(), bucket(), integer()) -> insert_result().
insert(Cand, Bucket, Now) ->
    Id = macula_dht_entry:node_id(Cand),
    dispatch_insert(find(Id, Bucket), Cand, Bucket, Now).

-spec dispatch_insert({ok, macula_dht_entry:entry()} | error,
                      macula_dht_entry:entry(), bucket(), integer()) ->
          insert_result().
dispatch_insert({ok, _Existing}, Cand, Bucket, Now) ->
    Id = macula_dht_entry:node_id(Cand),
    {touched, replace(Id, fun(E) -> macula_dht_entry:touch(E, Now) end, Bucket)};
dispatch_insert(error, Cand, Bucket, Now) ->
    insert_new(Cand, Bucket, Now).

-spec insert_new(macula_dht_entry:entry(), bucket(), integer()) ->
          insert_result().
insert_new(Cand, #{entries := Es, capacity := K} = B, _Now) when length(Es) < K ->
    {admitted, B#{entries := [Cand | Es]}};
insert_new(Cand, #{entries := Es} = B, Now) ->
    maybe_evict(Cand, weakest(Es, Now), score(Cand, Es, Now), B).

-spec maybe_evict(macula_dht_entry:entry(),
                  {float(), macula_dht_entry:entry()},
                  float(), bucket()) -> insert_result().
maybe_evict(_Cand, {MinScore, _Weakest}, CandScore, B)
  when CandScore =< MinScore ->
    {rejected, B};
maybe_evict(Cand, {_MinScore, Weakest}, _CandScore, #{entries := Es} = B) ->
    WeakId = macula_dht_entry:node_id(Weakest),
    Remaining = [E || E <- Es, macula_dht_entry:node_id(E) =/= WeakId],
    {replaced, Weakest, B#{entries := [Cand | Remaining]}}.

%%=====================================================================
%% Removal / touch
%%=====================================================================

-spec remove(macula_dht_xor:id(), bucket()) -> bucket().
remove(Id, #{entries := Es} = B) ->
    B#{entries := [E || E <- Es, macula_dht_entry:node_id(E) =/= Id]}.

-spec touch(macula_dht_xor:id(), bucket(), integer()) -> bucket().
touch(Id, Bucket, Now) ->
    replace(Id, fun(E) -> macula_dht_entry:touch(E, Now) end, Bucket).

%%=====================================================================
%% Scoring (exported for tests and for the routing table's tie-break
%% logic during bucket splits)
%%=====================================================================

-spec score(macula_dht_entry:entry(), [macula_dht_entry:entry()], integer()) ->
          float().
score(E, Reference, Now) ->
    Others = [X || X <- Reference,
                   macula_dht_entry:node_id(X) =/= macula_dht_entry:node_id(E)],
    (?W_UPTIME     * macula_dht_entry:uptime(E))
      + (?W_NOVELTY    * (macula_dht_diversity:novelty_score(E, Others) / 3.0))
      + (?W_INCUMBENCY * incumbency(E, Now))
      - (?W_LATENCY    * latency_frac(macula_dht_entry:latency(E))).

%%=====================================================================
%% Internal
%%=====================================================================

-spec find_by_id(macula_dht_xor:id(), [macula_dht_entry:entry()]) ->
          {ok, macula_dht_entry:entry()} | error.
find_by_id(_Id, []) -> error;
find_by_id(Id, [E | Rest]) ->
    find_or_continue(macula_dht_entry:node_id(E) =:= Id, E, Id, Rest).

-spec find_or_continue(boolean(), macula_dht_entry:entry(),
                       macula_dht_xor:id(), [macula_dht_entry:entry()]) ->
          {ok, macula_dht_entry:entry()} | error.
find_or_continue(true,  E, _Id, _Rest) -> {ok, E};
find_or_continue(false, _, Id, Rest)   -> find_by_id(Id, Rest).

-spec replace(macula_dht_xor:id(),
              fun((macula_dht_entry:entry()) -> macula_dht_entry:entry()),
              bucket()) -> bucket().
replace(Id, F, #{entries := Es} = B) ->
    B#{entries := [maybe_replace(Id, F, E) || E <- Es]}.

-spec maybe_replace(macula_dht_xor:id(),
                    fun((macula_dht_entry:entry()) -> macula_dht_entry:entry()),
                    macula_dht_entry:entry()) -> macula_dht_entry:entry().
maybe_replace(Id, F, E) ->
    apply_if_match(macula_dht_entry:node_id(E) =:= Id, F, E).

-spec apply_if_match(boolean(),
                     fun((macula_dht_entry:entry()) -> macula_dht_entry:entry()),
                     macula_dht_entry:entry()) -> macula_dht_entry:entry().
apply_if_match(true,  F, E) -> F(E);
apply_if_match(false, _, E) -> E.

-spec weakest([macula_dht_entry:entry(), ...], integer()) ->
          {float(), macula_dht_entry:entry()}.
weakest(Es, Now) ->
    Scored = [{score(E, Es, Now), E} || E <- Es],
    lists:foldl(fun pick_min/2, hd(Scored), tl(Scored)).

-spec pick_min({float(), macula_dht_entry:entry()},
               {float(), macula_dht_entry:entry()}) ->
          {float(), macula_dht_entry:entry()}.
pick_min({S, _} = A, {Best, _} = _B) when S < Best -> A;
pick_min(_A, B) -> B.

-spec incumbency(macula_dht_entry:entry(), integer()) -> float().
incumbency(E, Now) ->
    Age = macula_dht_entry:time_in_table_ms(E, Now),
    min(1.0, Age / ?INCUMBENCY_HORIZON_MS).

-spec latency_frac(non_neg_integer() | undefined) -> float().
latency_frac(undefined) -> 1.0;
latency_frac(Ms) when Ms =< ?LATENCY_FLOOR_MS -> 0.0;
latency_frac(Ms) when Ms >= ?LATENCY_CEIL_MS -> 1.0;
latency_frac(Ms) ->
    (Ms - ?LATENCY_FLOOR_MS) / (?LATENCY_CEIL_MS - ?LATENCY_FLOOR_MS).
