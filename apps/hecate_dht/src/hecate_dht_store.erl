%% @doc Orchestrator for STOREing a record to its k-closest
%% custodians with diversity-aware placement and quorum acks
%% (Part 3 §5.2, §5.6).
%%
%% Runs in the caller's process. Three stages:
%%
%% <ol>
%%   <li><strong>Candidate resolution</strong> — either takes an
%%       explicit `candidates' list from opts, or derives one by
%%       reading the `k * placement_factor' closest entries from
%%       the local routing table (default factor = 2). Full
%%       iterative lookup can be wired in by the caller.</li>
%%   <li><strong>Placement</strong> — `hecate_dht_placement:place/3'
%%       picks up to K replicas satisfying diversity constraints,
%%       reporting `degraded = true' when the pool falls short.</li>
%%   <li><strong>Fan-out</strong> — spawns one unlinked worker per
%%       chosen peer calling `hecate_dht:send_store/4'. Collects
%%       acks until quorum met or overall deadline expires. Workers
%%       that die silently are counted against the overall deadline;
%%       workers that return `{error, _}' are counted as failures.</li>
%% </ol>
%%
%% Result reports the full breakdown so callers can distinguish
%% "quorum met but degraded diversity" from "quorum not met".
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §5.2 + §5.6;
%% plans/PLAN_PHASE_3_BREAKDOWN.md Session 3.8.
-module(hecate_dht_store).

-export([store/2, store/3]).
-export_type([opts/0, result/0, outcome/0]).

-define(DEFAULT_K,                   20).
-define(DEFAULT_QUORUM,              16).
-define(DEFAULT_PLACEMENT_FACTOR,     2).
-define(DEFAULT_STORE_TIMEOUT_MS, 5_000).
-define(DEFAULT_OVERALL_TIMEOUT_MS, 10_000).

-type opts() :: #{
    k                   => pos_integer(),
    quorum              => pos_integer(),
    candidates          => [hecate_frame:station_ref()],
    constraints         => hecate_dht_placement:constraints(),
    placement_factor    => pos_integer(),
    store_timeout_ms    => pos_integer(),
    overall_timeout_ms  => pos_integer()
}.

-type outcome() :: #{
    chosen   := [hecate_frame:station_ref()],
    acks     := non_neg_integer(),
    nacks    := non_neg_integer(),
    timeouts := non_neg_integer(),
    degraded := boolean(),
    counts   := hecate_dht_placement:counts()
}.

-type result() :: {ok, outcome()}
                | {error, no_candidates | quorum_not_met, outcome()}.

%%=====================================================================
%% Public API
%%=====================================================================

-spec store(hecate_dht:dht(), hecate_record:record()) -> result().
store(Dht, Record) ->
    store(Dht, Record, #{}).

-spec store(hecate_dht:dht(), hecate_record:record(), opts()) -> result().
store(Dht, Record, Opts) when is_map(Record), is_map(Opts) ->
    Key         = hecate_record:storage_key(Record),
    K           = maps:get(k, Opts, ?DEFAULT_K),
    Quorum      = maps:get(quorum, Opts, ?DEFAULT_QUORUM),
    Constraints = resolve_constraints(Opts, K),
    Candidates  = resolve_candidates(Dht, Key, K, Opts),
    Placement   = hecate_dht_placement:place(Key, Candidates, Constraints),
    dispatch_placement(Dht, Record, Placement, Quorum, Opts).

%%=====================================================================
%% Candidate resolution
%%=====================================================================

-spec resolve_constraints(opts(), pos_integer()) ->
          hecate_dht_placement:constraints().
resolve_constraints(Opts, K) ->
    Default = hecate_dht_placement:default_constraints(),
    Custom  = maps:get(constraints, Opts, #{}),
    maps:merge(Default#{k := K}, Custom).

-spec resolve_candidates(hecate_dht:dht(), hecate_dht_xor:id(),
                         pos_integer(), opts()) ->
          [hecate_frame:station_ref()].
resolve_candidates(Dht, Key, K, Opts) ->
    pick_candidates(maps:find(candidates, Opts), Dht, Key, K, Opts).

-spec pick_candidates({ok, [hecate_frame:station_ref()]} | error,
                      hecate_dht:dht(), hecate_dht_xor:id(),
                      pos_integer(), opts()) ->
          [hecate_frame:station_ref()].
pick_candidates({ok, Explicit}, _Dht, _Key, _K, _Opts) ->
    Explicit;
pick_candidates(error, Dht, Key, K, Opts) ->
    Factor = maps:get(placement_factor, Opts, ?DEFAULT_PLACEMENT_FACTOR),
    Entries = hecate_dht:k_closest(Dht, Key, K * Factor),
    [hecate_dht_protocol:entry_to_station_ref(E) || E <- Entries].

%%=====================================================================
%% Dispatch + ack collection
%%=====================================================================

-spec dispatch_placement(hecate_dht:dht(), hecate_record:record(),
                         hecate_dht_placement:placement_result(),
                         pos_integer(), opts()) -> result().
dispatch_placement(_Dht, _Record, #{chosen := []} = Placement, _Quorum,
                   _Opts) ->
    {error, no_candidates, outcome_of(Placement, 0, 0, 0)};
dispatch_placement(Dht, Record, Placement, Quorum, Opts) ->
    Chosen    = maps:get(chosen, Placement),
    PerStore  = maps:get(store_timeout_ms, Opts, ?DEFAULT_STORE_TIMEOUT_MS),
    OverallMs = maps:get(overall_timeout_ms, Opts, ?DEFAULT_OVERALL_TIMEOUT_MS),
    Deadline  = erlang:monotonic_time(millisecond) + OverallMs,
    Tag = make_ref(),
    spawn_workers(Dht, Record, Chosen, PerStore, Tag),
    {A, N, T} = collect(length(Chosen), Tag, Deadline, 0, 0, 0),
    Outcome = outcome_of(Placement, A, N, T),
    classify(A, Quorum, Outcome).

-spec spawn_workers(hecate_dht:dht(), hecate_record:record(),
                    [hecate_frame:station_ref()], pos_integer(),
                    reference()) -> ok.
spawn_workers(Dht, Record, Chosen, PerStore, Tag) ->
    Parent = self(),
    lists:foreach(
      fun(Ref) ->
          PeerId = id_of(Ref),
          spawn(fun() ->
              Result = hecate_dht:send_store(Dht, PeerId, Record, PerStore),
              Parent ! {store_result, Tag, PeerId, Result}
          end)
      end, Chosen).

%% @doc Collect until every worker has reported OR the overall
%% deadline expires. No early termination on quorum — that would
%% leak late worker messages into the caller's mailbox and
%% contaminate subsequent calls. The caller's process is typically
%% short-lived (a single store/3 invocation) but in eunit the
%% runner pid is reused across tests, so strict collection matters.
%%
%% Messages are tagged with a fresh reference so only this call's
%% results are received; stray messages from elsewhere are ignored.
-spec collect(non_neg_integer(), reference(), integer(),
              non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
          {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
collect(0, _Tag, _Deadline, Acks, Nacks, Timeouts) ->
    {Acks, Nacks, Timeouts};
collect(Remaining, Tag, Deadline, Acks, Nacks, Timeouts) ->
    Wait = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {store_result, Tag, _PeerId, {ok, #{stored := true}}} ->
            collect(Remaining - 1, Tag, Deadline,
                    Acks + 1, Nacks, Timeouts);
        {store_result, Tag, _PeerId, {ok, #{stored := false}}} ->
            collect(Remaining - 1, Tag, Deadline,
                    Acks, Nacks + 1, Timeouts);
        {store_result, Tag, _PeerId, {error, _}} ->
            collect(Remaining - 1, Tag, Deadline,
                    Acks, Nacks, Timeouts + 1)
    after Wait ->
        {Acks, Nacks, Timeouts}
    end.

-spec outcome_of(hecate_dht_placement:placement_result(),
                 non_neg_integer(), non_neg_integer(),
                 non_neg_integer()) -> outcome().
outcome_of(#{chosen := Chosen, degraded := Degraded, counts := Counts},
           Acks, Nacks, Timeouts) ->
    #{
        chosen   => Chosen,
        acks     => Acks,
        nacks    => Nacks,
        timeouts => Timeouts,
        degraded => Degraded,
        counts   => Counts
    }.

-spec classify(non_neg_integer(), pos_integer(), outcome()) -> result().
classify(Acks, Quorum, Outcome) when Acks >= Quorum ->
    {ok, Outcome};
classify(_Acks, _Quorum, Outcome) ->
    {error, quorum_not_met, Outcome}.

%%=====================================================================
%% Utils
%%=====================================================================

-spec id_of(hecate_frame:station_ref()) -> macula_identity:pubkey().
id_of(#{node_id := Id}) -> Id.
