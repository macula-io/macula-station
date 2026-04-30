%% @doc Periodic observability for the DHT — bucket diversity,
%% replica diversity, and the headline counters from Part 3 §13.
%%
%% On every tick the monitor:
%%
%% <ol>
%%   <li>Asks the DHT server for routing-table size, bucket count,
%%       sibling count, and total record count.</li>
%%   <li>Walks every populated bucket and asks `macula_dht_diversity'
%%       which of the §4.3 soft constraints (≥5 ASN / ≥3 country /
%%       ≥2 tier) are met. Tallies violations into a bucket
%%       report.</li>
%%   <li>Walks every owned record (envelope key matches `self_id'),
%%       runs `macula_dht_placement:place/3' against the current
%%       k-closest set, and reports the resulting `degraded' flag
%%       plus the diversity counts. This is the §13 "replica
%%       diversity score" metric.</li>
%%   <li>Emits a single `_hecate.dht.metrics' event via
%%       `macula_diagnostics' carrying the full report, plus
%%       individual gauges for the headline numbers so dashboards
%%       can track them directly.</li>
%% </ol>
%%
%% The monitor's process dictionary holds the gauges, so callers
%% read them back via `gauges/1' rather than touching the calling
%% process's dictionary.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §4.3, §5.2,
%% §13; plans/PLAN_PHASE_3_BREAKDOWN.md Session 3.11.
-module(macula_dht_monitor).
-behaviour(gen_server).

-export([start_link/1, stop/1]).
-export([tick/1, report/1, gauges/1, stats/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, report/0, bucket_report/0, replica_report/0,
              replica_summary/0, stats/0]).

-define(DEFAULT_INTERVAL_MS, 60_000).   %% 1 min
-define(DEFAULT_K,                20).
-define(EVENT_TOPIC, <<"_hecate.dht.metrics">>).

-type opts() :: #{
    dht         := macula_dht:dht(),
    interval_ms => pos_integer(),
    k           => pos_integer(),
    self_id     => macula_identity:pubkey()
}.

-type bucket_report() :: #{
    populated              := non_neg_integer(),
    diverse                := non_neg_integer(),
    asn_violations         := non_neg_integer(),
    country_violations     := non_neg_integer(),
    tier_violations        := non_neg_integer()
}.

-type replica_summary() :: #{
    storage_key := macula_dht_xor:id(),
    chosen      := non_neg_integer(),
    degraded    := boolean(),
    counts      := macula_dht_placement:counts()
}.

-type replica_report() :: #{
    owned_records       := non_neg_integer(),
    placement_summaries := [replica_summary()]
}.

-type report() :: #{
    self_id       := macula_identity:pubkey(),
    rt_size       := non_neg_integer(),
    bucket_count  := non_neg_integer(),
    sibling_count := non_neg_integer(),
    record_count  := non_neg_integer(),
    buckets       := bucket_report(),
    replicas      := replica_report()
}.

-type stats() :: #{
    ticks      := non_neg_integer(),
    last_tick  := integer() | undefined
}.

-record(state, {
    dht         :: macula_dht:dht(),
    self_id     :: macula_identity:pubkey(),
    interval_ms :: pos_integer(),
    k           :: pos_integer(),
    ticks       :: non_neg_integer(),
    last_tick   :: integer() | undefined
}).

%%=====================================================================
%% Public API
%%=====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{dht := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Run a sample synchronously: build the report, emit the
%% diagnostics event, update gauges, and return the report.
-spec tick(pid()) -> report().
tick(Pid) -> gen_server:call(Pid, tick, infinity).

%% @doc Build the report without emitting events or touching gauges.
-spec report(pid()) -> report().
report(Pid) -> gen_server:call(Pid, report).

%% @doc Snapshot the gauge values held in the monitor's process
%% dictionary.
-spec gauges(pid()) -> [macula_diagnostics:sample()].
gauges(Pid) -> gen_server:call(Pid, gauges).

-spec stats(pid()) -> stats().
stats(Pid) -> gen_server:call(Pid, stats).

%%=====================================================================
%% gen_server callbacks
%%=====================================================================

init(#{dht := Dht} = Opts) ->
    Interval = maps:get(interval_ms, Opts, ?DEFAULT_INTERVAL_MS),
    SelfId   = maps:get(self_id, Opts, macula_dht:self_id(Dht)),
    State = #state{
        dht         = Dht,
        self_id     = SelfId,
        interval_ms = Interval,
        k           = maps:get(k, Opts, ?DEFAULT_K),
        ticks       = 0,
        last_tick   = undefined
    },
    _ = schedule_next(Interval),
    {ok, State}.

handle_call(tick, _From, State) ->
    Report = run_tick(State),
    {reply, Report, advance(State)};
handle_call(report, _From, State) ->
    {reply, build_report(State), State};
handle_call(gauges, _From, State) ->
    {reply, macula_diagnostics:snapshot(), State};
handle_call(stats, _From, State) ->
    {reply, build_stats(State), State};
handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(monitor_tick, #state{interval_ms = I} = State) ->
    _ = run_tick(State),
    _ = schedule_next(I),
    {noreply, advance(State)};
handle_info(_Msg, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%=====================================================================
%% Tick logic
%%=====================================================================

-spec schedule_next(pos_integer()) -> reference().
schedule_next(IntervalMs) ->
    erlang:send_after(IntervalMs, self(), monitor_tick).

-spec run_tick(#state{}) -> report().
run_tick(State) ->
    Report = build_report(State),
    publish(Report),
    Report.

-spec advance(#state{}) -> #state{}.
advance(#state{ticks = N} = State) ->
    State#state{ticks = N + 1, last_tick = erlang:system_time(millisecond)}.

-spec build_stats(#state{}) -> stats().
build_stats(#state{ticks = N, last_tick = L}) ->
    #{ticks => N, last_tick => L}.

%%=====================================================================
%% Report builders
%%=====================================================================

-spec build_report(#state{}) -> report().
build_report(#state{dht = Dht, self_id = SelfId} = State) ->
    Stats = macula_dht:stats(Dht),
    #{
        self_id       => SelfId,
        rt_size       => maps:get(size, Stats),
        bucket_count  => maps:get(bucket_count, Stats),
        sibling_count => maps:get(sibling_count, Stats),
        record_count  => macula_dht:record_count(Dht),
        buckets       => bucket_report(Dht),
        replicas      => replica_report(State)
    }.

-spec bucket_report(macula_dht:dht()) -> bucket_report().
bucket_report(Dht) ->
    Buckets = macula_dht:populated_buckets(Dht),
    Reports = [macula_dht_diversity:bucket_constraints_met(
                 macula_dht_bucket:members(B))
               || {_Ix, B} <- Buckets],
    AsnViol     = count(false, asn_ok,     Reports),
    CountryViol = count(false, country_ok, Reports),
    TierViol    = count(false, tier_ok,    Reports),
    Diverse     = length([1 || R <- Reports, all_ok(R)]),
    #{
        populated          => length(Reports),
        diverse            => Diverse,
        asn_violations     => AsnViol,
        country_violations => CountryViol,
        tier_violations    => TierViol
    }.

-spec all_ok(map()) -> boolean().
all_ok(#{asn_ok := A, country_ok := C, tier_ok := T}) ->
    A andalso C andalso T.

-spec count(boolean(), atom(), [map()]) -> non_neg_integer().
count(Match, Field, Reports) ->
    length([1 || R <- Reports, maps:get(Field, R) =:= Match]).

-spec replica_report(#state{}) -> replica_report().
replica_report(#state{dht = Dht, self_id = SelfId, k = K}) ->
    Records = macula_dht:list_records(Dht),
    Owned   = [R || R <- Records, macula_record:key(R) =:= SelfId],
    Summaries = [replica_summary(R, Dht, K) || R <- Owned],
    #{owned_records => length(Owned),
      placement_summaries => Summaries}.

-spec replica_summary(macula_record:record(), macula_dht:dht(),
                      pos_integer()) -> replica_summary().
replica_summary(Record, Dht, K) ->
    StorageKey = macula_record:storage_key(Record),
    Entries    = macula_dht:k_closest(Dht, StorageKey, K),
    Refs       = [macula_dht_protocol:entry_to_station_ref(E) || E <- Entries],
    Placement  = macula_dht_placement:place(
                   StorageKey, Refs,
                   macula_dht_placement:default_constraints()),
    #{
        storage_key => StorageKey,
        chosen      => length(maps:get(chosen, Placement)),
        degraded    => maps:get(degraded, Placement),
        counts      => maps:get(counts, Placement)
    }.

%%=====================================================================
%% Diagnostics emission
%%=====================================================================

-spec publish(report()) -> ok.
publish(Report) ->
    macula_diagnostics:event(?EVENT_TOPIC, Report),
    set_gauges(Report).

-spec set_gauges(report()) -> ok.
set_gauges(Report) ->
    Buckets = maps:get(buckets, Report),
    Replicas = maps:get(replicas, Report),
    gauge(<<"hecate.dht.rt_size">>,            maps:get(rt_size, Report)),
    gauge(<<"hecate.dht.bucket_count">>,       maps:get(bucket_count, Report)),
    gauge(<<"hecate.dht.sibling_count">>,      maps:get(sibling_count, Report)),
    gauge(<<"hecate.dht.record_count">>,       maps:get(record_count, Report)),
    gauge(<<"hecate.dht.buckets.populated">>,  maps:get(populated, Buckets)),
    gauge(<<"hecate.dht.buckets.diverse">>,    maps:get(diverse, Buckets)),
    gauge(<<"hecate.dht.buckets.asn_violations">>,
          maps:get(asn_violations, Buckets)),
    gauge(<<"hecate.dht.buckets.country_violations">>,
          maps:get(country_violations, Buckets)),
    gauge(<<"hecate.dht.buckets.tier_violations">>,
          maps:get(tier_violations, Buckets)),
    gauge(<<"hecate.dht.replicas.owned_records">>,
          maps:get(owned_records, Replicas)),
    gauge(<<"hecate.dht.replicas.degraded">>,
          length([1 || S <- maps:get(placement_summaries, Replicas),
                       maps:get(degraded, S)])),
    ok.

-spec gauge(binary(), number()) -> ok.
gauge(Name, V) -> macula_diagnostics:metric(Name, gauge, V).
