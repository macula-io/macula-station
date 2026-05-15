%% @doc Custodian replication loop (tReplicate, Part 3 §11.1 / §5.5).
%%
%% Every `interval_ms' (default 1h), iterate every record currently
%% held in the local ETS store and send a STORE to each of the
%% current k-closest peers known for that storage key. Whether a
%% given peer already has the record is irrelevant — STORE is
%% idempotent (the receiver's `store_put' replaces same-owner
%% records by version).
%%
%% Why it exists: churn moves peers in and out of "k-closest" for
%% any given key. Without periodic refresh, a record could end up
%% held only by nodes that are no longer among the k-closest, and
%% new arrivals closer to the key would never learn it — fatal for
%% lookup success. The 1h cadence sits well below the 48h
%% `T_EXPIRE_MS', giving ≥ 48 refresh opportunities before any
%% replica would reach expiry.
%%
%% This is the custodian obligation. The complementary owner
%% obligation (tRepublish, 24h) lands in Session 3.10 along with
%% the expiry reaper.
%%
%% == Usage ==
%%
%% Tests call `tick/1' directly on the replicator to advance the
%% loop synchronously and assert outcomes. Production starts the
%% replicator as a worker child of `macula_dht_sup' (wiring in a
%% later session) and lets the timer drive it.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §5.5, §11.1;
%% plans/PLAN_PHASE_3_BREAKDOWN.md Session 3.9.
-module(macula_dht_replicate).
-behaviour(gen_server).

-export([start_link/1, stop/1, tick/1, replicate_one/2, stats/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, outcome/0, stats/0]).

-define(DEFAULT_INTERVAL_MS,          3_600_000).  %% 1 h
-define(DEFAULT_K,                           20).
-define(DEFAULT_PER_STORE_TIMEOUT_MS,     3_000).

-type opts() :: #{
    dht                  := macula_dht:dht(),
    interval_ms          => pos_integer(),
    k                    => pos_integer(),
    per_store_timeout_ms => pos_integer()
}.

-type outcome() :: #{
    records_seen := non_neg_integer(),
    stores_sent  := non_neg_integer(),
    acks         := non_neg_integer(),
    nacks        := non_neg_integer(),
    timeouts     := non_neg_integer()
}.

-type stats() :: #{
    ticks      := non_neg_integer(),
    last_tick  := integer() | undefined,
    cumulative := outcome()
}.

-record(state, {
    dht                  :: macula_dht:dht(),
    interval_ms          :: pos_integer(),
    k                    :: pos_integer(),
    per_store_timeout_ms :: pos_integer(),
    ticks                :: non_neg_integer(),
    last_tick            :: integer() | undefined,
    cumulative           :: outcome()
}).

%%=====================================================================
%% Public API
%%=====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{dht := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% @doc Run one replication pass synchronously. Returns the outcome
%% of this tick only (not the cumulative stats).
-spec tick(pid()) -> outcome().
tick(Pid) ->
    gen_server:call(Pid, tick, infinity).

%% @doc Eager replication for a single record. Fire-and-forget — the
%% caller (typically the per-station `_dht.put_record' RPC handler)
%% returns to its daemon immediately while the gen_server fans STORE
%% out to the k-closest custodians in the background.
%%
%% This is the on-write half of the durability contract. The
%% periodic tick is the long-period repair sweep (default 1 h);
%% without `replicate_one/2', a record put on station A would only
%% reach station B after the next tick — too slow for any client
%% that puts and immediately reads from a different station.
-spec replicate_one(pid(), macula_record:record()) -> ok.
replicate_one(Pid, Record) when is_map(Record) ->
    gen_server:cast(Pid, {replicate_one, Record}).

-spec stats(pid()) -> stats().
stats(Pid) ->
    gen_server:call(Pid, stats).

%%=====================================================================
%% gen_server callbacks
%%=====================================================================

init(#{dht := Dht} = Opts) ->
    Interval = maps:get(interval_ms, Opts, ?DEFAULT_INTERVAL_MS),
    State = #state{
        dht                  = Dht,
        interval_ms          = Interval,
        k                    = maps:get(k, Opts, ?DEFAULT_K),
        per_store_timeout_ms = maps:get(per_store_timeout_ms, Opts,
                                        ?DEFAULT_PER_STORE_TIMEOUT_MS),
        ticks                = 0,
        last_tick            = undefined,
        cumulative           = zero_outcome()
    },
    _ = schedule_next(Interval),
    {ok, State}.

handle_call(tick, _From, State) ->
    {Outcome, NewState} = run_tick(State),
    {reply, Outcome, NewState};
handle_call(stats, _From, State) ->
    {reply, build_stats(State), State};
handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({replicate_one, Record},
            #state{dht = Dht, k = K,
                   per_store_timeout_ms = Tmo} = State) ->
    %% Spawn a worker for the WHOLE eager-replicate flow, not just
    %% the per-target STORE. `fire_eager_stores' calls
    %% `macula_dht:self_id/1' and `macula_dht:k_closest/3' — both
    %% are sync `gen_server:call' to the (under load) `macula_dht'
    %% server. Doing them inline blocked us between casts under
    %% sustained put-record pressure; under torture-test load the
    %% mailbox grew to 8000+ entries across five stations even
    %% though each subsequent cast looks "cheap". Moving the whole
    %% body into a worker lets the gen_server's mailbox drain at
    %% microsecond rate; the worker still spawns per-target stores
    %% inside `fire_eager_stores' so durability semantics are
    %% unchanged.
    _ = spawn(fun() -> fire_eager_stores(Record, Dht, K, Tmo) end),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(replicate_tick, #state{interval_ms = Interval} = State) ->
    {_Outcome, NewState} = run_tick(State),
    _ = schedule_next(Interval),
    {noreply, NewState};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%=====================================================================
%% Tick execution
%%=====================================================================

-spec schedule_next(pos_integer()) -> reference().
schedule_next(IntervalMs) ->
    erlang:send_after(IntervalMs, self(), replicate_tick).

-spec run_tick(#state{}) -> {outcome(), #state{}}.
run_tick(#state{dht = Dht} = State) ->
    SelfId  = macula_dht:self_id(Dht),
    Records = macula_dht:list_records(Dht),
    Outcome = lists:foldl(
                fun(R, Acc) -> replicate_record(R, SelfId, State, Acc) end,
                zero_outcome(), Records),
    {Outcome, advance(State, Outcome)}.

-spec fire_eager_stores(macula_record:record(), macula_dht:dht(),
                        pos_integer(), pos_integer()) -> ok.
fire_eager_stores(Record, Dht, K, Tmo) ->
    SelfId  = macula_dht:self_id(Dht),
    Key     = macula_record:storage_key(Record),
    Entries = macula_dht:k_closest(Dht, Key, K),
    Targets = [macula_dht_entry:node_id(E)
               || E <- Entries,
                  macula_dht_entry:node_id(E) =/= SelfId],
    _ = [spawn(fun() ->
            _ = macula_dht:send_store(Dht, PeerId, Record, Tmo)
         end) || PeerId <- Targets],
    ok.

-spec replicate_record(macula_record:record(), macula_identity:pubkey(),
                       #state{}, outcome()) -> outcome().
replicate_record(Record, SelfId,
                 #state{dht = Dht, k = K} = State, Acc) ->
    Key     = macula_record:storage_key(Record),
    Entries = macula_dht:k_closest(Dht, Key, K),
    Targets = [macula_dht_entry:node_id(E)
               || E <- Entries,
                  macula_dht_entry:node_id(E) =/= SelfId],
    lists:foldl(fun(PeerId, A) -> send_one(PeerId, Record, State, A) end,
                bump(Acc, records_seen), Targets).

-spec send_one(macula_identity:pubkey(), macula_record:record(),
               #state{}, outcome()) -> outcome().
send_one(PeerId, Record,
         #state{dht = Dht, per_store_timeout_ms = Timeout}, Acc) ->
    Result = macula_dht:send_store(Dht, PeerId, Record, Timeout),
    account(Result, Acc).

-spec account(macula_dht:send_store_result(), outcome()) -> outcome().
account({ok, #{stored := true}},  Acc) ->
    bump(bump(Acc, stores_sent), acks);
account({ok, #{stored := false}}, Acc) ->
    bump(bump(Acc, stores_sent), nacks);
account({error, _},               Acc) ->
    bump(bump(Acc, stores_sent), timeouts).

%%=====================================================================
%% State + stats helpers
%%=====================================================================

-spec advance(#state{}, outcome()) -> #state{}.
advance(#state{ticks = N, cumulative = C} = State, Outcome) ->
    State#state{
        ticks      = N + 1,
        last_tick  = erlang:system_time(millisecond),
        cumulative = merge(C, Outcome)
    }.

-spec build_stats(#state{}) -> stats().
build_stats(#state{ticks = N, last_tick = L, cumulative = C}) ->
    #{ticks => N, last_tick => L, cumulative => C}.

-spec zero_outcome() -> outcome().
zero_outcome() ->
    #{records_seen => 0, stores_sent => 0,
      acks => 0, nacks => 0, timeouts => 0}.

-spec merge(outcome(), outcome()) -> outcome().
merge(A, B) ->
    maps:fold(fun(K, V, Acc) -> bump(Acc, K, V) end, A, B).

-spec bump(outcome(), atom()) -> outcome().
bump(Map, Key) -> bump(Map, Key, 1).

-spec bump(outcome(), atom(), non_neg_integer()) -> outcome().
bump(Map, Key, N) ->
    maps:update_with(Key, fun(V) -> V + N end, N, Map).
