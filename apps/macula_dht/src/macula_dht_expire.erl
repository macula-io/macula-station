%% @doc Custodian expiry reaper (tExpire, Part 3 §5.1 / §11).
%%
%% Every `interval_ms' (default 1 h — well below the
%% `T_EXPIRE_MS = 48h' record TTL), walk the local store and evict
%% any record whose `expires_at' has passed.
%%
%% This is strictly a <em>custodian</em> responsibility: drop the
%% local replica so stale records don't accumulate. The
%% complementary action — publishing a `tombstone' record that
%% supersedes a deliberately revoked record — is the <em>owner's</em>
%% job and is not handled here.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §5.1, §11;
%% plans/PLAN_PHASE_3_BREAKDOWN.md Session 3.10.
-module(macula_dht_expire).
-behaviour(gen_server).

-export([start_link/1, stop/1, tick/1, stats/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, outcome/0, stats/0]).

-define(DEFAULT_INTERVAL_MS, 3_600_000).   %% 1 h reap cadence

-type opts() :: #{
    dht         := macula_dht:dht(),
    interval_ms => pos_integer()
}.

-type outcome() :: #{
    records_seen := non_neg_integer(),
    expired      := non_neg_integer(),
    kept         := non_neg_integer()
}.

-type stats() :: #{
    ticks      := non_neg_integer(),
    last_tick  := integer() | undefined,
    cumulative := outcome()
}.

-record(state, {
    dht         :: macula_dht:dht(),
    interval_ms :: pos_integer(),
    ticks       :: non_neg_integer(),
    last_tick   :: integer() | undefined,
    cumulative  :: outcome()
}).

%%=====================================================================
%% Public API
%%=====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{dht := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

-spec tick(pid()) -> outcome().
tick(Pid) -> gen_server:call(Pid, tick, infinity).

-spec stats(pid()) -> stats().
stats(Pid) -> gen_server:call(Pid, stats).

%%=====================================================================
%% gen_server callbacks
%%=====================================================================

init(#{dht := Dht} = Opts) ->
    Interval = maps:get(interval_ms, Opts, ?DEFAULT_INTERVAL_MS),
    State = #state{
        dht         = Dht,
        interval_ms = Interval,
        ticks       = 0,
        last_tick   = undefined,
        cumulative  = zero_outcome()
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

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(expire_tick, #state{interval_ms = I} = State) ->
    {_Outcome, NewState} = run_tick(State),
    _ = schedule_next(I),
    {noreply, NewState};
handle_info(_Msg, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%=====================================================================
%% Tick execution
%%=====================================================================

-spec schedule_next(pos_integer()) -> reference().
schedule_next(IntervalMs) ->
    erlang:send_after(IntervalMs, self(), expire_tick).

-spec run_tick(#state{}) -> {outcome(), #state{}}.
run_tick(#state{dht = Dht} = State) ->
    Now     = erlang:system_time(millisecond),
    Records = macula_dht:list_records(Dht),
    Outcome = lists:foldl(
                fun(R, Acc) -> process_record(R, Now, Dht, Acc) end,
                zero_outcome(), Records),
    {Outcome, advance(State, Outcome)}.

-spec process_record(macula_record:m_record(), integer(),
                     macula_dht:dht(), outcome()) -> outcome().
process_record(Record, Now, Dht, Acc) ->
    reap_or_keep(macula_record:expires_at(Record) =< Now,
                 Record, Dht, bump(Acc, records_seen)).

-spec reap_or_keep(boolean(), macula_record:m_record(),
                   macula_dht:dht(), outcome()) -> outcome().
reap_or_keep(true, Record, Dht, Acc) ->
    ok = macula_dht:delete_record(Dht, Record),
    bump(Acc, expired);
reap_or_keep(false, _Record, _Dht, Acc) ->
    bump(Acc, kept).

%%=====================================================================
%% Stats helpers
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
    #{records_seen => 0, expired => 0, kept => 0}.

-spec merge(outcome(), outcome()) -> outcome().
merge(A, B) ->
    maps:fold(fun(K, V, Acc) -> bump(Acc, K, V) end, A, B).

-spec bump(outcome(), atom()) -> outcome().
bump(Map, Key) -> bump(Map, Key, 1).

-spec bump(outcome(), atom(), non_neg_integer()) -> outcome().
bump(Map, Key, N) ->
    maps:update_with(Key, fun(V) -> V + N end, N, Map).
