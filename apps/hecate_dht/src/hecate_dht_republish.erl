%% @doc Owner republish loop (tRepublish, Part 3 §5.5 / §11).
%%
%% Every `interval_ms' (default 24h), walk local records whose
%% envelope key matches the node's `self_id' — i.e. records <em>we
%% own</em> — refresh each one (new UUIDv7 version, new created_at
%% and expires_at, re-signed with our identity), update the local
%% store, and re-STORE to the current k-closest custodians via the
%% full `hecate_dht_store:store/3' placement + quorum orchestrator.
%%
%% This is the owner obligation. It is orthogonal to tReplicate
%% (Session 3.9), which is the <em>custodian</em> obligation to
%% refresh all records it holds (owned or not) to their current
%% k-closest set.
%%
%% == Outcome classification ==
%%
%% Each owned record's republish outcome is one of:
%% <ul>
%%   <li>`ok' — quorum of acks reached (default ≥ 16 of K).</li>
%%   <li>`quorum_not_met' — published to some custodians but under
%%       quorum.</li>
%%   <li>`no_candidates' — no peers in the routing table to publish
%%       to; record stays local only.</li>
%% </ul>
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §5.5, §11;
%% plans/PLAN_PHASE_3_BREAKDOWN.md Session 3.10.
-module(hecate_dht_republish).
-behaviour(gen_server).

-export([start_link/1, stop/1, tick/1, stats/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, outcome/0, stats/0]).

-define(DEFAULT_INTERVAL_MS, 86_400_000).  %% 24 h

-type opts() :: #{
    dht         := hecate_dht:dht(),
    identity    := macula_identity:key_pair(),
    interval_ms => pos_integer(),
    store_opts  => hecate_dht_store:opts()
}.

-type outcome() :: #{
    records_seen    := non_neg_integer(),
    owned           := non_neg_integer(),
    republished     := non_neg_integer(),
    quorum_not_met  := non_neg_integer(),
    no_candidates   := non_neg_integer()
}.

-type stats() :: #{
    ticks      := non_neg_integer(),
    last_tick  := integer() | undefined,
    cumulative := outcome()
}.

-record(state, {
    dht         :: hecate_dht:dht(),
    identity    :: macula_identity:key_pair(),
    self_id     :: macula_identity:pubkey(),
    interval_ms :: pos_integer(),
    store_opts  :: hecate_dht_store:opts(),
    ticks       :: non_neg_integer(),
    last_tick   :: integer() | undefined,
    cumulative  :: outcome()
}).

%%=====================================================================
%% Public API
%%=====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{dht := _, identity := _} = Opts) ->
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

init(#{dht := Dht, identity := Identity} = Opts) ->
    Interval = maps:get(interval_ms, Opts, ?DEFAULT_INTERVAL_MS),
    State = #state{
        dht         = Dht,
        identity    = Identity,
        self_id     = macula_identity:public(Identity),
        interval_ms = Interval,
        store_opts  = maps:get(store_opts, Opts, default_store_opts()),
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

handle_info(republish_tick, #state{interval_ms = I} = State) ->
    {_Outcome, NewState} = run_tick(State),
    _ = schedule_next(I),
    {noreply, NewState};
handle_info(_Msg, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%=====================================================================
%% Tick execution
%%=====================================================================

-spec default_store_opts() -> hecate_dht_store:opts().
default_store_opts() ->
    #{store_timeout_ms => 3_000,
      overall_timeout_ms => 8_000}.

-spec schedule_next(pos_integer()) -> reference().
schedule_next(IntervalMs) ->
    erlang:send_after(IntervalMs, self(), republish_tick).

-spec run_tick(#state{}) -> {outcome(), #state{}}.
run_tick(#state{dht = Dht, self_id = SelfId} = State) ->
    Records = hecate_dht:list_records(Dht),
    Outcome = lists:foldl(
                fun(R, Acc) -> process_record(R, SelfId, State, Acc) end,
                zero_outcome(), Records),
    {Outcome, advance(State, Outcome)}.

-spec process_record(macula_record:record(), macula_identity:pubkey(),
                     #state{}, outcome()) -> outcome().
process_record(Record, SelfId, State, Acc0) ->
    Acc = bump(Acc0, records_seen),
    maybe_republish(macula_record:key(Record) =:= SelfId,
                    Record, State, Acc).

-spec maybe_republish(boolean(), macula_record:record(),
                      #state{}, outcome()) -> outcome().
maybe_republish(false, _Record, _State, Acc) ->
    Acc;
maybe_republish(true, Record,
                #state{dht = Dht, identity = Identity,
                       store_opts = StoreOpts}, Acc) ->
    Fresh = macula_record:refresh(Record, Identity),
    ok    = hecate_dht:put_record(Dht, Fresh),
    account(hecate_dht:store(Dht, Fresh, StoreOpts), bump(Acc, owned)).

-spec account(hecate_dht_store:result(), outcome()) -> outcome().
account({ok, _Outcome},                    Acc) -> bump(Acc, republished);
account({error, quorum_not_met, _Outcome}, Acc) -> bump(Acc, quorum_not_met);
account({error, no_candidates,  _Outcome}, Acc) -> bump(Acc, no_candidates).

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
    #{records_seen => 0, owned => 0, republished => 0,
      quorum_not_met => 0, no_candidates => 0}.

-spec merge(outcome(), outcome()) -> outcome().
merge(A, B) ->
    maps:fold(fun(K, V, Acc) -> bump(Acc, K, V) end, A, B).

-spec bump(outcome(), atom()) -> outcome().
bump(Map, Key) -> bump(Map, Key, 1).

-spec bump(outcome(), atom(), non_neg_integer()) -> outcome().
bump(Map, Key, N) ->
    maps:update_with(Key, fun(V) -> V + N end, N, Map).
