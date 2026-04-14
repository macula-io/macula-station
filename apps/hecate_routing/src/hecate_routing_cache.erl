%% @doc Path cache with 5-min TTL and SWIM-event invalidation
%% (Part 3 §6.3 + Part 4 §6.3).
%%
%% Path computation is non-trivial — Dijkstra over the local
%% routing-table graph plus disjoint-path elimination. Doing it
%% per CALL would burn cycles for nothing on hot procedures. The
%% cache stores the disjoint-path set per destination NodeId and
%% returns the cached set on lookup, evicting on three triggers:
%%
%% <ul>
%%   <li><strong>TTL</strong> — entries older than `ttl_ms'
%%       (default 300_000 = 5 min) are dropped on lookup and
%%       periodically swept by an internal timer.</li>
%%   <li><strong>SWIM event</strong> — when a hop transitions to
%%       `suspect' / `confirmed_failed', the SWIM adapter calls
%%       `invalidate_via_hop/2' which drops every cached entry
%%       whose paths traverse that hop.</li>
%%   <li><strong>Explicit invalidation</strong> — `invalidate/2'
%%       drops a single destination, used when a CALL fails with
%%       `unknown_next_peer' / `temporary_relay_failure' so the
%%       next attempt computes fresh paths.</li>
%% </ul>
%%
%% This module is the SWIM-cache integration point but does not
%% itself subscribe to SWIM events. The hecate_swim adapter
%% (or whatever owns the SWIM membership view) calls
%% `invalidate_via_hop/2' when its view changes.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §6.3;
%% plans/PLAN_PHASE_4_BREAKDOWN.md Session 4.4.
-module(hecate_routing_cache).
-behaviour(gen_server).

-export([start_link/0, start_link/1, stop/1]).
-export([
    lookup/2,
    store/3,
    invalidate/2,
    invalidate_via_hop/2,
    evict_expired/1,
    size/1,
    all_destinations/1,
    stats/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, entry/0, stats/0]).

-define(DEFAULT_TTL_MS,            300_000).   %% 5 min
-define(DEFAULT_SWEEP_INTERVAL_MS,  60_000).   %% 1 min

-type vertex() :: term().
-type path()   :: [vertex(), ...].

-type opts() :: #{
    ttl_ms            => pos_integer(),
    sweep_interval_ms => pos_integer()
}.

-type entry() :: #{
    paths       := [path()],
    inserted_at := integer(),
    hops        := sets:set(vertex())
}.

-type stats() :: #{
    size              := non_neg_integer(),
    ttl_ms            := pos_integer(),
    sweep_interval_ms := pos_integer(),
    last_sweep        := integer() | undefined
}.

-record(state, {
    ttl_ms            :: pos_integer(),
    sweep_interval_ms :: pos_integer(),
    last_sweep        :: integer() | undefined,
    entries = #{}     :: #{vertex() => entry()}
}).

%%=====================================================================
%% Public API
%%=====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) when is_map(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% @doc Look up cached paths for a destination. Returns `miss' on
%% empty cache OR if the entry has expired (in which case it is
%% also dropped).
-spec lookup(pid(), vertex()) -> {ok, [path()]} | miss.
lookup(Pid, Dest) ->
    gen_server:call(Pid, {lookup, Dest}).

%% @doc Insert (or replace) the cached paths for a destination.
%% Resets the TTL clock.
-spec store(pid(), vertex(), [path()]) -> ok.
store(Pid, Dest, Paths) when is_list(Paths) ->
    gen_server:call(Pid, {store, Dest, Paths}).

%% @doc Drop a single destination's entry.
-spec invalidate(pid(), vertex()) -> ok.
invalidate(Pid, Dest) ->
    gen_server:call(Pid, {invalidate, Dest}).

%% @doc Drop every entry whose paths traverse `Hop'. Returns the
%% list of destinations that were evicted (useful for telemetry +
%% test assertions).
-spec invalidate_via_hop(pid(), vertex()) -> [vertex()].
invalidate_via_hop(Pid, Hop) ->
    gen_server:call(Pid, {invalidate_via_hop, Hop}).

%% @doc Sweep expired entries on demand. Returns the list of
%% destinations evicted.
-spec evict_expired(pid()) -> [vertex()].
evict_expired(Pid) ->
    gen_server:call(Pid, evict_expired).

-spec size(pid()) -> non_neg_integer().
size(Pid) ->
    gen_server:call(Pid, size).

-spec all_destinations(pid()) -> [vertex()].
all_destinations(Pid) ->
    gen_server:call(Pid, all_destinations).

-spec stats(pid()) -> stats().
stats(Pid) ->
    gen_server:call(Pid, stats).

%%=====================================================================
%% gen_server callbacks
%%=====================================================================

init(Opts) ->
    Sweep = maps:get(sweep_interval_ms, Opts, ?DEFAULT_SWEEP_INTERVAL_MS),
    State = #state{
        ttl_ms            = maps:get(ttl_ms, Opts, ?DEFAULT_TTL_MS),
        sweep_interval_ms = Sweep,
        last_sweep        = undefined
    },
    _ = schedule_sweep(Sweep),
    {ok, State}.

handle_call({lookup, Dest}, _From, State) ->
    {Result, NewState} = do_lookup(Dest, State),
    {reply, Result, NewState};
handle_call({store, Dest, Paths}, _From, State) ->
    {reply, ok, do_store(Dest, Paths, State)};
handle_call({invalidate, Dest}, _From, State) ->
    {reply, ok, do_invalidate(Dest, State)};
handle_call({invalidate_via_hop, Hop}, _From, State) ->
    {Removed, NewState} = do_invalidate_via_hop(Hop, State),
    {reply, Removed, NewState};
handle_call(evict_expired, _From, State) ->
    {Removed, NewState} = do_evict_expired(State),
    {reply, Removed, NewState};
handle_call(size, _From, #state{entries = E} = State) ->
    {reply, map_size(E), State};
handle_call(all_destinations, _From, #state{entries = E} = State) ->
    {reply, maps:keys(E), State};
handle_call(stats, _From, State) ->
    {reply, build_stats(State), State};
handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(sweep_tick, State) ->
    {_Removed, S1} = do_evict_expired(State),
    _ = schedule_sweep(State#state.sweep_interval_ms),
    {noreply, S1};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%=====================================================================
%% Cache mutations
%%=====================================================================

-spec do_lookup(vertex(), #state{}) ->
        {{ok, [path()]} | miss, #state{}}.
do_lookup(Dest, #state{entries = E, ttl_ms = TTL} = State) ->
    Now = erlang:monotonic_time(millisecond),
    interpret_lookup(maps:find(Dest, E), Dest, Now, TTL, State).

-spec interpret_lookup({ok, entry()} | error, vertex(),
                       integer(), pos_integer(), #state{}) ->
        {{ok, [path()]} | miss, #state{}}.
interpret_lookup(error, _Dest, _Now, _TTL, State) ->
    {miss, State};
interpret_lookup({ok, Entry}, Dest, Now, TTL, State) ->
    classify_entry(is_expired(Entry, Now, TTL), Entry, Dest, State).

-spec classify_entry(boolean(), entry(), vertex(), #state{}) ->
        {{ok, [path()]} | miss, #state{}}.
classify_entry(true, _Entry, Dest, #state{entries = E} = State) ->
    {miss, State#state{entries = maps:remove(Dest, E)}};
classify_entry(false, Entry, _Dest, State) ->
    {{ok, maps:get(paths, Entry)}, State}.

-spec is_expired(entry(), integer(), pos_integer()) -> boolean().
is_expired(#{inserted_at := T0}, Now, TTL) ->
    Now - T0 >= TTL.

-spec do_store(vertex(), [path()], #state{}) -> #state{}.
do_store(Dest, Paths, #state{entries = E} = State) ->
    Now = erlang:monotonic_time(millisecond),
    Entry = #{
        paths       => Paths,
        inserted_at => Now,
        hops        => collect_hops(Paths)
    },
    State#state{entries = E#{Dest => Entry}}.

-spec collect_hops([path()]) -> sets:set(vertex()).
collect_hops(Paths) ->
    sets:from_list([V || P <- Paths, V <- P]).

-spec do_invalidate(vertex(), #state{}) -> #state{}.
do_invalidate(Dest, #state{entries = E} = State) ->
    State#state{entries = maps:remove(Dest, E)}.

-spec do_invalidate_via_hop(vertex(), #state{}) ->
        {[vertex()], #state{}}.
do_invalidate_via_hop(Hop, #state{entries = E} = State) ->
    {Keep, Removed} = partition_entries(
        fun(_Dest, Entry) ->
            sets:is_element(Hop, maps:get(hops, Entry))
        end, E),
    {Removed, State#state{entries = Keep}}.

-spec do_evict_expired(#state{}) -> {[vertex()], #state{}}.
do_evict_expired(#state{entries = E, ttl_ms = TTL} = State) ->
    Now = erlang:monotonic_time(millisecond),
    {Keep, Removed} = partition_entries(
        fun(_Dest, Entry) -> is_expired(Entry, Now, TTL) end, E),
    {Removed, State#state{entries = Keep, last_sweep = Now}}.

%% @doc Walk an entries map and split into kept + removed by predicate.
%% Predicate returns `true' for entries that should be REMOVED.
-spec partition_entries(fun((vertex(), entry()) -> boolean()),
                        #{vertex() => entry()}) ->
        {#{vertex() => entry()}, [vertex()]}.
partition_entries(Pred, Entries) ->
    maps:fold(
      fun(Dest, Entry, {K, Rs}) ->
          assign_partition(Pred(Dest, Entry), Dest, Entry, K, Rs)
      end, {#{}, []}, Entries).

-spec assign_partition(boolean(), vertex(), entry(),
                       #{vertex() => entry()}, [vertex()]) ->
        {#{vertex() => entry()}, [vertex()]}.
assign_partition(true,  Dest, _Entry, K, Rs) -> {K, [Dest | Rs]};
assign_partition(false, Dest,  Entry, K, Rs) -> {K#{Dest => Entry}, Rs}.

%%=====================================================================
%% Timer
%%=====================================================================

-spec schedule_sweep(pos_integer()) -> reference().
schedule_sweep(IntervalMs) ->
    erlang:send_after(IntervalMs, self(), sweep_tick).

%%=====================================================================
%% Stats
%%=====================================================================

-spec build_stats(#state{}) -> stats().
build_stats(#state{entries = E, ttl_ms = TTL,
                   sweep_interval_ms = SI, last_sweep = LS}) ->
    #{
        size              => map_size(E),
        ttl_ms            => TTL,
        sweep_interval_ms => SI,
        last_sweep        => LS
    }.
