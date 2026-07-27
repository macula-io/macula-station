%% @doc Bucket liveness probe — the classic Kademlia admission rule this
%% implementation was missing.
%%
%% == The problem it fixes ==
%%
%% A full bucket here admits NOTHING. `macula_dht_bucket:maybe_evict/4'
%% rejects on `CandScore =&lt; MinScore', and the admission score
%%
%%   1.0*uptime + 0.8*(novelty/3) + 0.3*incumbency - 0.5*latency_frac
%%
%% collapses to incumbency alone in production, because nothing sets
%% `uptime_frac' or `observed_latency_ms' and
%% `macula_station_peer_observer:direct_peer_spec/2' stamps identical
%% `asn'/`country'/`tier' on every observed peer. A newcomer scores 0 on
%% the only live term, so it always loses.
%%
%% That is not eviction pressure, it is LOCKOUT. A saturated bucket keeps
%% whatever connected first, for the lifetime of the process. Daemons churn
%% in their thousands and stations announce a handful of times a day, so the
%% resident set is not a sample of the network — it is a race winner. A
%% station that reconnects mid-life, or that is learned through a NODES
%% reply, can never get in.
%%
%% == The rule ==
%%
%% Real Kademlia does not score. When a bucket is full it pings the
%% least-recently-seen entry and only admits a newcomer if that entry fails
%% to answer. It needs no ASN, no country, no uptime and no latency — which
%% is exactly why it still works here while the scoring model has starved.
%%
%% This worker implements the liveness half: it periodically probes the
%% least-recently-seen occupant of each FULL bucket and forgets it when it is
%% provably gone, which frees the slot. Re-admission is left to the ordinary
%% `observe' path rather than a replacement cache — peers reconnect and walks
%% re-learn constantly on this fleet, so a freed slot fills on its own, and a
%% cache would add per-rejection state to `macula_dht_server' for a race it
%% does not need to win.
%%
%% ⚠ ITS CORRECTNESS IS NOW DOWNSTREAM OF THE CAPABILITY GATE.
%%
%% Before the gate, the routing table was ~88% daemons, so this module's
%% evictions landed almost entirely on peers that were alive but do not speak
%% DHT — the right ACTION for the wrong REASON. After the gate the table holds
%% only stations, so the blast radius inverts: this module can now only ever
%% evict a STATION.
%%
%% That is why a single unanswered ping is no longer enough. One lost packet,
%% or one moment of scheduler pressure on a loaded DHT server, must not remove
%% a healthy station from a table that has no cheap way to relearn it. Eviction
%% now requires `?STRIKES' CONSECUTIVE timeouts, and any answer resets the
%% count.
%%
%% If the gate is ever reverted, revisit this: the strike rule is calibrated
%% for a station-only population.
%%
%% == Why only a TIMEOUT evicts ==
%%
%% `macula_dht:ping_peer/3' goes out through
%% `macula_station_dht_transport:send_frame/2', which resolves a node ONLY
%% through an existing connection and answers `{error, no_route}' otherwise.
%% Most routing-table entries carry no dialable address, and since
%% `macula_station_peer_observer' keeps station entries across disconnect,
%% a perfectly healthy station that is merely not currently connected also
%% answers `no_route'.
%%
%% So `no_route' and `no_transport' are statements about US, not about the
%% peer, and evicting on them would reap exactly the stations this is meant
%% to protect — building the mirror image of the freeze. Only `{error,
%% timeout}', meaning we reached it and it did not answer, is evidence of
%% death.
%%
%% == Load ==
%%
%% Probing runs in spawned processes, never in the DHT gen_server: a ping is
%% a network round trip and that server is the fleet's serialising
%% chokepoint (see docs/CASCADE_INVESTIGATION.md). `max_probes_per_tick'
%% bounds the fan-out; full buckets are rare (one or two per station even at
%% ~100 connections), so the normal cost is zero.
-module(macula_dht_liveness).
-behaviour(gen_server).

-export([start_link/1, stop/1, tick/1, stats/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, stats/0]).

-define(DEFAULT_INTERVAL_MS,      60_000).
-define(DEFAULT_PING_TIMEOUT_MS,   3_000).
-define(DEFAULT_MAX_PROBES,            4).
%% Consecutive unanswered probes before eviction. >1 because post-gate the only
%% population here is stations.
-define(STRIKES,                       3).

-type opts() :: #{
    dht                 := macula_dht:dht(),
    interval_ms         => pos_integer(),
    ping_timeout_ms     => pos_integer(),
    max_probes_per_tick => pos_integer()
}.

-type stats() :: #{
    ticks       := non_neg_integer(),
    probed      := non_neg_integer(),
    alive       := non_neg_integer(),
    evicted     := non_neg_integer(),
    %% Probes that told us nothing about the peer, only about our own
    %% reachability. Counted separately because folding them into a failure
    %% count is what would turn this worker into a station reaper.
    unreachable := non_neg_integer()
}.

-record(state, {
    dht                 :: macula_dht:dht(),
    interval_ms         :: pos_integer(),
    ping_timeout_ms     :: pos_integer(),
    max_probes_per_tick :: pos_integer(),
    ticks               :: non_neg_integer(),
    %% NodeId -> consecutive unanswered probes. Reset by any answer.
    strikes             :: #{macula_identity:pubkey() => pos_integer()},
    probed              :: non_neg_integer(),
    alive               :: non_neg_integer(),
    evicted             :: non_neg_integer(),
    unreachable         :: non_neg_integer()
}).

%%=====================================================================
%% API
%%=====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{dht := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% @doc Run one probe round synchronously. Returns the number of probes
%% STARTED, not their outcomes — those arrive asynchronously and land in
%% `stats/1'.
-spec tick(pid()) -> non_neg_integer().
tick(Pid) ->
    gen_server:call(Pid, tick, infinity).

-spec stats(pid()) -> stats().
stats(Pid) ->
    gen_server:call(Pid, stats).

%%=====================================================================
%% gen_server
%%=====================================================================

init(#{dht := Dht} = Opts) ->
    Interval = maps:get(interval_ms, Opts, ?DEFAULT_INTERVAL_MS),
    _ = erlang:send_after(Interval, self(), liveness_tick),
    {ok, #state{
        dht                 = Dht,
        interval_ms         = Interval,
        ping_timeout_ms     = maps:get(ping_timeout_ms, Opts,
                                       ?DEFAULT_PING_TIMEOUT_MS),
        max_probes_per_tick = maps:get(max_probes_per_tick, Opts,
                                       ?DEFAULT_MAX_PROBES),
        ticks = 0, strikes = #{}, probed = 0, alive = 0, evicted = 0,
        unreachable = 0
    }}.

handle_call(tick, _From, State) ->
    {N, S1} = run_tick(State),
    {reply, N, S1};
handle_call(stats, _From, State) ->
    {reply, build_stats(State), State};
handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({probe_result, NodeId, alive}, #state{alive = A,
                                                  strikes = K} = S) ->
    {noreply, S#state{alive = A + 1, strikes = maps:remove(NodeId, K)}};
handle_cast({probe_result, NodeId, silent}, S) ->
    {noreply, count_strike(NodeId, S)};
handle_cast({probe_result, NodeId, unreachable}, #state{unreachable = U,
                                                        strikes = K} = S) ->
    %% Says nothing about the peer, so it neither strikes nor clears.
    _ = NodeId, _ = K,
    {noreply, S#state{unreachable = U + 1}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(liveness_tick, #state{interval_ms = Interval} = State) ->
    {_N, S1} = run_tick(State),
    _ = erlang:send_after(Interval, self(), liveness_tick),
    {noreply, S1};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_Old, State, _Extra) -> {ok, State}.

%%=====================================================================
%% Tick
%%=====================================================================

-spec run_tick(#state{}) -> {non_neg_integer(), #state{}}.
run_tick(#state{dht = Dht, max_probes_per_tick = Max,
                ping_timeout_ms = Tmo,
                ticks = T, probed = P} = S) ->
    Targets = probe_targets(Dht, Max),
    Server  = self(),
    _ = [spawn(fun() -> probe(Server, Dht, Id, Tmo) end) || Id <- Targets],
    N = length(Targets),
    {N, S#state{ticks = T + 1, probed = P + N}}.

%% Only FULL buckets matter: a bucket with room admits newcomers already, so
%% probing it would be pure cost.
-spec probe_targets(macula_dht:dht(), pos_integer()) ->
        [macula_identity:pubkey()].
probe_targets(Dht, Max) ->
    Buckets = macula_dht:populated_buckets(Dht),
    Full    = [B || {_I, B} <- Buckets, macula_dht_bucket:is_full(B)],
    Ids     = [macula_dht_entry:node_id(E)
               || B <- Full,
                  {ok, E} <- [macula_dht_bucket:least_recently_seen(B)]],
    lists:sublist(Ids, Max).

-spec probe(pid(), macula_dht:dht(), macula_identity:pubkey(),
            pos_integer()) -> ok.
probe(Server, Dht, NodeId, Timeout) ->
    gen_server:cast(Server, {probe_result, NodeId,
                             classify_ping(macula_dht:ping_peer(Dht, NodeId,
                                                                Timeout))}).

%% Eviction is decided in the SERVER, not the probe, because it needs the
%% strike history. A probe only reports what it saw.
-spec count_strike(macula_identity:pubkey(), #state{}) -> #state{}.
count_strike(NodeId, #state{strikes = K} = S) ->
    N = maps:get(NodeId, K, 0) + 1,
    decide_eviction(N >= ?STRIKES, NodeId, N, S).

decide_eviction(false, NodeId, N, #state{strikes = K} = S) ->
    S#state{strikes = K#{NodeId => N}};
decide_eviction(true, NodeId, _N, #state{strikes = K, evicted = E,
                                         dht = Dht} = S) ->
    logger:info("[dht_liveness] evicting ~s after ~p consecutive unanswered "
                "probes", [binary:encode_hex(binary:part(NodeId, 0, 4)),
                           ?STRIKES]),
    macula_dht:forget(Dht, NodeId),
    S#state{strikes = maps:remove(NodeId, K), evicted = E + 1}.

%% `timeout' is the only verdict about the PEER. Every other error is a
%% verdict about our own transport — see the moduledoc.
-spec classify_ping(macula_dht:ping_result()) ->
        alive | silent | unreachable.
classify_ping({ok, _})           -> alive;
classify_ping({error, timeout})  -> silent;
classify_ping({error, _Other})   -> unreachable.

-spec build_stats(#state{}) -> stats().
build_stats(#state{ticks = T, probed = P, alive = A,
                   evicted = E, unreachable = U}) ->
    #{ticks => T, probed => P, alive => A,
      evicted => E, unreachable => U}.
