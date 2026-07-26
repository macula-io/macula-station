%%% @doc Periodic health beacon — publishes mailbox + heap + reduction
%%% RATE of the station's most load-bearing processes on `_mesh.health.v1'
%%% so realm-side dashboards can render a load monitor without needing a
%%% separate management plane, AND trips a loud local ERROR when a
%%% control-plane process runs hot or a mailbox grows without bound.
%%%
%%% Cadence: every `?TICK_MS' (10s). Payload is ETF-encoded — same
%%% process families both sides, no cross-language compat concern.
%%% Realm decodes with `binary_to_term/2' + `[safe]'.
%%%
%%% Why the RATE and not raw reductions: `process_info(Pid, reductions)'
%%% is the CUMULATIVE lifetime counter — a monotonically growing integer
%%% that says nothing about whether the process is hot NOW. The chronic
%%% peering_router leak published a fine-looking counter for months. We
%%% keep the previous sample and publish `reds_per_s' (delta over the
%%% tick), which is the signal that actually separates "busy" from
%%% "pathological". Raw `reductions' stays in the payload for trend.
%%%
%%% Why this exists: bloom-convergence shows pubsub routing health,
%%% but a station can be wedged at the BEAM level (peer_observer
%%% mailbox 10k deep, GC-stalled, etc.) while still broadcasting a
%%% perfectly fine bloom every 30s. That's invisible to the
%%% bloom-convergence LV. This module's payload exposes the
%%% process-level signal the bloom view can't see.
%%%
%%% Tripwire (LOCAL only — no mesh fact, no neighbour consumption, no
%%% throttle; see plans/BRAINSTORM_CONTINUOUS_SELF_DIAGNOSIS.md §7):
%%%   - control-plane proc sustained above `?CONTROL_REDS_PER_S_LIMIT'
%%%     for `?RATE_STRIKE_LIMIT' consecutive ticks, OR
%%%   - any proc's mailbox strictly growing for `?MBOX_GROWTH_STRIKE_LIMIT'
%%%     consecutive ticks (above `?MBOX_GROWTH_FLOOR')
%%% fires one `?LOG_ERROR' on the rising edge (edge-triggered, not per
%%% tick) and one recovery log on the falling edge. Data-plane procs
%%% legitimately run at millions of reds/s, so the rate rule applies to
%%% control-plane procs only; the mailbox-growth rule applies to all.
-module(macula_station_health_publisher).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-export([rate/4, rate_strikes/3, mbox_strikes/3, eval_one/3, evaluate/3,
         new_strike/0]).
-endif.

-define(TICK_MS, 10_000).
-define(MESH_REALM, <<0:256>>).
-define(TOPIC, <<"_mesh.health.v1">>).

%% Tripwire thresholds. Control-plane baseline is near-idle (the router
%% post-leak-fix sits at ~34 reds/s); the leak ran ~34,000x that, so a
%% 10k/s ceiling has enormous headroom over legitimate control traffic
%% and near-zero false-positive risk. Mailbox growth is the leak
%% signature that raw depth misses (a steady deep mailbox is not a leak;
%% a growing one is), so we fire on GROWTH, not on absolute depth.
-define(CONTROL_REDS_PER_S_LIMIT, 10_000).
-define(RATE_STRIKE_LIMIT, 3).          %% 3 ticks = 30s sustained hot
-define(MBOX_GROWTH_STRIKE_LIMIT, 6).   %% 6 ticks = 60s monotone growth
-define(MBOX_GROWTH_FLOOR, 50).         %% ignore growth below this depth

%% Processes worth reporting. `{atom_name, display_label, class}'.
%% `display_label' is a binary the realm UI shows (atoms need not be
%% loaded there). `class' gates the rate tripwire: `control' procs are
%% near-idle at steady state so a high reds/s is pathological; `data'
%% procs (dispatcher, dht) legitimately burn, so only mailbox growth
%% flags them.
%% NOTE peer_observer is `data', not `control': it is the controlling_pid
%% for every peering conn and does per-frame signature-verify + pubsub
%% relay, so its reds/s scales with EVENT volume, not churn. Its historic
%% pathology (116k-deep mailbox) was a BLOCKED observer — low reds, growing
%% mbox — which the all-class mailbox-growth rule already catches. A rate
%% rule here would false-fire on every traffic-bearing station.
-define(TRACKED, [
    {macula_station_peer_observer,     <<"peer_observer">>,     data},
    {macula_station_peering_router,    <<"peering_router">>,    control},
    {macula_station_route_pubsub_frames, <<"pubsub_dispatcher">>, data},
    {macula_station_bloom_exchange,    <<"bloom_exchange">>,    control},
    {macula_station_record_fanout,     <<"record_fanout">>,     control},
    {macula_dht,                       <<"dht">>,               data},
    {macula_dht_replicate,             <<"dht_replicate">>,     data},
    {macula_station_event_dedup,       <<"event_dedup">>,       control}
]).

-record(state, {
    identity   :: macula_identity:key_pair(),
    timer_ref  :: reference() | undefined,
    %% Label => {LifetimeReductions, TsMs} of the previous tick, for
    %% deriving reds/s. Empty on the first tick (rate reported as 0).
    prev = #{} :: #{binary() => {non_neg_integer(), integer()}},
    %% Label => strike map (see new_strike/0). Tracks consecutive-tick
    %% counters + current alarm edge so logs fire on transitions only.
    strikes = #{} :: #{binary() => map()}
}).

-type opts() :: #{identity := macula_identity:key_pair()}.
-export_type([opts/0]).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{identity := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%%====================================================================
%% gen_server
%%====================================================================

init(#{identity := Kp}) ->
    process_flag(trap_exit, true),
    {ok, schedule_tick(#state{identity = Kp})}.

handle_call(_Msg, _From, S) -> {reply, {error, unknown_call}, S}.
handle_cast(_Msg, S) -> {noreply, S}.

handle_info({tick, Ref}, #state{timer_ref = Ref} = S0) ->
    Now = erlang:system_time(millisecond),
    Samples = sample(),
    {Procs, Rates} = enrich(Samples, S0#state.prev, Now),
    broadcast(build_payload(S0#state.identity, Now, Procs)),
    Strikes1 = evaluate(Samples, Rates, S0#state.strikes),
    Prev1 = maps:from_list(
              [{maps:get(label, X), {maps:get(reds, X), Now}} || X <- Samples]),
    {noreply, schedule_tick(S0#state{prev = Prev1, strikes = Strikes1})};
handle_info(_, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Sampling
%%====================================================================

%% Live snapshot of every tracked, running process.
sample() ->
    lists:filtermap(fun probe/1, ?TRACKED).

probe({RegName, Label, Class}) ->
    probe_pid(whereis(RegName), Label, Class).

probe_pid(undefined, _Label, _Class) ->
    false;
probe_pid(Pid, Label, Class) when is_pid(Pid) ->
    probe_info(process_info(Pid, [message_queue_len, total_heap_size,
                                  reductions]), Label, Class).

probe_info(undefined, _Label, _Class) ->
    false;
probe_info(Info, Label, Class) ->
    {true, #{
        label  => Label,
        class  => Class,
        mbox   => proplists:get_value(message_queue_len, Info, 0),
        heap_w => proplists:get_value(total_heap_size, Info, 0),
        reds   => proplists:get_value(reductions, Info, 0)
    }}.

%%====================================================================
%% Payload
%%====================================================================

%% Turn raw samples into the wire proc maps (adding reds_per_s from the
%% previous tick) and a Label => reds/s map for the tripwire.
enrich(Samples, Prev, Now) ->
    lists:foldr(
      fun(#{label := Label, mbox := Mbox, heap_w := Heap, reds := Reds}, {Ps, Rs}) ->
              Rate = rate(Label, Reds, Prev, Now),
              P = #{
                  <<"name">>       => Label,
                  <<"mbox">>       => Mbox,
                  <<"heap_w">>     => Heap,
                  <<"reductions">> => Reds,
                  <<"reds_per_s">> => Rate
              },
              {[P | Ps], Rs#{Label => Rate}}
      end, {[], #{}}, Samples).

%% Reductions/s over the tick. Clamped to 0 when there is no prior
%% sample or when the counter went backwards (process restarted under a
%% fresh pid — whereis now points at a reset lifetime counter).
rate(Label, Reds, Prev, Now) ->
    case maps:get(Label, Prev, undefined) of
        {PrevReds, PrevTs} when Now > PrevTs, Reds >= PrevReds ->
            (Reds - PrevReds) * 1000 div (Now - PrevTs);
        _ ->
            0
    end.

build_payload(Kp, Now, Procs) ->
    Term = #{
        <<"node_id">> => macula_identity:public(Kp),
        <<"ts_ms">>   => Now,
        <<"procs">>   => Procs
    },
    erlang:term_to_binary(Term).

broadcast(Payload) ->
    Conns = macula_station_peer_links:connections(),
    lists:foreach(
      fun({_Url, LinkPid}) ->
              catch macula_station_link:publish(LinkPid, ?MESH_REALM, ?TOPIC, Payload)
      end,
      Conns),
    ok.

%%====================================================================
%% Tripwire (local, edge-triggered)
%%====================================================================

new_strike() ->
    #{rate => 0, mbox => 0, last_mbox => 0, alarmed => false}.

evaluate(Samples, Rates, Strikes0) ->
    Labels = [maps:get(label, X) || X <- Samples],
    %% Drop counters for procs absent this tick BEFORE folding, so a
    %% strike count (or `alarmed') can never bridge process incarnations
    %% and log fabricated "sustained" evidence when the proc restarts.
    Strikes = prune(Labels, Strikes0),
    lists:foldl(
      fun(#{label := Label} = X, Acc) ->
              St0 = maps:get(Label, Acc, new_strike()),
              St1 = eval_one(X, maps:get(Label, Rates, 0), St0),
              Acc#{Label => St1}
      end, Strikes, Samples).

%% Keep only current-tick labels; note if we drop a proc that was still
%% in alarm (it vanished before clearing — worth a breadcrumb).
prune(Labels, Strikes) ->
    Gone = maps:without(Labels, Strikes),
    maps:foreach(
      fun(Label, #{alarmed := true}) ->
              ?LOG_NOTICE("station health: ~s gone while alarmed", [Label]);
         (_, _) -> ok
      end, Gone),
    maps:with(Labels, Strikes).

eval_one(#{label := Label, class := Class, mbox := Mbox}, Rate,
         #{rate := R0, mbox := M0, last_mbox := LastMbox, alarmed := Al0}) ->
    R1 = rate_strikes(Class, Rate, R0),
    M1 = mbox_strikes(Mbox, LastMbox, M0),
    Tripped = R1 >= ?RATE_STRIKE_LIMIT orelse M1 >= ?MBOX_GROWTH_STRIKE_LIMIT,
    Al1 = edge(Tripped, Al0, Label, Rate, Mbox, R1, M1),
    #{rate => R1, mbox => M1, last_mbox => Mbox, alarmed => Al1}.

rate_strikes(control, Rate, R0) when Rate > ?CONTROL_REDS_PER_S_LIMIT -> R0 + 1;
rate_strikes(_Class, _Rate, _R0) -> 0.

mbox_strikes(Mbox, LastMbox, M0) when Mbox >= ?MBOX_GROWTH_FLOOR, Mbox > LastMbox -> M0 + 1;
mbox_strikes(_Mbox, _LastMbox, _M0) -> 0.

%% Rising edge: log ERROR once. Falling edge: log recovery once.
edge(true, false, Label, Rate, Mbox, R1, M1) ->
    ?LOG_ERROR("station health tripwire: ~s pathological "
               "(reds/s=~B, mbox=~B, rate_strikes=~B, mbox_strikes=~B)",
               [Label, Rate, Mbox, R1, M1]),
    true;
edge(false, true, Label, Rate, Mbox, _R1, _M1) ->
    ?LOG_NOTICE("station health tripwire cleared: ~s (reds/s=~B, mbox=~B)",
                [Label, Rate, Mbox]),
    false;
edge(Tripped, _Al0, _Label, _Rate, _Mbox, _R1, _M1) ->
    Tripped.

%%====================================================================
%% Timer
%%====================================================================

schedule_tick(S) ->
    Ref = make_ref(),
    erlang:send_after(?TICK_MS, self(), {tick, Ref}),
    S#state{timer_ref = Ref}.
