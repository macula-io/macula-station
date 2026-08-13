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
%%%
%%% == Wire tripwire (`ingress_stall') ==
%%%
%%% ⚠ On 2026-08-13 `station-it-milan' served nothing for THIRTY HOURS
%%% while every signal above read green. The container healthcheck was
%%% healthy (it curls this station's own admin port over loopback), the
%%% listener process was alive with mailbox 0 and a clean cap, and
%%% `peer_observer' held 54 conns. tcpdump on the box showed every
%%% inbound QUIC Initial ARRIVING and ZERO packets leaving, including
%%% the keepalive to its own configured peer.
%%%
%%% Nothing fired because every signal this module publishes is derived
%%% from BEAM state, and a dead transport does not disturb BEAM state.
%%% No counter disagreed with any other counter. The only two things
%%% that disagreed were THE STATION AND THE NETWORK, and nothing
%%% compared those two. This rule is that comparison.
%%%
%%% The invariant: <em>while the kernel is holding undelivered datagrams
%%% on this station's own listener socket, the dispatched-frame counter
%%% must advance.</em>
%%%
%%% Both halves are chosen, not convenient:
%%%
%%% <ul>
%%%   <li>The backlog comes from `/proc/net/udp6' — the KERNEL's view of
%%%       the one socket we own. It is the only station-attributable
%%%       observable in this system that is not BEAM state.</li>
%%%   <li>The counter is `dispatch_self' from
%%%       `macula_station_frame_telemetry', which is recorded
%%%       unconditionally on both the legacy 4-tuple and the
%%%       timing-enabled 5-tuple receive paths, so it advances whenever
%%%       the station actually processes inbound work.</li>
%%% </ul>
%%%
%%% ⚠ NEVER rebuild this on a send-side quantity. Every one of them is a
%%% statement of intent rather than of transmission, and every one was
%%% GREEN for all thirty hours: `macula_peering:send_frame/2' is a
%%% `gen_statem:cast' that returns `ok' to a dead pid, the router's
%%% `forwarded' counter counts that cast and climbed throughout, and
%%% `macula_quic:getstat/2' answers hardcoded zeros. See
%%% `plans/PLAN_WIRE_LIVENESS_TRIPWIRE.md' §2 and §6.
%%%
%%% This rule is SILENT during a network partition by construction, not
%%% by tuning: its antecedent is a non-empty receive backlog, and a
%%% partition delivers no packets to queue. That is what makes it a
%%% local-fault detector, and it is why this is the rule permitted to
%%% escalate while `outbound_futility' only ever reports.
-module(macula_station_health_publisher).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/1, stop/1, wire/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-export([rate/4, rate_strikes/3, mbox_strikes/3, eval_one/3, evaluate/3,
         new_strike/0]).
-export([parse_udp6/2, port_hex/1, new_wire/0, wire_step/2, wire_verdict/1,
         sum_dispatch/2, outbound_futility/3]).
-endif.

-define(TICK_MS, 10_000).
-define(MESH_REALM, <<0:256>>).
-define(TOPIC, <<"_mesh.health.v1">>).

%% Wire tripwire. See the moduledoc for why the kernel and not the BEAM.
%%
%% 30 samples is 5 minutes at ?TICK_MS. A healthy station drains its UDP
%% receive queue sub-millisecond, so a backlog that is non-empty AND
%% never shrinks across 30 consecutive ticks is not a busy station. Milan
%% held that state for 10,800 samples — 360x this threshold — so the
%% headroom is enormous and the false-positive cost (one logged station
%% during a five-minute total processing stall) is accepted.
%%
%% The floor is 1 rather than a byte count on purpose: the rule is
%% "non-empty AND not draining", and the non-decreasing half is what
%% keeps a merely busy station safe. A queue that grows and shrinks
%% never accumulates a strike however deep it gets.
-define(WIRE_STALL_SAMPLES,  30).
-define(WIRE_RX_FLOOR_BYTES,  1).
-define(PROC_NET_UDP6, "/proc/net/udp6").
%% Short, because this call reaches into the listener and the listener is
%% exactly the process that may be wedged. A tripwire must never block on
%% the thing it is watching.
-define(LISTEN_ADDR_TIMEOUT_MS, 1_000).
%% Coupled to macula_station_outbound_link's ?HANDSHAKE_TIMEOUT_MS (30s)
%% and ?MAX_BACKOFF_MS (60s): ~90s is the worst legitimate dial cycle, so
%% this is 3.3 cycles. Raising either without raising this manufactures a
%% false futility verdict. Keep the three coupled.
-define(OUTBOUND_FUTILE_MS, 300_000).

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
    strikes = #{} :: #{binary() => map()},
    %% Wire tripwire state (see new_wire/0).
    wire = new_wire() :: map(),
    %% Outbound-futility alarm edge (I2). Reports only; never acts.
    futile_alarmed = false :: boolean(),
    %% Listener port, resolved lazily and cached. Resolved lazily rather
    %% than at init because this module boots after the listener but a
    %% restart of either must not order-depend on the other.
    port :: inet:port_number() | undefined
}).

-type opts() :: #{identity := macula_identity:key_pair()}.
-export_type([opts/0]).

%%====================================================================
%% API
%%====================================================================

%% Registered under the module name so `macula_station_admin' can read
%% the wire verdict without holding a pid. Matches the station's house
%% pattern of a fixed local atom per long-lived child.
-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{identity := _} = Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Current wire verdict, for the admin readout.
%%
%% Answers `unknown' rather than `ok' when this process is absent or too
%% busy to reply. That asymmetry is the whole point: the failure being
%% detected is one where everything reported healthy, so an unreachable
%% detector must never be reported as a passing one.
-spec wire() -> map().
wire() ->
    try gen_server:call(?MODULE, wire, ?LISTEN_ADDR_TIMEOUT_MS) of
        Verdict -> Verdict
    catch
        %% Deviation from let-it-crash, deliberate: without this an admin
        %% HTTP request would die with the detector instead of reporting
        %% that the detector is unreachable, which is the one answer a
        %% caller must never lose.
        exit:_ -> (new_wire())#{reason => publisher_unreachable}
    end.

%%====================================================================
%% gen_server
%%====================================================================

init(#{identity := Kp}) ->
    process_flag(trap_exit, true),
    {ok, schedule_tick(#state{identity = Kp})}.

handle_call(wire, _From, #state{wire = W} = S) ->
    {reply, W, S};
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
    {Port1, Wire1} = tick_wire(S0#state.port, S0#state.wire),
    Futile1 = tick_futility(S0#state.futile_alarmed),
    {noreply, schedule_tick(S0#state{prev = Prev1, strikes = Strikes1,
                                     port = Port1, wire = Wire1,
                                     futile_alarmed = Futile1})};
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
%% Wire tripwire — the kernel's view, not the BEAM's
%%====================================================================

%% One tick of the wire rule. Impure edges (procfs, ETS, the listener)
%% live here; every decision lives in `wire_step/2', which is pure and
%% test-exported.
tick_wire(Port0, Wire) ->
    Port = resolve_port(Port0),
    {Port, wire_step(wire_sample(Port), Wire)}.

%% Cached after the first success. `undefined' means "ask again next
%% tick", so a station whose listener is not up yet simply reports
%% `unknown' until it is, and never accumulates a strike for it.
resolve_port(Port) when is_integer(Port) -> Port;
resolve_port(undefined)                  -> listen_port().

listen_port() ->
    try macula_station:listen_addr() of
        {_Bind, Port} when is_integer(Port) -> Port;
        _Other                              -> undefined
    catch
        %% Deviation from let-it-crash, deliberate: `listen_addr/0' is a
        %% gen_server:call INTO THE LISTENER, and a wedged listener is
        %% precisely the fault being detected. Letting that exit kill the
        %% detector would make the tripwire fail exactly when it matters.
        _:_ -> undefined
    end.

%% `unavailable' is a THIRD state, never 0 and never ok. A station that
%% cannot read its own socket has not observed a healthy socket.
wire_sample(undefined) ->
    unavailable;
wire_sample(Port) ->
    build_sample(read_udp6(Port), frames_dispatched()).

build_sample({ok, #{rx_queue := Rx}}, Frames) when is_integer(Frames) ->
    #{rx_queue => Rx, frames => Frames};
build_sample(_Row, _Frames) ->
    unavailable.

read_udp6(Port) ->
    parse_udp6(file:read_file(?PROC_NET_UDP6), Port).

%% Total inbound frames this station has DISPATCHED, across every frame
%% type and latency bucket.
%%
%% `dispatch_self' and not `forward_send': the former is recorded after
%% the receive handler has actually run, the latter times a cast that
%% returns ok to a dead pid. Folding the wrong phase here would rebuild
%% the exact blindness this rule exists to remove.
frames_dispatched() ->
    total_from(ets:whereis(macula_station_frame_telemetry)).

total_from(undefined) -> unavailable;
total_from(Tid)       -> ets:foldl(fun sum_dispatch/2, 0, Tid).

sum_dispatch({{_Type, dispatch_self, _Bucket}, N}, Acc) -> Acc + N;
sum_dispatch(_Row, Acc)                                 -> Acc.

new_wire() ->
    #{verdict => unknown, strikes => 0, checks => 0,
      prev => undefined, alarmed => false}.

%% @doc Fold one sample into the wire state. Pure.
%%
%% `checks' always advances, including on `unavailable'. Per the SWIM
%% mechanism-counter rule: a detector with nothing to detect and a
%% detector that cannot run look identical from outside unless it counts
%% its own passes.
wire_step(unavailable, #{checks := C} = W) ->
    %% Cannot see => cannot judge. Strikes reset, so a gap in
    %% observability can never accumulate into a verdict.
    W#{verdict => unknown, strikes => 0, prev => undefined, checks => C + 1};
wire_step(#{rx_queue := Rx, frames := F} = Sample, #{checks := C} = W) ->
    Strikes = wire_strikes(Sample, maps:get(prev, W), maps:get(strikes, W)),
    Verdict = wire_verdict(Strikes),
    Alarmed = wire_edge(Verdict, maps:get(alarmed, W), Rx, F, Strikes),
    W#{verdict => Verdict, strikes => Strikes, prev => Sample,
       checks => C + 1, alarmed => Alarmed}.

%% No prior sample means no delta to judge.
wire_strikes(_Sample, undefined, _Strikes) ->
    0;
%% Frames advanced: the consequent holds, whatever the queue is doing.
%% This is the clause that keeps a slow-draining busy station green.
wire_strikes(#{frames := F}, #{frames := P}, _Strikes) when F > P ->
    0;
%% Counter went backwards — POST /telemetry/frames/reset. Clamp exactly
%% as rate/4 does; a reset is not evidence of a stall.
wire_strikes(#{frames := F}, #{frames := P}, _Strikes) when F < P ->
    0;
%% Frames flat AND the backlog is non-empty and not shrinking.
wire_strikes(#{rx_queue := Rx}, #{rx_queue := PrevRx}, Strikes)
  when Rx >= ?WIRE_RX_FLOOR_BYTES, Rx >= PrevRx ->
    Strikes + 1;
wire_strikes(_Sample, _Prev, _Strikes) ->
    0.

wire_verdict(Strikes) when Strikes >= ?WIRE_STALL_SAMPLES -> stalled;
wire_verdict(_Strikes)                                    -> ok.

%% Rising edge logs once, falling edge logs once. Per-tick logging of a
%% 30-hour condition is how a signal becomes noise and stops being read.
wire_edge(stalled, false, Rx, Frames, Strikes) ->
    ?LOG_ERROR("station wire tripwire: INGRESS STALLED — the kernel is "
               "holding ~B bytes on our listener socket and the dispatched "
               "frame counter has not moved for ~B ticks (~B s). "
               "frames_total=~B. This station is receiving packets and "
               "processing none of them.",
               [Rx, Strikes, Strikes * ?TICK_MS div 1000, Frames]),
    emit_wire_event(Rx, Frames, Strikes),
    true;
wire_edge(ok, true, Rx, Frames, _Strikes) ->
    ?LOG_NOTICE("station wire tripwire cleared: ingress draining again "
                "(rx_queue=~B, frames_total=~B)", [Rx, Frames]),
    false;
wire_edge(_Verdict, Alarmed, _Rx, _Frames, _Strikes) ->
    Alarmed.

emit_wire_event(Rx, Frames, Strikes) ->
    catch macula_diagnostics:event(<<"_macula.peering.wire_stalled">>,
                                   #{rx_queue     => Rx,
                                     frames_total => Frames,
                                     strikes      => Strikes}),
    ok.

%% @doc Find this station's own row in `/proc/net/udp6' and read its
%% queue depths. Pure over the file contents.
%%
%% Three outcomes, never collapsed: `{ok, Map}', `not_found' (the socket
%% is not bound — a real answer), and `unavailable' (no procfs, i.e. not
%% Linux, or unreadable). Collapsing `unavailable' to a zero backlog
%% would make every non-Linux dev box permanently green.
%%
%% Matches on the PORT SUFFIX only. The 32-hex-char local address is
%% word-wise little-endian and parsing it buys nothing but a new way to
%% be wrong.
-spec parse_udp6({ok, binary()} | {error, term()}, inet:port_number()) ->
    {ok, #{tx_queue := non_neg_integer(),
           rx_queue := non_neg_integer(),
           drops := non_neg_integer()}} | not_found | unavailable.
parse_udp6({error, _Reason}, _Port) ->
    unavailable;
parse_udp6({ok, Bin}, Port) ->
    Rows = binary:split(Bin, <<"\n">>, [global]),
    scan_rows(Rows, port_hex(Port)).

scan_rows([], _Want) ->
    not_found;
scan_rows([Row | Rest], Want) ->
    scan_row(string:lexemes(Row, " "), Rest, Want).

%% local_address is field 2 (1-based), queues field 5, drops last.
scan_row([_Sl, Local, _Remote, _St, Queues | Tail], Rest, Want) ->
    match_local(binary:split(Local, <<":">>), Queues, Tail, Rest, Want);
scan_row(_Fields, Rest, Want) ->
    scan_rows(Rest, Want).

match_local([_Addr, Want], Queues, Tail, _Rest, Want) ->
    row_queues(binary:split(Queues, <<":">>), Tail);
match_local(_Split, _Queues, _Tail, Rest, Want) ->
    scan_rows(Rest, Want).

row_queues([Tx, Rx], Tail) ->
    {ok, #{tx_queue => hex(Tx), rx_queue => hex(Rx), drops => drops_of(Tail)}};
row_queues(_Other, _Tail) ->
    unavailable.

%% Last column. Older kernels omit it; absence must read as 0 drops
%% rather than as a parse failure, since drops are reported and never
%% alarmed on.
drops_of([])   -> 0;
drops_of(Tail) -> hex_dec(lists:last(Tail)).

hex(Bin) ->
    binary_to_integer(Bin, 16).

%% The drops column is decimal while the queue columns are hex, which is
%% a genuine inconsistency in the kernel's own format.
hex_dec(Bin) ->
    try binary_to_integer(Bin) of
        N -> N
    catch
        _:_ -> 0
    end.

%% @doc Port as the kernel renders it: uppercase hex, zero-padded to 4.
%% 4433 becomes `<<"1151">>'.
-spec port_hex(inet:port_number()) -> binary().
port_hex(Port) ->
    list_to_binary(string:pad(string:uppercase(integer_to_list(Port, 16)),
                              4, leading, $0)).

%%====================================================================
%% Outbound futility (I2) — reports, never acts
%%====================================================================

%% Second rule on the same tick, with DISJOINT escalation from the
%% ingress rule above.
%%
%% Where I1 asks "are we failing to process what arrives", this asks
%% "are we failing to reach anyone we are configured to reach". milan was
%% false on both: it held one configured peer (paris),
%% `connected_hostnames()' was `[]', and zero conns had an outbound side,
%% for thirty hours.
%%
%% ⚠ This rule must NEVER act, at any escalation. Its predicate is
%% precisely what a genuine network partition produces on every station
%% simultaneously, so wiring it to a restart would convert a
%% self-healing network event into a fleet-wide cold boot — seven
%% stations at 10-20 minutes of reconvergence each. I1 gets to act
%% because a partition cannot trigger it; this one does not.
tick_futility(Alarmed) ->
    Stats = outbound_snapshot(),
    Verdict = outbound_futility(Stats, erlang:system_time(millisecond),
                                ?OUTBOUND_FUTILE_MS),
    futility_edge(Verdict, Alarmed, Stats).

outbound_snapshot() ->
    link_stats(whereis(macula_station_outbound_links_sup)).

%% No sup at all means no configured outbound peers — a LEAF station,
%% where having no outbound link is correct. Gate on presence, never on
%% a count, or every leaf alarms forever.
link_stats(undefined) ->
    [];
link_stats(Sup) ->
    [link_stat(Pid) || {_Id, Pid, _Type, _Mods} <- children_of(Sup),
                       is_pid(Pid)].

children_of(Sup) ->
    try supervisor:which_children(Sup) of
        Children -> Children
    catch
        %% Deviation from let-it-crash, deliberate: a busy or dying sup
        %% must yield "cannot judge" rather than kill the detector that
        %% is watching it.
        _:_ -> []
    end.

link_stat(Pid) ->
    try macula_station_outbound_link:stats(Pid) of
        Stats -> Stats
    catch
        %% Same rationale. `unknown' is a third state and never `futile':
        %% a slow link must not be reported as an unreachable one.
        _:_ -> unknown
    end.

%% @doc Is this station failing to reach every peer it is configured to
%% reach? Pure, so the threshold is testable without waiting 5 minutes.
-spec outbound_futility([map() | unknown], integer(), pos_integer()) ->
    ok | futile | unknown.
outbound_futility([], _Now, _Threshold) ->
    %% Leaf, or no outbound peers configured. Nothing owed.
    ok;
outbound_futility(Stats, Now, Threshold) ->
    verdict_from(any_verified(Stats), lists:member(unknown, Stats),
                 oldest_unverified(Stats), Now, Threshold).

%% One verified link is enough: the station can reach the mesh.
verdict_from(true, _AnyUnknown, _Oldest, _Now, _Threshold) ->
    ok;
verdict_from(false, true, _Oldest, _Now, _Threshold) ->
    unknown;
verdict_from(false, false, undefined, _Now, _Threshold) ->
    ok;
verdict_from(false, false, Since, Now, Threshold) when Now - Since >= Threshold ->
    futile;
verdict_from(false, false, _Since, _Now, _Threshold) ->
    ok.

any_verified(Stats) ->
    lists:any(fun(S) -> is_map(S) andalso maps:get(verified, S, false) end,
              Stats).

oldest_unverified(Stats) ->
    Times = [maps:get(unverified_since, S) || S <- Stats, is_map(S),
             maps:get(unverified_since, S, undefined) =/= undefined],
    min_or_undefined(Times).

min_or_undefined([])    -> undefined;
min_or_undefined(Times) -> lists:min(Times).

futility_edge(futile, false, Stats) ->
    ?LOG_ERROR("station wire tripwire: OUTBOUND FUTILE — this station is "
               "configured to dial ~B peer(s) and has verified NONE of them. "
               "links=~p", [length(Stats), Stats]),
    catch macula_diagnostics:event(<<"_macula.peering.outbound_futile">>,
                                   #{configured => length(Stats)}),
    true;
futility_edge(ok, true, _Stats) ->
    ?LOG_NOTICE("station wire tripwire cleared: an outbound link verified"),
    false;
futility_edge(_Verdict, Alarmed, _Stats) ->
    Alarmed.

%%====================================================================
%% Timer
%%====================================================================

schedule_tick(S) ->
    Ref = make_ref(),
    erlang:send_after(?TICK_MS, self(), {tick, Ref}),
    S#state{timer_ref = Ref}.
