%%%-------------------------------------------------------------------
%%% @doc Per-frame, per-phase, per-bucket latency histograms.
%%%
%%% Three phases per frame:
%%%
%%%   recv_to_dispatch
%%%     Mailbox wait — time from `peering_conn:notify_frame/2'
%%%     stamping `RecvAtUs' to the receiver's `handle_info/2' actually
%%%     running. Requires the source peering_conn to have
%%%     `timing_enabled = true' (macula >= 4.4.7). Without the stamp
%%%     this phase is not recorded.
%%%
%%%   dispatch_self
%%%     CPU time the receiver spent in the dispatch handler — verify,
%%%     lookup, mutation of state. Excludes the mailbox wait above.
%%%
%%%   forward_send
%%%     Time spent emitting the relayed outbound frame (CALL → advertiser
%%%     conn, RESULT → caller origin, stream chunk relay, etc.) up to
%%%     the point `macula_peering:send_frame/2' returns. Captures the
%%%     QUIC stream write latency.
%%%
%%% Storage is a single public ETS table with `write_concurrency' so
%%% updates from many peering_conns / dispatchers don't serialise. Each
%%% update is a lock-free `update_counter/4' with auto-init — ~500ns.
%%% At 1000 frames/sec across 3 phases the total telemetry overhead is
%%% ~1.5ms/sec of CPU. Bounded memory: one row per
%%% `(FrameType, Phase, Bucket)', so ~17 buckets × 3 phases × ~15 frame
%%% types ≈ 800 rows.
%%%
%%% Read out via `snapshot/0' (gen_server-free; just `ets:tab2list/1').
%%%-------------------------------------------------------------------
-module(macula_station_frame_telemetry).

-export([
    init/0,
    record/3,
    snapshot/0,
    reset/0
]).

-define(TABLE, ?MODULE).

-type frame_type() :: atom().
-type phase()      :: recv_to_dispatch | dispatch_self | forward_send.
-type bucket()     :: below_50us
                    | below_100us
                    | below_250us
                    | below_500us
                    | below_1ms
                    | below_2_5ms
                    | below_5ms
                    | below_10ms
                    | below_25ms
                    | below_50ms
                    | below_100ms
                    | below_250ms
                    | below_500ms
                    | below_1s
                    | below_2_5s
                    | below_5s
                    | above_5s.

-export_type([frame_type/0, phase/0, bucket/0]).

%% @doc Create the telemetry table. Idempotent; safe to call from
%% application boot AND from supervised init paths in tests.
-spec init() -> ok.
init() ->
    case ets:whereis(?TABLE) of
        undefined ->
            ets:new(?TABLE, [
                named_table,
                public,
                set,
                {write_concurrency, true},
                {read_concurrency,  true}
            ]),
            ok;
        _Tid ->
            ok
    end.

%% @doc Record one observation. `DurationUs' is microseconds, allowed
%% to be 0 or negative (clamped to 0 — negatives can appear when the
%% sender's monotonic clock is on a different BEAM than the receiver,
%% which shouldn't happen but we handle gracefully).
-spec record(frame_type(), phase(), integer()) -> ok.
record(FrameType, Phase, DurationUs)
  when is_atom(FrameType), is_atom(Phase), is_integer(DurationUs) ->
    Clamped = max(0, DurationUs),
    Bucket  = bucket(Clamped),
    Key     = {FrameType, Phase, Bucket},
    %% update_counter/4 with default tuple does auto-init on first hit.
    %% Position 2 is the counter slot in the {Key, Counter} row.
    %%
    %% The table is created lazily by peer_observer's `init/1', but
    %% pubsub_dispatcher workers spawn EARLIER in the boot chain
    %% (see macula_station_app:boot — pubsub_dispatcher at step ~145,
    %% peer_observer at step ~254). A worker that receives its first
    %% frame before peer_observer's init runs hits `ets:update_counter'
    %% on a non-existent named table, which raises `badarg' with
    %% `cause => id'. Without this guard the badarg propagates out of
    %% `worker_loop' and kills the dispatcher worker, which the
    %% supervisor respawns — but the frame in transit is LOST. This
    %% silently dropped every cross-pool PUBLISH whose 6-tuple recv
    %% path hit step 3's `record(_, recv_to_dispatch, _)' call before
    %% peer_observer was up.
    %%
    %% Lazy-init on the hot path so the first consumer ALSO wins the
    %% race against peer_observer. The catch is the belt-and-braces
    %% for any other window (peer_observer dying after init, owning
    %% the named table → table reaped → race to recreate).
    try ets:update_counter(?TABLE, Key, {2, 1}, {Key, 0}) of
        _ -> ok
    catch
        error:badarg ->
            init(),
            record_retry(Key)
    end.

record_retry(Key) ->
    try ets:update_counter(?TABLE, Key, {2, 1}, {Key, 0}) of
        _ -> ok
    catch
        _:_ -> ok
    end.

%% @doc Dump the current histogram state as a nested map. Phase 1
%% readout for the /telemetry/frames HTTP endpoint. ets:tab2list is
%% O(N) over total rows (~800 worst case), so cheap.
-spec snapshot() ->
    #{frame_type() := #{phase() := #{bucket() := non_neg_integer()}}}.
snapshot() ->
    Rows = ets:tab2list(?TABLE),
    lists:foldl(fun add_row/2, #{}, Rows).

%% @doc Drop every counter back to 0. Used by the readout endpoint when
%% the caller wants a window-since-reset view. Costs a single
%% `ets:delete_all_objects/1'; bounded by row count.
-spec reset() -> ok.
reset() ->
    ets:delete_all_objects(?TABLE),
    ok.

%%%-------------------------------------------------------------------
%%% Internals
%%%-------------------------------------------------------------------

bucket(Us) when Us <       50 -> below_50us;
bucket(Us) when Us <      100 -> below_100us;
bucket(Us) when Us <      250 -> below_250us;
bucket(Us) when Us <      500 -> below_500us;
bucket(Us) when Us <    1_000 -> below_1ms;
bucket(Us) when Us <    2_500 -> below_2_5ms;
bucket(Us) when Us <    5_000 -> below_5ms;
bucket(Us) when Us <   10_000 -> below_10ms;
bucket(Us) when Us <   25_000 -> below_25ms;
bucket(Us) when Us <   50_000 -> below_50ms;
bucket(Us) when Us <  100_000 -> below_100ms;
bucket(Us) when Us <  250_000 -> below_250ms;
bucket(Us) when Us <  500_000 -> below_500ms;
bucket(Us) when Us < 1_000_000 -> below_1s;
bucket(Us) when Us < 2_500_000 -> below_2_5s;
bucket(Us) when Us < 5_000_000 -> below_5s;
bucket(_Us)                    -> above_5s.

add_row({{FrameType, Phase, Bucket}, Count}, Acc) ->
    ByType  = maps:get(FrameType, Acc, #{}),
    ByPhase = maps:get(Phase, ByType, #{}),
    ByPhase2 = ByPhase#{Bucket => Count},
    Acc#{FrameType => ByType#{Phase => ByPhase2}}.
