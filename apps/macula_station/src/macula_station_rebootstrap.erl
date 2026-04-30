%% @doc Partition-recovery watchdog.
%%
%% Polls `macula_dht:size/1' on a steady tick. When the routing table
%% stays below `min_viable_peers' for longer than the current
%% effective partition window, the watchdog fires exactly one
%% re-bootstrap (`macula_station_bootstrap_runner:run/2') and resets
%% its low-clock so it will not fire again until at least one more
%% window has elapsed.
%%
%% == Exponential back-off (Sprint B) ==
%%
%% Consecutive triggers under sustained partition widen the window:
%% the Nth consecutive retry waits `base × 2^min(N, max_shift)' ms
%% before firing. `max_shift' defaults to 4, so the window tops out
%% at 16× the configured base. A DHT recovery (size ≥
%% `min_viable_peers') resets the consecutive counter so the next
%% partition starts from the base window again.
%%
%% Rationale: short outages still retry quickly (base window). Long
%% outages stop hammering the cascade — after 4 consecutive retries
%% at the 60 s default base, the watchdog probes at 60 → 120 → 240
%% → 480 → 960 s intervals, then stays at 960 s until the DHT
%% recovers.
%%
%% == Lifecycle ==
%%
%% The re-bootstrap itself runs in a spawned helper process so the
%% gen_server's polling loop never blocks on the cascade. The helper
%% result is logged but otherwise ignored — if the cascade fails the
%% next tick will find the DHT still below threshold and start a new
%% countdown.
%%
%% == Configuration ==
%%
%% Defaults come from `#rebootstrap_cfg{}' (plan §4):
%% `min_viable_peers = 8', `check_period_ms = 5_000',
%% `partition_window_ms = 60_000'. All may be overridden at the
%% `{rebootstrap, #{...}}' application env key.
-module(macula_station_rebootstrap).
-behaviour(gen_server).

-include("macula_station_cfg.hrl").

-export([
    start_link/1, stop/1,
    state/1,
    force_tick/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, status/0]).

-define(MAX_BACKOFF_SHIFT, 4).

-type opts() :: #{
    dht             := pid(),
    rebootstrap     := macula_station_config:rebootstrap_cfg(),
    %% Passed through to `macula_station_bootstrap_runner:run/2'.
    %% `from_app_env' (default) reads the standard `macula_bootstrap'
    %% application env so tests can inject stub tiers without
    %% re-plumbing the watchdog.
    bootstrap_cfg   => macula_station_bootstrap_runner:cfg() | from_app_env,
    %% Optional observer for test assertions — receives
    %% `{macula_station_rebootstrap, rebootstrapped, Result}' on
    %% each fire and `{macula_station_rebootstrap, recovered, N}'
    %% when the DHT returns above the floor after `N' consecutive
    %% triggers.
    notify          => pid()
}.

-type status() :: #{
    size                := non_neg_integer(),
    min_viable_peers    := pos_integer(),
    low_since_ms        := integer() | undefined,
    triggers            := non_neg_integer(),
    consecutive         := non_neg_integer(),
    current_window_ms   := pos_integer()
}.

-record(state, {
    dht           :: pid(),
    cfg           :: macula_station_config:rebootstrap_cfg(),
    bootstrap_cfg :: macula_station_bootstrap_runner:cfg() | from_app_env,
    notify        :: pid() | undefined,
    low_since     :: integer() | undefined,
    triggers    = 0 :: non_neg_integer(),
    %% Consecutive triggers since the last recovery. Drives the
    %% back-off window; reset to 0 when the DHT returns above
    %% `min_viable_peers'.
    consecutive = 0 :: non_neg_integer()
}).

%%==================================================================
%% API
%%==================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Current watchdog status. Handy for `/status' + tests.
-spec state(pid()) -> status().
state(Pid) -> gen_server:call(Pid, state).

%% @doc Run one check synchronously (instead of waiting for the
%% periodic tick). Used by tests that do not want to sleep.
-spec force_tick(pid()) -> status().
force_tick(Pid) -> gen_server:call(Pid, force_tick).

%%==================================================================
%% gen_server
%%==================================================================

init(#{dht := Dht, rebootstrap := #rebootstrap_cfg{} = Cfg} = Opts)
  when is_pid(Dht) ->
    State = #state{
        dht           = Dht,
        cfg           = Cfg,
        bootstrap_cfg = maps:get(bootstrap_cfg, Opts, from_app_env),
        notify        = maps:get(notify, Opts, undefined)
    },
    schedule_tick(Cfg),
    {ok, State}.

handle_call(state, _From, S) ->
    {reply, status(S), S};
handle_call(force_tick, _From, S) ->
    NewS = run_tick(S),
    {reply, status(NewS), NewS};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) -> {noreply, S}.

handle_info(tick, #state{cfg = Cfg} = S) ->
    schedule_tick(Cfg),
    {noreply, run_tick(S)};
handle_info(_Msg, S) -> {noreply, S}.

terminate(_Reason, _S) -> ok.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%==================================================================
%% Tick logic
%%==================================================================

schedule_tick(#rebootstrap_cfg{check_period_ms = Ms}) ->
    erlang:send_after(Ms, self(), tick).

run_tick(#state{dht = Dht} = S) ->
    evaluate(macula_dht:size(Dht), S).

evaluate(Size, #state{cfg = #rebootstrap_cfg{min_viable_peers = Floor}} = S)
  when Size >= Floor ->
    on_healthy(S);
evaluate(_Size, #state{low_since = undefined} = S) ->
    S#state{low_since = now_ms()};
evaluate(_Size, #state{low_since = Since, consecutive = N,
                       cfg = #rebootstrap_cfg{partition_window_ms = Base}} = S) ->
    Window = effective_window(Base, N),
    maybe_trigger(now_ms() - Since >= Window, S).

on_healthy(#state{consecutive = 0} = S) ->
    %% Already healthy — just keep the clock clear.
    S#state{low_since = undefined};
on_healthy(#state{consecutive = N} = S) when N > 0 ->
    notify_recovery(S, N),
    S#state{low_since = undefined, consecutive = 0}.

maybe_trigger(false, S) ->
    S;
maybe_trigger(true,  #state{triggers = T, consecutive = N} = S) ->
    fire_rebootstrap(S),
    S#state{low_since   = undefined,
            triggers    = T + 1,
            consecutive = N + 1}.

%% Window growth: base × 2^min(N, MAX_BACKOFF_SHIFT).
%% `N' is the number of consecutive triggers taken WITHOUT a
%% recovery; the Nth +1 retry therefore waits the window named by N.
-spec effective_window(pos_integer(), non_neg_integer()) -> pos_integer().
effective_window(Base, N) when is_integer(Base), Base > 0,
                                is_integer(N), N >= 0 ->
    Shift = min(N, ?MAX_BACKOFF_SHIFT),
    Base bsl Shift.

fire_rebootstrap(#state{dht = Dht, bootstrap_cfg = Cfg, notify = Notify}) ->
    Parent = self(),
    _ = spawn(fun() ->
        Result = macula_station_bootstrap_runner:run(Dht, Cfg),
        notify_result(Notify, Parent, Result)
    end),
    ok.

notify_result(undefined, _Parent, _Result) -> ok;
notify_result(Pid, _Parent, Result) when is_pid(Pid) ->
    Pid ! {macula_station_rebootstrap, rebootstrapped, Result},
    ok.

notify_recovery(#state{notify = undefined}, _N) -> ok;
notify_recovery(#state{notify = Pid}, N) when is_pid(Pid) ->
    Pid ! {macula_station_rebootstrap, recovered, N},
    ok.

status(#state{dht = Dht, cfg = #rebootstrap_cfg{min_viable_peers = Floor,
                                                 partition_window_ms = Base},
              low_since = LS, triggers = T, consecutive = N}) ->
    #{size              => macula_dht:size(Dht),
      min_viable_peers  => Floor,
      low_since_ms      => LS,
      triggers          => T,
      consecutive       => N,
      current_window_ms => effective_window(Base, N)}.

now_ms() -> erlang:monotonic_time(millisecond).
