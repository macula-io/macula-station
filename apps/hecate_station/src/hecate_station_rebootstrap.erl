%% @doc Partition-recovery watchdog.
%%
%% Polls `hecate_dht:size/1' on a steady tick. When the routing table
%% stays below `min_viable_peers' for longer than
%% `partition_window_ms', the watchdog fires exactly one re-bootstrap
%% (`hecate_station_bootstrap_runner:run/2') and resets its state so
%% it will not fire again until the DHT next recovers above the
%% threshold and then drops back below it. This rate-limits retries
%% on a sustained partition instead of spinning a cascade every tick
%% (the PLAN_STATION_INTEGRATION §8.5 acceptance clause: "not a
%% tight loop").
%%
%% == Lifecycle ==
%%
%% The re-bootstrap itself runs in a spawned helper process so the
%% gen_server's polling loop never blocks on the cascade. The helper
%% result is logged but otherwise ignored — if the cascade fails the
%% next tick will simply find the DHT still below threshold and start
%% a new countdown.
%%
%% == Configuration ==
%%
%% Defaults come from `#rebootstrap_cfg{}' (plan §4):
%% `min_viable_peers = 8', `check_period_ms = 5_000',
%% `partition_window_ms = 60_000'. All may be overridden at the
%% `{rebootstrap, #{...}}' application env key.
-module(hecate_station_rebootstrap).
-behaviour(gen_server).

-include("hecate_station_cfg.hrl").

-export([
    start_link/1, stop/1,
    state/1,
    force_tick/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, status/0]).

-type opts() :: #{
    dht             := pid(),
    rebootstrap     := hecate_station_config:rebootstrap_cfg(),
    %% Passed through to `hecate_station_bootstrap_runner:run/2'.
    %% `from_app_env' (default) reads the standard `hecate_bootstrap'
    %% application env so tests can inject stub tiers without
    %% re-plumbing the watchdog.
    bootstrap_cfg   => hecate_station_bootstrap_runner:cfg() | from_app_env,
    %% Optional observer for test assertions — receives
    %% `{hecate_station_rebootstrap, rebootstrapped, Result}'.
    notify          => pid()
}.

-type status() :: #{
    size                := non_neg_integer(),
    min_viable_peers    := pos_integer(),
    low_since_ms        := integer() | undefined,
    triggers            := non_neg_integer()
}.

-record(state, {
    dht           :: pid(),
    cfg           :: hecate_station_config:rebootstrap_cfg(),
    bootstrap_cfg :: hecate_station_bootstrap_runner:cfg() | from_app_env,
    notify        :: pid() | undefined,
    low_since     :: integer() | undefined,
    triggers = 0  :: non_neg_integer()
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
    evaluate(hecate_dht:size(Dht), S).

evaluate(Size, #state{cfg = #rebootstrap_cfg{min_viable_peers = Floor}} = S)
  when Size >= Floor ->
    %% Healthy — clear any pending partition countdown.
    S#state{low_since = undefined};
evaluate(_Size, #state{low_since = undefined} = S) ->
    S#state{low_since = now_ms()};
evaluate(_Size, #state{low_since = Since,
                       cfg = #rebootstrap_cfg{partition_window_ms = Window}} = S) ->
    maybe_trigger(now_ms() - Since >= Window, S).

maybe_trigger(false, S) ->
    S;
maybe_trigger(true,  S) ->
    fire_rebootstrap(S),
    %% Reset to undefined — next trigger requires the DHT to first
    %% recover above the floor and then fall below it again.
    S#state{low_since = undefined,
            triggers  = S#state.triggers + 1}.

fire_rebootstrap(#state{dht = Dht, bootstrap_cfg = Cfg, notify = Notify}) ->
    Parent = self(),
    _ = spawn(fun() ->
        Result = hecate_station_bootstrap_runner:run(Dht, Cfg),
        notify_result(Notify, Parent, Result)
    end),
    ok.

notify_result(undefined, _Parent, _Result) -> ok;
notify_result(Pid, _Parent, Result) when is_pid(Pid) ->
    Pid ! {hecate_station_rebootstrap, rebootstrapped, Result},
    ok.

status(#state{dht = Dht, cfg = #rebootstrap_cfg{min_viable_peers = Floor},
              low_since = LS, triggers = T}) ->
    #{size             => hecate_dht:size(Dht),
      min_viable_peers => Floor,
      low_since_ms     => LS,
      triggers         => T}.

now_ms() -> erlang:monotonic_time(millisecond).
