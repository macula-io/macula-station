%% @doc Hecate Bootstrap — five-tier cascade orchestrator.
%%
%% Facade for the Phase 6 bootstrap cascade. Accepts an ordered list
%% of strategy modules (implementing `macula_bootstrap_peer_discoverer') and
%% cascade options, runs probes in parallel with staggered starts
%% per Part 5 §3, and returns the first tier's verified peers that
%% reach the `min_peers' threshold.
%%
%% == Example ==
%%
%% ```
%% Discoverers = [
%%   {macula_bootstrap_tier_e, #{peer_urls => [Url1, Url2, Url3]}}
%% ],
%% {ok, Peers} = macula_bootstrap:cascade(Discoverers, #{min_peers => 3}).
%% '''
%%
%% Peers are `macula_bootstrap_peer_discoverer:verified_peer()' maps — signature
%% and expiry have already been checked by the yielding tier. The
%% caller feeds them into the DHT routing table (`macula_dht:observe/2')
%% and subsequent discovery walks from there.
%%
%% Reference:
%% - plans/PLAN_MACULA_V2_PART5_BOOTSTRAP.md §3 (cascade invariants)
%% - plans/PLAN_MACULA_V2_PART7_IMPLEMENTATION.md §11 (Phase 6)
-module(macula_bootstrap).

-export([cascade/1, cascade/2, run/0, run/1]).

-export_type([discoverer_spec/0, cascade_opts/0, cascade_result/0,
              station_config/0, run_error/0]).

-type discoverer_spec() ::
        {module(), macula_bootstrap_peer_discoverer:discover_opts()}.

-type cascade_opts() :: #{
    %% `0' is a valid value: via_seed_dial may yield an empty list
    %% on first boot of a fresh fleet. The cascade must accept that
    %% and let boot proceed so the listener comes up and siblings
    %% can dial back in.
    min_peers  => non_neg_integer(),
    timeout_ms => non_neg_integer()
}.

-type cascade_result() ::
        {ok, [macula_bootstrap_peer_discoverer:verified_peer()]}
      | {error, cascade_failed | timeout}.

%% Config shape consumed by `run/0,1'. The same shape lives behind
%% `application:get_all_env(macula_bootstrap)' when `run/0' is used.
-type station_config() :: #{
    discoverers  := [discoverer_spec()],
    cascade_opts => cascade_opts()
}.

-type run_error() :: no_tiers | cascade_failed | timeout.

-define(DEFAULT_MIN_PEERS,  3).
-define(DEFAULT_TIMEOUT_MS, 60_000).

%% @doc Station-level entry point. Reads the discoverer list and
%% cascade options from `application:get_env(macula_bootstrap, _)'
%% and runs the cascade. Intended to be called once during station
%% boot; returned peers feed `macula_dht:observe/2'.
-spec run() -> {ok, [macula_bootstrap_peer_discoverer:verified_peer()]}
             | {error, run_error()}.
run() ->
    run(#{
        discoverers  => application:get_env(macula_bootstrap, discoverers, []),
        cascade_opts => application:get_env(macula_bootstrap, cascade_opts,
                                            #{})
    }).

%% @doc Variant of `run/0' taking an explicit config map — useful when
%% the station builds its config at runtime (different per-realm) or
%% when tests supply deterministic discoverer lists.
-spec run(station_config() | map()) ->
          {ok, [macula_bootstrap_peer_discoverer:verified_peer()]}
        | {error, run_error()}.
run(#{discoverers := []}) ->
    {error, no_tiers};
run(#{discoverers := Discoverers} = Cfg) when is_list(Discoverers) ->
    Opts = maps:get(cascade_opts, Cfg, #{}),
    cascade(Discoverers, Opts);
run(_Cfg) ->
    {error, no_tiers}.

%% @doc Run the cascade with default options (min 3 peers, 60s deadline).
-spec cascade([discoverer_spec()]) -> cascade_result().
cascade(Discoverers) -> cascade(Discoverers, #{}).

%% @doc Run the cascade.
%%
%% `Discoverers' is the ordered list of strategy modules to attempt.
%% Each probe runs in its own linked process, starting at
%% `Module:stagger_ms()' milliseconds after cascade start. The
%% orchestrator returns as soon as any probe yields at least
%% `min_peers' verified peers; remaining probes are cancelled.
-spec cascade([discoverer_spec()], cascade_opts()) -> cascade_result().
cascade([], _Opts) ->
    {error, cascade_failed};
cascade(Discoverers, Opts) when is_list(Discoverers) ->
    MinPeers  = maps:get(min_peers,  Opts, ?DEFAULT_MIN_PEERS),
    Timeout   = maps:get(timeout_ms, Opts, ?DEFAULT_TIMEOUT_MS),
    Deadline  = erlang:monotonic_time(millisecond) + Timeout,
    Workers   = start_probes(Discoverers),
    collect(Workers, #{}, MinPeers, Deadline).

%%------------------------------------------------------------------
%% Probe workers
%%------------------------------------------------------------------

start_probes(Discoverers) ->
    [start_probe(Mod, DiscoverOpts) || {Mod, DiscoverOpts} <- Discoverers].

start_probe(Mod, DiscoverOpts) ->
    Parent = self(),
    Stagger = Mod:stagger_ms(),
    {Pid, Ref} = spawn_monitor(
        fun() -> probe_worker(Parent, Mod, DiscoverOpts, Stagger) end),
    #{mod => Mod, pid => Pid, ref => Ref}.

probe_worker(Parent, Mod, DiscoverOpts, Stagger) ->
    case Stagger of
        0 -> ok;
        N -> timer:sleep(N)
    end,
    Parent ! {probe_result, self(), Mod:discover(DiscoverOpts)}.

%%------------------------------------------------------------------
%% Result collection
%%------------------------------------------------------------------

collect([], _PerTier, _MinPeers, _Deadline) ->
    {error, cascade_failed};
collect(Workers, PerTier, MinPeers, Deadline) ->
    Remaining = remaining_ms(Deadline),
    receive
        {probe_result, Pid, Result} ->
            handle_result(Pid, Result, Workers, PerTier, MinPeers, Deadline);
        {'DOWN', Ref, process, _Pid, Reason} ->
            handle_down(Ref, Reason, Workers, PerTier, MinPeers, Deadline)
    after Remaining ->
        cancel_workers(Workers),
        {error, timeout}
    end.

handle_result(Pid, {ok, Peers}, Workers, _PerTier, MinPeers, _Deadline)
  when length(Peers) >= MinPeers ->
    cancel_workers(Workers -- [worker_for_pid(Pid, Workers)]),
    drain_monitors(Workers),
    {ok, Peers};
handle_result(Pid, {ok, Peers}, Workers, PerTier, MinPeers, Deadline) ->
    %% Not enough peers from this tier; record and keep waiting
    %% in case a later tier clears the bar.
    collect(Workers, PerTier#{Pid => Peers}, MinPeers, Deadline);
handle_result(_Pid, {error, _Reason}, Workers, PerTier, MinPeers, Deadline) ->
    collect(Workers, PerTier, MinPeers, Deadline).

handle_down(Ref, _Reason, Workers, PerTier, MinPeers, Deadline) ->
    Remaining = [W || #{ref := R} = W <- Workers, R =/= Ref],
    collect(Remaining, PerTier, MinPeers, Deadline).

worker_for_pid(Pid, Workers) ->
    [W] = [Wk || #{pid := P} = Wk <- Workers, P =:= Pid],
    W.

cancel_workers(Workers) ->
    [cancel_worker(W) || W <- Workers].

cancel_worker(#{pid := Pid}) ->
    is_process_alive(Pid) andalso exit(Pid, cascade_done).

drain_monitors(Workers) ->
    [drain_monitor(W) || W <- Workers].

drain_monitor(#{ref := Ref}) when is_reference(Ref) ->
    receive
        {'DOWN', Ref, process, _, _} -> ok
    after 0 ->
        ok
    end.

remaining_ms(Deadline) ->
    Now = erlang:monotonic_time(millisecond),
    max(Deadline - Now, 0).
