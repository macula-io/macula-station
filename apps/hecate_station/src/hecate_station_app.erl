%% @doc Application callback — orchestrates boot order.
%%
%% Session 8.2 boot sequence:
%%
%% 1. Start `hecate_station_sup' (empty children list).
%% 2. If `hecate_station' application env is empty, stop here —
%%    programmatic callers (walking-skeleton / chaos CT) drive
%%    `hecate_station_server' directly.
%% 3. Load + validate station config via `hecate_station_config:from_env/0'.
%%    A parse error (`{error, {bad_config, Reason}}') shuts the sup
%%    back down and returns the reason to the application framework.
%% 4. Start the DHT child with `self_id' = station NodeId.
%% 5. Run the bootstrap cascade + ingest verified peers into the
%%    DHT's routing table
%%    (`hecate_station_bootstrap_runner:run/1'). A `no_tiers' or any
%%    other cascade error refuses to bring SWIM up — the sup is shut
%%    down with a clear reason (PLAN_STATION_INTEGRATION §8.2
%%    acceptance).
%% 6. Start the SWIM child with the loaded identity.
%%
%% The supervisor owns the children; this module owns the ordering.
%% Doing the cascade in the application callback (rather than in a
%% transient child) is what lets step 5 reliably block step 6.
-module(hecate_station_app).
-behaviour(application).

-include("hecate_station_cfg.hrl").

-export([start/2, prep_stop/1, stop/1]).

start(_StartType, _StartArgs) ->
    boot(hecate_station_sup:start_link()).

%% @doc `application:stop(hecate_station)' calls `prep_stop/1' before
%% the application master tears down the sup tree. We use the hook
%% to publish the tombstone + flush the cache so a clean
%% `application:stop/1' from an operator / release handler gives the
%% same effect as `hecate_station:shutdown/0' — minus the sup
%% teardown, which is the app master's job.
prep_stop(State) ->
    _ = hecate_station:prepare_shutdown(application_stop),
    State.

stop(_State) ->
    _ = hecate_station:forget_dial_opts(),
    ok.

%%==================================================================
%% Boot pipeline — one step per head; no nesting.
%%==================================================================

boot({error, _} = E) ->
    E;
boot({ok, SupPid}) ->
    boot_enabled(SupPid, hecate_station_config:enabled()).

boot_enabled(SupPid, false) ->
    {ok, SupPid};
boot_enabled(SupPid, true) ->
    boot_cfg(SupPid, hecate_station_config:from_env()).

boot_cfg(SupPid, {error, Reason}) ->
    halt_sup(SupPid, Reason);
boot_cfg(SupPid, {ok, Cfg}) ->
    boot_dht(SupPid, Cfg, supervisor:start_child(SupPid, dht_child(Cfg))).

boot_dht(SupPid, _Cfg, {error, Reason}) ->
    halt_sup(SupPid, {dht_start_failed, Reason});
boot_dht(SupPid, Cfg, {ok, DhtPid}) ->
    %% Warm-boot optimisation: seed the DHT from the on-disk cache
    %% BEFORE the cascade runs (PLAN_STATION_INTEGRATION §8.5
    %% acceptance). A missing cache file is not an error — the
    %% cascade simply provides every peer. A corrupt cache is logged
    %% and ignored.
    _ = warm_load_cache(Cfg, DhtPid),
    boot_bootstrap(SupPid, Cfg, hecate_station_bootstrap_runner:run(DhtPid)).

warm_load_cache(#station_cfg{cache_cfg = undefined}, _Dht) ->
    ok;
warm_load_cache(#station_cfg{cache_cfg = #cache_cfg{path = Path}}, Dht) ->
    _ = hecate_station_cache:load(Dht, Path),
    ok.

boot_bootstrap(SupPid, _Cfg, {error, Reason}) ->
    halt_sup(SupPid, Reason);
boot_bootstrap(SupPid, Cfg, {ok, _Summary}) ->
    boot_swim(SupPid, Cfg, supervisor:start_child(SupPid, swim_child(Cfg))).

boot_swim(SupPid, _Cfg, {error, Reason}) ->
    halt_sup(SupPid, {swim_start_failed, Reason});
boot_swim(SupPid, Cfg, {ok, SwimPid}) ->
    boot_observer(SupPid, Cfg, SwimPid).

boot_observer(SupPid, Cfg, SwimPid) ->
    {ok, DhtPid} = hecate_station:dht(),
    ObsSpec = observer_child(DhtPid, SwimPid),
    on_observer_started(SupPid, Cfg,
                        supervisor:start_child(SupPid, ObsSpec)).

on_observer_started(SupPid, _Cfg, {error, Reason}) ->
    halt_sup(SupPid, {observer_start_failed, Reason});
on_observer_started(SupPid, Cfg, {ok, ObserverPid}) ->
    boot_listener(SupPid, Cfg, ObserverPid).

boot_listener(SupPid, Cfg, ObserverPid) ->
    Spec = listener_child(Cfg, ObserverPid),
    on_listener_started(SupPid, Cfg, ObserverPid,
                        supervisor:start_child(SupPid, Spec)).

on_listener_started(SupPid, _Cfg, _ObserverPid, {error, Reason}) ->
    halt_sup(SupPid, {listener_start_failed, Reason});
on_listener_started(SupPid, Cfg, _ObserverPid, {ok, _ListenerPid}) ->
    ok = hecate_station:remember_dial_opts(dial_template(Cfg)),
    boot_cache(SupPid, Cfg).

%%==================================================================
%% Optional periodic children — cache + rebootstrap.
%%==================================================================

boot_cache(SupPid, #station_cfg{cache_cfg = undefined} = Cfg) ->
    boot_rebootstrap(SupPid, Cfg);
boot_cache(SupPid, #station_cfg{cache_cfg = CacheCfg} = Cfg) ->
    {ok, DhtPid} = hecate_station:dht(),
    Spec = cache_child(DhtPid, CacheCfg),
    on_cache_started(SupPid, Cfg, supervisor:start_child(SupPid, Spec)).

on_cache_started(SupPid, _Cfg, {error, Reason}) ->
    halt_sup(SupPid, {cache_start_failed, Reason});
on_cache_started(SupPid, Cfg, {ok, _CachePid}) ->
    boot_rebootstrap(SupPid, Cfg).

boot_rebootstrap(SupPid, #station_cfg{rebootstrap_cfg = undefined} = Cfg) ->
    boot_admin(SupPid, Cfg);
boot_rebootstrap(SupPid, #station_cfg{rebootstrap_cfg = RbCfg} = Cfg) ->
    {ok, DhtPid} = hecate_station:dht(),
    Spec = rebootstrap_child(DhtPid, RbCfg),
    on_rebootstrap_started(SupPid, Cfg,
                           supervisor:start_child(SupPid, Spec)).

on_rebootstrap_started(SupPid, _Cfg, {error, Reason}) ->
    halt_sup(SupPid, {rebootstrap_start_failed, Reason});
on_rebootstrap_started(SupPid, Cfg, {ok, _RbPid}) ->
    boot_admin(SupPid, Cfg).

boot_admin(SupPid, #station_cfg{admin_cfg = undefined}) ->
    {ok, SupPid};
boot_admin(SupPid, #station_cfg{admin_cfg = AdminCfg}) ->
    Spec = admin_sup_child(AdminCfg),
    on_admin_started(SupPid, supervisor:start_child(SupPid, Spec)).

on_admin_started(SupPid, {error, Reason}) ->
    halt_sup(SupPid, {admin_start_failed, Reason});
on_admin_started(SupPid, {ok, _AdminSupPid}) ->
    {ok, SupPid}.

%% Stations are realm-agnostic — the CONNECT handshake advertises no
%% realm memberships. A future `hecate-realm' / `macula-realm' service
%% dials the same peering layer with its own realms list.
dial_template(#station_cfg{identity = Kp, capabilities = C}) ->
    #{identity => Kp, realms => [], capabilities => C}.

halt_sup(SupPid, Reason) ->
    _ = hecate_station:forget_dial_opts(),
    _ = catch exit(SupPid, shutdown),
    {error, Reason}.

%%==================================================================
%% Child specs.
%%==================================================================

dht_child(#station_cfg{identity = Kp}) ->
    Self = macula_identity:public(Kp),
    #{
        id       => hecate_dht,
        start    => {hecate_station_sup, start_dht, [#{self_id => Self}]},
        restart  => permanent,
        shutdown => 5000,
        type     => worker,
        modules  => [hecate_dht]
    }.

swim_child(#station_cfg{identity = Kp}) ->
    SwimOpts = #{
        self_node_id    => macula_identity:public(Kp),
        identity        => Kp,
        %% SWIM frames arrive via the peer observer; membership state
        %% notifications are not routed anywhere yet (Session 8.6
        %% plugs them into `/status'). Sup is a safe sink until then.
        controlling_pid => whereis(hecate_station_sup)
    },
    #{
        id       => hecate_swim,
        start    => {hecate_station_sup, start_swim, [SwimOpts]},
        restart  => permanent,
        shutdown => 5000,
        type     => worker,
        modules  => [hecate_swim]
    }.

cache_child(DhtPid, CacheCfg) ->
    Opts = #{dht => DhtPid, cache => CacheCfg},
    #{
        id       => hecate_station_cache,
        start    => {hecate_station_sup, start_cache, [Opts]},
        restart  => permanent,
        shutdown => 5000,
        type     => worker,
        modules  => [hecate_station_cache]
    }.

rebootstrap_child(DhtPid, RbCfg) ->
    Opts = #{dht => DhtPid, rebootstrap => RbCfg},
    #{
        id       => hecate_station_rebootstrap,
        start    => {hecate_station_sup, start_rebootstrap, [Opts]},
        restart  => permanent,
        shutdown => 5000,
        type     => worker,
        modules  => [hecate_station_rebootstrap]
    }.

admin_sup_child(AdminCfg) ->
    #{
        id       => hecate_station_admin_sup,
        start    => {hecate_station_admin_sup, start_link,
                     [#{admin => AdminCfg}]},
        restart  => permanent,
        shutdown => 10_000,
        type     => supervisor,
        modules  => [hecate_station_admin_sup]
    }.

observer_child(DhtPid, SwimPid) ->
    Opts = #{dht => DhtPid, swim => SwimPid},
    #{
        id       => hecate_station_peer_observer,
        start    => {hecate_station_sup, start_observer, [Opts]},
        restart  => permanent,
        shutdown => 5000,
        type     => worker,
        modules  => [hecate_station_peer_observer]
    }.

listener_child(#station_cfg{bind = Bind, port = Port,
                            certfile = Cert, keyfile = Key,
                            identity = Kp, capabilities = Caps},
               ObserverPid) ->
    Opts = #{
        bind         => Bind,
        port         => Port,
        certfile     => Cert,
        keyfile      => Key,
        identity     => Kp,
        %% Stations advertise no realm memberships on the CONNECT
        %% handshake — realm identity lives in the `hecate-realm'
        %% / `macula-realm' service, not here.
        realms       => [],
        capabilities => Caps,
        observer     => ObserverPid
    },
    #{
        id       => hecate_station_listener,
        start    => {hecate_station_sup, start_listener, [Opts]},
        restart  => permanent,
        shutdown => 5000,
        type     => worker,
        modules  => [hecate_station_listener]
    }.
