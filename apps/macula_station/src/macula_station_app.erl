%% @doc Application callback — orchestrates boot order.
%%
%% Single-station boot path (multi-identity removed 2026-05-04). One
%% macula-station container = one station-keypair = one DHT / SWIM /
%% observer / listener chain. Stations are realm-agnostic infrastructure;
%% the CONNECT handshake advertises no realm memberships. Realm-scoped
%% identities belong to daemons that may attach later, not to the
%% station itself.
%%
%% Boot sequence:
%%
%% 1. Start `macula_station_sup' (brings up `macula_station_peer_links'
%%    + `macula_station_record_fanout' as static children).
%% 2. If neither `macula_station' application env nor the
%%    `MACULA_STATION_CONFIG' env var is set, stop here —
%%    programmatic callers (walking-skeleton / chaos CT) drive
%%    `macula_station_server' directly.
%% 3. Load + validate station config via `macula_station_config:from_env/0'.
%% 4. Procedural-boot the runtime children in dependency order:
%%
%%        handler_registry → remote_advertise_registry →
%%        pubsub_registry → DHT (warm-load cache, run cascade) →
%%        dht_handlers → DHT custodians (replicate / republish /
%%        expire) → SWIM → observer → listener → record_fanout
%%        wiring + DHT callback install → announcer →
%%        content_announcer (when enabled) → bloom_exchange →
%%        peering_router → relay_ping →
%%        cache (optional) → rebootstrap (optional) → admin
%%        (optional).
%%
%% A boot failure at any step shuts the sup back down and returns the
%% reason to the application framework.
-module(macula_station_app).
-behaviour(application).

-include("macula_station_cfg.hrl").

-export([start/2, prep_stop/1, stop/1]).

%% Mesh-realm tag — protocol-internal events live under the all-zeros
%% realm. Used by `peering_router' and the announcer / fan-out path.
-define(MESH_REALM, <<0:256>>).

start(_StartType, _StartArgs) ->
    ok = macula_station_log_filters:install(),
    %% Create the frame-telemetry ETS table here so it's owned by the
    %% application controller and survives every supervisor restart.
    %% Previously this was created lazily by peer_observer's `init/1',
    %% which is FAR down the boot chain — pubsub_dispatcher workers
    %% spawn first and their `record/3' calls would hit a non-existent
    %% named table and crash with `badarg', silently dropping the
    %% frame in transit. See macula_station_frame_telemetry.
    ok = macula_station_frame_telemetry:init(),
    boot(macula_station_sup:start_link()).

prep_stop(State) ->
    _ = macula_station:prepare_shutdown(application_stop),
    State.

stop(_State) ->
    _ = macula_station:forget_dial_opts(),
    ok.

%%==================================================================
%% Boot pipeline — one step per head; no nesting.
%%==================================================================

boot({error, _} = E) ->
    E;
boot({ok, SupPid}) ->
    boot_enabled(SupPid, macula_station_config:enabled()).

boot_enabled(SupPid, false) ->
    {ok, SupPid};
boot_enabled(SupPid, true) ->
    boot_cfg(SupPid, macula_station_config:from_env()).

boot_cfg(SupPid, {error, Reason}) ->
    halt_sup(SupPid, Reason);
boot_cfg(SupPid, {ok, Cfg}) ->
    apply_bootstrap_env(Cfg),
    boot_handler_registry(SupPid, Cfg).

%% Push the JSON-supplied bootstrap config into `macula_bootstrap'
%% application env so `macula_bootstrap:run/0' picks it up. When the
%% JSON has `outbound_peers' but no explicit `bootstrap.discoverers',
%% the station defaults to a single `via_seed_dial' discoverer that
%% probes the peer-links registry — the typical fleet bootstrap
%% config.
%%
%% Skipped entirely when neither outbound_peers nor explicit
%% discoverers are configured (CT path with discoverers set in
%% sys.config or `application:set_env/3' elsewhere).
apply_bootstrap_env(#station_cfg{discoverers = [],
                                 outbound_peers = []}) ->
    ok;
apply_bootstrap_env(#station_cfg{discoverers = [],
                                 outbound_peers = [_ | _]}) ->
    application:set_env(macula_bootstrap, discoverers,
                        [{macula_station_via_seed_dial,
                          #{wait_ms => 3_000}}]),
    %% min_peers => 0 so the cascade accepts an empty via_seed_dial
    %% result. First-boot stations whose siblings aren't up yet need
    %% to come up anyway so the siblings can later dial in. The
    %% outbound link workers reconnect with backoff and SWIM /
    %% observer pick up peers organically once handshakes succeed.
    application:set_env(macula_bootstrap, cascade_opts,
                        #{min_peers => 0, timeout_ms => 30_000}),
    ok;
apply_bootstrap_env(#station_cfg{discoverers = Discoverers,
                                 bootstrap_cascade_opts = Opts}) ->
    application:set_env(macula_bootstrap, discoverers, Discoverers),
    case map_size(Opts) of
        0 -> ok;
        _ -> application:set_env(macula_bootstrap, cascade_opts, Opts)
    end,
    ok.

%%==================================================================
%% Registries — no inter-deps, started first so the DHT / observer
%% chain can wire to them.
%%==================================================================

boot_handler_registry(SupPid, Cfg) ->
    Spec = handler_registry_child(),
    on_handler_registry(SupPid, Cfg, supervisor:start_child(SupPid, Spec)).

on_handler_registry(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {handler_registry_start_failed, R});
on_handler_registry(SupPid, Cfg, {ok, _Pid}) ->
    boot_remote_advertise_registry(SupPid, Cfg).

boot_remote_advertise_registry(SupPid, Cfg) ->
    Spec = remote_advertise_registry_child(),
    on_remote_advertise_registry(SupPid, Cfg,
                                 supervisor:start_child(SupPid, Spec)).

on_remote_advertise_registry(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {remote_advertise_registry_start_failed, R});
on_remote_advertise_registry(SupPid, Cfg, {ok, _Pid}) ->
    boot_pubsub_registry(SupPid, Cfg).

boot_pubsub_registry(SupPid, #station_cfg{identity = Kp} = Cfg) ->
    Spec = pubsub_registry_child(Kp),
    on_pubsub_registry(SupPid, Cfg, supervisor:start_child(SupPid, Spec)).

on_pubsub_registry(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {pubsub_registry_start_failed, R});
on_pubsub_registry(SupPid, Cfg, {ok, _Pid}) ->
    boot_pubsub_dispatcher(SupPid, Cfg).

boot_pubsub_dispatcher(SupPid, Cfg) ->
    Spec = pubsub_dispatcher_child(),
    on_pubsub_dispatcher(SupPid, Cfg, supervisor:start_child(SupPid, Spec)).

on_pubsub_dispatcher(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {pubsub_dispatcher_start_failed, R});
on_pubsub_dispatcher(SupPid, Cfg, {ok, _Pid}) ->
    boot_outbound_links(SupPid, Cfg).

%%==================================================================
%% Outbound seed-dial links — start one `macula_station_outbound_link' worker
%% per configured outbound peer. They run in parallel with the rest
%% of the boot chain so they have time to handshake before the
%% bootstrap cascade's `seed_dial' tier probes the registry.
%% No-op when the JSON config carries no `outbound_peers' (e.g. CT
%% suites that drive the station programmatically with explicit
%% bootstrap tiers).
%%==================================================================

boot_outbound_links(SupPid, #station_cfg{outbound_peers = []} = Cfg) ->
    boot_dht(SupPid, Cfg);
boot_outbound_links(SupPid, #station_cfg{identity = Kp,
                                          capabilities = Caps,
                                          outbound_peers = Peers} = Cfg) ->
    Spec = outbound_links_sup_child(Kp, Caps, Peers),
    on_outbound_links(SupPid, Cfg, supervisor:start_child(SupPid, Spec)).

on_outbound_links(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {outbound_links_start_failed, R});
on_outbound_links(SupPid, Cfg, {ok, _Pid}) ->
    boot_dht(SupPid, Cfg).

%%==================================================================
%% DHT → cascade → custodians → SWIM → observer → listener.
%%==================================================================

boot_dht(SupPid, Cfg) ->
    on_dht(SupPid, Cfg, supervisor:start_child(SupPid, dht_child(Cfg))).

on_dht(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {dht_start_failed, R});
on_dht(SupPid, Cfg, {ok, DhtPid}) ->
    %% Warm-boot optimisation: seed the DHT from the on-disk cache
    %% BEFORE the cascade runs. A missing cache file is not an error
    %% — the cascade simply provides every peer. A corrupt cache is
    %% logged and ignored.
    _ = warm_load_cache(Cfg, DhtPid),
    boot_bootstrap(SupPid, Cfg, DhtPid,
                   macula_station_bootstrap_runner:run(DhtPid)).

warm_load_cache(#station_cfg{cache_cfg = undefined}, _Dht) ->
    ok;
warm_load_cache(#station_cfg{cache_cfg = #cache_cfg{path = Path}}, Dht) ->
    _ = macula_station_cache:load(Dht, Path),
    ok.

boot_bootstrap(SupPid, _Cfg, _DhtPid, {error, Reason}) ->
    halt_sup(SupPid, Reason);
boot_bootstrap(SupPid, Cfg, DhtPid, {ok, _Summary}) ->
    boot_dht_handlers(SupPid, Cfg, DhtPid).

boot_dht_handlers(SupPid, Cfg, DhtPid) ->
    HrPid = macula_handler_registry,
    Spec  = dht_handlers_child(HrPid, DhtPid),
    on_dht_handlers(SupPid, Cfg, DhtPid,
                    supervisor:start_child(SupPid, Spec)).

on_dht_handlers(SupPid, _Cfg, _DhtPid, {error, R}) ->
    halt_sup(SupPid, {dht_handlers_start_failed, R});
on_dht_handlers(SupPid, Cfg, DhtPid, {ok, _Pid}) ->
    boot_content_handlers(SupPid, Cfg, DhtPid).

boot_content_handlers(SupPid, Cfg, DhtPid) ->
    HrPid = macula_handler_registry,
    Spec  = content_handlers_child(HrPid),
    on_content_handlers(SupPid, Cfg, DhtPid,
                        supervisor:start_child(SupPid, Spec)).

on_content_handlers(SupPid, _Cfg, _DhtPid, {error, R}) ->
    halt_sup(SupPid, {content_handlers_start_failed, R});
on_content_handlers(SupPid, Cfg, DhtPid, {ok, _Pid}) ->
    boot_dht_custodians(SupPid, Cfg, DhtPid).

boot_dht_custodians(SupPid, Cfg, DhtPid) ->
    Specs = [dht_replicate_child(DhtPid),
             dht_republish_child(DhtPid, Cfg),
             dht_expire_child(DhtPid),
             dht_liveness_child(DhtPid)],
    boot_dht_custodians_each(SupPid, Cfg, DhtPid, Specs).

boot_dht_custodians_each(SupPid, Cfg, _DhtPid, []) ->
    boot_swim(SupPid, Cfg);
boot_dht_custodians_each(SupPid, Cfg, DhtPid, [Spec | Rest]) ->
    on_dht_custodian(SupPid, Cfg, DhtPid, Rest,
                     supervisor:start_child(SupPid, Spec)).

on_dht_custodian(SupPid, _Cfg, _DhtPid, _Rest, {error, R}) ->
    halt_sup(SupPid, {dht_custodian_start_failed, R});
on_dht_custodian(SupPid, Cfg, DhtPid, Rest, {ok, _Pid}) ->
    boot_dht_custodians_each(SupPid, Cfg, DhtPid, Rest).

boot_swim(SupPid, Cfg) ->
    on_swim(SupPid, Cfg, supervisor:start_child(SupPid, swim_child(Cfg))).

on_swim(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {swim_start_failed, R});
on_swim(SupPid, Cfg, {ok, SwimPid}) ->
    boot_observer(SupPid, Cfg, SwimPid).

boot_observer(SupPid, Cfg, SwimPid) ->
    {ok, DhtPid} = macula_station:dht(),
    Spec = observer_child(Cfg, DhtPid, SwimPid),
    on_observer(SupPid, Cfg, supervisor:start_child(SupPid, Spec)).

on_observer(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {observer_start_failed, R});
on_observer(SupPid, Cfg, {ok, ObserverPid}) ->
    boot_listener(SupPid, Cfg, ObserverPid).

boot_listener(SupPid, Cfg, ObserverPid) ->
    Spec = listener_child(Cfg, ObserverPid),
    on_listener(SupPid, Cfg, ObserverPid,
                supervisor:start_child(SupPid, Spec)).

on_listener(SupPid, _Cfg, _ObserverPid, {error, R}) ->
    halt_sup(SupPid, {listener_start_failed, R});
on_listener(SupPid, Cfg, ObserverPid, {ok, _Pid}) ->
    ok = macula_station:remember_dial_opts(dial_template(Cfg)),
    boot_record_fanout_wiring(SupPid, Cfg, ObserverPid).

%%==================================================================
%% Wire up record_fanout (was started in static children) + install
%% the DHT on_record_stored callback so puts fan out to local
%% subscribers via the singleton.
%%==================================================================

boot_record_fanout_wiring(SupPid, #station_cfg{identity = Kp} = Cfg,
                          ObserverPid) ->
    Reg = hecate_pubsub_registry,
    ok  = macula_station_record_fanout:set_wiring(
            #{pubsub_registry => Reg,
              peer_observer   => ObserverPid,
              identity        => Kp}),
    {ok, DhtPid} = macula_station:dht(),
    ok = macula_dht:set_on_record_stored(
           DhtPid,
           fun(Record) ->
               macula_station_record_fanout:on_record(Record)
           end),
    %% Phase 4 step 3 — install the outbound DHT transport now that
    %% the observer is registered. Without this, every wire-bound DHT
    %% call (ping_peer/find_node/find_value/send_store) returns
    %% {error, no_transport} immediately. See PLAN_DHT_TRANSPORT_WIRING.
    ok = macula_dht:set_identity(DhtPid, Kp),
    ok = macula_dht:set_send_frame(
           DhtPid,
           fun(NodeId, Frame) ->
               macula_station_dht_transport:send_frame(NodeId, Frame)
           end),
    boot_announcer(SupPid, Cfg, DhtPid, ObserverPid).

%%==================================================================
%% Cross-relay fabric — announcer, content_announcer, bloom_exchange,
%% peering_router, relay_ping.
%%==================================================================

boot_announcer(SupPid, Cfg, DhtPid, ObserverPid) ->
    Spec = announcer_child(Cfg, DhtPid, ObserverPid),
    on_announcer(SupPid, Cfg, DhtPid,
                 supervisor:start_child(SupPid, Spec)).

on_announcer(SupPid, _Cfg, _DhtPid, {error, R}) ->
    halt_sup(SupPid, {announcer_start_failed, R});
on_announcer(SupPid, Cfg, DhtPid, {ok, _Pid}) ->
    boot_content_announcer(SupPid, Cfg, DhtPid).

boot_content_announcer(SupPid, Cfg, DhtPid) ->
    Spec = content_announcer_child(Cfg, DhtPid),
    on_content_announcer(SupPid, Cfg,
                         supervisor:start_child(SupPid, Spec)).

on_content_announcer(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {content_announcer_start_failed, R});
on_content_announcer(SupPid, Cfg, {ok, _Pid}) ->
    boot_bloom_exchange(SupPid, Cfg).

boot_bloom_exchange(SupPid, Cfg) ->
    Spec = bloom_exchange_child(Cfg),
    on_bloom_exchange(SupPid, Cfg, supervisor:start_child(SupPid, Spec)).

on_bloom_exchange(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {bloom_exchange_start_failed, R});
on_bloom_exchange(SupPid, Cfg, {ok, _Pid}) ->
    boot_peering_router(SupPid, Cfg).

boot_peering_router(SupPid, Cfg) ->
    Spec = peering_router_child(Cfg),
    on_peering_router(SupPid, Cfg, supervisor:start_child(SupPid, Spec)).

on_peering_router(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {peering_router_start_failed, R});
on_peering_router(SupPid, Cfg, {ok, _Pid}) ->
    boot_relay_ping(SupPid, Cfg).

boot_relay_ping(SupPid, Cfg) ->
    Spec = relay_ping_child(Cfg),
    on_relay_ping(SupPid, Cfg, supervisor:start_child(SupPid, Spec)).

on_relay_ping(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {relay_ping_start_failed, R});
on_relay_ping(SupPid, Cfg, {ok, _Pid}) ->
    boot_health_publisher(SupPid, Cfg).

%% Periodic mailbox/heap beacon on `_mesh.health.v1'. Optional — if
%% the start fails (e.g. peer_links not yet alive), we still proceed
%% to the cache layer rather than aborting the whole boot. The
%% publisher itself tolerates having no peer connections at tick time.
boot_health_publisher(SupPid, Cfg) ->
    Spec = health_publisher_child(Cfg),
    case supervisor:start_child(SupPid, Spec) of
        {ok, _Pid} -> boot_cache(SupPid, Cfg);
        {error, _} -> boot_cache(SupPid, Cfg)
    end.

health_publisher_child(#station_cfg{identity = Kp}) ->
    Opts = #{identity => Kp},
    #{
        id       => macula_station_health_publisher,
        start    => {macula_station_health_publisher, start_link, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_health_publisher]
    }.

%%==================================================================
%% Optional periodic children — cache + rebootstrap + admin.
%%==================================================================

boot_cache(SupPid, #station_cfg{cache_cfg = undefined} = Cfg) ->
    boot_rebootstrap(SupPid, Cfg);
boot_cache(SupPid, #station_cfg{cache_cfg = CacheCfg} = Cfg) ->
    {ok, DhtPid} = macula_station:dht(),
    Spec = cache_child(DhtPid, CacheCfg),
    on_cache(SupPid, Cfg, supervisor:start_child(SupPid, Spec)).

on_cache(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {cache_start_failed, R});
on_cache(SupPid, Cfg, {ok, _Pid}) ->
    boot_rebootstrap(SupPid, Cfg).

boot_rebootstrap(SupPid, #station_cfg{rebootstrap_cfg = undefined} = Cfg) ->
    boot_admin(SupPid, Cfg);
boot_rebootstrap(SupPid, #station_cfg{rebootstrap_cfg = RbCfg} = Cfg) ->
    {ok, DhtPid} = macula_station:dht(),
    Spec = rebootstrap_child(DhtPid, RbCfg),
    on_rebootstrap(SupPid, Cfg, supervisor:start_child(SupPid, Spec)).

on_rebootstrap(SupPid, _Cfg, {error, R}) ->
    halt_sup(SupPid, {rebootstrap_start_failed, R});
on_rebootstrap(SupPid, Cfg, {ok, _Pid}) ->
    boot_admin(SupPid, Cfg).

boot_admin(SupPid, #station_cfg{admin_cfg = undefined}) ->
    {ok, SupPid};
boot_admin(SupPid, #station_cfg{admin_cfg = AdminCfg}) ->
    Spec = admin_sup_child(AdminCfg),
    on_admin(SupPid, supervisor:start_child(SupPid, Spec)).

on_admin(SupPid, {error, R}) ->
    halt_sup(SupPid, {admin_start_failed, R});
on_admin(SupPid, {ok, _Pid}) ->
    {ok, SupPid}.

%% Stations are realm-agnostic — the CONNECT handshake advertises no
%% realm memberships. A future `hecate-realm' / `macula-realm' service
%% dials the same peering layer with its own realms list.
dial_template(#station_cfg{identity = Kp, capabilities = C}) ->
    #{identity => Kp, realms => [], capabilities => C}.

halt_sup(SupPid, Reason) ->
    _ = macula_station:forget_dial_opts(),
    _ = catch exit(SupPid, shutdown),
    {error, Reason}.

%%==================================================================
%% Child specs.
%%==================================================================

handler_registry_child() ->
    #{
        id       => macula_handler_registry,
        start    => {macula_station_sup, start_handler_registry, [#{}]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_handler_registry]
    }.

remote_advertise_registry_child() ->
    #{
        id       => macula_remote_advertise_registry,
        start    => {macula_station_sup, start_remote_advertise_registry,
                     [#{}]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_remote_advertise_registry]
    }.

pubsub_registry_child(Kp) ->
    #{
        id       => hecate_pubsub_registry,
        start    => {macula_station_sup, start_pubsub_registry,
                     [#{identity => Kp}]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [hecate_pubsub_registry]
    }.

pubsub_dispatcher_child() ->
    Opts = #{pubsub_registry => hecate_pubsub_registry},
    #{
        id       => macula_station_route_pubsub_frames,
        start    => {macula_station_sup, start_route_pubsub_frames, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_route_pubsub_frames]
    }.

outbound_links_sup_child(Kp, Caps, Peers) ->
    Opts = #{identity => Kp, capabilities => Caps, peers => Peers},
    #{
        id       => macula_station_outbound_links_sup,
        start    => {macula_station_outbound_links_sup, start_link, [Opts]},
        restart  => permanent,
        shutdown => 10_000,
        type     => supervisor,
        modules  => [macula_station_outbound_links_sup]
    }.

dht_child(#station_cfg{identity = Kp}) ->
    Self = macula_identity:public(Kp),
    #{
        id       => macula_dht,
        start    => {macula_station_sup, start_dht, [#{self_id => Self}]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_dht]
    }.

dht_handlers_child(HrPid, DhtPid) ->
    Opts = #{handler_registry => HrPid, dht => DhtPid},
    #{
        id       => macula_station_dht_handlers,
        start    => {macula_station_sup, start_dht_handlers, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_dht_handlers]
    }.

content_handlers_child(HrPid) ->
    Opts = #{handler_registry => HrPid},
    #{
        id       => macula_station_content_handlers,
        start    => {macula_station_sup, start_content_handlers, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_content_handlers]
    }.

dht_replicate_child(DhtPid) ->
    #{
        id       => macula_dht_replicate,
        start    => {macula_station_sup, start_dht_replicate,
                     [#{dht => DhtPid}]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_dht_replicate]
    }.

dht_republish_child(DhtPid, #station_cfg{identity = Kp}) ->
    #{
        id       => macula_dht_republish,
        start    => {macula_station_sup, start_dht_republish,
                     [#{dht => DhtPid, identity => Kp}]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_dht_republish]
    }.

dht_expire_child(DhtPid) ->
    #{
        id       => macula_dht_expire,
        start    => {macula_station_sup, start_dht_expire,
                     [#{dht => DhtPid}]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_dht_expire]
    }.

%% Probes the least-recently-seen occupant of each FULL bucket and forgets it
%% only when it provably fails to answer. Without it a saturated bucket admits
%% nothing at all -- the admission score reduces to incumbency in production,
%% so a newcomer always loses and the bucket keeps whatever connected first
%% for the life of the process. See macula_dht_liveness for why only a timeout
%% counts as death.
dht_liveness_child(DhtPid) ->
    #{
        id       => macula_dht_liveness,
        start    => {macula_station_sup, start_dht_liveness,
                     [#{dht => DhtPid}]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_dht_liveness]
    }.

swim_child(#station_cfg{identity = Kp}) ->
    %% controlling_pid is a registered atom name (NOT a pid) because
    %% SWIM boots before `macula_station_peer_observer' (per the
    %% bootstrap order DHT → SWIM → observer). At each SWIM
    %% notification time we `whereis(macula_station_peer_observer)';
    %% before observer registers, notifications are dropped (SWIM
    %% keeps tracking, the next state change after observer comes
    %% up surfaces normally).
    %%
    %% Previously this was `whereis(macula_station_sup)' which sent
    %% SWIM notifications to the supervisor process — supervisors
    %% don't have a clause for `{macula_swim, member_state, _, _}',
    %% so every notification logged
    %% `Supervisor received unexpected message' and was dropped.
    SwimOpts = #{
        self_node_id    => macula_identity:public(Kp),
        identity        => Kp,
        controlling_pid => macula_station_peer_observer
    },
    #{
        id       => macula_swim,
        start    => {macula_station_sup, start_swim, [SwimOpts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_swim]
    }.

observer_child(#station_cfg{identity = Kp}, DhtPid, SwimPid) ->
    Opts = #{
        dht              => DhtPid,
        swim             => SwimPid,
        handler_registry => macula_handler_registry,
        pubsub_registry  => hecate_pubsub_registry,
        remote_advertise => macula_remote_advertise_registry,
        self_id          => macula_identity:public(Kp)
    },
    #{
        id       => macula_station_peer_observer,
        start    => {macula_station_sup, start_observer, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_peer_observer]
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
        realms       => [],
        capabilities => Caps,
        observer     => ObserverPid
    },
    #{
        id       => macula_station_listener,
        start    => {macula_station_sup, start_listener, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_listener]
    }.

announcer_child(#station_cfg{identity = Kp, capabilities = Caps,
                             bind = Bind, port = Port, hostname = Host,
                             city = City, country = Country,
                             lat = Lat, lng = Lng,
                             power_m = PowerM} = _Cfg,
                DhtPid, ObserverPid) ->
    Base = #{
        dht           => DhtPid,
        identity      => Kp,
        realms        => [],
        capabilities  => Caps,
        peer_observer => ObserverPid,
        hostname      => hostname_or_default(Host),
        bind          => bind_to_binary(Bind),
        %% Own QUIC port, published in the station_endpoint record so a
        %% resolver can dial us (direct-dial discovery, Slice 3).
        port          => Port
    },
    Opts = add_optional_geo(Base, #{city    => City,
                                    country => Country,
                                    lat     => Lat,
                                    lng     => Lng,
                                    power_m => PowerM}),
    #{
        id       => macula_station_announcer,
        start    => {macula_station_sup, start_announcer, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_announcer]
    }.

hostname_or_default(undefined)               -> station_hostname();
hostname_or_default(B) when is_binary(B)     -> B.

%% Drop `undefined' geo fields so the announcer only stamps
%% known-good values onto the node_record.
add_optional_geo(Opts, Geo) ->
    maps:fold(fun(_K, undefined, Acc) -> Acc;
                 (K,  V,         Acc) -> Acc#{K => V}
              end, Opts, Geo).

content_announcer_child(#station_cfg{identity = Kp, bind = Bind, port = Port},
                        DhtPid) ->
    Opts = #{
        dht        => DhtPid,
        identity   => Kp,
        station_id => macula_identity:public(Kp),
        endpoint   => endpoint_url(Bind, Port)
    },
    #{
        id       => macula_content_announcer,
        start    => {macula_station_sup, start_content_announcer, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_content_announcer]
    }.

bloom_exchange_child(#station_cfg{identity = Kp}) ->
    Opts = #{
        pubsub_registry => hecate_pubsub_registry,
        identity        => Kp
    },
    #{
        id       => macula_station_bloom_exchange,
        start    => {macula_station_sup, start_bloom_exchange, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_bloom_exchange]
    }.

peering_router_child(#station_cfg{identity = Kp}) ->
    %% No `realm' opt — the router enumerates every materialised realm
    %% via `hecate_pubsub_registry:list_realms/1' on each tick and
    %% reconciles the subscribe-on-peer set per (Realm, Topic, Peer)
    %% triple. Pre-Gap-B versions hard-coded ?MESH_REALM here, which
    %% silently dropped every user-realm topic on the floor.
    Opts = #{
        pubsub_registry => hecate_pubsub_registry,
        identity        => Kp
    },
    #{
        id       => macula_station_peering_router,
        start    => {macula_station_sup, start_peering_router, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_peering_router]
    }.

relay_ping_child(#station_cfg{identity = Kp}) ->
    Opts = #{
        handler_registry => macula_handler_registry,
        pubsub_registry  => hecate_pubsub_registry,
        identity         => Kp,
        hostname         => station_hostname()
    },
    #{
        id       => macula_station_relay_ping,
        start    => {macula_station_sup, start_relay_ping, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_relay_ping]
    }.

cache_child(DhtPid, CacheCfg) ->
    Opts = #{dht => DhtPid, cache => CacheCfg},
    #{
        id       => macula_station_cache,
        start    => {macula_station_sup, start_cache, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_cache]
    }.

rebootstrap_child(DhtPid, RbCfg) ->
    Opts = #{dht => DhtPid, rebootstrap => RbCfg},
    #{
        id       => macula_station_rebootstrap,
        start    => {macula_station_sup, start_rebootstrap, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_rebootstrap]
    }.

admin_sup_child(AdminCfg) ->
    #{
        id       => macula_station_admin_sup,
        start    => {macula_station_admin_sup, start_link,
                     [#{admin => AdminCfg}]},
        restart  => permanent,
        shutdown => 10_000,
        type     => supervisor,
        modules  => [macula_station_admin_sup]
    }.

%%==================================================================
%% Helpers
%%==================================================================

station_hostname() ->
    {ok, Name} = inet:gethostname(),
    list_to_binary(Name).

bind_to_binary(L) when is_list(L)  -> list_to_binary(L);
bind_to_binary(T) when is_tuple(T) -> list_to_binary(inet:ntoa(T)).

endpoint_url(Bind, Port) ->
    iolist_to_binary(["quic://", endpoint_host(Bind), ":",
                      integer_to_list(Port)]).

endpoint_host(L) when is_list(L)  -> L;
endpoint_host(T) when is_tuple(T) -> inet:ntoa(T).
