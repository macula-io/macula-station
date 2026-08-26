%% @doc Top-level station supervisor.
%%
%% Owns long-lived runtime processes (DHT, SWIM, listener, …), each
%% registered under a fixed local atom so the station API can look
%% them up:
%%
%% <ul>
%%   <li>`macula_dht' — S/Kademlia routing table server.</li>
%%   <li>`macula_swim' — SWIM failure detector.</li>
%%   <li>`macula_station_peer_observer' — peering glue.</li>
%%   <li>`macula_station_listener' — QUIC listener.</li>
%%   <li>`macula_station_cache' / `macula_station_rebootstrap' — optional.</li>
%%   <li>`macula_handler_registry' / `macula_remote_advertise_registry' /
%%       `hecate_pubsub_registry' — singleton procedure / advertise /
%%       pubsub registries.</li>
%%   <li>`macula_station_dht_handlers' — `_dht.*' RPC handlers.</li>
%%   <li>`macula_dht_replicate' / `macula_dht_republish' /
%%       `macula_dht_expire' — DHT custodians.</li>
%%   <li>`macula_station_announcer' — node_record publisher.</li>
%%   <li>`macula_content_announcer' — content manifest announcer.</li>
%%   <li>`macula_station_bloom_exchange' / `macula_station_peering_router' /
%%       `macula_station_relay_ping' — cross-relay fabric.</li>
%%   <li>`macula_station_peer_links' — outbound station_link registry.</li>
%% </ul>
%%
%% Most of these are NOT listed in `init/1'. They are added at boot
%% time by `macula_station_app:start/2', which enforces strict
%% bootstrap ordering (registries → DHT → cascade → SWIM → observer
%% → listener → fabric) per PLAN_STATION_INTEGRATION §8.2 + the
%% multi-identity rip-out (2026-05-04).
%%
%% Four children DO live in `init/1':
%%
%% <ul>
%%   <li>`macula_station_record_fanout' — node-singleton DHT
%%       record-stored fan-out. Boots in `unwired' mode; the boot
%%       pipeline calls `set_wiring/1' once `pubsub_registry' +
%%       `peer_observer' are up.</li>
%%   <li>`macula_station_peer_links' — outbound station_link
%%       registry. Empty until an outbound dialer registers links.</li>
%%   <li>`macula_station_event_dedup' — `(publisher, seq)' pubsub
%%       dedup cache.</li>
%%   <li>`macula_station_dht_dialer' — on-demand outbound dial for
%%       peers a DHT walk discovers but holds no connection to yet.
%%       Reads the station's dial template and the observer lazily,
%%       on first use.</li>
%% </ul>
%%
%% All four are config-independent (running them under a disabled
%% station env costs four idle gen_servers) so they live in `init/1'
%% rather than the boot pipeline.
%%
%% The walking-skeleton / chaos CT suites drive `macula_station_server'
%% directly via `macula_station:start_link/1' and never touch this
%% supervisor; they run in parallel in a single VM without colliding
%% with the registered names because the sup is not started.
-module(macula_station_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

%% Start wrappers — referenced from child specs built in
%% `macula_station_app'. They register the child pid under a fixed
%% local name so the station API can find it.
-export([start_dht/1, start_swim/1, start_observer/1, start_listener/1,
         start_cache/1, start_rebootstrap/1, start_peering_redundancy/1,
         start_handler_registry/1, start_remote_advertise_registry/1,
         start_pubsub_registry/1, start_route_pubsub_frames/1,
         start_dht_handlers/1,
         start_content_handlers/1,
         start_dht_replicate/1, start_dht_republish/1, start_dht_expire/1,
         start_dht_liveness/1,
         start_announcer/1, start_content_announcer/1,
         start_bloom_exchange/1,
         start_peering_router/1, start_relay_ping/1]).

-define(DHT_NAME,                        macula_dht).
-define(SWIM_NAME,                       macula_swim).
-define(OBSERVER_NAME,                   macula_station_peer_observer).
-define(LISTENER_NAME,                   macula_station_listener).
-define(CACHE_NAME,                      macula_station_cache).
-define(REBOOTSTRAP_NAME,                macula_station_rebootstrap).
-define(PEERING_REDUNDANCY_NAME,         macula_station_peering_redundancy).
-define(HANDLER_REGISTRY_NAME,           macula_handler_registry).
-define(REMOTE_ADVERTISE_REGISTRY_NAME,  macula_remote_advertise_registry).
-define(PUBSUB_REGISTRY_NAME,            hecate_pubsub_registry).
-define(DHT_HANDLERS_NAME,               macula_station_dht_handlers).
-define(CONTENT_HANDLERS_NAME,           macula_station_content_handlers).
-define(DHT_REPLICATE_NAME,              macula_dht_replicate).
-define(DHT_REPUBLISH_NAME,              macula_dht_republish).
-define(DHT_EXPIRE_NAME,                 macula_dht_expire).
-define(DHT_LIVENESS_NAME,               macula_dht_liveness).
-define(ANNOUNCER_NAME,                  macula_station_announcer).
-define(CONTENT_ANNOUNCER_NAME,          macula_content_announcer).
-define(BLOOM_EXCHANGE_NAME,             macula_station_bloom_exchange).
-define(PEERING_ROUTER_NAME,             macula_station_peering_router).
-define(ROUTE_PUBSUB_FRAMES_NAME,          macula_station_route_pubsub_frames).
-define(RELAY_PING_NAME,                 macula_station_relay_ping).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    {ok, {SupFlags, [peer_links_child(),
                     record_fanout_child(),
                     event_dedup_child(),
                     dht_dialer_child()]}}.

%% `(publisher, seq)' pubsub-event dedup cache. Config-independent
%% (one idle gen_server + a small ETS table when no traffic flows),
%% so it lives here in `init/1' rather than the boot pipeline — like
%% `peer_links' and `record_fanout'. Observe-only today (pubsub
%% Phase 2 step 2); becomes the cross-station loop kill in step 3.
event_dedup_child() ->
    #{
        id       => macula_station_event_dedup,
        start    => {macula_station_event_dedup, start_link, []},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_event_dedup]
    }.

%% Outbound station_link registry — empty until an outbound dialer
%% registers something. Started early because cross-relay fabric
%% modules query it on every tick.
peer_links_child() ->
    #{
        id       => macula_station_peer_links,
        start    => {macula_station_peer_links, start_link, []},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_peer_links]
    }.

%% Node-singleton fan-out for DHT record-stored events. Boots in
%% `unwired' mode; `macula_station_app' calls `set_wiring/1' once
%% the pubsub_registry + observer come up.
record_fanout_child() ->
    #{
        id       => macula_station_record_fanout,
        start    => {macula_station_record_fanout, start_link, []},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_record_fanout]
    }.

%% On-demand outbound dial for peers a DHT walk discovers mid-lookup but
%% holds no connection to yet. Config-independent at init time — like
%% `peer_links' and `record_fanout', it reads the station's dial template
%% and the observer lazily, on first use, not at boot.
dht_dialer_child() ->
    #{
        id       => macula_station_dht_dialer,
        start    => {macula_station_dht_dialer, start_link, []},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_dht_dialer]
    }.

%%==================================================================
%% Name-registering start wrappers.
%%==================================================================

-spec start_dht(macula_dht:opts()) -> {ok, pid()} | {error, term()}.
start_dht(Opts) ->
    register_result(macula_dht:start_link(Opts), ?DHT_NAME).

-spec start_swim(macula_swim:opts()) -> {ok, pid()} | {error, term()}.
start_swim(Opts) ->
    register_result(macula_swim:start_link(Opts), ?SWIM_NAME).

-spec start_observer(macula_station_peer_observer:opts()) ->
    {ok, pid()} | {error, term()}.
start_observer(Opts) ->
    register_result(macula_station_peer_observer:start_link(Opts),
                    ?OBSERVER_NAME).

-spec start_listener(macula_station_listener:opts()) ->
    {ok, pid()} | {error, term()}.
start_listener(Opts) ->
    register_result(macula_station_listener:start_link(Opts),
                    ?LISTENER_NAME).

-spec start_cache(macula_station_cache:opts()) ->
    {ok, pid()} | {error, term()}.
start_cache(Opts) ->
    register_result(macula_station_cache:start_link(Opts), ?CACHE_NAME).

-spec start_rebootstrap(macula_station_rebootstrap:opts()) ->
    {ok, pid()} | {error, term()}.
start_rebootstrap(Opts) ->
    register_result(macula_station_rebootstrap:start_link(Opts),
                    ?REBOOTSTRAP_NAME).

-spec start_peering_redundancy(macula_station_peering_redundancy:opts()) ->
    {ok, pid()} | {error, term()}.
start_peering_redundancy(Opts) ->
    register_result(macula_station_peering_redundancy:start_link(Opts),
                    ?PEERING_REDUNDANCY_NAME).

-spec start_handler_registry(map()) -> {ok, pid()} | {error, term()}.
start_handler_registry(Opts) ->
    register_result(macula_handler_registry:start_link(Opts),
                    ?HANDLER_REGISTRY_NAME).

-spec start_remote_advertise_registry(map()) -> {ok, pid()} | {error, term()}.
start_remote_advertise_registry(Opts) ->
    register_result(macula_remote_advertise_registry:start_link(Opts),
                    ?REMOTE_ADVERTISE_REGISTRY_NAME).

-spec start_pubsub_registry(map()) -> {ok, pid()} | {error, term()}.
start_pubsub_registry(Opts) ->
    register_result(hecate_pubsub_registry:start_link(Opts),
                    ?PUBSUB_REGISTRY_NAME).

-spec start_dht_handlers(macula_station_dht_handlers:opts()) ->
    {ok, pid()} | {error, term()}.
start_dht_handlers(Opts) ->
    register_result(macula_station_dht_handlers:start_link(Opts),
                    ?DHT_HANDLERS_NAME).

-spec start_content_handlers(macula_station_content_handlers:opts()) ->
    {ok, pid()} | {error, term()}.
start_content_handlers(Opts) ->
    register_result(macula_station_content_handlers:start_link(Opts),
                    ?CONTENT_HANDLERS_NAME).

-spec start_dht_replicate(map()) -> {ok, pid()} | {error, term()}.
start_dht_replicate(Opts) ->
    register_result(macula_dht_replicate:start_link(Opts),
                    ?DHT_REPLICATE_NAME).

-spec start_dht_republish(map()) -> {ok, pid()} | {error, term()}.
start_dht_republish(Opts) ->
    register_result(macula_dht_republish:start_link(Opts),
                    ?DHT_REPUBLISH_NAME).

-spec start_dht_expire(map()) -> {ok, pid()} | {error, term()}.
start_dht_expire(Opts) ->
    register_result(macula_dht_expire:start_link(Opts),
                    ?DHT_EXPIRE_NAME).

-spec start_dht_liveness(map()) -> {ok, pid()} | {error, term()}.
start_dht_liveness(Opts) ->
    register_result(macula_dht_liveness:start_link(Opts),
                    ?DHT_LIVENESS_NAME).

-spec start_announcer(macula_station_announcer:opts()) ->
    {ok, pid()} | {error, term()}.
start_announcer(Opts) ->
    register_result(macula_station_announcer:start_link(Opts),
                    ?ANNOUNCER_NAME).

-spec start_content_announcer(macula_content_announcer:opts()) ->
    {ok, pid()} | {error, term()}.
start_content_announcer(Opts) ->
    register_result(macula_content_announcer:start_link(Opts),
                    ?CONTENT_ANNOUNCER_NAME).

-spec start_bloom_exchange(macula_station_bloom_exchange:opts()) ->
    {ok, pid()} | {error, term()}.
start_bloom_exchange(Opts) ->
    register_result(macula_station_bloom_exchange:start_link(Opts),
                    ?BLOOM_EXCHANGE_NAME).

-spec start_peering_router(macula_station_peering_router:opts()) ->
    {ok, pid()} | {error, term()}.
start_peering_router(Opts) ->
    register_result(macula_station_peering_router:start_link(Opts),
                    ?PEERING_ROUTER_NAME).

-spec start_relay_ping(macula_station_relay_ping:opts()) ->
    {ok, pid()} | {error, term()}.
start_relay_ping(Opts) ->
    register_result(macula_station_relay_ping:start_link(Opts),
                    ?RELAY_PING_NAME).

-spec start_route_pubsub_frames(macula_station_route_pubsub_frames:opts()) ->
    {ok, pid()} | {error, term()}.
start_route_pubsub_frames(Opts) ->
    register_result(macula_station_route_pubsub_frames:start_link(Opts),
                    ?ROUTE_PUBSUB_FRAMES_NAME).

register_result({ok, Pid} = Ok, Name) ->
    ensure_registered(Name, Pid),
    Ok;
register_result({error, _} = E, _Name) ->
    E.

%% On restart the old Pid is dead and its registration is already
%% gone, but be defensive so a zombie name does not block the new
%% registration.
ensure_registered(Name, Pid) ->
    _ = catch unregister(Name),
    true = register(Name, Pid),
    ok.
