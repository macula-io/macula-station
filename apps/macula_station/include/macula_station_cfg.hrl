%% -*- erlang -*-
%% Typed station configuration record.
%%
%% Populated by `macula_station_config:from_env/0' from a single
%% JSON file pointed to by the `MACULA_STATION_CONFIG' env var.
%% Falls back to the application environment (sys.config) when no
%% config path is set, so the walking-skeleton / chaos CT suites
%% that drive the station programmatically still work.
%%
%% The record is the canonical shape; for back-compat with the
%% walking-skeleton API, `macula_station_config:to_opts/1' projects it
%% to the legacy `station_opts()' map.

-ifndef(MACULA_STATION_CFG_HRL).
-define(MACULA_STATION_CFG_HRL, true).

-record(station_cfg, {
    %% Data root — identity file, cache, tombstone log live under here.
    data_dir       :: file:name_all(),
    %% Full path to the Ed25519 identity blob (derived from data_dir
    %% when the operator does not override it).
    identity_file  :: file:name_all(),
    %% Loaded identity (filled by the loader after disk I/O).
    identity       :: macula_identity:key_pair() | undefined,
    %% QUIC listener.
    bind           :: inet:ip_address() | string(),
    port           :: inet:port_number(),
    certfile       :: file:name_all(),
    keyfile        :: file:name_all(),
    %% Peering capability bits advertised in the CONNECT handshake
    %% (`macula_frame' feature negotiation). Stations are
    %% realm-agnostic infrastructure per the railroad mental model —
    %% realm identity, admission, and overlay belong to a separate
    %% `hecate-realm' / `macula-realm' service — so no realm
    %% membership lives here.
    capabilities   = 0  :: non_neg_integer(),
    %% Routing-table cache — persists the DHT contacts to disk so
    %% warm boots can seed the DHT before the cascade runs.
    cache_cfg      = undefined :: cache_cfg() | undefined,
    %% Partition-recovery watchdog — triggers `macula_bootstrap:run/0'
    %% again when the DHT size stays below a floor for a sustained
    %% window.
    rebootstrap_cfg = undefined :: rebootstrap_cfg() | undefined,
    %% Admin HTTP API binding. When present, the boot pipeline
    %% starts a loopback-only httpd exposing `/status', `/dht/stats',
    %% `/swim/members', `/bootstrap/rerun'.
    admin_cfg       = undefined :: admin_cfg() | undefined,
    %% Optional geo block — surfaced into announce records so the
    %% realm topology can render station markers without a
    %% side-channel topology poll. All fields are optional; an
    %% unset value flows through as `undefined' and is dropped before
    %% the announcer constructs the node_record.
    hostname        = undefined :: binary() | undefined,
    city            = undefined :: binary() | undefined,
    country         = undefined :: binary() | undefined,
    lat             = undefined :: number() | undefined,
    lng             = undefined :: number() | undefined,
    %% Coverage-radius hint in meters. Topology dashboards render a
    %% station's "halo" with this radius. Operator-set per station
    %% (named tier in stations.csv → radius in meters); flows through
    %% the announce payload as `power_m` so the realm doesn't need a
    %% side-channel lookup. `undefined' → consumer falls back to its
    %% default radius (typically ~1200m).
    power_m         = undefined :: pos_integer() | undefined,
    %% Optional bootstrap block — translated into `macula_bootstrap'
    %% application env (`tiers' + `cascade_opts') by the app callback
    %% before the boot pipeline runs. Without this, the cascade has no
    %% sources and the station refuses to boot with `{error, no_tiers}'.
    bootstrap_tiers        = [] :: [{atom(), map()}],
    bootstrap_cascade_opts = #{} :: map(),
    %% Outbound peer URLs the station dials at boot.
    %% `macula_station_outbound_links_sup' spawns one
    %% `macula_station_outbound_link' worker per entry; the
    %% `seed_dial' bootstrap tier then reads the verified
    %% peers from `macula_station_peer_links'. Empty list
    %% means no outbound dial (the station only accepts inbound).
    outbound_peers         = [] :: [#{host := binary(),
                                      port := inet:port_number()}]
}).

%% Matches the shape of `application:get_env(macula_station, realms_cfg, [])'
%% and the `realms' field in PLAN_STATION_INTEGRATION §4.
-record(realm_cfg, {
    realm_id            :: <<_:256>>,
    roles               = [<<"member">>] :: [binary()],
    active_view_size    = 5  :: pos_integer(),
    passive_view_size   = 20 :: pos_integer(),
    plumtree_fanout     = 3  :: pos_integer()
}).

-record(cache_cfg, {
    %% Directory the cache lives in. A single `routing-table.erl.bin'
    %% file is written under this path.
    path              :: file:name_all(),
    flush_period_ms   = 30_000 :: pos_integer()
}).

-type cache_cfg() :: #cache_cfg{}.

-record(rebootstrap_cfg, {
    min_viable_peers    = 8      :: pos_integer(),
    check_period_ms     = 5_000  :: pos_integer(),
    partition_window_ms = 60_000 :: pos_integer()
}).

-type rebootstrap_cfg() :: #rebootstrap_cfg{}.

-record(admin_cfg, {
    %% Loopback-only binding; plan §8.6 defers TLS + client-cert
    %% auth to Phase 7. A value of 0 asks the OS for an ephemeral
    %% port (test fixtures rely on this).
    bind = "127.0.0.1" :: inet:ip_address() | string(),
    port = 8443        :: inet:port_number()
}).

-type admin_cfg() :: #admin_cfg{}.

-endif.
