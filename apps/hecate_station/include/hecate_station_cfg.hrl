%% -*- erlang -*-
%% Typed station configuration record.
%%
%% Populated by `hecate_station_config:from_env/0' from the application
%% environment (sys.config) with HECATE_STATION_* env-var overrides.
%% The record is the canonical shape; for back-compat with the
%% walking-skeleton API, `hecate_station_config:to_opts/1' projects it
%% to the legacy `station_opts()' map.

-ifndef(HECATE_STATION_CFG_HRL).
-define(HECATE_STATION_CFG_HRL, true).

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
    %% Peering policy — realm ids this station advertises membership
    %% of in the CONNECT handshake. `realms_cfg' below carries the
    %% per-realm protocol configuration (overlay caps + plumtree
    %% fan-out).
    realms         = [] :: [macula_identity:pubkey()],
    capabilities   = 0  :: non_neg_integer(),
    %% Per-realm overlay configuration. One entry per realm the
    %% station participates in; each spawns a
    %% `hecate_station_realm' child under `hecate_station_realm_sup'.
    realms_cfg     = [] :: [realm_cfg()]
}).

%% Matches the shape of `application:get_env(hecate_station, realms_cfg, [])'
%% and the `realms' field in PLAN_STATION_INTEGRATION §4.
-record(realm_cfg, {
    realm_id            :: <<_:256>>,
    roles               = [<<"member">>] :: [binary()],
    active_view_size    = 5  :: pos_integer(),
    passive_view_size   = 20 :: pos_integer(),
    plumtree_fanout     = 3  :: pos_integer()
}).

-type realm_cfg() :: #realm_cfg{}.

-endif.
