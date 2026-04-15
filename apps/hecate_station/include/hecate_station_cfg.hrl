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
    %% Peering policy.
    realms         = [] :: [macula_identity:pubkey()],
    capabilities   = 0  :: non_neg_integer()
}).

-endif.
