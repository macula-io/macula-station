%% @doc Hecate Station configuration loader.
%%
%% Two entry points:
%% <ul>
%%   <li>`load/1' — in-VM use (walking skeleton + chaos CT). Takes a
%%       partial opts map, fills in identity, returns the legacy
%%       `station_opts()' map that `macula_station_server' consumes.</li>
%%   <li>`from_env/0' — application-boot use. Reads a JSON config file
%%       pointed to by the `MACULA_STATION_CONFIG' env var, falling back
%%       to the application environment (sys.config) when the env var
%%       is unset (CT path), loads or generates the identity, and
%%       returns a typed `#station_cfg{}' record.</li>
%% </ul>
%%
%% Supervised stations call `from_env/0' then project to the legacy
%% map via `to_opts/1'. Programmatic callers keep using `load/1'.
%%
%% JSON config shape:
%% ```
%%   {
%%     "data_dir":     "/var/lib/macula/station",
%%     "identity_file": "/var/lib/macula/station/identity.erl.bin",
%%     "bind":         "2600:3c1a:e001:19::be:01",
%%     "port":         4433,
%%     "certfile":     "/certs/.../wildcard_.macula.io.crt",
%%     "keyfile":      "/certs/.../wildcard_.macula.io.key",
%%     "capabilities": 0,
%%     "cache":        { "path": "/var/lib/macula/station/cache",
%%                       "flush_period_ms": 30000 },
%%     "rebootstrap":  { ... },
%%     "admin":        { "bind": "127.0.0.1", "port": 8443 },
%%     "geo":          { "hostname": "station-be-brussels.macula.io",
%%                       "city":     "Brussels",
%%                       "country":  "BE",
%%                       "lat":      50.8503,
%%                       "lng":      4.3517 }
%%   }
%% '''
%% Required: `data_dir', `bind', `port', `certfile', `keyfile'.
%% Everything else is optional.
-module(macula_station_config).

-include("macula_station_cfg.hrl").

-export([
    load/1,
    load_or_create_identity/1,
    from_env/0,
    to_opts/1,
    enabled/0
]).

-export_type([opts/0, station_opts/0, station_cfg/0,
              cache_cfg/0, rebootstrap_cfg/0, admin_cfg/0]).

-define(CONFIG_ENV_VAR, "MACULA_STATION_CONFIG").

-type opts() :: #{
    bind          => inet:ip_address() | string(),
    port          => inet:port_number(),
    certfile      => file:name_all(),
    keyfile       => file:name_all(),
    identity      => macula_identity:key_pair(),
    identity_file => file:name_all(),
    realms        => [macula_identity:pubkey()],
    capabilities  => non_neg_integer()
}.

-type station_opts() :: #{
    bind         := inet:ip_address() | string(),
    port         := inet:port_number(),
    certfile     := file:name_all(),
    keyfile      := file:name_all(),
    identity     := macula_identity:key_pair(),
    realms       := [macula_identity:pubkey()],
    capabilities := non_neg_integer()
}.

-type station_cfg() :: #station_cfg{}.

%%==================================================================
%% Legacy map API — walking-skeleton / chaos CT.
%%==================================================================

%% @doc Resolve a partial opts map into a complete `station_opts()'.
%% Loads or generates the identity when the caller passes
%% `identity_file' instead of an inline `identity'.
-spec load(opts()) -> {ok, station_opts()} | {error, term()}.
load(Spec) ->
    resolve_identity(Spec).

resolve_identity(#{identity := _Kp} = Spec) ->
    finalise(Spec);
resolve_identity(#{identity_file := Path} = Spec) ->
    on_identity_loaded(macula_station_identity:load_or_generate(Path), Spec);
resolve_identity(_Spec) ->
    {error, identity_required}.

on_identity_loaded({ok, Kp}, Spec) -> finalise(Spec#{identity => Kp});
on_identity_loaded({error, _} = E, _Spec) -> E.

finalise(Spec) ->
    require([bind, port, certfile, keyfile, identity], Spec, fun apply_defaults/1).

apply_defaults(Spec) ->
    {ok, Spec#{
        realms       => maps:get(realms,       Spec, []),
        capabilities => maps:get(capabilities, Spec, 0)
    }}.

require([], Spec, K) ->
    K(Spec);
require([Key | Rest], Spec, K) ->
    require_one(maps:is_key(Key, Spec), Key, Rest, Spec, K).

require_one(true,  _Key, Rest, Spec, K)   -> require(Rest, Spec, K);
require_one(false,  Key, _Rest, _Spec, _K) -> {error, {missing, Key}}.

%% @doc Back-compat shim — delegates to `macula_station_identity'.
-spec load_or_create_identity(file:name_all()) ->
    {ok, macula_identity:key_pair()} | {error, term()}.
load_or_create_identity(Path) ->
    macula_station_identity:load_or_generate(Path).

%%==================================================================
%% Application-env API — supervision tree.
%%==================================================================

%% @doc True when either the JSON config env var is set, or the
%% application env carries a `bind'. The supervisor tolerates
%% unconfigured environments so the walking-skeleton CT suites
%% (which drive `macula_station_server' directly) still run under
%% `rebar3 ct'.
%%
%% Fails loudly if the legacy `MACULA_RELAY_IDENTITIES' var is
%% present (multi-identity removed 2026-05-04).
-spec enabled() -> boolean().
enabled() ->
    refuse_legacy_relay_identities(),
    has_json_config() orelse
    application:get_env(macula_station, bind) =/= undefined.

has_json_config() ->
    case os:getenv(?CONFIG_ENV_VAR) of
        false -> false;
        ""    -> false;
        _     -> true
    end.

refuse_legacy_relay_identities() ->
    refuse_legacy_var(os:getenv("MACULA_RELAY_IDENTITIES")).

refuse_legacy_var(false) -> ok;
refuse_legacy_var("")    -> ok;
refuse_legacy_var(_) ->
    error({legacy_multi_identity_env,
           <<"MACULA_RELAY_IDENTITIES is no longer supported. "
             "Each macula-station container hosts exactly one "
             "station-keypair. Configure with MACULA_STATION_CONFIG "
             "pointing to a JSON config file.">>}).

%% @doc Read JSON config + load identity. JSON path wins; otherwise
%% falls back to `application:get_env/2' on `macula_station' (CT path).
%%
%% Errors surface as `{error, {bad_config, Reason}}' (exact shape per
%% PLAN_STATION_INTEGRATION 8.1) so the supervisor can translate into
%% `{stop, {bad_config, Reason}}'.
-spec from_env() -> {ok, station_cfg()} | {error, {bad_config, term()}}.
from_env() ->
    continue_with_identity(build_cfg()).

continue_with_identity({ok, #station_cfg{identity_file = Path} = Cfg}) ->
    on_identity(macula_station_identity:load_or_generate(Path), Cfg);
continue_with_identity({error, _} = E) ->
    E.

-spec build_cfg() -> {ok, station_cfg()} | {error, {bad_config, term()}}.
build_cfg() ->
    case os:getenv(?CONFIG_ENV_VAR) of
        false -> from_app_env();
        ""    -> from_app_env();
        Path  -> from_json_file(Path)
    end.

%%==================================================================
%% JSON-file path (production).
%%==================================================================

from_json_file(Path) ->
    decode_or_err(file:read_file(Path), Path).

decode_or_err({ok, Bin}, Path) ->
    try json:decode(Bin) of
        M when is_map(M) -> assemble_from_json(M, Path);
        _Other           -> {error, {bad_config, {parse, Path, not_an_object}}}
    catch
        error:Reason -> {error, {bad_config, {parse, Path, Reason}}}
    end;
decode_or_err({error, Reason}, Path) ->
    {error, {bad_config, {read, Path, Reason}}}.

assemble_from_json(M, Path) ->
    require_keys([<<"data_dir">>, <<"bind">>, <<"port">>,
                  <<"certfile">>, <<"keyfile">>],
                 M, Path,
                 fun() -> {ok, build_record_from_json(M)} end).

require_keys([], _M, _Path, K) ->
    K();
require_keys([Key | Rest], M, Path, K) ->
    case maps:is_key(Key, M) of
        true  -> require_keys(Rest, M, Path, K);
        false -> {error, {bad_config, {missing, key_to_atom(Key)}}}
    end.

build_record_from_json(M) ->
    DataDir   = to_str(maps:get(<<"data_dir">>, M)),
    Geo       = maps:get(<<"geo">>,       M, #{}),
    Bootstrap = maps:get(<<"bootstrap">>, M, #{}),
    #station_cfg{
        data_dir               = DataDir,
        identity_file          = identity_file_or_default(M, DataDir),
        bind                   = to_str(maps:get(<<"bind">>, M)),
        port                   = maps:get(<<"port">>, M),
        certfile               = to_str(maps:get(<<"certfile">>, M)),
        keyfile                = to_str(maps:get(<<"keyfile">>, M)),
        capabilities           = maps:get(<<"capabilities">>, M, 0),
        cache_cfg              = decode_cache_json(maps:get(<<"cache">>, M, undefined)),
        rebootstrap_cfg        = decode_rebootstrap_json(maps:get(<<"rebootstrap">>, M, undefined)),
        admin_cfg              = decode_admin_json(maps:get(<<"admin">>, M, undefined)),
        hostname               = to_bin_or_undef(maps:get(<<"hostname">>, Geo, undefined)),
        city                   = to_bin_or_undef(maps:get(<<"city">>,     Geo, undefined)),
        country                = to_bin_or_undef(maps:get(<<"country">>,  Geo, undefined)),
        lat                    = to_number_or_undef(maps:get(<<"lat">>,   Geo, undefined)),
        lng                    = to_number_or_undef(maps:get(<<"lng">>,   Geo, undefined)),
        bootstrap_tiers        = decode_tiers_json(maps:get(<<"tiers">>,        Bootstrap, [])),
        bootstrap_cascade_opts = decode_atom_keyed(maps:get(<<"cascade_opts">>, Bootstrap, #{}))
    }.

%% JSON tier shape: [{"module": "macula_bootstrap_tier_e",
%%                    "opts":   {"peer_urls": ["quic://...", ...]}}, ...]
decode_tiers_json(L) when is_list(L) ->
    [decode_tier_json(T) || T <- L].

decode_tier_json(#{<<"module">> := ModBin} = T) ->
    Mod  = binary_to_existing_atom(ModBin, utf8),
    Opts = decode_atom_keyed(maps:get(<<"opts">>, T, #{})),
    {Mod, Opts}.

%% Convert binary-keyed JSON map to atom-keyed map (existing atoms
%% only — refuses to mint new ones). Tier opts and cascade opts are
%% read by Erlang code that expects atom keys; values are left
%% untouched.
decode_atom_keyed(M) when is_map(M) ->
    maps:fold(fun(K, V, Acc) -> Acc#{binary_to_existing_atom(K, utf8) => V} end,
              #{}, M).

identity_file_or_default(#{<<"identity_file">> := Path}, _DataDir) ->
    to_str(Path);
identity_file_or_default(_M, DataDir) ->
    macula_station_identity:path_for(DataDir).

decode_cache_json(undefined) -> undefined;
decode_cache_json(M) when is_map(M) ->
    #cache_cfg{
        path            = to_str(maps:get(<<"path">>, M)),
        flush_period_ms = maps:get(<<"flush_period_ms">>, M, 30_000)
    }.

decode_rebootstrap_json(undefined) -> undefined;
decode_rebootstrap_json(M) when is_map(M) ->
    #rebootstrap_cfg{
        min_viable_peers    = maps:get(<<"min_viable_peers">>,    M, 8),
        check_period_ms     = maps:get(<<"check_period_ms">>,     M, 5_000),
        partition_window_ms = maps:get(<<"partition_window_ms">>, M, 60_000)
    }.

decode_admin_json(undefined) -> undefined;
decode_admin_json(M) when is_map(M) ->
    #admin_cfg{
        bind = to_str(maps:get(<<"bind">>, M, "127.0.0.1")),
        port = maps:get(<<"port">>, M, 8443)
    }.

to_str(B) when is_binary(B) -> binary_to_list(B);
to_str(L) when is_list(L)   -> L.

to_bin_or_undef(undefined) -> undefined;
to_bin_or_undef(B) when is_binary(B) -> B;
to_bin_or_undef(L) when is_list(L)   -> list_to_binary(L).

to_number_or_undef(undefined) -> undefined;
to_number_or_undef(N) when is_number(N) -> N.

key_to_atom(<<"data_dir">>) -> data_dir;
key_to_atom(<<"bind">>)     -> bind;
key_to_atom(<<"port">>)     -> port;
key_to_atom(<<"certfile">>) -> certfile;
key_to_atom(<<"keyfile">>)  -> keyfile;
key_to_atom(B)              -> binary_to_atom(B, utf8).

%%==================================================================
%% sys.config / app-env path (CT fallback).
%%==================================================================

from_app_env() ->
    promote(read_many_app_env([
        {data_dir,      required},
        {identity_file, optional},
        {bind,          required},
        {port,          required},
        {certfile,      required},
        {keyfile,       required},
        {capabilities,  {optional, 0}},
        {cache,         optional},
        {rebootstrap,   optional},
        {admin,         optional}
    ])).

promote({ok, Map})          -> {ok, finalise_app_env(Map)};
promote({error, _} = E)     -> E.

read_many_app_env(Specs) ->
    read_many_app_env(Specs, #{}).

read_many_app_env([], Acc) ->
    {ok, Acc};
read_many_app_env([Spec | Rest], Acc) ->
    read_many_app_env_step(read_app_env(Spec), Spec, Rest, Acc).

read_many_app_env_step({ok, V},        {Key, _Req}, Rest, Acc) ->
    read_many_app_env(Rest, Acc#{Key => V});
read_many_app_env_step(absent,         _Spec,       Rest, Acc) ->
    read_many_app_env(Rest, Acc);
read_many_app_env_step({error, _} = E, _Spec,       _Rest, _Acc) ->
    E.

read_app_env({Key, Req}) ->
    materialise_app_env(application:get_env(macula_station, Key), Key, Req).

materialise_app_env({ok, V}, _Key, _Req) ->
    {ok, V};
materialise_app_env(undefined, Key, required) ->
    {error, {bad_config, {missing, Key}}};
materialise_app_env(undefined, _Key, optional) ->
    absent;
materialise_app_env(undefined, _Key, {optional, Default}) ->
    {ok, Default}.

finalise_app_env(Map0) ->
    DataDir = maps:get(data_dir, Map0),
    Map     = ensure_identity_file(Map0, DataDir),
    #station_cfg{
        data_dir         = DataDir,
        identity_file    = maps:get(identity_file, Map),
        bind             = maps:get(bind, Map),
        port             = ensure_int(maps:get(port, Map)),
        certfile         = maps:get(certfile, Map),
        keyfile          = maps:get(keyfile, Map),
        capabilities     = maps:get(capabilities, Map, 0),
        cache_cfg        = decode_cache_app_env(maps:get(cache, Map, undefined)),
        rebootstrap_cfg  = decode_rebootstrap_app_env(maps:get(rebootstrap, Map, undefined)),
        admin_cfg        = decode_admin_app_env(maps:get(admin, Map, undefined))
    }.

decode_cache_app_env(undefined)            -> undefined;
decode_cache_app_env(#cache_cfg{} = R)     -> R;
decode_cache_app_env(#{path := Path} = M)  ->
    #cache_cfg{
        path            = Path,
        flush_period_ms = maps:get(flush_period_ms, M, 30_000)
    }.

decode_rebootstrap_app_env(undefined)              -> undefined;
decode_rebootstrap_app_env(#rebootstrap_cfg{} = R) -> R;
decode_rebootstrap_app_env(M) when is_map(M) ->
    #rebootstrap_cfg{
        min_viable_peers    = maps:get(min_viable_peers,    M, 8),
        check_period_ms     = maps:get(check_period_ms,     M, 5_000),
        partition_window_ms = maps:get(partition_window_ms, M, 60_000)
    }.

decode_admin_app_env(undefined)         -> undefined;
decode_admin_app_env(#admin_cfg{} = R)  -> R;
decode_admin_app_env(M) when is_map(M)  ->
    #admin_cfg{
        bind = maps:get(bind, M, "127.0.0.1"),
        port = maps:get(port, M, 8443)
    }.

ensure_identity_file(#{identity_file := _} = Map, _DataDir) -> Map;
ensure_identity_file(Map, DataDir) ->
    Map#{identity_file => macula_station_identity:path_for(DataDir)}.

ensure_int(N) when is_integer(N) -> N;
ensure_int(L) when is_list(L)    -> list_to_integer(L);
ensure_int(B) when is_binary(B)  -> binary_to_integer(B).

%%==================================================================
%% Identity load / project.
%%==================================================================

on_identity({ok, Kp}, Cfg)          -> {ok, Cfg#station_cfg{identity = Kp}};
on_identity({error, Reason}, _Cfg)  -> {error, {bad_config, {identity, Reason}}}.

%% @doc Project a typed station cfg into the legacy `station_opts()'
%% map consumed by `macula_station_server:init/1'. The `realms' key
%% in that map is the realm-pubkey list the CONNECT handshake
%% advertises; stations are realm-agnostic infrastructure, so the
%% projection hard-codes it to `[]'.
-spec to_opts(station_cfg()) -> station_opts().
to_opts(#station_cfg{bind = Bind, port = Port, certfile = C, keyfile = K,
                     identity = Id, capabilities = Caps})
  when Id =/= undefined ->
    #{bind => Bind, port => Port, certfile => C, keyfile => K,
      identity => Id, realms => [], capabilities => Caps}.
