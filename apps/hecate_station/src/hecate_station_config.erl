%% @doc Hecate Station configuration loader.
%%
%% Two entry points:
%% <ul>
%%   <li>`load/1' — in-VM use (walking skeleton + chaos CT). Takes a
%%       partial opts map, fills in identity, returns the legacy
%%       `station_opts()' map that `hecate_station_server' consumes.</li>
%%   <li>`from_env/0' — application-boot use. Reads `sys.config'
%%       under the `hecate_station' app, applies `HECATE_STATION_*'
%%       env-var overrides, loads or generates the identity, and
%%       returns a typed `#station_cfg{}' record.</li>
%% </ul>
%%
%% Supervised stations call `from_env/0' then project to the legacy
%% map via `to_opts/1'. Programmatic callers keep using `load/1'.
-module(hecate_station_config).

-include("hecate_station_cfg.hrl").

-export([
    load/1,
    load_or_create_identity/1,
    from_env/0,
    to_opts/1,
    enabled/0
]).

-export_type([opts/0, station_opts/0, station_cfg/0, realm_cfg/0,
              cache_cfg/0, rebootstrap_cfg/0, admin_cfg/0]).

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
    on_identity_loaded(hecate_station_identity:load_or_generate(Path), Spec);
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

%% @doc Back-compat shim — delegates to `hecate_station_identity'.
-spec load_or_create_identity(file:name_all()) ->
    {ok, macula_identity:key_pair()} | {error, term()}.
load_or_create_identity(Path) ->
    hecate_station_identity:load_or_generate(Path).

%%==================================================================
%% Application-env API — supervision tree.
%%==================================================================

%% @doc True when the application env or an `HECATE_STATION_BIND'
%% override is set. The supervisor tolerates unconfigured environments
%% so the walking-skeleton CT suites (which drive
%% `hecate_station_server' directly) still run under `rebar3 ct'.
-spec enabled() -> boolean().
enabled() ->
    application:get_env(hecate_station, bind) =/= undefined orelse
    os:getenv("HECATE_STATION_BIND") =/= false.

%% @doc Read `sys.config' + `HECATE_STATION_*' env-var overrides into a
%% typed station cfg. Loads or generates the identity from disk.
%%
%% Errors surface as `{error, {bad_config, Reason}}' (exact shape per
%% PLAN_STATION_INTEGRATION 8.1) so the supervisor can translate into
%% `{stop, {bad_config, Reason}}'.
-spec from_env() -> {ok, station_cfg()} | {error, {bad_config, term()}}.
from_env() ->
    continue_with_identity(build_cfg()).

continue_with_identity({ok, #station_cfg{identity_file = Path} = Cfg}) ->
    on_identity(hecate_station_identity:load_or_generate(Path), Cfg);
continue_with_identity({error, _} = E) ->
    E.

-spec build_cfg() -> {ok, station_cfg()} | {error, {bad_config, term()}}.
build_cfg() ->
    promote(read_many([
        {data_dir,      "HECATE_STATION_DATA_DIR",      str,               required},
        {identity_file, "HECATE_STATION_IDENTITY_FILE", str,               optional},
        {bind,          "HECATE_STATION_BIND",          str,               required},
        {port,          "HECATE_STATION_PORT",          integer,           required},
        {certfile,      "HECATE_STATION_CERTFILE",      str,               required},
        {keyfile,       "HECATE_STATION_KEYFILE",       str,               required},
        {capabilities,  "HECATE_STATION_CAPABILITIES",  integer,           {optional, 0}},
        {realms,        undefined,                      realm_specs,       {optional, []}},
        {cache,         undefined,                      cache_spec,        optional},
        {rebootstrap,   undefined,                      rebootstrap_spec,  optional},
        {admin,         undefined,                      admin_spec,        optional}
    ])).

promote({ok, Map})          -> {ok, finalise_cfg_map(Map)};
promote({error, _} = E)     -> E.

%% Thread a list of field specs, collecting values into a map.
-spec read_many([_]) -> {ok, map()} | {error, {bad_config, term()}}.
read_many(Specs) ->
    read_many(Specs, #{}).

read_many([], Acc) ->
    {ok, Acc};
read_many([Spec | Rest], Acc) ->
    read_many_step(read_spec(Spec), Spec, Rest, Acc).

read_many_step({ok, V},         {Key, _, _, _}, Rest, Acc) ->
    read_many(Rest, Acc#{Key => V});
read_many_step(absent,          _Spec,          Rest, Acc) ->
    read_many(Rest, Acc);
read_many_step({error, _} = E,  _Spec,          _Rest, _Acc) ->
    E.

read_spec({Key, EnvVar, Parse, Req}) ->
    materialise_spec(raw_value(Key, EnvVar), Key, Parse, Req).

materialise_spec({ok, {Src, Raw}}, Key, Parse, _Req) ->
    parse_value(Raw, Parse, {Src, Key});
materialise_spec(absent,            Key, _Parse, required) ->
    {error, {bad_config, {missing, Key}}};
materialise_spec(absent,           _Key, _Parse, optional) ->
    absent;
materialise_spec(absent,           _Key, _Parse, {optional, Default}) ->
    {ok, Default}.

%% Env var wins; otherwise application env. Tags the source for errors.
%% Some config fields have no `HECATE_STATION_*' override — the env-var
%% name is `undefined' for those; we skip the `os:getenv/1' path.
raw_value(Key, EnvVar) ->
    choose_raw(env_value(EnvVar), application:get_env(hecate_station, Key),
               EnvVar, Key).

env_value(undefined) -> false;
env_value(Name)      -> os:getenv(Name).

choose_raw(V,     _AppVal, EnvVar, _Key) when is_list(V) -> {ok, {{env, EnvVar}, V}};
choose_raw(false, {ok, V},  _EnvVar, Key) -> {ok, {{app_env, Key}, V}};
choose_raw(false, undefined, _EnvVar, _Key) -> absent.

%% Parse by declared type. Keep each clause total.
parse_value(V, str,     _Src) when is_list(V);  is_binary(V)  -> {ok, V};
parse_value(V, integer, _Src) when is_integer(V)              -> {ok, V};
parse_value(V, integer, Src)  when is_list(V)                 -> parse_int(V, Src);
parse_value(V, integer, Src)  when is_binary(V)               -> parse_int(binary_to_list(V), Src);
parse_value(V, realm_specs, Src) when is_list(V)              -> decode_realm_specs(V, Src);
parse_value(V, cache_spec,  Src) when is_map(V)               -> decode_cache_spec(V, Src);
parse_value(V, rebootstrap_spec, Src) when is_map(V)          -> decode_rebootstrap_spec(V, Src);
parse_value(V, admin_spec,  Src) when is_map(V)               -> decode_admin_spec(V, Src);
parse_value(V, T,       Src)                                  -> {error, {bad_config, {parse, Src, T, V}}}.

parse_int(Str, Src) ->
    parse_int_result(string:to_integer(Str), Src).

parse_int_result({N, ""},       _Src) when is_integer(N) -> {ok, N};
parse_int_result({N, Rest},      Src) when is_integer(N)  -> {error, {bad_config, {parse, Src, integer, Rest}}};
parse_int_result({error, R},     Src)                     -> {error, {bad_config, {parse, Src, integer, R}}}.

%% Map → #realm_cfg{} — one entry per configured realm.
decode_realm_specs(Specs, Src) ->
    decode_realm_specs(Specs, Src, []).

decode_realm_specs([], _Src, Acc) -> {ok, lists:reverse(Acc)};
decode_realm_specs([Spec | Rest], Src, Acc) ->
    decode_realm_specs_step(decode_realm_spec(Spec, Src), Rest, Src, Acc).

decode_realm_specs_step({ok, R},           Rest, Src, Acc) ->
    decode_realm_specs(Rest, Src, [R | Acc]);
decode_realm_specs_step({error, _} = E,   _Rest, _Src, _Acc) ->
    E.

decode_realm_spec(#{realm_id := Id} = Spec, _Src)
  when is_binary(Id), byte_size(Id) =:= 32 ->
    {ok, #realm_cfg{
        realm_id          = Id,
        roles             = maps:get(roles,             Spec, [<<"member">>]),
        active_view_size  = maps:get(active_view_size,  Spec, 5),
        passive_view_size = maps:get(passive_view_size, Spec, 20),
        plumtree_fanout   = maps:get(plumtree_fanout,   Spec, 3)
     }};
decode_realm_spec(Spec, Src) ->
    {error, {bad_config, {parse, Src, realm_specs, Spec}}}.

decode_cache_spec(#{path := Path} = M, _Src) ->
    {ok, #cache_cfg{
        path            = Path,
        flush_period_ms = maps:get(flush_period_ms, M, 30_000)
     }};
decode_cache_spec(M, Src) ->
    {error, {bad_config, {parse, Src, cache_spec, M}}}.

decode_rebootstrap_spec(M, _Src) when is_map(M) ->
    {ok, #rebootstrap_cfg{
        min_viable_peers    = maps:get(min_viable_peers,    M, 8),
        check_period_ms     = maps:get(check_period_ms,     M, 5_000),
        partition_window_ms = maps:get(partition_window_ms, M, 60_000)
     }}.

decode_admin_spec(M, _Src) when is_map(M) ->
    {ok, #admin_cfg{
        bind = maps:get(bind, M, "127.0.0.1"),
        port = maps:get(port, M, 8443)
     }}.

finalise_cfg_map(Map0) ->
    DataDir    = maps:get(data_dir, Map0),
    Map        = ensure_identity_file(Map0, DataDir),
    RealmsCfg  = maps:get(realms, Map, []),
    #station_cfg{
        data_dir         = DataDir,
        identity_file    = maps:get(identity_file, Map),
        bind             = maps:get(bind, Map),
        port             = maps:get(port, Map),
        certfile         = maps:get(certfile, Map),
        keyfile          = maps:get(keyfile, Map),
        %% Derive the flat pubkey list for the CONNECT handshake
        %% from the per-realm config rather than asking the operator
        %% to repeat themselves.
        realms           = [R#realm_cfg.realm_id || R <- RealmsCfg],
        capabilities     = maps:get(capabilities, Map, 0),
        realms_cfg       = RealmsCfg,
        cache_cfg        = maps:get(cache, Map, undefined),
        rebootstrap_cfg  = maps:get(rebootstrap, Map, undefined),
        admin_cfg        = maps:get(admin, Map, undefined)
    }.

ensure_identity_file(#{identity_file := _} = Map, _DataDir) -> Map;
ensure_identity_file(Map, DataDir) ->
    Map#{identity_file => hecate_station_identity:path_for(DataDir)}.

on_identity({ok, Kp}, Cfg)          -> {ok, Cfg#station_cfg{identity = Kp}};
on_identity({error, Reason}, _Cfg)  -> {error, {bad_config, {identity, Reason}}}.

%% @doc Project a typed station cfg into the legacy `station_opts()'
%% map consumed by `hecate_station_server:init/1'.
-spec to_opts(station_cfg()) -> station_opts().
to_opts(#station_cfg{bind = Bind, port = Port, certfile = C, keyfile = K,
                     identity = Id, realms = R, capabilities = Caps})
  when Id =/= undefined ->
    #{bind => Bind, port => Port, certfile => C, keyfile => K,
      identity => Id, realms => R, capabilities => Caps}.
