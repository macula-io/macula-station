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

-export_type([opts/0, station_opts/0, station_cfg/0]).

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
        {data_dir,      "HECATE_STATION_DATA_DIR",      str,       required},
        {identity_file, "HECATE_STATION_IDENTITY_FILE", str,       optional},
        {bind,          "HECATE_STATION_BIND",          str,       required},
        {port,          "HECATE_STATION_PORT",          integer,   required},
        {certfile,      "HECATE_STATION_CERTFILE",      str,       required},
        {keyfile,       "HECATE_STATION_KEYFILE",       str,       required},
        {realms,        "HECATE_STATION_REALMS",        realms,    {optional, []}},
        {capabilities,  "HECATE_STATION_CAPABILITIES",  integer,   {optional, 0}}
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
raw_value(Key, EnvVar) ->
    choose_raw(os:getenv(EnvVar), application:get_env(hecate_station, Key),
               EnvVar, Key).

choose_raw(V,     _AppVal, EnvVar, _Key) when is_list(V) -> {ok, {{env, EnvVar}, V}};
choose_raw(false, {ok, V},  _EnvVar, Key) -> {ok, {{app_env, Key}, V}};
choose_raw(false, undefined, _EnvVar, _Key) -> absent.

%% Parse by declared type. Keep each clause total.
parse_value(V, str,     _Src) when is_list(V);  is_binary(V)  -> {ok, V};
parse_value(V, integer, _Src) when is_integer(V)              -> {ok, V};
parse_value(V, integer, Src)  when is_list(V)                 -> parse_int(V, Src);
parse_value(V, integer, Src)  when is_binary(V)               -> parse_int(binary_to_list(V), Src);
parse_value(V, realms,  _Src) when is_list(V)                 -> decode_realms(V);
parse_value(V, realms,  _Src) when is_binary(V)               -> decode_realms(binary_to_list(V));
parse_value(V, T,       Src)                                  -> {error, {bad_config, {parse, Src, T, V}}}.

parse_int(Str, Src) ->
    parse_int_result(string:to_integer(Str), Src).

parse_int_result({N, ""},       _Src) when is_integer(N) -> {ok, N};
parse_int_result({N, Rest},      Src) when is_integer(N)  -> {error, {bad_config, {parse, Src, integer, Rest}}};
parse_int_result({error, R},     Src)                     -> {error, {bad_config, {parse, Src, integer, R}}}.

%% Realms: list of 32-byte binaries, or comma-separated hex strings.
decode_realms([]) -> {ok, []};
decode_realms([H | _] = L) when is_integer(H) ->
    %% flat string of hex, comma-separated.
    decode_realm_parts(string:split(L, ",", all), []);
decode_realms(L) when is_list(L) ->
    decode_realm_list(L, []).

decode_realm_list([], Acc) -> {ok, lists:reverse(Acc)};
decode_realm_list([H | T], Acc) ->
    decode_realm_step(decode_one_realm(H), T, Acc).

decode_realm_step({ok, R}, T, Acc)          -> decode_realm_list(T, [R | Acc]);
decode_realm_step({error, _} = E, _T, _Acc) -> E.

decode_realm_parts([], Acc) -> {ok, lists:reverse(Acc)};
decode_realm_parts([""|T], Acc) -> decode_realm_parts(T, Acc);
decode_realm_parts([H|T], Acc)  ->
    decode_realm_step(decode_one_realm(H), T, Acc).

decode_one_realm(B) when is_binary(B), byte_size(B) =:= 32 -> {ok, B};
decode_one_realm(L) when is_list(L), length(L) =:= 64      -> decode_hex(L, <<>>);
decode_one_realm(V) -> {error, {bad_config, {realm, V}}}.

%% Pair-at-a-time hex decode. Returns {ok, <<_:256>>} | {error, _}.
decode_hex([], Acc) when byte_size(Acc) =:= 32 -> {ok, Acc};
decode_hex([A, B | Rest], Acc) ->
    decode_hex_step(nibble(A), nibble(B), Rest, Acc);
decode_hex(_, _) ->
    {error, {bad_config, bad_hex}}.

decode_hex_step({ok, H}, {ok, L}, Rest, Acc) ->
    decode_hex(Rest, <<Acc/binary, (H bsl 4 bor L)>>);
decode_hex_step(_, _, _, _) ->
    {error, {bad_config, bad_hex}}.

nibble(C) when C >= $0, C =< $9 -> {ok, C - $0};
nibble(C) when C >= $a, C =< $f -> {ok, C - $a + 10};
nibble(C) when C >= $A, C =< $F -> {ok, C - $A + 10};
nibble(_)                       -> error.

finalise_cfg_map(Map0) ->
    DataDir = maps:get(data_dir, Map0),
    Map     = ensure_identity_file(Map0, DataDir),
    #station_cfg{
        data_dir      = DataDir,
        identity_file = maps:get(identity_file, Map),
        bind          = maps:get(bind, Map),
        port          = maps:get(port, Map),
        certfile      = maps:get(certfile, Map),
        keyfile       = maps:get(keyfile, Map),
        realms        = maps:get(realms, Map, []),
        capabilities  = maps:get(capabilities, Map, 0)
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
