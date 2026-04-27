%% @doc Identity-list config parser.
%%
%% PLAN_MULTI_IDENTITY_RELAY §Phase 4. Reads the V1-compatible
%% `MACULA_RELAY_IDENTITIES' environment variable and turns it into
%% a list of `identity_spec()' maps that the multi-identity boot
%% pipeline feeds straight into
%% `hecate_station_identity_registry:register/2'.
%%
%% == Format ==
%%
%% Comma-separated identity entries; each entry is slash-separated
%% with five mandatory fields and one optional bind address:
%%
%% ```
%% hostname/city/country/lat/lng[/bind_addr]
%% '''
%%
%% Slash separator (not colon) so IPv6 addresses can be embedded
%% verbatim. Whitespace around individual entries is tolerated.
%% Empty entries (e.g. trailing commas) are ignored. A malformed
%% entry aborts the whole parse with `{error, {invalid_entry, ...}}'
%% — the caller decides whether that is fatal.
%%
%% == Example ==
%%
%% ```
%% MACULA_RELAY_IDENTITIES="\
%%   relay-be-leuven.macula.io/Leuven/BE/50.8798/4.7005/2a01:4f8:1c1f:8ab8::100,\
%%   relay-be-ghent.macula.io/Ghent/BE/51.0543/3.7174/2a01:4f8:1c1f:8ab8::101"
%% '''
%%
%% Yields:
%%
%% ```
%% [#{ hostname => &lt;&lt;"relay-be-leuven.macula.io"&gt;&gt;,
%%     city     => &lt;&lt;"Leuven"&gt;&gt;,
%%     country  => &lt;&lt;"BE"&gt;&gt;,
%%     lat      => 50.8798,
%%     lng      => 4.7005,
%%     bind     => &lt;&lt;"2a01:4f8:1c1f:8ab8::100"&gt;&gt; },
%%  #{...}]
%% '''
-module(hecate_station_identity_config).

-export([from_env/0,
         from_env/1,
         parse/1,
         %% Phase 5 — turn an identity_spec() into the full
         %% identity_opts() the registry/sup consume. Shared
         %% across the boot pipeline (station_app) and the admin
         %% API runtime add path.
         spec_to_opts/3,
         load_box_secret/0,
         shared_listener_opts/0,
         box_secret_path/0]).

-export_type([identity_spec/0]).

-type identity_spec() :: #{
    hostname := binary(),
    city     := binary(),
    country  := binary(),
    lat      := float(),
    lng      := float(),
    bind     := binary() | undefined
}.

-define(ENV_VAR, "MACULA_RELAY_IDENTITIES").

%%====================================================================
%% Public API
%%====================================================================

%% @doc Read + parse the standard `MACULA_RELAY_IDENTITIES' env var.
%% Empty or unset env returns `[]' — multi-identity is opt-in, the
%% caller falls back to the single-identity legacy boot path when
%% the list is empty.
-spec from_env() -> {ok, [identity_spec()]} | {error, term()}.
from_env() ->
    from_env(?ENV_VAR).

-spec from_env(string()) -> {ok, [identity_spec()]} | {error, term()}.
from_env(VarName) ->
    parse_env_value(os:getenv(VarName)).

parse_env_value(false) -> {ok, []};
parse_env_value("")    -> {ok, []};
parse_env_value(Value) -> parse(Value).

%% @doc Parse an in-memory env value (string or binary). Returns the
%% same shape as `from_env/0,1'.
-spec parse(string() | binary()) ->
    {ok, [identity_spec()]} | {error, {invalid_entry, string()}
                                    | {invalid_lat_lng, string(), string()}}.
parse(Bin) when is_binary(Bin) ->
    parse(unicode:characters_to_list(Bin));
parse(Str) when is_list(Str) ->
    Entries = string:split(Str, ",", all),
    parse_entries(Entries, []).

%%====================================================================
%% Internal
%%====================================================================

parse_entries([], Acc) ->
    {ok, lists:reverse(Acc)};
parse_entries([Entry | Rest], Acc) ->
    parse_entries_step(string:trim(Entry), Rest, Acc).

%% Empty entries (trailing commas, double commas) are silently
%% dropped — V1 parser tolerates them.
parse_entries_step("", Rest, Acc) ->
    parse_entries(Rest, Acc);
parse_entries_step(Trimmed, Rest, Acc) ->
    parse_entries_with_result(parse_entry(Trimmed), Rest, Acc).

parse_entries_with_result({ok, Spec}, Rest, Acc) ->
    parse_entries(Rest, [Spec | Acc]);
parse_entries_with_result({error, _} = E, _Rest, _Acc) ->
    E.

parse_entry(Entry) ->
    classify_fields(string:split(Entry, "/", all), Entry).

classify_fields([Host, City, Country, LatS, LngS, Addr], _Entry) ->
    build_spec(Host, City, Country, LatS, LngS, Addr);
classify_fields([Host, City, Country, LatS, LngS], _Entry) ->
    build_spec(Host, City, Country, LatS, LngS, undefined);
classify_fields(_, Entry) ->
    {error, {invalid_entry, Entry}}.

build_spec(Host, City, Country, LatS, LngS, AddrIn) ->
    on_lat_lng(Host, City, Country, AddrIn,
               parse_float(LatS), parse_float(LngS), LatS, LngS).

on_lat_lng(Host, City, Country, AddrIn, {ok, Lat}, {ok, Lng}, _, _) ->
    {ok, #{
        hostname => list_to_binary(Host),
        city     => list_to_binary(City),
        country  => list_to_binary(Country),
        lat      => Lat,
        lng      => Lng,
        bind     => bind_to_binary(AddrIn)
    }};
on_lat_lng(_Host, _City, _Country, _AddrIn, _, _, LatS, LngS) ->
    {error, {invalid_lat_lng, LatS, LngS}}.

bind_to_binary(undefined) -> undefined;
bind_to_binary(Str)       -> list_to_binary(Str).

parse_float(S) ->
    parse_float_step(string:to_float(S), S).

parse_float_step({F, []}, _S) when is_float(F) ->
    {ok, F};
parse_float_step({error, _}, S) ->
    parse_int(S);
parse_float_step({F, _Rest}, _S) when is_float(F) ->
    %% Fully-consume guard — partial float ("12.3xyz") is invalid.
    error.

parse_int(S) ->
    parse_int_step(string:to_integer(S), S).

parse_int_step({I, []}, _S) when is_integer(I) ->
    {ok, float(I)};
parse_int_step(_, _S) ->
    error.

%%====================================================================
%% Phase 5 — identity_spec() ⇒ identity_opts() construction
%%====================================================================

%% @doc Build the per-identity opts map the registry/sup consume from
%% an `identity_spec()' (parsed) plus a `box_secret' (loaded once for
%% the whole BEAM) plus the shared listener config (cert, key, port).
%%
%% Pure: derives keypair via
%% `hecate_station_identity_keys:derive/2'; constructs the
%% endpoint URL; falls back to the box-wide bind when the spec
%% does not provide a per-identity address.
-spec spec_to_opts(identity_spec(),
                   hecate_station_identity_keys:box_secret(),
                   #{port := inet:port_number(),
                     certfile := file:name_all(),
                     keyfile := file:name_all(),
                     _ => _}) ->
    hecate_station_identity_sup:identity_opts().
spec_to_opts(Spec, BoxSecret, Shared) ->
    Hostname = maps:get(hostname, Spec),
    Port     = maps:get(port, Shared),
    Kp       = hecate_station_identity_keys:derive(BoxSecret, Hostname),
    StationId = macula_identity:public(Kp),
    Endpoint = endpoint_for(Hostname, Port),
    Capabilities = maps:get(capabilities, Shared,
                    application:get_env(hecate_station, capabilities, 0)),
    %% Pass hostname/city/country/lat/lng through to identity_opts so
    %% the per-identity announcer can stamp them onto the published
    %% `node_record'. Without this the realm topology dashboard sees
    %% only the endpoint URL — the geo metadata silently drops here
    %% on its way from the parsed Spec to the announcer.
    #{
        identity_key => Hostname,
        identity     => Kp,
        bind         => bind_for(Spec, Shared),
        port         => Port,
        certfile     => maps:get(certfile, Shared),
        keyfile      => maps:get(keyfile, Shared),
        capabilities => Capabilities,
        realms       => [],
        station_id   => StationId,
        endpoint     => Endpoint,
        hostname     => Hostname,
        city         => maps:get(city,    Spec),
        country      => maps:get(country, Spec),
        lat          => maps:get(lat,     Spec),
        lng          => maps:get(lng,     Spec)
    }.

bind_for(#{bind := undefined}, Shared) ->
    %% No per-identity bind in the spec; fall back to the box-wide
    %% bind from app env. Useful for tests on a single loopback.
    application:get_env(hecate_station, bind,
                        maps:get(bind, Shared, "0.0.0.0"));
bind_for(#{bind := Addr}, _Shared) when is_binary(Addr) ->
    binary_to_list(Addr).

endpoint_for(Hostname, Port) ->
    iolist_to_binary([<<"quic://">>, Hostname,
                      $:, integer_to_binary(Port)]).

%%====================================================================
%% Phase 5 — app-env access for the box-secret + shared listener.
%% Both the boot pipeline (`hecate_station_app') and the admin API
%% runtime add path consume these.
%%====================================================================

%% @doc Load the box-secret from the configured path. Generates a
%% fresh 32-byte secret on first run.
-spec load_box_secret() ->
    {ok, hecate_station_identity_keys:box_secret()} | {error, term()}.
load_box_secret() ->
    hecate_station_identity_keys:load_or_generate_box_secret(box_secret_path()).

%% @doc Resolved path for the box-secret. Resolution order, first
%% hit wins:
%% <ol>
%%   <li>`application:get_env(hecate_station, box_secret_path)'</li>
%%   <li>OS env var `HECATE_STATION_BOX_SECRET_PATH'
%%       (used by the Dockerfile to point at the persistent volume
%%       mount inside the container)</li>
%%   <li>`$HOME/.hecate/box-secret'</li>
%% </ol>
-spec box_secret_path() -> file:name_all().
box_secret_path() ->
    classify_box_secret_path(
        application:get_env(hecate_station, box_secret_path),
        os:getenv("HECATE_STATION_BOX_SECRET_PATH")).

classify_box_secret_path({ok, P}, _Env) -> P;
classify_box_secret_path(undefined, false) -> default_box_secret_path();
classify_box_secret_path(undefined, "")    -> default_box_secret_path();
classify_box_secret_path(undefined, P)     -> P.

default_box_secret_path() ->
    Home = case os:getenv("HOME") of
               false -> "/tmp";
               H     -> H
           end,
    filename:join([Home, ".hecate", "box-secret"]).

%% @doc Read `port', `certfile', `keyfile' from the `hecate_station'
%% app env, with V1-compatible OS env-var fallback so hecate-station
%% drops into the existing macula-relay V1 docker-compose without
%% editing `sys.config'.
%%
%% Resolution order, first hit wins:
%% <ol>
%%   <li>`application:get_env(hecate_station, Key)'</li>
%%   <li>OS env var: `MACULA_QUIC_PORT' / `MACULA_TLS_CERTFILE'
%%       / `MACULA_TLS_KEYFILE' (V1 conventions)</li>
%% </ol>
%%
%% Each field is mandatory for the multi-identity boot path —
%% missing entries yield `{error, {missing_station_env, K}}'.
-spec shared_listener_opts() ->
    {ok, #{port := inet:port_number(),
           certfile := file:name_all(),
           keyfile := file:name_all()}}
  | {error, {missing_station_env, atom()}}.
shared_listener_opts() ->
    require_app_env([
        {port,     "MACULA_QUIC_PORT",     integer},
        {certfile, "MACULA_TLS_CERTFILE",  string},
        {keyfile,  "MACULA_TLS_KEYFILE",   string}
    ], #{}).

require_app_env([], Acc) ->
    {ok, Acc};
require_app_env([Spec | Rest], Acc) ->
    require_one(resolve_one(Spec), Spec, Rest, Acc).

resolve_one({Key, EnvVar, Type}) ->
    classify_resolution(application:get_env(hecate_station, Key),
                        os:getenv(EnvVar), Type).

classify_resolution({ok, V}, _Env, _Type) -> {ok, V};
classify_resolution(undefined, false, _Type) -> undefined;
classify_resolution(undefined, "",    _Type) -> undefined;
classify_resolution(undefined, Raw,   integer) -> parse_integer(Raw);
classify_resolution(undefined, Raw,   string)  -> {ok, Raw}.

parse_integer(Raw) ->
    parse_integer_step(string:to_integer(Raw)).

parse_integer_step({I, []}) when is_integer(I) -> {ok, I};
parse_integer_step(_)                          -> undefined.

require_one({ok, V}, {Key, _, _}, Rest, Acc) ->
    require_app_env(Rest, Acc#{Key => V});
require_one(undefined, {Key, _, _}, _Rest, _Acc) ->
    {error, {missing_station_env, Key}}.
