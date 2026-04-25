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
         parse/1]).

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
