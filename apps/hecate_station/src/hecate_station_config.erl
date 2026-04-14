%% @doc Hecate Station configuration loader.
%%
%% Phase 1 surface:
%% <ul>
%%   <li>`load/1' — derive a `hecate_station' opts map from a partial spec
%%       plus environment defaults.</li>
%%   <li>`load_or_create_identity/1' — read the on-disk Ed25519 key file,
%%       generating + persisting one if absent.</li>
%% </ul>
-module(hecate_station_config).

-export([
    load/1,
    load_or_create_identity/1
]).

-export_type([opts/0, station_opts/0]).

-type opts() :: #{
    bind         => inet:ip_address() | string(),
    port         => inet:port_number(),
    certfile     => file:name_all(),
    keyfile      => file:name_all(),
    identity     => macula_identity:key_pair(),
    identity_file => file:name_all(),
    realms       => [macula_identity:pubkey()],
    capabilities => non_neg_integer()
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

%% @doc Resolve the full station opts from an input spec.
%% Loads or generates identity if a `identity_file' is present and
%% no inline `identity' was supplied.
-spec load(opts()) -> {ok, station_opts()} | {error, term()}.
load(Spec) ->
    resolve_identity(Spec).

resolve_identity(#{identity := _Kp} = Spec) ->
    finalise(Spec);
resolve_identity(#{identity_file := Path} = Spec) ->
    case load_or_create_identity(Path) of
        {ok, Kp} -> finalise(Spec#{identity => Kp});
        Err      -> Err
    end;
resolve_identity(_Spec) ->
    {error, identity_required}.

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

require_one(true,  _Key, Rest, Spec, K) -> require(Rest, Spec, K);
require_one(false,  Key, _Rest, _Spec, _K) -> {error, {missing, Key}}.

%% @doc Load an Ed25519 identity from disk, generating + persisting if absent.
-spec load_or_create_identity(file:name_all()) ->
    {ok, macula_identity:key_pair()} | {error, term()}.
load_or_create_identity(Path) ->
    materialise(macula_identity:load(Path), Path).

materialise({ok, Kp}, _Path) ->
    {ok, Kp};
materialise({error, enoent}, Path) ->
    Kp = macula_identity:generate(),
    case macula_identity:save(Path, Kp) of
        ok       -> {ok, Kp};
        Err      -> Err
    end;
materialise({error, _} = Err, _Path) ->
    Err.
