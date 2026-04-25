%% @doc Persistent station identity.
%%
%% A station's NodeId is an Ed25519 public key. The private half lives on
%% disk at `{data_dir}/identity.erl.bin' with mode `0600'. First boot
%% generates the pair; every subsequent boot loads the same file so the
%% NodeId is stable across restarts.
%%
%% Atomicity + permissions are provided by `hecate_identity:save/2'
%% (write-tmp + chmod 0600 + rename). This module owns the path
%% convention and the fallback policy.
-module(hecate_station_identity).

-export([
    path_for/1,
    load/1,
    generate/1,
    load_or_generate/1
]).

-define(IDENTITY_FILENAME, "identity.erl.bin").

-type data_dir() :: file:name_all().
-type kp()       :: hecate_identity:key_pair().

-export_type([data_dir/0]).

%%------------------------------------------------------------------
%% Path convention
%%------------------------------------------------------------------

%% @doc Canonical identity file path for a station data directory.
-spec path_for(data_dir()) -> file:name_all().
path_for(DataDir) ->
    filename:join(DataDir, ?IDENTITY_FILENAME).

%%------------------------------------------------------------------
%% Load — read-only.
%%------------------------------------------------------------------

%% @doc Load a persisted identity. Does NOT generate on miss.
-spec load(file:name_all()) -> {ok, kp()} | {error, term()}.
load(Path) ->
    hecate_identity:load(Path).

%%------------------------------------------------------------------
%% Generate — fresh key pair, persisted atomically with 0600 perms.
%%------------------------------------------------------------------

%% @doc Generate a fresh identity and persist it to `Path' atomically.
%% Creates missing parent directories; leaves the file at mode 0600.
-spec generate(file:name_all()) -> {ok, kp()} | {error, term()}.
generate(Path) ->
    persist(hecate_identity:generate(), Path).

-spec persist(kp(), file:name_all()) -> {ok, kp()} | {error, term()}.
persist(Kp, Path) ->
    on_save(hecate_identity:save(Path, Kp), Kp).

on_save(ok, Kp)            -> {ok, Kp};
on_save({error, _} = E, _) -> E.

%%------------------------------------------------------------------
%% Load-or-generate — the common station-boot idiom.
%%------------------------------------------------------------------

%% @doc Load `Path' if present; otherwise generate + persist a new
%% identity at that path. Missing directories are created.
-spec load_or_generate(file:name_all()) -> {ok, kp()} | {error, term()}.
load_or_generate(Path) ->
    materialise(hecate_identity:load(Path), Path).

materialise({ok, _} = Ok,       _Path) -> Ok;
materialise({error, enoent},     Path) -> generate(Path);
materialise({error, _} = Err,   _Path) -> Err.
