%% @doc Deterministic per-identity Ed25519 keypair derivation.
%%
%% PLAN_MULTI_IDENTITY_RELAY §Phase 4. A box hosting N relay
%% identities needs N distinct Ed25519 keypairs. Storing N key files
%% on disk is operationally noisy (rotation, backup, copy-paste
%% across box rebuilds), so V1 derives them deterministically from
%% a single per-box secret + each identity's hostname:
%%
%% ```
%% seed     = HMAC-SHA256(box_secret, hostname)
%% (pub, priv) = Ed25519(seed)
%% '''
%%
%% Properties:
%%
%% <ul>
%%   <li>Same `(box_secret, hostname)' → same keypair forever.
%%       Restarts and rebuilds preserve the identity.</li>
%%   <li>Different hostname under the same box_secret → different
%%       keypair. No cross-identity collisions.</li>
%%   <li>Different box_secret → different keypairs. Boxes do not
%%       share identities even if they share a hostname (which they
%%       should not, but the property is useful when migrating).</li>
%%   <li>Knowledge of box_secret + hostnames lets an operator
%%       reconstruct every per-identity keypair without coordinating
%%       with the box. This is the recovery story.</li>
%% </ul>
%%
%% The box_secret itself is loaded once via `load_or_generate/1' from
%% a file path (typically `~/.hecate/box-secret'), generated on
%% first run, persisted with 0600 permissions.
-module(hecate_station_identity_keys).

-export([derive/2,
         load_or_generate_box_secret/1]).

-export_type([box_secret/0]).

-type box_secret() :: <<_:256>>.

%%====================================================================
%% Public API
%%====================================================================

%% @doc Derive a deterministic Ed25519 keypair for the given identity
%% hostname under the given box-secret.
-spec derive(box_secret(), binary() | string()) ->
    macula_identity:key_pair().
derive(BoxSecret, Hostname) when is_binary(BoxSecret),
                                 byte_size(BoxSecret) =:= 32 ->
    Seed = seed_for(BoxSecret, normalize(Hostname)),
    {Pub, Priv} = crypto:generate_key(eddsa, ed25519, Seed),
    #{public => Pub, private => Priv}.

%% @doc Load the box-secret from disk, or generate + persist a fresh
%% 32-byte secret on first run. Same atomic-write semantics as
%% `macula_identity:save/2': temp file + rename + 0600.
-spec load_or_generate_box_secret(file:name_all()) ->
    {ok, box_secret()} | {error, term()}.
load_or_generate_box_secret(Path) ->
    on_read(file:read_file(Path), Path).

%%====================================================================
%% Internal
%%====================================================================

normalize(Bin) when is_binary(Bin) -> Bin;
normalize(Str) when is_list(Str)   -> list_to_binary(Str).

seed_for(BoxSecret, Hostname) ->
    crypto:mac(hmac, sha256, BoxSecret, Hostname).

on_read({ok, Bin}, _Path) when byte_size(Bin) =:= 32 ->
    {ok, Bin};
on_read({ok, _Bin}, _Path) ->
    {error, malformed_box_secret};
on_read({error, enoent}, Path) ->
    generate_and_save(Path);
on_read({error, _} = E, _Path) ->
    E.

generate_and_save(Path) ->
    Secret = crypto:strong_rand_bytes(32),
    on_save(write_secret(Path, Secret), Secret).

on_save(ok,            Secret) -> {ok, Secret};
on_save({error, _} = E, _Sec)  -> E.

write_secret(Path, Secret) ->
    Tmp = Path ++ ".tmp",
    write_secret_step(file:write_file(Tmp, Secret), Tmp, Path).

write_secret_step({error, _} = E, _Tmp, _Path) ->
    E;
write_secret_step(ok, Tmp, Path) ->
    chmod_then_rename(file:change_mode(Tmp, 8#0600), Tmp, Path).

chmod_then_rename({error, _} = E, _Tmp, _Path) ->
    E;
chmod_then_rename(ok, Tmp, Path) ->
    file:rename(Tmp, Path).
