%% @doc Local content store — gen_server backed by the filesystem.
%%
%% Layout:
%% <pre>
%%   {base_dir}/
%%   ├── blocks/
%%   │   ├── 5d/
%%   │   │   └── 5d41402abc...c592.blk
%%   │   └── ...
%%   └── manifests/
%%       ├── 7f/
%%       │   └── 7f83b1657f...d65d.man
%%       └── ...
%% </pre>
%%
%% First two hex chars of the hash shard the directory tree so a
%% single dir never holds millions of entries.
%%
%% Two ETS indices held in process state — one for blocks, one for
%% manifests — track size + last-touched timestamp. They are
%% rebuilt by scanning the disk on startup.
%%
%% Hash verification is dual-algorithm: BLAKE3 first, SHA256 fallback.
%% The MCID encodes the algorithm in its codec byte but the store
%% does not consult that — it accepts whichever algorithm produces
%% a matching digest, which keeps the API stable across the v3
%% transition (new content is BLAKE3 by default but legacy SHA256
%% blocks remain valid).
-module(hecate_content_store).
-behaviour(gen_server).

-export([
    start_link/1, stop/0,
    %% Block ops
    put_block/2, get_block/1, has_block/1, delete_block/1,
    %% Manifest ops
    put_manifest/1, get_manifest/1, list_manifests/0, delete_manifest/1,
    %% Maintenance
    gc/0, stats/0, verify_integrity/0,
    %% Path helpers (testing)
    block_path/1, manifest_path/1,
    %% Eventing — pg group used for manifest_stored notifications
    events_group/0
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(BLOCK_EXT, ".blk").
-define(MANIFEST_EXT, ".man").
-define(CODEC_RAW,      16#55).
-define(CODEC_MANIFEST, 16#56).

-record(state, {
    base_dir       :: string(),
    blocks_dir     :: string(),
    manifests_dir  :: string(),
    block_index    :: ets:tid(),
    manifest_index :: ets:tid()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

-spec stop() -> ok.
stop() -> gen_server:stop(?SERVER).

-spec put_block(binary(), binary()) -> ok | {error, hash_mismatch}.
put_block(MCID, Data) -> gen_server:call(?SERVER, {put_block, MCID, Data}).

-spec get_block(binary()) ->
        {ok, binary()} | {error, not_found | hash_mismatch}.
get_block(MCID) -> gen_server:call(?SERVER, {get_block, MCID}).

-spec has_block(binary()) -> boolean().
has_block(MCID) -> gen_server:call(?SERVER, {has_block, MCID}).

-spec delete_block(binary()) -> ok.
delete_block(MCID) -> gen_server:call(?SERVER, {delete_block, MCID}).

-spec put_manifest(map()) -> ok.
put_manifest(Manifest) -> gen_server:call(?SERVER, {put_manifest, Manifest}).

-spec get_manifest(binary()) -> {ok, map()} | {error, not_found}.
get_manifest(MCID) -> gen_server:call(?SERVER, {get_manifest, MCID}).

-spec list_manifests() -> [binary()].
list_manifests() -> gen_server:call(?SERVER, list_manifests).

-spec delete_manifest(binary()) -> ok.
delete_manifest(MCID) -> gen_server:call(?SERVER, {delete_manifest, MCID}).

-spec gc() -> {ok, #{removed := non_neg_integer()}}.
gc() -> gen_server:call(?SERVER, gc, 60000).

-spec stats() -> map().
stats() -> gen_server:call(?SERVER, stats).

-spec verify_integrity() ->
        {ok, non_neg_integer()} | {error, {corrupted, [binary()]}}.
verify_integrity() -> gen_server:call(?SERVER, verify_integrity, 60000).

%% @doc Path helper for tests — uses the application-env default
%% base dir, not whatever a running server was configured with.
-spec block_path(binary()) -> string().
block_path(<<_:8, _:8, Hash:32/binary>>) ->
    block_path_in(Hash, default_base_dir()).

-spec manifest_path(binary()) -> string().
manifest_path(<<_:8, _:8, Hash:32/binary>>) ->
    manifest_path_in(Hash, default_base_dir()).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    BaseDir       = resolve_base_dir(Opts),
    BlocksDir     = filename:join(BaseDir, "blocks"),
    ManifestsDir  = filename:join(BaseDir, "manifests"),
    ok = ensure_dir_exists(BlocksDir),
    ok = ensure_dir_exists(ManifestsDir),
    State = #state{
        base_dir       = BaseDir,
        blocks_dir     = BlocksDir,
        manifests_dir  = ManifestsDir,
        block_index    = ets:new(content_block_index, [set, private]),
        manifest_index = ets:new(content_manifest_index, [set, private])
    },
    rebuild_index(State),
    {ok, State}.

handle_call({put_block, MCID, Data}, _From, S) ->
    {reply, do_put_block(MCID, Data, S), S};
handle_call({get_block, MCID}, _From, S) ->
    {reply, do_get_block(MCID, S), S};
handle_call({has_block, <<_:8, _:8, Hash:32/binary>>}, _From, S) ->
    {reply, ets:member(S#state.block_index, Hash), S};
handle_call({delete_block, MCID}, _From, S) ->
    {reply, do_delete_block(MCID, S), S};
handle_call({put_manifest, M}, _From, S) ->
    {reply, do_put_manifest(M, S), S};
handle_call({get_manifest, MCID}, _From, S) ->
    {reply, do_get_manifest(MCID, S), S};
handle_call(list_manifests, _From, S) ->
    Hashes = ets:tab2list(S#state.manifest_index),
    {reply, [<<1, ?CODEC_MANIFEST, H/binary>> || {H, _} <- Hashes], S};
handle_call({delete_manifest, MCID}, _From, S) ->
    {reply, do_delete_manifest(MCID, S), S};
handle_call(gc, _From, S) ->
    {reply, do_gc(S), S};
handle_call(stats, _From, S) ->
    {reply, do_stats(S), S};
handle_call(verify_integrity, _From, S) ->
    {reply, do_verify_integrity(S), S}.

handle_cast(_, S) -> {noreply, S}.
handle_info(_, S) -> {noreply, S}.
terminate(_, _) -> ok.

%%====================================================================
%% Block ops
%%====================================================================

do_put_block(<<_:8, _:8, Hash:32/binary>>, Data, S) ->
    case verify_data_hash(Data, Hash) of
        ok    -> write_block(Hash, Data, S);
        Error -> Error
    end.

verify_data_hash(Data, Hash) ->
    case match_any_hash(Data, Hash) of
        true  -> ok;
        false -> {error, hash_mismatch}
    end.

match_any_hash(Data, Hash) ->
    hecate_content_hasher:hash(blake3, Data) =:= Hash
      orelse hecate_content_hasher:hash(sha256, Data) =:= Hash.

write_block(Hash, Data, S) ->
    Path = block_path_in(Hash, S#state.base_dir),
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, Data),
    ets:insert(S#state.block_index,
               {Hash, {byte_size(Data), erlang:system_time(second)}}),
    ok.

do_get_block(<<_:8, _:8, Hash:32/binary>>, S) ->
    fetch_block(ets:member(S#state.block_index, Hash), Hash, S).

fetch_block(false, _Hash, _S) ->
    {error, not_found};
fetch_block(true, Hash, S) ->
    Path = block_path_in(Hash, S#state.base_dir),
    read_and_verify(file:read_file(Path), Hash).

read_and_verify({error, _}, _Hash) ->
    {error, not_found};
read_and_verify({ok, Data}, Hash) ->
    verify_returned_data(match_any_hash(Data, Hash), Data).

verify_returned_data(true,  Data) -> {ok, Data};
verify_returned_data(false, _Data) -> {error, hash_mismatch}.

do_delete_block(<<_:8, _:8, Hash:32/binary>>, S) ->
    Path = block_path_in(Hash, S#state.base_dir),
    file:delete(Path),
    ets:delete(S#state.block_index, Hash),
    ok.

%%====================================================================
%% Manifest ops
%%====================================================================

do_put_manifest(Manifest, S) ->
    <<_:8, _:8, Hash:32/binary>> = MCID = maps:get(mcid, Manifest),
    Path = manifest_path_in(Hash, S#state.base_dir),
    ok = filelib:ensure_dir(Path),
    {ok, Encoded} = hecate_content_manifest:encode(Manifest),
    ok = file:write_file(Path, Encoded),
    Info = {maps:get(name, Manifest),
            maps:get(size, Manifest),
            maps:get(chunk_count, Manifest),
            erlang:system_time(second)},
    ets:insert(S#state.manifest_index, {Hash, Info}),
    notify_subscribers({manifest_stored, MCID, Manifest}),
    ok.

%% @doc pg group used to broadcast `{manifest_stored, MCID, Manifest}'
%% messages. Subscribers (e.g. `hecate_content_announcer') join this
%% group and react.
-spec events_group() -> atom().
events_group() ->
    hecate_content_events.

notify_subscribers(Event) ->
    Members = pg_safe_get_members(events_group()),
    lists:foreach(fun(P) -> P ! Event end, Members).

pg_safe_get_members(Group) ->
    try pg:get_members(Group) of
        Pids -> Pids
    catch
        _:_ -> []
    end.

do_get_manifest(<<_:8, _:8, Hash:32/binary>>, S) ->
    fetch_manifest(ets:member(S#state.manifest_index, Hash), Hash, S).

fetch_manifest(false, _Hash, _S) ->
    {error, not_found};
fetch_manifest(true, Hash, S) ->
    Path = manifest_path_in(Hash, S#state.base_dir),
    decode_manifest_file(file:read_file(Path)).

decode_manifest_file({ok, Encoded}) ->
    hecate_content_manifest:decode(Encoded);
decode_manifest_file({error, _}) ->
    {error, not_found}.

do_delete_manifest(<<_:8, _:8, Hash:32/binary>>, S) ->
    Path = manifest_path_in(Hash, S#state.base_dir),
    file:delete(Path),
    ets:delete(S#state.manifest_index, Hash),
    ok.

%%====================================================================
%% Maintenance
%%====================================================================

do_gc(S) ->
    Referenced = collect_referenced_hashes(S),
    All = [H || {H, _} <- ets:tab2list(S#state.block_index)],
    Orphans = [H || H <- All, not sets:is_element(H, Referenced)],
    lists:foreach(
        fun(H) -> do_delete_block(<<1, ?CODEC_RAW, H/binary>>, S) end,
        Orphans),
    {ok, #{removed => length(Orphans)}}.

collect_referenced_hashes(S) ->
    Hashes = [H || {H, _} <- ets:tab2list(S#state.manifest_index)],
    lists:foldl(
        fun(MH, Acc) -> collect_one_manifest(MH, S, Acc) end,
        sets:new(), Hashes).

collect_one_manifest(ManifestHash, S, Acc) ->
    MCID = <<1, ?CODEC_MANIFEST, ManifestHash/binary>>,
    case do_get_manifest(MCID, S) of
        {ok, Manifest} -> add_chunk_hashes(maps:get(chunks, Manifest, []), Acc);
        {error, _}     -> Acc
    end.

add_chunk_hashes(Chunks, Acc) ->
    lists:foldl(
        fun(C, A) -> sets:add_element(maps:get(hash, C), A) end,
        Acc, Chunks).

do_stats(S) ->
    BlockCount    = ets:info(S#state.block_index, size),
    ManifestCount = ets:info(S#state.manifest_index, size),
    TotalSize     = ets:foldl(
        fun({_, {Sz, _}}, A) -> A + Sz end,
        0, S#state.block_index),
    #{block_count    => BlockCount,
      manifest_count => ManifestCount,
      total_size     => TotalSize}.

do_verify_integrity(S) ->
    Corrupted = ets:foldl(
        fun({Hash, _}, Acc) ->
            MCID = <<1, ?CODEC_RAW, Hash/binary>>,
            collect_corrupted(do_get_block(MCID, S), MCID, Acc)
        end, [], S#state.block_index),
    case Corrupted of
        []  -> {ok, ets:info(S#state.block_index, size)};
        _   -> {error, {corrupted, Corrupted}}
    end.

collect_corrupted({ok, _}, _MCID, Acc)            -> Acc;
collect_corrupted({error, _}, MCID, Acc)          -> [MCID | Acc].

%%====================================================================
%% Paths & dirs
%%====================================================================

block_path_in(Hash, BaseDir) ->
    Hex = hecate_content_hasher:hex_encode(Hash),
    <<Shard:2/binary, _/binary>> = Hex,
    filename:join([BaseDir, "blocks", binary_to_list(Shard),
                   binary_to_list(Hex) ++ ?BLOCK_EXT]).

manifest_path_in(Hash, BaseDir) ->
    Hex = hecate_content_hasher:hex_encode(Hash),
    <<Shard:2/binary, _/binary>> = Hex,
    filename:join([BaseDir, "manifests", binary_to_list(Shard),
                   binary_to_list(Hex) ++ ?MANIFEST_EXT]).

resolve_base_dir(Opts) ->
    case maps:get(store_path, Opts, undefined) of
        undefined -> maps:get(base_dir, Opts, default_base_dir());
        Path      -> Path
    end.

default_base_dir() ->
    case application:get_env(hecate_content, store_dir) of
        {ok, Dir} -> Dir;
        undefined -> "/var/lib/hecate/content"
    end.

ensure_dir_exists(Dir) ->
    case filelib:is_dir(Dir) of
        true  -> ok;
        false -> create_dir_tree(Dir)
    end.

create_dir_tree(Dir) ->
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    create_final_dir(file:make_dir(Dir)).

create_final_dir(ok)              -> ok;
create_final_dir({error, eexist}) -> ok;
create_final_dir({error, _} = E)  -> E.

%%====================================================================
%% Index rebuild
%%====================================================================

rebuild_index(S) ->
    rebuild_blocks(S),
    rebuild_manifests(S),
    ok.

rebuild_blocks(S) ->
    Pat = filename:join([S#state.blocks_dir, "*", "*" ++ ?BLOCK_EXT]),
    lists:foreach(fun(P) -> index_block_file(P, S) end, filelib:wildcard(Pat)).

index_block_file(Path, S) ->
    case file:read_file_info(Path) of
        {ok, Info} -> maybe_index_block(Path, Info, S);
        {error, _} -> ok
    end.

maybe_index_block(Path, Info, S) ->
    Basename = filename:basename(Path, ?BLOCK_EXT),
    case hecate_content_hasher:hex_decode(list_to_binary(Basename)) of
        {ok, Hash} ->
            Size = element(2, Info),
            ets:insert(S#state.block_index, {Hash, {Size, 0}});
        {error, _} ->
            ok
    end.

rebuild_manifests(S) ->
    Pat = filename:join([S#state.manifests_dir, "*", "*" ++ ?MANIFEST_EXT]),
    lists:foreach(fun(P) -> index_manifest_file(P, S) end, filelib:wildcard(Pat)).

index_manifest_file(Path, S) ->
    Basename = filename:basename(Path, ?MANIFEST_EXT),
    case hecate_content_hasher:hex_decode(list_to_binary(Basename)) of
        {ok, Hash} -> maybe_index_manifest(Path, Hash, S);
        {error, _} -> ok
    end.

maybe_index_manifest(Path, Hash, S) ->
    case file:read_file(Path) of
        {ok, Encoded} -> store_manifest_index(Encoded, Hash, S);
        {error, _}    -> ok
    end.

store_manifest_index(Encoded, Hash, S) ->
    case hecate_content_manifest:decode(Encoded) of
        {ok, Manifest} ->
            Info = {maps:get(name, Manifest),
                    maps:get(size, Manifest),
                    maps:get(chunk_count, Manifest),
                    0},
            ets:insert(S#state.manifest_index, {Hash, Info});
        {error, _} ->
            ok
    end.
