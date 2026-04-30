%% @doc High-level facade for the content subsystem.
%%
%% Wraps the lower-level modules — manifest, store, hasher, chunker —
%% behind a small set of ergonomic operations: `store/1' takes raw
%% bytes and returns an MCID; `fetch/1' takes an MCID and returns
%% the bytes (locally only — remote fetches go through
%% `macula_content_transfer' + `macula_content_dht').
%%
%% Intended for application code; the supervised gen_servers
%% (`macula_content_store', `macula_content_transfer') are the
%% preferred call targets only when the facade isn't expressive
%% enough.
-module(macula_content).

-export([
    %% MCID + manifest
    mcid/1, mcid/2,
    mcid_to_string/1, mcid_from_string/1,
    create_manifest/1, create_manifest/2,
    verify_manifest/2,
    get_chunks/1, missing_chunks/1,
    %% Local store
    store/1, store/2,
    fetch/1, fetch/2,
    is_local/1,
    status/0
]).

-export_type([mcid/0, manifest/0, store_opts/0]).

-type mcid()       :: macula_content_manifest:mcid().
-type manifest()   :: macula_content_manifest:manifest().
-type store_opts() :: #{
    name           => binary(),
    chunk_size     => pos_integer(),
    hash_algorithm => macula_content_hasher:algorithm()
}.

%%====================================================================
%% MCID
%%====================================================================

-spec mcid(binary()) -> mcid().
mcid(Data) ->
    mcid(Data, macula_content_hasher:default_algorithm()).

-spec mcid(binary(), macula_content_hasher:algorithm()) -> mcid().
mcid(Data, Algorithm) ->
    Hash = macula_content_hasher:hash(Algorithm, Data),
    <<1, 16#55, Hash/binary>>.

-spec mcid_to_string(mcid()) -> binary().
mcid_to_string(MCID) ->
    macula_content_manifest:mcid_to_string(MCID).

-spec mcid_from_string(binary()) -> {ok, mcid()} | {error, invalid_mcid}.
mcid_from_string(String) ->
    macula_content_manifest:mcid_from_string(String).

%%====================================================================
%% Manifest
%%====================================================================

-spec create_manifest(binary()) -> {ok, manifest()}.
create_manifest(Data) ->
    macula_content_manifest:create(Data).

-spec create_manifest(binary(), store_opts()) -> {ok, manifest()}.
create_manifest(Data, Opts) ->
    macula_content_manifest:create(Data, Opts).

-spec verify_manifest(manifest(), binary()) -> boolean().
verify_manifest(Manifest, Data) ->
    case macula_content_manifest:verify(Manifest, Data) of
        ok            -> true;
        {error, _}    -> false
    end.

-spec get_chunks(manifest()) -> [macula_content_chunker:chunk_info()].
get_chunks(Manifest) ->
    maps:get(chunks, Manifest).

-spec missing_chunks(manifest()) -> [macula_content_chunker:chunk_info()].
missing_chunks(#{chunks := Chunks}) ->
    [C || C <- Chunks, not has_chunk_locally(C)].

%%====================================================================
%% Store / fetch
%%====================================================================

-spec store(binary()) -> {ok, mcid()}.
store(Data) ->
    store(Data, #{}).

-spec store(binary(), store_opts()) -> {ok, mcid()}.
store(Data, Opts) ->
    {ok, Manifest} = macula_content_manifest:create(Data, Opts),
    ok = put_chunks(Data, Manifest),
    ok = macula_content_store:put_manifest(Manifest),
    {ok, maps:get(mcid, Manifest)}.

-spec fetch(mcid()) -> {ok, binary()} | {error, not_found | term()}.
fetch(MCID) ->
    fetch(MCID, #{}).

-spec fetch(mcid(), map()) -> {ok, binary()} | {error, term()}.
fetch(MCID, _Opts) ->
    case macula_content_store:get_manifest(MCID) of
        {ok, Manifest} -> reassemble_from_manifest(Manifest);
        {error, _} = E -> E
    end.

-spec is_local(mcid()) -> boolean().
is_local(<<_:8, 16#56, _/binary>> = MCID) ->
    case macula_content_store:get_manifest(MCID) of
        {ok, Manifest} -> all_chunks_local(Manifest);
        {error, _}     -> false
    end;
is_local(MCID) ->
    macula_content_store:has_block(MCID).

-spec status() -> map().
status() ->
    macula_content_store:stats().

%%====================================================================
%% Internal
%%====================================================================

put_chunks(Data, #{chunk_size := CS, chunks := Infos}) ->
    {ok, Chunks} = macula_content_chunker:chunk(Data, CS),
    write_chunk_list(lists:zip(Infos, Chunks)).

write_chunk_list([]) ->
    ok;
write_chunk_list([{Info, Chunk} | Rest]) ->
    Hash = maps:get(hash, Info),
    MCID = <<1, 16#55, Hash/binary>>,
    ok = macula_content_store:put_block(MCID, Chunk),
    write_chunk_list(Rest).

reassemble_from_manifest(#{chunks := Chunks} = Manifest) ->
    case fetch_all_chunks(Chunks, []) of
        {ok, Bins} ->
            Data = macula_content_chunker:reassemble(Bins),
            verify_reassembled(Manifest, Data);
        {error, _} = E ->
            E
    end.

verify_reassembled(Manifest, Data) ->
    case macula_content_manifest:verify(Manifest, Data) of
        ok            -> {ok, Data};
        {error, _} = E -> E
    end.

fetch_all_chunks([], Acc) ->
    {ok, lists:reverse(Acc)};
fetch_all_chunks([Info | Rest], Acc) ->
    Hash = maps:get(hash, Info),
    MCID = <<1, 16#55, Hash/binary>>,
    case macula_content_store:get_block(MCID) of
        {ok, Bin}       -> fetch_all_chunks(Rest, [Bin | Acc]);
        {error, _} = E  -> E
    end.

has_chunk_locally(#{hash := Hash}) ->
    macula_content_store:has_block(<<1, 16#55, Hash/binary>>).

all_chunks_local(#{chunks := Chunks}) ->
    lists:all(fun has_chunk_locally/1, Chunks).
