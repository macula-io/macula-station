%% @doc Manifests describe content (size + chunk topology + Merkle root)
%% and carry an MCID — Macula Content IDentifier — that uniquely
%% addresses the content by hash.
%%
%% MCID format (34 bytes):
%% <pre>
%%   &lt;&lt;Version:8, Codec:8, Hash:32/binary&gt;&gt;
%% </pre>
%% Codec is `?CODEC_RAW' (16#55) for chunk MCIDs and `?CODEC_MANIFEST'
%% (16#56) for manifest MCIDs.
%%
%% Encoding uses `macula_record_cbor' (deterministic CBOR per RFC 8949
%% §4.2.1) — same wire format used everywhere in macula 3.1 for signed
%% / canonical data.
-module(macula_content_manifest).

-export([
    version/0,
    create/2, create/1,
    encode/1, decode/1,
    verify/2,
    mcid_to_string/1, mcid_from_string/1,
    get_chunk_mcid/2
]).

-export_type([mcid/0, manifest/0]).

-type mcid() :: <<_:272>>.

-type manifest() :: #{
    mcid           := mcid(),
    version        := pos_integer(),
    name           := binary(),
    size           := non_neg_integer(),
    created        := non_neg_integer(),
    chunk_size     := pos_integer(),
    chunk_count    := non_neg_integer(),
    hash_algorithm := macula_content_hasher:algorithm(),
    root_hash      := binary(),
    chunks         := [macula_content_chunker:chunk_info()]
}.

-define(VERSION,          1).
-define(CODEC_RAW,        16#55).
-define(CODEC_MANIFEST,   16#56).

%%====================================================================
%% API
%%====================================================================

-spec version() -> pos_integer().
version() -> ?VERSION.

-spec create(binary()) -> {ok, manifest()}.
create(Data) -> create(Data, #{}).

%% @doc Build a manifest from `Data'. Options:
%% <ul>
%%   <li>`name' — content name (default `<<"unnamed">>')</li>
%%   <li>`chunk_size' — bytes per chunk
%%       (default `macula_content_chunker:default_chunk_size()')</li>
%%   <li>`hash_algorithm' — `blake3' | `sha256'
%%       (default `blake3')</li>
%% </ul>
-spec create(binary(), map()) -> {ok, manifest()}.
create(Data, Opts) when is_binary(Data), is_map(Opts) ->
    Name      = maps:get(name, Opts, <<"unnamed">>),
    ChunkSize = maps:get(chunk_size, Opts,
                         macula_content_chunker:default_chunk_size()),
    Algorithm = maps:get(hash_algorithm, Opts,
                         macula_content_hasher:default_algorithm()),

    {ok, Chunks} = macula_content_chunker:chunk(Data, ChunkSize),
    ChunkInfos   = macula_content_chunker:chunk_info(Chunks, Algorithm),
    RootHash     = root_hash_for(ChunkInfos, Algorithm),

    Body = #{
        version        => ?VERSION,
        name           => Name,
        size           => byte_size(Data),
        created        => erlang:system_time(second),
        chunk_size     => ChunkSize,
        chunk_count    => length(ChunkInfos),
        hash_algorithm => Algorithm,
        root_hash      => RootHash,
        chunks         => ChunkInfos
    },
    {ok, Body#{mcid => compute_mcid(Body, Algorithm)}}.

-spec encode(manifest()) -> {ok, binary()}.
encode(Manifest) ->
    {ok, macula_record_cbor:encode(to_cbor(Manifest))}.

-spec decode(binary()) -> {ok, manifest()} | {error, invalid_manifest}.
decode(Bin) when is_binary(Bin) ->
    decode_safe(catch macula_record_cbor:decode(Bin)).

-spec verify(manifest(), binary()) ->
        ok | {error, size_mismatch | root_hash_mismatch}.
verify(#{size := ExpectedSize} = Manifest, Data) when is_binary(Data) ->
    case byte_size(Data) of
        ExpectedSize -> verify_root_hash(Manifest, Data);
        _            -> {error, size_mismatch}
    end.

-spec mcid_to_string(mcid()) -> binary().
mcid_to_string(<<Version:8, Codec:8, Hash:32/binary>>) ->
    AlgoStr = <<"blake3">>,
    Hex     = macula_content_hasher:hex_encode(Hash),
    <<"mcid", (integer_to_binary(Version))/binary, "-",
      (codec_to_string(Codec))/binary, "-",
      AlgoStr/binary, "-", Hex/binary>>.

-spec mcid_from_string(binary()) -> {ok, mcid()} | {error, invalid_mcid}.
mcid_from_string(<<"mcid", Rest/binary>>) ->
    parse_mcid_parts(binary:split(Rest, <<"-">>, [global]));
mcid_from_string(_) ->
    {error, invalid_mcid}.

-spec get_chunk_mcid(manifest(), non_neg_integer()) ->
        {ok, mcid()} | {error, invalid_index}.
get_chunk_mcid(#{chunks := Chunks}, Index) ->
    chunk_mcid_at(Chunks, Index).

%%====================================================================
%% Internal — MCID
%%====================================================================

compute_mcid(Body, Algorithm) ->
    %% Deterministic canonical encoding — exclude `created' (timestamp)
    %% and `chunks' (chunk hashes already roll up into root_hash).
    Canonical = #{
        {text, <<"name">>}           => {text, maps:get(name, Body)},
        {text, <<"size">>}           => maps:get(size, Body),
        {text, <<"chunk_size">>}     => maps:get(chunk_size, Body),
        {text, <<"chunk_count">>}    => maps:get(chunk_count, Body),
        {text, <<"hash_algorithm">>} => {text, atom_to_binary(Algorithm)},
        {text, <<"root_hash">>}      => maps:get(root_hash, Body)
    },
    Bytes = macula_record_cbor:encode(Canonical),
    make_mcid(?VERSION, ?CODEC_MANIFEST,
              macula_content_hasher:hash(Algorithm, Bytes)).

make_mcid(Version, Codec, Hash) ->
    <<Version:8, Codec:8, Hash/binary>>.

codec_to_string(?CODEC_RAW)      -> <<"raw">>;
codec_to_string(?CODEC_MANIFEST) -> <<"manifest">>;
codec_to_string(_)               -> <<"unknown">>.

string_to_codec(<<"raw">>)      -> {ok, ?CODEC_RAW};
string_to_codec(<<"manifest">>) -> {ok, ?CODEC_MANIFEST};
string_to_codec(_)              -> error.

parse_mcid_parts([VersionStr, CodecStr, _Algo, HashHex]) ->
    parse_mcid_with_codec(string_to_codec(CodecStr),
                           binary_to_integer(VersionStr),
                           HashHex);
parse_mcid_parts(_) ->
    {error, invalid_mcid}.

parse_mcid_with_codec({ok, Codec}, Version, Hex) ->
    parse_mcid_with_hash(macula_content_hasher:hex_decode(Hex), Version, Codec);
parse_mcid_with_codec(error, _Version, _Hex) ->
    {error, invalid_mcid}.

parse_mcid_with_hash({ok, Hash}, Version, Codec) when byte_size(Hash) =:= 32 ->
    {ok, <<Version:8, Codec:8, Hash/binary>>};
parse_mcid_with_hash(_, _, _) ->
    {error, invalid_mcid}.

chunk_mcid_at(Chunks, Index) when Index >= 0, Index < length(Chunks) ->
    Hash = maps:get(hash, lists:nth(Index + 1, Chunks)),
    {ok, make_mcid(?VERSION, ?CODEC_RAW, Hash)};
chunk_mcid_at(_Chunks, _Index) ->
    {error, invalid_index}.

%%====================================================================
%% Internal — verification
%%====================================================================

root_hash_for([], Algorithm) ->
    macula_content_hasher:hash(Algorithm, <<>>);
root_hash_for(Infos, Algorithm) ->
    macula_content_chunker:merkle_root(Infos, Algorithm).

verify_root_hash(#{chunk_size := CS, hash_algorithm := Alg,
                    root_hash := Expected}, Data) ->
    {ok, Chunks} = macula_content_chunker:chunk(Data, CS),
    Infos        = macula_content_chunker:chunk_info(Chunks, Alg),
    Actual       = root_hash_for(Infos, Alg),
    case Actual =:= Expected of
        true  -> ok;
        false -> {error, root_hash_mismatch}
    end.

%%====================================================================
%% Internal — CBOR encoding
%%====================================================================

to_cbor(M) ->
    #{
        {text, <<"mcid">>}           => maps:get(mcid, M),
        {text, <<"version">>}        => maps:get(version, M),
        {text, <<"name">>}           => {text, text_value(maps:get(name, M))},
        {text, <<"size">>}           => maps:get(size, M),
        {text, <<"created">>}        => maps:get(created, M),
        {text, <<"chunk_size">>}     => maps:get(chunk_size, M),
        {text, <<"chunk_count">>}    => maps:get(chunk_count, M),
        {text, <<"hash_algorithm">>} => {text, atom_to_binary(maps:get(hash_algorithm, M))},
        {text, <<"root_hash">>}      => maps:get(root_hash, M),
        {text, <<"chunks">>}         => [chunk_info_to_cbor(C) || C <- maps:get(chunks, M)]
    }.

%% `name' arrives here either as a plain binary (a manifest built
%% natively via create/2) or as `{text, Bin}' (a manifest decoded off
%% the wire by macula_frame:from_wire_envelope/1, whose maybe_atom/2
%% leaves arbitrary content names -- never pre-existing atoms -- as
%% `{text, Bin}'). Unwrap either shape to the plain binary macula_record_cbor:encode/1's
%% {text, binary()} clause requires; a name arriving already-wrapped
%% must not be wrapped a second time.
text_value({text, B}) when is_binary(B) -> B;
text_value(B) when is_binary(B) -> B.

chunk_info_to_cbor(C) ->
    #{
        {text, <<"index">>}  => maps:get(index, C),
        {text, <<"offset">>} => maps:get(offset, C),
        {text, <<"size">>}   => maps:get(size, C),
        {text, <<"hash">>}   => maps:get(hash, C)
    }.

decode_safe({'EXIT', _}) ->
    {error, invalid_manifest};
decode_safe(Map) when is_map(Map) ->
    decode_manifest(Map);
decode_safe(_) ->
    {error, invalid_manifest}.

decode_manifest(Map) ->
    case maps:find({text, <<"mcid">>}, Map) of
        {ok, MCID} -> {ok, build_manifest(Map, MCID)};
        error      -> {error, invalid_manifest}
    end.

build_manifest(Map, MCID) ->
    #{
        mcid           => MCID,
        version        => get_int(Map, <<"version">>, 1),
        name           => get_text(Map, <<"name">>, <<"unnamed">>),
        size           => get_int(Map, <<"size">>, 0),
        created        => get_int(Map, <<"created">>, 0),
        chunk_size     => get_int(Map, <<"chunk_size">>, 262144),
        chunk_count    => get_int(Map, <<"chunk_count">>, 0),
        hash_algorithm => binary_to_existing_atom(
                            get_text(Map, <<"hash_algorithm">>, <<"blake3">>),
                            utf8),
        root_hash      => get_bin(Map, <<"root_hash">>, <<>>),
        chunks         => [chunk_info_from_cbor(C)
                           || C <- get_list(Map, <<"chunks">>, [])]
    }.

chunk_info_from_cbor(C) ->
    #{
        index  => get_int(C, <<"index">>, 0),
        offset => get_int(C, <<"offset">>, 0),
        size   => get_int(C, <<"size">>, 0),
        hash   => get_bin(C, <<"hash">>, <<>>)
    }.

get_int(Map, Key, Default) ->
    case maps:find({text, Key}, Map) of
        {ok, V} when is_integer(V) -> V;
        _                          -> Default
    end.

get_text(Map, Key, Default) ->
    case maps:find({text, Key}, Map) of
        {ok, {text, V}} -> V;
        {ok, V} when is_binary(V) -> V;
        _ -> Default
    end.

get_bin(Map, Key, Default) ->
    case maps:find({text, Key}, Map) of
        {ok, V} when is_binary(V) -> V;
        _                         -> Default
    end.

get_list(Map, Key, Default) ->
    case maps:find({text, Key}, Map) of
        {ok, V} when is_list(V) -> V;
        _                       -> Default
    end.
