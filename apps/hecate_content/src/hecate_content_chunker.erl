%% @doc Fixed-size chunking + Merkle-root computation.
%%
%% Default chunk size is 256 KiB — balances network throughput,
%% memory locality, and parallel-fetch granularity. Merkle root
%% follows the V1 layout: pair adjacent chunk hashes (duplicate
%% the last on odd counts), hash the concatenation, recurse.
-module(hecate_content_chunker).

-export([
    chunk/2, reassemble/1, chunk_info/2, merkle_root/2,
    verify_chunk/3, default_chunk_size/0
]).

-export_type([chunk_info/0]).

-type chunk_info() :: #{
    index  := non_neg_integer(),
    offset := non_neg_integer(),
    size   := pos_integer(),
    hash   := binary()
}.

-define(DEFAULT_CHUNK_SIZE, 262144).

-spec default_chunk_size() -> pos_integer().
default_chunk_size() -> ?DEFAULT_CHUNK_SIZE.

-spec chunk(binary(), pos_integer()) -> {ok, [binary()]}.
chunk(<<>>, _ChunkSize) ->
    {ok, []};
chunk(Data, ChunkSize) when is_binary(Data), is_integer(ChunkSize), ChunkSize > 0 ->
    {ok, do_chunk(Data, ChunkSize, [])}.

-spec reassemble([binary()]) -> binary().
reassemble(Chunks) when is_list(Chunks) ->
    iolist_to_binary(Chunks).

-spec chunk_info([binary()], hecate_content_hasher:algorithm()) -> [chunk_info()].
chunk_info(Chunks, Algorithm) ->
    {Infos, _} = lists:foldl(
        fun(C, {Acc, {Idx, Off}}) ->
            Sz = byte_size(C),
            H  = hecate_content_hasher:hash(Algorithm, C),
            {[#{index => Idx, offset => Off, size => Sz, hash => H} | Acc],
             {Idx + 1, Off + Sz}}
        end, {[], {0, 0}}, Chunks),
    lists:reverse(Infos).

-spec merkle_root([chunk_info()], hecate_content_hasher:algorithm()) -> binary().
merkle_root([], Algorithm) ->
    hecate_content_hasher:hash(Algorithm, <<>>);
merkle_root(Infos, Algorithm) ->
    Hashes = [maps:get(hash, I) || I <- Infos],
    fold_pairs(Hashes, Algorithm).

-spec verify_chunk(binary(), binary(), hecate_content_hasher:algorithm()) -> boolean().
verify_chunk(Chunk, ExpectedHash, Algorithm) ->
    hecate_content_hasher:verify(Algorithm, Chunk, ExpectedHash).

%%--- helpers ---

do_chunk(<<>>, _CS, Acc) ->
    lists:reverse(Acc);
do_chunk(Data, CS, Acc) when byte_size(Data) =< CS ->
    lists:reverse([Data | Acc]);
do_chunk(Data, CS, Acc) ->
    <<Chunk:CS/binary, Rest/binary>> = Data,
    do_chunk(Rest, CS, [Chunk | Acc]).

fold_pairs([H], _Algorithm) ->
    H;
fold_pairs(Hashes, Algorithm) ->
    Combined = combine(Hashes, Algorithm, []),
    fold_pairs(Combined, Algorithm).

combine([], _Algorithm, Acc) ->
    lists:reverse(Acc);
combine([Last], Algorithm, Acc) ->
    %% Odd count — pair the last hash with itself per V1 convention.
    H = hecate_content_hasher:hash(Algorithm, <<Last/binary, Last/binary>>),
    lists:reverse([H | Acc]);
combine([L, R | Rest], Algorithm, Acc) ->
    H = hecate_content_hasher:hash(Algorithm, <<L/binary, R/binary>>),
    combine(Rest, Algorithm, [H | Acc]).
