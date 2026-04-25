%% @doc Cryptographic hashing for content-addressed storage.
%%
%% Two algorithms supported:
%% <ul>
%%   <li><strong>BLAKE3</strong> — fast (≈ 3-5x SHA256 on SIMD-capable
%%       hardware), parallel-tree-friendly. Backed by the Rust NIF
%%       `macula_blake3_nif' from the macula SDK.</li>
%%   <li><strong>SHA256</strong> — Erlang `crypto' built-in. Used when
%%       the application explicitly requests it (e.g., interop with
%%       legacy fixtures) or as a conservative fallback.</li>
%% </ul>
%%
%% Both produce 32-byte digests; the algorithm is encoded in the MCID
%% codec byte so the verifier can pick the right one. The default for
%% new content is BLAKE3.
%%
%% No NIF-missing fallback. The macula SDK installs the BLAKE3 NIF as
%% part of its build pipeline (`native/macula_blake3_nif/'); if the
%% NIF isn't loaded the application is misbuilt and we fail fast,
%% same stance the SDK takes for `macula_cbor_nif'.
-module(hecate_content_hasher).

-export([
    hash/2, hash_streaming/2, verify/3,
    supported_algorithms/0, is_supported/1, hash_size/1,
    default_algorithm/0,
    hex_encode/1, hex_decode/1
]).

-export_type([algorithm/0, hash/0]).

-type algorithm() :: blake3 | sha256.
-type hash()      :: <<_:256>>.

-spec default_algorithm() -> algorithm().
default_algorithm() -> blake3.

-spec hash(algorithm(), binary()) -> hash().
hash(blake3, Data) when is_binary(Data) ->
    macula_blake3_nif:hash(Data);
hash(sha256, Data) when is_binary(Data) ->
    crypto:hash(sha256, Data).

-spec hash_streaming(algorithm(), [binary()]) -> hash().
hash_streaming(blake3, Chunks) when is_list(Chunks) ->
    %% The NIF exposes streaming hashing of an iolist; one call,
    %% no Erlang-side concatenation.
    macula_blake3_nif:hash_streaming(Chunks);
hash_streaming(sha256, Chunks) when is_list(Chunks) ->
    Ctx = lists:foldl(
        fun(C, S) -> crypto:hash_update(S, C) end,
        crypto:hash_init(sha256),
        Chunks),
    crypto:hash_final(Ctx).

-spec verify(algorithm(), binary(), hash()) -> boolean().
verify(Algorithm, Data, Expected) ->
    hash(Algorithm, Data) =:= Expected.

-spec supported_algorithms() -> [algorithm()].
supported_algorithms() -> [blake3, sha256].

-spec is_supported(atom()) -> boolean().
is_supported(blake3) -> true;
is_supported(sha256) -> true;
is_supported(_)      -> false.

-spec hash_size(algorithm()) -> 32.
hash_size(blake3) -> 32;
hash_size(sha256) -> 32.

-spec hex_encode(binary()) -> binary().
hex_encode(Bin) ->
    << <<(hex_digit(N))>> || <<N:4>> <= Bin >>.

-spec hex_decode(binary()) -> {ok, binary()} | {error, invalid_hex}.
hex_decode(Hex) when is_binary(Hex) ->
    hex_decode(Hex, <<>>).

%%--- helpers ---

hex_digit(N) when N < 10 -> $0 + N;
hex_digit(N)             -> $a + N - 10.

hex_decode(<<>>, Acc) ->
    {ok, Acc};
hex_decode(<<H1, H2, Rest/binary>>, Acc) ->
    hex_decode_byte(hex_value(H1), hex_value(H2), Rest, Acc);
hex_decode(_, _) ->
    {error, invalid_hex}.

hex_decode_byte({ok, V1}, {ok, V2}, Rest, Acc) ->
    Byte = (V1 bsl 4) bor V2,
    hex_decode(Rest, <<Acc/binary, Byte>>);
hex_decode_byte(_, _, _, _) ->
    {error, invalid_hex}.

hex_value(C) when C >= $0, C =< $9 -> {ok, C - $0};
hex_value(C) when C >= $a, C =< $f -> {ok, C - $a + 10};
hex_value(C) when C >= $A, C =< $F -> {ok, C - $A + 10};
hex_value(_)                       -> error.
