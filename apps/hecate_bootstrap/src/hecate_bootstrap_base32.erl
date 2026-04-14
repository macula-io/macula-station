%% @doc RFC 4648 base32 codec (canonical alphabet).
%%
%% Used to derive the DoH zone label for foundation pubkeys
%% (Part 5 §4.1 — `_pkarr.&lt;NodeId-base32&gt;.macula.io'). A 32-byte
%% Ed25519 pubkey encodes to 52 lowercase characters, well within the
%% 63-character DNS-label limit, and requires no padding because we
%% emit only whole characters and decode tolerates trailing sub-byte
%% bit fragments.
%%
%% Encoding is lowercase; decoding is case-insensitive. No padding is
%% emitted or accepted — the RFC `=' pad character is not a legal DNS
%% label character, and our inputs are fixed-width pubkeys so the
%% decoder can reconstruct the exact byte length from the input
%% character count.
%%
%% This module is intentionally minimal: it does the RFC 4648 base32
%% alphabet and nothing else. No base32hex, no z-base32, no crockford.
-module(hecate_bootstrap_base32).

-export([encode/1, decode/1]).

-export_type([decode_error/0]).

-type decode_error() :: bad_char | {bad_char, byte()}.

%% @doc Encode `Bin' as unpadded lowercase base32. An `N'-byte input
%% produces `ceil(N * 8 / 5)' characters.
-spec encode(binary()) -> binary().
encode(Bin) when is_binary(Bin) ->
    NumBits = bit_size(Bin),
    Padding = (5 - NumBits rem 5) rem 5,
    Padded  = <<Bin/bitstring, 0:Padding>>,
    << <<(alphabet(N))>> || <<N:5>> <= Padded >>.

%% @doc Decode a base32 string (case-insensitive, no padding) back to
%% bytes. Any trailing sub-byte bit fragment (introduced by encoding
%% an input whose bit-length is not a multiple of 8) is discarded —
%% the result is the longest whole-byte prefix of the bitstring.
-spec decode(binary()) -> {ok, binary()} | {error, decode_error()}.
decode(Bin) when is_binary(Bin) ->
    try
        Bits = << <<(char_value(C)):5>> || <<C>> <= Bin >>,
        NumBytes = bit_size(Bits) div 8,
        <<Result:NumBytes/binary, _/bitstring>> = Bits,
        {ok, Result}
    catch
        throw:{bad_char, _} = E -> {error, E};
        error:{badmatch, _}      -> {error, bad_char}
    end.

%%------------------------------------------------------------------
%% Alphabet tables
%%------------------------------------------------------------------

-spec alphabet(0..31) -> byte().
alphabet(N) when N >= 0, N =< 25 -> $a + N;
alphabet(N) when N >= 26, N =< 31 -> $2 + (N - 26).

-spec char_value(byte()) -> 0..31.
char_value(C) when C >= $a, C =< $z -> C - $a;
char_value(C) when C >= $A, C =< $Z -> C - $A;
char_value(C) when C >= $2, C =< $7 -> C - $2 + 26;
char_value(C) -> throw({bad_char, C}).
