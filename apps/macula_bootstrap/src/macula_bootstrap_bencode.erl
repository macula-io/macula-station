%% @doc BEP 3 bencode codec.
%%
%% Needed for Tier C (Part 5 §6 — Mainline DHT bridge). Bencode is
%% the wire format of every BitTorrent DHT RPC; BEP 44's signature
%% layer also signs over a bencoded tuple. The codec is pure,
%% deterministic, and has no dependency on the DHT transport — it is
%% consumed by `macula_bootstrap_bep44' and any test harness that
%% needs to build or inspect BT-DHT packets.
%%
%% == Type mapping ==
%%
%% | Erlang term | Bencoded form       |
%% | ----------- | ------------------- |
%% | `integer()' | `i<N>e'             |
%% | `binary()'  | `<len>:<bytes>'     |
%% | `list()'    | `l<elements>e'      |
%% | `map()' (binary keys) | `d<pairs>e' (keys sorted) |
%%
%% Decode produces the same mapping; dictionaries come back as maps
%% with binary keys.
-module(macula_bootstrap_bencode).

-export([encode/1, decode/1]).

-export_type([value/0, decode_error/0]).

-type value() :: integer()
               | binary()
               | [value()]
               | #{binary() => value()}.

-type decode_error() ::
        bad_syntax
      | unterminated_int
      | bad_int
      | bad_string_length
      | no_colon
      | short_string
      | trailing_data.

%%==================================================================
%% Encode
%%==================================================================

-spec encode(value()) -> binary().
encode(N) when is_integer(N) ->
    <<"i", (integer_to_binary(N))/binary, "e">>;
encode(B) when is_binary(B) ->
    Len = integer_to_binary(byte_size(B)),
    <<Len/binary, ":", B/binary>>;
encode(L) when is_list(L) ->
    <<"l", (encode_list(L))/binary, "e">>;
encode(M) when is_map(M) ->
    Keys = lists:sort(maps:keys(M)),
    <<"d", (encode_pairs(Keys, M))/binary, "e">>.

encode_list([])         -> <<>>;
encode_list([H | T])    -> <<(encode(H))/binary, (encode_list(T))/binary>>.

encode_pairs([], _M) -> <<>>;
encode_pairs([K | Rest], M) when is_binary(K) ->
    <<(encode(K))/binary,
      (encode(maps:get(K, M)))/binary,
      (encode_pairs(Rest, M))/binary>>.

%%==================================================================
%% Decode
%%==================================================================

-spec decode(binary()) -> {ok, value()} | {error, decode_error()}.
decode(Bin) when is_binary(Bin) ->
    finalize(decode_value(Bin)).

finalize({ok, V, <<>>})  -> {ok, V};
finalize({ok, _, _Rest}) -> {error, trailing_data};
finalize({error, _} = E) -> E.

decode_value(<<"i", Rest/binary>>) -> decode_int(Rest);
decode_value(<<"l", Rest/binary>>) -> decode_list(Rest, []);
decode_value(<<"d", Rest/binary>>) -> decode_dict(Rest, []);
decode_value(<<D, _/binary>> = Bin) when D >= $0, D =< $9 ->
    decode_string(Bin);
decode_value(_) ->
    {error, bad_syntax}.

decode_int(Bin) ->
    take_until_e(Bin, fun digits_to_int/1).

digits_to_int(Digits) ->
    try {ok, binary_to_integer(Digits)}
    catch _:_ -> {error, bad_int}
    end.

take_until_e(Bin, Cont) ->
    case binary:split(Bin, <<"e">>) of
        [Digits, Rest] ->
            case Cont(Digits) of
                {ok, N}         -> {ok, N, Rest};
                {error, _} = E  -> E
            end;
        [_] ->
            {error, unterminated_int}
    end.

decode_string(Bin) ->
    case binary:split(Bin, <<":">>) of
        [LenBin, Rest] -> split_payload(LenBin, Rest);
        [_]            -> {error, no_colon}
    end.

split_payload(LenBin, Rest) ->
    try binary_to_integer(LenBin) of
        Len when Len >= 0 -> take_payload(Len, Rest);
        _                 -> {error, bad_string_length}
    catch
        _:_ -> {error, bad_string_length}
    end.

take_payload(Len, Rest) when byte_size(Rest) >= Len ->
    <<Payload:Len/binary, Tail/binary>> = Rest,
    {ok, Payload, Tail};
take_payload(_Len, _Rest) ->
    {error, short_string}.

decode_list(<<"e", Rest/binary>>, Acc) ->
    {ok, lists:reverse(Acc), Rest};
decode_list(Bin, Acc) ->
    step_list(decode_value(Bin), Acc).

step_list({ok, V, Rest}, Acc)  -> decode_list(Rest, [V | Acc]);
step_list({error, _} = E, _Acc) -> E.

decode_dict(<<"e", Rest/binary>>, Acc) ->
    {ok, maps:from_list(Acc), Rest};
decode_dict(Bin, Acc) ->
    step_dict(decode_string(Bin), Acc).

step_dict({ok, K, Rest}, Acc)  -> step_dict_value(decode_value(Rest), K, Acc);
step_dict({error, _} = E, _Acc) -> E.

step_dict_value({ok, V, Rest}, K, Acc) ->
    decode_dict(Rest, [{K, V} | Acc]);
step_dict_value({error, _} = E, _K, _Acc) ->
    E.
