%% @doc Bitcoin Esplora chain adapter for Tier D (Part 5 §7).
%%
%% Reads the foundation's latest anchor from a Blockstream/Esplora
%% HTTP API (`GET /address/<addr>/txs'). Bitcoin's 80-byte OP_RETURN
%% standardness limit cannot hold a full `hecate_record' envelope, so
%% the foundation publishes a <em>pointer</em>: a 32-byte SHA-256 of
%% the record plus an HTTPS URL where the record is hosted. The
%% adapter fetches the URL, verifies the hash, and returns the
%% record bytes upstream.
%%
%% == OP_RETURN anchor format (up to 80 bytes) ==
%%
%% ```
%%   4 bytes : magic "MCLA"
%%  32 bytes : SHA-256(anchor_record_bytes)
%%   N bytes : UTF-8 URL, zero-padded up to the remaining space (max 44)
%% '''
%%
%% Earliest vout in the earliest tx (Esplora returns newest-first)
%% wins. The chain is a <em>locator</em>, not a trust anchor — the
%% hash verification plus the Ed25519 signature on the inner record
%% give trust.
%%
%% == Probe options ==
%%
%% <ul>
%%   <li>`base_url' :: binary() — e.g. `<<"https://blockstream.info/api">>'.</li>
%%   <li>`address'  :: binary() — foundation's Bitcoin address.</li>
%%   <li>`http'     :: module() — defaults `hecate_bootstrap_http_httpc'.</li>
%% </ul>
-module(hecate_bootstrap_chain_esplora).
-behaviour(hecate_bootstrap_chain_transport).

-export([latest_anchor/2]).

%% Exposed for testing.
-export([extract_anchor_pointer/1, parse_op_return/1]).

-export_type([probe_opts/0, anchor_pointer/0]).

-type probe_opts() :: #{
    base_url := binary(),
    address  := binary(),
    http     => module()
}.

-type anchor_pointer() :: #{
    hash := <<_:256>>,
    url  := binary()
}.

-define(MAGIC,         <<"MCLA">>).
-define(OP_RETURN,     16#6A).
-define(OP_PUSHDATA1,  16#4C).
-define(DEFAULT_HTTP,  hecate_bootstrap_http_httpc).

-spec latest_anchor(probe_opts(), pos_integer()) ->
          hecate_bootstrap_chain_transport:anchor_result().
latest_anchor(Opts, TimeoutMs) ->
    Base    = maps:get(base_url, Opts),
    Address = maps:get(address,  Opts),
    Http    = maps:get(http,     Opts, ?DEFAULT_HTTP),
    TxsUrl  = <<Base/binary, "/address/", Address/binary, "/txs">>,
    dispatch(Http:get(TxsUrl, TimeoutMs), Http, TimeoutMs).

dispatch({ok, Json}, Http, TimeoutMs) ->
    chain([
        fun() -> parse_txs(Json) end,
        fun(Txs) -> first_anchor(Txs) end,
        fun(#{hash := H, url := U}) ->
                fetch_and_verify(Http, U, H, TimeoutMs)
        end
    ]);
dispatch({error, _} = E, _Http, _TimeoutMs) ->
    E.

%%==================================================================
%% Pure codec
%%==================================================================

parse_txs(Json) ->
    try
        case json:decode(Json) of
            L when is_list(L) -> {ok, L};
            _                 -> {error, bad_esplora_response}
        end
    catch _:_ ->
        {error, bad_esplora_json}
    end.

first_anchor([])        -> {error, no_anchor_found};
first_anchor([Tx | Rest]) ->
    case extract_anchor_pointer(Tx) of
        {ok, _} = Ok -> Ok;
        {error, _}   -> first_anchor(Rest)
    end.

%% @doc Extract an anchor pointer from an Esplora tx map.
-spec extract_anchor_pointer(map()) ->
          {ok, anchor_pointer()} | {error, term()}.
extract_anchor_pointer(#{<<"vout">> := Vouts}) when is_list(Vouts) ->
    first_ptr_in_vouts(Vouts);
extract_anchor_pointer(_) ->
    {error, bad_tx}.

first_ptr_in_vouts([]) ->
    {error, no_op_return};
first_ptr_in_vouts([V | Rest]) ->
    case extract_ptr_from_vout(V) of
        {ok, _} = Ok -> Ok;
        {error, _}   -> first_ptr_in_vouts(Rest)
    end.

extract_ptr_from_vout(#{<<"scriptpubkey">> := Hex,
                        <<"scriptpubkey_type">> := <<"op_return">>})
  when is_binary(Hex) ->
    case hex_to_bytes(Hex) of
        {ok, Script}    -> parse_op_return(Script);
        {error, _} = E  -> E
    end;
extract_ptr_from_vout(_) ->
    {error, not_op_return}.

%% @doc Decode a Bitcoin OP_RETURN script.
%%
%% Accepts the two push forms: raw single-byte push (`<<Op, Len, Data>>'
%% with `1 <= Len <= 75') and OP_PUSHDATA1 (`<<Op, 0x4C, Len, Data>>').
-spec parse_op_return(binary()) ->
          {ok, anchor_pointer()} | {error, term()}.
parse_op_return(<<?OP_RETURN, ?OP_PUSHDATA1, Len:8, Data:Len/binary>>) ->
    parse_anchor_payload(Data);
parse_op_return(<<?OP_RETURN, Len:8, Data:Len/binary>>)
  when Len >= 1, Len =< 75 ->
    parse_anchor_payload(Data);
parse_op_return(_) ->
    {error, bad_op_return_script}.

parse_anchor_payload(<<"MCLA", Hash:32/binary, UrlBytes/binary>>) ->
    case trim_url(UrlBytes) of
        <<>>  -> {error, empty_url};
        Url   -> {ok, #{hash => Hash, url => Url}}
    end;
parse_anchor_payload(_) ->
    {error, not_our_marker}.

trim_url(Bin) ->
    case binary:split(Bin, <<0>>) of
        [Head | _] -> Head;
        _          -> Bin
    end.

%%==================================================================
%% Hash-verified fetch
%%==================================================================

fetch_and_verify(Http, Url, ExpectedHash, TimeoutMs) ->
    case Http:get(Url, TimeoutMs) of
        {ok, Bytes} ->
            check_hash(crypto:hash(sha256, Bytes), ExpectedHash, Bytes);
        {error, _} = E ->
            E
    end.

check_hash(Hash, Hash, Bytes) -> {ok, Bytes};
check_hash(_, _, _)           -> {error, hash_mismatch}.

%%==================================================================
%% Helpers
%%==================================================================

hex_to_bytes(Hex) ->
    try {ok, binary:decode_hex(Hex)}
    catch _:_ -> {error, bad_hex}
    end.

chain([Step | Rest])           -> chain(Step(), Rest).
chain({error, _} = E, _Rest)   -> E;
chain({ok, V}, [])             -> {ok, V};
chain({ok, V}, [Step | Rest])  -> chain(Step(V), Rest).
