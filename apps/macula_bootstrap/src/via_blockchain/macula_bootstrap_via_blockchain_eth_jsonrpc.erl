%% @doc Ethereum JSON-RPC chain adapter for Tier D (Part 5 §7).
%%
%% Reads the foundation's latest `AnchorPublished(bytes)' event from
%% a foundation-deployed contract via `eth_getLogs'. Ethereum has
%% effectively no size limit on log data, so the event's bytes
%% argument carries the `macula_record:encode/1' envelope of the
%% foundation seed list directly — no URL indirection, no hash
%% verification step (the Ed25519 signature on the inner record is
%% the trust anchor).
%%
%% == Probe options ==
%%
%% <ul>
%%   <li>`endpoint' :: binary() — HTTPS JSON-RPC endpoint
%%       (e.g. Infura, Alchemy, Ankr, a self-hosted `geth'). Used
%%       via POST `application/json'.</li>
%%   <li>`contract' :: binary() — foundation contract address,
%%       hex with `0x' prefix.</li>
%%   <li>`topic' :: binary() — event signature hash
%%       `keccak256("AnchorPublished(bytes)")' (or whatever the
%%       foundation publishes), hex with `0x' prefix. Pre-computed
%%       externally — OTP's `crypto' module exposes SHA-3 but not
%%       Ethereum's keccak256 variant.</li>
%%   <li>`from_block' / `to_block' :: binary() — block range.
%%       Defaults `earliest' / `latest'.</li>
%%   <li>`http' :: module() implementing `macula_bootstrap_http'.
%%       Defaults `macula_bootstrap_http_httpc'.</li>
%% </ul>
%%
%% == Layering ==
%%
%% The chain-specific logic (building the RPC request, parsing the
%% response, ABI-decoding the `bytes' event argument) is pure and
%% exercised by unit tests with a canned HTTP module. The concrete
%% `httpc' transport is not unit-tested — it is a thin wrapper.
-module(macula_bootstrap_via_blockchain_eth_jsonrpc).
-behaviour(macula_bootstrap_via_blockchain_transport).

-export([latest_anchor/2]).

%% Exposed for testing.
-export([build_request/3, parse_response/1, decode_bytes_arg/1]).

-export_type([probe_opts/0]).

-type probe_opts() :: #{
    endpoint   := binary(),
    contract   := binary(),
    topic      := binary(),
    from_block => binary(),
    to_block   => binary(),
    http       => module()
}.

-define(DEFAULT_FROM, <<"earliest">>).
-define(DEFAULT_TO,   <<"latest">>).
-define(DEFAULT_HTTP, macula_bootstrap_http_httpc).

-spec latest_anchor(probe_opts(), pos_integer()) ->
          macula_bootstrap_via_blockchain_transport:anchor_result().
latest_anchor(Opts, TimeoutMs) ->
    Endpoint = maps:get(endpoint, Opts),
    Contract = maps:get(contract, Opts),
    Topic    = maps:get(topic,    Opts),
    Range    = #{from => maps:get(from_block, Opts, ?DEFAULT_FROM),
                 to   => maps:get(to_block,   Opts, ?DEFAULT_TO)},
    Http     = maps:get(http,     Opts, ?DEFAULT_HTTP),
    Body     = iolist_to_binary(json:encode(
                                  build_request(Contract, Topic, Range))),
    dispatch(Http:post_json(Endpoint, Body, TimeoutMs)).

dispatch({ok, Body})          -> parse_response(Body);
dispatch({error, _} = Err)    -> Err.

%%==================================================================
%% Pure codec
%%==================================================================

%% @doc Build the JSON-RPC request map for `eth_getLogs'.
-spec build_request(binary(), binary(),
                    #{from := binary(), to := binary()}) ->
          #{binary() => term()}.
build_request(Contract, Topic, #{from := FromBlock, to := ToBlock}) ->
    #{
        <<"jsonrpc">> => <<"2.0">>,
        <<"id">>      => 1,
        <<"method">>  => <<"eth_getLogs">>,
        <<"params">>  =>
            [#{
                <<"address">>   => Contract,
                <<"topics">>    => [Topic],
                <<"fromBlock">> => FromBlock,
                <<"toBlock">>   => ToBlock
             }]
    }.

%% @doc Parse a JSON-RPC response binary.
%% Picks the log with the highest `blockNumber', ABI-decodes its
%% `data' field as a single `bytes' argument.
-spec parse_response(binary()) ->
          {ok, binary()} | {error, term()}.
parse_response(Body) when is_binary(Body) ->
    dispatch_parsed(decode_json(Body)).

dispatch_parsed({ok, #{<<"error">> := Err}}) ->
    {error, {rpc_error, Err}};
dispatch_parsed({ok, #{<<"result">> := Logs}}) when is_list(Logs) ->
    pick_latest(Logs);
dispatch_parsed({ok, _Other}) ->
    {error, bad_rpc_response};
dispatch_parsed({error, _} = E) ->
    E.

decode_json(Body) ->
    try {ok, json:decode(Body)}
    catch _:_ -> {error, bad_json}
    end.

pick_latest([])   -> {error, no_logs};
pick_latest(Logs) ->
    extract_data(lists:foldl(fun keep_later/2, hd(Logs), tl(Logs))).

keep_later(Log, Best) ->
    case block_number(Log) >= block_number(Best) of
        true  -> Log;
        false -> Best
    end.

block_number(#{<<"blockNumber">> := Hex}) when is_binary(Hex) ->
    hex_to_int(Hex);
block_number(_) ->
    0.

extract_data(#{<<"data">> := Hex}) when is_binary(Hex) ->
    chain_hex(Hex);
extract_data(_) ->
    {error, missing_data}.

chain_hex(Hex) ->
    case hex_to_bytes(Hex) of
        {ok, Bytes}    -> decode_bytes_arg(Bytes);
        {error, _} = E -> E
    end.

%% @doc ABI-decode a single `bytes' argument from a log data field.
%% Layout: `<<Offset:256, Len:256, Payload:Len/binary, _Padding/binary>>'.
-spec decode_bytes_arg(binary()) -> {ok, binary()} | {error, bad_abi}.
decode_bytes_arg(<<_Offset:256/big, Len:256/big, Payload:Len/binary,
                   _Pad/binary>>)
  when Len > 0 ->
    {ok, Payload};
decode_bytes_arg(_) ->
    {error, bad_abi}.

hex_to_int(<<"0x", Hex/binary>>) ->
    try binary_to_integer(Hex, 16)
    catch _:_ -> 0
    end;
hex_to_int(_) -> 0.

hex_to_bytes(<<"0x", Hex/binary>>) ->
    try {ok, binary:decode_hex(Hex)}
    catch _:_ -> {error, bad_hex}
    end;
hex_to_bytes(_) ->
    {error, bad_hex}.
