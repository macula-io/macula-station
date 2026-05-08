-module(macula_bootstrap_via_blockchain_eth_jsonrpc_tests).
-include_lib("eunit/include/eunit.hrl").

-define(ENDPOINT, <<"https://eth.example/v1/rpc">>).
-define(CONTRACT, <<"0x00000000000000000000000000000000000000ff">>).
-define(TOPIC,    <<"0x",
                    "aabbccddeeff00112233445566778899",
                    "aabbccddeeff00112233445566778899">>).

%%==================================================================
%% build_request
%%==================================================================

build_request_shape_test() ->
    Req = macula_bootstrap_via_blockchain_eth_jsonrpc:build_request(
            ?CONTRACT, ?TOPIC,
            #{from => <<"earliest">>, to => <<"latest">>}),
    #{<<"jsonrpc">> := <<"2.0">>,
      <<"method">>  := <<"eth_getLogs">>,
      <<"params">>  := [#{<<"address">>  := ?CONTRACT,
                          <<"topics">>   := [?TOPIC],
                          <<"fromBlock">> := <<"earliest">>,
                          <<"toBlock">>   := <<"latest">>}]} = Req.

build_request_roundtrips_via_json_test() ->
    Req = macula_bootstrap_via_blockchain_eth_jsonrpc:build_request(
            ?CONTRACT, ?TOPIC,
            #{from => <<"0x100">>, to => <<"0x200">>}),
    Json = iolist_to_binary(json:encode(Req)),
    Decoded = json:decode(Json),
    ?assertEqual(Req, Decoded).

%%==================================================================
%% parse_response
%%==================================================================

parse_response_no_logs_test() ->
    Body = iolist_to_binary(json:encode(rpc_ok([]))),
    ?assertEqual({error, no_logs},
                 macula_bootstrap_via_blockchain_eth_jsonrpc:parse_response(Body)).

parse_response_rpc_error_test() ->
    Body = iolist_to_binary(json:encode(
             #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => 1,
               <<"error">> => #{<<"code">> => -32000,
                                 <<"message">> => <<"nope">>}})),
    ?assertMatch({error, {rpc_error, _}},
                 macula_bootstrap_via_blockchain_eth_jsonrpc:parse_response(Body)).

parse_response_bad_json_test() ->
    ?assertEqual({error, bad_json},
                 macula_bootstrap_via_blockchain_eth_jsonrpc:parse_response(
                   <<"not json">>)).

parse_response_happy_path_test() ->
    Payload = <<1, 2, 3, 4, 5>>,
    Log = mk_log(<<"0x1">>, Payload),
    Body = iolist_to_binary(json:encode(rpc_ok([Log]))),
    ?assertEqual({ok, Payload},
                 macula_bootstrap_via_blockchain_eth_jsonrpc:parse_response(Body)).

parse_response_picks_highest_block_test() ->
    Old = mk_log(<<"0x1">>, <<"old">>),
    Mid = mk_log(<<"0xa">>, <<"mid">>),
    New = mk_log(<<"0xff">>, <<"new">>),
    Body = iolist_to_binary(json:encode(rpc_ok([Old, New, Mid]))),
    ?assertEqual({ok, <<"new">>},
                 macula_bootstrap_via_blockchain_eth_jsonrpc:parse_response(Body)).

parse_response_missing_data_test() ->
    Log = #{<<"blockNumber">> => <<"0x1">>,
            <<"address">> => ?CONTRACT},
    Body = iolist_to_binary(json:encode(rpc_ok([Log]))),
    ?assertEqual({error, missing_data},
                 macula_bootstrap_via_blockchain_eth_jsonrpc:parse_response(Body)).

parse_response_bad_data_hex_test() ->
    Log = #{<<"blockNumber">> => <<"0x1">>,
            <<"data">> => <<"0xzz">>},
    Body = iolist_to_binary(json:encode(rpc_ok([Log]))),
    ?assertEqual({error, bad_hex},
                 macula_bootstrap_via_blockchain_eth_jsonrpc:parse_response(Body)).

%%==================================================================
%% decode_bytes_arg (ABI)
%%==================================================================

decode_bytes_arg_happy_test() ->
    Payload = <<"hello, world — this is the record bytes">>,
    Abi = abi_bytes(Payload),
    ?assertEqual({ok, Payload},
                 macula_bootstrap_via_blockchain_eth_jsonrpc:decode_bytes_arg(Abi)).

decode_bytes_arg_zero_length_rejected_test() ->
    ?assertEqual({error, bad_abi},
                 macula_bootstrap_via_blockchain_eth_jsonrpc:decode_bytes_arg(
                   <<0:256, 0:256>>)).

decode_bytes_arg_short_header_test() ->
    ?assertEqual({error, bad_abi},
                 macula_bootstrap_via_blockchain_eth_jsonrpc:decode_bytes_arg(
                   <<0:200>>)).

decode_bytes_arg_length_exceeds_payload_test() ->
    %% Claims 100 bytes but only has 4 after the header.
    Bin = <<32:256, 100:256, 0, 0, 0, 0>>,
    ?assertEqual({error, bad_abi},
                 macula_bootstrap_via_blockchain_eth_jsonrpc:decode_bytes_arg(Bin)).

%%==================================================================
%% latest_anchor/2 — full end-to-end with canned HTTP
%%==================================================================

latest_anchor_happy_end_to_end_test_() ->
    {setup,
     fun setup_http/0,
     fun cleanup_http/1,
     fun(_Ctx) ->
        fun() ->
            Payload = <<"macula-record-bytes">>,
            Log = mk_log(<<"0x1">>, Payload),
            Body = iolist_to_binary(json:encode(rpc_ok([Log]))),
            macula_bootstrap_http_fake:set_post(?ENDPOINT, {ok, Body}),
            Opts = #{endpoint => ?ENDPOINT,
                     contract => ?CONTRACT,
                     topic    => ?TOPIC,
                     http     => macula_bootstrap_http_fake},
            ?assertEqual({ok, Payload},
                         macula_bootstrap_via_blockchain_eth_jsonrpc:latest_anchor(
                           Opts, 500))
        end
     end}.

latest_anchor_http_error_wrapped_test_() ->
    {setup,
     fun setup_http/0,
     fun cleanup_http/1,
     fun(_Ctx) ->
        fun() ->
            macula_bootstrap_http_fake:set_post(
              ?ENDPOINT, {error, timeout}),
            Opts = #{endpoint => ?ENDPOINT, contract => ?CONTRACT,
                     topic    => ?TOPIC,
                     http     => macula_bootstrap_http_fake},
            ?assertEqual({error, timeout},
                         macula_bootstrap_via_blockchain_eth_jsonrpc:latest_anchor(
                           Opts, 500))
        end
     end}.

%%==================================================================
%% Helpers
%%==================================================================

setup_http() ->
    macula_bootstrap_http_fake:init(),
    ok.

cleanup_http(_) ->
    macula_bootstrap_http_fake:reset(),
    ok.

rpc_ok(Result) ->
    #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => 1,
      <<"result">>  => Result}.

mk_log(BlockNumHex, PayloadBytes) ->
    AbiHex = <<"0x",
               (binary:encode_hex(abi_bytes(PayloadBytes)))/binary>>,
    #{<<"blockNumber">> => BlockNumHex,
      <<"address">>     => ?CONTRACT,
      <<"topics">>      => [?TOPIC],
      <<"data">>        => string:lowercase(AbiHex)}.

%% ABI-encode a single `bytes' argument: 32-byte offset (0x20),
%% 32-byte length, then payload zero-padded to 32-byte boundary.
abi_bytes(Bin) ->
    Len = byte_size(Bin),
    Pad = case Len rem 32 of
              0 -> 0;
              R -> 32 - R
          end,
    <<32:256/big, Len:256/big, Bin/binary, 0:(Pad * 8)>>.
