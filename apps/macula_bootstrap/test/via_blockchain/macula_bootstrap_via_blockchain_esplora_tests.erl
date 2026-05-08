-module(macula_bootstrap_via_blockchain_esplora_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BASE,    <<"https://esplora.example/api">>).
-define(ADDR,    <<"bc1qfoundationaddressfake0000000000000000">>).
-define(TXS_URL, <<"https://esplora.example/api/address/",
                   "bc1qfoundationaddressfake0000000000000000/txs">>).
-define(URL,     <<"https://pins.example/foundation/seed_list.bin">>).

%%==================================================================
%% parse_op_return
%%==================================================================

parse_op_return_raw_push_test() ->
    %% Raw single-byte push limit = 75 bytes, so we pair a 32-byte
    %% hash with a short URL (4 + 32 + 32 = 68 bytes).
    Hash = crypto:hash(sha256, <<"payload">>),
    ShortUrl = <<"https://x.example/seed_list.bin">>, %% 31 bytes
    Payload = <<"MCLA", Hash/binary, ShortUrl/binary>>,
    Script = <<16#6A, (byte_size(Payload)):8, Payload/binary>>,
    {ok, #{hash := H, url := U}} =
        macula_bootstrap_via_blockchain_esplora:parse_op_return(Script),
    ?assertEqual(Hash, H),
    ?assertEqual(ShortUrl, U).

parse_op_return_pushdata1_test() ->
    Hash = crypto:hash(sha256, <<"payload">>),
    Payload = <<"MCLA", Hash/binary, ?URL/binary>>,
    Script = <<16#6A, 16#4C, (byte_size(Payload)):8, Payload/binary>>,
    ?assertMatch({ok, _},
                 macula_bootstrap_via_blockchain_esplora:parse_op_return(Script)).

parse_op_return_not_our_marker_test() ->
    Script = <<16#6A, 8, "NOPEnope">>,
    ?assertEqual({error, not_our_marker},
                 macula_bootstrap_via_blockchain_esplora:parse_op_return(Script)).

parse_op_return_non_op_return_test() ->
    %% Leading byte is P2WPKH (0x00), not OP_RETURN.
    ?assertEqual({error, bad_op_return_script},
                 macula_bootstrap_via_blockchain_esplora:parse_op_return(
                   <<16#00, 1, 2, 3>>)).

parse_op_return_empty_url_rejected_test() ->
    Hash = crypto:hash(sha256, <<"x">>),
    Payload = <<"MCLA", Hash/binary>>,
    Script = <<16#6A, (byte_size(Payload)):8, Payload/binary>>,
    ?assertEqual({error, empty_url},
                 macula_bootstrap_via_blockchain_esplora:parse_op_return(Script)).

parse_op_return_strips_trailing_null_padding_test() ->
    Hash    = crypto:hash(sha256, <<"payload">>),
    Padding = binary:copy(<<0>>, 8),
    Payload = <<"MCLA", Hash/binary, ?URL/binary, Padding/binary>>,
    Script = <<16#6A, 16#4C, (byte_size(Payload)):8, Payload/binary>>,
    {ok, #{url := U}} =
        macula_bootstrap_via_blockchain_esplora:parse_op_return(Script),
    ?assertEqual(?URL, U).

%%==================================================================
%% extract_anchor_pointer (from a tx map)
%%==================================================================

extract_anchor_skips_non_op_return_vouts_test() ->
    Hash = crypto:hash(sha256, <<"payload">>),
    Payload = <<"MCLA", Hash/binary, ?URL/binary>>,
    Hex = binary:encode_hex(
            <<16#6A, 16#4C, (byte_size(Payload)):8, Payload/binary>>),
    Tx = #{<<"vout">> => [
               #{<<"scriptpubkey_type">> => <<"v0_p2wpkh">>,
                 <<"scriptpubkey">>      => <<"001400">>},
               #{<<"scriptpubkey_type">> => <<"op_return">>,
                 <<"scriptpubkey">>      => string:lowercase(Hex)}
           ]},
    ?assertMatch({ok, #{hash := _, url := _}},
                 macula_bootstrap_via_blockchain_esplora:extract_anchor_pointer(Tx)).

extract_anchor_missing_vout_test() ->
    ?assertEqual({error, bad_tx},
                 macula_bootstrap_via_blockchain_esplora:extract_anchor_pointer(
                   #{<<"txid">> => <<"abc">>})).

extract_anchor_no_op_return_test() ->
    Tx = #{<<"vout">> => [
               #{<<"scriptpubkey_type">> => <<"v0_p2wpkh">>,
                 <<"scriptpubkey">>      => <<"001400">>}
           ]},
    ?assertEqual({error, no_op_return},
                 macula_bootstrap_via_blockchain_esplora:extract_anchor_pointer(Tx)).

%%==================================================================
%% latest_anchor/2 — full pipeline
%%==================================================================

latest_anchor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun happy_end_to_end/1,
      fun no_matching_tx/1,
      fun hash_mismatch_rejected/1,
      fun url_fetch_error/1,
      fun bad_json_response/1
     ]}.

setup() ->
    macula_bootstrap_http_fake:init(),
    ok.

cleanup(_) ->
    macula_bootstrap_http_fake:reset(),
    ok.

happy_end_to_end(_Ctx) ->
    fun() ->
        Bytes = <<"foundation-record-bytes">>,
        Hash  = crypto:hash(sha256, Bytes),
        Txs   = [tx_with_anchor(Hash, ?URL)],
        macula_bootstrap_http_fake:set_get(
          ?TXS_URL, {ok, iolist_to_binary(json:encode(Txs))}),
        macula_bootstrap_http_fake:set_get(?URL, {ok, Bytes}),
        ?assertEqual({ok, Bytes}, probe())
    end.

no_matching_tx(_Ctx) ->
    fun() ->
        Txs = [#{<<"vout">> =>
                    [#{<<"scriptpubkey_type">> => <<"v0_p2wpkh">>,
                       <<"scriptpubkey">>      => <<"001400">>}]}],
        macula_bootstrap_http_fake:set_get(
          ?TXS_URL, {ok, iolist_to_binary(json:encode(Txs))}),
        ?assertEqual({error, no_anchor_found}, probe())
    end.

hash_mismatch_rejected(_Ctx) ->
    fun() ->
        Claimed = crypto:hash(sha256, <<"claimed">>),
        Actual  = <<"actually-different-bytes">>,
        Txs = [tx_with_anchor(Claimed, ?URL)],
        macula_bootstrap_http_fake:set_get(
          ?TXS_URL, {ok, iolist_to_binary(json:encode(Txs))}),
        macula_bootstrap_http_fake:set_get(?URL, {ok, Actual}),
        ?assertEqual({error, hash_mismatch}, probe())
    end.

url_fetch_error(_Ctx) ->
    fun() ->
        Bytes = <<"x">>,
        Hash  = crypto:hash(sha256, Bytes),
        Txs = [tx_with_anchor(Hash, ?URL)],
        macula_bootstrap_http_fake:set_get(
          ?TXS_URL, {ok, iolist_to_binary(json:encode(Txs))}),
        macula_bootstrap_http_fake:set_get(?URL, {error, timeout}),
        ?assertEqual({error, timeout}, probe())
    end.

bad_json_response(_Ctx) ->
    fun() ->
        macula_bootstrap_http_fake:set_get(
          ?TXS_URL, {ok, <<"not json">>}),
        ?assertEqual({error, bad_esplora_json}, probe())
    end.

%%==================================================================
%% Helpers
%%==================================================================

probe() ->
    macula_bootstrap_via_blockchain_esplora:latest_anchor(
      #{base_url => ?BASE,
        address  => ?ADDR,
        http     => macula_bootstrap_http_fake},
      500).

tx_with_anchor(Hash, Url) ->
    Payload = <<"MCLA", Hash/binary, Url/binary>>,
    Hex = binary:encode_hex(
            <<16#6A, 16#4C, (byte_size(Payload)):8, Payload/binary>>),
    #{<<"txid">> => <<"deadbeef">>,
      <<"vout">> => [
          #{<<"scriptpubkey_type">> => <<"op_return">>,
            <<"scriptpubkey">>      => string:lowercase(Hex)}
      ]}.
