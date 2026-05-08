-module(macula_bootstrap_via_mainline_dht_bencode_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Encode — published BEP 3 vectors
%%==================================================================

encode_int_test_() ->
    [?_assertEqual(<<"i0e">>,    macula_bootstrap_via_mainline_dht_bencode:encode(0)),
     ?_assertEqual(<<"i42e">>,   macula_bootstrap_via_mainline_dht_bencode:encode(42)),
     ?_assertEqual(<<"i-1e">>,   macula_bootstrap_via_mainline_dht_bencode:encode(-1)),
     ?_assertEqual(<<"i1234567890e">>,
                   macula_bootstrap_via_mainline_dht_bencode:encode(1234567890))].

encode_binary_test_() ->
    [?_assertEqual(<<"0:">>,     macula_bootstrap_via_mainline_dht_bencode:encode(<<>>)),
     ?_assertEqual(<<"5:hello">>, macula_bootstrap_via_mainline_dht_bencode:encode(<<"hello">>)),
     ?_assertEqual(<<"4:\0\0\0\0">>,
                   macula_bootstrap_via_mainline_dht_bencode:encode(<<0, 0, 0, 0>>))].

encode_list_test_() ->
    [?_assertEqual(<<"le">>, macula_bootstrap_via_mainline_dht_bencode:encode([])),
     ?_assertEqual(<<"l4:spami42ee">>,
                   macula_bootstrap_via_mainline_dht_bencode:encode([<<"spam">>, 42])),
     ?_assertEqual(<<"lli1ei2eeli3ei4eee">>,
                   macula_bootstrap_via_mainline_dht_bencode:encode([[1, 2], [3, 4]]))].

encode_dict_sorts_keys_test() ->
    Bin = macula_bootstrap_via_mainline_dht_bencode:encode(
            #{<<"b">> => 2, <<"a">> => 1, <<"c">> => 3}),
    ?assertEqual(<<"d1:ai1e1:bi2e1:ci3ee">>, Bin).

encode_dict_nested_test() ->
    Bin = macula_bootstrap_via_mainline_dht_bencode:encode(
            #{<<"t">> => <<"aa">>,
              <<"y">> => <<"q">>,
              <<"a">> => #{<<"id">>     => <<1:160>>,
                           <<"target">> => <<2:160>>}}),
    ?assertMatch(<<"d1:ad", _/binary>>, Bin).

%%==================================================================
%% Decode
%%==================================================================

decode_int_test_() ->
    [?_assertEqual({ok, 0},    macula_bootstrap_via_mainline_dht_bencode:decode(<<"i0e">>)),
     ?_assertEqual({ok, 42},   macula_bootstrap_via_mainline_dht_bencode:decode(<<"i42e">>)),
     ?_assertEqual({ok, -1},   macula_bootstrap_via_mainline_dht_bencode:decode(<<"i-1e">>))].

decode_string_test_() ->
    [?_assertEqual({ok, <<"hello">>},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"5:hello">>)),
     ?_assertEqual({ok, <<>>},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"0:">>)),
     ?_assertEqual({ok, <<0, 1, 2>>},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"3:", 0, 1, 2>>))].

decode_list_test_() ->
    [?_assertEqual({ok, []},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"le">>)),
     ?_assertEqual({ok, [<<"spam">>, 42]},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"l4:spami42ee">>))].

decode_dict_test_() ->
    [?_assertEqual({ok, #{}},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"de">>)),
     ?_assertEqual({ok, #{<<"a">> => 1, <<"b">> => 2}},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"d1:ai1e1:bi2ee">>))].

%%==================================================================
%% Round-trip
%%==================================================================

roundtrip_test_() ->
    Cases = [0, 1, -1, 65535,
             <<>>, <<"hello">>, <<0, 1, 2, 3, 4>>,
             [],
             [1, 2, 3],
             [<<"a">>, <<"b">>],
             #{},
             #{<<"a">> => 1},
             #{<<"a">> => 1, <<"b">> => <<"x">>, <<"z">> => [1, 2]},
             #{<<"outer">> => #{<<"inner">> => <<"deep">>}}],
    [?_assertEqual({ok, V},
                   macula_bootstrap_via_mainline_dht_bencode:decode(
                     macula_bootstrap_via_mainline_dht_bencode:encode(V)))
     || V <- Cases].

%%==================================================================
%% Error paths
%%==================================================================

decode_errors_test_() ->
    [?_assertEqual({error, bad_syntax},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"x">>)),
     ?_assertEqual({error, unterminated_int},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"i42">>)),
     ?_assertEqual({error, bad_int},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"ioe">>)),
     ?_assertEqual({error, no_colon},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"5hello">>)),
     ?_assertEqual({error, short_string},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"10:hi">>)),
     ?_assertEqual({error, trailing_data},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"i1eextra">>)),
     ?_assertEqual({error, bad_string_length},
                   macula_bootstrap_via_mainline_dht_bencode:decode(<<"99x:">>))].

%%==================================================================
%% Canonicalisation — dict-key order is deterministic
%%==================================================================

canonical_encoding_test() ->
    A = macula_bootstrap_via_mainline_dht_bencode:encode(
          #{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3}),
    B = macula_bootstrap_via_mainline_dht_bencode:encode(
          #{<<"c">> => 3, <<"a">> => 1, <<"b">> => 2}),
    ?assertEqual(A, B).
