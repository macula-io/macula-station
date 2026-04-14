-module(hecate_bootstrap_base32_tests).
-include_lib("eunit/include/eunit.hrl").

rfc4648_vectors_test_() ->
    %% RFC 4648 §10 test vectors (ASCII inputs). Our encoder is
    %% lowercase and unpadded, so we strip padding and lowercase the
    %% published uppercase expectations before comparing.
    Vectors = [
        {<<"">>,       <<"">>},
        {<<"f">>,      <<"my">>},
        {<<"fo">>,     <<"mzxq">>},
        {<<"foo">>,    <<"mzxw6">>},
        {<<"foob">>,   <<"mzxw6yq">>},
        {<<"fooba">>,  <<"mzxw6ytb">>},
        {<<"foobar">>, <<"mzxw6ytboi">>}
    ],
    [?_assertEqual(Expected, hecate_bootstrap_base32:encode(Input))
     || {Input, Expected} <- Vectors].

round_trip_test_() ->
    [?_assertEqual({ok, Bin},
                   hecate_bootstrap_base32:decode(
                     hecate_bootstrap_base32:encode(Bin)))
     || Bin <- [<<>>,
                <<0>>,
                <<"abc">>,
                crypto:strong_rand_bytes(1),
                crypto:strong_rand_bytes(7),
                crypto:strong_rand_bytes(32),
                crypto:strong_rand_bytes(255)]].

pubkey_fits_in_dns_label_test() ->
    Pub = crypto:strong_rand_bytes(32),
    Encoded = hecate_bootstrap_base32:encode(Pub),
    ?assertEqual(52, byte_size(Encoded)),
    ?assert(byte_size(Encoded) =< 63),
    [?assert((C >= $a andalso C =< $z) orelse
             (C >= $2 andalso C =< $7))
     || <<C>> <= Encoded].

case_insensitive_decode_test() ->
    Pub = crypto:strong_rand_bytes(32),
    Lower = hecate_bootstrap_base32:encode(Pub),
    Upper = string:uppercase(Lower),
    Mixed = mix_case(Lower),
    ?assertEqual({ok, Pub}, hecate_bootstrap_base32:decode(Lower)),
    ?assertEqual({ok, Pub}, hecate_bootstrap_base32:decode(Upper)),
    ?assertEqual({ok, Pub}, hecate_bootstrap_base32:decode(Mixed)).

bad_char_rejected_test_() ->
    [?_assertMatch({error, {bad_char, $=}},
                   hecate_bootstrap_base32:decode(<<"mzxw=">>)),
     ?_assertMatch({error, {bad_char, $0}},
                   hecate_bootstrap_base32:decode(<<"mz0xw">>)),
     ?_assertMatch({error, {bad_char, $1}},
                   hecate_bootstrap_base32:decode(<<"m1zxw">>)),
     ?_assertMatch({error, {bad_char, $ }},
                   hecate_bootstrap_base32:decode(<<"mz xw">>))].

%%------------------------------------------------------------------

mix_case(<<A, B, Rest/binary>>) ->
    <<(upcase(A)), B, (mix_case(Rest))/binary>>;
mix_case(Rest) ->
    Rest.

upcase(C) when C >= $a, C =< $z -> C - $a + $A;
upcase(C)                       -> C.
