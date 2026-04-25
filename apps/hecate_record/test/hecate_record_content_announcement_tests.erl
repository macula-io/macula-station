-module(hecate_record_content_announcement_tests).
-include_lib("eunit/include/eunit.hrl").

mcid()     -> <<1, 16#56, (crypto:strong_rand_bytes(32))/binary>>.
keypair()  -> macula_identity:generate().
station()  -> Kp = keypair(), {Kp, macula_identity:public(Kp)}.

constructor_3_arg_carries_announcer_mcid_endpoint_test() ->
    {_Kp, Pub} = station(),
    M = mcid(),
    R = hecate_record:content_announcement(Pub, M, <<"quic://h:4">>),
    ?assertEqual(16#11, hecate_record:type(R)),
    ?assertEqual(Pub,   hecate_record:key(R)).

constructor_4_arg_with_metadata_test() ->
    {_Kp, Pub} = station(),
    M = mcid(),
    R = hecate_record:content_announcement(Pub, M, <<"quic://h:4">>,
                                            #{name => <<"file.txt">>,
                                              size => 4096,
                                              chunk_count => 2}),
    Payload = hecate_record:payload(R),
    ?assertEqual({text, <<"file.txt">>},
                 maps:get({text, <<"name">>}, Payload)),
    ?assertEqual(4096,
                 maps:get({text, <<"size">>}, Payload)),
    ?assertEqual(2,
                 maps:get({text, <<"chunk_count">>}, Payload)).

constructor_rejects_short_mcid_test() ->
    {_Kp, Pub} = station(),
    ?assertError(function_clause,
                 hecate_record:content_announcement(
                   Pub, <<"too short">>, <<"quic://h:4">>)).

sign_verify_roundtrip_test() ->
    {Kp, Pub} = station(),
    R = hecate_record:content_announcement(Pub, mcid(), <<"e">>),
    Signed = hecate_record:sign(R, Kp),
    ?assertMatch({ok, _}, hecate_record:verify(Signed)).

encode_decode_roundtrip_test() ->
    {Kp, Pub} = station(),
    R = hecate_record:content_announcement(
          Pub, mcid(), <<"quic://h:4">>,
          #{name => <<"x">>, size => 1, chunk_count => 1}),
    Signed = hecate_record:sign(R, Kp),
    Bin = hecate_record:encode(Signed),
    {ok, Decoded} = hecate_record:decode(Bin),
    ?assertEqual(hecate_record:type(Signed),    hecate_record:type(Decoded)),
    ?assertEqual(hecate_record:key(Signed),     hecate_record:key(Decoded)),
    ?assertEqual(hecate_record:payload(Signed), hecate_record:payload(Decoded)),
    ?assertMatch({ok, _}, hecate_record:verify(Decoded)).
