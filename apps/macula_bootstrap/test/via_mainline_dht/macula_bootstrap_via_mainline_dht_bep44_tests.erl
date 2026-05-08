-module(macula_bootstrap_via_mainline_dht_bep44_tests).
-include_lib("eunit/include/eunit.hrl").

target_id_no_salt_test() ->
    Pub = <<1:256>>,
    ?assertEqual(crypto:hash(sha, Pub),
                 macula_bootstrap_via_mainline_dht_bep44:target_id(Pub)).

target_id_with_salt_test() ->
    Pub  = <<1:256>>,
    Salt = <<"foo">>,
    ?assertEqual(crypto:hash(sha, <<Pub/binary, Salt/binary>>),
                 macula_bootstrap_via_mainline_dht_bep44:target_id(Pub, Salt)).

signed_payload_shape_test() ->
    %% Example from BEP 44:
    %% 3:seqi1234e1:v12:Hello World!
    Payload = macula_bootstrap_via_mainline_dht_bep44:signed_payload(
                1234, <<"Hello World!">>),
    ?assertEqual(<<"3:seqi1234e1:v12:Hello World!">>, Payload).

signed_payload_with_salt_exact_test() ->
    Payload = macula_bootstrap_via_mainline_dht_bep44:signed_payload(
                1234, <<"Hello World!">>, <<"foobar">>),
    ?assertEqual(<<"4:salt6:foobar3:seqi1234e1:v12:Hello World!">>,
                 Payload).

sign_and_verify_no_salt_test() ->
    Kp     = macula_identity:generate(),
    Value  = <<"payload-bytes">>,
    Item   = macula_bootstrap_via_mainline_dht_bep44:sign(1, Value, Kp),
    ?assertEqual(ok, macula_bootstrap_via_mainline_dht_bep44:verify(Item)).

sign_and_verify_with_salt_test() ->
    Kp     = macula_identity:generate(),
    Salt   = <<"realm-a">>,
    Value  = <<"payload-bytes">>,
    Item   = macula_bootstrap_via_mainline_dht_bep44:sign(3, Value, Salt, Kp),
    ?assertEqual(ok, macula_bootstrap_via_mainline_dht_bep44:verify(Item)).

tampered_value_rejected_test() ->
    Kp     = macula_identity:generate(),
    Item0  = macula_bootstrap_via_mainline_dht_bep44:sign(5, <<"original">>, Kp),
    Tampered = Item0#{value := <<"modified">>},
    ?assertEqual({error, signature_invalid},
                 macula_bootstrap_via_mainline_dht_bep44:verify(Tampered)).

tampered_seq_rejected_test() ->
    Kp     = macula_identity:generate(),
    Item0  = macula_bootstrap_via_mainline_dht_bep44:sign(5, <<"v">>, Kp),
    ?assertEqual({error, signature_invalid},
                 macula_bootstrap_via_mainline_dht_bep44:verify(Item0#{seq := 6})).

wrong_pubkey_rejected_test() ->
    Kp      = macula_identity:generate(),
    Imp     = macula_identity:generate(),
    Item0   = macula_bootstrap_via_mainline_dht_bep44:sign(1, <<"v">>, Kp),
    ImpPub  = macula_identity:public(Imp),
    ?assertEqual({error, signature_invalid},
                 macula_bootstrap_via_mainline_dht_bep44:verify(Item0#{pubkey := ImpPub})).

salt_mismatch_rejected_test() ->
    Kp = macula_identity:generate(),
    Item0 = macula_bootstrap_via_mainline_dht_bep44:sign(1, <<"v">>, <<"salt-a">>, Kp),
    ?assertEqual({error, signature_invalid},
                 macula_bootstrap_via_mainline_dht_bep44:verify(
                   Item0#{salt := <<"salt-b">>})).

bad_pubkey_shape_rejected_test() ->
    Item = #{pubkey => <<"short">>, seq => 1, value => <<>>,
             sig => <<0:512>>},
    ?assertEqual({error, bad_pubkey},
                 macula_bootstrap_via_mainline_dht_bep44:verify(Item)).

bad_sig_shape_rejected_test() ->
    Item = #{pubkey => <<0:256>>, seq => 1, value => <<>>,
             sig => <<0:128>>},
    ?assertEqual({error, bad_sig},
                 macula_bootstrap_via_mainline_dht_bep44:verify(Item)).

negative_seq_rejected_test() ->
    Item = #{pubkey => <<0:256>>, seq => -1, value => <<>>,
             sig => <<0:512>>},
    ?assertEqual({error, bad_seq},
                 macula_bootstrap_via_mainline_dht_bep44:verify(Item)).
