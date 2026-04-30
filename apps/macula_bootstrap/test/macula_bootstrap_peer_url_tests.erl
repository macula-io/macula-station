%% EUnit tests for macula_bootstrap_peer_url.
-module(macula_bootstrap_peer_url_tests).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------
%% encode / decode roundtrip
%%------------------------------------------------------------------

roundtrip_empty_hints_test() ->
    {Url, OrigRecord} = build_url([]),
    {ok, Decoded, Addrs} = macula_bootstrap_peer_url:decode(Url),
    ?assertEqual([], Addrs),
    ?assertEqual(macula_record:key(OrigRecord), macula_record:key(Decoded)),
    ?assertEqual(macula_record:version(OrigRecord),
                 macula_record:version(Decoded)).

roundtrip_with_address_hints_test() ->
    Addr1 = #{
        {text, <<"v6">>}   => {text, <<"2a02::1">>},
        {text, <<"port">>} => 7000,
        {text, <<"kind">>} => {text, <<"primary">>}
    },
    Addr2 = #{
        {text, <<"v6">>}   => {text, <<"2a01::2">>},
        {text, <<"port">>} => 7001,
        {text, <<"kind">>} => {text, <<"fallback">>}
    },
    {Url, _Rec} = build_url([Addr1, Addr2]),
    {ok, _Decoded, Addrs} = macula_bootstrap_peer_url:decode(Url),
    ?assertEqual(2, length(Addrs)),
    [A1, A2] = Addrs,
    ?assertEqual({text, <<"2a02::1">>}, maps:get({text, <<"v6">>}, A1)),
    ?assertEqual({text, <<"2a01::2">>}, maps:get({text, <<"v6">>}, A2)).

scheme_prefix_is_present_test() ->
    {Url, _} = build_url([]),
    ?assertMatch(<<"macula-peer:", _/binary>>, Url).

%%------------------------------------------------------------------
%% decode failure modes
%%------------------------------------------------------------------

decode_bad_scheme_test() ->
    ?assertEqual({error, bad_scheme},
                 macula_bootstrap_peer_url:decode(<<"http://nope">>)).

decode_malformed_base64_test() ->
    ?assertEqual({error, bad_base64},
                 macula_bootstrap_peer_url:decode(
                   <<"macula-peer:!!!not-base64!!!">>)).

decode_non_cbor_payload_test() ->
    %% Valid base64url of non-CBOR bytes.
    Nonsense = base64:encode(<<"this is not CBOR at all">>,
                             #{mode => urlsafe, padding => false}),
    Url = <<"macula-peer:", Nonsense/binary>>,
    ?assertMatch({error, _}, macula_bootstrap_peer_url:decode(Url)).

decode_wrong_record_type_test() ->
    %% Build an endorsement record (type 0x05) and wrap it — should
    %% be rejected because only node_record (0x01) is acceptable.
    AdminKp = macula_identity:generate(),
    RealmId = macula_identity:public(AdminKp),
    Member  = crypto:strong_rand_bytes(32),
    Endorsement = macula_record:sign(
        macula_record:realm_member_endorsement(
          RealmId, #{realm => RealmId, member_node => Member,
                     roles => [<<"member">>]}),
        AdminKp),
    Url = macula_bootstrap_peer_url:encode(Endorsement, []),
    ?assertEqual({error, wrong_type},
                 macula_bootstrap_peer_url:decode(Url)).

decode_rejects_tampered_record_test() ->
    {Url, _} = build_url([]),
    %% Flip a byte in the middle of the base64 body.
    <<Prefix:20/binary, Byte:8, Rest/binary>> = Url,
    Flipped = <<Prefix/binary, (Byte bxor 1):8, Rest/binary>>,
    Result = macula_bootstrap_peer_url:decode(Flipped),
    ?assertMatch({error, _}, Result).

decode_rejects_expired_record_test() ->
    Kp = macula_identity:generate(),
    R = macula_record:node_record(macula_identity:public(Kp), [], 0,
                                  #{ttl_ms => 1}),
    Signed = macula_record:sign(R, Kp),
    Url = macula_bootstrap_peer_url:encode(Signed, []),
    timer:sleep(5),
    ?assertEqual({error, expired},
                 macula_bootstrap_peer_url:decode(Url)).

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

build_url(Addrs) ->
    Kp = macula_identity:generate(),
    Record = macula_record:sign(
        macula_record:node_record(macula_identity:public(Kp), [], 0),
        Kp),
    {macula_bootstrap_peer_url:encode(Record, Addrs), Record}.
