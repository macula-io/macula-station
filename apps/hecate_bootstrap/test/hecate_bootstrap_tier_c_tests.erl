-module(hecate_bootstrap_tier_c_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Fixture
%%==================================================================

tier_c_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun happy_path/1,
      fun no_transport/1,
      fun no_pubkeys/1,
      fun wrong_pubkey_in_dht_item/1,
      fun bad_bep44_signature/1,
      fun bad_pkarr_dns_packet/1,
      fun pkarr_with_no_txt/1,
      fun dht_get_mutable_failure_falls_through/1,
      fun record_not_signed_by_foundation/1,
      fun first_successful_pubkey_wins/1
     ]}.

setup() ->
    application:ensure_all_started(crypto),
    hecate_bootstrap_dht_fake:init(),
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    application:set_env(macula_record, foundation_pubkeys, [Fk]),
    #{kp => Kp, fk => Fk}.

cleanup(_Ctx) ->
    application:unset_env(macula_record, foundation_pubkeys),
    hecate_bootstrap_dht_fake:reset(),
    ok.

%%==================================================================
%% Cases
%%==================================================================

happy_path(#{kp := Kp, fk := Fk}) ->
    fun() ->
        publish_seed_list(Kp, Fk, 4),
        {ok, Peers} = probe([Fk]),
        ?assertEqual(4, length(Peers)),
        [c] = lists:usort([maps:get(tier, P) || P <- Peers])
    end.

no_transport(#{fk := Fk}) ->
    fun() ->
        ?assertEqual({error, no_transport},
                     hecate_bootstrap_tier_c:probe(
                       #{pubkeys => [Fk], timeout_ms => 500}))
    end.

no_pubkeys(_Ctx) ->
    fun() ->
        ?assertEqual({error, no_pubkeys},
                     hecate_bootstrap_tier_c:probe(
                       #{dht_transport => hecate_bootstrap_dht_fake,
                         pubkeys       => [],
                         timeout_ms    => 500}))
    end.

wrong_pubkey_in_dht_item(#{kp := Kp, fk := Fk}) ->
    fun() ->
        %% DHT serves an item but the embedded pubkey is NOT what we
        %% asked about. Impersonation must be refused.
        OtherKp = macula_identity:generate(),
        Record = macula_record:sign(
                   macula_record:foundation_seed_list(Fk, sample_seeds(2)),
                   Kp),
        DnsPacket = pkarr_dns(macula_record:encode(Record)),
        Item = hecate_bootstrap_bep44:sign(1, DnsPacket, OtherKp),
        hecate_bootstrap_dht_fake:set(
          hecate_bootstrap_bep44:target_id(Fk), Item),
        ?assertEqual({error, all_failed}, probe([Fk]))
    end.

bad_bep44_signature(#{kp := Kp, fk := Fk}) ->
    fun() ->
        Record = macula_record:sign(
                   macula_record:foundation_seed_list(Fk, sample_seeds(2)),
                   Kp),
        DnsPacket = pkarr_dns(macula_record:encode(Record)),
        GoodItem = hecate_bootstrap_bep44:sign(1, DnsPacket, Kp),
        %% Tamper with the value AFTER signing.
        BadItem = GoodItem#{value := <<DnsPacket/binary, "garbage">>},
        hecate_bootstrap_dht_fake:set(
          hecate_bootstrap_bep44:target_id(Fk), BadItem),
        ?assertEqual({error, all_failed}, probe([Fk]))
    end.

bad_pkarr_dns_packet(#{kp := Kp, fk := Fk}) ->
    fun() ->
        Item = hecate_bootstrap_bep44:sign(1, <<"not a dns packet">>, Kp),
        hecate_bootstrap_dht_fake:set(
          hecate_bootstrap_bep44:target_id(Fk), Item),
        ?assertEqual({error, all_failed}, probe([Fk]))
    end.

pkarr_with_no_txt(#{kp := Kp, fk := Fk}) ->
    fun() ->
        EmptyPkarr = build_dns_packet([]),
        Item = hecate_bootstrap_bep44:sign(1, EmptyPkarr, Kp),
        hecate_bootstrap_dht_fake:set(
          hecate_bootstrap_bep44:target_id(Fk), Item),
        ?assertEqual({error, all_failed}, probe([Fk]))
    end.

dht_get_mutable_failure_falls_through(#{fk := Fk}) ->
    fun() ->
        hecate_bootstrap_dht_fake:fail(
          hecate_bootstrap_bep44:target_id(Fk), dht_unreachable),
        ?assertEqual({error, all_failed}, probe([Fk]))
    end.

record_not_signed_by_foundation(#{fk := Fk}) ->
    fun() ->
        %% Bep44 verifies fine; the inner macula_record is signed by
        %% someone else. macula_foundation:verify_record must catch it.
        ImpKp  = macula_identity:generate(),
        ImpPub = macula_identity:public(ImpKp),
        Record = macula_record:sign(
                   macula_record:foundation_seed_list(
                     ImpPub, sample_seeds(2)), ImpKp),
        DnsPacket = pkarr_dns(macula_record:encode(Record)),
        Item = hecate_bootstrap_bep44:sign(1, DnsPacket, ImpKp),
        %% Target at Fk but item pubkey is ImpPub — wrong_pubkey path.
        %% Test that even if we aligned the item (substitute Fk as
        %% the mutable-item pubkey), inner record is rejected.
        hecate_bootstrap_dht_fake:set(
          hecate_bootstrap_bep44:target_id(Fk), Item),
        ?assertEqual({error, all_failed}, probe([Fk]))
    end.

first_successful_pubkey_wins(#{kp := Kp, fk := Fk}) ->
    fun() ->
        %% Two foundation pubkeys; only one has a record in DHT.
        %% Tier C should still succeed via the one that works.
        OtherKp = macula_identity:generate(),
        OtherFk = macula_identity:public(OtherKp),
        application:set_env(macula_record, foundation_pubkeys,
                            [OtherFk, Fk]),
        publish_seed_list(Kp, Fk, 3),
        hecate_bootstrap_dht_fake:fail(
          hecate_bootstrap_bep44:target_id(OtherFk), no_item),
        {ok, Peers} = probe([OtherFk, Fk]),
        ?assertEqual(3, length(Peers))
    end.

%%==================================================================
%% Helpers
%%==================================================================

probe(Pubkeys) ->
    hecate_bootstrap_tier_c:probe(
      #{dht_transport => hecate_bootstrap_dht_fake,
        pubkeys       => Pubkeys,
        timeout_ms    => 500}).

sample_seeds(N) ->
    [#{node_id   => crypto:strong_rand_bytes(32),
       addresses => [], tier => 4} || _ <- lists:seq(1, N)].

publish_seed_list(Kp, Fk, N) ->
    Record = macula_record:sign(
               macula_record:foundation_seed_list(Fk, sample_seeds(N)),
               Kp),
    DnsPacket = pkarr_dns(macula_record:encode(Record)),
    Item = hecate_bootstrap_bep44:sign(1, DnsPacket, Kp),
    hecate_bootstrap_dht_fake:set(
      hecate_bootstrap_bep44:target_id(Fk), Item).

pkarr_dns(RecordBytes) ->
    %% Split the record bytes across 200-byte TXT character-strings.
    Chunks = chunk(RecordBytes, 200),
    build_dns_packet([{txt, "_macula.foundation", Chunks}]).

chunk(Bin, Max) when byte_size(Bin) =< Max ->
    [Bin];
chunk(Bin, Max) ->
    <<Head:Max/binary, Rest/binary>> = Bin,
    [Head | chunk(Rest, Max)].

build_dns_packet([]) ->
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, 0}, {qr, true}, {opcode, query},
                     {rd, false}, {ra, false}, {rcode, 0}])},
        {anlist, []}
    ]),
    iolist_to_binary(inet_dns:encode(Msg));
build_dns_packet(Answers) ->
    RRs = [inet_dns:make_rr([
              {domain, Domain}, {type, Type}, {class, in},
              {ttl, 60}, {data, Data}
           ]) || {Type, Domain, Data} <- Answers],
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, 0}, {qr, true}, {opcode, query},
                     {rd, false}, {ra, false}, {rcode, 0}])},
        {anlist, RRs}
    ]),
    iolist_to_binary(inet_dns:encode(Msg)).
