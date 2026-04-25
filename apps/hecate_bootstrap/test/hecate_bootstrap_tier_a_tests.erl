-module(hecate_bootstrap_tier_a_tests).

-include_lib("eunit/include/eunit.hrl").

-define(URL_A, <<"https://cloudflare.example/dns-query">>).
-define(URL_B, <<"https://quad9.example/dns-query">>).
-define(URL_C, <<"https://mullvad.example/dns-query">>).

%%==================================================================
%% Test fixture
%%==================================================================

tier_a_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun corroboration_threshold_met/1,
      fun corroboration_threshold_unmet/1,
      fun single_hijack_outvoted/1,
      fun untrusted_signer_rejected/1,
      fun wrong_storage_key_rejected/1,
      fun no_resolvers/1,
      fun no_pubkeys/1,
      fun verified_peer_shape/1,
      fun slow_resolver_does_not_block/1
     ]}.

setup() ->
    application:ensure_all_started(crypto),
    hecate_bootstrap_tier_a_fake:init(),
    Kp     = macula_identity:generate(),
    Fk     = macula_identity:public(Kp),
    application:set_env(hecate_record, foundation_pubkeys, [Fk]),
    Seeds  = sample_seeds(3),
    Record = hecate_record:sign(
               hecate_record:foundation_seed_list(Fk, Seeds), Kp),
    Bytes  = hecate_record:encode(Record),
    #{kp     => Kp,
      fk     => Fk,
      seeds  => Seeds,
      record => Record,
      bytes  => Bytes}.

cleanup(_Ctx) ->
    application:unset_env(hecate_record, foundation_pubkeys),
    hecate_bootstrap_tier_a_fake:reset(),
    ok.

%%==================================================================
%% Cases
%%==================================================================

corroboration_threshold_met(#{fk := Fk, bytes := Bytes}) ->
    fun() ->
        ok = canned(?URL_A, Fk, {ok, Bytes}),
        ok = canned(?URL_B, Fk, {ok, Bytes}),
        ok = canned(?URL_C, Fk, {ok, Bytes}),
        {ok, Peers} = probe(default_resolvers(), [Fk], 2),
        ?assertEqual(3, length(Peers))
    end.

corroboration_threshold_unmet(#{fk := Fk, bytes := Bytes}) ->
    fun() ->
        ok = canned(?URL_A, Fk, {ok, Bytes}),
        ok = canned(?URL_B, Fk, {error, dns_failure}),
        ok = canned(?URL_C, Fk, {error, dns_failure}),
        ?assertEqual({error, no_corroboration},
                     probe(default_resolvers(), [Fk], 2))
    end.

single_hijack_outvoted(#{fk := Fk, bytes := Bytes}) ->
    fun() ->
        ImpostorBytes = impostor_bytes(Fk),
        ok = canned(?URL_A, Fk, {ok, ImpostorBytes}),
        ok = canned(?URL_B, Fk, {ok, Bytes}),
        ok = canned(?URL_C, Fk, {ok, Bytes}),
        {ok, Peers} = probe(default_resolvers(), [Fk], 2),
        ?assertEqual(3, length(Peers))
    end.

untrusted_signer_rejected(#{fk := Fk}) ->
    fun() ->
        ImpostorBytes = impostor_bytes(Fk),
        ok = canned(?URL_A, Fk, {ok, ImpostorBytes}),
        ok = canned(?URL_B, Fk, {ok, ImpostorBytes}),
        ok = canned(?URL_C, Fk, {ok, ImpostorBytes}),
        ?assertEqual({error, no_corroboration},
                     probe(default_resolvers(), [Fk], 2))
    end.

wrong_storage_key_rejected(#{kp := Kp, fk := Fk}) ->
    fun() ->
        OtherKp     = macula_identity:generate(),
        OtherFk     = macula_identity:public(OtherKp),
        application:set_env(hecate_record, foundation_pubkeys,
                            [Fk, OtherFk]),
        OtherRecord = hecate_record:sign(
                        hecate_record:foundation_seed_list(
                          OtherFk, sample_seeds(2)), OtherKp),
        OtherBytes  = hecate_record:encode(OtherRecord),
        %% Resolvers return a record whose storage key matches
        %% OtherFk, but we ask about Fk — must be rejected even
        %% though signature + signer pair are individually valid.
        ok = canned(?URL_A, Fk, {ok, OtherBytes}),
        ok = canned(?URL_B, Fk, {ok, OtherBytes}),
        ok = canned(?URL_C, Fk, {ok, OtherBytes}),
        %% Re-sign with the original Kp so signer trust passes; only
        %% the storage-key check should fail.
        Resigned = hecate_record:sign(
                     hecate_record:foundation_seed_list(
                       OtherFk, sample_seeds(2)), Kp),
        ResignedBytes = hecate_record:encode(Resigned),
        ok = canned(?URL_A, Fk, {ok, ResignedBytes}),
        ok = canned(?URL_B, Fk, {ok, ResignedBytes}),
        ok = canned(?URL_C, Fk, {ok, ResignedBytes}),
        ?assertEqual({error, no_corroboration},
                     probe(default_resolvers(), [Fk], 2))
    end.

no_resolvers(#{fk := Fk}) ->
    fun() ->
        ?assertEqual({error, no_resolvers},
                     probe([], [Fk], 2))
    end.

no_pubkeys(_Ctx) ->
    fun() ->
        ?assertEqual({error, no_pubkeys},
                     probe(default_resolvers(), [], 2))
    end.

verified_peer_shape(#{fk := Fk, seeds := Seeds, record := Record,
                      bytes := Bytes}) ->
    fun() ->
        ok = canned(?URL_A, Fk, {ok, Bytes}),
        ok = canned(?URL_B, Fk, {ok, Bytes}),
        {ok, [P | _]} = probe(default_resolvers(), [Fk], 2),
        #{node_id := NodeId, record := PeerRecord,
          addresses := Addrs, tier := Tier, via := Via} = P,
        ?assertEqual(a, Tier),
        ?assertEqual(hecate_bootstrap_tier_a, Via),
        ?assertEqual(Record, PeerRecord),
        ?assert(is_list(Addrs)),
        ExpectedIds = [maps:get(node_id, S) || S <- Seeds],
        ?assert(lists:member(NodeId, ExpectedIds))
    end.

slow_resolver_does_not_block(#{fk := Fk, bytes := Bytes}) ->
    fun() ->
        ok = canned(?URL_A, Fk, {sleep, 5_000, {ok, Bytes}}),
        ok = canned(?URL_B, Fk, {ok, Bytes}),
        ok = canned(?URL_C, Fk, {ok, Bytes}),
        T0 = erlang:monotonic_time(millisecond),
        {ok, Peers} = probe(slow_aware_resolvers(),
                            [Fk], 2, 200),
        T1 = erlang:monotonic_time(millisecond),
        ?assertEqual(3, length(Peers)),
        ?assert((T1 - T0) < 1_000)
    end.

%%==================================================================
%% Helpers
%%==================================================================

default_resolvers() ->
    [{hecate_bootstrap_tier_a_fake, ?URL_A},
     {hecate_bootstrap_tier_a_fake, ?URL_B},
     {hecate_bootstrap_tier_a_fake, ?URL_C}].

slow_aware_resolvers() -> default_resolvers().

probe(Resolvers, Pubkeys, Threshold) ->
    probe(Resolvers, Pubkeys, Threshold, 1_000).

probe(Resolvers, Pubkeys, Threshold, TimeoutMs) ->
    hecate_bootstrap_tier_a:probe(
      #{resolvers     => Resolvers,
        pubkeys       => Pubkeys,
        corroboration => Threshold,
        timeout_ms    => TimeoutMs}).

canned(Url, Pubkey, Reply) ->
    hecate_bootstrap_tier_a_fake:set(Url, Pubkey, Reply).

sample_seeds(N) ->
    [#{node_id   => crypto:strong_rand_bytes(32),
       addresses => [#{ {text, <<"ip">>}   => {text, <<"::1">>},
                        {text, <<"port">>} => 7000 }],
       tier      => 4} || _ <- lists:seq(1, N)].

%% Build a foundation_seed_list signed by an untrusted key — same
%% wire format and target pubkey, but signature won't validate
%% against the trusted foundation list.
impostor_bytes(Fk) ->
    ImpostorKp = macula_identity:generate(),
    Record = hecate_record:sign(
               hecate_record:foundation_seed_list(Fk, sample_seeds(2)),
               ImpostorKp),
    hecate_record:encode(Record).
