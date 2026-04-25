-module(hecate_bootstrap_tier_d_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BTC, bitcoin).
-define(ETH, ethereum).

%%==================================================================
%% Fixture
%%==================================================================

tier_d_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun happy_single_chain/1,
      fun no_chains_configured/1,
      fun all_chains_fail/1,
      fun first_successful_chain_wins/1,
      fun slow_chain_does_not_block_fast_one/1,
      fun untrusted_signer_rejected/1,
      fun garbage_bytes_rejected/1,
      fun expired_record_rejected/1
     ]}.

setup() ->
    application:ensure_all_started(crypto),
    hecate_bootstrap_chain_fake:init(),
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    application:set_env(hecate_record, foundation_pubkeys, [Fk]),
    #{kp => Kp, fk => Fk}.

cleanup(_Ctx) ->
    application:unset_env(hecate_record, foundation_pubkeys),
    hecate_bootstrap_chain_fake:reset(),
    ok.

%%==================================================================
%% Cases
%%==================================================================

happy_single_chain(#{kp := Kp, fk := Fk}) ->
    fun() ->
        Bytes = foundation_bytes(Kp, Fk, 4),
        hecate_bootstrap_chain_fake:set(?BTC, Bytes),
        {ok, Peers} = probe([{?BTC, #{}}]),
        ?assertEqual(4, length(Peers)),
        [d] = lists:usort([maps:get(tier, P) || P <- Peers])
    end.

no_chains_configured(_Ctx) ->
    fun() ->
        ?assertEqual({error, no_chains}, probe([]))
    end.

all_chains_fail(_Ctx) ->
    fun() ->
        hecate_bootstrap_chain_fake:fail(?BTC, chain_unreachable),
        hecate_bootstrap_chain_fake:fail(?ETH, rate_limited),
        ?assertEqual({error, all_failed},
                     probe([{?BTC, #{}}, {?ETH, #{}}]))
    end.

first_successful_chain_wins(#{kp := Kp, fk := Fk}) ->
    fun() ->
        Bytes = foundation_bytes(Kp, Fk, 2),
        hecate_bootstrap_chain_fake:fail(?BTC, chain_unreachable),
        hecate_bootstrap_chain_fake:set(?ETH, Bytes),
        {ok, Peers} = probe([{?BTC, #{}}, {?ETH, #{}}]),
        ?assertEqual(2, length(Peers))
    end.

slow_chain_does_not_block_fast_one(#{kp := Kp, fk := Fk}) ->
    fun() ->
        Bytes = foundation_bytes(Kp, Fk, 1),
        hecate_bootstrap_chain_fake:set(?BTC, Bytes),
        hecate_bootstrap_chain_fake:set(?ETH, Bytes),
        T0 = erlang:monotonic_time(millisecond),
        {ok, _} = probe([{?BTC, #{delay_ms => 0}},
                         {?ETH, #{delay_ms => 5_000}}],
                        1_000),
        T1 = erlang:monotonic_time(millisecond),
        ?assert((T1 - T0) < 1_000)
    end.

untrusted_signer_rejected(#{fk := Fk}) ->
    fun() ->
        ImpKp = macula_identity:generate(),
        Record = hecate_record:sign(
                   hecate_record:foundation_seed_list(
                     Fk, sample_seeds(3)),
                   ImpKp),
        hecate_bootstrap_chain_fake:set(?BTC,
                                        hecate_record:encode(Record)),
        ?assertEqual({error, all_failed},
                     probe([{?BTC, #{}}]))
    end.

garbage_bytes_rejected(_Ctx) ->
    fun() ->
        hecate_bootstrap_chain_fake:set(?BTC, <<"not a macula record">>),
        ?assertEqual({error, all_failed},
                     probe([{?BTC, #{}}]))
    end.

expired_record_rejected(#{kp := Kp, fk := Fk}) ->
    fun() ->
        Record = hecate_record:sign(
                   hecate_record:foundation_seed_list(
                     Fk, sample_seeds(2), #{ttl_ms => 1}),
                   Kp),
        timer:sleep(5),
        hecate_bootstrap_chain_fake:set(?BTC,
                                        hecate_record:encode(Record)),
        ?assertEqual({error, all_failed},
                     probe([{?BTC, #{}}]))
    end.

%%==================================================================
%% Helpers
%%==================================================================

probe(Chains) ->
    probe(Chains, 500).

probe(Chains, TimeoutMs) ->
    hecate_bootstrap_tier_d:probe(
      #{chains     => [{hecate_bootstrap_chain_fake, ChainOpts#{label => L}}
                        || {L, ChainOpts} <- Chains],
        timeout_ms => TimeoutMs}).

sample_seeds(N) ->
    [#{node_id   => crypto:strong_rand_bytes(32),
       addresses => [], tier => 4} || _ <- lists:seq(1, N)].

foundation_bytes(Kp, Fk, N) ->
    Record = hecate_record:sign(
               hecate_record:foundation_seed_list(Fk, sample_seeds(N)),
               Kp),
    hecate_record:encode(Record).
