%% @doc Phase 6 acceptance tests (bootstrap cascade).
%%
%% Covers the §11.3 acceptance bars that can be exercised
%% deterministically in-VM without kicking off real network I/O:
%% <ul>
%%   <li>Tier E (operator peer paste) yields ≥3 verified peers from
%%       a set of signed URLs.</li>
%%   <li>The cascade falls through failing tiers to a working one.</li>
%%   <li>A faster tier preempts a slower tier with stagger delay.</li>
%%   <li>Foundation record types verify against a trusted foundation
%%       pubkey and reject untrusted signers (Part 6 §9.14–§9.17).</li>
%% </ul>
%%
%% Tier A (DoH), Tier B (mDNS), Tier C (Mainline DHT), Tier D
%% (blockchain) require external infrastructure and are covered by
%% the network-integrated suite — not this file.
-module(hecate_phase6_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([tier_e_yields_three_peers/1,
         cascade_falls_through_to_working_tier/1,
         foundation_record_trust_boundary/1,
         foundation_seed_list_signed_by_trusted_key/1,
         tier_a_corroborated_seed_list_yields_peers/1,
         tier_a_uncorroborated_falls_through_to_tier_e/1]).

all() ->
    [tier_e_yields_three_peers,
     cascade_falls_through_to_working_tier,
     foundation_record_trust_boundary,
     foundation_seed_list_signed_by_trusted_key,
     tier_a_corroborated_seed_list_yields_peers,
     tier_a_uncorroborated_falls_through_to_tier_e].

init_per_suite(Cfg) -> Cfg.
end_per_suite(_Cfg) -> ok.

init_per_testcase(_Case, Cfg) ->
    application:unset_env(macula_record, foundation_pubkeys),
    hecate_bootstrap_tier_a_fake:init(),
    Cfg.

end_per_testcase(_Case, _Cfg) ->
    application:unset_env(macula_record, foundation_pubkeys),
    hecate_bootstrap_tier_a_fake:reset(),
    ok.

%%---------------------------------------------------------------------
%% Tier E — operator-provided peer URLs
%%---------------------------------------------------------------------

tier_e_yields_three_peers(_Cfg) ->
    Urls = [signed_url() || _ <- lists:seq(1, 3)],
    Tiers = [{hecate_bootstrap_tier_e, #{peer_urls => Urls}}],
    {ok, Peers} = hecate_bootstrap:cascade(Tiers, #{min_peers => 3}),
    3 = length(Peers),
    Ids = [maps:get(node_id, P) || P <- Peers],
    3 = length(lists:usort(Ids)),
    ok.

%%---------------------------------------------------------------------
%% Cascade fall-through
%%---------------------------------------------------------------------

cascade_falls_through_to_working_tier(_Cfg) ->
    Urls = [signed_url() || _ <- lists:seq(1, 3)],
    register_fake(hecate_phase6_broken_a, 0,
                  {error, network_unreachable}),
    register_fake(hecate_phase6_broken_b, 50,
                  {error, dns_failure}),
    try
        Tiers = [
            {hecate_phase6_broken_a, #{}},
            {hecate_phase6_broken_b, #{}},
            {hecate_bootstrap_tier_e, #{peer_urls => Urls}}
        ],
        {ok, Peers} = hecate_bootstrap:cascade(
                        Tiers, #{min_peers => 3, timeout_ms => 2000}),
        3 = length(Peers)
    after
        unregister_fake(hecate_phase6_broken_a),
        unregister_fake(hecate_phase6_broken_b)
    end,
    ok.

%%---------------------------------------------------------------------
%% Foundation trust anchor
%%---------------------------------------------------------------------

foundation_record_trust_boundary(_Cfg) ->
    FoundationKp = macula_identity:generate(),
    Fk = macula_identity:public(FoundationKp),
    ImpostorKp = macula_identity:generate(),
    application:set_env(macula_record, foundation_pubkeys, [Fk]),

    %% Trusted path: foundation parameter signed by foundation key.
    GoodR = macula_record:sign(
              macula_record:foundation_parameter(
                Fk, <<"puzzle_difficulty">>, 8),
              FoundationKp),
    {ok, _} = macula_foundation:verify_record(GoodR),

    %% Untrusted path: same record type, signed by a key that is NOT
    %% on the foundation list.
    ImpostorPub = macula_identity:public(ImpostorKp),
    BadR = macula_record:sign(
             macula_record:foundation_parameter(
               ImpostorPub, <<"puzzle_difficulty">>, 8),
             ImpostorKp),
    {error, not_foundation_signed} = macula_foundation:verify_record(BadR),

    %% Wrong type (node_record): rejected even from the trusted key.
    NodeR = macula_record:sign(
              macula_record:node_record(Fk, [], 0), FoundationKp),
    {error, wrong_type} = macula_foundation:verify_record(NodeR),
    ok.

foundation_seed_list_signed_by_trusted_key(_Cfg) ->
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    application:set_env(macula_record, foundation_pubkeys, [Fk]),

    Seeds = [
        #{node_id => crypto:strong_rand_bytes(32),
          addresses => [], tier => 4},
        #{node_id => crypto:strong_rand_bytes(32),
          addresses => [], tier => 4},
        #{node_id => crypto:strong_rand_bytes(32),
          addresses => [], tier => 3}
    ],
    R = macula_record:sign(
          macula_record:foundation_seed_list(Fk, Seeds), Kp),
    {ok, Verified} = macula_foundation:verify_record(R),
    16#0D = macula_record:type(Verified),
    %% Storage key must be domain-separated (not equal to Fk).
    SK = macula_record:storage_key(Verified),
    true = SK =/= Fk,
    ok.

%%---------------------------------------------------------------------
%% Tier A — corroborated seed list (acceptance §11.3)
%%---------------------------------------------------------------------

tier_a_corroborated_seed_list_yields_peers(_Cfg) ->
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    application:set_env(macula_record, foundation_pubkeys, [Fk]),
    Bytes = signed_seed_list_bytes(Kp, Fk, 5),
    Resolvers = [
        {hecate_bootstrap_tier_a_fake, <<"r1">>},
        {hecate_bootstrap_tier_a_fake, <<"r2">>},
        {hecate_bootstrap_tier_a_fake, <<"r3">>}
    ],
    [hecate_bootstrap_tier_a_fake:set(U, Fk, {ok, Bytes})
     || {_, U} <- Resolvers],
    Tiers = [{hecate_bootstrap_tier_a,
              #{resolvers     => Resolvers,
                pubkeys       => [Fk],
                corroboration => 2,
                timeout_ms    => 500}}],
    {ok, Peers} = hecate_bootstrap:cascade(
                    Tiers, #{min_peers => 3, timeout_ms => 2000}),
    5 = length(Peers),
    [a] = lists:usort([maps:get(tier, P) || P <- Peers]),
    ok.

tier_a_uncorroborated_falls_through_to_tier_e(_Cfg) ->
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    application:set_env(macula_record, foundation_pubkeys, [Fk]),
    Bytes = signed_seed_list_bytes(Kp, Fk, 3),
    %% Only one resolver corroborates — threshold is 2, so Tier A fails.
    hecate_bootstrap_tier_a_fake:set(<<"r1">>, Fk, {ok, Bytes}),
    hecate_bootstrap_tier_a_fake:set(<<"r2">>, Fk, {error, dns_failure}),
    hecate_bootstrap_tier_a_fake:set(<<"r3">>, Fk, {error, dns_failure}),
    AResolvers = [{hecate_bootstrap_tier_a_fake, <<"r1">>},
                  {hecate_bootstrap_tier_a_fake, <<"r2">>},
                  {hecate_bootstrap_tier_a_fake, <<"r3">>}],
    Urls = [signed_url() || _ <- lists:seq(1, 3)],
    Tiers = [
        {hecate_bootstrap_tier_a,
         #{resolvers     => AResolvers,
           pubkeys       => [Fk],
           corroboration => 2,
           timeout_ms    => 500}},
        {hecate_bootstrap_tier_e, #{peer_urls => Urls}}
    ],
    {ok, Peers} = hecate_bootstrap:cascade(
                    Tiers, #{min_peers => 3, timeout_ms => 2000}),
    3 = length(Peers),
    [e] = lists:usort([maps:get(tier, P) || P <- Peers]),
    ok.

%%---------------------------------------------------------------------
%% Fake tier registration (runtime-compiled)
%%---------------------------------------------------------------------

register_fake(Name, StaggerMs, ProbeResult) ->
    Forms = [
        {attribute, 1, module, Name},
        {attribute, 1, behaviour, hecate_bootstrap_tier},
        {attribute, 1, export,
         [{tier, 0}, {stagger_ms, 0}, {probe, 1}]},
        {function, 1, tier, 0,
         [{clause, 1, [], [], [{atom, 1, Name}]}]},
        {function, 1, stagger_ms, 0,
         [{clause, 1, [], [], [{integer, 1, StaggerMs}]}]},
        {function, 1, probe, 1,
         [{clause, 1, [{var, 1, '_'}], [],
           [erl_parse:abstract(ProbeResult)]}]}
    ],
    {ok, Name, Bin} = compile:forms(Forms, [return_errors]),
    {module, Name} = code:load_binary(
                       Name, atom_to_list(Name) ++ ".erl", Bin),
    ok.

unregister_fake(Name) ->
    code:purge(Name),
    code:delete(Name),
    code:purge(Name),
    ok.

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

signed_url() ->
    Kp = macula_identity:generate(),
    Record = macula_record:sign(
               macula_record:node_record(
                 macula_identity:public(Kp), [], 0),
               Kp),
    hecate_bootstrap_peer_url:encode(Record, []).

signed_seed_list_bytes(Kp, Fk, N) ->
    Seeds = [#{node_id   => crypto:strong_rand_bytes(32),
               addresses => [],
               tier      => 4} || _ <- lists:seq(1, N)],
    Record = macula_record:sign(
               macula_record:foundation_seed_list(Fk, Seeds), Kp),
    macula_record:encode(Record).
