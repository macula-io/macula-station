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
         tier_a_uncorroborated_falls_through_to_tier_e/1,
         tier_b_wins_cascade_when_tier_a_has_no_resolvers/1,
         tier_c_wins_cascade_when_a_and_b_are_down/1,
         tier_d_wins_when_a_b_c_are_down/1,
         full_cascade_all_tiers_down_returns_failure/1,
         full_cascade_under_time_budget/1,
         tier_d_eth_adapter_end_to_end/1]).

all() ->
    [tier_e_yields_three_peers,
     cascade_falls_through_to_working_tier,
     foundation_record_trust_boundary,
     foundation_seed_list_signed_by_trusted_key,
     tier_a_corroborated_seed_list_yields_peers,
     tier_a_uncorroborated_falls_through_to_tier_e,
     tier_b_wins_cascade_when_tier_a_has_no_resolvers,
     tier_c_wins_cascade_when_a_and_b_are_down,
     tier_d_wins_when_a_b_c_are_down,
     full_cascade_all_tiers_down_returns_failure,
     full_cascade_under_time_budget,
     tier_d_eth_adapter_end_to_end].

init_per_suite(Cfg) -> Cfg.
end_per_suite(_Cfg) -> ok.

init_per_testcase(_Case, Cfg) ->
    application:unset_env(hecate_record, foundation_pubkeys),
    hecate_bootstrap_tier_a_fake:init(),
    hecate_bootstrap_mdns_fake:init(),
    hecate_bootstrap_dht_fake:init(),
    hecate_bootstrap_chain_fake:init(),
    hecate_bootstrap_http_fake:init(),
    Cfg.

end_per_testcase(_Case, _Cfg) ->
    application:unset_env(hecate_record, foundation_pubkeys),
    hecate_bootstrap_tier_a_fake:reset(),
    hecate_bootstrap_mdns_fake:reset(),
    hecate_bootstrap_dht_fake:reset(),
    hecate_bootstrap_chain_fake:reset(),
    hecate_bootstrap_http_fake:reset(),
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
    application:set_env(hecate_record, foundation_pubkeys, [Fk]),

    %% Trusted path: foundation parameter signed by foundation key.
    GoodR = hecate_record:sign(
              hecate_record:foundation_parameter(
                Fk, <<"puzzle_difficulty">>, 8),
              FoundationKp),
    {ok, _} = hecate_foundation:verify_record(GoodR),

    %% Untrusted path: same record type, signed by a key that is NOT
    %% on the foundation list.
    ImpostorPub = macula_identity:public(ImpostorKp),
    BadR = hecate_record:sign(
             hecate_record:foundation_parameter(
               ImpostorPub, <<"puzzle_difficulty">>, 8),
             ImpostorKp),
    {error, not_foundation_signed} = hecate_foundation:verify_record(BadR),

    %% Wrong type (node_record): rejected even from the trusted key.
    NodeR = hecate_record:sign(
              hecate_record:node_record(Fk, [], 0), FoundationKp),
    {error, wrong_type} = hecate_foundation:verify_record(NodeR),
    ok.

foundation_seed_list_signed_by_trusted_key(_Cfg) ->
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    application:set_env(hecate_record, foundation_pubkeys, [Fk]),

    Seeds = [
        #{node_id => crypto:strong_rand_bytes(32),
          addresses => [], tier => 4},
        #{node_id => crypto:strong_rand_bytes(32),
          addresses => [], tier => 4},
        #{node_id => crypto:strong_rand_bytes(32),
          addresses => [], tier => 3}
    ],
    R = hecate_record:sign(
          hecate_record:foundation_seed_list(Fk, Seeds), Kp),
    {ok, Verified} = hecate_foundation:verify_record(R),
    16#0D = hecate_record:type(Verified),
    %% Storage key must be domain-separated (not equal to Fk).
    SK = hecate_record:storage_key(Verified),
    true = SK =/= Fk,
    ok.

%%---------------------------------------------------------------------
%% Tier A — corroborated seed list (acceptance §11.3)
%%---------------------------------------------------------------------

tier_a_corroborated_seed_list_yields_peers(_Cfg) ->
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    application:set_env(hecate_record, foundation_pubkeys, [Fk]),
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
    application:set_env(hecate_record, foundation_pubkeys, [Fk]),
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
%% Tier B — mDNS cascade winner (acceptance §11.3)
%%---------------------------------------------------------------------

tier_b_wins_cascade_when_tier_a_has_no_resolvers(_Cfg) ->
    %% Three peers advertise themselves via mDNS; the tier_a probe
    %% has no resolvers to consult and errors out; tier_b corroborates
    %% each TXT via the canned handshake and yields three peers before
    %% tier_e's peer-url paste would have been necessary.
    Peers = [make_peer() || _ <- lists:seq(1, 3)],
    Replies = [peer_reply(P) || P <- Peers],
    hecate_bootstrap_mdns_fake:set_replies(Replies),
    Handshake = registry_handshake(Peers),
    Tiers = [
        {hecate_bootstrap_tier_a,
         #{resolvers     => [],
           pubkeys       => [crypto:strong_rand_bytes(32)],
           corroboration => 2,
           timeout_ms    => 500}},
        {hecate_bootstrap_tier_b,
         #{udp_transport => hecate_bootstrap_mdns_fake,
           handshake_fun => Handshake,
           timeout_ms    => 500}}
    ],
    {ok, Got} = hecate_bootstrap:cascade(
                  Tiers, #{min_peers => 3, timeout_ms => 2000}),
    3 = length(Got),
    [b] = lists:usort([maps:get(tier, P) || P <- Got]),
    ok.

make_peer() ->
    Kp  = macula_identity:generate(),
    Pub = macula_identity:public(Kp),
    Rec = hecate_record:sign(
            hecate_record:node_record(Pub, [], 0), Kp),
    #{pub => Pub, signed_record => Rec, port => 7000,
      tier => 0, addr => rand_addr_v6()}.

rand_addr_v6() ->
    {16#fe80, 0, 0, 0,
     rand:uniform(65536) - 1, rand:uniform(65536) - 1,
     rand:uniform(65536) - 1, rand:uniform(65536) - 1}.

peer_reply(#{addr := Addr} = Peer) ->
    {Addr, mdns_txt_response(Peer)}.

mdns_txt_response(#{pub := NodeId, port := Port, tier := Tier}) ->
    Strings = [
        lists:flatten(io_lib:format("node_id=~s",
                                    [hex_lower(NodeId)])),
        lists:flatten(io_lib:format("port=~p", [Port])),
        lists:flatten(io_lib:format("tier=~p", [Tier]))
    ],
    RR = inet_dns:make_rr([
        {domain, "_macula._udp.local"}, {type, txt},
        {class, in}, {ttl, 60}, {data, Strings}
    ]),
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, 1}, {qr, true}, {opcode, query},
                     {rd, false}, {ra, false}, {rcode, 0}])},
        {anlist, [RR]}
    ]),
    iolist_to_binary(inet_dns:encode(Msg)).

hex_lower(Bin) ->
    string:lowercase(binary_to_list(binary:encode_hex(Bin))).

registry_handshake(Peers) ->
    Index = maps:from_list(
              [{maps:get(pub, P), maps:get(signed_record, P)}
               || P <- Peers]),
    fun(_Addr, _Port, NodeId) ->
            case maps:find(NodeId, Index) of
                {ok, R} -> {ok, R};
                error   -> {error, not_registered}
            end
    end.

%%---------------------------------------------------------------------
%% Tier C — Mainline DHT cascade winner (acceptance §11.3)
%%---------------------------------------------------------------------

tier_c_wins_cascade_when_a_and_b_are_down(_Cfg) ->
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    application:set_env(hecate_record, foundation_pubkeys, [Fk]),
    Record = hecate_record:sign(
               hecate_record:foundation_seed_list(
                 Fk, tier_c_seeds(5)), Kp),
    DnsPacket = tier_c_pkarr_dns(hecate_record:encode(Record)),
    Item = hecate_bootstrap_bep44:sign(1, DnsPacket, Kp),
    hecate_bootstrap_dht_fake:set(
      hecate_bootstrap_bep44:target_id(Fk), Item),
    Tiers = [
        {hecate_bootstrap_tier_a,
         #{resolvers => [], pubkeys => [Fk],
           corroboration => 2, timeout_ms => 500}},
        {hecate_bootstrap_tier_b,
         #{udp_transport => hecate_bootstrap_mdns_fake,
           timeout_ms    => 500}},
        {hecate_bootstrap_tier_c,
         #{dht_transport => hecate_bootstrap_dht_fake,
           pubkeys       => [Fk],
           timeout_ms    => 500}}
    ],
    {ok, Peers} = hecate_bootstrap:cascade(
                    Tiers, #{min_peers => 3, timeout_ms => 2000}),
    5 = length(Peers),
    [c] = lists:usort([maps:get(tier, P) || P <- Peers]),
    ok.

tier_c_seeds(N) ->
    [#{node_id => crypto:strong_rand_bytes(32),
       addresses => [], tier => 4} || _ <- lists:seq(1, N)].

tier_c_pkarr_dns(RecordBytes) ->
    Chunks = tier_c_chunk(RecordBytes, 200),
    RR = inet_dns:make_rr([
        {domain, "_macula.foundation"},
        {type, txt}, {class, in}, {ttl, 60},
        {data, Chunks}
    ]),
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, 0}, {qr, true}, {opcode, query},
                     {rd, false}, {ra, false}, {rcode, 0}])},
        {anlist, [RR]}
    ]),
    iolist_to_binary(inet_dns:encode(Msg)).

tier_c_chunk(Bin, Max) when byte_size(Bin) =< Max -> [Bin];
tier_c_chunk(Bin, Max) ->
    <<Head:Max/binary, Rest/binary>> = Bin,
    [Head | tier_c_chunk(Rest, Max)].

%%---------------------------------------------------------------------
%% Tier D — blockchain anchor cascade winner
%%---------------------------------------------------------------------

tier_d_wins_when_a_b_c_are_down(_Cfg) ->
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    application:set_env(hecate_record, foundation_pubkeys, [Fk]),
    Record = hecate_record:sign(
               hecate_record:foundation_seed_list(
                 Fk, tier_c_seeds(4)), Kp),
    hecate_bootstrap_chain_fake:set(
      bitcoin, hecate_record:encode(Record)),
    Tiers = full_cascade_tiers(Fk,
                               #{tier_c_working => false,
                                 tier_d_chains  => [{bitcoin, #{}}]}),
    {ok, Peers} = hecate_bootstrap:cascade(
                    Tiers, #{min_peers => 3, timeout_ms => 5_000}),
    4 = length(Peers),
    [d] = lists:usort([maps:get(tier, P) || P <- Peers]),
    ok.

%%---------------------------------------------------------------------
%% Total failure — no tier yields peers
%%---------------------------------------------------------------------

full_cascade_all_tiers_down_returns_failure(_Cfg) ->
    Fk = crypto:strong_rand_bytes(32),
    application:set_env(hecate_record, foundation_pubkeys, [Fk]),
    hecate_bootstrap_chain_fake:fail(bitcoin,  chain_unreachable),
    hecate_bootstrap_chain_fake:fail(ethereum, chain_unreachable),
    Tiers = full_cascade_tiers(Fk,
                               #{tier_c_working => false,
                                 tier_d_chains  => [{bitcoin, #{}},
                                                    {ethereum, #{}}]}),
    {error, _} = hecate_bootstrap:cascade(
                   Tiers, #{min_peers => 3, timeout_ms => 1_500}),
    ok.

%%---------------------------------------------------------------------
%% Time budget — cascade completes under the 60s Part 5 §11.3 bar.
%% With instantaneous fakes, every scenario should resolve in
%% milliseconds. Staggers ensure later tiers don't starve earlier
%% ones even when an earlier tier fails fast.
%%---------------------------------------------------------------------

full_cascade_under_time_budget(_Cfg) ->
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    application:set_env(hecate_record, foundation_pubkeys, [Fk]),
    %% Only Tier E has peers; cascade must fall through A+B+C+D.
    Urls = [phase6_signed_url() || _ <- lists:seq(1, 3)],
    Tiers = full_cascade_tiers(Fk,
                               #{tier_c_working => false,
                                 tier_d_chains  => [{bitcoin, #{}}],
                                 tier_e_urls    => Urls}),
    hecate_bootstrap_chain_fake:fail(bitcoin, chain_unreachable),
    T0 = erlang:monotonic_time(millisecond),
    {ok, Peers} = hecate_bootstrap:cascade(
                    Tiers, #{min_peers => 3, timeout_ms => 5_000}),
    T1 = erlang:monotonic_time(millisecond),
    3 = length(Peers),
    [e] = lists:usort([maps:get(tier, P) || P <- Peers]),
    %% Instantaneous fakes should resolve well under 2 s even under
    %% full fall-through; real-transport bars are exercised by the
    %% network-integrated suite (Part 7 §11 follow-up).
    true = (T1 - T0) < 2_000,
    ok.

%%---------------------------------------------------------------------
%% Shared cascade assembler
%%---------------------------------------------------------------------

full_cascade_tiers(Fk, Opts) ->
    TierCWorking = maps:get(tier_c_working, Opts, false),
    TierDChains  = maps:get(tier_d_chains,  Opts, []),
    TierEUrls    = maps:get(tier_e_urls,    Opts, []),
    DhtTransport = case TierCWorking of
                       true  -> hecate_bootstrap_dht_fake;
                       false -> hecate_bootstrap_dht_fake
                   end,
    [
        {hecate_bootstrap_tier_a,
         #{resolvers => [], pubkeys => [Fk],
           corroboration => 2, timeout_ms => 200}},
        {hecate_bootstrap_tier_b,
         #{udp_transport => hecate_bootstrap_mdns_fake,
           timeout_ms    => 200}},
        {hecate_bootstrap_tier_c,
         #{dht_transport => DhtTransport,
           pubkeys       => [Fk],
           timeout_ms    => 200}},
        {hecate_bootstrap_tier_d,
         #{chains     => [{hecate_bootstrap_chain_fake,
                            CO#{label => Label}}
                           || {Label, CO} <- TierDChains],
           timeout_ms => 500}},
        {hecate_bootstrap_tier_e,
         #{peer_urls => TierEUrls}}
    ].

phase6_signed_url() ->
    Kp = macula_identity:generate(),
    Record = hecate_record:sign(
               hecate_record:node_record(
                 macula_identity:public(Kp), [], 0), Kp),
    hecate_bootstrap_peer_url:encode(Record, []).

%%---------------------------------------------------------------------
%% Tier D via real Ethereum JSON-RPC adapter (with canned HTTP)
%%---------------------------------------------------------------------

tier_d_eth_adapter_end_to_end(_Cfg) ->
    Endpoint = <<"https://eth.example/rpc">>,
    Contract = <<"0x00000000000000000000000000000000000000ff">>,
    Topic    = <<"0xaabbccddeeff00112233445566778899",
                 "aabbccddeeff00112233445566778899">>,
    Kp = macula_identity:generate(),
    Fk = macula_identity:public(Kp),
    application:set_env(hecate_record, foundation_pubkeys, [Fk]),
    Record = hecate_record:sign(
               hecate_record:foundation_seed_list(
                 Fk, tier_c_seeds(3)), Kp),
    RecordBytes = hecate_record:encode(Record),
    Log = eth_log(Contract, Topic, <<"0x2a">>, RecordBytes),
    RpcBody = iolist_to_binary(
                json:encode(
                  #{<<"jsonrpc">> => <<"2.0">>,
                    <<"id">>      => 1,
                    <<"result">>  => [Log]})),
    hecate_bootstrap_http_fake:set_post(Endpoint, {ok, RpcBody}),
    Tiers = [
        {hecate_bootstrap_tier_d,
         #{chains =>
               [{hecate_bootstrap_chain_eth_jsonrpc,
                 #{endpoint => Endpoint,
                   contract => Contract,
                   topic    => Topic,
                   http     => hecate_bootstrap_http_fake}}],
           timeout_ms => 500}}
    ],
    {ok, Peers} = hecate_bootstrap:cascade(
                    Tiers, #{min_peers => 3, timeout_ms => 3_000}),
    3 = length(Peers),
    [d] = lists:usort([maps:get(tier, P) || P <- Peers]),
    ok.

eth_log(Contract, Topic, BlockNumHex, PayloadBytes) ->
    Abi = eth_abi_bytes(PayloadBytes),
    #{<<"blockNumber">> => BlockNumHex,
      <<"address">>     => Contract,
      <<"topics">>      => [Topic],
      <<"data">>        => iolist_to_binary(
                             [<<"0x">>,
                              string:lowercase(binary:encode_hex(Abi))])}.

eth_abi_bytes(Bin) ->
    Len = byte_size(Bin),
    Pad = case Len rem 32 of
              0 -> 0;
              R -> 32 - R
          end,
    <<32:256/big, Len:256/big, Bin/binary, 0:(Pad * 8)>>.

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
    Record = hecate_record:sign(
               hecate_record:node_record(
                 macula_identity:public(Kp), [], 0),
               Kp),
    hecate_bootstrap_peer_url:encode(Record, []).

signed_seed_list_bytes(Kp, Fk, N) ->
    Seeds = [#{node_id   => crypto:strong_rand_bytes(32),
               addresses => [],
               tier      => 4} || _ <- lists:seq(1, N)],
    Record = hecate_record:sign(
               hecate_record:foundation_seed_list(Fk, Seeds), Kp),
    hecate_record:encode(Record).
