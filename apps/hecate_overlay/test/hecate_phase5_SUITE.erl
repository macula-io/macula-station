%% @doc Phase 5 acceptance tests (intra-realm overlay).
%%
%% Covers the §10.3 acceptance bars that can be exercised
%% deterministically in-VM without kicking off a real network:
%% <ul>
%%   <li>Realm-join admits a new member after endorsement verifies
%%       and rejects a joiner with a bogus endorsement.</li>
%%   <li>Plumtree delivers a published EVENT to every subscriber
%%       in a connected mesh.</li>
%%   <li>Cross-realm isolation — an event published in realm R1
%%       never surfaces in a station whose membership is realm R2.</li>
%% </ul>
%%
%% Larger-scale acceptance (20-station convergence, sub-second
%% repair) is deferred to the network-integrated suite.
-module(hecate_phase5_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([realm_join_admits_new_member/1,
         realm_join_rejects_bogus_endorsement/1,
         plumtree_delivers_to_all_subscribers/1,
         cross_realm_isolation/1]).

all() ->
    [realm_join_admits_new_member,
     realm_join_rejects_bogus_endorsement,
     plumtree_delivers_to_all_subscribers,
     cross_realm_isolation].

init_per_suite(Cfg) -> Cfg.
end_per_suite(_Cfg) -> ok.

%%---------------------------------------------------------------------
%% Realm-join admission
%%---------------------------------------------------------------------

realm_join_admits_new_member(_Cfg) ->
    AdminKp = macula_identity:generate(),
    Realm   = macula_identity:public(AdminKp),
    Net = phase5_helper:start_fleet([seed, joiner], [Realm], #{}),
    try
        End = phase5_helper:endorse(Net, AdminKp, Realm, joiner),
        phase5_helper:join(Net, joiner, seed, Realm, End),
        Active = phase5_helper:active_view(Net, seed, Realm),
        JoinerPub = pubkey_of(Net, joiner),
        true = lists:member(JoinerPub, Active)
    after
        phase5_helper:stop_fleet(Net)
    end.

realm_join_rejects_bogus_endorsement(_Cfg) ->
    RealAdmin = macula_identity:generate(),
    Realm     = macula_identity:public(RealAdmin),
    Impostor  = macula_identity:generate(),
    Net = phase5_helper:start_fleet([seed, joiner], [Realm], #{}),
    try
        %% Build endorsement but sign with the impostor — verify must fail.
        RealEnd = phase5_helper:endorse(Net, RealAdmin, Realm, joiner),
        JoinerPub = pubkey_of(Net, joiner),
        Bogus0 = hecate_record:realm_member_endorsement(
                   Realm,
                   #{realm => Realm, member_node => JoinerPub,
                     roles => [<<"peer">>]}),
        Bogus = hecate_record:sign(Bogus0, Impostor),
        phase5_helper:join(Net, joiner, seed, Realm, Bogus),
        %% Seed must NOT admit the bogus joiner.
        Active = phase5_helper:active_view(Net, seed, Realm),
        false = lists:member(JoinerPub, Active),
        %% Sanity: the real endorsement still works on a different seed?
        %% Use the same seed; after the prior bogus attempt, a valid
        %% follow-up admits the joiner.
        phase5_helper:join(Net, joiner, seed, Realm, RealEnd),
        Active2 = phase5_helper:active_view(Net, seed, Realm),
        true  = lists:member(JoinerPub, Active2)
    after
        phase5_helper:stop_fleet(Net)
    end.

%%---------------------------------------------------------------------
%% Plumtree + PubSub end-to-end delivery
%%---------------------------------------------------------------------

plumtree_delivers_to_all_subscribers(_Cfg) ->
    AdminKp = macula_identity:generate(),
    Realm   = macula_identity:public(AdminKp),
    Net = phase5_helper:start_fleet([a, b, c, d, e], [Realm], #{}),
    try
        %% Chain topology: a — b — c — d — e. Every message must
        %% traverse intermediate hops.
        ok = phase5_helper:connect(Net, a, b, Realm),
        ok = phase5_helper:connect(Net, b, c, Realm),
        ok = phase5_helper:connect(Net, c, d, Realm),
        ok = phase5_helper:connect(Net, d, e, Realm),

        [ok = phase5_helper:subscribe(Net, N, Realm, <<"chat">>)
         || N <- [a, b, c, d, e]],

        ok = phase5_helper:publish(Net, a, Realm, <<"chat">>, <<"hello">>),
        timer:sleep(100),

        %% Each station must see exactly one delivery of the event.
        [begin
             Payloads = phase5_helper:deliveries(Net, N, {Realm, <<"chat">>}),
             1 = length(Payloads),
             [<<"hello">>] = Payloads
         end || N <- [a, b, c, d, e]]
    after
        phase5_helper:stop_fleet(Net)
    end.

%%---------------------------------------------------------------------
%% Cross-realm isolation
%%---------------------------------------------------------------------

cross_realm_isolation(_Cfg) ->
    AdminR1 = macula_identity:generate(), R1 = macula_identity:public(AdminR1),
    AdminR2 = macula_identity:generate(), R2 = macula_identity:public(AdminR2),
    %% Both realms share the same fleet identities; wiring is per-realm.
    Net = phase5_helper:start_fleet([a, b, c], [R1, R2], #{}),
    try
        %% In R1: a—b connected. In R2: b—c connected.
        ok = phase5_helper:connect(Net, a, b, R1),
        ok = phase5_helper:connect(Net, b, c, R2),

        ok = phase5_helper:subscribe(Net, a, R1, <<"feed">>),
        ok = phase5_helper:subscribe(Net, b, R1, <<"feed">>),
        ok = phase5_helper:subscribe(Net, b, R2, <<"feed">>),
        ok = phase5_helper:subscribe(Net, c, R2, <<"feed">>),

        ok = phase5_helper:publish(Net, a, R1, <<"feed">>, <<"r1-only">>),
        timer:sleep(100),

        %% Delivered on R1 subscribers in R1:
        1 = length(phase5_helper:deliveries(Net, a, {R1, <<"feed">>})),
        1 = length(phase5_helper:deliveries(Net, b, {R1, <<"feed">>})),
        %% NOT delivered on R2 subscribers — not even b (same station,
        %% different realm) — and definitely not c (other realm).
        0 = length(phase5_helper:deliveries(Net, b, {R2, <<"feed">>})),
        0 = length(phase5_helper:deliveries(Net, c, {R2, <<"feed">>}))
    after
        phase5_helper:stop_fleet(Net)
    end.

%%=====================================================================
%% Helpers
%%=====================================================================

pubkey_of(Net, Name) -> phase5_helper:pubkey_of(Net, Name).
