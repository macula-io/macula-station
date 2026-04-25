%% EUnit tests for hecate_realm_join.
-module(hecate_realm_join_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Verify endorsement
%%---------------------------------------------------------------------

accepts_valid_endorsement_test() ->
    {AdminKp, RealmId} = admin(),
    Member = crypto:strong_rand_bytes(32),
    R = signed_endorsement(AdminKp, RealmId, Member, [<<"peer">>]),
    ?assertEqual({ok, [<<"peer">>]},
                 hecate_realm_join:verify_endorsement(R, RealmId, Member)).

accepts_multiple_roles_test() ->
    {AdminKp, RealmId} = admin(),
    Member = crypto:strong_rand_bytes(32),
    R = signed_endorsement(AdminKp, RealmId, Member,
                           [<<"peer">>, <<"directory">>, <<"replica">>]),
    ?assertEqual({ok, [<<"peer">>, <<"directory">>, <<"replica">>]},
                 hecate_realm_join:verify_endorsement(R, RealmId, Member)).

rejects_unsigned_endorsement_test() ->
    {_AdminKp, RealmId} = admin(),
    Member = crypto:strong_rand_bytes(32),
    Unsigned = hecate_record:realm_member_endorsement(
                 RealmId,
                 #{realm => RealmId, member_node => Member, roles => []}),
    %% hecate_record:verify returns {error, bad_record} for unsigned.
    ?assertEqual({error, bad_record},
                 hecate_realm_join:verify_endorsement(Unsigned, RealmId, Member)).

rejects_wrong_signer_test() ->
    {_AdminKp, RealmId} = admin(),
    Impostor = hecate_identity:generate(),
    Member = crypto:strong_rand_bytes(32),
    R = hecate_record:realm_member_endorsement(
          RealmId,
          #{realm => RealmId, member_node => Member, roles => [<<"peer">>]}),
    %% Impostor signs — signature verifies against payload's realm key,
    %% which is the admin key, so the signature check must fail.
    Bad = hecate_record:sign(R, Impostor),
    ?assertEqual({error, signature_invalid},
                 hecate_realm_join:verify_endorsement(Bad, RealmId, Member)).

rejects_wrong_type_test() ->
    {AdminKp, RealmId} = admin(),
    %% realm_directory is a different type (0x03).
    R = hecate_record:sign(hecate_record:realm_directory(
                             RealmId, <<"demo">>, RealmId), AdminKp),
    Member = crypto:strong_rand_bytes(32),
    ?assertEqual({error, wrong_type},
                 hecate_realm_join:verify_endorsement(R, RealmId, Member)).

rejects_wrong_member_test() ->
    {AdminKp, RealmId} = admin(),
    Real  = crypto:strong_rand_bytes(32),
    Fake  = crypto:strong_rand_bytes(32),
    R = signed_endorsement(AdminKp, RealmId, Real, [<<"peer">>]),
    ?assertEqual({error, wrong_member},
                 hecate_realm_join:verify_endorsement(R, RealmId, Fake)).

rejects_wrong_realm_test() ->
    {AdminKp, RealmId} = admin(),
    {_, OtherRealm}    = admin(),
    Member = crypto:strong_rand_bytes(32),
    R = signed_endorsement(AdminKp, RealmId, Member, []),
    ?assertEqual({error, wrong_realm},
                 hecate_realm_join:verify_endorsement(R, OtherRealm, Member)).

rejects_not_yet_valid_test() ->
    {AdminKp, RealmId} = admin(),
    Member = crypto:strong_rand_bytes(32),
    Future = erlang:system_time(millisecond) + 60 * 1000,
    R0 = hecate_record:realm_member_endorsement(
           RealmId,
           #{realm => RealmId, member_node => Member, roles => []},
           #{valid_from => Future, valid_until => Future + 1000}),
    R = hecate_record:sign(R0, AdminKp),
    ?assertEqual({error, not_yet_valid},
                 hecate_realm_join:verify_endorsement(R, RealmId, Member)).

rejects_endorsement_expired_test() ->
    {AdminKp, RealmId} = admin(),
    Member = crypto:strong_rand_bytes(32),
    NowMs = erlang:system_time(millisecond),
    %% valid_until is in the past; but record envelope itself must
    %% not be expired — give envelope a fresh 5min ttl via opts.
    R0 = hecate_record:realm_member_endorsement(
           RealmId,
           #{realm => RealmId, member_node => Member, roles => []},
           #{valid_from  => NowMs - 10_000,
             valid_until => NowMs - 5_000,
             ttl_ms      => 5 * 60 * 1000}),
    R = hecate_record:sign(R0, AdminKp),
    ?assertEqual({error, endorsement_expired},
                 hecate_realm_join:verify_endorsement(R, RealmId, Member)).

%%---------------------------------------------------------------------
%% Build join frame
%%---------------------------------------------------------------------

build_join_frame_is_signed_hyparview_join_test() ->
    {AdminKp, RealmId} = admin(),
    MemberKp = hecate_identity:generate(),
    Member   = hecate_identity:public(MemberKp),
    Endorsement = signed_endorsement(AdminKp, RealmId, Member, [<<"peer">>]),
    F = hecate_realm_join:build_join(RealmId, Member, Endorsement, MemberKp),
    ?assertEqual(hyparview_join, hecate_frame:frame_type(F)),
    ?assertEqual(RealmId, maps:get(realm, F)),
    ?assertEqual(Member,  maps:get(new_member, F)),
    %% Signed by the joining member → verifies against Member pubkey.
    ?assertMatch({ok, _}, hecate_frame:verify(F, Member)).

%%=====================================================================
%% Helpers
%%=====================================================================

admin() ->
    Kp = hecate_identity:generate(),
    {Kp, hecate_identity:public(Kp)}.

signed_endorsement(AdminKp, RealmId, Member, Roles) ->
    R = hecate_record:realm_member_endorsement(
          RealmId,
          #{realm => RealmId, member_node => Member, roles => Roles}),
    hecate_record:sign(R, AdminKp).
