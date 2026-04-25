%% EUnit tests for hecate_identity.
-module(hecate_identity_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

%%------------------------------------------------------------------
%% Generation
%%------------------------------------------------------------------

generate_returns_32byte_keys_test() ->
    Kp = hecate_identity:generate(),
    ?assertMatch(#{public := _, private := _}, Kp),
    ?assertEqual(32, byte_size(hecate_identity:public(Kp))),
    ?assertEqual(32, byte_size(hecate_identity:private(Kp))).

generated_keys_are_random_test() ->
    ?assertNotEqual(hecate_identity:generate(), hecate_identity:generate()).

%%------------------------------------------------------------------
%% Sign / verify
%%------------------------------------------------------------------

sign_produces_64_byte_signature_test() ->
    Kp = hecate_identity:generate(),
    Sig = hecate_identity:sign(<<"msg">>, Kp),
    ?assertEqual(64, byte_size(Sig)).

sign_verify_roundtrip_test() ->
    Kp = hecate_identity:generate(),
    Msg = <<"hello macula">>,
    Sig = hecate_identity:sign(Msg, Kp),
    ?assert(hecate_identity:verify(Msg, Sig, hecate_identity:public(Kp))).

sign_verify_accepts_iolist_message_test() ->
    Kp = hecate_identity:generate(),
    Iolist = [<<"hello">>, $\s, <<"macula">>],
    Flat   = iolist_to_binary(Iolist),
    Sig    = hecate_identity:sign(Iolist, Kp),
    ?assert(hecate_identity:verify(Flat, Sig, hecate_identity:public(Kp))).

verify_rejects_tampered_message_test() ->
    Kp = hecate_identity:generate(),
    Sig = hecate_identity:sign(<<"original">>, Kp),
    ?assertNot(hecate_identity:verify(<<"tampered">>, Sig,
                                      hecate_identity:public(Kp))).

verify_rejects_wrong_pubkey_test() ->
    Kp1 = hecate_identity:generate(),
    Kp2 = hecate_identity:generate(),
    Msg = <<"signed-by-1">>,
    Sig = hecate_identity:sign(Msg, Kp1),
    ?assertNot(hecate_identity:verify(Msg, Sig, hecate_identity:public(Kp2))).

sign_accepts_raw_private_key_test() ->
    Kp = hecate_identity:generate(),
    Priv = hecate_identity:private(Kp),
    Sig  = hecate_identity:sign(<<"m">>, Priv),
    ?assert(hecate_identity:verify(<<"m">>, Sig, hecate_identity:public(Kp))).

%%------------------------------------------------------------------
%% NodeId
%%------------------------------------------------------------------

node_id_of_key_pair_is_public_key_test() ->
    Kp = hecate_identity:generate(),
    ?assertEqual(hecate_identity:public(Kp), hecate_identity:node_id(Kp)).

node_id_of_pubkey_is_identity_test() ->
    Pub = crypto:strong_rand_bytes(32),
    ?assertEqual(Pub, hecate_identity:node_id(Pub)).

%%------------------------------------------------------------------
%% Puzzle
%%------------------------------------------------------------------

puzzle_evidence_is_sha256_of_pubkey_test() ->
    Kp   = hecate_identity:generate(),
    Pub  = hecate_identity:public(Kp),
    Want = crypto:hash(sha256, Pub),
    ?assertEqual(Want, hecate_identity:puzzle_evidence(Kp)),
    ?assertEqual(Want, hecate_identity:puzzle_evidence(Pub)).

puzzle_difficulty_zero_always_valid_test() ->
    ?assert(hecate_identity:puzzle_valid(hecate_identity:generate(), 0)).

puzzle_validity_is_deterministic_test() ->
    Kp = hecate_identity:generate(),
    V1 = hecate_identity:puzzle_valid(Kp, 4),
    V2 = hecate_identity:puzzle_valid(Kp, 4),
    ?assertEqual(V1, V2).

grind_produces_valid_puzzle_test_() ->
    %% Difficulty 10 means ~1024 attempts expected; comfortably within 30s.
    {timeout, 30,
     fun() ->
         Kp = hecate_identity:generate(#{puzzle => true, difficulty => 10}),
         ?assert(hecate_identity:puzzle_valid(Kp, 10))
     end}.

puzzle_higher_difficulty_implies_lower_difficulty_test_() ->
    {timeout, 30,
     fun() ->
         Kp = hecate_identity:generate(#{puzzle => true, difficulty => 10}),
         ?assert(hecate_identity:puzzle_valid(Kp,  0)),
         ?assert(hecate_identity:puzzle_valid(Kp,  5)),
         ?assert(hecate_identity:puzzle_valid(Kp, 10))
     end}.

%%------------------------------------------------------------------
%% Persistence
%%------------------------------------------------------------------

save_load_roundtrip_test() ->
    Path = mktmp("identity.key"),
    Kp   = hecate_identity:generate(),
    ok = hecate_identity:save(Path, Kp),
    ?assertEqual({ok, Kp}, hecate_identity:load(Path)).

load_rejects_bad_format_test() ->
    Path = mktmp("bad.key"),
    ok = file:write_file(Path, <<"not a valid key">>),
    ?assertEqual({error, bad_key_file}, hecate_identity:load(Path)).

load_returns_enoent_for_missing_file_test() ->
    ?assertEqual({error, enoent}, hecate_identity:load("/nonexistent/xyz/key")).

saved_file_has_restrictive_permissions_test() ->
    Path = mktmp("identity.key"),
    Kp   = hecate_identity:generate(),
    ok = hecate_identity:save(Path, Kp),
    {ok, #file_info{mode = Mode}} = file:read_file_info(Path),
    ?assertEqual(8#0600, Mode band 8#0777).

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

mktmp(Name) ->
    Dir  = filename:join([
        "/tmp",
        "hecate_identity_tests",
        integer_to_list(erlang:unique_integer([positive]))
    ]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    filename:join(Dir, Name).
