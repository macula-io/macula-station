%% @doc Eunit suite for deterministic identity-key derivation.
-module(macula_station_identity_keys_tests).
-include_lib("eunit/include/eunit.hrl").

-define(MOD, macula_station_identity_keys).

%%==================================================================
%% Determinism — same inputs ⇒ same keypair.
%%==================================================================

same_box_secret_and_hostname_yield_same_keypair_test() ->
    BoxSecret = <<1:256>>,
    Host      = <<"relay-be-leuven.macula.io">>,
    KpA       = ?MOD:derive(BoxSecret, Host),
    KpB       = ?MOD:derive(BoxSecret, Host),
    ?assertEqual(macula_identity:public(KpA),  macula_identity:public(KpB)),
    ?assertEqual(macula_identity:private(KpA), macula_identity:private(KpB)).

string_hostname_and_binary_hostname_match_test() ->
    BoxSecret = <<1:256>>,
    Bin       = <<"relay-be-leuven.macula.io">>,
    Str       = "relay-be-leuven.macula.io",
    ?assertEqual(macula_identity:public(?MOD:derive(BoxSecret, Bin)),
                 macula_identity:public(?MOD:derive(BoxSecret, Str))).

%%==================================================================
%% Distinctness — different inputs ⇒ different keypairs.
%%==================================================================

different_hostname_under_same_box_secret_gives_distinct_keys_test() ->
    BoxSecret = <<1:256>>,
    KpA = ?MOD:derive(BoxSecret, <<"a.macula.io">>),
    KpB = ?MOD:derive(BoxSecret, <<"b.macula.io">>),
    ?assertNotEqual(macula_identity:public(KpA),
                    macula_identity:public(KpB)).

different_box_secret_gives_distinct_keys_test() ->
    Host = <<"relay-be-leuven.macula.io">>,
    KpA = ?MOD:derive(<<1:256>>, Host),
    KpB = ?MOD:derive(<<2:256>>, Host),
    ?assertNotEqual(macula_identity:public(KpA),
                    macula_identity:public(KpB)).

%%==================================================================
%% Validation — bad input is rejected.
%%==================================================================

bad_box_secret_size_rejected_test() ->
    ?assertError(function_clause,
                 ?MOD:derive(<<1:128>>, <<"x">>)).

%%==================================================================
%% box-secret on disk
%%==================================================================

cold_boot_generates_and_persists_test_() ->
    {setup, fun tmpdir/0, fun rm_rf/1, fun(Dir) ->
        fun() ->
            Path = filename:join(Dir, "box-secret"),
            ?assertNot(filelib:is_regular(Path)),
            {ok, S1} = ?MOD:load_or_generate_box_secret(Path),
            ?assertEqual(32, byte_size(S1)),
            ?assert(filelib:is_regular(Path)),
            %% Same path on reload returns the same secret.
            {ok, S2} = ?MOD:load_or_generate_box_secret(Path),
            ?assertEqual(S1, S2)
        end
    end}.

malformed_secret_returns_error_test_() ->
    {setup, fun tmpdir/0, fun rm_rf/1, fun(Dir) ->
        fun() ->
            Path = filename:join(Dir, "box-secret"),
            ok = file:write_file(Path, <<"too-short">>),
            ?assertEqual({error, malformed_box_secret},
                         ?MOD:load_or_generate_box_secret(Path))
        end
    end}.

%%==================================================================
%% Helpers
%%==================================================================

tmpdir() ->
    Dir = filename:join("/tmp",
            "hecate-identity-keys-"
            ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

rm_rf(Path) ->
    case filelib:is_dir(Path) of
        true ->
            {ok, Names} = file:list_dir(Path),
            [rm_rf(filename:join(Path, N)) || N <- Names],
            file:del_dir(Path);
        false ->
            file:delete(Path)
    end.
