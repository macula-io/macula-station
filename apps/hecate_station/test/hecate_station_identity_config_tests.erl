%% @doc Eunit suite for the identity config parser.
-module(hecate_station_identity_config_tests).
-include_lib("eunit/include/eunit.hrl").

-define(MOD, hecate_station_identity_config).

%%==================================================================
%% Empty / absent env
%%==================================================================

empty_input_yields_empty_list_test_() ->
    [
        ?_assertEqual({ok, []}, ?MOD:parse("")),
        ?_assertEqual({ok, []}, ?MOD:parse(<<>>))
    ].

%%==================================================================
%% Single identity
%%==================================================================

single_identity_with_bind_test() ->
    Input = "relay-be-leuven.macula.io/Leuven/BE/50.8798/4.7005/2a01:4f8:1c1f:8ab8::100",
    {ok, [Spec]} = ?MOD:parse(Input),
    ?assertEqual(<<"relay-be-leuven.macula.io">>, maps:get(hostname, Spec)),
    ?assertEqual(<<"Leuven">>,                    maps:get(city, Spec)),
    ?assertEqual(<<"BE">>,                        maps:get(country, Spec)),
    ?assertEqual(50.8798,                         maps:get(lat, Spec)),
    ?assertEqual(4.7005,                          maps:get(lng, Spec)),
    ?assertEqual(<<"2a01:4f8:1c1f:8ab8::100">>,   maps:get(bind, Spec)).

single_identity_without_bind_test() ->
    Input = "relay-be-leuven.macula.io/Leuven/BE/50.8798/4.7005",
    {ok, [Spec]} = ?MOD:parse(Input),
    ?assertEqual(undefined, maps:get(bind, Spec)).

%%==================================================================
%% Multiple identities
%%==================================================================

three_identities_test() ->
    Input = "a.macula.io/A/AA/1.0/2.0/::1,"
            "b.macula.io/B/BB/3.0/4.0/::2,"
            "c.macula.io/C/CC/5.0/6.0/::3",
    {ok, Specs} = ?MOD:parse(Input),
    ?assertEqual(3, length(Specs)),
    ?assertEqual([<<"a.macula.io">>, <<"b.macula.io">>, <<"c.macula.io">>],
                 [maps:get(hostname, S) || S <- Specs]).

whitespace_around_entries_is_trimmed_test() ->
    Input = "  a.macula.io/A/AA/1.0/2.0  ,  b.macula.io/B/BB/3.0/4.0  ",
    {ok, Specs} = ?MOD:parse(Input),
    ?assertEqual(2, length(Specs)),
    ?assertEqual(<<"a.macula.io">>, maps:get(hostname, hd(Specs))).

trailing_comma_is_silently_ignored_test() ->
    Input = "a.macula.io/A/AA/1.0/2.0,",
    {ok, [_]} = ?MOD:parse(Input),
    ok.

double_commas_are_silently_ignored_test() ->
    Input = "a.macula.io/A/AA/1.0/2.0,,b.macula.io/B/BB/3.0/4.0",
    {ok, [_, _]} = ?MOD:parse(Input),
    ok.

%%==================================================================
%% Integer lat/lng
%%==================================================================

integer_lat_lng_promotes_to_float_test() ->
    Input = "x.macula.io/X/XX/45/90",
    {ok, [Spec]} = ?MOD:parse(Input),
    ?assertEqual(45.0, maps:get(lat, Spec)),
    ?assertEqual(90.0, maps:get(lng, Spec)).

negative_lat_lng_test() ->
    Input = "x.macula.io/X/XX/-33.86/-151.21",
    {ok, [Spec]} = ?MOD:parse(Input),
    ?assertEqual(-33.86,  maps:get(lat, Spec)),
    ?assertEqual(-151.21, maps:get(lng, Spec)).

%%==================================================================
%% Errors
%%==================================================================

malformed_entry_returns_error_test() ->
    Input = "missing-fields-here",
    ?assertMatch({error, {invalid_entry, _}}, ?MOD:parse(Input)).

bad_lat_returns_error_test() ->
    Input = "x.macula.io/X/XX/not-a-number/2.0",
    ?assertMatch({error, {invalid_lat_lng, _, _}}, ?MOD:parse(Input)).

partial_float_is_rejected_test() ->
    Input = "x.macula.io/X/XX/12.3abc/45.0",
    ?assertMatch({error, {invalid_lat_lng, _, _}}, ?MOD:parse(Input)).

%%==================================================================
%% from_env/1
%%==================================================================

from_env_unset_yields_empty_test_() ->
    %% Use a name that is virtually guaranteed not to be set in CI.
    {setup,
     fun() -> os:unsetenv("HECATE_PHASE4_TEST_VAR") end,
     fun(_) -> ok end,
     fun(_) ->
         ?_assertEqual({ok, []},
                       ?MOD:from_env("HECATE_PHASE4_TEST_VAR"))
     end}.

from_env_with_value_parses_test_() ->
    {setup,
     fun() ->
         os:putenv("HECATE_PHASE4_TEST_VAR",
                   "a.macula.io/A/AA/1.0/2.0,"
                   "b.macula.io/B/BB/3.0/4.0")
     end,
     fun(_) -> os:unsetenv("HECATE_PHASE4_TEST_VAR") end,
     fun(_) ->
         {ok, Specs} = ?MOD:from_env("HECATE_PHASE4_TEST_VAR"),
         ?_assertEqual(2, length(Specs))
     end}.
