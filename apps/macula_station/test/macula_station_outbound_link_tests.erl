%%% @doc Futility accounting on an outbound link.
%%%
%%% A dial that never succeeds used to cost nothing:
%%% `handle_dial_result({error, _Reason}, S)' discarded the reason with
%%% no log, no counter and no event. A link that had NEVER completed a
%%% handshake was therefore indistinguishable from a healthy idle one,
%%% which is how station-it-milan sat unreachable for 30 hours with one
%%% configured peer and nothing counting it.
-module(macula_station_outbound_link_tests).
-include_lib("eunit/include/eunit.hrl").

-define(M, macula_station_outbound_link).
-define(THRESHOLD, 300_000).

%%====================================================================
%% futility_verdict/3
%%====================================================================

verdict_verified_is_ok_test() ->
    %% `undefined' means the clock is not running, i.e. verified.
    ?assertEqual(ok, ?M:futility_verdict(undefined, 9_000_000, ?THRESHOLD)).

verdict_within_budget_is_ok_test() ->
    %% 30s handshake timeout + 60s max backoff is the ~90s worst
    %% legitimate cycle; a link inside the budget is not futile.
    ?assertEqual(ok, ?M:futility_verdict(0, ?THRESHOLD - 1, ?THRESHOLD)).

verdict_past_budget_is_futile_test() ->
    ?assertEqual(futile, ?M:futility_verdict(0, ?THRESHOLD, ?THRESHOLD)).

verdict_far_past_budget_is_futile_test() ->
    %% milan's actual interval: 30 hours.
    ?assertEqual(futile, ?M:futility_verdict(0, 108_000_000, ?THRESHOLD)).

%%====================================================================
%% The clock must not be restartable by a redial
%%
%% THE load-bearing subtlety. milan redialled on a 60s backoff for the
%% whole outage. A clock that each failed dial reset would have been
%% re-zeroed forever, so the interval could never reach the threshold
%% and the rule could never fire — it would look permanently fresh
%% while being permanently dead.
%%====================================================================

clock_starts_when_it_is_not_running_test() ->
    %% Verified -> unverified transition: start counting from now.
    ?assertEqual(1_234, ?M:unverified_from(undefined, 1_234)).

clock_is_not_restarted_by_a_later_failure_test() ->
    %% Already counting: keep the ORIGINAL start, not now.
    ?assertEqual(500, ?M:unverified_from(500, 9_999)).
