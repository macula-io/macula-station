-module(macula_station_listener_puzzle_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% puzzle_decision/2 — pure enforcement decision, no process state
%%==================================================================

off_accepts_a_valid_identity_test() ->
    ?assertEqual(accept, macula_station_listener:puzzle_decision(off, true)).

off_accepts_an_invalid_identity_test() ->
    %% The whole point of `off': the escape hatch never rejects,
    %% regardless of whether the puzzle actually holds.
    ?assertEqual(accept, macula_station_listener:puzzle_decision(off, false)).

log_only_accepts_a_valid_identity_test() ->
    ?assertEqual(accept, macula_station_listener:puzzle_decision(log_only, true)).

log_only_accepts_an_invalid_identity_test() ->
    %% `log_only' computes and (per on_handshake_complete/3) emits a
    %% diagnostic, but still lets the peer through.
    ?assertEqual(accept, macula_station_listener:puzzle_decision(log_only, false)).

enforce_accepts_a_valid_identity_test() ->
    ?assertEqual(accept, macula_station_listener:puzzle_decision(enforce, true)).

enforce_rejects_an_invalid_identity_test() ->
    %% The one case that actually rejects a handshake.
    ?assertEqual(reject, macula_station_listener:puzzle_decision(enforce, false)).
