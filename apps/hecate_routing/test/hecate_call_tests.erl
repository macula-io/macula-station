%% EUnit tests for hecate_call — the CALL state machine.
-module(hecate_call_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Defaults + basic shape
%%---------------------------------------------------------------------

init_starts_in_resolving_after_internal_start_test() ->
    {ok, P} = start_call(),
    %% A fast call gives the FSM time to transition idle → resolving.
    timer:sleep(5),
    ?assertEqual(resolving, hecate_call:state(P)),
    hecate_call:stop(P).

info_reports_call_id_and_state_test() ->
    {ok, P} = start_call(),
    timer:sleep(5),
    Info = hecate_call:info(P),
    ?assertEqual(16, byte_size(maps:get(call_id, Info))),
    ?assertEqual(resolving, maps:get(state, Info)),
    ?assertEqual(undefined, maps:get(target, Info)),
    hecate_call:stop(P).

%%---------------------------------------------------------------------
%% Happy-path traversal idle → resolving → ... → succeeded
%%---------------------------------------------------------------------

happy_path_to_succeeded_test() ->
    {ok, P} = start_call(short_timeouts()),
    Tgt = node_id(),
    Path = [node_id(), Tgt],
    ok = hecate_call:resolved(P, Tgt, Path),
    ok = hecate_call:selected(P, Tgt),
    ok = hecate_call:connected(P),
    ok = hecate_call:ack(P, <<"reply payload">>),
    {ok, <<"reply payload">>} = hecate_call:await(P, 1_000),
    ?assertEqual(succeeded, hecate_call:state(P)),
    Info = hecate_call:info(P),
    ?assertEqual(Tgt, maps:get(target, Info)),
    ?assertEqual(Path, maps:get(path, Info)),
    hecate_call:stop(P).

%%---------------------------------------------------------------------
%% Per-state timeouts → BOLT#4 failure codes
%%---------------------------------------------------------------------

resolving_timeout_yields_temporary_relay_failure_test() ->
    {ok, P} = start_call(#{resolving_timeout_ms => 30}),
    {error, temporary_relay_failure, Detail} = hecate_call:await(P, 500),
    ?assertEqual(resolving, maps:get(phase, Detail)),
    ?assertEqual(failed, hecate_call:state(P)),
    hecate_call:stop(P).

selected_target_timeout_yields_unknown_next_peer_test() ->
    {ok, P} = start_call(#{resolving_timeout_ms => 1_000,
                            selected_target_timeout_ms => 30}),
    ok = hecate_call:resolved(P, node_id(), [node_id()]),
    {error, unknown_next_peer, _} = hecate_call:await(P, 500),
    ?assertEqual(failed, hecate_call:state(P)),
    hecate_call:stop(P).

connecting_timeout_yields_temporary_relay_failure_test() ->
    {ok, P} = start_call(#{resolving_timeout_ms => 1_000,
                            selected_target_timeout_ms => 1_000,
                            connecting_timeout_ms => 30}),
    Tgt = node_id(),
    ok = hecate_call:resolved(P, Tgt, [Tgt]),
    ok = hecate_call:selected(P, Tgt),
    {error, temporary_relay_failure, Detail} = hecate_call:await(P, 500),
    ?assertEqual(connecting, maps:get(phase, Detail)),
    hecate_call:stop(P).

awaiting_ack_timeout_yields_expiry_too_soon_test() ->
    {ok, P} = start_call(#{resolving_timeout_ms => 1_000,
                            selected_target_timeout_ms => 1_000,
                            connecting_timeout_ms => 1_000,
                            awaiting_ack_timeout_ms => 30}),
    Tgt = node_id(),
    ok = hecate_call:resolved(P, Tgt, [Tgt]),
    ok = hecate_call:selected(P, Tgt),
    ok = hecate_call:connected(P),
    {error, expiry_too_soon, _} = hecate_call:await(P, 500),
    hecate_call:stop(P).

%%---------------------------------------------------------------------
%% ack_error: caller-driven failure with arbitrary BOLT#4 code
%%---------------------------------------------------------------------

ack_error_propagates_bolt4_atom_test() ->
    {ok, P} = run_to_awaiting_ack(),
    Hop = node_id(),
    ok = hecate_call:ack_error(P, target_realm_refused, Hop),
    {error, target_realm_refused, Detail} = hecate_call:await(P, 500),
    ?assertEqual(Hop, maps:get(offending_hop, Detail)),
    hecate_call:stop(P).

ack_error_accepts_integer_code_and_resolves_to_atom_test() ->
    {ok, P} = run_to_awaiting_ack(),
    ok = hecate_call:ack_error(P, 16#03, undefined),
    {error, relay_disabled, Detail} = hecate_call:await(P, 500),
    ?assertNot(maps:is_key(offending_hop, Detail)),
    hecate_call:stop(P).

%%---------------------------------------------------------------------
%% notify_pid receives outcome on terminal state
%%---------------------------------------------------------------------

notify_pid_receives_success_message_test() ->
    Self = self(),
    {ok, P} = start_call(short_timeouts(), #{notify_pid => Self}),
    Tgt = node_id(),
    ok = hecate_call:resolved(P, Tgt, [Tgt]),
    ok = hecate_call:selected(P, Tgt),
    ok = hecate_call:connected(P),
    ok = hecate_call:ack(P, ok),
    receive
        {hecate_call, P, {ok, ok}} -> ok
    after 500 ->
        ?assert(false)
    end,
    hecate_call:stop(P).

notify_pid_receives_failure_message_test() ->
    Self = self(),
    {ok, P} = start_call(#{resolving_timeout_ms => 30},
                        #{notify_pid => Self}),
    receive
        {hecate_call, P, {error, temporary_relay_failure, _}} -> ok
    after 500 ->
        ?assert(false)
    end,
    hecate_call:stop(P).

%%---------------------------------------------------------------------
%% await semantics
%%---------------------------------------------------------------------

await_after_terminal_returns_outcome_test() ->
    {ok, P} = start_call(#{resolving_timeout_ms => 30}),
    {error, temporary_relay_failure, _} = hecate_call:await(P, 500),
    %% Second await against an already-terminal FSM should still
    %% return the same outcome.
    {error, temporary_relay_failure, _} = hecate_call:await(P, 100),
    hecate_call:stop(P).

multiple_awaiters_all_receive_outcome_test() ->
    {ok, P} = start_call(#{resolving_timeout_ms => 100}),
    Self = self(),
    Spawn = fun() ->
        Outcome = hecate_call:await(P, 1_000),
        Self ! {await_done, self(), Outcome}
    end,
    A = spawn_link(Spawn),
    B = spawn_link(Spawn),
    receive {await_done, A, OutA} -> ?assertMatch({error, _, _}, OutA)
    after 1_000 -> ?assert(false) end,
    receive {await_done, B, OutB} -> ?assertMatch({error, _, _}, OutB)
    after 1_000 -> ?assert(false) end,
    hecate_call:stop(P).

%%---------------------------------------------------------------------
%% Stale events in wrong state are ignored
%%---------------------------------------------------------------------

resolved_in_connecting_is_silently_ignored_test() ->
    {ok, P} = start_call(short_timeouts()),
    Tgt = node_id(),
    ok = hecate_call:resolved(P, Tgt, [Tgt]),
    ok = hecate_call:selected(P, Tgt),
    %% Resolved arriving in connecting must not move state back.
    ok = hecate_call:resolved(P, Tgt, [Tgt]),
    timer:sleep(10),
    ?assertEqual(connecting, hecate_call:state(P)),
    hecate_call:stop(P).

%%=====================================================================
%% Helpers
%%=====================================================================

start_call() ->
    start_call(#{}).

start_call(ExtraOpts) ->
    start_call(ExtraOpts, #{}).

start_call(ExtraOpts, MoreOpts) ->
    Base = #{
        caller    => node_id(),
        procedure => <<"realm/acme/weather/forecast/get_v1">>,
        realm     => node_id(),
        payload   => #{city => <<"Brussels">>}
    },
    Opts = maps:merge(maps:merge(Base, ExtraOpts), MoreOpts),
    hecate_call:start_link(Opts).

short_timeouts() ->
    #{resolving_timeout_ms       => 1_000,
      selected_target_timeout_ms => 1_000,
      connecting_timeout_ms      => 1_000,
      awaiting_ack_timeout_ms    => 1_000}.

run_to_awaiting_ack() ->
    {ok, P} = start_call(short_timeouts()),
    Tgt = node_id(),
    ok = hecate_call:resolved(P, Tgt, [Tgt]),
    ok = hecate_call:selected(P, Tgt),
    ok = hecate_call:connected(P),
    timer:sleep(10),
    ?assertEqual(awaiting_ack, hecate_call:state(P)),
    {ok, P}.

node_id() ->
    crypto:strong_rand_bytes(32).
