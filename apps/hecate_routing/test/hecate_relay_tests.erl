%% EUnit tests for hecate_relay — per-hop CALL processing.
-module(hecate_relay_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Forward — middle hop with live next hop
%%---------------------------------------------------------------------

forward_advances_source_route_and_returns_next_hop_test() ->
    Kp = macula_identity:generate(),
    SelfId = macula_identity:public(Kp),
    NextId = macula_identity:public(macula_identity:generate()),
    DestId = macula_identity:public(macula_identity:generate()),

    %% Build the source route with self at the head.
    SR  = hecate_source_route:new([SelfId, NextId, DestId],
                                  future_deadline()),
    Frame = build_call(Kp, SR),
    Ctx = ctx(Kp, fun(_) -> true end),

    {forward, ReturnedNext, Frame1} =
        hecate_relay:process_call(Frame, Ctx),
    %% Returned next hop is the truncated NodeId of NextId.
    ?assertEqual(hecate_source_route:truncate_hop(NextId), ReturnedNext),
    %% The frame's source_route field has advanced: current_hop = 1.
    {ok, SR1} = hecate_source_route:decode(maps:get(source_route, Frame1)),
    ?assertEqual(1, hecate_source_route:current_hop(SR1)).

forward_at_first_intermediate_hop_with_three_hop_path_test() ->
    Kp = macula_identity:generate(),
    SelfId = macula_identity:public(Kp),
    Mid    = macula_identity:public(macula_identity:generate()),
    Dest   = macula_identity:public(macula_identity:generate()),
    SR     = hecate_source_route:new([SelfId, Mid, Dest], future_deadline()),
    Frame  = build_call(Kp, SR),

    {forward, _NextHop, Frame1} =
        hecate_relay:process_call(Frame,
                                  ctx(Kp, fun(_) -> true end)),
    {ok, SR1} = hecate_source_route:decode(maps:get(source_route, Frame1)),
    ?assertEqual(1, hecate_source_route:current_hop(SR1)),
    ?assertNot(hecate_source_route:is_complete(SR1)).

%%---------------------------------------------------------------------
%% Deliver locally — final hop
%%---------------------------------------------------------------------

deliver_local_when_we_are_the_final_hop_test() ->
    Kp = macula_identity:generate(),
    SelfId = macula_identity:public(Kp),
    Mid    = macula_identity:public(macula_identity:generate()),
    %% Path: [Mid, SelfId]. We're the final hop after one advance.
    SR0    = hecate_source_route:new([Mid, SelfId], future_deadline()),
    SR     = hecate_source_route:advance(SR0),  %% current_hop = 1 (us)
    Frame  = build_call(Kp, SR),

    Action = hecate_relay:process_call(Frame,
                                       ctx(Kp, fun(_) -> true end)),
    ?assertMatch({deliver_local, _}, Action).

%%---------------------------------------------------------------------
%% invalid_path_header — missing / corrupt source_route
%%---------------------------------------------------------------------

missing_source_route_yields_invalid_path_header_test() ->
    Kp = macula_identity:generate(),
    Frame = (build_call(Kp, sample_sr(Kp)))#{source_route := <<>>},
    Action = hecate_relay:process_call(Frame,
                                       ctx(Kp, fun(_) -> true end)),
    assert_error_action(Action, invalid_path_header, Kp).

corrupt_source_route_yields_invalid_path_header_test() ->
    Kp = macula_identity:generate(),
    Frame = (build_call(Kp, sample_sr(Kp)))#{
              source_route := <<0:32, 0:64, 0:1024>>},  %% nonsense bytes
    Action = hecate_relay:process_call(Frame,
                                       ctx(Kp, fun(_) -> true end)),
    assert_error_action(Action, invalid_path_header, Kp).

position_mismatch_yields_invalid_path_header_test() ->
    Kp = macula_identity:generate(),
    %% Path lists a different NodeId at the current hop.
    Other = macula_identity:public(macula_identity:generate()),
    SR    = hecate_source_route:new([Other,
                                     macula_identity:public(Kp)],
                                    future_deadline()),
    Frame = build_call(Kp, SR),
    Action = hecate_relay:process_call(Frame,
                                       ctx(Kp, fun(_) -> true end)),
    assert_error_action(Action, invalid_path_header, Kp).

%%---------------------------------------------------------------------
%% expiry_too_soon
%%---------------------------------------------------------------------

expired_deadline_yields_expiry_too_soon_test() ->
    Kp = macula_identity:generate(),
    SR = hecate_source_route:new([macula_identity:public(Kp),
                                  random_id()],
                                 1),  %% deadline in the past
    Frame = build_call(Kp, SR),
    Action = hecate_relay:process_call(Frame,
                                       ctx(Kp, fun(_) -> true end)),
    assert_error_action(Action, expiry_too_soon, Kp).

%%---------------------------------------------------------------------
%% loop_detected
%%---------------------------------------------------------------------

duplicate_hop_yields_loop_detected_test() ->
    Kp = macula_identity:generate(),
    Self = macula_identity:public(Kp),
    Other = macula_identity:public(macula_identity:generate()),
    %% Path repeats a hop — the loop check fires before position
    %% checks, so the order in the path doesn't matter.
    SR = hecate_source_route:new([Self, Other, Other], future_deadline()),
    Frame = build_call(Kp, SR),
    Action = hecate_relay:process_call(Frame,
                                       ctx(Kp, fun(_) -> true end)),
    assert_error_action(Action, loop_detected, Kp).

%%---------------------------------------------------------------------
%% unknown_next_peer with offending_hop attribution
%%---------------------------------------------------------------------

dead_next_peer_yields_unknown_next_peer_with_hop_attribution_test() ->
    Kp = macula_identity:generate(),
    SelfId = macula_identity:public(Kp),
    Dead = macula_identity:public(macula_identity:generate()),
    SR = hecate_source_route:new([SelfId, Dead, random_id()],
                                 future_deadline()),
    Frame = build_call(Kp, SR),
    Action = hecate_relay:process_call(Frame,
                                       ctx(Kp, fun(_) -> false end)),
    {reply_error, ErrFrame, _Origin} = Action,
    ?assertEqual(error, hecate_frame:frame_type(ErrFrame)),
    ?assertEqual(unknown_next_peer, maps:get(name, ErrFrame)),
    %% offending_hop is the (zero-padded) Dead hop prefix.
    HopPrefix = hecate_source_route:truncate_hop(Dead),
    OffHop = maps:get(offending_hop, ErrFrame),
    ?assertEqual(HopPrefix, binary:part(OffHop, 0, 16)).

%%---------------------------------------------------------------------
%% Error frame is signed, addressed to the originator,
%% and carries source_route_partial
%%---------------------------------------------------------------------

error_frame_is_signed_and_carries_originator_test() ->
    Kp = macula_identity:generate(),
    SelfId = macula_identity:public(Kp),
    SR = hecate_source_route:new([SelfId, random_id()], 1),  %% expired
    CallerKp = macula_identity:generate(),
    CallerId = macula_identity:public(CallerKp),
    Frame = (build_call(Kp, SR))#{caller := CallerId},

    {reply_error, ErrFrame, Origin} =
        hecate_relay:process_call(Frame,
                                  ctx(Kp, fun(_) -> true end)),
    ?assertEqual(CallerId, Origin),
    ?assertMatch({ok, _}, hecate_frame:verify(ErrFrame, SelfId)),
    ?assertEqual(SelfId, maps:get(reported_by, ErrFrame)),
    %% source_route_partial is the original incoming SR bytes.
    ?assertEqual(maps:get(source_route, Frame),
                 maps:get(source_route_partial, ErrFrame)).

%%---------------------------------------------------------------------
%% Custom now_ms in context overrides the wall clock
%%---------------------------------------------------------------------

deadline_check_uses_context_supplied_now_ms_test() ->
    Kp = macula_identity:generate(),
    SR = hecate_source_route:new([macula_identity:public(Kp),
                                  random_id()],
                                 1_000_000),  %% deadline = 1_000_000ms
    Frame = build_call(Kp, SR),
    Ctx = (ctx(Kp, fun(_) -> true end))#{now_ms => 2_000_000},
    %% now is past the deadline → expiry_too_soon
    assert_error_action(hecate_relay:process_call(Frame, Ctx),
                        expiry_too_soon, Kp).

%%=====================================================================
%% Helpers
%%=====================================================================

future_deadline() ->
    erlang:system_time(millisecond) + 60_000.

random_id() ->
    macula_identity:public(macula_identity:generate()).

ctx(Kp, AliveFn) ->
    #{
        self_id  => macula_identity:public(Kp),
        identity => Kp,
        is_alive => AliveFn
    }.

build_call(Kp, SR) ->
    hecate_frame:sign(
      hecate_frame:call(#{
        call_id      => crypto:strong_rand_bytes(16),
        procedure    => <<"realm/acme/proc/v1">>,
        realm        => crypto:strong_rand_bytes(32),
        payload      => <<"hello">>,
        deadline_ms  => future_deadline(),
        caller       => macula_identity:public(macula_identity:generate()),
        source_route => hecate_source_route:encode(SR)
      }), Kp).

sample_sr(Kp) ->
    hecate_source_route:new([macula_identity:public(Kp), random_id()],
                            future_deadline()).

assert_error_action({reply_error, ErrFrame, _Origin}, ExpectedName, Kp) ->
    ?assertEqual(error, hecate_frame:frame_type(ErrFrame)),
    ?assertEqual(ExpectedName, maps:get(name, ErrFrame)),
    %% Verify signed by us.
    ?assertMatch({ok, _},
                 hecate_frame:verify(ErrFrame, macula_identity:public(Kp)));
assert_error_action(Other, ExpectedName, _Kp) ->
    erlang:error({unexpected_action, Other, expected_error, ExpectedName}).
