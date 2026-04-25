%% EUnit tests for hecate_overlay_proto.
-module(hecate_overlay_proto_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Outbound builders
%%---------------------------------------------------------------------

build_join_returns_signed_frame_test() ->
    Kp = macula_identity:generate(),
    Self = macula_identity:public(Kp),
    Ctx = ctx(Kp, Self),
    Frame = hecate_overlay_proto:build_join(Ctx),
    ?assertEqual(hyparview_join, hecate_frame:frame_type(Frame)),
    ?assertEqual(Self, maps:get(new_member, Frame)),
    ?assertMatch({ok, _}, hecate_frame:verify(Frame, Self)).

%%---------------------------------------------------------------------
%% JOIN handler
%%---------------------------------------------------------------------

join_adds_sender_to_active_and_replies_neighbor_high_test() ->
    SelfKp = macula_identity:generate(),
    Self = macula_identity:public(SelfKp),
    Joiner = id(1),
    View0 = hecate_overlay_view:new(Self),
    Ctx = ctx(SelfKp, Self),
    Frame = hecate_overlay_proto:build_join(ctx(Joiner)),
    {View1, Actions} = hecate_overlay_proto:process(View0, Joiner, Frame, Ctx),
    ?assert(hecate_overlay_view:is_active(Joiner, View1)),
    %% At least one action: NEIGHBOR(high) to the joiner.
    ?assert(lists:any(
              fun({send, T, F}) ->
                  T =:= Joiner andalso
                  hecate_frame:frame_type(F) =:= hyparview_neighbor andalso
                  maps:get(priority, F) =:= high
              end, Actions)).

join_with_existing_actives_forwards_join_test() ->
    SelfKp = macula_identity:generate(),
    Self = macula_identity:public(SelfKp),
    %% Pre-populate two existing active peers.
    View0 = lists:foldl(fun(P, V) -> hecate_overlay_view:add_active(V, P) end,
                        hecate_overlay_view:new(Self,
                                                #{active_cap => 5,
                                                  passive_cap => 20}),
                        [id(10), id(11)]),
    Joiner = id(1),
    Ctx = ctx(SelfKp, Self),
    Frame = hecate_overlay_proto:build_join(ctx(Joiner)),
    {_View1, Actions} = hecate_overlay_proto:process(View0, Joiner,
                                                    Frame, Ctx),
    Forwards = [A || {send, T, F} = A <- Actions,
                     hecate_frame:frame_type(F) =:= hyparview_forward_join,
                     T =/= Joiner],
    ?assertEqual(2, length(Forwards)),
    %% Each forward carries Joiner as new_member.
    [?assertEqual(Joiner, maps:get(new_member, F)) ||
        {send, _, F} <- Forwards].

%%---------------------------------------------------------------------
%% FORWARD_JOIN handler
%%---------------------------------------------------------------------

forward_join_with_zero_ttl_accepts_test() ->
    SelfKp = macula_identity:generate(),
    Self   = macula_identity:public(SelfKp),
    NewM   = id(7),
    Sender = id(8),
    View0  = hecate_overlay_view:new(Self),
    Ctx    = ctx(SelfKp, Self),
    Frame  = build_fwd(NewM, 0, 6, 3, Sender),
    {View1, Actions} = hecate_overlay_proto:process(View0, Sender,
                                                   Frame, Ctx),
    ?assert(hecate_overlay_view:is_active(NewM, View1)),
    %% Reply NEIGHBOR(high) to the new member.
    ?assert(lists:any(fun({send, T, F}) ->
                          T =:= NewM andalso
                          hecate_frame:frame_type(F) =:= hyparview_neighbor
                      end, Actions)).

forward_join_with_empty_active_accepts_test() ->
    SelfKp = macula_identity:generate(),
    Self   = macula_identity:public(SelfKp),
    NewM   = id(7),
    Sender = id(8),
    View0  = hecate_overlay_view:new(Self),
    Ctx    = ctx(SelfKp, Self),
    Frame  = build_fwd(NewM, 6, 6, 3, Sender),
    {View1, _} = hecate_overlay_proto:process(View0, Sender, Frame, Ctx),
    %% Active view was empty → accept regardless of TTL.
    ?assert(hecate_overlay_view:is_active(NewM, View1)).

forward_join_at_prwl_threshold_adds_to_passive_test() ->
    SelfKp = macula_identity:generate(),
    Self   = macula_identity:public(SelfKp),
    NewM   = id(7),
    Sender = id(8),
    Other  = id(9),
    %% Two existing actives so we don't take the empty-view path.
    View0  = lists:foldl(fun(P, V) -> hecate_overlay_view:add_active(V, P) end,
                         hecate_overlay_view:new(Self),
                         [Sender, Other]),
    Ctx    = ctx(SelfKp, Self),
    %% PRWL = 3, set ttl == prwl so the passive-add fires.
    Frame  = build_fwd(NewM, 3, 6, 3, Sender),
    {View1, _Actions} = hecate_overlay_proto:process(View0, Sender,
                                                   Frame, Ctx),
    ?assert(hecate_overlay_view:is_passive(NewM, View1)).

forward_join_above_zero_ttl_forwards_to_random_active_test() ->
    SelfKp = macula_identity:generate(),
    Self   = macula_identity:public(SelfKp),
    NewM   = id(7),
    Sender = id(8),
    Other  = id(9),
    View0  = lists:foldl(fun(P, V) -> hecate_overlay_view:add_active(V, P) end,
                         hecate_overlay_view:new(Self),
                         [Sender, Other]),
    Ctx    = ctx(SelfKp, Self),
    Frame  = build_fwd(NewM, 5, 6, 3, Sender),
    {_, Actions} = hecate_overlay_proto:process(View0, Sender, Frame, Ctx),
    %% A single forward to Other (excludes Sender + NewM).
    [{send, T, F}] = Actions,
    ?assertEqual(Other, T),
    ?assertEqual(hyparview_forward_join, hecate_frame:frame_type(F)),
    %% TTL decremented.
    ?assertEqual(4, maps:get(ttl, F)).

%%---------------------------------------------------------------------
%% NEIGHBOR handler
%%---------------------------------------------------------------------

neighbor_high_always_admits_test() ->
    SelfKp = macula_identity:generate(),
    Self   = macula_identity:public(SelfKp),
    Sender = id(1),
    View0  = hecate_overlay_view:new(Self, #{active_cap => 1,
                                             passive_cap => 4}),
    %% Pre-fill active so admission requires eviction.
    View1  = hecate_overlay_view:add_active(View0, id(2)),
    Ctx    = ctx(SelfKp, Self),
    Frame  = sign_neighbor(high, Ctx),
    {View2, _} = hecate_overlay_proto:process(View1, Sender, Frame, Ctx),
    ?assert(hecate_overlay_view:is_active(Sender, View2)),
    %% The pre-existing active was demoted to passive.
    ?assert(hecate_overlay_view:is_passive(id(2), View2)).

neighbor_low_admits_only_if_room_test() ->
    SelfKp = macula_identity:generate(),
    Self   = macula_identity:public(SelfKp),
    View0  = hecate_overlay_view:new(Self, #{active_cap => 1,
                                             passive_cap => 4}),
    View1  = hecate_overlay_view:add_active(View0, id(2)),
    Ctx    = ctx(SelfKp, Self),
    Frame  = sign_neighbor(low, Ctx),
    {View2, _} = hecate_overlay_proto:process(View1, id(3), Frame, Ctx),
    %% Active is full; low priority → goes to passive.
    ?assertNot(hecate_overlay_view:is_active(id(3), View2)),
    ?assert(hecate_overlay_view:is_passive(id(3), View2)).

%%---------------------------------------------------------------------
%% DISCONNECT handler
%%---------------------------------------------------------------------

disconnect_demotes_sender_test() ->
    SelfKp = macula_identity:generate(),
    Self   = macula_identity:public(SelfKp),
    Sender = id(5),
    View0  = hecate_overlay_view:add_active(
               hecate_overlay_view:new(Self), Sender),
    Ctx    = ctx(SelfKp, Self),
    Frame  = hecate_frame:sign(hecate_frame:hyparview_disconnect(
                                 #{realm => maps:get(realm, Ctx)}),
                               SelfKp),
    {View1, _} = hecate_overlay_proto:process(View0, Sender, Frame, Ctx),
    ?assertNot(hecate_overlay_view:is_active(Sender, View1)),
    ?assert(hecate_overlay_view:is_passive(Sender, View1)).

%%---------------------------------------------------------------------
%% SHUFFLE / SHUFFLE_REPLY handlers
%%---------------------------------------------------------------------

shuffle_with_zero_ttl_replies_and_merges_test() ->
    SelfKp = macula_identity:generate(),
    Self   = macula_identity:public(SelfKp),
    Origin = id(20),
    Sender = id(21),
    Sample = [id(30), id(31)],
    View0  = hecate_overlay_view:add_passive(
               hecate_overlay_view:new(Self), id(40)),
    Ctx    = ctx(SelfKp, Self),
    Frame  = hecate_frame:sign(hecate_frame:hyparview_shuffle(
                                 #{realm => maps:get(realm, Ctx),
                                   origin => Origin,
                                   ttl    => 0,
                                   peer_sample => Sample}),
                               SelfKp),
    {View1, [{send, T, ReplyFrame}]} =
        hecate_overlay_proto:process(View0, Sender, Frame, Ctx),
    ?assertEqual(Origin, T),
    ?assertEqual(hyparview_shuffle_reply,
                 hecate_frame:frame_type(ReplyFrame)),
    %% Incoming sample peers merged into passive view.
    ?assert(hecate_overlay_view:is_passive(id(30), View1)),
    ?assert(hecate_overlay_view:is_passive(id(31), View1)).

shuffle_with_positive_ttl_forwards_to_random_active_test() ->
    SelfKp = macula_identity:generate(),
    Self   = macula_identity:public(SelfKp),
    Origin = id(50),
    Sender = id(51),
    Other  = id(52),
    View0  = lists:foldl(fun(P, V) -> hecate_overlay_view:add_active(V, P) end,
                         hecate_overlay_view:new(Self),
                         [Sender, Other]),
    Ctx    = ctx(SelfKp, Self),
    Frame  = hecate_frame:sign(hecate_frame:hyparview_shuffle(
                                 #{realm => maps:get(realm, Ctx),
                                   origin => Origin,
                                   ttl    => 3,
                                   peer_sample => []}),
                               SelfKp),
    {_, [{send, T, F}]} = hecate_overlay_proto:process(View0, Sender,
                                                       Frame, Ctx),
    ?assertEqual(Other, T),
    ?assertEqual(hyparview_shuffle, hecate_frame:frame_type(F)),
    ?assertEqual(2, maps:get(ttl, F)).

shuffle_reply_merges_into_passive_test() ->
    SelfKp = macula_identity:generate(),
    Self   = macula_identity:public(SelfKp),
    Sender = id(60),
    Sample = [id(70), id(71)],
    View0  = hecate_overlay_view:new(Self),
    Ctx    = ctx(SelfKp, Self),
    Frame  = hecate_frame:sign(hecate_frame:hyparview_shuffle_reply(
                                 #{realm => maps:get(realm, Ctx),
                                   peer_sample => Sample}), SelfKp),
    {View1, []} = hecate_overlay_proto:process(View0, Sender, Frame, Ctx),
    ?assert(hecate_overlay_view:is_passive(id(70), View1)),
    ?assert(hecate_overlay_view:is_passive(id(71), View1)).

%%=====================================================================
%% Helpers
%%=====================================================================

id(N) -> <<N:256>>.

ctx(Self) ->
    %% Used when only the self_id is known (other-side ctx).
    Kp = macula_identity:generate(),
    #{
        self_id  => Self,
        realm    => crypto:strong_rand_bytes(32),
        identity => Kp
    }.

ctx(Kp, Self) ->
    #{
        self_id  => Self,
        realm    => crypto:strong_rand_bytes(32),
        identity => Kp
    }.

build_fwd(NewMember, Ttl, Arwl, Prwl, _Sender) ->
    %% A fresh signing identity stands in for the sender; the
    %% handler only reads the frame's fields, not its signature.
    Kp = macula_identity:generate(),
    hecate_frame:sign(hecate_frame:hyparview_forward_join(
                        #{realm      => crypto:strong_rand_bytes(32),
                          new_member => NewMember,
                          ttl        => Ttl,
                          arwl       => Arwl,
                          prwl       => Prwl}), Kp).

sign_neighbor(Priority, #{identity := Kp, realm := R}) ->
    hecate_frame:sign(hecate_frame:hyparview_neighbor(
                        #{realm => R, priority => Priority}), Kp).
