%% @doc HyParView protocol orchestrator (Part 3 §7.1).
%%
%% Pure functional layer on top of `hecate_overlay_view'. Given the
%% current view, an incoming HyParView frame, and a context (self
%% NodeId, realm, signing identity, ARWL/PRWL constants),
%% `process/4' returns the updated view plus a list of `{send,
%% TargetPeer, Frame}' actions the wrapping process should
%% transmit.
%%
%% == Message handlers ==
%%
%% <ul>
%%   <li><strong>JOIN</strong> — receiver becomes the joiner's
%%       contact: add_active(joiner), reply NEIGHBOR(high),
%%       FORWARD_JOIN(ttl=ARWL) to every other active peer.
%%       Eviction in the active view emits DISCONNECT to the
%%       demoted peer.</li>
%%   <li><strong>FORWARD_JOIN</strong> — if ttl=0 or active view
%%       is empty: add_active(new_member) + reply NEIGHBOR(high).
%%       Otherwise: if ttl == PRWL, also add_passive(new_member);
%%       decrement ttl and forward to a random active peer that
%%       is not the sender.</li>
%%   <li><strong>NEIGHBOR(high)</strong> — always add_active(sender),
%%       evicting if needed.</li>
%%   <li><strong>NEIGHBOR(low)</strong> — add_active(sender) only
%%       when the active view has room.</li>
%%   <li><strong>DISCONNECT</strong> — demote sender to passive.</li>
%%   <li><strong>SHUFFLE</strong> — if ttl > 0 and a forwardable
%%       neighbour exists, decrement ttl + forward; otherwise
%%       build SHUFFLE_REPLY against our own sample and send back
%%       to the origin, then merge the incoming sample into our
%%       passive view.</li>
%%   <li><strong>SHUFFLE_REPLY</strong> — merge the incoming
%%       sample into the passive view.</li>
%% </ul>
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §7.1;
%% plans/PLAN_PHASE_5_BREAKDOWN.md Session 5.2.
-module(hecate_overlay_proto).

-export([
    build_join/1,
    build_shuffle/1,
    process/4
]).

-export_type([ctx/0, action/0, peer/0]).

-define(DEFAULT_ARWL,                 6).
-define(DEFAULT_PRWL,                 3).
-define(DEFAULT_SHUFFLE_TTL,          4).
-define(DEFAULT_SHUFFLE_ACTIVE,       3).
-define(DEFAULT_SHUFFLE_PASSIVE,      4).

-type peer() :: macula_identity:pubkey().

-type ctx() :: #{
    self_id              := peer(),
    realm                := <<_:256>>,
    identity             := macula_identity:key_pair(),
    arwl                 => non_neg_integer(),
    prwl                 => non_neg_integer(),
    shuffle_ttl          => non_neg_integer(),
    shuffle_active_size  => non_neg_integer(),
    shuffle_passive_size => non_neg_integer()
}.

-type action() :: {send, peer(), macula_frame:frame()}.

%%=====================================================================
%% Outbound builders for events the local process initiates
%%=====================================================================

%% @doc Build a signed JOIN frame to send to a contact peer.
-spec build_join(ctx()) -> macula_frame:frame().
build_join(#{self_id := Self, realm := R, identity := Id}) ->
    sign_with(macula_frame:hyparview_join(
                #{realm => R, new_member => Self}), Id).

%% @doc Build a signed SHUFFLE frame for a periodic shuffle round.
%% The orchestrator picks a random active neighbour to send it to.
-spec build_shuffle(ctx()) -> {ok, macula_frame:frame()}.
build_shuffle(#{self_id := Self, realm := R, identity := Id} = Ctx) ->
    Ttl = maps:get(shuffle_ttl, Ctx, ?DEFAULT_SHUFFLE_TTL),
    %% Sample drawn from our own view by the caller — keep this
    %% function pure of the view structure. Caller computes the
    %% sample list and passes it in via process/4 if needed.
    F = macula_frame:hyparview_shuffle(
          #{realm => R, origin => Self, ttl => Ttl,
            peer_sample => []}),
    {ok, sign_with(F, Id)}.

%%=====================================================================
%% Process incoming frame
%%=====================================================================

-spec process(hecate_overlay_view:view(), peer(), macula_frame:frame(),
              ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
process(View, FromId, #{frame_type := hyparview_join} = F, Ctx) ->
    on_join(View, FromId, F, Ctx);
process(View, FromId, #{frame_type := hyparview_forward_join} = F, Ctx) ->
    on_forward_join(View, FromId, F, Ctx);
process(View, FromId, #{frame_type := hyparview_neighbor} = F, Ctx) ->
    on_neighbor(View, FromId, F, Ctx);
process(View, FromId, #{frame_type := hyparview_disconnect} = _F, _Ctx) ->
    on_disconnect(View, FromId);
process(View, FromId, #{frame_type := hyparview_shuffle} = F, Ctx) ->
    on_shuffle(View, FromId, F, Ctx);
process(View, FromId, #{frame_type := hyparview_shuffle_reply} = F, _Ctx) ->
    on_shuffle_reply(View, FromId, F);
process(View, _FromId, _Frame, _Ctx) ->
    {View, []}.

%%=====================================================================
%% JOIN
%%=====================================================================

-spec on_join(hecate_overlay_view:view(), peer(),
              macula_frame:frame(), ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
on_join(View, FromId, _Frame, Ctx) ->
    NewMember = FromId,
    {View1, EvictionActions} = absorb_active(View, NewMember, Ctx),
    NeighborAck = neighbor(NewMember, high, Ctx),
    Forwards = forward_join_to_others(View1, NewMember, FromId, Ctx),
    {View1, [NeighborAck | EvictionActions ++ Forwards]}.

-spec absorb_active(hecate_overlay_view:view(), peer(), ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
absorb_active(View, Peer, Ctx) ->
    OldActive = sets_from_list(hecate_overlay_view:active(View)),
    View1 = hecate_overlay_view:add_active(View, Peer),
    NewActive = sets_from_list(hecate_overlay_view:active(View1)),
    Evicted = sets:to_list(sets:subtract(OldActive, NewActive)),
    Disconnects = [{send, E, build_disconnect(Ctx)} || E <- Evicted],
    {View1, Disconnects}.

-spec forward_join_to_others(hecate_overlay_view:view(), peer(), peer(),
                             ctx()) -> [action()].
forward_join_to_others(View, NewMember, Origin, Ctx) ->
    Targets = [P || P <- hecate_overlay_view:active(View),
                    P =/= NewMember, P =/= Origin],
    Arwl = arwl(Ctx),
    Prwl = prwl(Ctx),
    [{send, T,
      sign_with(macula_frame:hyparview_forward_join(
                  #{realm      => realm(Ctx),
                    new_member => NewMember,
                    ttl        => Arwl,
                    arwl       => Arwl,
                    prwl       => Prwl}),
                identity(Ctx))} || T <- Targets].

%%=====================================================================
%% FORWARD_JOIN
%%=====================================================================

-spec on_forward_join(hecate_overlay_view:view(), peer(),
                      macula_frame:frame(), ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
on_forward_join(View, FromId, Frame, Ctx) ->
    NewMember = maps:get(new_member, Frame),
    Ttl       = maps:get(ttl, Frame),
    Prwl      = maps:get(prwl, Frame),
    classify_forward_join(NewMember, FromId, Ttl, Prwl, View, Frame, Ctx).

-spec classify_forward_join(peer(), peer(), non_neg_integer(),
                            non_neg_integer(),
                            hecate_overlay_view:view(),
                            macula_frame:frame(), ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
classify_forward_join(NewMember, _From, 0, _Prwl, View, _Frame, Ctx) ->
    accept_into_active(View, NewMember, Ctx);
classify_forward_join(NewMember, From, Ttl, Prwl, View, Frame, Ctx) ->
    case hecate_overlay_view:active_size(View) of
        N when N =< 1 ->
            accept_into_active(View, NewMember, Ctx);
        _ ->
            View1 = maybe_add_passive(NewMember, Ttl, Prwl, View),
            forward_to_random(View1, From, NewMember, Ttl - 1, Frame, Ctx)
    end.

-spec accept_into_active(hecate_overlay_view:view(), peer(), ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
accept_into_active(View, NewMember, Ctx) ->
    {View1, Evictions} = absorb_active(View, NewMember, Ctx),
    Ack = neighbor(NewMember, high, Ctx),
    {View1, [Ack | Evictions]}.

-spec maybe_add_passive(peer(), non_neg_integer(), non_neg_integer(),
                        hecate_overlay_view:view()) ->
        hecate_overlay_view:view().
maybe_add_passive(NewMember, Ttl, Prwl, View) when Ttl =:= Prwl ->
    hecate_overlay_view:add_passive(View, NewMember);
maybe_add_passive(_NewMember, _Ttl, _Prwl, View) ->
    View.

-spec forward_to_random(hecate_overlay_view:view(), peer(), peer(),
                        non_neg_integer(), macula_frame:frame(),
                        ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
forward_to_random(View, From, NewMember, NewTtl, Frame, Ctx) ->
    Candidates = [P || P <- hecate_overlay_view:active(View),
                       P =/= From, P =/= NewMember],
    pick_forward_target(Candidates, View, NewMember, NewTtl, Frame, Ctx).

-spec pick_forward_target([peer()], hecate_overlay_view:view(), peer(),
                          non_neg_integer(), macula_frame:frame(),
                          ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
pick_forward_target([], View, NewMember, _NewTtl, _Frame, Ctx) ->
    accept_into_active(View, NewMember, Ctx);
pick_forward_target(Cands, View, _NewMember, NewTtl, Frame, Ctx) ->
    Target = lists:nth(rand:uniform(length(Cands)), Cands),
    Forward = sign_with(macula_frame:hyparview_forward_join(
                          #{realm      => realm(Ctx),
                            new_member => maps:get(new_member, Frame),
                            ttl        => NewTtl,
                            arwl       => maps:get(arwl, Frame),
                            prwl       => maps:get(prwl, Frame)}),
                        identity(Ctx)),
    {View, [{send, Target, Forward}]}.

%%=====================================================================
%% NEIGHBOR
%%=====================================================================

-spec on_neighbor(hecate_overlay_view:view(), peer(),
                  macula_frame:frame(), ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
on_neighbor(View, FromId, #{priority := high}, Ctx) ->
    {View1, Evictions} = absorb_active(View, FromId, Ctx),
    {View1, Evictions};
on_neighbor(View, FromId, #{priority := low}, _Ctx) ->
    case hecate_overlay_view:active_size(View)
            < hecate_overlay_view:active_cap(View) of
        true  -> {hecate_overlay_view:add_active(View, FromId), []};
        false -> {hecate_overlay_view:add_passive(View, FromId), []}
    end.

%%=====================================================================
%% DISCONNECT
%%=====================================================================

-spec on_disconnect(hecate_overlay_view:view(), peer()) ->
        {hecate_overlay_view:view(), [action()]}.
on_disconnect(View, FromId) ->
    {hecate_overlay_view:demote(View, FromId), []}.

%%=====================================================================
%% SHUFFLE / SHUFFLE_REPLY
%%=====================================================================

-spec on_shuffle(hecate_overlay_view:view(), peer(),
                 macula_frame:frame(), ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
on_shuffle(View, FromId, Frame, Ctx) ->
    Ttl = maps:get(ttl, Frame),
    case Ttl > 0 of
        true  -> shuffle_forward(View, FromId, Frame, Ctx);
        false -> shuffle_reply(View, FromId, Frame, Ctx)
    end.

-spec shuffle_forward(hecate_overlay_view:view(), peer(),
                      macula_frame:frame(), ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
shuffle_forward(View, From, Frame, Ctx) ->
    Origin = maps:get(origin, Frame),
    Cands = [P || P <- hecate_overlay_view:active(View),
                  P =/= From, P =/= Origin],
    pick_shuffle_target(Cands, View, From, Frame, Ctx).

-spec pick_shuffle_target([peer()], hecate_overlay_view:view(), peer(),
                          macula_frame:frame(), ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
pick_shuffle_target([], View, From, Frame, Ctx) ->
    %% No-one to forward to — answer ourselves.
    shuffle_reply(View, From, Frame, Ctx);
pick_shuffle_target(Cands, View, _From, Frame, Ctx) ->
    Target = lists:nth(rand:uniform(length(Cands)), Cands),
    NewFrame = sign_with(macula_frame:hyparview_shuffle(
                           #{realm      => realm(Ctx),
                             origin     => maps:get(origin, Frame),
                             ttl        => maps:get(ttl, Frame) - 1,
                             peer_sample => maps:get(peer_sample,
                                                     Frame)}),
                         identity(Ctx)),
    {View, [{send, Target, NewFrame}]}.

-spec shuffle_reply(hecate_overlay_view:view(), peer(),
                    macula_frame:frame(), ctx()) ->
        {hecate_overlay_view:view(), [action()]}.
shuffle_reply(View, _FromId, Frame, Ctx) ->
    Origin = maps:get(origin, Frame),
    Sample = maps:get(peer_sample, Frame),
    LocalSample = collect_sample(View, Ctx),
    Reply = sign_with(macula_frame:hyparview_shuffle_reply(
                        #{realm => realm(Ctx),
                          peer_sample => LocalSample}),
                      identity(Ctx)),
    %% Merge incoming sample into our passive view.
    View1 = hecate_overlay_view:merge_shuffle(View, Sample),
    {View1, [{send, Origin, Reply}]}.

-spec on_shuffle_reply(hecate_overlay_view:view(), peer(),
                       macula_frame:frame()) ->
        {hecate_overlay_view:view(), [action()]}.
on_shuffle_reply(View, _FromId, Frame) ->
    Sample = maps:get(peer_sample, Frame),
    {hecate_overlay_view:merge_shuffle(View, Sample), []}.

-spec collect_sample(hecate_overlay_view:view(), ctx()) -> [peer()].
collect_sample(View, Ctx) ->
    NA = maps:get(shuffle_active_size, Ctx, ?DEFAULT_SHUFFLE_ACTIVE),
    NP = maps:get(shuffle_passive_size, Ctx, ?DEFAULT_SHUFFLE_PASSIVE),
    hecate_overlay_view:random_active_subset(View, NA)
        ++ hecate_overlay_view:random_passive_subset(View, NP).

%%=====================================================================
%% Frame helpers
%%=====================================================================

-spec neighbor(peer(), macula_frame:neighbor_priority(), ctx()) ->
        action().
neighbor(Target, Priority, Ctx) ->
    F = macula_frame:hyparview_neighbor(
          #{realm => realm(Ctx), priority => Priority}),
    {send, Target, sign_with(F, identity(Ctx))}.

-spec build_disconnect(ctx()) -> macula_frame:frame().
build_disconnect(Ctx) ->
    sign_with(macula_frame:hyparview_disconnect(
                #{realm => realm(Ctx)}),
              identity(Ctx)).

-spec sign_with(macula_frame:frame(), macula_identity:key_pair()) ->
        macula_frame:frame().
sign_with(Frame, Identity) ->
    macula_frame:sign(Frame, Identity).

%%=====================================================================
%% Context accessors with defaults
%%=====================================================================

realm(#{realm := R}) -> R.
identity(#{identity := I}) -> I.
arwl(C) -> maps:get(arwl, C, ?DEFAULT_ARWL).
prwl(C) -> maps:get(prwl, C, ?DEFAULT_PRWL).

sets_from_list(L) -> sets:from_list(L).
