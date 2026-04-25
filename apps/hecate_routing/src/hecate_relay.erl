%% @doc Per-hop CALL relay logic (Part 3 §6.6).
%%
%% Given an incoming signed CALL frame plus a relaying context
%% (the relay's own NodeId, signing identity, and a callback that
%% answers "is this peer alive in my routing table?"), decide one
%% of three outcomes:
%%
%% <ul>
%%   <li><strong>`{forward, NextHop, NewFrame}'</strong> — advance
%%       the source-route header, re-encode it into the CALL, and
%%       hand the result back so the I/O layer can write it to
%%       `NextHop'.</li>
%%   <li><strong>`{deliver_local, Frame}'</strong> — we are the
%%       destination; deliver to the local CALL handler.</li>
%%   <li><strong>`{reply_error, ErrorFrame, OriginatorId}'</strong>
%%       — refuse and ship a <em>signed</em> BOLT#4 error back to
%%       the originator. The frame is fully signed with our
%%       identity so downstream hops cannot forge "not my
%%       fault".</li>
%% </ul>
%%
%% Per §6.6 the per-hop checks are, in order:
%%
%% <ol>
%%   <li>Source-route header decodes + path_hash verifies (handled
%%       by `hecate_source_route:decode/1' inline).</li>
%%   <li>Deadline not expired — else `expiry_too_soon'.</li>
%%   <li>No duplicate hops — else `loop_detected'.</li>
%%   <li>`current_hop_id(SR)' matches our 16-byte NodeId prefix —
%%       else `invalid_path_header'.</li>
%%   <li>If we are the final hop, deliver locally; otherwise look
%%       up the next hop and check its SWIM state. Missing or
%%       suspect/failed → `unknown_next_peer' (signed, with
%%       `offending_hop' set).</li>
%% </ol>
%%
%% This module is pure: every observable side effect is encoded as
%% a return value. The wrapper that actually transmits the
%% returned frame lands in Session 4.7+ when the orchestrator
%% gains its full I/O surface.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §6.6;
%% plans/PLAN_MACULA_V2_PART4_LIFECYCLE.md §6.1, §6.3;
%% plans/PLAN_PHASE_4_BREAKDOWN.md Session 4.6.
-module(hecate_relay).

-export([process_call/2]).

-export_type([context/0, action/0]).

-type alive_predicate() ::
        fun((hecate_identity:pubkey()) -> boolean()).

-type context() :: #{
    self_id  := hecate_identity:pubkey(),
    identity := hecate_identity:key_pair(),
    is_alive := alive_predicate(),
    now_ms   => non_neg_integer()
}.

-type action() ::
      {forward, NextHop :: hecate_identity:pubkey(),
                hecate_frame:frame()}
    | {deliver_local, hecate_frame:frame()}
    | {reply_error, hecate_frame:frame(),
                    OriginatorId :: hecate_identity:pubkey() | undefined}.

%%=====================================================================
%% Public API
%%=====================================================================

%% @doc Process an inbound CALL frame and return the next action.
-spec process_call(hecate_frame:frame(), context()) -> action().
process_call(#{frame_type := call} = Frame, Context) ->
    process_source_route(maps:get(source_route, Frame, <<>>),
                         Frame, Context).

%%=====================================================================
%% Pipeline
%%=====================================================================

-spec process_source_route(binary(), hecate_frame:frame(), context()) ->
        action().
process_source_route(<<>>, Frame, Context) ->
    reply_error(Frame, invalid_path_header, undefined, Context);
process_source_route(SrBin, Frame, Context) ->
    process_decoded(hecate_source_route:decode(SrBin), Frame, Context).

-spec process_decoded({ok, hecate_source_route:header()}
                    | {error, hecate_source_route:decode_error()},
                      hecate_frame:frame(), context()) -> action().
process_decoded({error, _Reason}, Frame, Context) ->
    reply_error(Frame, invalid_path_header, undefined, Context);
process_decoded({ok, SR}, Frame, Context) ->
    check_deadline(SR, Frame, Context).

-spec check_deadline(hecate_source_route:header(), hecate_frame:frame(),
                     context()) -> action().
check_deadline(SR, Frame, Context) ->
    Now = maps:get(now_ms, Context, erlang:system_time(millisecond)),
    advance_after_deadline(hecate_source_route:deadline(SR) =< Now,
                           SR, Frame, Context).

-spec advance_after_deadline(boolean(), hecate_source_route:header(),
                             hecate_frame:frame(), context()) -> action().
advance_after_deadline(true, _SR, Frame, Context) ->
    reply_error(Frame, expiry_too_soon, undefined, Context);
advance_after_deadline(false, SR, Frame, Context) ->
    check_loop(SR, Frame, Context).

-spec check_loop(hecate_source_route:header(), hecate_frame:frame(),
                 context()) -> action().
check_loop(SR, Frame, Context) ->
    Hops = hecate_source_route:hops(SR),
    detect_loop(Hops, length(Hops), length(lists:usort(Hops)),
                SR, Frame, Context).

-spec detect_loop([hecate_source_route:hop_id()], non_neg_integer(),
                  non_neg_integer(), hecate_source_route:header(),
                  hecate_frame:frame(), context()) -> action().
detect_loop(_Hops, N, U, _SR, Frame, Context) when N =/= U ->
    reply_error(Frame, loop_detected, undefined, Context);
detect_loop(_Hops, _N, _U, SR, Frame, Context) ->
    check_position(SR, Frame, Context).

-spec check_position(hecate_source_route:header(), hecate_frame:frame(),
                     context()) -> action().
check_position(SR, Frame, Context) ->
    SelfPrefix = hecate_source_route:truncate_hop(maps:get(self_id, Context)),
    inspect_current_hop(hecate_source_route:current_hop_id(SR),
                        SelfPrefix, SR, Frame, Context).

-spec inspect_current_hop({ok, hecate_source_route:hop_id()} | error,
                          hecate_source_route:hop_id(),
                          hecate_source_route:header(),
                          hecate_frame:frame(), context()) -> action().
inspect_current_hop(error, _SelfPrefix, _SR, Frame, Context) ->
    reply_error(Frame, invalid_path_header, undefined, Context);
inspect_current_hop({ok, Expected}, SelfPrefix, _SR, Frame, Context)
  when Expected =/= SelfPrefix ->
    reply_error(Frame, invalid_path_header, undefined, Context);
inspect_current_hop({ok, _Match}, _SelfPrefix, SR, Frame, Context) ->
    dispatch_or_forward(hecate_source_route:is_final_hop(SR),
                        SR, Frame, Context).

-spec dispatch_or_forward(boolean(), hecate_source_route:header(),
                          hecate_frame:frame(), context()) -> action().
dispatch_or_forward(true, _SR, Frame, _Context) ->
    {deliver_local, Frame};
dispatch_or_forward(false, SR, Frame, Context) ->
    {ok, NextHopPrefix} = hecate_source_route:next_hop_id(SR),
    %% The source-route header carries truncated 16-byte prefixes,
    %% but the alive-predicate works on full 32-byte NodeIds. The
    %% caller's `is_alive' callback is responsible for resolving
    %% prefix → full NodeId via the routing table; we pass the
    %% prefix and trust the lookup logic on the other side.
    AliveFn = maps:get(is_alive, Context),
    forward_if_alive(AliveFn(NextHopPrefix), NextHopPrefix, SR, Frame, Context).

-spec forward_if_alive(boolean(), hecate_source_route:hop_id(),
                       hecate_source_route:header(),
                       hecate_frame:frame(), context()) -> action().
forward_if_alive(false, NextHop, _SR, Frame, Context) ->
    reply_error(Frame, unknown_next_peer, NextHop, Context);
forward_if_alive(true, NextHop, SR, Frame, _Context) ->
    SR1    = hecate_source_route:advance(SR),
    SrBin1 = hecate_source_route:encode(SR1),
    {forward, NextHop, Frame#{source_route := SrBin1}}.

%%=====================================================================
%% Signed BOLT#4 error responses
%%=====================================================================

-spec reply_error(hecate_frame:frame(), hecate_bolt4:name(),
                  hecate_source_route:hop_id() | undefined,
                  context()) -> action().
reply_error(CallFrame, Code, OffendingHop, Context) ->
    ErrorFrame = build_signed_error(CallFrame, Code, OffendingHop, Context),
    Originator = maps:get(caller, CallFrame, undefined),
    {reply_error, ErrorFrame, Originator}.

-spec build_signed_error(hecate_frame:frame(), hecate_bolt4:name(),
                         hecate_source_route:hop_id() | undefined,
                         context()) -> hecate_frame:frame().
build_signed_error(CallFrame, Code, OffendingHop, Context) ->
    SelfId   = maps:get(self_id, Context),
    Identity = maps:get(identity, Context),
    Base = #{
        call_id     => maps:get(call_id, CallFrame, <<0:128>>),
        code        => hecate_bolt4:code(Code),
        reported_by => SelfId
    },
    WithHop  = maybe_add_hop(Base, OffendingHop),
    WithSr   = maybe_add_partial(WithHop, CallFrame),
    hecate_frame:sign(hecate_frame:call_error(WithSr), Identity).

-spec maybe_add_hop(map(), hecate_source_route:hop_id() | undefined) -> map().
maybe_add_hop(Spec, undefined) -> Spec;
maybe_add_hop(Spec, <<_:128>> = Hop) ->
    %% The SDK ERROR frame demands a full 32-byte offending_hop;
    %% source routes carry 16-byte prefixes. Zero-pad so the
    %% receiver can correlate the prefix from the first 16 bytes
    %% without further decoding work.
    Spec#{offending_hop => <<Hop/binary, 0:128>>}.

-spec maybe_add_partial(map(), hecate_frame:frame()) -> map().
maybe_add_partial(Spec, CallFrame) ->
    case maps:get(source_route, CallFrame, undefined) of
        undefined -> Spec;
        <<>>      -> Spec;
        SrBin     -> Spec#{source_route_partial => SrBin}
    end.
