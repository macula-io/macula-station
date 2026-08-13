%% @doc CALL-frame dispatcher.
%%
%% Pure-function helpers that translate an inbound `macula_frame:call'
%% into a `macula_frame:result' or `macula_frame:call_error' by
%% looking up the procedure in a per-identity
%% `macula_handler_registry' and invoking the registered handler.
%%
%% No state of its own — runs inside whichever process delivers the
%% CALL frame (typically `macula_station_peer_observer'). Returns
%% the reply frame so the caller can send it back over the QUIC
%% connection the CALL arrived on.
%%
%% == Reply taxonomy ==
%%
%% Mapping from handler return value → reply frame:
%%
%% <table>
%%   <tr><th>Handler returns</th><th>Reply</th><th>SDK observer sees</th></tr>
%%   <tr><td>`{ok, Value}'</td>          <td>`result(payload=Value)'</td>          <td>`{ok, Value}'</td></tr>
%%   <tr><td>`Value' (anything else)</td><td>`result(payload=Value)'</td>          <td>`{ok, Value}'</td></tr>
%%   <tr><td>`{error, Reason}'</td>      <td>`call_error(0x0F, detail=Reason)'</td>  <td>`{error, Reason}'</td></tr>
%%   <tr><td>(handler crashes)</td>      <td>`call_error(temporary_relay_failure)'</td><td>`{error, temporary_relay_failure}'</td></tr>
%% </table>
%%
%% Procedure-not-found yields `call_error(unknown_next_peer)' since
%% semantically the station has no route to a handler for that
%% procedure.
-module(macula_handler_dispatch).

-export([dispatch_call/3]).

-export_type([dispatch_result/0]).

-type dispatch_result() :: macula_frame:frame().

%%====================================================================
%% Public API
%%====================================================================

%% @doc Dispatch a CALL frame against a registry.
%% Returns the reply frame. The caller is responsible for signing
%% and sending the frame on the originating connection.
%%
%% THE REGISTRY IS A PID OR A REGISTERED NAME, and this guard used to insist on
%% a pid. `macula_station_sup' registers the registry under the name
%% `macula_handler_registry' and hands that name to the peer observer, so every
%% call arriving at a real station came in here as an atom and raised
%% `function_clause'.
%%
%% IT FAILED AFTER THE LOOKUP HAD ALREADY SUCCEEDED, which is what made it hard
%% to see. `local_lookup' resolves the handler through the same atom without
%% complaint, because `gen_server:call/2' takes a registered name; only this
%% guard objected. The observer then spawns a worker per call, so the crash
%% killed the worker, `send_reply_to' never ran, and the caller waited out its
%% deadline. The container stayed healthy and the station went on reporting
%% healthy links the whole time.
%%
%% Measured on station-de-frankfurt: 11,638 of these in 24 hours, about eight a
%% minute, every one of them a `_relay.ping' that was never answered.
-spec dispatch_call(macula_frame:frame(),
                    macula_handler_registry:registry(),
                    macula_identity:pubkey()) ->
    dispatch_result().
dispatch_call(CallFrame, Registry, ResponderId)
  when is_map(CallFrame), is_pid(Registry) orelse is_atom(Registry),
       is_binary(ResponderId), byte_size(ResponderId) =:= 32 ->
    Procedure = maps:get(procedure, CallFrame),
    on_lookup(CallFrame, ResponderId,
              macula_handler_registry:lookup(Registry, Procedure)).

%%====================================================================
%% Internals
%%====================================================================

on_lookup(CallFrame, ResponderId, {error, not_found}) ->
    error_frame(CallFrame, ResponderId, 16#01);  %% unknown_next_peer
on_lookup(CallFrame, ResponderId, {ok, Handler}) ->
    Args = maps:get(payload, CallFrame),
    safe_invoke(CallFrame, ResponderId, Handler, Args).

%% Wrap handler invocation so a crash becomes a structured CALL error
%% rather than taking down the dispatching process. The peer_observer
%% must keep running; one bad handler should not take an identity
%% offline.
safe_invoke(CallFrame, ResponderId, Handler, Args) ->
    try invoke(Handler, Args) of
        %% A refusal crosses the wire as an ERROR frame with the reason
        %% in `detail', exactly as the SDK-hosted path does
        %% (macula_station_link:safe_invoke_handler/4). Before this it
        %% was `result_frame(payload={error,Reason})', so a caller saw
        %% `{ok, {error, Reason}}' — the opposite contract from an
        %% SDK-hosted handler, and quite possibly UNSENDABLE, since a
        %% RESULT payload carrying a tuple has no wire encoder clause and
        %% would fail at sign time. Code 0x0F is what this SDK reads as
        %% "the handler said no".
        {error, Reason} -> error_frame(CallFrame, ResponderId, 16#0F,
                                       format_detail(Reason));
        Reply -> result_frame(CallFrame, ResponderId, normalise(Reply))
    catch
        Class:Reason:Stack ->
            logger:warning("[handler_dispatch] handler crashed: "
                           "~p:~p~n  procedure=~p~n  stack=~p",
                           [Class, Reason,
                            maps:get(procedure, CallFrame), Stack]),
            error_frame(CallFrame, ResponderId, 16#02)  %% temporary_relay_failure
    end.

invoke(Fun, Args) when is_function(Fun, 1) ->
    Fun(Args);
invoke({M, F}, Args) when is_atom(M), is_atom(F) ->
    M:F(Args).

%% Normalise a NON-error handler return into the RESULT payload. `{ok,
%% X}' is unwrapped to `X' so the SDK observes the handler's intent
%% directly. `{error, _}' never reaches here — it is routed to an ERROR
%% frame in safe_invoke/4.
normalise({ok, Value}) -> Value;
normalise(Other)       -> Other.

result_frame(#{call_id := CallId} = _Call, ResponderId, Payload) ->
    macula_frame:result(#{
        call_id      => CallId,
        payload      => Payload,
        responded_by => ResponderId
    }).

error_frame(Call, ResponderId, Code) ->
    error_frame(Call, ResponderId, Code, undefined).

error_frame(#{call_id := CallId}, ResponderId, Code, Detail) ->
    macula_frame:call_error(#{
        call_id     => CallId,
        code        => Code,
        reported_by => ResponderId,
        detail      => Detail
    }).

%% Mirror macula_station_link:format_error_detail/1 exactly: a binary
%% reason crosses verbatim; anything else is printed and capped at 256
%% bytes so a huge term cannot bloat the frame.
format_detail(Reason) when is_binary(Reason) ->
    capped(Reason);
format_detail(Reason) ->
    capped(iolist_to_binary(io_lib:format("~0p", [Reason]))).

capped(Bin) when byte_size(Bin) =< 256 -> Bin;
capped(Bin) -> <<(binary:part(Bin, 0, 253))/binary, "...">>.
