%% @doc CALL-frame dispatcher.
%%
%% Pure-function helpers that translate an inbound `macula_frame:call'
%% into a `macula_frame:result' or `macula_frame:call_error' by
%% looking up the procedure in a per-identity
%% `hecate_handler_registry' and invoking the registered handler.
%%
%% No state of its own — runs inside whichever process delivers the
%% CALL frame (typically `hecate_station_peer_observer'). Returns
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
%%   <tr><td>`{error, Reason}'</td>      <td>`result(payload={error,Reason})'</td> <td>`{ok, {error, Reason}}'</td></tr>
%%   <tr><td>(handler crashes)</td>      <td>`call_error(temporary_relay_failure)'</td><td>`{error, temporary_relay_failure}'</td></tr>
%% </table>
%%
%% Procedure-not-found yields `call_error(unknown_next_peer)' since
%% semantically the station has no route to a handler for that
%% procedure.
-module(hecate_handler_dispatch).

-export([dispatch_call/3]).

-export_type([dispatch_result/0]).

-type dispatch_result() :: macula_frame:frame().

%%====================================================================
%% Public API
%%====================================================================

%% @doc Dispatch a CALL frame against a registry.
%% Returns the reply frame. The caller is responsible for signing
%% and sending the frame on the originating connection.
-spec dispatch_call(macula_frame:frame(),
                    pid(),
                    macula_identity:pubkey()) ->
    dispatch_result().
dispatch_call(CallFrame, RegistryPid, ResponderId)
  when is_map(CallFrame), is_pid(RegistryPid),
       is_binary(ResponderId), byte_size(ResponderId) =:= 32 ->
    Procedure = maps:get(procedure, CallFrame),
    on_lookup(CallFrame, ResponderId,
              hecate_handler_registry:lookup(RegistryPid, Procedure)).

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

%% Normalise handler return into the value to put in the RESULT
%% payload. `{ok, X}' is unwrapped to `X' so the SDK side observes
%% the handler's intent directly. `{error, _}' is preserved verbatim
%% so the caller can pattern-match.
normalise({ok, Value})        -> Value;
normalise({error, _} = Error) -> Error;
normalise(Other)              -> Other.

result_frame(#{call_id := CallId} = _Call, ResponderId, Payload) ->
    macula_frame:result(#{
        call_id      => CallId,
        payload      => Payload,
        responded_by => ResponderId
    }).

error_frame(#{call_id := CallId}, ResponderId, Code) ->
    macula_frame:call_error(#{
        call_id     => CallId,
        code        => Code,
        reported_by => ResponderId
    }).
