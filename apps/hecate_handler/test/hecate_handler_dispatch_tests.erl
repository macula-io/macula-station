%% @doc Eunit suite for hecate_handler_dispatch.
%%
%% Verifies the CALL frame → RESULT/ERROR frame translation and the
%% reply taxonomy from the handler's return value.
-module(hecate_handler_dispatch_tests).
-include_lib("eunit/include/eunit.hrl").

%% Referenced via {?MODULE, mfa_handler} from inside a test.
-export([mfa_handler/1]).

-define(PROC, <<"_dht.put_record">>).

%%==================================================================
%% Fixture
%%==================================================================

fresh_registry() ->
    {ok, Pid} = hecate_handler_registry:start_link(),
    Pid.

call_frame_for(Procedure, Payload) ->
    Caller = <<1:256>>,
    Realm  = <<2:256>>,
    macula_frame:call(#{
        call_id     => crypto:strong_rand_bytes(16),
        procedure   => Procedure,
        realm       => Realm,
        payload     => Payload,
        deadline_ms => erlang:system_time(millisecond) + 5_000,
        caller      => Caller
    }).

%%==================================================================
%% Procedure not advertised → call_error(unknown_next_peer)
%%==================================================================

unknown_procedure_returns_call_error_test() ->
    Reg = fresh_registry(),
    SelfId = <<3:256>>,
    Frame  = call_frame_for(?PROC, <<"args">>),
    Reply  = hecate_handler_dispatch:dispatch_call(Frame, Reg, SelfId),
    ?assertEqual(error, macula_frame:frame_type(Reply)),
    ?assertEqual(16#01, maps:get(code, Reply)),
    ?assertEqual(SelfId, maps:get(reported_by, Reply)),
    ?assertEqual(maps:get(call_id, Frame), maps:get(call_id, Reply)),
    catch hecate_handler_registry:stop(Reg).

%%==================================================================
%% Handler returns {ok, X} → result(payload=X)
%%==================================================================

handler_ok_returns_result_test() ->
    Reg = fresh_registry(),
    SelfId = <<3:256>>,
    ok = hecate_handler_registry:advertise(Reg, ?PROC,
            fun(<<"args">>) -> {ok, <<"reply">>} end),
    Frame = call_frame_for(?PROC, <<"args">>),
    Reply = hecate_handler_dispatch:dispatch_call(Frame, Reg, SelfId),
    ?assertEqual(result, macula_frame:frame_type(Reply)),
    ?assertEqual(<<"reply">>, maps:get(payload, Reply)),
    ?assertEqual(SelfId, maps:get(responded_by, Reply)),
    ?assertEqual(maps:get(call_id, Frame), maps:get(call_id, Reply)),
    catch hecate_handler_registry:stop(Reg).

%%==================================================================
%% Handler returns {error, Reason} → result(payload={error, Reason})
%%==================================================================

handler_error_returns_result_with_error_payload_test() ->
    Reg = fresh_registry(),
    SelfId = <<3:256>>,
    ok = hecate_handler_registry:advertise(Reg, ?PROC,
            fun(_) -> {error, bad_signature} end),
    Reply = hecate_handler_dispatch:dispatch_call(
              call_frame_for(?PROC, <<"x">>), Reg, SelfId),
    ?assertEqual(result, macula_frame:frame_type(Reply)),
    ?assertEqual({error, bad_signature}, maps:get(payload, Reply)),
    catch hecate_handler_registry:stop(Reg).

%%==================================================================
%% Handler crash → call_error(temporary_relay_failure)
%%==================================================================

handler_crash_returns_call_error_test() ->
    Reg = fresh_registry(),
    SelfId = <<3:256>>,
    ok = hecate_handler_registry:advertise(Reg, ?PROC,
            fun(_) -> error(boom) end),
    Frame = call_frame_for(?PROC, <<"x">>),
    Reply = hecate_handler_dispatch:dispatch_call(Frame, Reg, SelfId),
    ?assertEqual(error, macula_frame:frame_type(Reply)),
    ?assertEqual(16#02, maps:get(code, Reply)),  %% temporary_relay_failure
    ?assertEqual(maps:get(call_id, Frame), maps:get(call_id, Reply)),
    catch hecate_handler_registry:stop(Reg).

%%==================================================================
%% Module-function handler form
%%==================================================================

module_function_handler_test() ->
    Reg = fresh_registry(),
    SelfId = <<3:256>>,
    ok = hecate_handler_registry:advertise(Reg, ?PROC,
            {?MODULE, mfa_handler}),
    Reply = hecate_handler_dispatch:dispatch_call(
              call_frame_for(?PROC, <<"input">>), Reg, SelfId),
    ?assertEqual(result, macula_frame:frame_type(Reply)),
    ?assertEqual(<<"mfa-input">>, maps:get(payload, Reply)),
    catch hecate_handler_registry:stop(Reg).

mfa_handler(Bin) when is_binary(Bin) ->
    {ok, <<"mfa-", Bin/binary>>}.

%%==================================================================
%% Bare value (not wrapped in {ok, _}) goes through verbatim
%%==================================================================

bare_value_handler_returned_verbatim_test() ->
    Reg = fresh_registry(),
    SelfId = <<3:256>>,
    ok = hecate_handler_registry:advertise(Reg, ?PROC,
            fun(_) -> not_found end),
    Reply = hecate_handler_dispatch:dispatch_call(
              call_frame_for(?PROC, <<"x">>), Reg, SelfId),
    ?assertEqual(result, macula_frame:frame_type(Reply)),
    ?assertEqual(not_found, maps:get(payload, Reply)),
    catch hecate_handler_registry:stop(Reg).
