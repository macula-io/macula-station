%% @doc Eunit suite for macula_handler_dispatch.
%%
%% Verifies the CALL frame → RESULT/ERROR frame translation and the
%% reply taxonomy from the handler's return value.
-module(macula_handler_dispatch_tests).
-include_lib("eunit/include/eunit.hrl").

%% Referenced via {?MODULE, mfa_handler} from inside a test.
-export([mfa_handler/1]).

-define(PROC, <<"_dht.put_record">>).

%%==================================================================
%% Fixture
%%==================================================================

fresh_registry() ->
    {ok, Pid} = macula_handler_registry:start_link(),
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
    Reply  = macula_handler_dispatch:dispatch_call(Frame, Reg, SelfId),
    ?assertEqual(error, macula_frame:frame_type(Reply)),
    ?assertEqual(16#01, maps:get(code, Reply)),
    ?assertEqual(SelfId, maps:get(reported_by, Reply)),
    ?assertEqual(maps:get(call_id, Frame), maps:get(call_id, Reply)),
    catch macula_handler_registry:stop(Reg).

%%==================================================================
%% Handler returns {ok, X} → result(payload=X)
%%==================================================================

handler_ok_returns_result_test() ->
    Reg = fresh_registry(),
    SelfId = <<3:256>>,
    ok = macula_handler_registry:advertise(Reg, ?PROC,
            fun(<<"args">>) -> {ok, <<"reply">>} end),
    Frame = call_frame_for(?PROC, <<"args">>),
    Reply = macula_handler_dispatch:dispatch_call(Frame, Reg, SelfId),
    ?assertEqual(result, macula_frame:frame_type(Reply)),
    ?assertEqual(<<"reply">>, maps:get(payload, Reply)),
    ?assertEqual(SelfId, maps:get(responded_by, Reply)),
    ?assertEqual(maps:get(call_id, Frame), maps:get(call_id, Reply)),
    catch macula_handler_registry:stop(Reg).

%%==================================================================
%% Handler returns {error, Reason} → result(payload={error, Reason})
%%==================================================================

handler_error_returns_result_with_error_payload_test() ->
    Reg = fresh_registry(),
    SelfId = <<3:256>>,
    ok = macula_handler_registry:advertise(Reg, ?PROC,
            fun(_) -> {error, bad_signature} end),
    Reply = macula_handler_dispatch:dispatch_call(
              call_frame_for(?PROC, <<"x">>), Reg, SelfId),
    ?assertEqual(result, macula_frame:frame_type(Reply)),
    ?assertEqual({error, bad_signature}, maps:get(payload, Reply)),
    catch macula_handler_registry:stop(Reg).

%%==================================================================
%% Handler crash → call_error(temporary_relay_failure)
%%==================================================================

handler_crash_returns_call_error_test() ->
    Reg = fresh_registry(),
    SelfId = <<3:256>>,
    ok = macula_handler_registry:advertise(Reg, ?PROC,
            fun(_) -> error(boom) end),
    Frame = call_frame_for(?PROC, <<"x">>),
    Reply = macula_handler_dispatch:dispatch_call(Frame, Reg, SelfId),
    ?assertEqual(error, macula_frame:frame_type(Reply)),
    ?assertEqual(16#02, maps:get(code, Reply)),  %% temporary_relay_failure
    ?assertEqual(maps:get(call_id, Frame), maps:get(call_id, Reply)),
    catch macula_handler_registry:stop(Reg).

%%==================================================================
%% Module-function handler form
%%==================================================================

module_function_handler_test() ->
    Reg = fresh_registry(),
    SelfId = <<3:256>>,
    ok = macula_handler_registry:advertise(Reg, ?PROC,
            {?MODULE, mfa_handler}),
    Reply = macula_handler_dispatch:dispatch_call(
              call_frame_for(?PROC, <<"input">>), Reg, SelfId),
    ?assertEqual(result, macula_frame:frame_type(Reply)),
    ?assertEqual(<<"mfa-input">>, maps:get(payload, Reply)),
    catch macula_handler_registry:stop(Reg).

mfa_handler(Bin) when is_binary(Bin) ->
    {ok, <<"mfa-", Bin/binary>>}.

%%==================================================================
%% Bare value (not wrapped in {ok, _}) goes through verbatim
%%==================================================================

bare_value_handler_returned_verbatim_test() ->
    Reg = fresh_registry(),
    SelfId = <<3:256>>,
    ok = macula_handler_registry:advertise(Reg, ?PROC,
            fun(_) -> not_found end),
    Reply = macula_handler_dispatch:dispatch_call(
              call_frame_for(?PROC, <<"x">>), Reg, SelfId),
    ?assertEqual(result, macula_frame:frame_type(Reply)),
    ?assertEqual(not_found, maps:get(payload, Reply)),
    catch macula_handler_registry:stop(Reg).

%%==================================================================
%% Addressed by REGISTERED NAME, which is how a real station calls it
%%==================================================================

%% THE BUG THIS SUITE COULD NOT SEE. Every test above hands `dispatch_call/3' a
%% pid, and the guard required one, so the suite agreed with itself and was
%% wrong about production for as long as both existed.
%%
%% `macula_station_sup' starts the registry unnamed and then registers it as
%% `macula_handler_registry', handing the NAME to the peer observer, whose own
%% record type has always said `atom() | pid() | undefined'. So every call
%% arriving at a live station came in here as an atom and raised
%% `function_clause'.
%%
%% IT FAILED AFTER THE LOOKUP HAD ALREADY SUCCEEDED, which is what hid it.
%% `local_lookup' resolves the handler through the same atom without complaint,
%% because `gen_server:call/2' takes a registered name. The observer spawns a
%% worker per call, so the crash killed the worker, the reply was never sent,
%% and the caller waited out its deadline while the container went on reporting
%% healthy. 11,638 of them in 24 hours on station-de-frankfurt, every one a
%% `_relay.ping' that was never answered.
dispatch_through_a_registered_name_test() ->
    Reg = fresh_registry(),
    Name = macula_handler_dispatch_tests_registry,
    true = erlang:register(Name, Reg),
    SelfId = <<3:256>>,
    ok = macula_handler_registry:advertise(Name, ?PROC,
            fun(<<"args">>) -> {ok, <<"pong">>} end),
    Frame = call_frame_for(?PROC, <<"args">>),
    Reply = macula_handler_dispatch:dispatch_call(Frame, Name, SelfId),
    ?assertEqual(result, macula_frame:frame_type(Reply)),
    ?assertEqual(<<"pong">>, maps:get(payload, Reply)),
    true = erlang:unregister(Name),
    catch macula_handler_registry:stop(Reg).
