%% @doc Eunit suite for the per-identity procedure registry.
-module(hecate_handler_registry_tests).
-include_lib("eunit/include/eunit.hrl").

%% Referenced via {?MODULE, dummy_handler} from inside a test —
%% warn_unused_function would otherwise flag it.
-export([dummy_handler/1]).

-define(REG, hecate_handler_registry).
-define(PROC_PUT,  <<"_dht.put_record">>).
-define(PROC_FIND, <<"_dht.find_record">>).

with_registry(Body) ->
    {setup,
     fun() -> {ok, Pid} = ?REG:start_link(), Pid end,
     fun(Pid) -> catch ?REG:stop(Pid) end,
     fun(Pid) -> Body(Pid) end}.

%%==================================================================
%% advertise + lookup
%%==================================================================

empty_registry_returns_not_found_test_() ->
    with_registry(fun(Pid) ->
        ?_test(?assertEqual({error, not_found},
                            ?REG:lookup(Pid, ?PROC_PUT)))
    end).

advertise_then_lookup_returns_handler_test_() ->
    with_registry(fun(Pid) ->
        ?_test(begin
            Handler = fun(_Args) -> {ok, ack} end,
            ok = ?REG:advertise(Pid, ?PROC_PUT, Handler),
            ?assertEqual({ok, Handler}, ?REG:lookup(Pid, ?PROC_PUT))
        end)
    end).

advertise_replaces_existing_handler_test_() ->
    with_registry(fun(Pid) ->
        ?_test(begin
            H1 = fun(_) -> {ok, first} end,
            H2 = fun(_) -> {ok, second} end,
            ok = ?REG:advertise(Pid, ?PROC_PUT, H1),
            ok = ?REG:advertise(Pid, ?PROC_PUT, H2),
            ?assertEqual({ok, H2}, ?REG:lookup(Pid, ?PROC_PUT))
        end)
    end).

advertise_accepts_module_function_tuple_test_() ->
    with_registry(fun(Pid) ->
        ?_test(begin
            ok = ?REG:advertise(Pid, ?PROC_PUT, {?MODULE, dummy_handler}),
            ?assertEqual({ok, {?MODULE, dummy_handler}},
                         ?REG:lookup(Pid, ?PROC_PUT))
        end)
    end).

%% Used by advertise_accepts_module_function_tuple_test_ via {M,F}.
dummy_handler(_Args) -> {ok, dummy}.

unadvertise_clears_handler_test_() ->
    with_registry(fun(Pid) ->
        ?_test(begin
            ok = ?REG:advertise(Pid, ?PROC_PUT, fun(_) -> ok end),
            ok = ?REG:unadvertise(Pid, ?PROC_PUT),
            ?assertEqual({error, not_found},
                         ?REG:lookup(Pid, ?PROC_PUT))
        end)
    end).

list_returns_advertised_procedures_test_() ->
    with_registry(fun(Pid) ->
        ?_test(begin
            ok = ?REG:advertise(Pid, ?PROC_PUT, fun(_) -> ok end),
            ok = ?REG:advertise(Pid, ?PROC_FIND, fun(_) -> ok end),
            Got = lists:sort(?REG:list(Pid)),
            ?assertEqual(lists:sort([?PROC_PUT, ?PROC_FIND]), Got)
        end)
    end).

%%==================================================================
%% Multi-instance — distinct identity registries coexist
%%==================================================================

distinct_registries_isolate_handlers_test() ->
    {ok, A} = ?REG:start_link(),
    {ok, B} = ?REG:start_link(),
    try
        ok = ?REG:advertise(A, ?PROC_PUT, fun(_) -> {ok, from_a} end),
        ?assertEqual({error, not_found}, ?REG:lookup(B, ?PROC_PUT)),
        ?assertMatch({ok, _},            ?REG:lookup(A, ?PROC_PUT))
    after
        catch ?REG:stop(A),
        catch ?REG:stop(B)
    end.
