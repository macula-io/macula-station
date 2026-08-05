%% @doc Eunit suite for the per-identity procedure registry.
-module(macula_handler_registry_tests).
-include_lib("eunit/include/eunit.hrl").

%% Referenced via {?MODULE, dummy_handler} from inside a test —
%% warn_unused_function would otherwise flag it.
-export([dummy_handler/1]).

-define(REG, macula_handler_registry).
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

%%==================================================================
%% ⚠ THE MIRROR: A LOOKUP MUST NOT GO THROUGH THE MAILBOX
%%==================================================================
%%
%% `lookup/2' sits on the CALL hot path — `macula_station_peer_observer'
%% calls it from inside its own `handle_info/2' for every inbound CALL frame.
%% As a `gen_server:call' it serialised every CALL on the node through one
%% process, and on 2026-08-05 a five-second stall there killed the observer
%% of station-de-frankfurt, which recreated its conns ETS mirror empty and
%% left pubsub fan-out permanently blind. These assert the read never touches
%% the registry process again.

%% The sharpest form of the claim: SUSPEND the registry, which makes any
%% `gen_server:call' into it block until timeout, and require the answer
%% anyway. This fails on the old implementation by taking five seconds and
%% then exiting, so it is a real check and not a restatement.
lookup_answers_while_the_registry_process_is_blocked_test() ->
    {ok, Pid} = ?REG:start_link(),
    Handler = fun(_Args) -> {ok, ack} end,
    ok = ?REG:advertise(Pid, ?PROC_PUT, Handler),
    true = erlang:suspend_process(Pid),
    try
        ?assertEqual({ok, Handler}, ?REG:lookup(Pid, ?PROC_PUT)),
        ?assertEqual({error, not_found}, ?REG:lookup(Pid, ?PROC_FIND))
    after
        true = erlang:resume_process(Pid),
        catch ?REG:stop(Pid)
    end.

%% And it answers fast. A second would be a mailbox; this is an ETS read.
lookup_is_not_paying_a_mailbox_round_trip_test() ->
    {ok, Pid} = ?REG:start_link(),
    ok = ?REG:advertise(Pid, ?PROC_PUT, fun(_) -> ok end),
    true = erlang:suspend_process(Pid),
    {Micros, _} = timer:tc(fun() -> ?REG:lookup(Pid, ?PROC_PUT) end),
    true = erlang:resume_process(Pid),
    catch ?REG:stop(Pid),
    ?assert(Micros < 100_000).

%% Unadvertise has to clear the mirror too, or a revoked procedure stays
%% callable — which would be worse than the bug being fixed.
unadvertise_clears_the_mirror_test() ->
    {ok, Pid} = ?REG:start_link(),
    ok = ?REG:advertise(Pid, ?PROC_PUT, fun(_) -> ok end),
    ok = ?REG:unadvertise(Pid, ?PROC_PUT),
    true = erlang:suspend_process(Pid),
    try ?assertEqual({error, not_found}, ?REG:lookup(Pid, ?PROC_PUT))
    after true = erlang:resume_process(Pid), catch ?REG:stop(Pid)
    end.

%% ⚠ THE TABLE IS NAMED, SO IT IS ONE PER BEAM. A second registry must not
%% read the first one's handlers; it falls back to its own mailbox. Without
%% the owner check this returns the wrong handler, silently.
a_second_registry_does_not_read_the_first_ones_table_test() ->
    {ok, A} = ?REG:start_link(),
    {ok, B} = ?REG:start_link(),
    ok = ?REG:advertise(A, ?PROC_PUT, fun(_) -> from_a end),
    ?assertEqual({error, not_found}, ?REG:lookup(B, ?PROC_PUT)),
    catch ?REG:stop(A),
    catch ?REG:stop(B).
