%% EUnit tests for hecate_pubsub_registry.
-module(hecate_pubsub_registry_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

realm()   -> crypto:strong_rand_bytes(32).
id(N)     -> <<N:256>>.
keypair() -> hecate_identity:generate().

setup() ->
    {ok, Sup} = hecate_overlay_sup:start_link(),
    unlink(Sup),
    Sup.

cleanup(Sup) ->
    case is_process_alive(Sup) of
        true  -> catch proc_lib:stop(Sup, shutdown, 5000), ok;
        false -> ok
    end.

%%---------------------------------------------------------------------
%% Generator
%%---------------------------------------------------------------------

registry_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun(_) -> ?_test(register_creates_server()) end,
         fun(_) -> ?_test(register_idempotent_returns_same_pid()) end,
         fun(_) -> ?_test(register_after_child_death_yields_fresh_pid()) end,
         fun(_) -> ?_test(lookup_unknown_realm_returns_not_found()) end,
         fun(_) -> ?_test(lookup_after_register_returns_pid()) end,
         fun(_) -> ?_test(child_death_clears_map()) end,
         fun(_) -> ?_test(dispatch_subscribe_routes_to_server()) end,
         fun(_) -> ?_test(dispatch_event_returns_local_subscribers()) end,
         fun(_) -> ?_test(dispatch_unknown_realm_returns_not_found()) end,
         fun(_) -> ?_test(dispatch_after_child_death_returns_not_found()) end,
         fun(_) -> ?_test(distinct_realms_isolated()) end,
         fun(_) -> ?_test(shutdown_propagates_to_children()) end
     ]}.

%%---------------------------------------------------------------------
%% Register / lookup
%%---------------------------------------------------------------------

register_creates_server() ->
    R  = realm(),
    Kp = keypair(),
    {ok, Pid} = hecate_pubsub_registry:register(R, Kp),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ?assertEqual(R, hecate_pubsub_server:realm(Pid)).

register_idempotent_returns_same_pid() ->
    R  = realm(),
    Kp = keypair(),
    {ok, Pid1} = hecate_pubsub_registry:register(R, Kp),
    {ok, Pid2} = hecate_pubsub_registry:register(R, Kp),
    ?assertEqual(Pid1, Pid2).

register_after_child_death_yields_fresh_pid() ->
    R  = realm(),
    Kp = keypair(),
    {ok, Pid1} = hecate_pubsub_registry:register(R, Kp),
    %% Kill the server abruptly. The DOWN message will reach the
    %% registry; wait for it to be processed by polling lookup.
    exit(Pid1, kill),
    wait_until(fun() ->
        hecate_pubsub_registry:lookup(R) =:= {error, not_found}
    end, 1000),
    {ok, Pid2} = hecate_pubsub_registry:register(R, Kp),
    ?assertNotEqual(Pid1, Pid2),
    ?assert(is_process_alive(Pid2)).

lookup_unknown_realm_returns_not_found() ->
    ?assertEqual({error, not_found},
                 hecate_pubsub_registry:lookup(realm())).

lookup_after_register_returns_pid() ->
    R  = realm(),
    Kp = keypair(),
    {ok, Pid} = hecate_pubsub_registry:register(R, Kp),
    ?assertEqual({ok, Pid}, hecate_pubsub_registry:lookup(R)).

%%---------------------------------------------------------------------
%% Child lifecycle
%%---------------------------------------------------------------------

child_death_clears_map() ->
    R  = realm(),
    Kp = keypair(),
    {ok, Pid} = hecate_pubsub_registry:register(R, Kp),
    exit(Pid, kill),
    wait_until(fun() ->
        hecate_pubsub_registry:lookup(R) =:= {error, not_found}
    end, 1000),
    ?assertEqual({error, not_found}, hecate_pubsub_registry:lookup(R)).

%%---------------------------------------------------------------------
%% Dispatch
%%---------------------------------------------------------------------

dispatch_subscribe_routes_to_server() ->
    R     = realm(),
    Kp    = keypair(),
    SubKp = keypair(),
    SubId = hecate_identity:public(SubKp),
    {ok, Pid} = hecate_pubsub_registry:register(R, Kp),

    Frame = hecate_frame:sign(hecate_frame:subscribe(#{
        topic      => <<"news">>,
        realm      => R,
        subscriber => SubId
    }), SubKp),

    {ok, Subs} = hecate_pubsub_registry:dispatch_frame(R, SubId, Frame),
    ?assertEqual([], Subs),
    ?assert(hecate_pubsub_server:is_subscribed(Pid, <<"news">>, SubId)).

dispatch_event_returns_local_subscribers() ->
    R     = realm(),
    Kp    = keypair(),
    SubKp = keypair(),
    SubId = hecate_identity:public(SubKp),
    {ok, _Pid} = hecate_pubsub_registry:register(R, Kp),

    %% Subscribe via the registry's dispatch path.
    SubF = hecate_frame:sign(hecate_frame:subscribe(#{
        topic      => <<"news">>,
        realm      => R,
        subscriber => SubId
    }), SubKp),
    {ok, []} = hecate_pubsub_registry:dispatch_frame(R, SubId, SubF),

    %% Now an inbound EVENT must match the local subscriber.
    EventF = hecate_frame:sign(hecate_frame:event(#{
        topic         => <<"news">>,
        realm         => R,
        publisher     => hecate_identity:public(Kp),
        seq           => 1,
        payload       => <<"hello">>,
        delivered_via => plumtree
    }), Kp),

    {ok, Matched} = hecate_pubsub_registry:dispatch_frame(
                      R, hecate_identity:public(Kp), EventF),
    ?assertEqual([SubId], Matched).

dispatch_unknown_realm_returns_not_found() ->
    R   = realm(),
    Kp  = keypair(),
    Pub = hecate_identity:public(Kp),
    Frame = hecate_frame:sign(hecate_frame:subscribe(#{
        topic      => <<"x">>,
        realm      => R,
        subscriber => Pub
    }), Kp),
    ?assertEqual({error, not_found},
                 hecate_pubsub_registry:dispatch_frame(R, Pub, Frame)).

dispatch_after_child_death_returns_not_found() ->
    R     = realm(),
    Kp    = keypair(),
    {ok, Pid} = hecate_pubsub_registry:register(R, Kp),
    exit(Pid, kill),
    wait_until(fun() ->
        hecate_pubsub_registry:lookup(R) =:= {error, not_found}
    end, 1000),
    Pub = hecate_identity:public(Kp),
    Frame = hecate_frame:sign(hecate_frame:subscribe(#{
        topic      => <<"x">>,
        realm      => R,
        subscriber => Pub
    }), Kp),
    ?assertEqual({error, not_found},
                 hecate_pubsub_registry:dispatch_frame(R, Pub, Frame)).

%%---------------------------------------------------------------------
%% Multi-realm isolation
%%---------------------------------------------------------------------

distinct_realms_isolated() ->
    R1  = realm(),
    R2  = realm(),
    Kp  = keypair(),
    {ok, P1} = hecate_pubsub_registry:register(R1, Kp),
    {ok, P2} = hecate_pubsub_registry:register(R2, Kp),
    ?assertNotEqual(P1, P2),

    ok = hecate_pubsub_server:subscribe(P1, <<"t">>, id(1)),
    ?assertEqual(1, hecate_pubsub_server:subscriber_count(P1)),
    ?assertEqual(0, hecate_pubsub_server:subscriber_count(P2)),

    %% Event tagged with R2 must not match P1's subscribers.
    EventF = hecate_frame:sign(hecate_frame:event(#{
        topic         => <<"t">>,
        realm         => R2,
        publisher     => hecate_identity:public(Kp),
        seq           => 1,
        payload       => <<"x">>,
        delivered_via => plumtree
    }), Kp),
    {ok, Matched} = hecate_pubsub_registry:dispatch_frame(
                      R2, hecate_identity:public(Kp), EventF),
    ?assertEqual([], Matched).

%%---------------------------------------------------------------------
%% Shutdown propagation
%%---------------------------------------------------------------------

shutdown_propagates_to_children() ->
    R1 = realm(),
    R2 = realm(),
    Kp = keypair(),
    {ok, P1} = hecate_pubsub_registry:register(R1, Kp),
    {ok, P2} = hecate_pubsub_registry:register(R2, Kp),

    Ref1 = erlang:monitor(process, P1),
    Ref2 = erlang:monitor(process, P2),

    %% Graceful shutdown via sys (works regardless of caller identity).
    Sup = whereis(hecate_overlay_sup),
    ok  = proc_lib:stop(Sup, shutdown, 5000),

    %% After proc_lib:stop returns, the supervisor's terminate cascade
    %% has already run; DOWN messages for P1/P2 should be in the
    %% mailbox or arriving imminently.
    receive {'DOWN', Ref1, process, P1, _} -> ok
    after 1000 -> ?assert(false) end,
    receive {'DOWN', Ref2, process, P2, _} -> ok
    after 1000 -> ?assert(false) end,

    ?assertNot(is_process_alive(P1)),
    ?assertNot(is_process_alive(P2)),
    ?assertEqual(undefined, whereis(hecate_pubsub_registry)),
    ?assertEqual(undefined, whereis(hecate_pubsub_server_sup)).

%%---------------------------------------------------------------------
%% Polling helper
%%---------------------------------------------------------------------

wait_until(_Pred, Budget) when Budget =< 0 ->
    erlang:error(wait_until_timeout);
wait_until(Pred, Budget) ->
    case Pred() of
        true  -> ok;
        false ->
            timer:sleep(10),
            wait_until(Pred, Budget - 10)
    end.
