%% @doc Station-integration tests — supervisor lifecycle, app
%% callback, and the config-driven `run/0,1' orchestrator.
-module(macula_bootstrap_integration_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Supervisor
%%==================================================================

sup_with_no_responder_env_starts_empty_test() ->
    with_trapped_exits(fun() ->
        ok = set_env(responder, disabled),
        run_sup(fun(Pid) ->
            ?assertEqual([], supervisor:which_children(Pid))
        end)
    end).

sup_with_missing_env_starts_empty_test() ->
    with_trapped_exits(fun() ->
        ok = unset_env(responder),
        run_sup(fun(Pid) ->
            ?assertEqual([], supervisor:which_children(Pid))
        end),
        set_env(responder, disabled)
    end).

sup_with_responder_config_starts_responder_test() ->
    with_trapped_exits(fun() ->
        Opener = fun() ->
                     gen_udp:open(0, [inet6, binary, {active, once},
                                      {ip, {0, 0, 0, 0, 0, 0, 0, 1}}])
                 end,
        Cfg = #{
            node_id       => crypto:strong_rand_bytes(32),
            port          => 7000,
            tier          => 0,
            socket_opener => Opener
        },
        ok = set_env(responder, Cfg),
        try
            run_sup(fun(Pid) ->
                [Child] = supervisor:which_children(Pid),
                ?assertMatch({macula_bootstrap_via_mdns_responder, _, worker, _},
                             Child)
            end)
        after
            set_env(responder, disabled)
        end
    end).

%%==================================================================
%% Application callback
%%==================================================================

app_ensure_all_started_and_stop_test() ->
    ok = set_env(responder, disabled),
    {ok, Started} = application:ensure_all_started(macula_bootstrap),
    ?assert(lists:member(macula_bootstrap, Started)),
    Sup = whereis(macula_bootstrap_sup),
    ?assert(is_pid(Sup)),
    ok = application:stop(macula_bootstrap),
    ?assertEqual(undefined, whereis(macula_bootstrap_sup)).

%%==================================================================
%% run/0 + run/1
%%==================================================================

run_with_no_tiers_returns_no_tiers_test() ->
    ?assertEqual({error, no_tiers}, macula_bootstrap:run(#{discoverers => []})).

run_with_missing_tiers_key_returns_no_tiers_test() ->
    ?assertEqual({error, no_tiers}, macula_bootstrap:run(#{})).

run_from_explicit_config_reaches_tier_e_test() ->
    Urls = [signed_url() || _ <- lists:seq(1, 3)],
    Cfg = #{
        discoverers => [{macula_bootstrap_via_operator_paste, #{peer_urls => Urls}}],
        cascade_opts => #{min_peers => 3, timeout_ms => 500}
    },
    {ok, Peers} = macula_bootstrap:run(Cfg),
    ?assertEqual(3, length(Peers)),
    [?assertEqual(via_operator_paste, maps:get(strategy, P)) || P <- Peers].

run_from_application_env_test() ->
    Urls = [signed_url() || _ <- lists:seq(1, 3)],
    Tiers = [{macula_bootstrap_via_operator_paste, #{peer_urls => Urls}}],
    ok = set_env(discoverers, Tiers),
    ok = set_env(cascade_opts, #{min_peers => 3, timeout_ms => 500}),
    try
        {ok, Peers} = macula_bootstrap:run(),
        ?assertEqual(3, length(Peers))
    after
        unset_env(discoverers),
        unset_env(cascade_opts)
    end.

run_empty_env_returns_no_tiers_test() ->
    ok = unset_env(discoverers),
    ok = unset_env(cascade_opts),
    ?assertEqual({error, no_tiers}, macula_bootstrap:run()).

%%==================================================================
%% Helpers
%%==================================================================

set_env(Key, Value) ->
    application:set_env(macula_bootstrap, Key, Value),
    ok.

unset_env(Key) ->
    application:unset_env(macula_bootstrap, Key),
    ok.

with_trapped_exits(Fun) ->
    Prev = process_flag(trap_exit, true),
    try Fun()
    after
        drain_exits(),
        process_flag(trap_exit, Prev)
    end.

run_sup(Body) ->
    {ok, Pid} = macula_bootstrap_sup:start_link(),
    try Body(Pid)
    after
        exit(Pid, shutdown),
        wait_down(Pid)
    end.

wait_down(Pid) ->
    Ref = monitor(process, Pid),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 1000 -> demonitor(Ref, [flush]), ok
    end.

drain_exits() ->
    receive {'EXIT', _, _} -> drain_exits() after 0 -> ok end.

signed_url() ->
    Kp = macula_identity:generate(),
    Record = macula_record:sign(
               macula_record:node_record(
                 macula_identity:public(Kp), [], 0), Kp),
    macula_bootstrap_via_operator_paste_peer_url:encode(Record, []).
