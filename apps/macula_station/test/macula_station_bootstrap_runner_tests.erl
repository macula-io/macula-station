-module(macula_station_bootstrap_runner_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Happy path — cascade yields peers, runner ingests them.
%%==================================================================

runner_ingests_cascade_peers_test_() ->
    {setup, fun start_dht/0, fun stop_dht/1, fun({Dht}) ->
        fun() ->
            Peers = macula_station_stub_tier:stub_peers(3),
            Cfg   = stub_cfg(Peers),
            {ok, #{peers := Out, summary := Summary}} =
                macula_station_bootstrap_runner:run(Dht, Cfg),
            ?assertEqual(3, length(Out)),
            ?assertEqual(3, maps:get(observed, Summary)),
            ?assertEqual(3, maps:get(admitted, Summary)),
            ?assertEqual(3, macula_dht:size(Dht))
        end
    end}.

%%==================================================================
%% no_tiers — surfaced verbatim so the caller refuses SWIM.
%%==================================================================

runner_no_tiers_is_verbatim_test_() ->
    {setup, fun start_dht/0, fun stop_dht/1, fun({Dht}) ->
        fun() ->
            Cfg = #{discoverers => [], cascade_opts => #{}},
            ?assertEqual({error, no_tiers},
                         macula_station_bootstrap_runner:run(Dht, Cfg)),
            ?assertEqual(0, macula_dht:size(Dht))
        end
    end}.

%%==================================================================
%% cascade_failed — wrapped as {bootstrap_failed, _}.
%%==================================================================

runner_cascade_failure_is_wrapped_test_() ->
    {setup, fun start_dht/0, fun stop_dht/1, fun({Dht}) ->
        fun() ->
            %% Tier returns an error → cascade never crosses min_peers →
            %% orchestrator returns cascade_failed → runner wraps.
            Tiers = [{macula_station_stub_tier, #{error => boom}}],
            Cfg   = #{discoverers => Tiers,
                      cascade_opts => #{min_peers => 1, timeout_ms => 500}},
            ?assertMatch({error, {bootstrap_failed, _}},
                         macula_station_bootstrap_runner:run(Dht, Cfg)),
            ?assertEqual(0, macula_dht:size(Dht))
        end
    end}.

%%==================================================================
%% run/1 reads the bootstrap app env
%%==================================================================

runner_reads_application_env_test_() ->
    {setup, fun start_dht/0, fun stop_dht/1, fun({Dht}) ->
        fun() ->
            Peers = macula_station_stub_tier:stub_peers(2),
            Tiers = [{macula_station_stub_tier, #{peers => Peers}}],
            application:set_env(macula_bootstrap, discoverers, Tiers),
            application:set_env(macula_bootstrap, cascade_opts,
                                #{min_peers => 1}),
            try
                {ok, #{summary := #{admitted := 2}}} =
                    macula_station_bootstrap_runner:run(Dht)
            after
                application:unset_env(macula_bootstrap, discoverers),
                application:unset_env(macula_bootstrap, cascade_opts)
            end
        end
    end}.

%%==================================================================
%% Helpers
%%==================================================================

start_dht() ->
    application:ensure_all_started(crypto),
    {ok, Dht} = macula_dht:start_link(#{self_id => <<0:256>>}),
    {Dht}.

stop_dht({Dht}) ->
    ok = macula_dht:stop(Dht),
    ok.

stub_cfg(Peers) ->
    #{
        discoverers => [{macula_station_stub_tier, #{peers => Peers}}],
        cascade_opts => #{min_peers => 1, timeout_ms => 500}
    }.
