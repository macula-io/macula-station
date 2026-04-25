-module(hecate_station_config_tests).
-include_lib("eunit/include/eunit.hrl").
-include("hecate_station_cfg.hrl").

%%==================================================================
%% Legacy load/1 — walking skeleton contract
%%==================================================================

load_with_inline_identity_test() ->
    Kp = hecate_identity:generate(),
    Spec = #{bind => "127.0.0.1", port => 9000,
             certfile => "c.pem", keyfile => "k.pem",
             identity => Kp},
    {ok, Opts} = hecate_station_config:load(Spec),
    ?assertEqual(Kp,             maps:get(identity, Opts)),
    ?assertEqual([],             maps:get(realms, Opts)),
    ?assertEqual(0,              maps:get(capabilities, Opts)).

load_missing_bind_reports_missing_test() ->
    Kp = hecate_identity:generate(),
    Spec = #{port => 9000, certfile => "c", keyfile => "k",
             identity => Kp},
    ?assertEqual({error, {missing, bind}},
                 hecate_station_config:load(Spec)).

load_with_identity_file_generates_on_demand_test_() ->
    {setup, fun tmpdir/0, fun rm_rf/1, fun(Dir) ->
        fun() ->
            IdPath = hecate_station_identity:path_for(Dir),
            Spec   = #{bind => "127.0.0.1", port => 9000,
                       certfile => "c", keyfile => "k",
                       identity_file => IdPath},
            {ok, Opts}  = hecate_station_config:load(Spec),
            {ok, Opts2} = hecate_station_config:load(Spec),
            %% Warm-boot opts share identity bytes.
            Pub1 = hecate_identity:public(maps:get(identity, Opts)),
            Pub2 = hecate_identity:public(maps:get(identity, Opts2)),
            ?assertEqual(Pub1, Pub2)
        end
    end}.

%%==================================================================
%% from_env/0 — supervisor path
%%==================================================================

enabled_false_when_no_config_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            ?assertNot(hecate_station_config:enabled())
        end
    end}.

enabled_true_when_sys_config_has_bind_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            application:set_env(hecate_station, bind, "127.0.0.1"),
            ?assert(hecate_station_config:enabled())
        end
    end}.

from_env_happy_path_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                set_all(Dir, "127.0.0.1", 9000),
                {ok, Cfg} = hecate_station_config:from_env(),
                ?assertEqual("127.0.0.1",  Cfg#station_cfg.bind),
                ?assertEqual(9000,          Cfg#station_cfg.port),
                ?assertEqual(Dir,           Cfg#station_cfg.data_dir),
                ?assertEqual(0,             Cfg#station_cfg.capabilities),
                ?assert(is_map(Cfg#station_cfg.identity)),
                Expected = hecate_station_identity:path_for(Dir),
                ?assertEqual(Expected, Cfg#station_cfg.identity_file),
                %% to_opts projects cleanly for the server.
                Opts = hecate_station_config:to_opts(Cfg),
                ?assertEqual(9000, maps:get(port, Opts))
            after
                rm_rf(Dir)
            end
        end
    end}.

from_env_missing_port_is_bad_config_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                application:set_env(hecate_station, data_dir, Dir),
                application:set_env(hecate_station, bind, "127.0.0.1"),
                application:set_env(hecate_station, certfile, "c"),
                application:set_env(hecate_station, keyfile, "k"),
                ?assertEqual({error, {bad_config, {missing, port}}},
                             hecate_station_config:from_env())
            after
                rm_rf(Dir)
            end
        end
    end}.

from_env_non_integer_port_is_bad_config_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                set_all(Dir, "127.0.0.1", "nope"),
                ?assertMatch({error, {bad_config, {parse, _, integer, _}}},
                             hecate_station_config:from_env())
            after
                rm_rf(Dir)
            end
        end
    end}.

from_env_env_var_overrides_app_env_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                set_all(Dir, "127.0.0.1", 9000),
                os:putenv("HECATE_STATION_PORT", "9999"),
                {ok, Cfg} = hecate_station_config:from_env(),
                ?assertEqual(9999, Cfg#station_cfg.port)
            after
                os:unsetenv("HECATE_STATION_PORT"),
                rm_rf(Dir)
            end
        end
    end}.

from_env_preserves_identity_across_warm_boots_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                set_all(Dir, "127.0.0.1", 9000),
                {ok, Cfg1} = hecate_station_config:from_env(),
                {ok, Cfg2} = hecate_station_config:from_env(),
                Pub1 = hecate_identity:public(Cfg1#station_cfg.identity),
                Pub2 = hecate_identity:public(Cfg2#station_cfg.identity),
                ?assertEqual(Pub1, Pub2)
            after
                rm_rf(Dir)
            end
        end
    end}.

%%==================================================================
%% Helpers
%%==================================================================

set_all(Dir, Bind, Port) ->
    application:set_env(hecate_station, data_dir, Dir),
    application:set_env(hecate_station, bind,     Bind),
    application:set_env(hecate_station, port,     Port),
    application:set_env(hecate_station, certfile, "/tmp/c.pem"),
    application:set_env(hecate_station, keyfile,  "/tmp/k.pem").

clear_env() ->
    Keys = [data_dir, identity_file, bind, port, certfile, keyfile,
            capabilities, cache, rebootstrap, admin],
    Saved = [{K, application:get_env(hecate_station, K)} || K <- Keys],
    [application:unset_env(hecate_station, K) || K <- Keys],
    [os:unsetenv(V) || V <- ["HECATE_STATION_DATA_DIR",
                             "HECATE_STATION_IDENTITY_FILE",
                             "HECATE_STATION_BIND",
                             "HECATE_STATION_PORT",
                             "HECATE_STATION_CERTFILE",
                             "HECATE_STATION_KEYFILE",
                             "HECATE_STATION_CAPABILITIES"]],
    Saved.

restore_env(Saved) ->
    [restore_one(K, V) || {K, V} <- Saved],
    ok.

restore_one(K, undefined)  -> application:unset_env(hecate_station, K);
restore_one(K, {ok, V})    -> application:set_env(hecate_station, K, V).

make_tmpdir() ->
    Base = filename:join(["/tmp", "hecate-station-cfg-test",
                          integer_to_list(erlang:unique_integer([positive]))]),
    ok   = filelib:ensure_dir(filename:join(Base, "placeholder")),
    Base.

tmpdir() -> make_tmpdir().

rm_rf(Dir) -> _ = os:cmd("rm -rf " ++ Dir), ok.
