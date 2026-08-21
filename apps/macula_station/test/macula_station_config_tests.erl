-module(macula_station_config_tests).
-include_lib("eunit/include/eunit.hrl").
-include("macula_station_cfg.hrl").

%%==================================================================
%% Legacy load/1 — walking skeleton contract
%%==================================================================

load_with_inline_identity_test() ->
    Kp = macula_identity:generate(),
    Spec = #{bind => "127.0.0.1", port => 9000,
             certfile => "c.pem", keyfile => "k.pem",
             identity => Kp},
    {ok, Opts} = macula_station_config:load(Spec),
    ?assertEqual(Kp,             maps:get(identity, Opts)),
    ?assertEqual([],             maps:get(realms, Opts)),
    ?assertEqual(0,              maps:get(capabilities, Opts)).

load_missing_bind_reports_missing_test() ->
    Kp = macula_identity:generate(),
    Spec = #{port => 9000, certfile => "c", keyfile => "k",
             identity => Kp},
    ?assertEqual({error, {missing, bind}},
                 macula_station_config:load(Spec)).

load_with_identity_file_generates_on_demand_test_() ->
    {setup, fun tmpdir/0, fun rm_rf/1, fun(Dir) ->
        fun() ->
            IdPath = macula_station_identity:path_for(Dir),
            Spec   = #{bind => "127.0.0.1", port => 9000,
                       certfile => "c", keyfile => "k",
                       identity_file => IdPath},
            {ok, Opts}  = macula_station_config:load(Spec),
            {ok, Opts2} = macula_station_config:load(Spec),
            %% Warm-boot opts share identity bytes.
            Pub1 = macula_identity:public(maps:get(identity, Opts)),
            Pub2 = macula_identity:public(maps:get(identity, Opts2)),
            ?assertEqual(Pub1, Pub2)
        end
    end}.

%%==================================================================
%% from_env/0 — JSON-config path (production)
%%==================================================================

enabled_false_when_no_config_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            ?assertNot(macula_station_config:enabled())
        end
    end}.

enabled_true_when_json_config_set_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            os:putenv("MACULA_STATION_CONFIG", "/tmp/whatever.json"),
            ?assert(macula_station_config:enabled())
        end
    end}.

enabled_true_when_sys_config_has_bind_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            application:set_env(macula_station, bind, "127.0.0.1"),
            ?assert(macula_station_config:enabled())
        end
    end}.

from_env_json_happy_path_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                Path = write_json(Dir, base_config(Dir, <<"127.0.0.1">>, 9000)),
                os:putenv("MACULA_STATION_CONFIG", Path),
                {ok, Cfg} = macula_station_config:from_env(),
                ?assertEqual("127.0.0.1",  Cfg#station_cfg.bind),
                ?assertEqual(9000,         Cfg#station_cfg.port),
                ?assertEqual(Dir,          Cfg#station_cfg.data_dir),
                %% Config decode now OR-s in ?CAP_STATION (1 bsl 0)
                %% so the station's CONNECT advertises its role —
                %% lets the peer's peer_observer tell gossip relays
                %% from direct daemon ADVERTISEs. Operator-supplied
                %% bits add to that base.
                ?assertEqual(1,            Cfg#station_cfg.capabilities),
                ?assert(is_map(Cfg#station_cfg.identity)),
                Expected = macula_station_identity:path_for(Dir),
                ?assertEqual(Expected, Cfg#station_cfg.identity_file),
                Opts = macula_station_config:to_opts(Cfg),
                ?assertEqual(9000, maps:get(port, Opts))
            after
                rm_rf(Dir)
            end
        end
    end}.

from_env_json_geo_block_populates_record_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                Cfg0 = base_config(Dir, <<"127.0.0.1">>, 9000),
                WithGeo = Cfg0#{<<"geo">> => #{
                    <<"hostname">> => <<"station-be-brussels.macula.io">>,
                    <<"city">>     => <<"Brussels">>,
                    <<"country">>  => <<"BE">>,
                    <<"lat">>      => 50.8503,
                    <<"lng">>      => 4.3517
                }},
                Path = write_json(Dir, WithGeo),
                os:putenv("MACULA_STATION_CONFIG", Path),
                {ok, Cfg} = macula_station_config:from_env(),
                ?assertEqual(<<"station-be-brussels.macula.io">>,
                             Cfg#station_cfg.hostname),
                ?assertEqual(<<"Brussels">>, Cfg#station_cfg.city),
                ?assertEqual(<<"BE">>,       Cfg#station_cfg.country),
                ?assertEqual(50.8503,        Cfg#station_cfg.lat),
                ?assertEqual(4.3517,         Cfg#station_cfg.lng)
            after
                rm_rf(Dir)
            end
        end
    end}.

from_env_json_missing_bind_is_bad_config_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                Cfg = maps:remove(<<"bind">>, base_config(Dir, <<"x">>, 9000)),
                Path = write_json(Dir, Cfg),
                os:putenv("MACULA_STATION_CONFIG", Path),
                ?assertEqual({error, {bad_config, {missing, bind}}},
                             macula_station_config:from_env())
            after
                rm_rf(Dir)
            end
        end
    end}.

from_env_json_unreadable_file_is_bad_config_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            os:putenv("MACULA_STATION_CONFIG", "/nonexistent/path.json"),
            ?assertMatch({error, {bad_config, {read, _, enoent}}},
                         macula_station_config:from_env())
        end
    end}.

from_env_json_malformed_is_bad_config_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                Path = filename:join(Dir, "bad.json"),
                ok = file:write_file(Path, <<"{not valid json">>),
                os:putenv("MACULA_STATION_CONFIG", Path),
                ?assertMatch({error, {bad_config, {parse, _, _}}},
                             macula_station_config:from_env())
            after
                rm_rf(Dir)
            end
        end
    end}.

from_env_json_preserves_identity_across_warm_boots_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                Path = write_json(Dir, base_config(Dir, <<"127.0.0.1">>, 9000)),
                os:putenv("MACULA_STATION_CONFIG", Path),
                {ok, Cfg1} = macula_station_config:from_env(),
                {ok, Cfg2} = macula_station_config:from_env(),
                Pub1 = macula_identity:public(Cfg1#station_cfg.identity),
                Pub2 = macula_identity:public(Cfg2#station_cfg.identity),
                ?assertEqual(Pub1, Pub2)
            after
                rm_rf(Dir)
            end
        end
    end}.

%%==================================================================
%% from_env/0 — sys.config fallback path (CT)
%%==================================================================

from_env_app_env_happy_path_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                set_app_env(Dir, "127.0.0.1", 9000),
                {ok, Cfg} = macula_station_config:from_env(),
                ?assertEqual("127.0.0.1", Cfg#station_cfg.bind),
                ?assertEqual(9000,        Cfg#station_cfg.port)
            after
                rm_rf(Dir)
            end
        end
    end}.

from_env_app_env_missing_port_is_bad_config_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                application:set_env(macula_station, data_dir, Dir),
                application:set_env(macula_station, bind, "127.0.0.1"),
                application:set_env(macula_station, certfile, "c"),
                application:set_env(macula_station, keyfile, "k"),
                ?assertEqual({error, {bad_config, {missing, port}}},
                             macula_station_config:from_env())
            after
                rm_rf(Dir)
            end
        end
    end}.

%%==================================================================
%% Legacy multi-identity guard
%%==================================================================

refuses_legacy_multi_identity_env_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            os:putenv("MACULA_RELAY_IDENTITIES", "anything"),
            try
                ?assertError({legacy_multi_identity_env, _},
                             macula_station_config:enabled())
            after
                os:unsetenv("MACULA_RELAY_IDENTITIES")
            end
        end
    end}.

%%==================================================================
%% from_env/0 — outbound_peers JSON parsing
%%==================================================================

from_env_json_outbound_peers_populates_record_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                Cfg0      = base_config(Dir, <<"127.0.0.1">>, 9000),
                WithPeers = Cfg0#{<<"outbound_peers">> => [
                    #{<<"host">> => <<"station-be-ghent.macula.io">>,
                      <<"port">> => 4433},
                    #{<<"host">> => <<"station-be-leuven.macula.io">>,
                      <<"port">> => 4433}
                ]},
                Path = write_json(Dir, WithPeers),
                os:putenv("MACULA_STATION_CONFIG", Path),
                {ok, Cfg} = macula_station_config:from_env(),
                ?assertEqual([#{host => <<"station-be-ghent.macula.io">>,
                                port => 4433},
                              #{host => <<"station-be-leuven.macula.io">>,
                                port => 4433}],
                             Cfg#station_cfg.outbound_peers)
            after
                rm_rf(Dir)
            end
        end
    end}.

from_env_json_omits_outbound_peers_defaults_to_empty_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                Path = write_json(Dir, base_config(Dir, <<"127.0.0.1">>, 9000)),
                os:putenv("MACULA_STATION_CONFIG", Path),
                {ok, Cfg} = macula_station_config:from_env(),
                ?assertEqual([], Cfg#station_cfg.outbound_peers)
            after
                rm_rf(Dir)
            end
        end
    end}.

%%==================================================================
%% from_env/0 — puzzle_enforcement JSON parsing
%%==================================================================

from_env_json_omits_puzzle_enforcement_defaults_to_off_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                Path = write_json(Dir, base_config(Dir, <<"127.0.0.1">>, 9000)),
                os:putenv("MACULA_STATION_CONFIG", Path),
                {ok, Cfg} = macula_station_config:from_env(),
                ?assertEqual(off, Cfg#station_cfg.puzzle_enforcement)
            after
                rm_rf(Dir)
            end
        end
    end}.

from_env_json_puzzle_enforcement_log_only_populates_record_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                Cfg0 = base_config(Dir, <<"127.0.0.1">>, 9000),
                With = Cfg0#{<<"puzzle_enforcement">> => <<"log_only">>},
                Path = write_json(Dir, With),
                os:putenv("MACULA_STATION_CONFIG", Path),
                {ok, Cfg} = macula_station_config:from_env(),
                ?assertEqual(log_only, Cfg#station_cfg.puzzle_enforcement)
            after
                rm_rf(Dir)
            end
        end
    end}.

from_env_json_puzzle_enforcement_enforce_populates_record_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                Cfg0 = base_config(Dir, <<"127.0.0.1">>, 9000),
                With = Cfg0#{<<"puzzle_enforcement">> => <<"enforce">>},
                Path = write_json(Dir, With),
                os:putenv("MACULA_STATION_CONFIG", Path),
                {ok, Cfg} = macula_station_config:from_env(),
                ?assertEqual(enforce, Cfg#station_cfg.puzzle_enforcement)
            after
                rm_rf(Dir)
            end
        end
    end}.

%% A typo (or a future mode this build doesn't know about yet) must
%% surface as the same {error, {bad_config, _}} shape every other
%% malformed field does -- silently falling back to `off' would mean
%% an operator's JSON says "enforce" while the station quietly never
%% enforces anything, which is exactly the failure this whole feature
%% exists to prevent.
from_env_json_puzzle_enforcement_typo_is_bad_config_test_() ->
    {setup, fun clear_env/0, fun restore_env/1, fun(_) ->
        fun() ->
            Dir = make_tmpdir(),
            try
                Cfg0 = base_config(Dir, <<"127.0.0.1">>, 9000),
                With = Cfg0#{<<"puzzle_enforcement">> => <<"enforced">>},
                Path = write_json(Dir, With),
                os:putenv("MACULA_STATION_CONFIG", Path),
                ?assertEqual(
                   {error, {bad_config, {puzzle_enforcement, <<"enforced">>}}},
                   macula_station_config:from_env())
            after
                rm_rf(Dir)
            end
        end
    end}.

%%==================================================================
%% Helpers
%%==================================================================

base_config(Dir, Bind, Port) ->
    #{
        <<"data_dir">> => list_to_binary(Dir),
        <<"bind">>     => Bind,
        <<"port">>     => Port,
        <<"certfile">> => <<"/tmp/c.pem">>,
        <<"keyfile">>  => <<"/tmp/k.pem">>
    }.

write_json(Dir, Map) ->
    Path = filename:join(Dir, "station.json"),
    ok = file:write_file(Path, iolist_to_binary(json:encode(Map))),
    Path.

set_app_env(Dir, Bind, Port) ->
    application:set_env(macula_station, data_dir, Dir),
    application:set_env(macula_station, bind,     Bind),
    application:set_env(macula_station, port,     Port),
    application:set_env(macula_station, certfile, "/tmp/c.pem"),
    application:set_env(macula_station, keyfile,  "/tmp/k.pem").

clear_env() ->
    Keys = [data_dir, identity_file, bind, port, certfile, keyfile,
            capabilities, cache, rebootstrap, admin, puzzle_enforcement],
    Saved = [{K, application:get_env(macula_station, K)} || K <- Keys],
    [application:unset_env(macula_station, K) || K <- Keys],
    os:unsetenv("MACULA_STATION_CONFIG"),
    os:unsetenv("MACULA_RELAY_IDENTITIES"),
    Saved.

restore_env(Saved) ->
    [restore_one(K, V) || {K, V} <- Saved],
    os:unsetenv("MACULA_STATION_CONFIG"),
    os:unsetenv("MACULA_RELAY_IDENTITIES"),
    ok.

restore_one(K, undefined)  -> application:unset_env(macula_station, K);
restore_one(K, {ok, V})    -> application:set_env(macula_station, K, V).

make_tmpdir() ->
    Base = filename:join(["/tmp", "macula-station-cfg-test",
                          integer_to_list(erlang:unique_integer([positive]))]),
    ok   = filelib:ensure_dir(filename:join(Base, "placeholder")),
    Base.

tmpdir() -> make_tmpdir().

rm_rf(Dir) -> _ = os:cmd("rm -rf " ++ Dir), ok.
