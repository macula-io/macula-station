%% @doc End-to-end admin HTTP API tests.
%%
%% Boots the full station app with an admin config bound to an
%% ephemeral port, hits every exposed endpoint via `httpc', and
%% asserts the decoded JSON shape. One test per endpoint so a single
%% handler regression surfaces as a single red dot.
-module(hecate_station_admin_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("hecate_station/include/hecate_station_cfg.hrl").

%%==================================================================
%% GET /status
%%==================================================================

status_endpoint_returns_healthy_snapshot_test_() ->
    {setup, fun setup_app/0, fun teardown_app/1, fun(#{port := Port} = Ctx) ->
        fun() ->
            {ok, Body, Status} = get_json(Port, "/status"),
            ?assertEqual(200, Status),
            ?assertEqual(true, maps:get(<<"healthy">>, Body)),
            ?assert(is_binary(maps:get(<<"node_id">>, Body))),
            ?assert(is_binary(maps:get(<<"listen_addr">>, Body))),
            ?assertMatch(#{<<"size">> := _}, maps:get(<<"dht">>, Body)),
            ?assertMatch(#{<<"members">> := _}, maps:get(<<"swim">>, Body)),
            ?assertEqual([], maps:get(<<"realms">>, Body)),
            _ = Ctx
        end
    end}.

%%==================================================================
%% GET /dht/stats
%%==================================================================

dht_stats_endpoint_test_() ->
    {setup, fun setup_app/0, fun teardown_app/1, fun(#{port := Port}) ->
        fun() ->
            {ok, Body, 200} = get_json(Port, "/dht/stats"),
            ?assert(is_integer(maps:get(<<"size">>, Body))),
            ?assert(is_binary(maps:get(<<"self_id">>, Body))),
            ?assertEqual(64, byte_size(maps:get(<<"self_id">>, Body)))
        end
    end}.

%%==================================================================
%% GET /swim/members
%%==================================================================

swim_members_endpoint_test_() ->
    {setup, fun setup_app/0, fun teardown_app/1, fun(#{port := Port}) ->
        fun() ->
            {ok, Body, 200} = get_json(Port, "/swim/members"),
            ?assertEqual([], maps:get(<<"members">>, Body))
        end
    end}.

%%==================================================================
%% POST /bootstrap/rerun
%%==================================================================

bootstrap_rerun_endpoint_test_() ->
    {setup, fun setup_app/0, fun teardown_app/1, fun(#{port := Port}) ->
        fun() ->
            {ok, Body, 200} = post_json(Port, "/bootstrap/rerun", <<>>),
            ?assertEqual(<<"ok">>, maps:get(<<"result">>, Body)),
            ?assertMatch(#{<<"admitted">> := _},
                         maps:get(<<"summary">>, Body))
        end
    end}.

%%==================================================================
%% Unknown routes
%%==================================================================

unknown_path_returns_404_test_() ->
    {setup, fun setup_app/0, fun teardown_app/1, fun(#{port := Port}) ->
        fun() ->
            {ok, Body, 404} = get_json(Port, "/nope"),
            ?assertEqual(<<"not_found">>, maps:get(<<"reason">>, Body))
        end
    end}.

%%==================================================================
%% Fixture
%%==================================================================

setup_app() ->
    process_flag(trap_exit, true),
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(macula),
    {ok, _} = application:ensure_all_started(inets),
    Dir   = make_tmpdir(),
    Peers = hecate_station_stub_tier:stub_peers(1),
    Saved = clear_env(),
    set_station_env(Dir),
    application:set_env(hecate_bootstrap, tiers,
                        [{hecate_station_stub_tier, #{peers => Peers}}]),
    application:set_env(hecate_bootstrap, cascade_opts,
                        #{min_peers => 1, timeout_ms => 1000}),
    application:set_env(hecate_station, admin,
                        #{bind => "127.0.0.1", port => 0}),
    {ok, Sup} = hecate_station_app:start(normal, []),
    {ok, Port} = hecate_station:admin_addr(),
    #{sup => Sup, dir => Dir, port => Port, saved => Saved}.

teardown_app(#{sup := Sup, dir := Dir, saved := Saved}) ->
    cleanup_sup(Sup),
    restore_env(Saved),
    rm_rf(Dir),
    ok.

%%------------------------------------------------------------------
%% HTTP helpers — use httpc so we exercise real socket I/O.
%%------------------------------------------------------------------

get_json(Port, Path) ->
    Url = "http://127.0.0.1:" ++ integer_to_list(Port) ++ Path,
    http_decode(httpc:request(get, {Url, []}, [{timeout, 5000}], [])).

post_json(Port, Path, Body) ->
    Url = "http://127.0.0.1:" ++ integer_to_list(Port) ++ Path,
    http_decode(httpc:request(post,
        {Url, [], "application/json", Body},
        [{timeout, 5000}], [])).

http_decode({ok, {{_, Status, _}, _Headers, Body}}) ->
    {ok, json:decode(iolist_to_binary(Body)), Status};
http_decode(Other) ->
    {error, Other}.

%%------------------------------------------------------------------
%% Env wiring — duplicates hecate_station_app_tests' helpers so the
%% admin suite stays self-contained.
%%------------------------------------------------------------------

clear_env() ->
    Station = [data_dir, identity_file, bind, port, certfile, keyfile,
               realms, capabilities, cache, rebootstrap, admin],
    Boot    = [tiers, cascade_opts],
    Saved = [{K, station, application:get_env(hecate_station, K)} || K <- Station]
         ++ [{K, bootstrap, application:get_env(hecate_bootstrap, K)} || K <- Boot],
    [application:unset_env(hecate_station, K) || K <- Station],
    [application:unset_env(hecate_bootstrap, K) || K <- Boot],
    Saved.

restore_env(Saved) ->
    [case V of
         undefined -> application:unset_env(App, K);
         {ok, X}   -> application:set_env(App, K, X)
     end || {K, App, V} <- Saved],
    ok.

set_station_env(Dir) ->
    {Cert, Key} = generate_test_cert(Dir),
    application:set_env(hecate_station, data_dir, Dir),
    application:set_env(hecate_station, bind,     "127.0.0.1"),
    application:set_env(hecate_station, port,     free_port()),
    application:set_env(hecate_station, certfile, Cert),
    application:set_env(hecate_station, keyfile,  Key).

generate_test_cert(Dir) ->
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Cert = filename:join(Dir, "cert.pem"),
    Key  = filename:join(Dir, "key.pem"),
    Cmd  = lists:flatten(io_lib:format(
        "openssl req -x509 -newkey rsa:2048 -nodes "
        "-keyout ~s -out ~s -days 1 -subj /CN=localhost 2>&1",
        [Key, Cert])),
    Out = os:cmd(Cmd),
    true = filelib:is_regular(Cert) orelse error({openssl_failed, Out}),
    {Cert, Key}.

free_port() ->
    {ok, S} = gen_udp:open(0, [{reuseaddr, true}]),
    {ok, P} = inet:port(S),
    ok = gen_udp:close(S),
    P.

make_tmpdir() ->
    Base = filename:join(["/tmp", "hecate-station-admin-test",
                          integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(Base, "placeholder")),
    Base.

rm_rf(Dir) -> _ = os:cmd("rm -rf " ++ Dir), ok.

cleanup_sup(Sup) ->
    true = unlink(Sup),
    Ref  = monitor(process, Sup),
    exit(Sup, shutdown),
    receive {'DOWN', Ref, process, Sup, _} -> ok
    after 5000 -> timeout end.
