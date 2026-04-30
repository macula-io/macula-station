%% @doc End-to-end tests for the multi-identity admin endpoints
%% (PLAN_MULTI_IDENTITY_RELAY §Phase 5).
%%
%% Boots a station via the multi-identity branch (one identity
%% provisioned at boot), then drives the admin HTTP API through
%% `httpc' to verify:
%%
%% <ul>
%%   <li>Bearer auth — missing token, wrong token, valid token.</li>
%%   <li>`GET /admin/identities' lists the registered identities,
%%       including the one provisioned at boot.</li>
%%   <li>`POST /admin/identities/:id/start' creates a new identity
%%       at runtime; verifies it shows up in the listing + has a
%%       live listener.</li>
%%   <li>`POST /admin/identities/:id/stop' tears it back down.</li>
%%   <li>`POST /admin/identities/:id/reload' restarts an identity
%%       with the same opts (sup pid changes).</li>
%% </ul>
%%
%% The whole module's tests are wrapped in a single `foreach' so
%% the per-test setup that boots the full station app does not
%% race with other parallel test modules over the singleton
%% `macula_station_sup' registered name.
-module(macula_station_admin_identities_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BOOT_IDENTITY, <<"boot.macula.io">>).
-define(RUNTIME_IDENTITY, <<"runtime.macula.io">>).

%%==================================================================
%% Generator — single foreach so all per-test fixtures run inorder.
%%==================================================================

admin_identities_test_() ->
    {timeout, 120,
     {foreach,
      fun setup_app/0,
      fun teardown_app/1,
      [
        fun list_endpoint_returns_boot_identity/1,
        fun missing_authorization_returns_401/1,
        fun wrong_token_returns_401/1,
        fun start_then_stop_runtime_identity/1,
        fun stop_unknown_identity_returns_404/1,
        fun reload_changes_sup_pid/1,
        fun start_with_id_hostname_mismatch_returns_400/1,
        fun start_with_invalid_json_returns_400/1,
        fun health_endpoint_returns_per_identity_snapshot/1,
        fun health_endpoint_unknown_id_returns_404/1
      ]}}.

%% Auth-no-token has its own setup (different MACULA_ADMIN_TOKEN
%% state) so it runs in a separate foreach.
missing_admin_token_env_test_() ->
    {timeout, 60,
     {foreach,
      fun setup_app_no_token/0,
      fun teardown_app/1,
      [
        fun missing_admin_token_env_returns_503/1
      ]}}.

%%==================================================================
%% Tests
%%==================================================================

list_endpoint_returns_boot_identity(#{port := Port, token := Token}) ->
    ?_test(begin
        {ok, Body, 200} = get_json_authed(Port, "/admin/identities", Token),
        Identities = maps:get(<<"identities">>, Body),
        ?assertMatch([_], Identities),
        [Entry] = Identities,
        ?assertEqual(?BOOT_IDENTITY, maps:get(<<"identity_key">>, Entry)),
        Listener = maps:get(<<"listener">>, Entry),
        ?assertEqual(<<"alive">>, maps:get(<<"state">>, Listener)),
        ?assert(is_binary(maps:get(<<"addr">>, Listener)))
    end).

missing_authorization_returns_401(#{port := Port}) ->
    ?_test(begin
        {ok, Body, 401} = get_json(Port, "/admin/identities"),
        ?assertEqual(<<"unauthorized">>, maps:get(<<"reason">>, Body))
    end).

wrong_token_returns_401(#{port := Port}) ->
    ?_test(begin
        {ok, Body, 401} =
            get_json_authed(Port, "/admin/identities", "wrong-token"),
        ?assertEqual(<<"unauthorized">>, maps:get(<<"reason">>, Body))
    end).

missing_admin_token_env_returns_503(#{port := Port}) ->
    ?_test(begin
        {ok, Body, 503} = get_json(Port, "/admin/identities"),
        ?assertEqual(<<"admin_token_not_configured">>,
                     maps:get(<<"reason">>, Body))
    end).

start_then_stop_runtime_identity(#{port := Port, token := Token}) ->
    ?_test(begin
        Spec = #{
            <<"hostname">> => ?RUNTIME_IDENTITY,
            <<"city">>     => <<"Runtime">>,
            <<"country">>  => <<"XX">>,
            <<"lat">>      => 0.0,
            <<"lng">>      => 0.0,
            <<"bind">>     => <<"127.0.0.4">>
        },
        Body = json:encode(Spec),
        Path = "/admin/identities/runtime.macula.io/start",
        {ok, Reply, 201} = post_json_authed(Port, Path, Body, Token),
        ?assertEqual(<<"ok">>, maps:get(<<"result">>, Reply)),

        {ok, ListBody, 200} =
            get_json_authed(Port, "/admin/identities", Token),
        Hostnames = lists:sort(
            [maps:get(<<"identity_key">>, E)
             || E <- maps:get(<<"identities">>, ListBody)]),
        ?assertEqual(lists:sort([?BOOT_IDENTITY, ?RUNTIME_IDENTITY]),
                     Hostnames),

        StopPath = "/admin/identities/runtime.macula.io/stop",
        {ok, StopReply, 200} =
            post_json_authed(Port, StopPath, <<>>, Token),
        ?assertEqual(<<"ok">>, maps:get(<<"result">>, StopReply)),

        {ok, ListBody2, 200} =
            get_json_authed(Port, "/admin/identities", Token),
        ?assertMatch([_], maps:get(<<"identities">>, ListBody2))
    end).

stop_unknown_identity_returns_404(#{port := Port, token := Token}) ->
    ?_test(begin
        Path = "/admin/identities/unknown.macula.io/stop",
        {ok, Body, 404} =
            post_json_authed(Port, Path, <<>>, Token),
        ?assertEqual(<<"not_found">>, maps:get(<<"reason">>, Body))
    end).

reload_changes_sup_pid(#{port := Port, token := Token}) ->
    ?_test(begin
        {ok, ListBefore, 200} =
            get_json_authed(Port, "/admin/identities", Token),
        [Entry1] = maps:get(<<"identities">>, ListBefore),
        SupPid1 = maps:get(<<"sup_pid">>, Entry1),

        ReloadPath = "/admin/identities/boot.macula.io/reload",
        {ok, Reload, 200} =
            post_json_authed(Port, ReloadPath, <<>>, Token),
        ?assertEqual(<<"ok">>, maps:get(<<"result">>, Reload)),
        SupPid2 = maps:get(<<"sup_pid">>, Reload),
        ?assertNotEqual(SupPid1, SupPid2),

        {ok, ListAfter, 200} =
            get_json_authed(Port, "/admin/identities", Token),
        [Entry2] = maps:get(<<"identities">>, ListAfter),
        ?assertEqual(SupPid2, maps:get(<<"sup_pid">>, Entry2))
    end).

start_with_id_hostname_mismatch_returns_400(#{port := Port, token := Token}) ->
    ?_test(begin
        Spec = #{
            <<"hostname">> => <<"different.macula.io">>,
            <<"city">>     => <<"X">>,
            <<"country">>  => <<"XX">>,
            <<"lat">>      => 0.0,
            <<"lng">>      => 0.0
        },
        Body = json:encode(Spec),
        Path = "/admin/identities/url-says-this.macula.io/start",
        {ok, Reply, 400} =
            post_json_authed(Port, Path, Body, Token),
        ?assertEqual(<<"id_hostname_mismatch">>,
                     maps:get(<<"reason">>, Reply))
    end).

start_with_invalid_json_returns_400(#{port := Port, token := Token}) ->
    ?_test(begin
        Path = "/admin/identities/x.macula.io/start",
        {ok, Reply, 400} =
            post_json_authed(Port, Path, <<"not-json">>, Token),
        ?assertEqual(<<"invalid_json">>,
                     maps:get(<<"reason">>, Reply))
    end).

health_endpoint_returns_per_identity_snapshot(#{port := Port, token := Token}) ->
    ?_test(begin
        Path = "/admin/identities/boot.macula.io/health",
        {ok, Body, 200} = get_json_authed(Port, Path, Token),

        ?assertEqual(?BOOT_IDENTITY, maps:get(<<"identity_key">>, Body)),
        ?assertEqual(true,           maps:get(<<"healthy">>, Body)),

        Listener = maps:get(<<"listener">>, Body),
        ?assertEqual(<<"alive">>, maps:get(<<"state">>, Listener)),
        ?assert(is_binary(maps:get(<<"addr">>, Listener))),

        Dht = maps:get(<<"dht">>, Body),
        ?assertEqual(<<"alive">>, maps:get(<<"state">>, Dht)),
        ?assert(is_integer(maps:get(<<"size">>, Dht))),
        ?assertEqual(64, byte_size(maps:get(<<"self_id">>, Dht))),

        Swim = maps:get(<<"swim">>, Body),
        ?assertEqual(<<"alive">>, maps:get(<<"state">>, Swim)),
        ?assert(is_integer(maps:get(<<"members">>, Swim))),

        PubSub = maps:get(<<"pubsub_registry">>, Body),
        ?assertEqual(<<"alive">>, maps:get(<<"state">>, PubSub)),
        %% fact_publisher eagerly registers the all-zeros mesh realm
        %% in init/1 so inbound SUBSCRIBE frames from realm topology
        %% subscribers don't hit a missing server. Expect ≥ 1 realm.
        ?assert(maps:get(<<"realms">>, PubSub) >= 1),

        Observer = maps:get(<<"peer_observer">>, Body),
        ?assertEqual(<<"alive">>, maps:get(<<"state">>, Observer))
    end).

health_endpoint_unknown_id_returns_404(#{port := Port, token := Token}) ->
    ?_test(begin
        Path = "/admin/identities/unknown.macula.io/health",
        {ok, Body, 404} = get_json_authed(Port, Path, Token),
        ?assertEqual(<<"not_found">>, maps:get(<<"reason">>, Body))
    end).

%%==================================================================
%% Setup / teardown
%%==================================================================

setup_app() ->
    setup_app_with(<<"shared-secret-token">>).

setup_app_no_token() ->
    setup_app_with(no_token).

setup_app_with(TokenSpec) ->
    process_flag(trap_exit, true),
    {ok, _} = application:ensure_all_started(inets),
    Dir = make_tmpdir(),
    Saved = save_env(),
    {Cert, Key} = generate_test_cert(Dir),
    BoxSecretPath = filename:join(Dir, "box-secret"),
    Port = free_port(),
    AdminPort = 0,
    %% Multi-identity boot via env var with one identity.
    EnvIdentities = "boot.macula.io/Boot/XX/0.0/0.0/127.0.0.5",
    os:putenv("MACULA_RELAY_IDENTITIES", EnvIdentities),
    apply_token(TokenSpec),
    application:set_env(macula_station, port, Port),
    application:set_env(macula_station, certfile, Cert),
    application:set_env(macula_station, keyfile, Key),
    application:set_env(macula_station, box_secret_path, BoxSecretPath),
    application:set_env(macula_station, admin,
                        #{bind => "127.0.0.1", port => AdminPort}),
    {ok, Sup} = macula_station_app:start(normal, []),
    {ok, AdminBoundPort} = macula_station:admin_addr(),
    Token = case TokenSpec of
                no_token  -> undefined;
                Tok       -> Tok
            end,
    #{sup => Sup, dir => Dir, port => AdminBoundPort,
      token => Token, saved => Saved}.

apply_token(no_token) -> os:unsetenv("MACULA_ADMIN_TOKEN");
apply_token(Tok)      -> os:putenv("MACULA_ADMIN_TOKEN", binary_to_list(Tok)).

teardown_app(#{sup := Sup, dir := Dir, saved := Saved}) ->
    cleanup_sup(Sup),
    restore_env(Saved),
    os:unsetenv("MACULA_RELAY_IDENTITIES"),
    os:unsetenv("MACULA_ADMIN_TOKEN"),
    rm_rf(Dir),
    ok.

save_env() ->
    Keys = [port, certfile, keyfile, admin, box_secret_path,
            bind, data_dir, identity_file, capabilities],
    [{K, application:get_env(macula_station, K)} || K <- Keys].

restore_env(Saved) ->
    [restore_one(K, V) || {K, V} <- Saved].

restore_one(K, undefined)  -> application:unset_env(macula_station, K);
restore_one(K, {ok, V})    -> application:set_env(macula_station, K, V).

%%==================================================================
%% HTTP helpers
%%==================================================================

get_json(Port, Path) ->
    Url = "http://127.0.0.1:" ++ integer_to_list(Port) ++ Path,
    http_decode(httpc:request(get, {Url, []}, [{timeout, 5000}], [])).

get_json_authed(Port, Path, Token) ->
    Url = "http://127.0.0.1:" ++ integer_to_list(Port) ++ Path,
    Headers = [{"authorization", "Bearer " ++ token_str(Token)}],
    http_decode(httpc:request(get, {Url, Headers}, [{timeout, 5000}], [])).

post_json_authed(Port, Path, Body, Token) ->
    Url = "http://127.0.0.1:" ++ integer_to_list(Port) ++ Path,
    Headers = [{"authorization", "Bearer " ++ token_str(Token)}],
    http_decode(httpc:request(post,
        {Url, Headers, "application/json", Body},
        [{timeout, 5000}], [])).

token_str(B) when is_binary(B) -> binary_to_list(B);
token_str(L) when is_list(L)   -> L.

http_decode({ok, {{_, Status, _}, _Headers, Body}}) ->
    {ok, json:decode(iolist_to_binary(Body)), Status};
http_decode(Other) ->
    {error, Other}.

%%==================================================================
%% Cert + tmpdir
%%==================================================================

generate_test_cert(Dir) ->
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Cert = filename:join(Dir, "cert.pem"),
    Key  = filename:join(Dir, "key.pem"),
    Cmd = lists:flatten(io_lib:format(
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
    Base = filename:join(["/tmp", "hecate-admin-identities-test",
                          integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(Base, "placeholder")),
    Base.

rm_rf(Dir) -> _ = os:cmd("rm -rf " ++ Dir), ok.

cleanup_sup(Sup) ->
    true = unlink(Sup),
    Ref = monitor(process, Sup),
    exit(Sup, shutdown),
    receive {'DOWN', Ref, process, Sup, _} -> ok
    after 5000 -> timeout end.
