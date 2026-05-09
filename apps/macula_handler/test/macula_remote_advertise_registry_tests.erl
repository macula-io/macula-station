%% EUnit tests for `macula_remote_advertise_registry'.
%%
%% The registry is a thin gen_server keyed by `(Realm, Procedure)';
%% these tests cover the API surface (register/unregister/lookup,
%% bulk purge_conn, list, single-provider replacement semantics).
-module(macula_remote_advertise_registry_tests).

-include_lib("eunit/include/eunit.hrl").

-define(REALM,  <<1:256>>).
-define(REALM2, <<2:256>>).

setup() ->
    {ok, Pid} = macula_remote_advertise_registry:start_link(),
    Pid.

setup_short_tombstone() ->
    {ok, Pid} = macula_remote_advertise_registry:start_link(
                  #{tombstone_ms => 50}),
    Pid.

teardown(Pid) ->
    macula_remote_advertise_registry:stop(Pid).

with_registry(Body) ->
    Pid = setup(),
    try Body(Pid) after teardown(Pid) end.

register_and_lookup_test() ->
    with_registry(fun(Pid) ->
        Adv     = <<10:256>>,
        ConnPid = self(),
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => Adv, conn_pid => ConnPid}),
        ?assertEqual(
           {ok, #{advertiser => Adv, conn_pid => ConnPid}},
           macula_remote_advertise_registry:lookup(Pid, ?REALM, <<"_p">>))
    end).

lookup_returns_not_found_for_missing_test() ->
    with_registry(fun(Pid) ->
        ?assertEqual(
           {error, not_found},
           macula_remote_advertise_registry:lookup(Pid, ?REALM, <<"_x">>))
    end).

re_register_replaces_prior_test() ->
    with_registry(fun(Pid) ->
        Adv1 = <<11:256>>, Adv2 = <<12:256>>,
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => Adv1, conn_pid => self()}),
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => Adv2, conn_pid => self()}),
        {ok, #{advertiser := A}} =
            macula_remote_advertise_registry:lookup(Pid, ?REALM, <<"_p">>),
        ?assertEqual(Adv2, A)
    end).

unregister_removes_entry_test() ->
    with_registry(fun(Pid) ->
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => <<13:256>>, conn_pid => self()}),
        ok = macula_remote_advertise_registry:unregister(
               Pid, ?REALM, <<"_p">>),
        ?assertEqual(
           {error, not_found},
           macula_remote_advertise_registry:lookup(Pid, ?REALM, <<"_p">>))
    end).

purge_conn_drops_only_matching_test() ->
    with_registry(fun(Pid) ->
        Conn1 = self(),
        Conn2 = spawn(fun() -> receive _ -> ok end end),
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_a">>,
               #{advertiser => <<14:256>>, conn_pid => Conn1}),
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_b">>,
               #{advertiser => <<15:256>>, conn_pid => Conn2}),
        ok = macula_remote_advertise_registry:purge_conn(Pid, Conn1),
        ?assertMatch({error, not_found},
                     macula_remote_advertise_registry:lookup(
                       Pid, ?REALM, <<"_a">>)),
        ?assertMatch({ok, #{conn_pid := Conn2}},
                     macula_remote_advertise_registry:lookup(
                       Pid, ?REALM, <<"_b">>)),
        exit(Conn2, kill)
    end).

list_returns_all_entries_test() ->
    with_registry(fun(Pid) ->
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_a">>,
               #{advertiser => <<16:256>>, conn_pid => self()}),
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM2, <<"_b">>,
               #{advertiser => <<17:256>>, conn_pid => self()}),
        L = macula_remote_advertise_registry:list(Pid),
        ?assertEqual(2, length(L)),
        Realms = [R || {R, _, _} <- L],
        ?assert(lists:member(?REALM,  Realms)),
        ?assert(lists:member(?REALM2, Realms))
    end).

different_realms_isolated_test() ->
    with_registry(fun(Pid) ->
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => <<18:256>>, conn_pid => self()}),
        ?assertMatch({error, not_found},
                     macula_remote_advertise_registry:lookup(
                       Pid, ?REALM2, <<"_p">>))
    end).

%%--- Tombstones -----------------------------------------------------

unregister_tombstones_block_register_test() ->
    Pid = setup_short_tombstone(),
    try
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => <<19:256>>, conn_pid => self()}),
        ok = macula_remote_advertise_registry:unregister(
               Pid, ?REALM, <<"_p">>),
        %% Tombstone is active — register is a no-op.
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => <<20:256>>, conn_pid => self()}),
        ?assertMatch({error, not_found},
                     macula_remote_advertise_registry:lookup(
                       Pid, ?REALM, <<"_p">>))
    after
        teardown(Pid)
    end.

tombstone_expires_and_register_resumes_test() ->
    Pid = setup_short_tombstone(),
    try
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => <<21:256>>, conn_pid => self()}),
        ok = macula_remote_advertise_registry:unregister(
               Pid, ?REALM, <<"_p">>),
        timer:sleep(150),
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => <<22:256>>, conn_pid => self()}),
        ?assertMatch({ok, #{advertiser := <<22:256>>}},
                     macula_remote_advertise_registry:lookup(
                       Pid, ?REALM, <<"_p">>))
    after
        teardown(Pid)
    end.

tombstone_allows_same_source_readvertise_test() ->
    Pid = setup_short_tombstone(),
    try
        Adv = <<25:256>>,
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => Adv, conn_pid => self()}),
        ok = macula_remote_advertise_registry:unregister(
               Pid, ?REALM, <<"_p">>),
        %% Same advertiser punches through the tombstone (reconnect
        %% case): the original source IS allowed to re-advertise.
        Conn2 = spawn(fun() -> receive _ -> ok end end),
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => Adv, conn_pid => Conn2}),
        ?assertMatch({ok, #{advertiser := Adv, conn_pid := Conn2}},
                     macula_remote_advertise_registry:lookup(
                       Pid, ?REALM, <<"_p">>)),
        exit(Conn2, kill)
    after
        teardown(Pid)
    end.

purge_conn_tombstones_block_register_test() ->
    Pid = setup_short_tombstone(),
    try
        Conn = self(),
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => <<23:256>>, conn_pid => Conn}),
        ok = macula_remote_advertise_registry:purge_conn(Pid, Conn),
        %% Gossip-resurrection attempt during the disconnect window.
        ok = macula_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => <<24:256>>, conn_pid => self()}),
        ?assertMatch({error, not_found},
                     macula_remote_advertise_registry:lookup(
                       Pid, ?REALM, <<"_p">>))
    after
        teardown(Pid)
    end.
