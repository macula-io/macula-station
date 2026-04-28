%% EUnit tests for `hecate_remote_advertise_registry'.
%%
%% The registry is a thin gen_server keyed by `(Realm, Procedure)';
%% these tests cover the API surface (register/unregister/lookup,
%% bulk purge_conn, list, single-provider replacement semantics).
-module(hecate_remote_advertise_registry_tests).

-include_lib("eunit/include/eunit.hrl").

-define(REALM,  <<1:256>>).
-define(REALM2, <<2:256>>).

setup() ->
    {ok, Pid} = hecate_remote_advertise_registry:start_link(),
    Pid.

teardown(Pid) ->
    hecate_remote_advertise_registry:stop(Pid).

with_registry(Body) ->
    Pid = setup(),
    try Body(Pid) after teardown(Pid) end.

register_and_lookup_test() ->
    with_registry(fun(Pid) ->
        Adv     = <<10:256>>,
        ConnPid = self(),
        ok = hecate_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => Adv, conn_pid => ConnPid}),
        ?assertEqual(
           {ok, #{advertiser => Adv, conn_pid => ConnPid}},
           hecate_remote_advertise_registry:lookup(Pid, ?REALM, <<"_p">>))
    end).

lookup_returns_not_found_for_missing_test() ->
    with_registry(fun(Pid) ->
        ?assertEqual(
           {error, not_found},
           hecate_remote_advertise_registry:lookup(Pid, ?REALM, <<"_x">>))
    end).

re_register_replaces_prior_test() ->
    with_registry(fun(Pid) ->
        Adv1 = <<11:256>>, Adv2 = <<12:256>>,
        ok = hecate_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => Adv1, conn_pid => self()}),
        ok = hecate_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => Adv2, conn_pid => self()}),
        {ok, #{advertiser := A}} =
            hecate_remote_advertise_registry:lookup(Pid, ?REALM, <<"_p">>),
        ?assertEqual(Adv2, A)
    end).

unregister_removes_entry_test() ->
    with_registry(fun(Pid) ->
        ok = hecate_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => <<13:256>>, conn_pid => self()}),
        ok = hecate_remote_advertise_registry:unregister(
               Pid, ?REALM, <<"_p">>),
        ?assertEqual(
           {error, not_found},
           hecate_remote_advertise_registry:lookup(Pid, ?REALM, <<"_p">>))
    end).

purge_conn_drops_only_matching_test() ->
    with_registry(fun(Pid) ->
        Conn1 = self(),
        Conn2 = spawn(fun() -> receive _ -> ok end end),
        ok = hecate_remote_advertise_registry:register(
               Pid, ?REALM, <<"_a">>,
               #{advertiser => <<14:256>>, conn_pid => Conn1}),
        ok = hecate_remote_advertise_registry:register(
               Pid, ?REALM, <<"_b">>,
               #{advertiser => <<15:256>>, conn_pid => Conn2}),
        ok = hecate_remote_advertise_registry:purge_conn(Pid, Conn1),
        ?assertMatch({error, not_found},
                     hecate_remote_advertise_registry:lookup(
                       Pid, ?REALM, <<"_a">>)),
        ?assertMatch({ok, #{conn_pid := Conn2}},
                     hecate_remote_advertise_registry:lookup(
                       Pid, ?REALM, <<"_b">>)),
        exit(Conn2, kill)
    end).

list_returns_all_entries_test() ->
    with_registry(fun(Pid) ->
        ok = hecate_remote_advertise_registry:register(
               Pid, ?REALM, <<"_a">>,
               #{advertiser => <<16:256>>, conn_pid => self()}),
        ok = hecate_remote_advertise_registry:register(
               Pid, ?REALM2, <<"_b">>,
               #{advertiser => <<17:256>>, conn_pid => self()}),
        L = hecate_remote_advertise_registry:list(Pid),
        ?assertEqual(2, length(L)),
        Realms = [R || {R, _, _} <- L],
        ?assert(lists:member(?REALM,  Realms)),
        ?assert(lists:member(?REALM2, Realms))
    end).

different_realms_isolated_test() ->
    with_registry(fun(Pid) ->
        ok = hecate_remote_advertise_registry:register(
               Pid, ?REALM, <<"_p">>,
               #{advertiser => <<18:256>>, conn_pid => self()}),
        ?assertMatch({error, not_found},
                     hecate_remote_advertise_registry:lookup(
                       Pid, ?REALM2, <<"_p">>))
    end).
