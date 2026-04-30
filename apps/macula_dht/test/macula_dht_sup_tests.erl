%% EUnit tests for macula_dht_sup.
-module(macula_dht_sup_tests).

-include_lib("eunit/include/eunit.hrl").

start_link_returns_sup_pid_test() ->
    {ok, Sup} = macula_dht_sup:start_link(#{self_id => <<0:256>>}),
    ?assert(is_pid(Sup)),
    unlink_and_stop(Sup).

sup_starts_server_child_test() ->
    {ok, Sup} = macula_dht_sup:start_link(#{self_id => <<0:256>>}),
    {ok, Server} = macula_dht_sup:get_server(Sup),
    ?assert(is_pid(Server)),
    ?assertEqual(<<0:256>>, macula_dht:self_id(Server)),
    unlink_and_stop(Sup).

sup_propagates_opts_to_server_test() ->
    {ok, Sup} = macula_dht_sup:start_link(
                  #{self_id => <<7:256>>, k => 5, s => 3}),
    {ok, Server} = macula_dht_sup:get_server(Sup),
    ?assertMatch(#{self_id := <<7:256>>, k := 5, s := 3},
                 macula_dht:stats(Server)),
    unlink_and_stop(Sup).

sup_restarts_crashed_server_test() ->
    {ok, Sup} = macula_dht_sup:start_link(#{self_id => <<0:256>>}),
    {ok, S1} = macula_dht_sup:get_server(Sup),
    Ref = erlang:monitor(process, S1),
    exit(S1, kill),
    receive {'DOWN', Ref, process, S1, _} -> ok
    after 2_000 -> ?assert(false) end,
    %% Wait briefly for supervisor to restart.
    timer:sleep(50),
    {ok, S2} = macula_dht_sup:get_server(Sup),
    ?assertNotEqual(S1, S2),
    ?assert(is_pid(S2)),
    unlink_and_stop(Sup).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

unlink_and_stop(Sup) ->
    unlink(Sup),
    Ref = erlang:monitor(process, Sup),
    exit(Sup, shutdown),
    receive {'DOWN', Ref, process, Sup, _} -> ok
    after 2_000 -> ok end.
