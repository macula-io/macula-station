%% EUnit tests for the owner republish + custodian expire loops.
-module(hecate_dht_lifecycle_tests).

-include_lib("eunit/include/eunit.hrl").

%%=====================================================================
%% tRepublish — owner loop
%%=====================================================================

republish_empty_store_is_noop_test() ->
    Net = start_net([a]),
    #{a := {A, KpA}} = Net,
    R = start_republisher(A, KpA),
    Outcome = hecate_dht_republish:tick(R),
    ?assertEqual(0, maps:get(records_seen, Outcome)),
    ?assertEqual(0, maps:get(owned, Outcome)),
    stop_republisher(R),
    stop_net(Net).

republish_skips_non_owned_records_test() ->
    Net = start_net([a]),
    #{a := {A, KpA}} = Net,
    %% A holds a record signed by someone ELSE (a random key).
    KpOther = macula_identity:generate(),
    Record  = hecate_record:sign(
                hecate_record:node_record(macula_identity:public(KpOther),
                                          [], 0),
                KpOther),
    ok = hecate_dht:put_record(A, Record),

    R = start_republisher(A, KpA),
    Outcome = hecate_dht_republish:tick(R),
    ?assertEqual(1, maps:get(records_seen, Outcome)),
    ?assertEqual(0, maps:get(owned, Outcome)),
    ?assertEqual(0, maps:get(republished, Outcome)),
    stop_republisher(R),
    stop_net(Net).

republish_refreshes_owned_record_version_test() ->
    Net = start_net([a, b]),
    #{a := {A, KpA}, b := {_, KpB}} = Net,
    AId = macula_identity:public(KpA),
    BId = macula_identity:public(KpB),

    %% A's own node_record, owned by A.
    Record = hecate_record:sign(hecate_record:node_record(AId, [], 0), KpA),
    ok = hecate_dht:put_record(A, Record),
    OriginalVersion = hecate_record:version(Record),
    admitted = hecate_dht:observe(A, peer_spec(BId)),

    timer:sleep(2),

    R = start_republisher(A, KpA, #{store_opts => short_store_opts()}),
    Outcome = hecate_dht_republish:tick(R),
    ?assertEqual(1, maps:get(records_seen, Outcome)),
    ?assertEqual(1, maps:get(owned, Outcome)),

    %% Local store now holds a refreshed copy with a different version.
    [Refreshed] = hecate_dht:find_local_record(A, AId),
    ?assertNotEqual(OriginalVersion, hecate_record:version(Refreshed)),

    stop_republisher(R),
    stop_net(Net).

republish_propagates_to_custodian_test() ->
    Net = start_net([a, b]),
    #{a := {A, KpA}, b := {B, KpB}} = Net,
    AId = macula_identity:public(KpA),
    BId = macula_identity:public(KpB),

    Record = hecate_record:sign(hecate_record:node_record(AId, [], 0), KpA),
    ok = hecate_dht:put_record(A, Record),
    admitted = hecate_dht:observe(A, peer_spec(BId)),

    R = start_republisher(A, KpA, #{store_opts => short_store_opts()}),
    Outcome = hecate_dht_republish:tick(R),
    ?assertEqual(1, maps:get(republished, Outcome)),
    [OnB] = hecate_dht:find_local_record(B, AId),
    ?assertEqual(AId, hecate_record:key(OnB)),

    stop_republisher(R),
    stop_net(Net).

republish_no_candidates_is_counted_test() ->
    %% A has an owned record but no peers in RT → store/3 returns
    %% no_candidates. Local store still holds the refreshed version.
    Net = start_net([a]),
    #{a := {A, KpA}} = Net,
    AId = macula_identity:public(KpA),
    Record = hecate_record:sign(hecate_record:node_record(AId, [], 0), KpA),
    ok = hecate_dht:put_record(A, Record),

    R = start_republisher(A, KpA, #{store_opts => short_store_opts()}),
    Outcome = hecate_dht_republish:tick(R),
    ?assertEqual(1, maps:get(owned, Outcome)),
    ?assertEqual(1, maps:get(no_candidates, Outcome)),
    ?assertEqual(0, maps:get(republished, Outcome)),

    stop_republisher(R),
    stop_net(Net).

%%=====================================================================
%% tExpire — custodian reaper
%%=====================================================================

expire_empty_store_is_noop_test() ->
    Net = start_net([a]),
    #{a := {A, _}} = Net,
    R = start_expire(A),
    Outcome = hecate_dht_expire:tick(R),
    ?assertEqual(0, maps:get(records_seen, Outcome)),
    ?assertEqual(0, maps:get(expired, Outcome)),
    stop_expire(R),
    stop_net(Net).

expire_retains_fresh_record_test() ->
    Net = start_net([a]),
    #{a := {A, _}} = Net,
    Record = fresh_record(60_000),
    ok = hecate_dht:put_record(A, Record),
    R = start_expire(A),
    Outcome = hecate_dht_expire:tick(R),
    ?assertEqual(1, maps:get(records_seen, Outcome)),
    ?assertEqual(0, maps:get(expired, Outcome)),
    ?assertEqual(1, maps:get(kept, Outcome)),
    ?assertEqual(1, hecate_dht:record_count(A)),
    stop_expire(R),
    stop_net(Net).

expire_reaps_past_ttl_record_test() ->
    Net = start_net([a]),
    #{a := {A, _}} = Net,
    %% TTL 1 ms; sleep long enough to exceed.
    Record = fresh_record(1),
    ok = hecate_dht:put_record(A, Record),
    timer:sleep(5),

    R = start_expire(A),
    Outcome = hecate_dht_expire:tick(R),
    ?assertEqual(1, maps:get(records_seen, Outcome)),
    ?assertEqual(1, maps:get(expired, Outcome)),
    ?assertEqual(0, hecate_dht:record_count(A)),
    stop_expire(R),
    stop_net(Net).

expire_separates_fresh_from_stale_records_test() ->
    Net = start_net([a]),
    #{a := {A, _}} = Net,
    %% Two records — one long-lived, one soon to expire.
    LongLived = fresh_record(60_000),
    Ephemeral = fresh_record(1),
    ok = hecate_dht:put_record(A, LongLived),
    ok = hecate_dht:put_record(A, Ephemeral),
    timer:sleep(5),

    R = start_expire(A),
    Outcome = hecate_dht_expire:tick(R),
    ?assertEqual(2, maps:get(records_seen, Outcome)),
    ?assertEqual(1, maps:get(expired, Outcome)),
    ?assertEqual(1, maps:get(kept, Outcome)),
    ?assertEqual(1, hecate_dht:record_count(A)),
    [Kept] = hecate_dht:find_local_record(
               A, hecate_record:storage_key(LongLived)),
    ?assertEqual(hecate_record:key(LongLived), hecate_record:key(Kept)),
    stop_expire(R),
    stop_net(Net).

expire_timer_fires_on_interval_test() ->
    Net = start_net([a]),
    #{a := {A, _}} = Net,
    R = start_expire(A, #{interval_ms => 60}),
    timer:sleep(180),
    #{ticks := Ticks} = hecate_dht_expire:stats(R),
    ?assert(Ticks >= 2),
    stop_expire(R),
    stop_net(Net).

%%=====================================================================
%% Harness
%%=====================================================================

start_republisher(Dht, Identity) ->
    start_republisher(Dht, Identity, #{}).

start_republisher(Dht, Identity, Extra) ->
    Base = #{dht => Dht, identity => Identity, interval_ms => 3_600_000},
    {ok, R} = hecate_dht_republish:start_link(maps:merge(Base, Extra)),
    R.

stop_republisher(R) -> hecate_dht_republish:stop(R).

start_expire(Dht) ->
    start_expire(Dht, #{}).

start_expire(Dht, Extra) ->
    Base = #{dht => Dht, interval_ms => 3_600_000},
    {ok, R} = hecate_dht_expire:start_link(maps:merge(Base, Extra)),
    R.

stop_expire(R) -> hecate_dht_expire:stop(R).

short_store_opts() ->
    %% Tests run with a tiny peer pool, so force K/quorum down to 1
    %% and relax diversity constraints.
    #{k => 1,
      quorum => 1,
      constraints => #{k => 1, asn_min => 1, country_min => 1,
                       tier_min => 1, operator_max => 20},
      store_timeout_ms => 200,
      overall_timeout_ms => 500}.

start_net(Names) ->
    Router = spawn_router(),
    Entries = [{N, make_server(Router)} || N <- Names],
    Net = maps:from_list([{N, {P, Kp}} || {N, {P, Kp}} <- Entries]),
    Table = maps:from_list([{macula_identity:public(Kp), P}
                            || {_, {P, Kp}} <- Entries]),
    Router ! {table, Table},
    put(lifecycle_router, Router),
    Net.

make_server(Router) ->
    Kp = macula_identity:generate(),
    SelfId = macula_identity:public(Kp),
    Send = fun(DstId, Frame) ->
        Router ! {route, SelfId, DstId, Frame}, ok
    end,
    {ok, P} = hecate_dht:start_link(#{self_id    => SelfId,
                                      identity   => Kp,
                                      send_frame => Send}),
    {P, Kp}.

stop_net(Net) ->
    maps:foreach(fun(_, {P, _}) -> hecate_dht:stop(P) end, Net),
    Router = get(lifecycle_router),
    Router ! stop.

spawn_router() ->
    spawn(fun() -> router_loop(undefined) end).

router_loop(Table) ->
    receive
        {table, T} -> router_loop(T);
        {route, From, Dst, Frame} ->
            route_frame(Table, From, Dst, Frame),
            router_loop(Table);
        stop -> ok
    end.

route_frame(undefined, _, _, _) -> ok;
route_frame(Table, From, Dst, Frame) ->
    case maps:find(Dst, Table) of
        {ok, Pid} -> hecate_dht:handle_frame(Pid, From, Frame);
        error     -> ok
    end.

peer_spec(NodeId) ->
    #{node_id => NodeId, asn => 64512, country => <<"BE">>, tier => t1}.

fresh_record(TtlMs) ->
    Kp = macula_identity:generate(),
    hecate_record:sign(
        hecate_record:node_record(macula_identity:public(Kp), [], 0,
                                  #{ttl_ms => TtlMs}),
        Kp).
