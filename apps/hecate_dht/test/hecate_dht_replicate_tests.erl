%% EUnit tests for hecate_dht_replicate — the tReplicate custodian loop.
-module(hecate_dht_replicate_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Empty store: tick walks nothing
%%---------------------------------------------------------------------

tick_on_empty_store_is_a_noop_test() ->
    Net = start_net([a]),
    #{a := {A, _}} = Net,
    R = start_replicator(A),
    Outcome = hecate_dht_replicate:tick(R),
    ?assertEqual(0, maps:get(records_seen, Outcome)),
    ?assertEqual(0, maps:get(stores_sent, Outcome)),
    stop_replicator(R),
    stop_net(Net).

%%---------------------------------------------------------------------
%% Single record + single peer: tick re-STOREs it
%%---------------------------------------------------------------------

tick_replicates_record_to_known_peer_test() ->
    Net = start_net([a, b]),
    #{a := {A, _}, b := {B, KpB}} = Net,
    BId = macula_identity:public(KpB),

    %% A holds a record and knows B.
    Record = signed_record(),
    ok = hecate_dht:put_record(A, Record),
    admitted = hecate_dht:observe(A, peer_spec(BId)),

    R = start_replicator(A, #{k => 20, per_store_timeout_ms => 500}),
    Outcome = hecate_dht_replicate:tick(R),
    ?assertEqual(1, maps:get(records_seen, Outcome)),
    ?assertEqual(1, maps:get(stores_sent, Outcome)),
    ?assertEqual(1, maps:get(acks, Outcome)),

    %% B must now have the replicated record.
    [Stored] = hecate_dht:find_local_record(B,
                                            macula_record:storage_key(Record)),
    ?assertEqual(macula_record:key(Record), macula_record:key(Stored)),
    stop_replicator(R),
    stop_net(Net).

%%---------------------------------------------------------------------
%% Multiple records + multiple peers: counts add up
%%---------------------------------------------------------------------

tick_replicates_all_records_to_all_peers_test() ->
    Net = start_net([a, b, c]),
    #{a := {A, _}, b := {_, KpB}, c := {_, KpC}} = Net,
    BId = macula_identity:public(KpB),
    CId = macula_identity:public(KpC),
    Rec1 = signed_record(),
    Rec2 = signed_record(),
    Rec3 = signed_record(),
    [ok = hecate_dht:put_record(A, R) || R <- [Rec1, Rec2, Rec3]],
    admitted = hecate_dht:observe(A, peer_spec(BId)),
    admitted = hecate_dht:observe(A, peer_spec(CId)),

    R = start_replicator(A, #{k => 20, per_store_timeout_ms => 500}),
    Outcome = hecate_dht_replicate:tick(R),
    ?assertEqual(3, maps:get(records_seen, Outcome)),
    %% 3 records × 2 peers = 6 STOREs.
    ?assertEqual(6, maps:get(stores_sent, Outcome)),
    ?assertEqual(6, maps:get(acks, Outcome)),
    stop_replicator(R),
    stop_net(Net).

%%---------------------------------------------------------------------
%% Cumulative stats accumulate across ticks
%%---------------------------------------------------------------------

cumulative_stats_accumulate_across_ticks_test() ->
    Net = start_net([a, b]),
    #{a := {A, _}, b := {_, KpB}} = Net,
    BId = macula_identity:public(KpB),
    ok = hecate_dht:put_record(A, signed_record()),
    admitted = hecate_dht:observe(A, peer_spec(BId)),

    R = start_replicator(A, #{k => 20, per_store_timeout_ms => 500}),
    _ = hecate_dht_replicate:tick(R),
    _ = hecate_dht_replicate:tick(R),
    _ = hecate_dht_replicate:tick(R),
    #{ticks := Ticks, cumulative := Cum, last_tick := LastTick} =
        hecate_dht_replicate:stats(R),
    ?assertEqual(3, Ticks),
    ?assert(is_integer(LastTick)),
    ?assertEqual(3, maps:get(records_seen, Cum)),
    ?assertEqual(3, maps:get(stores_sent, Cum)),
    ?assertEqual(3, maps:get(acks, Cum)),
    stop_replicator(R),
    stop_net(Net).

%%---------------------------------------------------------------------
%% Records with no known peers don't send anything
%%---------------------------------------------------------------------

tick_with_record_but_no_peers_sends_nothing_test() ->
    Net = start_net([a]),
    #{a := {A, _}} = Net,
    ok = hecate_dht:put_record(A, signed_record()),
    R = start_replicator(A, #{k => 20, per_store_timeout_ms => 500}),
    Outcome = hecate_dht_replicate:tick(R),
    ?assertEqual(1, maps:get(records_seen, Outcome)),
    ?assertEqual(0, maps:get(stores_sent, Outcome)),
    stop_replicator(R),
    stop_net(Net).

%%---------------------------------------------------------------------
%% Timer fires automatically on the configured interval
%%---------------------------------------------------------------------

timer_fires_on_interval_test() ->
    Net = start_net([a, b]),
    #{a := {A, _}, b := {_, KpB}} = Net,
    BId = macula_identity:public(KpB),
    ok = hecate_dht:put_record(A, signed_record()),
    admitted = hecate_dht:observe(A, peer_spec(BId)),

    %% Interval 80 ms — after 250 ms we expect ≥ 2 automatic ticks.
    R = start_replicator(A, #{interval_ms => 80,
                              per_store_timeout_ms => 500}),
    timer:sleep(250),
    #{ticks := Ticks, cumulative := Cum} = hecate_dht_replicate:stats(R),
    ?assert(Ticks >= 2),
    ?assert(maps:get(stores_sent, Cum) >= 2),
    stop_replicator(R),
    stop_net(Net).

%%=====================================================================
%% Harness
%%=====================================================================

start_replicator(Dht) ->
    start_replicator(Dht, #{}).

start_replicator(Dht, Extra) ->
    Base = #{dht                  => Dht,
             interval_ms          => 3_600_000,
             k                    => 20,
             per_store_timeout_ms => 500},
    {ok, R} = hecate_dht_replicate:start_link(maps:merge(Base, Extra)),
    R.

stop_replicator(R) ->
    hecate_dht_replicate:stop(R).

start_net(Names) ->
    Router = spawn_router(),
    Entries = [{N, make_server(Router)} || N <- Names],
    Net = maps:from_list([{N, {P, Kp}} || {N, {P, Kp}} <- Entries]),
    Table = maps:from_list([{macula_identity:public(Kp), P}
                            || {_, {P, Kp}} <- Entries]),
    Router ! {table, Table},
    put(replicate_router, Router),
    Net.

make_server(Router) ->
    Kp = macula_identity:generate(),
    SelfId = macula_identity:public(Kp),
    Send = fun(DstId, Frame) ->
        Router ! {route, SelfId, DstId, Frame}, ok
    end,
    {ok, P} = hecate_dht:start_link(#{
        self_id    => SelfId,
        identity   => Kp,
        send_frame => Send
    }),
    {P, Kp}.

stop_net(Net) ->
    maps:foreach(fun(_, {P, _}) -> hecate_dht:stop(P) end, Net),
    Router = get(replicate_router),
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

signed_record() ->
    Kp = macula_identity:generate(),
    macula_record:sign(
        macula_record:node_record(macula_identity:public(Kp), [], 0),
        Kp).
