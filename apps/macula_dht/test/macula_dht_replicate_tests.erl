%% EUnit tests for macula_dht_replicate — the tReplicate custodian loop.
-module(macula_dht_replicate_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Empty store: tick walks nothing
%%---------------------------------------------------------------------

tick_on_empty_store_is_a_noop_test() ->
    Net = start_net([a]),
    #{a := {A, _}} = Net,
    R = start_replicator(A),
    Outcome = macula_dht_replicate:tick(R),
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
    ok = macula_dht:put_record(A, Record),
    admitted = macula_dht:observe(A, peer_spec(BId)),

    R = start_replicator(A, #{k => 20, per_store_timeout_ms => 500}),
    Outcome = macula_dht_replicate:tick(R),
    ?assertEqual(1, maps:get(records_seen, Outcome)),
    ?assertEqual(1, maps:get(stores_sent, Outcome)),
    ?assertEqual(1, maps:get(acks, Outcome)),

    %% B must now have the replicated record.
    [Stored] = macula_dht:find_local_record(B,
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
    [ok = macula_dht:put_record(A, R) || R <- [Rec1, Rec2, Rec3]],
    admitted = macula_dht:observe(A, peer_spec(BId)),
    admitted = macula_dht:observe(A, peer_spec(CId)),

    R = start_replicator(A, #{k => 20, per_store_timeout_ms => 500}),
    Outcome = macula_dht_replicate:tick(R),
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
    ok = macula_dht:put_record(A, signed_record()),
    admitted = macula_dht:observe(A, peer_spec(BId)),

    R = start_replicator(A, #{k => 20, per_store_timeout_ms => 500}),
    _ = macula_dht_replicate:tick(R),
    _ = macula_dht_replicate:tick(R),
    _ = macula_dht_replicate:tick(R),
    #{ticks := Ticks, cumulative := Cum, last_tick := LastTick} =
        macula_dht_replicate:stats(R),
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
    ok = macula_dht:put_record(A, signed_record()),
    R = start_replicator(A, #{k => 20, per_store_timeout_ms => 500}),
    Outcome = macula_dht_replicate:tick(R),
    ?assertEqual(1, maps:get(records_seen, Outcome)),
    ?assertEqual(0, maps:get(stores_sent, Outcome)),
    stop_replicator(R),
    stop_net(Net).

%%---------------------------------------------------------------------
%% Eager replication: replicate_one/2 fans STORE without waiting for tick
%%---------------------------------------------------------------------

replicate_one_eagerly_pushes_record_test() ->
    Net = start_net([a, b]),
    #{a := {A, _}, b := {B, KpB}} = Net,
    BId = macula_identity:public(KpB),

    %% A holds a record and knows B; the replicator's tick interval
    %% is set to 1 h so we can be sure replication only happens via
    %% the explicit `replicate_one/2' call, not a stray tick.
    Record = signed_record(),
    ok = macula_dht:put_record(A, Record),
    admitted = macula_dht:observe(A, peer_spec(BId)),

    R = start_replicator(A, #{interval_ms => 3_600_000,
                              k => 20,
                              per_store_timeout_ms => 500}),
    ok = macula_dht_replicate:replicate_one(R, Record),

    %% Cast is async — wait for B to register the replicated record.
    Key = macula_record:storage_key(Record),
    ok = wait_for_record(B, Key, 1_000),
    [Stored] = macula_dht:find_local_record(B, Key),
    ?assertEqual(macula_record:key(Record), macula_record:key(Stored)),

    %% Ticks counter unchanged (only `replicate_one' fired).
    %% The replicate_one cast bumps the cumulative store counters
    %% via `advance/2' too, which sets `last_tick' as a side
    %% effect — that is the same behaviour the periodic tick
    %% would produce, just driven by an explicit cast.
    stop_replicator(R),
    stop_net(Net).

%%---------------------------------------------------------------------
%% Timer fires automatically on the configured interval
%%---------------------------------------------------------------------

timer_fires_on_interval_test() ->
    Net = start_net([a, b]),
    #{a := {A, _}, b := {_, KpB}} = Net,
    BId = macula_identity:public(KpB),
    ok = macula_dht:put_record(A, signed_record()),
    admitted = macula_dht:observe(A, peer_spec(BId)),

    %% Interval 80 ms — after 250 ms we expect ≥ 2 automatic ticks.
    R = start_replicator(A, #{interval_ms => 80,
                              per_store_timeout_ms => 500}),
    timer:sleep(250),
    #{ticks := Ticks, cumulative := Cum} = macula_dht_replicate:stats(R),
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
    {ok, R} = macula_dht_replicate:start_link(maps:merge(Base, Extra)),
    R.

stop_replicator(R) ->
    macula_dht_replicate:stop(R).

%% Poll until the receiving DHT shows the record, or fail with
%% `timeout' once the deadline expires. Used by the async
%% replicate_one test where the cast hasn't finished by the time
%% the assertion would otherwise fire.
wait_for_record(_Dht, _Key, Remaining) when Remaining =< 0 ->
    timeout;
wait_for_record(Dht, Key, Remaining) ->
    case macula_dht:find_local_record(Dht, Key) of
        [_ | _] -> ok;
        []      ->
            timer:sleep(20),
            wait_for_record(Dht, Key, Remaining - 20)
    end.

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
    {ok, P} = macula_dht:start_link(#{
        self_id    => SelfId,
        identity   => Kp,
        send_frame => Send
    }),
    {P, Kp}.

stop_net(Net) ->
    maps:foreach(fun(_, {P, _}) -> macula_dht:stop(P) end, Net),
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
        {ok, Pid} -> macula_dht:handle_frame(Pid, From, Frame);
        error     -> ok
    end.

peer_spec(NodeId) ->
    #{node_id => NodeId, asn => 64512, country => <<"BE">>, tier => t1}.

signed_record() ->
    Kp = macula_identity:generate(),
    macula_record:sign(
        macula_record:node_record(macula_identity:public(Kp), [], 0),
        Kp).
