%% EUnit tests for the STORE fan-out orchestrator — small harness
%% with N custodian DHT servers all reachable through a single router.
-module(hecate_dht_store_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% send_store wire op — single peer happy path
%%---------------------------------------------------------------------

send_store_records_replica_on_custodian_test() ->
    #{a := {A, _}, b := {B, KpB}} = Net = start_pair(),
    BId = hecate_identity:public(KpB),
    Record = signed_record(),
    {ok, #{stored := true, reason := undefined}} =
        hecate_dht:send_store(A, BId, Record),
    [Stored] = hecate_dht:find_local_record(B, hecate_record:storage_key(Record)),
    ?assertEqual(hecate_record:key(Record), hecate_record:key(Stored)),
    stop_net(Net).

send_store_no_transport_returns_error_test() ->
    Kp = hecate_identity:generate(),
    {ok, D} = hecate_dht:start_link(#{self_id => hecate_identity:public(Kp)}),
    ?assertEqual({error, no_transport},
                 hecate_dht:send_store(D, <<1:256>>, signed_record())),
    hecate_dht:stop(D).

send_store_timeout_on_silent_peer_test() ->
    Kp = hecate_identity:generate(),
    {ok, D} = hecate_dht:start_link(#{
        self_id    => hecate_identity:public(Kp),
        identity   => Kp,
        send_frame => fun(_, _) -> ok end
    }),
    ?assertEqual({error, timeout},
                 hecate_dht:send_store(D, <<1:256>>, signed_record(), 120)),
    hecate_dht:stop(D).

%%---------------------------------------------------------------------
%% Orchestrated store/3 — fan-out + quorum
%%---------------------------------------------------------------------

store_succeeds_with_quorum_of_explicit_custodians_test() ->
    %% Four custodians, quorum 3. All online → ok outcome with acks=4.
    Net = start_net([a, b, c, d, e]),
    #{a := {A, _}} = Net,
    Targets = [ref_for(Net, N) || N <- [b, c, d, e]],
    Record = signed_record(),
    Result = hecate_dht:store(A, Record, #{k => 4, quorum => 3,
                                           candidates => Targets,
                                           constraints =>
                                               diverse_constraints(4),
                                           store_timeout_ms => 500,
                                           overall_timeout_ms => 1_500}),
    {ok, Outcome} = Result,
    ?assertEqual(4, maps:get(acks, Outcome)),
    ?assertEqual(0, maps:get(nacks, Outcome)),
    ?assertEqual(0, maps:get(timeouts, Outcome)),
    ?assertEqual(4, length(maps:get(chosen, Outcome))),
    stop_net(Net).

store_fails_with_quorum_not_met_on_silent_custodians_test() ->
    %% Two explicit custodians — one responds, one has no live peer
    %% in the router table (fake NodeId). Quorum 2 → fails.
    Net = start_net([a, b]),
    #{a := {A, _}} = Net,
    BRef = ref_for(Net, b),
    FakeRef = dummy_ref(<<99:256>>),
    {error, quorum_not_met, Outcome} =
        hecate_dht:store(A, signed_record(),
                         #{k => 2, quorum => 2,
                           candidates => [BRef, FakeRef],
                           constraints => diverse_constraints(2),
                           store_timeout_ms => 200,
                           overall_timeout_ms => 500}),
    ?assertEqual(1, maps:get(acks, Outcome)),
    ?assertEqual(1, maps:get(timeouts, Outcome)),
    stop_net(Net).

store_propagates_placement_degraded_flag_test() ->
    %% Only one custodian but constraints require 3 ASNs → degraded.
    Net = start_net([a, b]),
    #{a := {A, _}} = Net,
    BRef = ref_for(Net, b),
    {ok, Outcome} =
        hecate_dht:store(A, signed_record(),
                         #{k => 1, quorum => 1,
                           candidates => [BRef],
                           constraints => #{k => 1,
                                            asn_min => 3,
                                            country_min => 1,
                                            tier_min => 1,
                                            operator_max => 10},
                           store_timeout_ms => 500,
                           overall_timeout_ms => 1_000}),
    ?assert(maps:get(degraded, Outcome)),
    stop_net(Net).

store_no_candidates_returns_error_test() ->
    Kp = hecate_identity:generate(),
    {ok, D} = hecate_dht:start_link(#{
        self_id    => hecate_identity:public(Kp),
        identity   => Kp,
        send_frame => fun(_, _) -> ok end
    }),
    {error, no_candidates, _} =
        hecate_dht:store(D, signed_record(),
                         #{candidates => [],
                           overall_timeout_ms => 500,
                           store_timeout_ms => 200}),
    hecate_dht:stop(D).

%%=====================================================================
%% Harness — N DHT servers wired via a single dispatch router
%%=====================================================================

start_pair() ->
    start_net([a, b]).

start_net(Names) ->
    Router = spawn_router(),
    Entries = [{N, make_server(Router)} || N <- Names],
    Net = maps:from_list([{N, {P, Kp}} || {N, {P, Kp}} <- Entries]),
    Table = maps:from_list([{hecate_identity:public(Kp), P}
                            || {_, {P, Kp}} <- Entries]),
    Router ! {table, Table},
    put(store_router, Router),
    Net.

make_server(Router) ->
    Kp = hecate_identity:generate(),
    SelfId = hecate_identity:public(Kp),
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
    Router = get(store_router),
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

%%=====================================================================
%% Fixtures
%%=====================================================================

ref_for(Net, Name) ->
    #{Name := {_, Kp}} = Net,
    NodeId = hecate_identity:public(Kp),
    #{node_id      => NodeId,
      station_id   => NodeId,
      addresses    => [],
      tier         => 1,
      asn          => 64512,
      country      => <<"BE">>,
      last_seen_at => 1}.

dummy_ref(NodeId) ->
    #{node_id      => NodeId,
      station_id   => NodeId,
      addresses    => [],
      tier         => 1,
      asn          => 64999,
      country      => <<"JP">>,
      last_seen_at => 1}.

signed_record() ->
    Kp = hecate_identity:generate(),
    hecate_record:sign(
        hecate_record:node_record(hecate_identity:public(Kp), [], 0),
        Kp).

diverse_constraints(K) ->
    #{k => K, asn_min => 1, country_min => 1, tier_min => 1,
      operator_max => 20}.
