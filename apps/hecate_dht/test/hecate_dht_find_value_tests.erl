%% EUnit tests for the FIND_VALUE / VALUE wire op — two-server
%% harness with a dispatch router between them.
-module(hecate_dht_find_value_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% VALUE path — B has the record, A's find_value returns {value, _}
%%---------------------------------------------------------------------

find_value_hit_returns_value_from_peer_test() ->
    Net = start_pair(),
    #{a := {A, _KpA}, b := {B, KpB}} = Net,
    BId = hecate_identity:public(KpB),

    %% Seed B's record store with a realm_directory record.
    RealmKp  = hecate_identity:generate(),
    RealmId  = hecate_identity:public(RealmKp),
    Rec = hecate_record:sign(
            hecate_record:realm_directory(RealmId, <<"test realm">>,
                                          RealmId),
            RealmKp),
    ok = hecate_dht:put_record(B, Rec),

    Result = hecate_dht:find_value(A, RealmId, BId),
    ?assertMatch({value, [_]}, Result),
    {value, [Returned]} = Result,
    ?assertEqual(hecate_record:storage_key(Rec),
                 hecate_record:storage_key(Returned)),
    stop_pair(Net).

%%---------------------------------------------------------------------
%% NODES fallback — B doesn't have the record, replies with NODES
%%---------------------------------------------------------------------

find_value_miss_falls_back_to_nodes_test() ->
    Net = start_pair(),
    #{a := {A, _}, b := {B, KpB}} = Net,
    BId = hecate_identity:public(KpB),

    %% B knows a couple of peers but has no record at the queried key.
    admitted = hecate_dht:observe(B, peer_spec(<<1:256>>)),
    admitted = hecate_dht:observe(B, peer_spec(<<2:256>>)),

    Result = hecate_dht:find_value(A, <<99:256>>, BId),
    ?assertMatch({nodes, [_ | _]}, Result),
    {nodes, Refs} = Result,
    Ids = lists:sort([maps:get(node_id, R) || R <- Refs]),
    ?assertEqual(lists:sort([<<1:256>>, <<2:256>>]), Ids),
    stop_pair(Net).

%%---------------------------------------------------------------------
%% Multiple records at same key returned together (procedure lookup)
%%---------------------------------------------------------------------

find_value_returns_all_records_at_storage_key_test() ->
    Net = start_pair(),
    #{a := {A, _}, b := {B, KpB}} = Net,
    BId = hecate_identity:public(KpB),
    Uri = <<"mcp://news/headlines">>,
    UriHash = crypto:hash(sha256, Uri),

    Kp1 = hecate_identity:generate(),
    Kp2 = hecate_identity:generate(),
    Rec1 = hecate_record:sign(
             hecate_record:procedure_advertisement(
                 hecate_identity:public(Kp1), Uri,
                 crypto:strong_rand_bytes(32)), Kp1),
    Rec2 = hecate_record:sign(
             hecate_record:procedure_advertisement(
                 hecate_identity:public(Kp2), Uri,
                 crypto:strong_rand_bytes(32)), Kp2),
    ok = hecate_dht:put_record(B, Rec1),
    ok = hecate_dht:put_record(B, Rec2),

    {value, Records} = hecate_dht:find_value(A, UriHash, BId),
    ?assertEqual(2, length(Records)),
    stop_pair(Net).

%%---------------------------------------------------------------------
%% Timeout
%%---------------------------------------------------------------------

find_value_timeout_on_silent_peer_test() ->
    Kp = hecate_identity:generate(),
    {ok, A} = hecate_dht:start_link(#{
        self_id                 => hecate_identity:public(Kp),
        identity                => Kp,
        send_frame              => fun(_, _) -> ok end,
        find_node_timeout_ms    => 120
    }),
    ?assertEqual({error, timeout},
                 hecate_dht:find_value(A, <<1:256>>, <<2:256>>, 120)),
    hecate_dht:stop(A).

find_value_no_transport_returns_error_test() ->
    Kp = hecate_identity:generate(),
    {ok, D} = hecate_dht:start_link(#{self_id => hecate_identity:public(Kp)}),
    ?assertEqual({error, no_transport},
                 hecate_dht:find_value(D, <<1:256>>, <<2:256>>)),
    hecate_dht:stop(D).

%%=====================================================================
%% Harness
%%=====================================================================

start_pair() ->
    KpA = hecate_identity:generate(),
    KpB = hecate_identity:generate(),
    AId = hecate_identity:public(KpA),
    BId = hecate_identity:public(KpB),
    Router = spawn_router(),
    SendFrom = fun(FromId) ->
        fun(DstId, Frame) ->
            Router ! {route, FromId, DstId, Frame}, ok
        end
    end,
    {ok, A} = hecate_dht:start_link(#{
        self_id              => AId,
        identity             => KpA,
        send_frame           => SendFrom(AId),
        find_node_timeout_ms => 500
    }),
    {ok, B} = hecate_dht:start_link(#{
        self_id              => BId,
        identity             => KpB,
        send_frame           => SendFrom(BId),
        find_node_timeout_ms => 500
    }),
    Router ! {table, #{AId => A, BId => B}},
    put(fv_router, Router),
    #{a => {A, KpA}, b => {B, KpB}}.

stop_pair(#{a := {A, _}, b := {B, _}}) ->
    Router = get(fv_router),
    Router ! stop,
    hecate_dht:stop(A),
    hecate_dht:stop(B).

spawn_router() ->
    spawn(fun() -> router_loop(undefined) end).

router_loop(Table) ->
    receive
        {table, T}                -> router_loop(T);
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
