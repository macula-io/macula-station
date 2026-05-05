%% @doc Hecate DHT — public facade.
%%
%% Every operation is a thin delegation to `macula_dht_server'. This
%% module is the only one external callers should use: the server,
%% its supervisor, and the pure primitives (`macula_dht_xor',
%% `macula_dht_entry', `macula_dht_bucket', `macula_dht_routing_table',
%% `macula_dht_siblings') are implementation detail.
%%
%% == Usage ==
%%
%% ```
%% {ok, Dht} = macula_dht:start_link(#{self_id => Self}),
%% admitted  = macula_dht:observe(Dht, #{node_id => PeerId,
%%                                       asn => 64512,
%%                                       country => <<"BE">>,
%%                                       tier => t1}),
%% [Peer|_]  = macula_dht:k_closest(Dht, Target, 3),
%% ok        = macula_dht:stop(Dht).
%% '''
%%
%% Multiple DHT instances can run in one BEAM VM — all API calls are
%% pid-scoped, no singleton state.
%%
%% Reference: plans/PLAN_PHASE_3_BREAKDOWN.md Session 3.3.
-module(macula_dht).

-export([
    start_link/1,
    stop/1,
    observe/2,
    touch/2,
    forget/2,
    self_id/1,
    find/2,
    contains/2,
    k_closest/3,
    siblings/1,
    sibling_ids/1,
    size/1,
    bucket_count/1,
    populated_buckets/1,
    stats/1,
    ping_peer/2, ping_peer/3,
    find_node/3, find_node/4,
    lookup_nodes/2, lookup_nodes/3,
    find_value/3, find_value/4,
    send_store/3, send_store/4,
    store/2, store/3,
    put_record/2,
    find_local_record/2,
    list_records/1,
    record_count/1,
    delete_record/2,
    handle_frame/3,
    set_on_record_stored/2,
    set_send_frame/2,
    set_identity/2,
    version/0
]).

-export_type([dht/0, opts/0, observe_result/0, stats/0,
              ping_result/0, find_node_result/0, find_value_result/0,
              send_store_result/0]).

-type dht()               :: pid().
-type opts()              :: macula_dht_server:opts().
-type observe_result()    :: macula_dht_server:observe_result().
-type stats()             :: macula_dht_server:stats().
-type ping_result()       :: macula_dht_server:ping_result().
-type find_node_result()  :: macula_dht_server:find_node_result().
-type find_value_result() :: macula_dht_server:find_value_result().
-type send_store_result() :: macula_dht_server:send_store_result().

%%=====================================================================
%% Lifecycle
%%=====================================================================

-spec start_link(opts()) -> {ok, dht()} | {error, term()}.
start_link(Opts) ->
    macula_dht_server:start_link(Opts).

-spec stop(dht()) -> ok.
stop(Dht) ->
    macula_dht_server:stop(Dht).

%%=====================================================================
%% Writes
%%=====================================================================

-spec observe(dht(), macula_dht_entry:spec()) -> observe_result().
observe(Dht, Spec) ->
    macula_dht_server:observe(Dht, Spec).

-spec touch(dht(), macula_dht_xor:id()) -> ok.
touch(Dht, Id) ->
    macula_dht_server:touch(Dht, Id).

-spec forget(dht(), macula_dht_xor:id()) -> ok.
forget(Dht, Id) ->
    macula_dht_server:forget(Dht, Id).

%%=====================================================================
%% Reads
%%=====================================================================

-spec self_id(dht()) -> macula_dht_xor:id().
self_id(Dht) ->
    macula_dht_server:self_id(Dht).

-spec find(dht(), macula_dht_xor:id()) ->
        {ok, macula_dht_entry:entry()} | error.
find(Dht, Id) ->
    macula_dht_server:find(Dht, Id).

-spec contains(dht(), macula_dht_xor:id()) -> boolean().
contains(Dht, Id) ->
    macula_dht_server:contains(Dht, Id).

-spec k_closest(dht(), macula_dht_xor:id(), non_neg_integer()) ->
          [macula_dht_entry:entry()].
k_closest(Dht, Target, K) ->
    macula_dht_server:k_closest(Dht, Target, K).

-spec siblings(dht()) -> [macula_dht_entry:entry()].
siblings(Dht) ->
    macula_dht_server:siblings(Dht).

-spec sibling_ids(dht()) -> [macula_dht_xor:id()].
sibling_ids(Dht) ->
    macula_dht_server:sibling_ids(Dht).

-spec size(dht()) -> non_neg_integer().
size(Dht) ->
    macula_dht_server:size(Dht).

-spec bucket_count(dht()) -> non_neg_integer().
bucket_count(Dht) ->
    macula_dht_server:bucket_count(Dht).

-spec populated_buckets(dht()) ->
        [{macula_dht_xor:bucket_ix(), macula_dht_bucket:bucket()}].
populated_buckets(Dht) ->
    macula_dht_server:populated_buckets(Dht).

-spec stats(dht()) -> stats().
stats(Dht) ->
    macula_dht_server:stats(Dht).

%%=====================================================================
%% Wire operations (Session 3.5)
%%=====================================================================

-spec ping_peer(dht(), macula_identity:pubkey()) -> ping_result().
ping_peer(Dht, TargetId) ->
    macula_dht_server:ping_peer(Dht, TargetId).

-spec ping_peer(dht(), macula_identity:pubkey(), pos_integer()) -> ping_result().
ping_peer(Dht, TargetId, Timeout) ->
    macula_dht_server:ping_peer(Dht, TargetId, Timeout).

-spec find_node(dht(), macula_dht_xor:id(), macula_identity:pubkey()) ->
        find_node_result().
find_node(Dht, Key, PeerId) ->
    macula_dht_server:find_node(Dht, Key, PeerId).

-spec find_node(dht(), macula_dht_xor:id(), macula_identity:pubkey(),
                pos_integer()) -> find_node_result().
find_node(Dht, Key, PeerId, Timeout) ->
    macula_dht_server:find_node(Dht, Key, PeerId, Timeout).

-spec lookup_nodes(dht(), macula_dht_xor:id()) -> macula_dht_lookup:result().
lookup_nodes(Dht, Key) ->
    macula_dht_lookup:lookup_nodes(Dht, Key).

-spec lookup_nodes(dht(), macula_dht_xor:id(), macula_dht_lookup:opts()) ->
        macula_dht_lookup:result().
lookup_nodes(Dht, Key, Opts) ->
    macula_dht_lookup:lookup_nodes(Dht, Key, Opts).

%%=====================================================================
%% Record storage + FIND_VALUE (Session 3.7)
%%=====================================================================

-spec put_record(dht(), macula_record:record()) -> ok.
put_record(Dht, Record) ->
    macula_dht_server:put_record(Dht, Record).

-spec find_local_record(dht(), macula_dht_xor:id()) ->
        [macula_record:record()].
find_local_record(Dht, Key) ->
    macula_dht_server:find_local_record(Dht, Key).

-spec list_records(dht()) -> [macula_record:record()].
list_records(Dht) ->
    macula_dht_server:list_records(Dht).

-spec record_count(dht()) -> non_neg_integer().
record_count(Dht) ->
    macula_dht_server:record_count(Dht).

-spec delete_record(dht(), macula_record:record()) -> ok.
delete_record(Dht, Record) ->
    macula_dht_server:delete_record(Dht, Record).

-spec find_value(dht(), macula_dht_xor:id(), macula_identity:pubkey()) ->
        find_value_result().
find_value(Dht, Key, PeerId) ->
    macula_dht_server:find_value(Dht, Key, PeerId).

-spec find_value(dht(), macula_dht_xor:id(), macula_identity:pubkey(),
                 pos_integer()) -> find_value_result().
find_value(Dht, Key, PeerId, Timeout) ->
    macula_dht_server:find_value(Dht, Key, PeerId, Timeout).

-spec send_store(dht(), macula_identity:pubkey(),
                 macula_record:record()) -> send_store_result().
send_store(Dht, PeerId, Record) ->
    macula_dht_server:send_store(Dht, PeerId, Record).

-spec send_store(dht(), macula_identity:pubkey(), macula_record:record(),
                 pos_integer()) -> send_store_result().
send_store(Dht, PeerId, Record, Timeout) ->
    macula_dht_server:send_store(Dht, PeerId, Record, Timeout).

-spec store(dht(), macula_record:record()) -> macula_dht_store:result().
store(Dht, Record) ->
    macula_dht_store:store(Dht, Record).

-spec store(dht(), macula_record:record(), macula_dht_store:opts()) ->
        macula_dht_store:result().
store(Dht, Record, Opts) ->
    macula_dht_store:store(Dht, Record, Opts).

-spec handle_frame(dht(), macula_identity:pubkey(), macula_frame:frame()) -> ok.
handle_frame(Dht, FromNodeId, Frame) ->
    macula_dht_server:handle_frame(Dht, FromNodeId, Frame).

-spec set_on_record_stored(dht(),
        fun((macula_record:record()) -> any()) | undefined) -> ok.
set_on_record_stored(Dht, Fun) ->
    macula_dht_server:set_on_record_stored(Dht, Fun).

-spec set_send_frame(dht(),
        macula_dht_server:send_fun() | undefined) -> ok.
set_send_frame(Dht, Fun) ->
    macula_dht_server:set_send_frame(Dht, Fun).

-spec set_identity(dht(),
        macula_identity:key_pair() | undefined) -> ok.
set_identity(Dht, Kp) ->
    macula_dht_server:set_identity(Dht, Kp).

%%=====================================================================
%% Version
%%=====================================================================

-spec version() -> binary().
version() ->
    <<"0.1.0-phase3">>.
