%% @doc Hecate DHT — public facade.
%%
%% Every operation is a thin delegation to `hecate_dht_server'. This
%% module is the only one external callers should use: the server,
%% its supervisor, and the pure primitives (`hecate_dht_xor',
%% `hecate_dht_entry', `hecate_dht_bucket', `hecate_dht_routing_table',
%% `hecate_dht_siblings') are implementation detail.
%%
%% == Usage ==
%%
%% ```
%% {ok, Dht} = hecate_dht:start_link(#{self_id => Self}),
%% admitted  = hecate_dht:observe(Dht, #{node_id => PeerId,
%%                                       asn => 64512,
%%                                       country => <<"BE">>,
%%                                       tier => t1}),
%% [Peer|_]  = hecate_dht:k_closest(Dht, Target, 3),
%% ok        = hecate_dht:stop(Dht).
%% '''
%%
%% Multiple DHT instances can run in one BEAM VM — all API calls are
%% pid-scoped, no singleton state.
%%
%% Reference: plans/PLAN_PHASE_3_BREAKDOWN.md Session 3.3.
-module(hecate_dht).

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
    stats/1,
    ping_peer/2, ping_peer/3,
    find_node/3, find_node/4,
    lookup_nodes/2, lookup_nodes/3,
    find_value/3, find_value/4,
    put_record/2,
    find_local_record/2,
    record_count/1,
    handle_frame/3,
    version/0
]).

-export_type([dht/0, opts/0, observe_result/0, stats/0,
              ping_result/0, find_node_result/0, find_value_result/0]).

-type dht()               :: pid().
-type opts()              :: hecate_dht_server:opts().
-type observe_result()    :: hecate_dht_server:observe_result().
-type stats()             :: hecate_dht_server:stats().
-type ping_result()       :: hecate_dht_server:ping_result().
-type find_node_result()  :: hecate_dht_server:find_node_result().
-type find_value_result() :: hecate_dht_server:find_value_result().

%%=====================================================================
%% Lifecycle
%%=====================================================================

-spec start_link(opts()) -> {ok, dht()} | {error, term()}.
start_link(Opts) ->
    hecate_dht_server:start_link(Opts).

-spec stop(dht()) -> ok.
stop(Dht) ->
    hecate_dht_server:stop(Dht).

%%=====================================================================
%% Writes
%%=====================================================================

-spec observe(dht(), hecate_dht_entry:spec()) -> observe_result().
observe(Dht, Spec) ->
    hecate_dht_server:observe(Dht, Spec).

-spec touch(dht(), hecate_dht_xor:id()) -> ok.
touch(Dht, Id) ->
    hecate_dht_server:touch(Dht, Id).

-spec forget(dht(), hecate_dht_xor:id()) -> ok.
forget(Dht, Id) ->
    hecate_dht_server:forget(Dht, Id).

%%=====================================================================
%% Reads
%%=====================================================================

-spec self_id(dht()) -> hecate_dht_xor:id().
self_id(Dht) ->
    hecate_dht_server:self_id(Dht).

-spec find(dht(), hecate_dht_xor:id()) ->
        {ok, hecate_dht_entry:entry()} | error.
find(Dht, Id) ->
    hecate_dht_server:find(Dht, Id).

-spec contains(dht(), hecate_dht_xor:id()) -> boolean().
contains(Dht, Id) ->
    hecate_dht_server:contains(Dht, Id).

-spec k_closest(dht(), hecate_dht_xor:id(), non_neg_integer()) ->
          [hecate_dht_entry:entry()].
k_closest(Dht, Target, K) ->
    hecate_dht_server:k_closest(Dht, Target, K).

-spec siblings(dht()) -> [hecate_dht_entry:entry()].
siblings(Dht) ->
    hecate_dht_server:siblings(Dht).

-spec sibling_ids(dht()) -> [hecate_dht_xor:id()].
sibling_ids(Dht) ->
    hecate_dht_server:sibling_ids(Dht).

-spec size(dht()) -> non_neg_integer().
size(Dht) ->
    hecate_dht_server:size(Dht).

-spec bucket_count(dht()) -> non_neg_integer().
bucket_count(Dht) ->
    hecate_dht_server:bucket_count(Dht).

-spec stats(dht()) -> stats().
stats(Dht) ->
    hecate_dht_server:stats(Dht).

%%=====================================================================
%% Wire operations (Session 3.5)
%%=====================================================================

-spec ping_peer(dht(), macula_identity:pubkey()) -> ping_result().
ping_peer(Dht, TargetId) ->
    hecate_dht_server:ping_peer(Dht, TargetId).

-spec ping_peer(dht(), macula_identity:pubkey(), pos_integer()) -> ping_result().
ping_peer(Dht, TargetId, Timeout) ->
    hecate_dht_server:ping_peer(Dht, TargetId, Timeout).

-spec find_node(dht(), hecate_dht_xor:id(), macula_identity:pubkey()) ->
        find_node_result().
find_node(Dht, Key, PeerId) ->
    hecate_dht_server:find_node(Dht, Key, PeerId).

-spec find_node(dht(), hecate_dht_xor:id(), macula_identity:pubkey(),
                pos_integer()) -> find_node_result().
find_node(Dht, Key, PeerId, Timeout) ->
    hecate_dht_server:find_node(Dht, Key, PeerId, Timeout).

-spec lookup_nodes(dht(), hecate_dht_xor:id()) -> hecate_dht_lookup:result().
lookup_nodes(Dht, Key) ->
    hecate_dht_lookup:lookup_nodes(Dht, Key).

-spec lookup_nodes(dht(), hecate_dht_xor:id(), hecate_dht_lookup:opts()) ->
        hecate_dht_lookup:result().
lookup_nodes(Dht, Key, Opts) ->
    hecate_dht_lookup:lookup_nodes(Dht, Key, Opts).

%%=====================================================================
%% Record storage + FIND_VALUE (Session 3.7)
%%=====================================================================

-spec put_record(dht(), macula_record:record()) -> ok.
put_record(Dht, Record) ->
    hecate_dht_server:put_record(Dht, Record).

-spec find_local_record(dht(), hecate_dht_xor:id()) ->
        [macula_record:record()].
find_local_record(Dht, Key) ->
    hecate_dht_server:find_local_record(Dht, Key).

-spec record_count(dht()) -> non_neg_integer().
record_count(Dht) ->
    hecate_dht_server:record_count(Dht).

-spec find_value(dht(), hecate_dht_xor:id(), macula_identity:pubkey()) ->
        find_value_result().
find_value(Dht, Key, PeerId) ->
    hecate_dht_server:find_value(Dht, Key, PeerId).

-spec find_value(dht(), hecate_dht_xor:id(), macula_identity:pubkey(),
                 pos_integer()) -> find_value_result().
find_value(Dht, Key, PeerId, Timeout) ->
    hecate_dht_server:find_value(Dht, Key, PeerId, Timeout).

-spec handle_frame(dht(), macula_identity:pubkey(), macula_frame:frame()) -> ok.
handle_frame(Dht, FromNodeId, Frame) ->
    hecate_dht_server:handle_frame(Dht, FromNodeId, Frame).

%%=====================================================================
%% Version
%%=====================================================================

-spec version() -> binary().
version() ->
    <<"0.1.0-phase3">>.
