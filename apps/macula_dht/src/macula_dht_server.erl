%% @doc gen_server owning a Kademlia routing table and sibling list
%% for a single station.
%%
%% State is built from `macula_dht_routing_table' and
%% `macula_dht_siblings' (Session 3.3) plus pending-request tables
%% for wire-level PING/PONG and FIND_NODE/NODES correlation
%% (Session 3.5). STORE / FIND_VALUE / REPLICATE land in later
%% sessions (3.7+).
%%
%% == Transport ==
%%
%% The server is transport-agnostic. Callers supply an opaque
%% `send_frame/2' callback in `opts()' that the server invokes to
%% push a signed frame at a target NodeId. Incoming frames enter via
%% `handle_frame/3' (cast). Production wires `send_frame' to
%% `macula_peering'; the Session 3.12 CT harness wires it to direct
%% in-VM `macula_dht_server' pid dispatch.
%%
%% == Observation semantics ==
%%
%% Every observed peer is offered to both the routing table and the
%% sibling list:
%%
%% <ul>
%%   <li>Routing-table admission is the *scored* algorithm
%%       (`macula_dht_bucket'): uptime, novelty, incumbency, latency.</li>
%%   <li>Sibling admission is *pure XOR distance to self* — the 16
%%       closest peers, regardless of scoring.</li>
%% </ul>
%%
%% The two containers are independent. A peer can be in siblings but
%% not in the routing table (bucket was full of stronger peers), or
%% vice versa (peer admitted to bucket but too far for the sibling
%% list). The server reports the routing-table outcome to the caller;
%% sibling state is retrievable via its own accessors.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §4.
-module(macula_dht_server).
-behaviour(gen_server).

-export([
    start_link/1,
    stop/1,
    observe/2,
    observe_async/2,
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
    find_value/3, find_value/4,
    send_store/3, send_store/4,
    put_record/2,
    find_local_record/2,
    list_records/1,
    record_count/1,
    delete_record/2,
    handle_frame/3,
    set_on_record_stored/2,
    set_send_frame/2,
    set_identity/2
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, observe_result/0, stats/0, send_fun/0,
              ping_result/0, find_node_result/0, find_value_result/0,
              send_store_result/0]).

-define(DEFAULT_K, 20).
-define(DEFAULT_S, 16).
-define(DEFAULT_PING_TIMEOUT_MS,      2_000).
-define(DEFAULT_FIND_NODE_TIMEOUT_MS, 5_000).
-define(DEFAULT_STORE_TIMEOUT_MS,     5_000).

-type send_fun() :: fun((macula_identity:pubkey(), macula_frame:frame()) ->
                              ok | {error, term()}).

%% Optional hook fired after every successful `store_put' — both
%% own `put_record/2' calls and incoming custodian STORE frames
%% land here. Receivers (e.g. `publish_record_facts') translate
%% the record into a typed business event on the peering pubsub
%% channel.
-type on_record_stored_fun() :: fun((macula_record:record()) -> any()).

-type opts() :: #{
    self_id               := macula_dht_xor:id(),
    k                     => pos_integer(),
    s                     => pos_integer(),
    identity              => macula_identity:key_pair(),
    send_frame            => send_fun(),
    ping_timeout_ms       => pos_integer(),
    find_node_timeout_ms  => pos_integer(),
    on_record_stored      => on_record_stored_fun()
}.

-type ping_result() :: {ok, #{rtt_ms := non_neg_integer()}}
                     | {error, timeout | no_transport | term()}.

-type find_node_result() :: {ok, [macula_frame:station_ref()]}
                          | {error, timeout | no_transport | term()}.

-type find_value_result() :: {value, [macula_record:record()]}
                           | {nodes, [macula_frame:station_ref()]}
                           | {error, timeout | no_transport | term()}.

-type send_store_result() :: {ok, #{stored := boolean(),
                                    reason := atom() | undefined}}
                           | {error, timeout | no_transport | term()}.

-type observe_result() ::
      admitted
    | touched
    | {replaced, Evicted :: macula_dht_entry:entry()}
    | rejected.

-type stats() :: #{
    self_id       := macula_dht_xor:id(),
    size          := non_neg_integer(),
    bucket_count  := non_neg_integer(),
    sibling_count := non_neg_integer(),
    k             := pos_integer(),
    s             := pos_integer()
}.

-record(state, {
    self_id              :: macula_dht_xor:id(),
    rt                   :: macula_dht_routing_table:table(),
    sibs                 :: macula_dht_siblings:siblings(),
    k                    :: pos_integer(),
    s                    :: pos_integer(),
    identity             :: macula_identity:key_pair() | undefined,
    send_frame           :: send_fun() | undefined,
    ping_timeout_ms      :: pos_integer(),
    find_node_timeout_ms :: pos_integer(),
    %% {Nonce => {TargetNodeId, From, TimerRef, StartedMono}}
    pending_pings = #{}  :: #{macula_dht_protocol:nonce() =>
                              {macula_identity:pubkey(),
                               gen_server:from(),
                               reference(),
                               integer()}},
    %% {{PeerNodeId, Key} => {From, TimerRef}}
    pending_find_nodes = #{} :: #{{macula_identity:pubkey(),
                                   macula_dht_xor:id()} =>
                                  {gen_server:from(), reference()}},
    pending_find_values = #{} :: #{{macula_identity:pubkey(),
                                    macula_dht_xor:id()} =>
                                   {gen_server:from(), reference()}},
    pending_stores = #{}       :: #{{macula_identity:pubkey(),
                                     macula_dht_xor:id()} =>
                                    {gen_server:from(), reference()}},
    record_store                :: ets:tid(),
    on_record_stored            :: on_record_stored_fun() | undefined
}).

%%=====================================================================
%% Public API
%%=====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{self_id := <<_:256>>} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

-spec observe(pid(), macula_dht_entry:spec()) -> observe_result().
observe(Pid, Spec) when is_map(Spec) ->
    gen_server:call(Pid, {observe, Spec}).

%% Fire-and-forget admit; same state mutation as `observe/2' but no
%% admit-result reply. See `macula_dht:observe_async/2' for the
%% why — short version: hot-path callers (peer_observer) shouldn't
%% block on DHT-server load and shouldn't crash if it's wedged.
-spec observe_async(pid(), macula_dht_entry:spec()) -> ok.
observe_async(Pid, Spec) when is_map(Spec) ->
    gen_server:cast(Pid, {observe_async, Spec}).

-spec touch(pid(), macula_dht_xor:id()) -> ok.
touch(Pid, <<_:256>> = Id) ->
    gen_server:cast(Pid, {touch, Id}).

-spec forget(pid(), macula_dht_xor:id()) -> ok.
forget(Pid, <<_:256>> = Id) ->
    gen_server:cast(Pid, {forget, Id}).

-spec self_id(pid()) -> macula_dht_xor:id().
self_id(Pid) ->
    gen_server:call(Pid, self_id).

-spec find(pid(), macula_dht_xor:id()) ->
        {ok, macula_dht_entry:entry()} | error.
find(Pid, <<_:256>> = Id) ->
    gen_server:call(Pid, {find, Id}).

-spec contains(pid(), macula_dht_xor:id()) -> boolean().
contains(Pid, <<_:256>> = Id) ->
    gen_server:call(Pid, {contains, Id}).

-spec k_closest(pid(), macula_dht_xor:id(), non_neg_integer()) ->
          [macula_dht_entry:entry()].
k_closest(Pid, <<_:256>> = Target, K) when is_integer(K), K >= 0 ->
    gen_server:call(Pid, {k_closest, Target, K}).

-spec siblings(pid()) -> [macula_dht_entry:entry()].
siblings(Pid) ->
    gen_server:call(Pid, siblings).

-spec sibling_ids(pid()) -> [macula_dht_xor:id()].
sibling_ids(Pid) ->
    gen_server:call(Pid, sibling_ids).

-spec size(pid()) -> non_neg_integer().
size(Pid) ->
    gen_server:call(Pid, size).

-spec bucket_count(pid()) -> non_neg_integer().
bucket_count(Pid) ->
    gen_server:call(Pid, bucket_count).

-spec populated_buckets(pid()) ->
        [{macula_dht_xor:bucket_ix(), macula_dht_bucket:bucket()}].
populated_buckets(Pid) ->
    gen_server:call(Pid, populated_buckets).

-spec stats(pid()) -> stats().
stats(Pid) ->
    gen_server:call(Pid, stats).

-spec ping_peer(pid(), macula_identity:pubkey()) -> ping_result().
ping_peer(Pid, TargetId) ->
    ping_peer(Pid, TargetId, ?DEFAULT_PING_TIMEOUT_MS).

-spec ping_peer(pid(), macula_identity:pubkey(), pos_integer()) -> ping_result().
ping_peer(Pid, <<_:256>> = TargetId, Timeout)
  when is_integer(Timeout), Timeout > 0 ->
    gen_server:call(Pid, {ping_peer, TargetId, Timeout}, Timeout + 1_000).

-spec find_node(pid(), macula_dht_xor:id(), macula_identity:pubkey()) ->
        find_node_result().
find_node(Pid, Key, PeerId) ->
    find_node(Pid, Key, PeerId, ?DEFAULT_FIND_NODE_TIMEOUT_MS).

-spec find_node(pid(), macula_dht_xor:id(), macula_identity:pubkey(),
                pos_integer()) -> find_node_result().
find_node(Pid, <<_:256>> = Key, <<_:256>> = PeerId, Timeout)
  when is_integer(Timeout), Timeout > 0 ->
    gen_server:call(Pid, {find_node, Key, PeerId, Timeout}, Timeout + 1_000).

-spec handle_frame(pid(), macula_identity:pubkey(), macula_frame:frame()) -> ok.
handle_frame(Pid, <<_:256>> = FromNodeId, Frame) when is_map(Frame) ->
    gen_server:cast(Pid, {frame, FromNodeId, Frame}).

-spec find_value(pid(), macula_dht_xor:id(), macula_identity:pubkey()) ->
        find_value_result().
find_value(Pid, Key, PeerId) ->
    find_value(Pid, Key, PeerId, ?DEFAULT_FIND_NODE_TIMEOUT_MS).

-spec find_value(pid(), macula_dht_xor:id(), macula_identity:pubkey(),
                 pos_integer()) -> find_value_result().
find_value(Pid, <<_:256>> = Key, <<_:256>> = PeerId, Timeout)
  when is_integer(Timeout), Timeout > 0 ->
    gen_server:call(Pid, {find_value, Key, PeerId, Timeout}, Timeout + 1_000).

-spec send_store(pid(), macula_identity:pubkey(),
                 macula_record:record()) -> send_store_result().
send_store(Pid, PeerId, Record) ->
    send_store(Pid, PeerId, Record, ?DEFAULT_STORE_TIMEOUT_MS).

-spec send_store(pid(), macula_identity:pubkey(), macula_record:record(),
                 pos_integer()) -> send_store_result().
send_store(Pid, <<_:256>> = PeerId, Record, Timeout)
  when is_map(Record), is_integer(Timeout), Timeout > 0 ->
    gen_server:call(Pid, {send_store, PeerId, Record, Timeout},
                    Timeout + 1_000).

-spec put_record(pid(), macula_record:record()) -> ok.
put_record(Pid, Record) when is_map(Record) ->
    gen_server:call(Pid, {put_record, Record}).

-spec find_local_record(pid(), macula_dht_xor:id()) ->
        [macula_record:record()].
find_local_record(Pid, <<_:256>> = Key) ->
    gen_server:call(Pid, {find_local_record, Key}).

-spec list_records(pid()) -> [macula_record:record()].
list_records(Pid) ->
    gen_server:call(Pid, list_records).

-spec record_count(pid()) -> non_neg_integer().
record_count(Pid) ->
    gen_server:call(Pid, record_count).

-spec delete_record(pid(), macula_record:record()) -> ok.
delete_record(Pid, Record) when is_map(Record) ->
    gen_server:call(Pid, {delete_record, Record}).

%% @doc Install (or replace) the `on_record_stored' callback at
%% runtime. Used by the identity supervisor to wire the DHT to the
%% node-singleton `macula_station_record_fanout' (the fan-out
%% depends on observer + pubsub_registry, which in turn depend on
%% DHT — late wiring breaks the cycle).
-spec set_on_record_stored(pid(), on_record_stored_fun() | undefined) -> ok.
set_on_record_stored(Pid, Fun)
  when is_pid(Pid), (is_function(Fun, 1) orelse Fun =:= undefined) ->
    gen_server:call(Pid, {set_on_record_stored, Fun}).

%% @doc Install (or replace) the outbound `send_frame' callback at
%% runtime. Production wires this from `macula_station_app' after the
%% peer observer is up — DHT depends on observer for NodeId → ConnPid
%% lookup, so the callback can only be installed late.
-spec set_send_frame(pid(), send_fun() | undefined) -> ok.
set_send_frame(Pid, Fun)
  when is_pid(Pid), (is_function(Fun, 2) orelse Fun =:= undefined) ->
    gen_server:call(Pid, {set_send_frame, Fun}).

%% @doc Install (or replace) the station identity (key pair) used to
%% sign outbound DHT frames. Wired alongside `set_send_frame/2' at
%% station boot.
-spec set_identity(pid(), macula_identity:key_pair() | undefined) -> ok.
set_identity(Pid, Kp)
  when is_pid(Pid) ->
    gen_server:call(Pid, {set_identity, Kp}).

%%=====================================================================
%% gen_server callbacks
%%=====================================================================

init(#{self_id := Self} = Opts) ->
    K = maps:get(k, Opts, ?DEFAULT_K),
    S = maps:get(s, Opts, ?DEFAULT_S),
    Ets = ets:new(macula_dht_record_store, [bag, private]),
    {ok, #state{
        self_id              = Self,
        rt                   = macula_dht_routing_table:new(Self, K),
        sibs                 = macula_dht_siblings:new(Self, S),
        k                    = K,
        s                    = S,
        identity             = maps:get(identity, Opts, undefined),
        send_frame           = maps:get(send_frame, Opts, undefined),
        ping_timeout_ms      = maps:get(ping_timeout_ms, Opts,
                                        ?DEFAULT_PING_TIMEOUT_MS),
        find_node_timeout_ms = maps:get(find_node_timeout_ms, Opts,
                                        ?DEFAULT_FIND_NODE_TIMEOUT_MS),
        record_store         = Ets,
        on_record_stored     = maps:get(on_record_stored, Opts, undefined)
    }}.

handle_call(self_id, _From, #state{self_id = Self} = S) ->
    {reply, Self, S};

handle_call({observe, Spec}, _From, S) ->
    handle_observe(Spec, S);

handle_call({find, Id}, _From, #state{rt = Rt} = S) ->
    {reply, macula_dht_routing_table:find(Id, Rt), S};

handle_call({contains, Id}, _From, #state{rt = Rt} = S) ->
    {reply, macula_dht_routing_table:contains(Id, Rt), S};

handle_call({k_closest, Target, K}, _From, #state{rt = Rt} = S) ->
    {reply, macula_dht_routing_table:k_closest(Target, K, Rt), S};

handle_call(siblings, _From, #state{sibs = Sibs} = S) ->
    {reply, macula_dht_siblings:members(Sibs), S};

handle_call(sibling_ids, _From, #state{sibs = Sibs} = S) ->
    {reply, macula_dht_siblings:node_ids(Sibs), S};

handle_call(size, _From, #state{rt = Rt} = S) ->
    {reply, macula_dht_routing_table:size(Rt), S};

handle_call(bucket_count, _From, #state{rt = Rt} = S) ->
    {reply, macula_dht_routing_table:bucket_count(Rt), S};

handle_call(populated_buckets, _From, #state{rt = Rt} = S) ->
    {reply, macula_dht_routing_table:populated_buckets(Rt), S};

handle_call(stats, _From, S) ->
    {reply, build_stats(S), S};

handle_call({ping_peer, TargetId, Timeout}, From, S) ->
    dispatch_ping(TargetId, Timeout, From, S);

handle_call({find_node, Key, PeerId, Timeout}, From, S) ->
    dispatch_find_node(Key, PeerId, Timeout, From, S);

handle_call({find_value, Key, PeerId, Timeout}, From, S) ->
    dispatch_find_value(Key, PeerId, Timeout, From, S);

handle_call({send_store, PeerId, Record, Timeout}, From, S) ->
    dispatch_send_store(PeerId, Record, Timeout, From, S);

handle_call({put_record, Record}, _From, #state{record_store = Ets} = S) ->
    store_put(Ets, Record),
    notify_record_stored(Record, S),
    {reply, ok, S};

handle_call({find_local_record, Key}, _From, #state{record_store = Ets} = S) ->
    {reply, store_lookup(Ets, Key), S};

handle_call(list_records, _From, #state{record_store = Ets} = S) ->
    {reply, [R || {_, R} <- ets:tab2list(Ets)], S};

handle_call(record_count, _From, #state{record_store = Ets} = S) ->
    {reply, ets:info(Ets, size), S};

handle_call({set_on_record_stored, Fun}, _From, S) ->
    {reply, ok, S#state{on_record_stored = Fun}};

handle_call({set_send_frame, Fun}, _From, S) ->
    {reply, ok, S#state{send_frame = Fun}};

handle_call({set_identity, Kp}, _From, S) ->
    {reply, ok, S#state{identity = Kp}};

handle_call({delete_record, Record}, _From, #state{record_store = Ets} = S) ->
    store_delete(Ets, Record),
    {reply, ok, S};

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast({observe_async, Spec}, S) ->
    {reply, _, NewS} = handle_observe(Spec, S),
    {noreply, NewS};

handle_cast({touch, Id}, #state{rt = Rt, sibs = Sibs} = S) ->
    Now = erlang:monotonic_time(millisecond),
    {noreply, S#state{
        rt   = macula_dht_routing_table:touch(Id, Rt, Now),
        sibs = macula_dht_siblings:touch(Id, Sibs, Now)
    }};

handle_cast({forget, Id}, #state{rt = Rt, sibs = Sibs} = S) ->
    {noreply, S#state{
        rt   = macula_dht_routing_table:remove(Id, Rt),
        sibs = macula_dht_siblings:remove(Id, Sibs)
    }};

handle_cast({frame, FromNodeId, Frame}, S) ->
    {noreply, dispatch_frame(FromNodeId, Frame, S)};

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({ping_timeout, Nonce}, S) ->
    {noreply, on_ping_timeout(Nonce, S)};

handle_info({find_node_timeout, Key, PeerId}, S) ->
    {noreply, on_find_node_timeout(Key, PeerId, S)};

handle_info({find_value_timeout, Key, PeerId}, S) ->
    {noreply, on_find_value_timeout(Key, PeerId, S)};

handle_info({send_store_timeout, Key, PeerId}, S) ->
    {noreply, on_send_store_timeout(Key, PeerId, S)};

%% Direct DHT-frame ingress from `macula_peering_conn' (when the SDK's
%% `dht_recipient' opt is set to this server's pid). Bypasses the
%% `macula_station_peer_observer' mailbox so DHT chatter — typically
%% 85%+ of inbound frames at steady state — does not queue behind
%% handler-dispatch and pubsub work in the observer. Same downstream
%% as `handle_cast({frame, _, _})'.
handle_info({macula_peering, dht_frame, _ConnPid, FromNodeId, Frame}, S)
        when is_binary(FromNodeId), byte_size(FromNodeId) =:= 32,
             is_map(Frame) ->
    {noreply, dispatch_frame(FromNodeId, Frame, S)};

handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%=====================================================================
%% Observation
%%=====================================================================

-spec handle_observe(macula_dht_entry:spec(), #state{}) ->
          {reply, observe_result(), #state{}}.
handle_observe(Spec, #state{rt = Rt, sibs = Sibs} = S) ->
    Now = erlang:monotonic_time(millisecond),
    Entry = macula_dht_entry:new(Spec, Now),
    {RtResult, Rt1} = apply_rt(Entry, Rt, Now),
    Sibs1 = apply_siblings(Entry, Sibs, Now),
    {reply, classify(RtResult), S#state{rt = Rt1, sibs = Sibs1}}.

-spec apply_rt(macula_dht_entry:entry(), macula_dht_routing_table:table(),
               integer()) ->
          {macula_dht_routing_table:insert_result(),
           macula_dht_routing_table:table()}.
apply_rt(Entry, Rt, Now) ->
    Result = macula_dht_routing_table:insert(Entry, Rt, Now),
    {Result, extract_table(Result)}.

-spec apply_siblings(macula_dht_entry:entry(), macula_dht_siblings:siblings(),
                     integer()) -> macula_dht_siblings:siblings().
apply_siblings(Entry, Sibs, Now) ->
    extract_siblings(macula_dht_siblings:insert(Entry, Sibs, Now)).

-spec extract_table(macula_dht_routing_table:insert_result()) ->
          macula_dht_routing_table:table().
extract_table({admitted, T})     -> T;
extract_table({touched,  T})     -> T;
extract_table({replaced, _E, T}) -> T;
extract_table({rejected, T})     -> T.

-spec extract_siblings(macula_dht_siblings:insert_result()) ->
          macula_dht_siblings:siblings().
extract_siblings({admitted, S})     -> S;
extract_siblings({touched,  S})     -> S;
extract_siblings({replaced, _E, S}) -> S;
extract_siblings({rejected, S})     -> S.

-spec classify(macula_dht_routing_table:insert_result()) -> observe_result().
classify({admitted, _T})      -> admitted;
classify({touched,  _T})      -> touched;
classify({replaced, E, _T})   -> {replaced, E};
classify({rejected, _T})      -> rejected.

%%=====================================================================
%% Stats
%%=====================================================================

-spec build_stats(#state{}) -> stats().
build_stats(#state{self_id = Self, rt = Rt, sibs = Sibs, k = K, s = S}) ->
    #{
        self_id       => Self,
        size          => macula_dht_routing_table:size(Rt),
        bucket_count  => macula_dht_routing_table:bucket_count(Rt),
        sibling_count => macula_dht_siblings:size(Sibs),
        k             => K,
        s             => S
    }.

%%=====================================================================
%% Wire op: PING outgoing
%%=====================================================================

-spec dispatch_ping(macula_identity:pubkey(), pos_integer(),
                    gen_server:from(), #state{}) ->
          {reply, ping_result(), #state{}} | {noreply, #state{}}.
dispatch_ping(_TargetId, _Timeout, _From,
              #state{send_frame = undefined} = S) ->
    {reply, {error, no_transport}, S};
dispatch_ping(_TargetId, _Timeout, _From,
              #state{identity = undefined} = S) ->
    {reply, {error, no_identity}, S};
dispatch_ping(TargetId, Timeout, From,
              #state{identity = Id, send_frame = Send,
                     pending_pings = P} = S) ->
    {Frame, Nonce} = macula_dht_protocol:build_ping(Id),
    Send(TargetId, Frame),
    TimerRef = erlang:send_after(Timeout, self(), {ping_timeout, Nonce}),
    StartedMono = erlang:monotonic_time(millisecond),
    {noreply, S#state{pending_pings =
        P#{Nonce => {TargetId, From, TimerRef, StartedMono}}}}.

-spec on_ping_timeout(macula_dht_protocol:nonce(), #state{}) -> #state{}.
on_ping_timeout(Nonce, #state{pending_pings = P} = S) ->
    dispatch_ping_timeout(maps:take(Nonce, P), S).

-spec dispatch_ping_timeout(error | {tuple(), map()}, #state{}) -> #state{}.
dispatch_ping_timeout(error, S) ->
    S;
dispatch_ping_timeout({{_Target, From, _Timer, _Started}, NewP}, S) ->
    gen_server:reply(From, {error, timeout}),
    S#state{pending_pings = NewP}.

%%=====================================================================
%% Wire op: FIND_NODE outgoing
%%=====================================================================

-spec dispatch_find_node(macula_dht_xor:id(), macula_identity:pubkey(),
                         pos_integer(), gen_server:from(), #state{}) ->
          {reply, find_node_result(), #state{}} | {noreply, #state{}}.
dispatch_find_node(_Key, _PeerId, _Timeout, _From,
                   #state{send_frame = undefined} = S) ->
    {reply, {error, no_transport}, S};
dispatch_find_node(_Key, _PeerId, _Timeout, _From,
                   #state{identity = undefined} = S) ->
    {reply, {error, no_identity}, S};
dispatch_find_node(Key, PeerId, Timeout, From,
                   #state{self_id = Self, identity = Id, send_frame = Send,
                          pending_find_nodes = P} = S) ->
    Frame = macula_dht_protocol:build_find_node(Key, Self, 0, Id),
    Send(PeerId, Frame),
    TimerRef = erlang:send_after(Timeout, self(),
                                 {find_node_timeout, Key, PeerId}),
    {noreply, S#state{pending_find_nodes =
        P#{{PeerId, Key} => {From, TimerRef}}}}.

-spec on_find_node_timeout(macula_dht_xor:id(), macula_identity:pubkey(),
                           #state{}) -> #state{}.
on_find_node_timeout(Key, PeerId, #state{pending_find_nodes = P} = S) ->
    dispatch_find_node_timeout(maps:take({PeerId, Key}, P), S).

-spec dispatch_find_node_timeout(error | {tuple(), map()}, #state{}) ->
          #state{}.
dispatch_find_node_timeout(error, S) ->
    S;
dispatch_find_node_timeout({{From, _Timer}, NewP}, S) ->
    gen_server:reply(From, {error, timeout}),
    S#state{pending_find_nodes = NewP}.

%%=====================================================================
%% Wire op: FIND_VALUE outgoing
%%=====================================================================

-spec dispatch_find_value(macula_dht_xor:id(), macula_identity:pubkey(),
                          pos_integer(), gen_server:from(), #state{}) ->
          {reply, find_value_result(), #state{}} | {noreply, #state{}}.
dispatch_find_value(_Key, _PeerId, _Timeout, _From,
                    #state{send_frame = undefined} = S) ->
    {reply, {error, no_transport}, S};
dispatch_find_value(_Key, _PeerId, _Timeout, _From,
                    #state{identity = undefined} = S) ->
    {reply, {error, no_identity}, S};
dispatch_find_value(Key, PeerId, Timeout, From,
                    #state{self_id = Self, identity = Id, send_frame = Send,
                           pending_find_values = P} = S) ->
    Frame = macula_dht_protocol:build_find_value(Key, Self, Id),
    Send(PeerId, Frame),
    TimerRef = erlang:send_after(Timeout, self(),
                                 {find_value_timeout, Key, PeerId}),
    {noreply, S#state{pending_find_values =
        P#{{PeerId, Key} => {From, TimerRef}}}}.

-spec on_find_value_timeout(macula_dht_xor:id(), macula_identity:pubkey(),
                            #state{}) -> #state{}.
on_find_value_timeout(Key, PeerId, #state{pending_find_values = P} = S) ->
    dispatch_find_value_timeout(maps:take({PeerId, Key}, P), S).

-spec dispatch_find_value_timeout(error | {tuple(), map()}, #state{}) ->
          #state{}.
dispatch_find_value_timeout(error, S) ->
    S;
dispatch_find_value_timeout({{From, _Timer}, NewP}, S) ->
    gen_server:reply(From, {error, timeout}),
    S#state{pending_find_values = NewP}.

%%=====================================================================
%% Record store (ETS bag keyed by storage key; value = record()).
%% `put' replaces any prior record from the same envelope owner so
%% versions don't accumulate, and preserves records from other
%% owners that share the same storage key (e.g. multiple advertisers
%% of the same procedure_uri).
%%=====================================================================

-spec store_put(ets:tid(), macula_record:record()) -> ok.
store_put(Ets, Record) ->
    StorageKey  = macula_record:storage_key(Record),
    EnvelopeKey = macula_record:key(Record),
    Keep = [Tup || {_, R} = Tup <- ets:lookup(Ets, StorageKey),
                   macula_record:key(R) =/= EnvelopeKey],
    ets:delete(Ets, StorageKey),
    ets:insert(Ets, [{StorageKey, Record} | Keep]),
    ok.

%% Fire the optional `on_record_stored' callback. Wrapped so a
%% misbehaving callback never destabilises the DHT server.
-spec notify_record_stored(macula_record:record(), #state{}) -> ok.
notify_record_stored(_Record, #state{on_record_stored = undefined}) ->
    ok;
notify_record_stored(Record, #state{on_record_stored = Fun}) ->
    catch Fun(Record),
    ok.

-spec store_lookup(ets:tid(), macula_dht_xor:id()) ->
          [macula_record:record()].
store_lookup(Ets, StorageKey) ->
    [R || {_, R} <- ets:lookup(Ets, StorageKey)].

%% @doc Remove a single record (identified by storage key + envelope
%% owner) while preserving other records from other owners that
%% share the same storage key.
-spec store_delete(ets:tid(), macula_record:record()) -> ok.
store_delete(Ets, Record) ->
    StorageKey  = macula_record:storage_key(Record),
    EnvelopeKey = macula_record:key(Record),
    Keep = [Tup || {_, R} = Tup <- ets:lookup(Ets, StorageKey),
                   macula_record:key(R) =/= EnvelopeKey],
    ets:delete(Ets, StorageKey),
    ets:insert(Ets, Keep),
    ok.

%%=====================================================================
%% Wire op: STORE outgoing + STORE_ACK correlation
%%=====================================================================

-spec dispatch_send_store(macula_identity:pubkey(), macula_record:record(),
                          pos_integer(), gen_server:from(), #state{}) ->
          {reply, send_store_result(), #state{}} | {noreply, #state{}}.
dispatch_send_store(_PeerId, _Record, _Timeout, _From,
                    #state{send_frame = undefined} = S) ->
    {reply, {error, no_transport}, S};
dispatch_send_store(_PeerId, _Record, _Timeout, _From,
                    #state{identity = undefined} = S) ->
    {reply, {error, no_identity}, S};
dispatch_send_store(PeerId, Record, Timeout, From,
                    #state{identity = Id, send_frame = Send,
                           pending_stores = P} = S) ->
    Frame = macula_dht_protocol:build_store(Record, Id),
    Send(PeerId, Frame),
    Key = macula_record:storage_key(Record),
    TimerRef = erlang:send_after(Timeout, self(),
                                 {send_store_timeout, Key, PeerId}),
    {noreply, S#state{pending_stores =
        P#{{PeerId, Key} => {From, TimerRef}}}}.

-spec on_send_store_timeout(macula_dht_xor:id(), macula_identity:pubkey(),
                            #state{}) -> #state{}.
on_send_store_timeout(Key, PeerId, #state{pending_stores = P} = S) ->
    dispatch_store_timeout(maps:take({PeerId, Key}, P), S).

-spec dispatch_store_timeout(error | {tuple(), map()}, #state{}) ->
          #state{}.
dispatch_store_timeout(error, S) ->
    S;
dispatch_store_timeout({{From, _Timer}, NewP}, S) ->
    gen_server:reply(From, {error, timeout}),
    S#state{pending_stores = NewP}.

%%---------------------------------------------------------------------
%% Incoming STORE — verify record, persist on success, always reply
%% STORE_ACK so the requester learns the outcome.
%%---------------------------------------------------------------------

-spec on_store(macula_frame:frame(), macula_identity:pubkey(), #state{}) ->
          #state{}.
on_store(_Frame, _From, #state{identity = undefined} = S) ->
    S;
on_store(_Frame, _From, #state{send_frame = undefined} = S) ->
    S;
on_store(Frame, FromNodeId, S) ->
    Record = maps:get(record, Frame),
    persist_if_valid(macula_record:verify(Record), Record, FromNodeId, S).

-spec persist_if_valid({ok, macula_record:record()} | {error, term()},
                       macula_record:record(),
                       macula_identity:pubkey(), #state{}) -> #state{}.
persist_if_valid({ok, Record}, _Orig, FromNodeId,
                 #state{record_store = Ets, identity = Id,
                        send_frame = Send} = S) ->
    store_put(Ets, Record),
    notify_record_stored(Record, S),
    Key   = macula_record:storage_key(Record),
    Reply = macula_dht_protocol:build_store_ack(Key, true, undefined, Id),
    Send(FromNodeId, Reply),
    S;
persist_if_valid({error, Reason}, Record, FromNodeId,
                 #state{identity = Id, send_frame = Send} = S) ->
    Key   = macula_record:storage_key(Record),
    Reply = macula_dht_protocol:build_store_ack(Key, false, Reason, Id),
    Send(FromNodeId, Reply),
    S.

%%---------------------------------------------------------------------
%% Incoming STORE_ACK — correlate with outstanding STORE.
%%---------------------------------------------------------------------

-spec on_store_ack(macula_frame:frame(), macula_identity:pubkey(),
                   #state{}) -> #state{}.
on_store_ack(Frame, FromNodeId, #state{pending_stores = P} = S) ->
    Key    = maps:get(key, Frame),
    Stored = maps:get(stored, Frame),
    Reason = maps:get(reason, Frame),
    resolve_store_ack(maps:take({FromNodeId, Key}, P), Stored, Reason, S).

-spec resolve_store_ack({{gen_server:from(), reference()}, map()} | error,
                        boolean(), atom() | undefined, #state{}) -> #state{}.
resolve_store_ack(error, _Stored, _Reason, S) ->
    S;
resolve_store_ack({{From, Timer}, NewP}, Stored, Reason, S) ->
    _ = erlang:cancel_timer(Timer),
    gen_server:reply(From, {ok, #{stored => Stored, reason => Reason}}),
    S#state{pending_stores = NewP}.

%%=====================================================================
%% Incoming frame dispatch
%%
%% Every frame is verified against the claimed sender NodeId before
%% the handler runs. `FromNodeId' is supplied by the transport
%% (QUIC/TLS authenticated in production; in-VM test-harness caller
%% in eunit). Unsigned / malformed / tampered frames are dropped
%% silently — logging lands with observability (Session 3.11).
%%=====================================================================

-spec dispatch_frame(macula_identity:pubkey(), macula_frame:frame(),
                     #state{}) -> #state{}.
dispatch_frame(FromNodeId, Frame, S) ->
    route_verified(macula_dht_protocol:verify(Frame, FromNodeId),
                   FromNodeId, S).

-spec route_verified({ok, macula_frame:frame()} | {error, term()},
                     macula_identity:pubkey(), #state{}) -> #state{}.
route_verified({error, _Reason}, _FromNodeId, S) ->
    S;
route_verified({ok, Frame}, FromNodeId, S) ->
    route_by_type(macula_frame:frame_type(Frame), Frame, FromNodeId, S).

-spec route_by_type(macula_frame:frame_type(), macula_frame:frame(),
                    macula_identity:pubkey(), #state{}) -> #state{}.
route_by_type(ping,       F, From, S) -> on_ping(F, From, S);
route_by_type(pong,       F, From, S) -> on_pong(F, From, S);
route_by_type(find_node,  F, From, S) -> on_find_node(F, From, S);
route_by_type(nodes,      F, From, S) -> on_nodes(F, From, S);
route_by_type(find_value, F, From, S) -> on_find_value(F, From, S);
route_by_type(value,      F, From, S) -> on_value(F, From, S);
route_by_type(store,      F, From, S) -> on_store(F, From, S);
route_by_type(store_ack,  F, From, S) -> on_store_ack(F, From, S);
route_by_type(_,         _F, _From, S) -> S.

%%---------------------------------------------------------------------
%% Incoming PING → PONG
%%---------------------------------------------------------------------

-spec on_ping(macula_frame:frame(), macula_identity:pubkey(), #state{}) ->
          #state{}.
on_ping(_Frame, _From, #state{identity = undefined} = S) ->
    S;
on_ping(_Frame, _From, #state{send_frame = undefined} = S) ->
    S;
on_ping(Frame, FromNodeId,
        #state{identity = Id, send_frame = Send} = S) ->
    Nonce = maps:get(nonce, Frame),
    Pong  = macula_dht_protocol:build_pong(Nonce, Id),
    Send(FromNodeId, Pong),
    S.

%%---------------------------------------------------------------------
%% Incoming PONG — match outstanding PING, touch peer.
%%---------------------------------------------------------------------

-spec on_pong(macula_frame:frame(), macula_identity:pubkey(), #state{}) ->
          #state{}.
on_pong(Frame, FromNodeId, #state{pending_pings = P} = S) ->
    Nonce = maps:get(nonce, Frame),
    resolve_pong(maps:find(Nonce, P), Nonce, FromNodeId, S).

-spec resolve_pong({ok, tuple()} | error, macula_dht_protocol:nonce(),
                   macula_identity:pubkey(), #state{}) -> #state{}.
resolve_pong(error, _Nonce, _From, S) ->
    %% Unsolicited PONG or nonce already timed out.
    S;
resolve_pong({ok, {Target, _From, _Timer, _Started}}, _Nonce, FromNodeId, S)
  when Target =/= FromNodeId ->
    %% PONG claims a matching nonce but comes from the wrong peer —
    %% drop it; the real target may still respond before timeout.
    S;
resolve_pong({ok, {TargetNodeId, From, TimerRef, Started}}, Nonce, TargetNodeId,
             #state{pending_pings = P, rt = Rt, sibs = Sibs} = S) ->
    _ = erlang:cancel_timer(TimerRef),
    Now = erlang:monotonic_time(millisecond),
    gen_server:reply(From, {ok, #{rtt_ms => max(0, Now - Started)}}),
    S#state{pending_pings = maps:remove(Nonce, P),
            rt   = macula_dht_routing_table:touch(TargetNodeId, Rt, Now),
            sibs = macula_dht_siblings:touch(TargetNodeId, Sibs, Now)}.

%%---------------------------------------------------------------------
%% Incoming FIND_NODE — reply with NODES (k-closest from RT).
%%---------------------------------------------------------------------

-spec on_find_node(macula_frame:frame(), macula_identity:pubkey(),
                   #state{}) -> #state{}.
on_find_node(_Frame, _From, #state{identity = undefined} = S) ->
    S;
on_find_node(_Frame, _From, #state{send_frame = undefined} = S) ->
    S;
on_find_node(Frame, FromNodeId,
             #state{identity = Id, send_frame = Send, rt = Rt, k = K} = S) ->
    Key = maps:get(key, Frame),
    Closest = macula_dht_routing_table:k_closest(Key, K, Rt),
    Reply = macula_dht_protocol:build_nodes_reply(Key, Closest, Id),
    Send(FromNodeId, Reply),
    S.

%%---------------------------------------------------------------------
%% Incoming NODES — can be a response to either FIND_NODE or a
%% FIND_VALUE fallback (see Part 6 §7.3). Match pending_find_values
%% first (the FIND_VALUE path), fall through to pending_find_nodes.
%%---------------------------------------------------------------------

-spec on_nodes(macula_frame:frame(), macula_identity:pubkey(), #state{}) ->
          #state{}.
on_nodes(Frame, FromNodeId, #state{pending_find_values = PFV,
                                    pending_find_nodes  = PFN} = S) ->
    Key  = maps:get(key, Frame),
    Refs = maps:get(nodes, Frame),
    Lookup = {FromNodeId, Key},
    route_nodes(maps:take(Lookup, PFV), maps:take(Lookup, PFN), Refs, S).

-spec route_nodes({{gen_server:from(), reference()}, map()} | error,
                  {{gen_server:from(), reference()}, map()} | error,
                  [macula_frame:station_ref()], #state{}) -> #state{}.
route_nodes({{From, Timer}, NewPFV}, _PFN, Refs, S) ->
    _ = erlang:cancel_timer(Timer),
    gen_server:reply(From, {nodes, Refs}),
    S#state{pending_find_values = NewPFV};
route_nodes(error, {{From, Timer}, NewPFN}, Refs, S) ->
    _ = erlang:cancel_timer(Timer),
    gen_server:reply(From, {ok, Refs}),
    S#state{pending_find_nodes = NewPFN};
route_nodes(error, error, _Refs, S) ->
    S.

%%---------------------------------------------------------------------
%% Incoming FIND_VALUE — local-store hit replies VALUE; miss replies
%% NODES (k-closest from the routing table) per Part 3 §4.5.
%%---------------------------------------------------------------------

-spec on_find_value(macula_frame:frame(), macula_identity:pubkey(),
                    #state{}) -> #state{}.
on_find_value(_Frame, _From, #state{identity = undefined} = S) ->
    S;
on_find_value(_Frame, _From, #state{send_frame = undefined} = S) ->
    S;
on_find_value(Frame, FromNodeId, #state{record_store = Ets} = S) ->
    Key = maps:get(key, Frame),
    reply_find_value(store_lookup(Ets, Key), Key, FromNodeId, S).

-spec reply_find_value([macula_record:record()], macula_dht_xor:id(),
                       macula_identity:pubkey(), #state{}) -> #state{}.
reply_find_value([], Key, FromNodeId,
                 #state{identity = Id, send_frame = Send, rt = Rt, k = K} = S) ->
    Closest = macula_dht_routing_table:k_closest(Key, K, Rt),
    Reply   = macula_dht_protocol:build_nodes_reply(Key, Closest, Id),
    Send(FromNodeId, Reply),
    S;
reply_find_value(Records, Key, FromNodeId,
                 #state{identity = Id, send_frame = Send} = S) ->
    Reply = macula_dht_protocol:build_value_reply(Key, Records, Id),
    Send(FromNodeId, Reply),
    S.

%%---------------------------------------------------------------------
%% Incoming VALUE — correlate with outstanding FIND_VALUE.
%%---------------------------------------------------------------------

-spec on_value(macula_frame:frame(), macula_identity:pubkey(), #state{}) ->
          #state{}.
on_value(Frame, FromNodeId, #state{pending_find_values = P} = S) ->
    Key = maps:get(key, Frame),
    Records = maps:get(records, Frame),
    resolve_value(maps:take({FromNodeId, Key}, P), Records, S).

-spec resolve_value({{gen_server:from(), reference()}, map()} | error,
                    [macula_record:record()], #state{}) -> #state{}.
resolve_value(error, _Records, S) ->
    S;
resolve_value({{From, Timer}, NewP}, Records, S) ->
    _ = erlang:cancel_timer(Timer),
    gen_server:reply(From, {value, Records}),
    S#state{pending_find_values = NewP}.
