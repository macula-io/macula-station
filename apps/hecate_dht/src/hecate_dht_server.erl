%% @doc gen_server owning a Kademlia routing table and sibling list
%% for a single station.
%%
%% State is built from `hecate_dht_routing_table' and
%% `hecate_dht_siblings' (Session 3.3) plus pending-request tables
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
%% in-VM `hecate_dht_server' pid dispatch.
%%
%% == Observation semantics ==
%%
%% Every observed peer is offered to both the routing table and the
%% sibling list:
%%
%% <ul>
%%   <li>Routing-table admission is the *scored* algorithm
%%       (`hecate_dht_bucket'): uptime, novelty, incumbency, latency.</li>
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
-module(hecate_dht_server).
-behaviour(gen_server).

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
    handle_frame/3
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, observe_result/0, stats/0, send_fun/0,
              ping_result/0, find_node_result/0]).

-define(DEFAULT_K, 20).
-define(DEFAULT_S, 16).
-define(DEFAULT_PING_TIMEOUT_MS,      2_000).
-define(DEFAULT_FIND_NODE_TIMEOUT_MS, 5_000).

-type send_fun() :: fun((macula_identity:pubkey(), macula_frame:frame()) ->
                              ok | {error, term()}).

-type opts() :: #{
    self_id               := hecate_dht_xor:id(),
    k                     => pos_integer(),
    s                     => pos_integer(),
    identity              => macula_identity:key_pair(),
    send_frame            => send_fun(),
    ping_timeout_ms       => pos_integer(),
    find_node_timeout_ms  => pos_integer()
}.

-type ping_result() :: {ok, #{rtt_ms := non_neg_integer()}}
                     | {error, timeout | no_transport | term()}.

-type find_node_result() :: {ok, [macula_frame:station_ref()]}
                          | {error, timeout | no_transport | term()}.

-type observe_result() ::
      admitted
    | touched
    | {replaced, Evicted :: hecate_dht_entry:entry()}
    | rejected.

-type stats() :: #{
    self_id       := hecate_dht_xor:id(),
    size          := non_neg_integer(),
    bucket_count  := non_neg_integer(),
    sibling_count := non_neg_integer(),
    k             := pos_integer(),
    s             := pos_integer()
}.

-record(state, {
    self_id              :: hecate_dht_xor:id(),
    rt                   :: hecate_dht_routing_table:table(),
    sibs                 :: hecate_dht_siblings:siblings(),
    k                    :: pos_integer(),
    s                    :: pos_integer(),
    identity             :: macula_identity:key_pair() | undefined,
    send_frame           :: send_fun() | undefined,
    ping_timeout_ms      :: pos_integer(),
    find_node_timeout_ms :: pos_integer(),
    %% {Nonce => {TargetNodeId, From, TimerRef, StartedMono}}
    pending_pings = #{}  :: #{hecate_dht_protocol:nonce() =>
                              {macula_identity:pubkey(),
                               gen_server:from(),
                               reference(),
                               integer()}},
    %% {{PeerNodeId, Key} => {From, TimerRef}}
    pending_find_nodes = #{} :: #{{macula_identity:pubkey(),
                                   hecate_dht_xor:id()} =>
                                  {gen_server:from(), reference()}}
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

-spec observe(pid(), hecate_dht_entry:spec()) -> observe_result().
observe(Pid, Spec) when is_map(Spec) ->
    gen_server:call(Pid, {observe, Spec}).

-spec touch(pid(), hecate_dht_xor:id()) -> ok.
touch(Pid, <<_:256>> = Id) ->
    gen_server:cast(Pid, {touch, Id}).

-spec forget(pid(), hecate_dht_xor:id()) -> ok.
forget(Pid, <<_:256>> = Id) ->
    gen_server:cast(Pid, {forget, Id}).

-spec self_id(pid()) -> hecate_dht_xor:id().
self_id(Pid) ->
    gen_server:call(Pid, self_id).

-spec find(pid(), hecate_dht_xor:id()) ->
        {ok, hecate_dht_entry:entry()} | error.
find(Pid, <<_:256>> = Id) ->
    gen_server:call(Pid, {find, Id}).

-spec contains(pid(), hecate_dht_xor:id()) -> boolean().
contains(Pid, <<_:256>> = Id) ->
    gen_server:call(Pid, {contains, Id}).

-spec k_closest(pid(), hecate_dht_xor:id(), non_neg_integer()) ->
          [hecate_dht_entry:entry()].
k_closest(Pid, <<_:256>> = Target, K) when is_integer(K), K >= 0 ->
    gen_server:call(Pid, {k_closest, Target, K}).

-spec siblings(pid()) -> [hecate_dht_entry:entry()].
siblings(Pid) ->
    gen_server:call(Pid, siblings).

-spec sibling_ids(pid()) -> [hecate_dht_xor:id()].
sibling_ids(Pid) ->
    gen_server:call(Pid, sibling_ids).

-spec size(pid()) -> non_neg_integer().
size(Pid) ->
    gen_server:call(Pid, size).

-spec bucket_count(pid()) -> non_neg_integer().
bucket_count(Pid) ->
    gen_server:call(Pid, bucket_count).

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

-spec find_node(pid(), hecate_dht_xor:id(), macula_identity:pubkey()) ->
        find_node_result().
find_node(Pid, Key, PeerId) ->
    find_node(Pid, Key, PeerId, ?DEFAULT_FIND_NODE_TIMEOUT_MS).

-spec find_node(pid(), hecate_dht_xor:id(), macula_identity:pubkey(),
                pos_integer()) -> find_node_result().
find_node(Pid, <<_:256>> = Key, <<_:256>> = PeerId, Timeout)
  when is_integer(Timeout), Timeout > 0 ->
    gen_server:call(Pid, {find_node, Key, PeerId, Timeout}, Timeout + 1_000).

-spec handle_frame(pid(), macula_identity:pubkey(), macula_frame:frame()) -> ok.
handle_frame(Pid, <<_:256>> = FromNodeId, Frame) when is_map(Frame) ->
    gen_server:cast(Pid, {frame, FromNodeId, Frame}).

%%=====================================================================
%% gen_server callbacks
%%=====================================================================

init(#{self_id := Self} = Opts) ->
    K = maps:get(k, Opts, ?DEFAULT_K),
    S = maps:get(s, Opts, ?DEFAULT_S),
    {ok, #state{
        self_id              = Self,
        rt                   = hecate_dht_routing_table:new(Self, K),
        sibs                 = hecate_dht_siblings:new(Self, S),
        k                    = K,
        s                    = S,
        identity             = maps:get(identity, Opts, undefined),
        send_frame           = maps:get(send_frame, Opts, undefined),
        ping_timeout_ms      = maps:get(ping_timeout_ms, Opts,
                                        ?DEFAULT_PING_TIMEOUT_MS),
        find_node_timeout_ms = maps:get(find_node_timeout_ms, Opts,
                                        ?DEFAULT_FIND_NODE_TIMEOUT_MS)
    }}.

handle_call(self_id, _From, #state{self_id = Self} = S) ->
    {reply, Self, S};

handle_call({observe, Spec}, _From, S) ->
    handle_observe(Spec, S);

handle_call({find, Id}, _From, #state{rt = Rt} = S) ->
    {reply, hecate_dht_routing_table:find(Id, Rt), S};

handle_call({contains, Id}, _From, #state{rt = Rt} = S) ->
    {reply, hecate_dht_routing_table:contains(Id, Rt), S};

handle_call({k_closest, Target, K}, _From, #state{rt = Rt} = S) ->
    {reply, hecate_dht_routing_table:k_closest(Target, K, Rt), S};

handle_call(siblings, _From, #state{sibs = Sibs} = S) ->
    {reply, hecate_dht_siblings:members(Sibs), S};

handle_call(sibling_ids, _From, #state{sibs = Sibs} = S) ->
    {reply, hecate_dht_siblings:node_ids(Sibs), S};

handle_call(size, _From, #state{rt = Rt} = S) ->
    {reply, hecate_dht_routing_table:size(Rt), S};

handle_call(bucket_count, _From, #state{rt = Rt} = S) ->
    {reply, hecate_dht_routing_table:bucket_count(Rt), S};

handle_call(stats, _From, S) ->
    {reply, build_stats(S), S};

handle_call({ping_peer, TargetId, Timeout}, From, S) ->
    dispatch_ping(TargetId, Timeout, From, S);

handle_call({find_node, Key, PeerId, Timeout}, From, S) ->
    dispatch_find_node(Key, PeerId, Timeout, From, S);

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast({touch, Id}, #state{rt = Rt, sibs = Sibs} = S) ->
    Now = erlang:monotonic_time(millisecond),
    {noreply, S#state{
        rt   = hecate_dht_routing_table:touch(Id, Rt, Now),
        sibs = hecate_dht_siblings:touch(Id, Sibs, Now)
    }};

handle_cast({forget, Id}, #state{rt = Rt, sibs = Sibs} = S) ->
    {noreply, S#state{
        rt   = hecate_dht_routing_table:remove(Id, Rt),
        sibs = hecate_dht_siblings:remove(Id, Sibs)
    }};

handle_cast({frame, FromNodeId, Frame}, S) ->
    {noreply, dispatch_frame(FromNodeId, Frame, S)};

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({ping_timeout, Nonce}, S) ->
    {noreply, on_ping_timeout(Nonce, S)};

handle_info({find_node_timeout, Key, PeerId}, S) ->
    {noreply, on_find_node_timeout(Key, PeerId, S)};

handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%=====================================================================
%% Observation
%%=====================================================================

-spec handle_observe(hecate_dht_entry:spec(), #state{}) ->
          {reply, observe_result(), #state{}}.
handle_observe(Spec, #state{rt = Rt, sibs = Sibs} = S) ->
    Now = erlang:monotonic_time(millisecond),
    Entry = hecate_dht_entry:new(Spec, Now),
    {RtResult, Rt1} = apply_rt(Entry, Rt, Now),
    Sibs1 = apply_siblings(Entry, Sibs, Now),
    {reply, classify(RtResult), S#state{rt = Rt1, sibs = Sibs1}}.

-spec apply_rt(hecate_dht_entry:entry(), hecate_dht_routing_table:table(),
               integer()) ->
          {hecate_dht_routing_table:insert_result(),
           hecate_dht_routing_table:table()}.
apply_rt(Entry, Rt, Now) ->
    Result = hecate_dht_routing_table:insert(Entry, Rt, Now),
    {Result, extract_table(Result)}.

-spec apply_siblings(hecate_dht_entry:entry(), hecate_dht_siblings:siblings(),
                     integer()) -> hecate_dht_siblings:siblings().
apply_siblings(Entry, Sibs, Now) ->
    extract_siblings(hecate_dht_siblings:insert(Entry, Sibs, Now)).

-spec extract_table(hecate_dht_routing_table:insert_result()) ->
          hecate_dht_routing_table:table().
extract_table({admitted, T})     -> T;
extract_table({touched,  T})     -> T;
extract_table({replaced, _E, T}) -> T;
extract_table({rejected, T})     -> T.

-spec extract_siblings(hecate_dht_siblings:insert_result()) ->
          hecate_dht_siblings:siblings().
extract_siblings({admitted, S})     -> S;
extract_siblings({touched,  S})     -> S;
extract_siblings({replaced, _E, S}) -> S;
extract_siblings({rejected, S})     -> S.

-spec classify(hecate_dht_routing_table:insert_result()) -> observe_result().
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
        size          => hecate_dht_routing_table:size(Rt),
        bucket_count  => hecate_dht_routing_table:bucket_count(Rt),
        sibling_count => hecate_dht_siblings:size(Sibs),
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
    {Frame, Nonce} = hecate_dht_protocol:build_ping(Id),
    Send(TargetId, Frame),
    TimerRef = erlang:send_after(Timeout, self(), {ping_timeout, Nonce}),
    StartedMono = erlang:monotonic_time(millisecond),
    {noreply, S#state{pending_pings =
        P#{Nonce => {TargetId, From, TimerRef, StartedMono}}}}.

-spec on_ping_timeout(hecate_dht_protocol:nonce(), #state{}) -> #state{}.
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

-spec dispatch_find_node(hecate_dht_xor:id(), macula_identity:pubkey(),
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
    Frame = hecate_dht_protocol:build_find_node(Key, Self, 0, Id),
    Send(PeerId, Frame),
    TimerRef = erlang:send_after(Timeout, self(),
                                 {find_node_timeout, Key, PeerId}),
    {noreply, S#state{pending_find_nodes =
        P#{{PeerId, Key} => {From, TimerRef}}}}.

-spec on_find_node_timeout(hecate_dht_xor:id(), macula_identity:pubkey(),
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
    route_verified(hecate_dht_protocol:verify(Frame, FromNodeId),
                   FromNodeId, S).

-spec route_verified({ok, macula_frame:frame()} | {error, term()},
                     macula_identity:pubkey(), #state{}) -> #state{}.
route_verified({error, _Reason}, _FromNodeId, S) ->
    S;
route_verified({ok, Frame}, FromNodeId, S) ->
    route_by_type(macula_frame:frame_type(Frame), Frame, FromNodeId, S).

-spec route_by_type(macula_frame:frame_type(), macula_frame:frame(),
                    macula_identity:pubkey(), #state{}) -> #state{}.
route_by_type(ping,      F, From, S) -> on_ping(F, From, S);
route_by_type(pong,      F, From, S) -> on_pong(F, From, S);
route_by_type(find_node, F, From, S) -> on_find_node(F, From, S);
route_by_type(nodes,     F, From, S) -> on_nodes(F, From, S);
route_by_type(_,        _F, _From, S) -> S.

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
    Pong  = hecate_dht_protocol:build_pong(Nonce, Id),
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

-spec resolve_pong({ok, tuple()} | error, hecate_dht_protocol:nonce(),
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
            rt   = hecate_dht_routing_table:touch(TargetNodeId, Rt, Now),
            sibs = hecate_dht_siblings:touch(TargetNodeId, Sibs, Now)}.

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
    Closest = hecate_dht_routing_table:k_closest(Key, K, Rt),
    Reply = hecate_dht_protocol:build_nodes_reply(Key, Closest, Id),
    Send(FromNodeId, Reply),
    S.

%%---------------------------------------------------------------------
%% Incoming NODES — correlate with outstanding FIND_NODE.
%%---------------------------------------------------------------------

-spec on_nodes(macula_frame:frame(), macula_identity:pubkey(), #state{}) ->
          #state{}.
on_nodes(Frame, FromNodeId, #state{pending_find_nodes = P} = S) ->
    Key = maps:get(key, Frame),
    Refs = maps:get(nodes, Frame),
    resolve_nodes(maps:take({FromNodeId, Key}, P), Refs, S).

-spec resolve_nodes(error | {tuple(), map()}, [macula_frame:station_ref()],
                    #state{}) -> #state{}.
resolve_nodes(error, _Refs, S) ->
    S;
resolve_nodes({{From, TimerRef}, NewP}, Refs, S) ->
    _ = erlang:cancel_timer(TimerRef),
    gen_server:reply(From, {ok, Refs}),
    S#state{pending_find_nodes = NewP}.
