%% @doc gen_server owning a Kademlia routing table and sibling list
%% for a single station.
%%
%% State is pure-Erlang data built from `hecate_dht_routing_table' and
%% `hecate_dht_siblings'. This is the first stateful session of Phase
%% 3 (Session 3.3) — wire-level DHT operations (PING/PONG, lookup,
%% STORE) land in Sessions 3.5+.
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
    stats/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, observe_result/0, stats/0]).

-define(DEFAULT_K, 20).
-define(DEFAULT_S, 16).

-type opts() :: #{
    self_id := hecate_dht_xor:id(),
    k       => pos_integer(),
    s       => pos_integer()
}.

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
    self_id  :: hecate_dht_xor:id(),
    rt       :: hecate_dht_routing_table:table(),
    sibs     :: hecate_dht_siblings:siblings(),
    k        :: pos_integer(),
    s        :: pos_integer()
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

%%=====================================================================
%% gen_server callbacks
%%=====================================================================

init(#{self_id := Self} = Opts) ->
    K = maps:get(k, Opts, ?DEFAULT_K),
    S = maps:get(s, Opts, ?DEFAULT_S),
    {ok, #state{
        self_id = Self,
        rt      = hecate_dht_routing_table:new(Self, K),
        sibs    = hecate_dht_siblings:new(Self, S),
        k       = K,
        s       = S
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

handle_cast(_Msg, S) ->
    {noreply, S}.

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
