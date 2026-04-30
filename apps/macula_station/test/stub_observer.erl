%% Tiny stub matching the `macula_station_peer_observer:conn_for/2'
%% surface. Tests inject one of these so the fact_publisher can
%% resolve subscriber pubkeys to fake conn pids without needing a
%% real peering layer.
%%
%% Public API mirrors the relevant subset of `peer_observer':
%%
%%   * `conn_for(Pid, NodeId)' — returns `{ok, ConnPid}' when
%%     `register_conn/3' was called for that NodeId, else `error'.
%%   * `register_conn(Pid, NodeId, ConnPid)' — set the mapping.
-module(stub_observer).
-behaviour(gen_server).

-export([start_link/0, stop/1, conn_for/2, register_conn/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

stop(Pid) ->
    gen_server:stop(Pid).

conn_for(Pid, NodeId) ->
    gen_server:call(Pid, {conn_for, NodeId}).

register_conn(Pid, NodeId, ConnPid) ->
    gen_server:call(Pid, {register_conn, NodeId, ConnPid}).

init([]) ->
    {ok, #{conns => #{}}}.

handle_call({conn_for, NodeId}, _From, #{conns := C} = S) ->
    {reply, lookup(maps:find(NodeId, C)), S};
handle_call({register_conn, NodeId, ConnPid}, _From, #{conns := C} = S) ->
    {reply, ok, S#{conns => C#{NodeId => ConnPid}}};
handle_call(_, _, S) ->
    {reply, {error, unknown}, S}.

handle_cast(_, S) -> {noreply, S}.
handle_info(_, S) -> {noreply, S}.
terminate(_, _)   -> ok.
code_change(_, S, _) -> {ok, S}.

lookup({ok, Pid}) -> {ok, Pid};
lookup(error)     -> error.
