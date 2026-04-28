%% @doc Per-identity registry of procedures advertised by connected
%% peers (clients of this station).
%%
%% Whereas `hecate_handler_registry' tracks procedures the station
%% itself serves locally (e.g. `_dht.put_record'), this registry
%% tracks procedures that a CONNECTED peer has registered through
%% an ADVERTISE wire frame. Inbound CALL frames whose
%% `(realm, procedure)' lives here are forwarded back across the
%% advertiser's QUIC connection rather than dispatched locally.
%%
%% == Lifecycle ==
%%
%% Started by `hecate_station_identity_sup' as a per-identity child
%% alongside `hecate_handler_registry'. The peer_observer registers
%% entries when it observes an ADVERTISE frame and purges them when
%% the originating QUIC connection drops or an UNADVERTISE arrives.
%%
%% == Single-provider invariant ==
%%
%% A `(realm, procedure)' tuple has at most one advertiser at a
%% time on this station. Re-advertising replaces the prior entry
%% (idempotent + reconnect-safe). Different stations may serve the
%% same procedure independently — that is a discovery concern, not
%% a per-station invariant.
-module(hecate_remote_advertise_registry).
-behaviour(gen_server).

-export([start_link/0, start_link/1,
         register/4,
         unregister/3,
         lookup/3,
         purge_conn/2,
         list/1,
         stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([entry/0]).

-type realm()     :: <<_:256>>.
-type procedure() :: binary().
-type node_id()   :: <<_:256>>.

-type entry() :: #{
    advertiser := node_id(),
    conn_pid   := pid()
}.

-type opts() :: #{identity_key => term()}.

-record(state, {
    %% (Realm, Procedure) -> entry()
    entries = #{} :: #{{realm(), procedure()} => entry()}
}).

%%====================================================================
%% Public API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) when is_map(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

%% @doc Register `(Realm, Procedure)' as advertised by `AdvertiserNodeId'
%% reachable through `ConnPid'. Idempotent: replaces any prior entry.
-spec register(pid(), realm(), procedure(), entry()) -> ok.
register(Pid, Realm, Procedure, #{advertiser := <<_:256>>,
                                  conn_pid   := ConnPid} = Entry)
  when is_binary(Realm),     byte_size(Realm)     =:= 32,
       is_binary(Procedure),
       is_pid(ConnPid) ->
    gen_server:call(Pid, {register, Realm, Procedure, Entry}).

%% @doc Drop the registration for `(Realm, Procedure)' iff its
%% advertiser pubkey matches `Advertiser'. Mismatched advertiser is
%% a no-op (defends against an UNADVERTISE racing a re-registration
%% from a fresh connection).
-spec unregister(pid(), realm(), procedure()) -> ok.
unregister(Pid, Realm, Procedure)
  when is_binary(Realm),     byte_size(Realm)     =:= 32,
       is_binary(Procedure) ->
    gen_server:call(Pid, {unregister, Realm, Procedure}).

%% @doc Look up an advertiser entry by `(Realm, Procedure)'.
-spec lookup(pid(), realm(), procedure()) ->
    {ok, entry()} | {error, not_found}.
lookup(Pid, Realm, Procedure)
  when is_binary(Realm),     byte_size(Realm)     =:= 32,
       is_binary(Procedure) ->
    gen_server:call(Pid, {lookup, Realm, Procedure}).

%% @doc Bulk-drop every entry whose `conn_pid' equals `ConnPid'.
%% Called by the peer_observer on `disconnected' so a peer's
%% advertisements cannot outlive its connection.
-spec purge_conn(pid(), pid()) -> ok.
purge_conn(Pid, ConnPid) when is_pid(ConnPid) ->
    gen_server:call(Pid, {purge_conn, ConnPid}).

-spec list(pid()) -> [{realm(), procedure(), entry()}].
list(Pid) ->
    gen_server:call(Pid, list).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%%====================================================================
%% gen_server
%%====================================================================

init(_Opts) ->
    {ok, #state{}}.

handle_call({register, Realm, Procedure, Entry}, _From,
            #state{entries = E} = S) ->
    {reply, ok,
     S#state{entries = E#{{Realm, Procedure} => Entry}}};

handle_call({unregister, Realm, Procedure}, _From,
            #state{entries = E} = S) ->
    {reply, ok,
     S#state{entries = maps:remove({Realm, Procedure}, E)}};

handle_call({lookup, Realm, Procedure}, _From,
            #state{entries = E} = S) ->
    Reply = case maps:find({Realm, Procedure}, E) of
                {ok, Entry} -> {ok, Entry};
                error       -> {error, not_found}
            end,
    {reply, Reply, S};

handle_call({purge_conn, ConnPid}, _From, #state{entries = E} = S) ->
    Kept = maps:filter(fun(_, #{conn_pid := P}) -> P =/= ConnPid end, E),
    {reply, ok, S#state{entries = Kept}};

handle_call(list, _From, #state{entries = E} = S) ->
    {reply, [{R, P, V} || {{R, P}, V} <- maps:to_list(E)], S};

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.
