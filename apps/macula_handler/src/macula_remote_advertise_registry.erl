%% @doc Station-level registry of procedures advertised by connected
%% peers (clients of this station).
%%
%% Whereas `macula_handler_registry' tracks procedures the station
%% itself serves locally (e.g. `_dht.put_record'), this registry
%% tracks procedures that a CONNECTED peer has registered through
%% an ADVERTISE wire frame. Inbound CALL frames whose
%% `(realm, procedure)' lives here are forwarded back across the
%% advertiser's QUIC connection rather than dispatched locally.
%%
%% == Lifecycle ==
%%
%% Started under the station's supervision tree alongside
%% `macula_handler_registry'. The peer_observer registers
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
%%
%% == Tombstones ==
%%
%% `unregister/3' (and `purge_conn/2') drops the entry AND records a
%% tombstone for the same `(Realm, Procedure)' key, remembering the
%% pubkey of the advertiser that owned the entry. While the tombstone
%% is active, `register/4' is rejected ONLY for foreign advertisers —
%% a re-advertise from the same source pubkey is allowed through.
%%
%% Why: the partial-mesh gossip-vs-unadvertise race never converges
%% if any register call after unregister can resurrect the entry.
%% Each station's UNADVERTISE clears its own copy, but a peer's
%% still-propagating gossip ADVERTISE re-registers it before the
%% peer's own UNADVERTISE catches up. Tombstones break that loop.
%%
%% But blocking ALL re-registers is too strict — a daemon that
%% reconnects after a network blip should be able to re-advertise
%% its own procedures immediately, not wait out the TTL. The
%% same-source carve-out preserves that case while still rejecting
%% foreign-station gossip echoes.
%%
%% Tombstone TTL is generous (`?TOMBSTONE_TTL_MS', default 10s)
%% relative to the router's 2s tick + a few hops of propagation. Once
%% the TTL expires, normal register semantics resume — by then the
%% UNADVERTISE wave has reached every reachable peer and quiesced.
-module(macula_remote_advertise_registry).
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

-type opts() :: #{tombstone_ms => non_neg_integer()}.

-define(TOMBSTONE_TTL_MS, 30_000).

-type tombstone() :: {Timer :: reference(),
                      Marker :: reference(),
                      OrigAdvertiser :: node_id() | undefined}.

-record(state, {
    %% (Realm, Procedure) -> entry()
    entries      = #{} :: #{{realm(), procedure()} => entry()},
    %% A key in this map blocks `register/4' for the same key (unless
    %% the new advertiser equals the recorded `OrigAdvertiser') until
    %% the timer fires and clears it.
    tombstones   = #{} :: #{{realm(), procedure()} => tombstone()},
    tombstone_ms = ?TOMBSTONE_TTL_MS :: non_neg_integer()
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
%% No-op if a tombstone is active for the same key (see module docs).
-spec register(pid(), realm(), procedure(), entry()) -> ok.
register(Pid, Realm, Procedure, #{advertiser := <<_:256>>,
                                  conn_pid   := ConnPid} = Entry)
  when is_binary(Realm),     byte_size(Realm)     =:= 32,
       is_binary(Procedure),
       is_pid(ConnPid) ->
    gen_server:call(Pid, {register, Realm, Procedure, Entry}).

%% @doc Drop the registration for `(Realm, Procedure)' and arm a
%% tombstone (TTL = `tombstone_ms') so a racing gossip ADVERTISE for
%% the same key cannot resurrect the entry while the UNADVERTISE wave
%% is still propagating.
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

%% @doc Bulk-drop every entry whose `conn_pid' equals `ConnPid' and
%% arm tombstones for each dropped key. Called by the peer_observer
%% on `disconnected' so a peer's advertisements cannot outlive its
%% connection — and cannot be resurrected by in-flight gossip
%% during the disconnect window.
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

init(Opts) ->
    Ttl = maps:get(tombstone_ms, Opts, ?TOMBSTONE_TTL_MS),
    {ok, #state{tombstone_ms = Ttl}}.

handle_call({register, Realm, Procedure, Entry}, _From,
            #state{entries = E, tombstones = T} = S) ->
    Key = {Realm, Procedure},
    {reply, ok, on_register(tombstone_verdict(Key, Entry, T), Key, Entry, E, S)};

handle_call({unregister, Realm, Procedure}, _From,
            #state{entries = E} = S) ->
    Key = {Realm, Procedure},
    {reply, ok, drop_and_tombstone([{Key, advertiser_of(Key, E)}], S)};

handle_call({lookup, Realm, Procedure}, _From,
            #state{entries = E} = S) ->
    Reply = case maps:find({Realm, Procedure}, E) of
                {ok, Entry} -> {ok, Entry};
                error       -> {error, not_found}
            end,
    {reply, Reply, S};

handle_call({purge_conn, ConnPid}, _From, #state{entries = E} = S) ->
    Drop = [{K, A} || {K, #{conn_pid := P, advertiser := A}}
                          <- maps:to_list(E),
                      P =:= ConnPid],
    {reply, ok, drop_and_tombstone(Drop, S)};

handle_call(list, _From, #state{entries = E} = S) ->
    {reply, [{R, P, V} || {{R, P}, V} <- maps:to_list(E)], S};

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({tombstone_expired, Key, TRef},
            #state{tombstones = T} = S) ->
    {noreply, S#state{tombstones = clear_if_match(Key, TRef, T)}};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, #state{tombstones = T}) ->
    _ = [erlang:cancel_timer(Timer, [{async, true}, {info, false}])
         || {Timer, _Marker, _OrigAdv} <- maps:values(T)],
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%====================================================================
%% Internals
%%====================================================================

on_register(allow, Key, Entry, E, S) ->
    S#state{entries = E#{Key => Entry}};
on_register(block, _Key, _Entry, _E, S) ->
    S.

%% A foreign advertiser hitting an active tombstone is rejected; the
%% original-source advertiser punches through (reconnect-and-readvertise
%% case). No tombstone = unconditional allow.
tombstone_verdict(Key, #{advertiser := NewAdv}, T) ->
    case maps:find(Key, T) of
        {ok, {_Timer, _Marker, OrigAdv}}
          when OrigAdv =:= NewAdv; OrigAdv =:= undefined ->
            allow;
        {ok, _Tombstone} ->
            block;
        error ->
            allow
    end.

advertiser_of(Key, E) ->
    case maps:find(Key, E) of
        {ok, #{advertiser := A}} -> A;
        error                    -> undefined
    end.

drop_and_tombstone(KeyAdvList, #state{entries = E, tombstones = T,
                                      tombstone_ms = Ttl} = S) ->
    NewE = lists:foldl(fun({K, _A}, Acc) -> maps:remove(K, Acc) end,
                       E, KeyAdvList),
    NewT = lists:foldl(fun({K, A}, Acc) -> arm_tombstone(K, A, Ttl, Acc) end,
                       T, KeyAdvList),
    S#state{entries = NewE, tombstones = NewT}.

arm_tombstone(Key, OrigAdv, Ttl, T) ->
    cancel_existing(maps:find(Key, T)),
    %% Marker lets `handle_info' detect a stale expiry that races a
    %% re-arm: a cancel + new send_after within the message-in-flight
    %% window means the OLD message can still arrive after the new
    %% timer is already in the map. Without the marker, we'd clear
    %% the fresh tombstone on the stale expiry.
    Marker = make_ref(),
    Timer  = erlang:send_after(Ttl, self(),
                               {tombstone_expired, Key, Marker}),
    %% If the prior tombstone had a known OrigAdv and the new one
    %% doesn't, keep the prior's — preserves the same-source carve-out
    %% even when a chain of unregister calls includes a "blind"
    %% second one (e.g. unregister called after the entry was already
    %% gone; the new call doesn't know who used to own it).
    Effective = preserve_orig_adv(OrigAdv, maps:find(Key, T)),
    T#{Key => {Timer, Marker, Effective}}.

preserve_orig_adv(undefined, {ok, {_, _, PriorAdv}}) when PriorAdv =/= undefined ->
    PriorAdv;
preserve_orig_adv(NewAdv, _Prior) ->
    NewAdv.

cancel_existing({ok, {Timer, _Marker, _OrigAdv}}) ->
    _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
    ok;
cancel_existing(error) ->
    ok.

clear_if_match(Key, Marker, T) ->
    case maps:find(Key, T) of
        {ok, {_Timer, Marker, _OrigAdv}} -> maps:remove(Key, T);
        _Other                           -> T
    end.
