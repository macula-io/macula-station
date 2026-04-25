%% @doc Per-station gen_server.
%%
%% Owns:
%% <ul>
%%   <li>The QUIC listener for inbound peers.</li>
%%   <li>The SWIM failure detector (`hecate_swim') for membership liveness.</li>
%%   <li>The map of live peer connections.</li>
%%   <li>The local tombstone log (Phase 1: in-memory; Phase 5+: gossip).</li>
%% </ul>
%%
%% Started linked by `hecate_station:start_link/1'. Multiple instances
%% can coexist in one VM (used by the walking-skeleton + chaos CT suites).
-module(hecate_station_server).
-behaviour(gen_server).

-export([
    start_link/1,
    stop/1, stop/2,
    identity/1,
    listen_addr/1,
    connect_to/2,
    peers/1,
    tombstones/1,
    swim_members/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export_type([connect_target/0, peer_entry/0]).

-record(state, {
    config       :: hecate_station_config:station_opts(),
    listener     :: macula_transport:listener(),
    listen_addr  :: {inet:ip_address() | string(), inet:port_number()},
    swim         :: pid(),
    peers        :: #{pid() => peer_entry()},
    tombstones   :: [hecate_record:record()]
}).

-type peer_entry() :: #{
    role         := client | server,
    peer_node_id => hecate_identity:pubkey(),
    connected_at => integer()
}.

-type connect_target() :: #{
    host := string() | binary(),
    port := inet:port_number(),
    timeout_ms => non_neg_integer()
}.

%%------------------------------------------------------------------
%% Public API
%%------------------------------------------------------------------

-spec start_link(hecate_station_config:opts()) -> {ok, pid()} | {error, term()}.
start_link(Spec) ->
    gen_server:start_link(?MODULE, Spec, []).

-spec stop(pid()) -> {ok, [hecate_record:record()]}.
stop(Pid) -> stop(Pid, operator_stop).

-spec stop(pid(), atom()) -> {ok, [hecate_record:record()]}.
stop(Pid, Reason) -> gen_server:call(Pid, {stop, Reason}, 10_000).

-spec identity(pid()) -> hecate_identity:key_pair().
identity(Pid) -> gen_server:call(Pid, identity).

-spec listen_addr(pid()) -> {inet:ip_address() | string(), inet:port_number()}.
listen_addr(Pid) -> gen_server:call(Pid, listen_addr).

-spec connect_to(pid(), connect_target()) -> {ok, pid()} | {error, term()}.
connect_to(Pid, Target) -> gen_server:call(Pid, {connect_to, Target}, 10_000).

-spec peers(pid()) -> [{pid(), peer_entry()}].
peers(Pid) -> gen_server:call(Pid, peers).

-spec tombstones(pid()) -> [hecate_record:record()].
tombstones(Pid) -> gen_server:call(Pid, tombstones).

-spec swim_members(pid()) -> [hecate_swim:member()].
swim_members(Pid) -> gen_server:call(Pid, swim_members).

%%------------------------------------------------------------------
%% gen_server
%%------------------------------------------------------------------

init(Spec) ->
    on_config_loaded(hecate_station_config:load(Spec)).

on_config_loaded({error, R}) ->
    {stop, R};
on_config_loaded({ok, Cfg}) ->
    process_flag(trap_exit, true),
    on_swim_started(start_swim(Cfg), Cfg).

start_swim(#{identity := Kp} = Cfg) ->
    SwimOpts0 = #{
        self_node_id    => hecate_identity:public(Kp),
        identity        => Kp,
        controlling_pid => self()
    },
    SwimOpts = maps:merge(maps:with([period_ms, ping_timeout_ms,
                                     suspect_timeout_ms], Cfg), SwimOpts0),
    hecate_swim:start_link(SwimOpts).

on_swim_started({ok, Swim}, Cfg) ->
    on_listener_started(start_listener(Cfg), Cfg, Swim);
on_swim_started(Err, _Cfg) ->
    {stop, {swim_failed, Err}}.

start_listener(#{bind := Bind, port := Port, certfile := C, keyfile := K}) ->
    macula_transport:listen(#{
        bind => Bind, port => Port, certfile => C, keyfile => K
    }).

on_listener_started({ok, Listener}, Cfg, Swim) ->
    ok = macula_transport:accept(Listener),
    LocalAddr = derive_listen_addr(Cfg),
    State = #state{
        config      = Cfg,
        listener    = Listener,
        listen_addr = LocalAddr,
        swim        = Swim,
        peers       = #{},
        tombstones  = []
    },
    {ok, State};
on_listener_started(Err, _Cfg, Swim) ->
    catch hecate_swim:stop(Swim),
    {stop, {listen_failed, Err}}.

derive_listen_addr(#{bind := Bind, port := Port}) ->
    {Bind, Port}.

%%------------------------------------------------------------------
%% Calls
%%------------------------------------------------------------------

handle_call(identity, _From, #state{config = #{identity := Kp}} = S) ->
    {reply, Kp, S};
handle_call(listen_addr, _From, #state{listen_addr = A} = S) ->
    {reply, A, S};
handle_call(peers, _From, #state{peers = P} = S) ->
    {reply, maps:to_list(P), S};
handle_call(tombstones, _From, #state{tombstones = T} = S) ->
    {reply, T, S};
handle_call(swim_members, _From, #state{swim = Swim} = S) ->
    {reply, hecate_swim:members(Swim), S};
handle_call({connect_to, Target}, _From, S) ->
    handle_connect(Target, S);
handle_call({stop, Reason}, _From, S) ->
    NewS = drain_and_tombstone(Reason, S),
    {stop, normal, {ok, NewS#state.tombstones}, NewS};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_connect(Target, #state{config = Cfg, peers = Peers} = S) ->
    Opts = peer_opts(Cfg, Target, client),
    on_outbound_started(hecate_peering:connect(Opts), Peers, S).

on_outbound_started({ok, Pid}, Peers, S) ->
    Entry = #{role => client, connected_at => erlang:system_time(millisecond)},
    {reply, {ok, Pid}, S#state{peers = Peers#{Pid => Entry}}};
on_outbound_started(Err, _Peers, S) ->
    {reply, Err, S}.

%%------------------------------------------------------------------
%% Casts
%%------------------------------------------------------------------

handle_cast(_Msg, S) -> {noreply, S}.

%%------------------------------------------------------------------
%% Info — listener + peering + swim notifications
%%------------------------------------------------------------------

handle_info({quic, new_conn, Conn, _Info}, S) ->
    handle_inbound(Conn, S);
handle_info({hecate_peering, connected, Pid, PeerNodeId}, S) ->
    {noreply, on_peer_connected(Pid, PeerNodeId, S)};
handle_info({hecate_peering, frame, ConnPid, Frame}, S) ->
    {noreply, dispatch_peer_frame(ConnPid, Frame, S)};
handle_info({hecate_peering, disconnected, Pid, _Reason}, S) ->
    {noreply, on_peer_disconnected(Pid, S)};
handle_info({hecate_swim, member_state, _NodeId, _State}, S) ->
    %% Phase 2: bubble SWIM events as diagnostics. Phase 3+ wires to DHT.
    {noreply, S};
handle_info({'EXIT', Pid, _Reason}, S) ->
    {noreply, on_peer_disconnected(Pid, S)};
handle_info(_Other, S) ->
    {noreply, S}.

handle_inbound(Conn, #state{config = Cfg, listener = Listener, peers = Peers} = S) ->
    Opts = peer_opts(Cfg, undefined, server),
    Result = hecate_peering:accept(Conn, Opts),
    ok = macula_transport:accept(Listener),
    on_inbound_started(Result, Peers, S).

on_inbound_started({ok, Pid}, Peers, S) ->
    Entry = #{role => server, connected_at => erlang:system_time(millisecond)},
    {noreply, S#state{peers = Peers#{Pid => Entry}}};
on_inbound_started(_Err, _Peers, S) ->
    {noreply, S}.

on_peer_connected(ConnPid, PeerNodeId, #state{swim = Swim} = S) ->
    hecate_swim:add_peer(Swim, PeerNodeId, ConnPid),
    update_peer(ConnPid, PeerNodeId, S).

on_peer_disconnected(ConnPid, #state{swim = Swim, peers = Peers} = S) ->
    _ = maybe_remove_swim_peer(ConnPid, Peers, Swim),
    drop_peer(ConnPid, S).

maybe_remove_swim_peer(ConnPid, Peers, Swim) ->
    case maps:get(ConnPid, Peers, undefined) of
        #{peer_node_id := NodeId} -> hecate_swim:remove_peer(Swim, NodeId);
        _ -> ok
    end.

dispatch_peer_frame(ConnPid, Frame, #state{swim = Swim, peers = Peers} = S) ->
    case {hecate_frame:frame_type(Frame), maps:get(ConnPid, Peers, undefined)} of
        {Type, #{peer_node_id := From}} when Type =:= swim_ping;
                                              Type =:= swim_ack;
                                              Type =:= swim_suspect;
                                              Type =:= swim_confirm ->
            case hecate_frame:verify(Frame, From) of
                {ok, _}         -> hecate_swim:handle_frame(Swim, From, Frame);
                {error, _Reason} -> ok
            end,
            S;
        _ ->
            S
    end.

%%------------------------------------------------------------------
%% Termination
%%------------------------------------------------------------------

terminate(_Reason, S) ->
    _ = drain_and_tombstone(operator_stop, S),
    ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%------------------------------------------------------------------
%% Internals
%%------------------------------------------------------------------

peer_opts(#{identity := Id, realms := R, capabilities := C}, undefined, Role) ->
    #{
        role            => Role,
        identity        => Id,
        realms          => R,
        capabilities    => C,
        controlling_pid => self()
    };
peer_opts(#{identity := Id, realms := R, capabilities := C}, Target, Role) ->
    #{
        role            => Role,
        identity        => Id,
        realms          => R,
        capabilities    => C,
        controlling_pid => self(),
        target          => Target
    }.

update_peer(Pid, PeerNodeId, #state{peers = P} = S) ->
    case maps:get(Pid, P, undefined) of
        undefined -> S;
        Entry -> S#state{peers = P#{Pid => Entry#{peer_node_id => PeerNodeId}}}
    end.

drop_peer(Pid, #state{peers = P} = S) ->
    S#state{peers = maps:remove(Pid, P)}.

%% Build self-tombstone, send GOODBYE to peers, stop SWIM, close listener.
drain_and_tombstone(Reason, #state{config = #{identity := Kp}, peers = Peers,
                                   tombstones = Tombs, listener = L,
                                   swim = Swim} = S) ->
    Tombstone = build_tombstone(Kp, Reason),
    [hecate_peering:close(P, Reason) || P <- maps:keys(Peers)],
    catch hecate_swim:stop(Swim),
    catch macula_transport:close_listener(L),
    S#state{tombstones = [Tombstone | Tombs], peers = #{}}.

build_tombstone(Kp, Reason) ->
    Pub = hecate_identity:public(Kp),
    %% Tombstone the node_record (type 0x01).
    Unsigned = hecate_record:tombstone(Pub, 16#01, Reason),
    hecate_record:sign(Unsigned, Kp).
