%% @doc Peer observer — glue between peering, DHT, SWIM, and realms.
%%
%% Each station has exactly one observer (when booted via the
%% supervisor). It is the `controlling_pid' for every
%% `macula_peering_conn' worker the listener accepts and for every
%% outbound connect initiated via `hecate_station:connect_to/1'. It
%% turns peering events into the mutations the rest of the station
%% expects:
%%
%% <ul>
%%   <li>`{macula_peering, connected, ConnPid, PeerNodeId}' →
%%       `hecate_dht:observe/2' with an entry spec carrying
%%       `tier = t0' and `hecate_swim:add_peer/3' so the failure
%%       detector starts probing the new member.</li>
%%   <li>`{macula_peering, frame, ConnPid, Frame}' — the observer
%%       classifies the frame:
%%       <ul>
%%         <li>SWIM frames (ping / ack / suspect / confirm) →
%%             verify signature + `hecate_swim:handle_frame/3'.</li>
%%         <li>HyParView + Plumtree + Pubsub frames → look up the
%%             target realm via the `realms' registry and forward
%%             to the matching `hecate_station_realm' process.</li>
%%         <li>Anything else → drop silently.</li>
%%       </ul></li>
%%   <li>`{macula_peering, disconnected, ConnPid, _Reason}' →
%%       `hecate_swim:remove_peer/2'.</li>
%% </ul>
%%
%% State is minimal: three maps.
%% <ul>
%%   <li>`peers :: #{ConnPid => NodeId}' — reverse from disconnect
%%       events to SWIM NodeIds.</li>
%%   <li>`conns :: #{NodeId => ConnPid}' — forward lookup for the
%%       realm `send_fun' dispatcher (see `conn_for/2').</li>
%%   <li>`realms :: #{RealmId => RealmPid}' — populated by
%%       `hecate_station_app' during boot; one entry per supervised
%%       realm process.</li>
%% </ul>
-module(hecate_station_peer_observer).
-behaviour(gen_server).

-export([
    start_link/1, stop/1,
    peers/1,
    conn_for/2,
    register_realm/3,
    unregister_realm/2,
    realm_for/2,
    send_to/3
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{dht := pid(), swim := pid()}.

-record(state, {
    dht    :: pid(),
    swim   :: pid(),
    peers  :: #{pid() => macula_identity:pubkey()},
    conns  :: #{macula_identity:pubkey() => pid()},
    realms :: #{<<_:256>> => pid()}
}).

%%==================================================================
%% API
%%==================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

-spec peers(pid()) -> [{pid(), macula_identity:pubkey()}].
peers(Pid) -> gen_server:call(Pid, peers).

-spec conn_for(pid(), macula_identity:pubkey()) ->
    {ok, pid()} | error.
conn_for(Pid, <<_:256>> = NodeId) ->
    gen_server:call(Pid, {conn_for, NodeId}).

-spec register_realm(pid(), <<_:256>>, pid()) -> ok.
register_realm(Pid, <<_:256>> = RealmId, RealmPid) when is_pid(RealmPid) ->
    gen_server:cast(Pid, {register_realm, RealmId, RealmPid}).

-spec unregister_realm(pid(), <<_:256>>) -> ok.
unregister_realm(Pid, <<_:256>> = RealmId) ->
    gen_server:cast(Pid, {unregister_realm, RealmId}).

-spec realm_for(pid(), <<_:256>>) -> {ok, pid()} | error.
realm_for(Pid, <<_:256>> = RealmId) ->
    gen_server:call(Pid, {realm_for, RealmId}).

%% @doc Wire-level send: resolve NodeId to a conn pid and hand the
%% frame to `macula_peering'. Used by the realm send_fun closures
%% built in `hecate_station_app'. Returns `ok' either way — a missing
%% peer drops silently (the plumtree GRAFT/IHAVE path will recover).
-spec send_to(pid(), macula_identity:pubkey(), macula_frame:frame()) -> ok.
send_to(Observer, <<_:256>> = NodeId, Frame) ->
    deliver(conn_for(Observer, NodeId), Frame).

deliver({ok, ConnPid}, Frame) ->
    macula_peering:send_frame(ConnPid, Frame);
deliver(error, _Frame) ->
    ok.

%%==================================================================
%% gen_server
%%==================================================================

init(#{dht := Dht, swim := Swim}) when is_pid(Dht), is_pid(Swim) ->
    {ok, #state{dht = Dht, swim = Swim,
                peers = #{}, conns = #{}, realms = #{}}}.

handle_call(peers, _From, #state{peers = P} = S) ->
    {reply, maps:to_list(P), S};
handle_call({conn_for, NodeId}, _From, #state{conns = C} = S) ->
    {reply, resolve(maps:find(NodeId, C)), S};
handle_call({realm_for, RealmId}, _From, #state{realms = R} = S) ->
    {reply, resolve(maps:find(RealmId, R)), S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

resolve({ok, Pid}) -> {ok, Pid};
resolve(error)     -> error.

handle_cast({register_realm, RealmId, RealmPid}, #state{realms = R} = S) ->
    {noreply, S#state{realms = R#{RealmId => RealmPid}}};
handle_cast({unregister_realm, RealmId}, #state{realms = R} = S) ->
    {noreply, S#state{realms = maps:remove(RealmId, R)}};
handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({macula_peering, connected, ConnPid, PeerNodeId}, S) ->
    {noreply, on_connected(ConnPid, PeerNodeId, S)};
handle_info({macula_peering, frame, ConnPid, Frame}, S) ->
    on_frame(ConnPid, Frame, S),
    {noreply, S};
handle_info({macula_peering, disconnected, ConnPid, _Reason}, S) ->
    {noreply, on_disconnected(ConnPid, S)};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%==================================================================
%% connected
%%==================================================================

on_connected(ConnPid, NodeId, #state{dht = Dht, swim = Swim,
                                     peers = P, conns = C} = S) ->
    _ = hecate_dht:observe(Dht, direct_peer_spec(NodeId)),
    ok = hecate_swim:add_peer(Swim, NodeId, ConnPid),
    S#state{peers = P#{ConnPid => NodeId},
            conns = C#{NodeId => ConnPid}}.

direct_peer_spec(NodeId) ->
    #{node_id   => NodeId,
      endpoints => [],
      asn       => 0,
      country   => <<"??">>,
      tier      => t0}.

%%==================================================================
%% frame
%%==================================================================

on_frame(ConnPid, Frame, #state{peers = P} = S) ->
    route(Frame, frame_source(ConnPid, P), S).

frame_source(ConnPid, P) ->
    maps:get(ConnPid, P, undefined).

route(_Frame, undefined, _S) ->
    ok;
route(Frame, NodeId, S) ->
    dispatch(frame_category(Frame), Frame, NodeId, S).

frame_category(Frame) -> classify(macula_frame:frame_type(Frame)).

classify(swim_ping)               -> swim;
classify(swim_ack)                -> swim;
classify(swim_suspect)            -> swim;
classify(swim_confirm)            -> swim;
classify(hyparview_join)          -> overlay;
classify(hyparview_forward_join)  -> overlay;
classify(hyparview_neighbor)      -> overlay;
classify(hyparview_disconnect)    -> overlay;
classify(hyparview_shuffle)       -> overlay;
classify(hyparview_shuffle_reply) -> overlay;
classify(plumtree_gossip)         -> overlay;
classify(plumtree_ihave)          -> overlay;
classify(plumtree_graft)          -> overlay;
classify(plumtree_prune)          -> overlay;
classify(_)                       -> other.

dispatch(swim, Frame, NodeId, #state{swim = Swim}) ->
    deliver_swim(macula_frame:verify(Frame, NodeId), Frame, NodeId, Swim);
dispatch(overlay, Frame, NodeId, #state{realms = R}) ->
    deliver_realm(maps:find(realm_of(Frame), R), Frame, NodeId);
dispatch(other, _Frame, _NodeId, _S) ->
    ok.

deliver_swim({ok, _}, Frame, NodeId, Swim) ->
    ok = hecate_swim:handle_frame(Swim, NodeId, Frame);
deliver_swim({error, _Reason}, _Frame, _NodeId, _Swim) ->
    ok.

deliver_realm({ok, RealmPid}, Frame, NodeId) ->
    ok = hecate_station_realm:handle_frame(RealmPid, NodeId, Frame);
deliver_realm(error, _Frame, _NodeId) ->
    ok.

realm_of(Frame) ->
    maps:get(realm, Frame, undefined).

%%==================================================================
%% disconnected
%%==================================================================

on_disconnected(ConnPid, #state{swim = Swim, peers = P, conns = C} = S) ->
    NodeId = maps:get(ConnPid, P, undefined),
    maybe_remove(NodeId, Swim),
    S#state{peers = maps:remove(ConnPid, P),
            conns = drop_conn(NodeId, C)}.

maybe_remove(undefined, _Swim) -> ok;
maybe_remove(NodeId,     Swim) -> hecate_swim:remove_peer(Swim, NodeId).

drop_conn(undefined, C) -> C;
drop_conn(NodeId,    C) -> maps:remove(NodeId, C).
