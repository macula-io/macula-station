%% @doc Peer observer — glue between peering, DHT, and SWIM.
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
%%       verifies the signature and dispatches SWIM frames
%%       (ping / ack / suspect / confirm) to
%%       `hecate_swim:handle_frame/3'. Application-layer frames
%%       (HyParView / Plumtree / pubsub / realm admission) are
%%       <em>not</em> a station concern — the station is
%%       realm-agnostic infrastructure per the railroad mental
%%       model; realm identity + overlay live in a separate
%%       `hecate-realm' / `macula-realm' service that dials this
%%       station like any other peer. Unknown frame types are
%%       dropped silently and flow end-to-end between the peering
%%       workers of the realm service and its clients.</li>
%%   <li>`{macula_peering, disconnected, ConnPid, _Reason}' →
%%       `hecate_swim:remove_peer/2'.</li>
%% </ul>
%%
%% State is two maps — `peers' for the ConnPid → NodeId reverse
%% lookup used by disconnect handling, and `conns' for the forward
%% NodeId → ConnPid lookup exposed via `conn_for/2' so callers
%% (e.g. the future realm service when co-resident with a station)
%% can send frames to a known peer without re-discovering it.
-module(hecate_station_peer_observer).
-behaviour(gen_server).

-export([
    start_link/1, stop/1,
    peers/1,
    conn_for/2
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{dht := pid(), swim := pid()}.

-record(state, {
    dht    :: pid(),
    swim   :: pid(),
    peers  :: #{pid() => macula_identity:pubkey()},
    conns  :: #{macula_identity:pubkey() => pid()}
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

%%==================================================================
%% gen_server
%%==================================================================

init(#{dht := Dht, swim := Swim}) when is_pid(Dht), is_pid(Swim) ->
    {ok, #state{dht = Dht, swim = Swim,
                peers = #{}, conns = #{}}}.

handle_call(peers, _From, #state{peers = P} = S) ->
    {reply, maps:to_list(P), S};
handle_call({conn_for, NodeId}, _From, #state{conns = C} = S) ->
    {reply, resolve(maps:find(NodeId, C)), S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

resolve({ok, Pid}) -> {ok, Pid};
resolve(error)     -> error.

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
%% frame — station cares about SWIM only; everything else is
%% realm-/application-level and flows end-to-end between the
%% peering workers of the relevant service.
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

classify(swim_ping)    -> swim;
classify(swim_ack)     -> swim;
classify(swim_suspect) -> swim;
classify(swim_confirm) -> swim;
classify(_)            -> other.

dispatch(swim, Frame, NodeId, #state{swim = Swim}) ->
    deliver_swim(macula_frame:verify(Frame, NodeId), Frame, NodeId, Swim);
dispatch(other, _Frame, _NodeId, _S) ->
    ok.

deliver_swim({ok, _}, Frame, NodeId, Swim) ->
    ok = hecate_swim:handle_frame(Swim, NodeId, Frame);
deliver_swim({error, _Reason}, _Frame, _NodeId, _Swim) ->
    ok.

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
