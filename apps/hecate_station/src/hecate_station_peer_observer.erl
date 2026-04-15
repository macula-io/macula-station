%% @doc Peer observer — glue between peering, DHT, and SWIM.
%%
%% Each station has exactly one observer (when booted via the
%% supervisor). It is the `controlling_pid' for every
%% `macula_peering_conn' worker the listener accepts and for every
%% outbound connect initiated via `hecate_station:connect_to/1'. It
%% turns the three peering events into the mutations the rest of the
%% station expects:
%%
%% <ul>
%%   <li>`{macula_peering, connected, ConnPid, PeerNodeId}' →
%%       `hecate_dht:observe/2' with an entry spec carrying
%%       `tier = t0' (unverified direct peer; a full
%%       handshake-and-record path lands in 8.3.x with geo-check)
%%       and `hecate_swim:add_peer/3' so the failure detector starts
%%       probing the new member.</li>
%%   <li>`{macula_peering, frame, ConnPid, Frame}' → verifies the
%%       frame signature against the known peer NodeId and routes
%%       SWIM frames (ping / ack / suspect / confirm) to
%%       `hecate_swim:handle_frame/3'. DHT-level frames (PING /
%%       FIND_NODE / STORE / FIND_VALUE) go to
%%       `hecate_dht:handle_frame/3'. Unknown or unsigned frames
%%       are dropped silently.</li>
%%   <li>`{macula_peering, disconnected, ConnPid, _Reason}' →
%%       `hecate_swim:remove_peer/2'. The DHT entry is kept (it
%%       will age out via the Phase 3 refresh / republish paths);
%%       the cached `conn_pid' held inside SWIM is purged so the
%%       next probe round picks a different target.</li>
%% </ul>
%%
%% Observer state is intentionally minimal: a `ConnPid → NodeId' map
%% so `disconnected' events (which carry only the ConnPid) can be
%% mapped back to the SWIM NodeId without round-tripping through
%% the DHT.
-module(hecate_station_peer_observer).
-behaviour(gen_server).

-export([start_link/1, stop/1, peers/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{dht := pid(), swim := pid()}.

-record(state, {
    dht    :: pid(),
    swim   :: pid(),
    peers  :: #{pid() => macula_identity:pubkey()}
}).

%%==================================================================
%% API
%%==================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

-spec peers(pid()) -> [{pid(), macula_identity:pubkey()}].
peers(Pid) ->
    gen_server:call(Pid, peers).

%%==================================================================
%% gen_server
%%==================================================================

init(#{dht := Dht, swim := Swim}) when is_pid(Dht), is_pid(Swim) ->
    {ok, #state{dht = Dht, swim = Swim, peers = #{}}}.

handle_call(peers, _From, #state{peers = P} = S) ->
    {reply, maps:to_list(P), S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

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

on_connected(ConnPid, NodeId, #state{dht = Dht, swim = Swim, peers = P} = S) ->
    _ = hecate_dht:observe(Dht, direct_peer_spec(NodeId)),
    ok = hecate_swim:add_peer(Swim, NodeId, ConnPid),
    S#state{peers = P#{ConnPid => NodeId}}.

%% Direct-peer observe spec (tier=t0). 8.3.x adds ASN / country via a
%% local geo_check lookup; the defaults here match
%% `hecate_station_bootstrap:to_entry_spec/1'.
direct_peer_spec(NodeId) ->
    #{
        node_id   => NodeId,
        endpoints => [],
        asn       => 0,
        country   => <<"??">>,
        tier      => t0
    }.

%%==================================================================
%% frame
%%==================================================================

on_frame(ConnPid, Frame, #state{peers = P} = S) ->
    route(Frame, frame_source(ConnPid, P), S).

frame_source(ConnPid, P) ->
    maps:get(ConnPid, P, undefined).

route(_Frame, undefined, _S) ->
    %% Frame from an unknown / pre-handshake connection — drop.
    ok;
route(Frame, NodeId, S) ->
    dispatch(frame_category(Frame), Frame, NodeId, S).

frame_category(Frame) ->
    classify(macula_frame:frame_type(Frame)).

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
    %% Bad signature — drop. We trust the Phase 2 detector to age out
    %% the sender if it stops producing valid frames.
    ok.

%%==================================================================
%% disconnected
%%==================================================================

on_disconnected(ConnPid, #state{swim = Swim, peers = P} = S) ->
    maybe_remove(maps:get(ConnPid, P, undefined), Swim),
    S#state{peers = maps:remove(ConnPid, P)}.

maybe_remove(undefined, _Swim) -> ok;
maybe_remove(NodeId,     Swim) -> hecate_swim:remove_peer(Swim, NodeId).
