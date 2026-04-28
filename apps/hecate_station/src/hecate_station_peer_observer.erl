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
    dht                  :: pid(),
    swim                 :: pid(),
    %% Per-identity RPC procedure registry (Track 2 — multi-identity
    %% mesh discovery). Optional: when undefined, CALL frames are
    %% dropped silently. Identity_sup wires it in when the handler
    %% app is part of the per-identity child set.
    handler_registry     :: pid() | undefined,
    %% Per-identity pubsub registry (Sprint A: SUBSCRIBE / UNSUBSCRIBE
    %% / EVENT frames route through here, keyed by realm tag).
    %% Optional: when undefined, those frames are dropped silently.
    pubsub_registry      :: pid() | undefined,
    %% Per-identity remote-advertise registry. Tracks procedures a
    %% connected peer has registered via ADVERTISE frames. CALL frames
    %% not matched by the local handler_registry fall through to here;
    %% if a match is found the CALL is forwarded over the advertiser's
    %% QUIC connection.
    remote_advertise     :: pid() | undefined,
    %% This station's own identity — RESULT / call_error frames
    %% carry it as the `responded_by' / `reported_by' pubkey so the
    %% caller knows who answered.
    self_id              :: macula_identity:pubkey() | undefined,
    peers                :: #{pid() => macula_identity:pubkey()},
    conns                :: #{macula_identity:pubkey() => pid()},
    %% In-flight forwarded CALLs. Maps each forwarded call_id to the
    %% origin connection that issued the CALL, so the inbound RESULT
    %% / call_error from the advertiser can be relayed back without
    %% disturbing the original caller's gen_server pending map.
    forwarded = #{}      :: #{<<_:128>> => pid()}
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

init(#{dht := Dht, swim := Swim} = Opts)
  when is_pid(Dht), is_pid(Swim) ->
    {ok, #state{dht              = Dht,
                swim             = Swim,
                handler_registry = maps:get(handler_registry, Opts, undefined),
                pubsub_registry  = maps:get(pubsub_registry, Opts, undefined),
                remote_advertise = maps:get(remote_advertise, Opts, undefined),
                self_id          = maps:get(self_id, Opts, undefined),
                peers            = #{},
                conns            = #{}}}.

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
    logger:info("[peer_observer] connected pid=~p peer=~p",
                [ConnPid, PeerNodeId]),
    {noreply, on_connected(ConnPid, PeerNodeId, S)};
handle_info({macula_peering, frame, ConnPid, Frame}, S) ->
    logger:debug("[peer_observer] frame pid=~p type=~p",
                 [ConnPid, macula_frame:frame_type(Frame)]),
    {noreply, on_frame(ConnPid, Frame, S)};
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
    route(Frame, ConnPid, frame_source(ConnPid, P), S).

frame_source(ConnPid, P) ->
    maps:get(ConnPid, P, undefined).

route(_Frame, _ConnPid, undefined, S) ->
    S;
route(Frame, ConnPid, NodeId, S) ->
    dispatch(frame_category(Frame), Frame, ConnPid, NodeId, S).

frame_category(Frame) -> classify(macula_frame:frame_type(Frame)).

classify(swim_ping)    -> swim;
classify(swim_ack)     -> swim;
classify(swim_suspect) -> swim;
classify(swim_confirm) -> swim;
classify(call)         -> call;
classify(result)       -> reply;
classify(error)        -> reply;
classify(advertise)    -> advertise;
classify(unadvertise)  -> advertise;
classify(subscribe)    -> pubsub;
classify(unsubscribe)  -> pubsub;
classify(publish)      -> pubsub;
classify(event)        -> pubsub;
classify(_)            -> other.

dispatch(swim, Frame, _ConnPid, NodeId, #state{swim = Swim} = S) ->
    deliver_swim(macula_frame:verify(Frame, NodeId), Frame, NodeId, Swim),
    S;
dispatch(call, Frame, ConnPid, NodeId, S) ->
    deliver_call(macula_frame:verify(Frame, NodeId), Frame, ConnPid, NodeId, S);
dispatch(reply, Frame, _ConnPid, NodeId, S) ->
    deliver_reply(macula_frame:verify(Frame, NodeId), Frame, S);
dispatch(advertise, Frame, ConnPid, NodeId, S) ->
    deliver_advertise(macula_frame:verify(Frame, NodeId), Frame, ConnPid, NodeId, S),
    S;
dispatch(pubsub, Frame, _ConnPid, NodeId, S) ->
    deliver_pubsub(macula_frame:verify(Frame, NodeId), Frame, NodeId, S),
    S;
dispatch(other, _Frame, _ConnPid, _NodeId, S) ->
    S.

deliver_swim({ok, _}, Frame, NodeId, Swim) ->
    ok = hecate_swim:handle_frame(Swim, NodeId, Frame);
deliver_swim({error, _Reason}, _Frame, _NodeId, _Swim) ->
    ok.

%% CALL dispatch — verify signature, then look the procedure up
%% first against the local per-identity handler_registry (DHT
%% primitives etc), and on miss fall through to the remote-advertise
%% registry (procedures registered by connected peers via the
%% ADVERTISE wire frame). On a remote-advertise hit we forward the
%% CALL across the advertiser's QUIC connection and remember the
%% origin in `forwarded' so the inbound RESULT / call_error can be
%% relayed back. Handler invocation for local procedures runs inside
%% `hecate_handler_dispatch' which traps crashes; the observer
%% process is never blocked by a misbehaving handler.
deliver_call(_Verified, _Frame, _ConnPid, _NodeId,
             #state{handler_registry = undefined,
                    remote_advertise = undefined} = S) ->
    S;
deliver_call({error, _}, _Frame, _ConnPid, _NodeId, S) ->
    S;
deliver_call({ok, Frame}, _OrigFrame, ConnPid, NodeId, S) ->
    on_call_lookup(local_lookup(Frame, S), Frame, ConnPid, NodeId, S).

local_lookup(_Frame, #state{handler_registry = undefined}) ->
    {error, not_found};
local_lookup(Frame, #state{handler_registry = Registry}) ->
    Procedure = maps:get(procedure, Frame),
    hecate_handler_registry:lookup(Registry, Procedure).

on_call_lookup({ok, _Handler}, Frame, _ConnPid, NodeId,
               #state{handler_registry = Registry,
                      self_id = SelfId, conns = C} = S) ->
    Reply = hecate_handler_dispatch:dispatch_call(Frame, Registry, SelfId),
    send_reply_to(maps:find(NodeId, C), Reply),
    S;
on_call_lookup({error, not_found}, Frame, ConnPid, NodeId, S) ->
    on_remote_lookup(remote_lookup(Frame, S), Frame, ConnPid, NodeId, S).

remote_lookup(_Frame, #state{remote_advertise = undefined}) ->
    {error, not_found};
remote_lookup(Frame, #state{remote_advertise = R}) ->
    Realm     = maps:get(realm,     Frame),
    Procedure = maps:get(procedure, Frame),
    hecate_remote_advertise_registry:lookup(R, Realm, Procedure).

%% Remote-advertise miss — synthesize a signed `unknown_next_peer'
%% reply to the caller (origin connection) so they fail fast.
on_remote_lookup({error, not_found}, Frame, _ConnPid, NodeId,
                 #state{self_id = SelfId, conns = C} = S) ->
    Reply = unknown_next_peer_reply(Frame, SelfId),
    send_reply_to(maps:find(NodeId, C), Reply),
    S;
on_remote_lookup({ok, #{conn_pid := AdvertiserConn}}, Frame, _ConnPid, NodeId,
                 #state{conns = C, forwarded = F} = S) ->
    %% Forward the CALL frame as-is over the advertiser's connection.
    %% The advertiser's `macula_station_link' will dispatch it to its
    %% local handler and emit a RESULT or call_error back. Track the
    %% origin so we can route the reply.
    macula_peering:send_frame(AdvertiserConn, Frame),
    Origin = origin_for_reply(maps:find(NodeId, C)),
    track_forwarded(maps:get(call_id, Frame), Origin, F, S).

origin_for_reply({ok, ConnPid}) -> ConnPid;
origin_for_reply(error)         -> undefined.

track_forwarded(_CallId, undefined, _F, S) ->
    %% Origin already gone; the eventual RESULT will be dropped on
    %% arrival because no `forwarded' entry exists. Safe.
    S;
track_forwarded(CallId, Origin, F, S) ->
    S#state{forwarded = F#{CallId => Origin}}.

unknown_next_peer_reply(#{call_id := CallId}, SelfId) ->
    macula_frame:call_error(#{call_id     => CallId,
                              code        => 16#01,
                              reported_by => SelfId}).

%% RESULT / call_error from an advertiser destined for a previously-
%% forwarded CALL. Match by call_id; relay back to the origin.
deliver_reply({error, _}, _Frame, S) ->
    S;
deliver_reply({ok, Frame}, _OrigFrame, S) ->
    relay_forwarded_reply(maps:take(maps:get(call_id, Frame, <<>>),
                                    S#state.forwarded), Frame, S).

relay_forwarded_reply(error, _Frame, S) ->
    %% Not a forwarded reply (e.g. station-internal callers like
    %% relay_ping carry their own state_link gen_server pending map).
    %% Pass through.
    S;
relay_forwarded_reply({Origin, NewF}, Frame, S) ->
    macula_peering:send_frame(Origin, Frame),
    S#state{forwarded = NewF}.

%% ADVERTISE / UNADVERTISE — register or drop a remote procedure
%% routing entry. Frames are signed by the advertiser; verification
%% already happened in `dispatch'. The advertiser pubkey on the
%% wire MUST equal the connection-bound NodeId so a peer cannot
%% advertise on behalf of another identity.
deliver_advertise({error, _}, _Frame, _ConnPid, _NodeId, _S) ->
    ok;
deliver_advertise({ok, Frame}, _OrigFrame, ConnPid, NodeId,
                  #state{remote_advertise = R}) ->
    on_advertise_frame(R, macula_frame:frame_type(Frame), Frame,
                       ConnPid, NodeId).

on_advertise_frame(undefined, _Type, _Frame, _ConnPid, _NodeId) ->
    ok;
on_advertise_frame(R, advertise, Frame, ConnPid, NodeId) ->
    Realm     = maps:get(realm,      Frame),
    Procedure = maps:get(procedure,  Frame),
    Adv       = maps:get(advertiser, Frame),
    %% Reject mismatched advertiser to keep the per-conn invariant.
    on_advertise_match(Adv =:= NodeId, R, Realm, Procedure, Adv, ConnPid);
on_advertise_frame(R, unadvertise, Frame, _ConnPid, NodeId) ->
    Realm     = maps:get(realm,      Frame),
    Procedure = maps:get(procedure,  Frame),
    Adv       = maps:get(advertiser, Frame),
    on_unadvertise_match(Adv =:= NodeId, R, Realm, Procedure).

on_advertise_match(false, _R, _Realm, _Proc, _Adv, _ConnPid) ->
    ok;
on_advertise_match(true, R, Realm, Proc, Adv, ConnPid) ->
    hecate_remote_advertise_registry:register(
      R, Realm, Proc, #{advertiser => Adv, conn_pid => ConnPid}).

on_unadvertise_match(false, _R, _Realm, _Proc) ->
    ok;
on_unadvertise_match(true, R, Realm, Proc) ->
    hecate_remote_advertise_registry:unregister(R, Realm, Proc).

send_reply_to(error, _Reply) ->
    %% Caller's connection is gone — RESULT goes nowhere. The
    %% connection cleanup already removed it from `conns'; nothing
    %% to do.
    ok;
send_reply_to({ok, ConnPid}, Reply) ->
    macula_peering:send_frame(ConnPid, Reply).

%% PubSub dispatch — verify signature, route SUBSCRIBE / UNSUBSCRIBE
%% / EVENT into the per-identity registry (which keys by realm tag
%% and forwards to the matching `hecate_pubsub_server'). The registry
%% creates a new server on first use, so SUBSCRIBE on an unseen realm
%% works without explicit pre-registration.
deliver_pubsub(_Verified, _Frame, _NodeId,
               #state{pubsub_registry = undefined}) ->
    logger:warning("[peer_observer] pubsub frame dropped — no registry"),
    ok;
deliver_pubsub({error, Reason}, _Frame, _NodeId, _S) ->
    logger:warning("[peer_observer] pubsub frame verify failed: ~p", [Reason]),
    ok;
deliver_pubsub({ok, Verified}, _Frame, NodeId, S) ->
    Realm = maps:get(realm, Verified),
    Topic = maps:get(topic, Verified, undefined),
    Type  = macula_frame:frame_type(Verified),
    logger:debug(
      "[peer_observer] pubsub ~s realm=~p topic=~s",
      [Type, Realm, Topic]),
    deliver_pubsub_typed(Type, Realm, NodeId, Verified, S).

deliver_pubsub_typed(publish, Realm, _NodeId, Verified,
                     #state{pubsub_registry = Reg, conns = Conns}) ->
    %% Inbound PUBLISH from a remote daemon. Build the EVENT frame
    %% in the realm's pubsub_server, fan out to each matched local
    %% subscriber's peering connection. Phase 1: single-station
    %% fan-out only; cross-station gossip is a Plan C.2 deliverable.
    on_relay_publish(
      hecate_pubsub_registry:relay_publish(Reg, Realm, Verified),
      Conns);
deliver_pubsub_typed(_Type, Realm, NodeId, Verified,
                     #state{pubsub_registry = Reg}) ->
    _ = hecate_pubsub_registry:dispatch_frame(Reg, Realm, NodeId, Verified),
    ok.

on_relay_publish({ok, EventFrame, Matched}, Conns) ->
    fan_out_event(EventFrame, Matched, Conns);
on_relay_publish({error, _Reason}, _Conns) ->
    ok.

fan_out_event(_EventFrame, [], _Conns) ->
    ok;
fan_out_event(EventFrame, [Sub | Rest], Conns) ->
    send_event_to_sub(maps:find(Sub, Conns), EventFrame),
    fan_out_event(EventFrame, Rest, Conns).

send_event_to_sub({ok, ConnPid}, EventFrame) ->
    macula_peering:send_frame(ConnPid, EventFrame);
send_event_to_sub(error, _EventFrame) ->
    %% Subscriber's connection already gone — drop the EVENT.
    %% The conns map cleanup happens in `on_disconnected'.
    ok.

%%==================================================================
%% disconnected
%%==================================================================

on_disconnected(ConnPid, #state{swim = Swim, peers = P, conns = C,
                                forwarded = F,
                                remote_advertise = R} = S) ->
    NodeId = maps:get(ConnPid, P, undefined),
    maybe_remove(NodeId, Swim),
    maybe_purge_advertise(R, ConnPid),
    S#state{peers     = maps:remove(ConnPid, P),
            conns     = drop_conn(NodeId, C),
            forwarded = drop_forwarded_for(ConnPid, F)}.

%% Drop in-flight forwarded entries whose origin has just gone. The
%% advertiser's eventual reply will be relayed but the origin is
%% gone so there's nowhere to send it; clearing now avoids growing
%% a stale map.
drop_forwarded_for(ConnPid, F) ->
    maps:filter(fun(_, Origin) -> Origin =/= ConnPid end, F).

maybe_purge_advertise(undefined, _ConnPid) ->
    ok;
maybe_purge_advertise(R, ConnPid) ->
    hecate_remote_advertise_registry:purge_conn(R, ConnPid).

maybe_remove(undefined, _Swim) -> ok;
maybe_remove(NodeId,     Swim) -> hecate_swim:remove_peer(Swim, NodeId).

drop_conn(undefined, C) -> C;
drop_conn(NodeId,    C) -> maps:remove(NodeId, C).
