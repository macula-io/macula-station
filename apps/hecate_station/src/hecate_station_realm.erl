%% @doc Per-realm overlay process.
%%
%% Owns — for one `RealmId' — the station's:
%%
%% <ul>
%%   <li>HyParView view (`hecate_overlay_view'), holding the active +
%%       passive partial views of realm members.</li>
%%   <li>Plumtree state (`hecate_plumtree') for push-lazy gossip
%%       across the active-view edges.</li>
%%   <li>Pubsub state (`hecate_pubsub') for realm-scoped
%%       SUBSCRIBE / EVENT fan-out.</li>
%% </ul>
%%
%% The underlying modules are pure; this process wraps them with a
%% gen_server and a `send_fun' callback that turns
%% `{send, NodeId, Frame}' actions (emitted by the overlay protocol
%% and plumtree) into wire-level sends. In production the send_fun
%% resolves `NodeId' to a live peering conn pid via
%% `hecate_station_peer_observer' and calls
%% `macula_peering:send_frame/2'; in tests it is a capture closure
%% so unit tests can assert the outgoing action stream without any
%% network.
%%
%% One process per realm. Multiple realms run side-by-side under
%% `hecate_station_realm_sup' (simple_one_for_one). Crashing one
%% realm gen_server does not affect the others (PLAN_STATION_INTEGRATION
%% §8.4 acceptance).
-module(hecate_station_realm).
-behaviour(gen_server).

-include("hecate_station_cfg.hrl").

-export([
    start_link/1,
    stop/1,
    realm_id/1,
    add_peer/2,
    remove_peer/2,
    handle_frame/3,
    publish/3,
    active_peers/1,
    is_active/2,
    counts/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, send_fun/0]).

-type send_fun() :: fun((macula_identity:pubkey(), macula_frame:frame()) -> ok).

-type opts() :: #{
    realm_cfg  := hecate_station_config:realm_cfg(),
    identity   := macula_identity:key_pair(),
    send_fun   := send_fun(),
    %% Optional — where deliveries + membership changes are reported.
    notify     => pid()
}.

-record(state, {
    realm_id  :: <<_:256>>,
    identity  :: macula_identity:key_pair(),
    view      :: hecate_overlay_view:view(),
    plumtree  :: hecate_plumtree:state(),
    pubsub    :: hecate_pubsub:state(),
    send_fun  :: send_fun(),
    notify    :: pid() | undefined
}).

%%==================================================================
%% API
%%==================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

-spec realm_id(pid()) -> <<_:256>>.
realm_id(Pid) -> gen_server:call(Pid, realm_id).

%% @doc Admit a realm member into the active view + Plumtree eager set.
-spec add_peer(pid(), macula_identity:pubkey()) -> ok.
add_peer(Pid, <<_:256>> = NodeId) ->
    gen_server:cast(Pid, {add_peer, NodeId}).

-spec remove_peer(pid(), macula_identity:pubkey()) -> ok.
remove_peer(Pid, <<_:256>> = NodeId) ->
    gen_server:cast(Pid, {remove_peer, NodeId}).

%% @doc Dispatch an inbound overlay / plumtree / pubsub frame.
-spec handle_frame(pid(), macula_identity:pubkey(), macula_frame:frame()) -> ok.
handle_frame(Pid, <<_:256>> = From, Frame) when is_map(Frame) ->
    gen_server:cast(Pid, {frame, From, Frame}).

%% @doc Local publish via Plumtree. Returns the message id the caller
%% can later use to track in-flight gossip if desired.
-spec publish(pid(), <<_:128>>, term()) -> ok.
publish(Pid, <<_:128>> = MsgId, Payload) ->
    gen_server:cast(Pid, {publish, MsgId, Payload}).

-spec active_peers(pid()) -> [macula_identity:pubkey()].
active_peers(Pid) -> gen_server:call(Pid, active_peers).

-spec is_active(pid(), macula_identity:pubkey()) -> boolean().
is_active(Pid, <<_:256>> = NodeId) ->
    gen_server:call(Pid, {is_active, NodeId}).

-spec counts(pid()) -> hecate_overlay_view:counts().
counts(Pid) -> gen_server:call(Pid, counts).

%%==================================================================
%% gen_server
%%==================================================================

init(#{realm_cfg := RealmCfg,
       identity  := Identity,
       send_fun  := SendFun} = Opts) ->
    {ok, build_state(RealmCfg, Identity, SendFun, maps:get(notify, Opts, undefined))}.

build_state(#realm_cfg{realm_id = R,
                       active_view_size = ACap,
                       passive_view_size = PCap}, Identity, SendFun, Notify) ->
    Self = macula_identity:public(Identity),
    View = hecate_overlay_view:new(Self, #{active_cap   => ACap,
                                           passive_cap  => PCap}),
    #state{
        realm_id  = R,
        identity  = Identity,
        view      = View,
        plumtree  = hecate_plumtree:new(Identity, R),
        pubsub    = hecate_pubsub:new(R),
        send_fun  = SendFun,
        notify    = Notify
    }.

handle_call(realm_id, _From, #state{realm_id = R} = S) ->
    {reply, R, S};
handle_call(active_peers, _From, #state{view = V} = S) ->
    {reply, hecate_overlay_view:active(V), S};
handle_call({is_active, NodeId}, _From, #state{view = V} = S) ->
    {reply, hecate_overlay_view:is_active(NodeId, V), S};
handle_call(counts, _From, #state{view = V} = S) ->
    {reply, hecate_overlay_view:counts(V), S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast({add_peer, NodeId}, S) ->
    {noreply, on_add_peer(NodeId, S)};
handle_cast({remove_peer, NodeId}, S) ->
    {noreply, on_remove_peer(NodeId, S)};
handle_cast({frame, From, Frame}, S) ->
    {noreply, on_frame(From, Frame, S)};
handle_cast({publish, MsgId, Payload}, S) ->
    {noreply, on_publish(MsgId, Payload, S)};
handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(_Msg, S) -> {noreply, S}.

terminate(_Reason, _S) -> ok.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%==================================================================
%% Membership
%%==================================================================

on_add_peer(NodeId, #state{view = V, plumtree = P} = S) ->
    NewV = hecate_overlay_view:add_active(V, NodeId),
    NewP = hecate_plumtree:add_peer(P, NodeId),
    notify(S, {realm, member_added, NodeId}),
    S#state{view = NewV, plumtree = NewP}.

on_remove_peer(NodeId, #state{view = V, plumtree = P} = S) ->
    NewV = hecate_overlay_view:remove_active(V, NodeId),
    NewP = hecate_plumtree:remove_peer(P, NodeId),
    notify(S, {realm, member_removed, NodeId}),
    S#state{view = NewV, plumtree = NewP}.

%%==================================================================
%% Frame dispatch
%%==================================================================

on_frame(From, Frame, S) ->
    route(frame_category(Frame), From, Frame, S).

frame_category(Frame) -> classify(macula_frame:frame_type(Frame)).

classify(hyparview_join)          -> hyparview;
classify(hyparview_forward_join)  -> hyparview;
classify(hyparview_neighbor)      -> hyparview;
classify(hyparview_disconnect)    -> hyparview;
classify(hyparview_shuffle)       -> hyparview;
classify(hyparview_shuffle_reply) -> hyparview;
classify(plumtree_gossip)         -> plumtree;
classify(plumtree_ihave)          -> plumtree;
classify(plumtree_graft)          -> plumtree;
classify(plumtree_prune)          -> plumtree;
classify(_)                       -> other.

route(hyparview, From, Frame, S) ->
    on_hyparview(From, Frame, S);
route(plumtree, From, Frame, S) ->
    on_plumtree(From, Frame, S);
route(other, _From, _Frame, S) ->
    S.

on_hyparview(From, Frame, #state{view = V} = S) ->
    Ctx = overlay_ctx(S),
    {NewV, Actions} = hecate_overlay_proto:process(V, From, Frame, Ctx),
    NewP            = sync_plumtree_membership(V, NewV, S#state.plumtree),
    dispatch_actions(Actions, S),
    S#state{view = NewV, plumtree = NewP}.

%% When HyParView promotes a peer, Plumtree's eager set must match.
sync_plumtree_membership(OldView, NewView, Plumtree) ->
    Before = sets:from_list(hecate_overlay_view:active(OldView)),
    After  = sets:from_list(hecate_overlay_view:active(NewView)),
    P1 = sets:fold(fun(P, Acc) -> hecate_plumtree:add_peer(Acc, P) end,
                   Plumtree, sets:subtract(After, Before)),
    sets:fold(fun(P, Acc) -> hecate_plumtree:remove_peer(Acc, P) end,
              P1, sets:subtract(Before, After)).

on_plumtree(From, Frame, #state{plumtree = P} = S) ->
    {NewP, Actions, Deliveries} = hecate_plumtree:process(P, From, Frame),
    dispatch_actions(Actions, S),
    deliver_many(Deliveries, S),
    S#state{plumtree = NewP}.

overlay_ctx(#state{realm_id = R, identity = Id, view = V}) ->
    Self = hecate_overlay_view:self_id(V),
    #{self_id => Self, realm => R, identity => Id}.

%%==================================================================
%% Local publish
%%==================================================================

on_publish(MsgId, Payload, #state{plumtree = P} = S) ->
    {NewP, Actions, Deliveries} = hecate_plumtree:publish(P, MsgId, Payload),
    dispatch_actions(Actions, S),
    deliver_many(Deliveries, S),
    S#state{plumtree = NewP}.

%%==================================================================
%% Side-effects — send + notify
%%==================================================================

dispatch_actions([], _S) -> ok;
dispatch_actions([{send, Peer, Frame} | Rest], #state{send_fun = F} = S) ->
    _ = F(Peer, Frame),
    dispatch_actions(Rest, S).

deliver_many([], _S) -> ok;
deliver_many([{MsgId, Payload} | Rest], S) ->
    notify(S, {realm, delivery, MsgId, Payload}),
    deliver_many(Rest, S).

notify(#state{notify = undefined}, _Msg) -> ok;
notify(#state{notify = Pid}, Msg) when is_pid(Pid) ->
    Pid ! {hecate_station_realm, Msg},
    ok.
