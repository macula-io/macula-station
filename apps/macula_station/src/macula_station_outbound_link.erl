%% @doc Outbound peer-link worker — one per configured outbound peer URL.
%%
%% Drives a single `macula_peering:connect/1' worker, captures the
%% peer's verified `node_id' once CONNECT/HELLO completes, registers
%% itself in `macula_station_peer_links', and reconnects on
%% disconnect with exponential backoff.
%%
%% Used by the seed-dial bootstrap tier: each link's `(host, port,
%% node_id)' tuple becomes a `verified_peer()' for the cascade once
%% the link reaches the connected state.
%%
%% Also implements the SDK station-link API surface
%% (`subscribe/4', `unsubscribe/2', `publish/4', `call/5',
%% `is_connected/1') so consumers in macula-station that hold an
%% outbound-link pid (via `macula_station_peer_links:connections/0')
%% can drive pubsub + RPC exactly like an SDK
%% `macula_station_link' client. Frames inbound on this worker's
%% peering connection are split: pubsub `event' frames matching a
%% local subscription are fanned out to the subscriber's mailbox as
%% `{macula_event, SubRef, Topic, Payload, Meta}' (the SDK's
%% delivery shape); `result' / `error' frames matching an
%% in-flight call complete the pending `gen_server:call/3'; every
%% other frame is still forwarded to `macula_station_peer_observer'
%% so the central dispatcher continues to see SWIM / DHT /
%% advertise / publish / call frames from outbound dials.
-module(macula_station_outbound_link).
-behaviour(gen_server).

-export([start_link/1, stop/1, peer_node_id/1, conn_pid/1]).
-export([subscribe/4, unsubscribe/2, publish/4, call/5, is_connected/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{
    url             := binary(),
    identity        := macula_identity:key_pair(),
    capabilities    => non_neg_integer()
}.

%% Must stay >= 2: schedule_reconnect jitters the delay with `Backoff div 2`, and
%% rand:uniform(0) is a badarg. The floor is 1_000, so this is only a compile-time
%% coupling for a future editor, not a runtime guard.
-define(INITIAL_BACKOFF_MS,  1_000).
-define(MAX_BACKOFF_MS,     60_000).
-define(HANDSHAKE_TIMEOUT_MS, 30_000).

%% App-level silence-detection: every healthy outbound peering should
%% see _mesh.bloom traffic from the peer at least every 30s (the
%% bloom_exchange periodic rebuild cadence). If we go this long
%% without ANY inbound frame, the peering connection is half-open —
%% QUIC keep_alive PINGs ACK at the transport layer (so we don't see
%% `disconnected`) but the peer's application-level peering_conn
%% worker is dead (e.g., killed by the handshake-complete dedup
%% close-cast on the peer side, where the close didn't propagate
%% back to us). Force-close + reconnect.
%%
%% Tuned conservatively: at 30s bloom cadence + 30s slack for normal
%% jitter, 5 minutes of silence is firmly anomalous on a station-to-
%% station link. False-positive risk: a peer briefly dropping all
%% traffic for legitimate reasons (long GC, IO storm) gets reconnected.
%% Acceptable — reconnect is cheap and the alternative is half-open
%% silently breaking cross-station pubsub for hours (the bug this
%% catches).
-define(SILENCE_THRESHOLD_MS, 300_000).
-define(SILENCE_CHECK_MS,      60_000).

%% Subscription bookkeeping mirrors macula_station_link in the SDK:
%% one entry per (Realm, Topic, SubscriberPid) tuple, keyed by an
%% opaque SubRef returned to the caller. A reverse `topic_index'
%% lets inbound EVENT frames fan out by (Realm, Topic) without
%% scanning the full map.
-type subscription() :: {Realm :: <<_:256>>,
                         Topic :: binary(),
                         Subscriber :: pid(),
                         Mon :: reference()}.

-record(state, {
    url             :: binary(),
    host            :: binary(),
    port            :: inet:port_number(),
    identity        :: macula_identity:key_pair(),
    capabilities    :: non_neg_integer(),
    conn_pid        :: pid() | undefined,
    peer_node_id    :: macula_identity:pubkey() | undefined,
    backoff_ms      = ?INITIAL_BACKOFF_MS :: pos_integer(),
    reconnect_timer :: reference() | undefined,
    %% SDK-surface state — populated regardless of conn state. SUBSCRIBE
    %% frames are replayed on every reconnect; PUBLISH / CALL require
    %% a connected handshake (mirrors SDK semantics).
    subscriptions = #{} :: #{reference() => subscription()},
    topic_index   = #{} :: #{{<<_:256>>, binary()} => sets:set(reference())},
    pending       = #{} :: #{<<_:128>> => {gen_server:from(), reference()}},
    publish_seq   = 0   :: non_neg_integer(),
    %% Monotonic ms timestamp of the last inbound peering frame from
    %% this connection. Drives silence-based half-open detection.
    %% `undefined' means no frame received on the current connection
    %% (set on connect, updated on every inbound frame).
    last_inbound_at :: integer() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% @doc Read the peer's verified `node_id' if the handshake has
%% completed. Returns `undefined' while still connecting / reconnecting.
-spec peer_node_id(pid()) -> macula_identity:pubkey() | undefined.
peer_node_id(Pid) ->
    gen_server:call(Pid, peer_node_id, 1_000).

%% @doc Read the underlying `macula_peering_conn' worker pid for this
%% link, if a dial is in progress or established. Returns `undefined'
%% while the link is between connection attempts. Used by the
%% `peer_observer' init reconciliation to derive its conns map from
%% the current outbound state without depending on having received
%% the original `connected' notification.
-spec conn_pid(pid()) -> pid() | undefined.
conn_pid(Pid) ->
    gen_server:call(Pid, conn_pid, 1_000).

%% @doc Subscribe `Subscriber' to `(Realm, Topic)' on this peer.
%%
%% Emits a SUBSCRIBE frame on the wire (when the peering handshake
%% has completed; otherwise replayed on the next `connected'
%% notification). `Subscriber' receives
%% `{macula_event, SubRef, Topic, Payload, Meta}' for every inbound
%% EVENT frame matching the subscription, mirroring the delivery
%% shape of `macula_station_link:subscribe/4'.
-spec subscribe(pid(), <<_:256>>, binary(), pid()) ->
        {ok, reference()} | {error, term()}.
subscribe(Pid, Realm, Topic, Subscriber)
  when is_pid(Pid),
       is_binary(Realm), byte_size(Realm) =:= 32,
       is_binary(Topic), is_pid(Subscriber) ->
    gen_server:call(Pid, {subscribe, Realm, Topic, Subscriber}, 5_000).

%% @doc Drop a subscription previously obtained from `subscribe/4'.
%% Idempotent — unknown SubRefs return `ok'.
-spec unsubscribe(pid(), reference()) -> ok.
unsubscribe(Pid, SubRef) when is_pid(Pid), is_reference(SubRef) ->
    gen_server:call(Pid, {unsubscribe, SubRef}, 5_000).

%% @doc Send a PUBLISH frame to the peer, fire-and-forget.
%%
%% Returns `{error, not_connected}' until the handshake completes —
%% PUBLISH is not queued because the peer would deliver it with the
%% wrong wall-clock and fight downstream dedup.
-spec publish(pid(), <<_:256>>, binary(), term()) ->
        ok | {error, not_connected | term()}.
publish(Pid, Realm, Topic, Payload)
  when is_pid(Pid),
       is_binary(Realm), byte_size(Realm) =:= 32,
       is_binary(Topic) ->
    gen_server:call(Pid, {publish, Realm, Topic, Payload}, 5_000).

%% @doc Issue a CALL frame and block until the peer replies, the
%% deadline elapses, or the connection drops.
-spec call(pid(), <<_:256>>, binary(), term(), pos_integer()) ->
        {ok, term()} | {error, term()}.
call(Pid, Realm, Procedure, Payload, TimeoutMs)
  when is_pid(Pid),
       is_binary(Realm), byte_size(Realm) =:= 32,
       is_binary(Procedure),
       is_integer(TimeoutMs), TimeoutMs > 0 ->
    GenTimeout = TimeoutMs + 500,
    try
        gen_server:call(Pid, {call, Realm, Procedure, Payload, TimeoutMs},
                        GenTimeout)
    catch
        exit:{timeout, _} -> {error, timeout};
        exit:{noproc, _}  -> {error, noproc};
        exit:{normal, _}  -> {error, gone}
    end.

%% @doc `true' once the peering handshake has completed (peer node id
%% verified). Mirrors `macula_station_link:is_connected/1'.
-spec is_connected(pid()) -> boolean().
is_connected(Pid) when is_pid(Pid) ->
    try gen_server:call(Pid, is_connected, 1_000)
    catch _:_ -> false
    end.

%%====================================================================
%% gen_server
%%====================================================================

init(#{url := Url, identity := Kp} = Opts) ->
    process_flag(trap_exit, true),
    {Host, Port} = parse_url(Url),
    State = #state{
        url          = Url,
        host         = Host,
        port         = Port,
        identity     = Kp,
        capabilities = maps:get(capabilities, Opts, 0)
    },
    %% Register in peer_links immediately so the registry can monitor
    %% us; the `node_id' is filled in once the handshake completes.
    ok = macula_station_peer_links:register(Url, self()),
    self() ! dial,
    schedule_silence_check(),
    {ok, State}.

handle_call(peer_node_id, _From, #state{peer_node_id = N} = S) ->
    {reply, N, S};
handle_call(conn_pid, _From, #state{conn_pid = C} = S) ->
    {reply, C, S};

handle_call(is_connected, _From, #state{peer_node_id = undefined} = S) ->
    {reply, false, S};
handle_call(is_connected, _From, S) ->
    {reply, true, S};

handle_call({subscribe, Realm, Topic, Subscriber}, _From,
            #state{subscriptions = Subs, topic_index = Idx} = S) ->
    SubRef  = make_ref(),
    Mon     = erlang:monitor(process, Subscriber),
    NewSubs = Subs#{SubRef => {Realm, Topic, Subscriber, Mon}},
    NewIdx  = add_topic_sub(Realm, Topic, SubRef, Idx),
    %% First subscriber for this (Realm, Topic) on this link: emit a
    %% SUBSCRIBE frame so the peer registers our interest. Subsequent
    %% local SubRefs share the wire subscription (one SUBSCRIBE per
    %% identity per pair, mirroring the SDK).
    case sets:size(maps:get({Realm, Topic}, NewIdx)) of
        1 -> maybe_send_subscribe(Realm, Topic, S);
        _ -> ok
    end,
    {reply, {ok, SubRef},
     S#state{subscriptions = NewSubs, topic_index = NewIdx}};

handle_call({unsubscribe, SubRef}, _From, S) ->
    {reply, ok, on_unsubscribe(SubRef, S)};

handle_call({publish, _Realm, _Topic, _Payload}, _From,
            #state{peer_node_id = undefined} = S) ->
    {reply, {error, not_connected}, S};
handle_call({publish, Realm, Topic, Payload}, _From,
            #state{conn_pid = ConnPid, identity = Id,
                   publish_seq = Seq} = S) ->
    Pub = macula_identity:public(Id),
    Frame = macula_frame:publish(#{
        topic           => Topic,
        realm           => Realm,
        publisher       => Pub,
        seq             => Seq,
        payload         => Payload,
        published_at_ms => erlang:system_time(millisecond)
    }),
    ok = macula_peering:send_frame(ConnPid, Frame),
    {reply, ok, S#state{publish_seq = Seq + 1}};

handle_call({call, _Realm, _Proc, _Payload, _Tmo}, _From,
            #state{peer_node_id = undefined} = S) ->
    {reply, {error, not_connected}, S};
handle_call({call, Realm, Proc, Payload, Tmo}, From,
            #state{conn_pid = ConnPid, identity = Id, pending = P} = S) ->
    CallId     = crypto:strong_rand_bytes(16),
    Caller     = macula_identity:public(Id),
    DeadlineMs = erlang:system_time(millisecond) + Tmo,
    Frame = macula_frame:call(#{
        call_id     => CallId,
        procedure   => Proc,
        realm       => Realm,
        payload     => Payload,
        deadline_ms => DeadlineMs,
        caller      => Caller
    }),
    ok = macula_peering:send_frame(ConnPid, Frame),
    TRef = erlang:send_after(Tmo, self(), {call_timeout, CallId}),
    {noreply, S#state{pending = P#{CallId => {From, TRef}}}};

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(dial, S) ->
    do_dial(S);
handle_info({macula_peering, connected, ConnPid, NodeId},
            #state{conn_pid = ConnPid, url = Url,
                   host = Host, port = Port} = S)
  when is_binary(NodeId) ->
    ok = macula_station_peer_links:set_peer_node_id(Url, NodeId),
    %% Re-tag the connected event with `connected_outbound' so
    %% peer_observer can distinguish this client-side conn from the
    %% inbound (listener-accepted) handshake of the SAME peer that
    %% mutual-peering produces. Without it peer_observer's `conns'
    %% map would race between inbound + outbound for the same NodeId
    %% and EVENT delivery would land on the wrong side of the link.
    %% Carry the DIALLED endpoint with the event rather than letting the
    %% observer look it up in `macula_station_peer_links'. Both are casts, to
    %% two different gen_servers, so a lookup could run before the
    %% `set_peer_node_id' above lands and would then silently observe the peer
    %% with no address — indistinguishable from a peer whose address we
    %% genuinely do not know. This address is the one we actually dialled, so
    %% it is known-good by construction.
    forward_to_observer({macula_peering, connected_outbound, ConnPid, NodeId,
                         [#{host => Host, port => Port, transport => quic}]}),
    %% Successful handshake — reset backoff for the next disconnect
    %% and replay every active SUBSCRIBE so the peer rebuilds its
    %% interest set after the reconnect.
    S1 = S#state{peer_node_id    = NodeId,
                 backoff_ms      = ?INITIAL_BACKOFF_MS,
                 last_inbound_at = now_ms()},
    drain_pending_subscribes(S1),
    {noreply, S1};
handle_info({macula_peering, disconnected, ConnPid, Reason},
            #state{conn_pid = ConnPid} = S) ->
    forward_to_observer({macula_peering, disconnected_outbound, ConnPid, Reason}),
    S1 = fail_all_pending({disconnected, Reason}, S),
    {noreply, schedule_reconnect(reset_conn(S1))};
handle_info({macula_peering, frame, ConnPid, Frame} = Msg,
            #state{conn_pid = ConnPid} = S) ->
    S1 = S#state{last_inbound_at = now_ms()},
    {noreply, on_inbound_frame(Frame, Msg, S1)};
handle_info({call_timeout, CallId}, #state{pending = P} = S) ->
    {noreply, on_call_timeout(maps:take(CallId, P), S)};
handle_info({'DOWN', Mon, process, _Pid, _Reason}, S) ->
    {noreply, on_subscriber_down(Mon, S)};
handle_info({'EXIT', ConnPid, _Reason}, #state{conn_pid = ConnPid} = S) ->
    S1 = fail_all_pending({disconnected, peering_exit}, S),
    {noreply, schedule_reconnect(reset_conn(S1))};
handle_info(silence_check, S) ->
    schedule_silence_check(),
    {noreply, maybe_force_reconnect_on_silence(S)};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, #state{conn_pid = undefined}) -> ok;
terminate(_Reason, #state{conn_pid = Pid}) ->
    catch macula_peering:close(Pid),
    ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Inbound frame routing
%%====================================================================

%% Pubsub `event' frames matching a local subscription get fanned out
%% to subscribers as `{macula_event, SubRef, Topic, Payload, Meta}'.
%% `result' / `error' frames complete in-flight CALLs. Every frame —
%% even ones we handled — is still forwarded to peer_observer so the
%% central dispatcher's view (SWIM / DHT / advertise registry / etc.)
%% remains complete; for `event' / `result' / `error' the observer
%% path is a no-op for our consumers but it keeps logging + diagnostics
%% intact.
on_inbound_frame(Frame, Msg, #state{peer_node_id = PeerNodeId,
                                    conn_pid     = ConnPid} = S) ->
    NewS = case macula_frame:frame_type(Frame) of
        event  -> deliver_event(Frame, S);
        result -> deliver_result(Frame, S);
        error  -> deliver_call_error(Frame, S);
        _Other -> S
    end,
    forward_dispatch(macula_frame:frame_type(Frame), Frame,
                     ConnPid, PeerNodeId, Msg),
    NewS.

%% Route the wire frame to the right downstream observer. Pubsub frames
%% go to the dedicated dispatcher (mirrors macula_peering_conn's
%% pubsub_recipient route — see SDK 4.4.4); everything else still flows
%% to peer_observer so its SWIM / DHT / advertise-registry / logging
%% view stays complete. The local-subscriber delivery in `deliver_event`
%% above already handled SDK-side fan-out for THIS link's subscribers
%% (e.g. peering_router subscribing for cross-station mesh gossip);
%% dispatcher additionally handles per-station pubsub_server fan-out
%% to any daemons connected directly to this station.
forward_dispatch(Type, Frame, ConnPid, NodeId, _Msg)
        when (Type =:= subscribe orelse Type =:= unsubscribe orelse
              Type =:= publish   orelse Type =:= event),
             is_pid(ConnPid), is_binary(NodeId) ->
    forward_pubsub(whereis(macula_station_route_pubsub_frames),
                   ConnPid, NodeId, Frame);
forward_dispatch(_OtherType, _Frame, _ConnPid, _NodeId, Msg) ->
    forward_to_observer(Msg).

forward_pubsub(undefined, _ConnPid, _NodeId, _Frame) ->
    %% Dispatcher not running (boot edge / restart); silently drop
    %% rather than fall back to observer, since the observer's pubsub
    %% paths have already been removed from the hot path in this
    %% station version.
    ok;
forward_pubsub(Pid, ConnPid, NodeId, Frame) when is_pid(Pid) ->
    Pid ! {macula_peering, pubsub_frame, ConnPid, NodeId, Frame},
    ok.

deliver_event(#{realm := Realm, topic := Topic} = Frame,
              #state{topic_index = Idx} = S) ->
    fanout_event(maps:find({Realm, Topic}, Idx), Topic, Frame, S),
    S.

fanout_event(error, _Topic, _Frame, _S) ->
    ok;
fanout_event({ok, Set}, Topic, Frame, S) ->
    Payload = maps:get(payload, Frame, undefined),
    Meta    = event_meta(Frame),
    sets:fold(fun(SubRef, _Acc) ->
        deliver_to_subscriber(SubRef, Topic, Payload, Meta, S)
    end, ok, Set).

deliver_to_subscriber(SubRef, Topic, Payload, Meta,
                      #state{subscriptions = Subs}) ->
    on_sub_lookup(maps:find(SubRef, Subs), SubRef, Topic, Payload, Meta).

on_sub_lookup(error, _SubRef, _Topic, _Payload, _Meta) ->
    ok;
on_sub_lookup({ok, {_R, _T, Subscriber, _Mon}}, SubRef, Topic, Payload, Meta) ->
    Subscriber ! {macula_event, SubRef, Topic, Payload, Meta},
    ok.

%% Carry the wire-level publisher / seq / clock through to the
%% subscriber so cross-link dedup + ordering work the same as under
%% the SDK station_link client.
event_meta(Frame) ->
    Keys = [publisher, seq, published_at_ms, delivered_via],
    lists:foldl(fun(K, Acc) ->
        on_meta_key(maps:find(K, Frame), K, Acc)
    end, #{}, Keys).

on_meta_key(error, _K, Acc)    -> Acc;
on_meta_key({ok, V}, K, Acc)   -> Acc#{K => V}.

deliver_result(#{call_id := CallId} = Frame,
               #state{pending = P} = S) ->
    Payload = maps:get(payload, Frame, undefined),
    complete_pending(maps:take(CallId, P), {ok, Payload}, S).

deliver_call_error(#{call_id := CallId} = Frame,
                   #state{pending = P} = S) ->
    Code    = maps:get(code, Frame, undefined),
    Message = maps:get(message, Frame, undefined),
    complete_pending(maps:take(CallId, P),
                     {error, {call_error, Code, Message}}, S).

complete_pending(error, _Reply, S) ->
    %% Unknown call_id (race with timeout, or duplicate reply). Drop.
    S;
complete_pending({{From, TRef}, NewP}, Reply, S) ->
    _ = erlang:cancel_timer(TRef),
    gen_server:reply(From, Reply),
    S#state{pending = NewP}.

%%====================================================================
%% Subscription helpers
%%====================================================================

on_unsubscribe(SubRef, #state{subscriptions = Subs, topic_index = Idx} = S) ->
    on_sub_remove(maps:take(SubRef, Subs), SubRef, Idx, S).

on_sub_remove(error, _SubRef, _Idx, S) ->
    S;
on_sub_remove({{Realm, Topic, _Sub, Mon}, NewSubs}, SubRef, Idx, S) ->
    erlang:demonitor(Mon, [flush]),
    NewIdx = del_topic_sub(Realm, Topic, SubRef, Idx),
    %% Last local subscriber for this (Realm, Topic) → send UNSUBSCRIBE.
    case maps:is_key({Realm, Topic}, NewIdx) of
        true  -> ok;
        false -> maybe_send_unsubscribe(Realm, Topic, S)
    end,
    S#state{subscriptions = NewSubs, topic_index = NewIdx}.

on_subscriber_down(Mon, #state{subscriptions = Subs} = S) ->
    case find_sub_by_mon(Mon, Subs) of
        none       -> S;
        {ok, Ref}  -> on_unsubscribe(Ref, S)
    end.

find_sub_by_mon(Mon, Subs) ->
    Iter = maps:iterator(Subs),
    find_sub_by_mon_next(maps:next(Iter), Mon).

find_sub_by_mon_next(none, _Mon) ->
    none;
find_sub_by_mon_next({Ref, {_R, _T, _Sub, Mon}, _Iter}, Mon) ->
    {ok, Ref};
find_sub_by_mon_next({_Ref, _Sub, Iter}, Mon) ->
    find_sub_by_mon_next(maps:next(Iter), Mon).

add_topic_sub(Realm, Topic, SubRef, Idx) ->
    Key = {Realm, Topic},
    Set = maps:get(Key, Idx, sets:new()),
    Idx#{Key => sets:add_element(SubRef, Set)}.

del_topic_sub(Realm, Topic, SubRef, Idx) ->
    Key = {Realm, Topic},
    Set = sets:del_element(SubRef, maps:get(Key, Idx, sets:new())),
    on_set_after_del(Key, Set, Idx).

on_set_after_del(Key, Set, Idx) ->
    case sets:size(Set) of
        0 -> maps:remove(Key, Idx);
        _ -> Idx#{Key => Set}
    end.

%%====================================================================
%% Wire frame helpers
%%====================================================================

maybe_send_subscribe(_Realm, _Topic, #state{conn_pid = undefined}) ->
    ok;
maybe_send_subscribe(_Realm, _Topic, #state{peer_node_id = undefined}) ->
    %% Peering worker exists but the HELLO handshake hasn't landed
    %% yet. Replayed by `drain_pending_subscribes/1' on `connected'.
    ok;
maybe_send_subscribe(Realm, Topic,
                     #state{conn_pid = ConnPid, identity = Id}) ->
    SubKey = macula_identity:public(Id),
    Frame = macula_frame:subscribe(#{topic      => Topic,
                                     realm      => Realm,
                                     subscriber => SubKey}),
    catch macula_peering:send_frame(ConnPid, Frame),
    ok.

maybe_send_unsubscribe(_Realm, _Topic, #state{conn_pid = undefined}) ->
    ok;
maybe_send_unsubscribe(_Realm, _Topic, #state{peer_node_id = undefined}) ->
    ok;
maybe_send_unsubscribe(Realm, Topic,
                       #state{conn_pid = ConnPid, identity = Id}) ->
    SubKey = macula_identity:public(Id),
    Frame = macula_frame:unsubscribe(#{topic      => Topic,
                                       realm      => Realm,
                                       subscriber => SubKey}),
    catch macula_peering:send_frame(ConnPid, Frame),
    ok.

%% Re-emit one SUBSCRIBE per unique (Realm, Topic) on every reconnect.
%% Multiple local SubRefs sharing the same wire pair share one
%% subscribe; the peer registers one entry keyed by our pubkey.
drain_pending_subscribes(#state{subscriptions = Subs} = S) ->
    Pairs = lists:usort(
              [{R, T} || {_Ref, {R, T, _Sub, _Mon}} <- maps:to_list(Subs)]),
    [maybe_send_subscribe(R, T, S) || {R, T} <- Pairs],
    ok.

%%====================================================================
%% Pending-call lifecycle
%%====================================================================

on_call_timeout(error, S) ->
    S;
on_call_timeout({{From, _OldTRef}, NewP}, #state{pending = _} = S) ->
    gen_server:reply(From, {error, timeout}),
    S#state{pending = NewP}.

%% Disconnect tears down every in-flight CALL with `{error, Reason}'.
%% Subscriptions are KEPT so they replay on the next handshake — the
%% bloom-exchange / peering-router consumers expect persistence
%% across reconnects, just like the SDK station_link.
fail_all_pending(Reason, #state{pending = P} = S) ->
    maps:foreach(fun(_CallId, {From, TRef}) ->
        _ = erlang:cancel_timer(TRef),
        gen_server:reply(From, {error, Reason})
    end, P),
    S#state{pending = #{}}.

%%====================================================================
%% Internals
%%====================================================================

forward_to_observer(Msg) ->
    case whereis(macula_station_peer_observer) of
        undefined -> ok;
        Pid       -> Pid ! Msg, ok
    end.

do_dial(#state{host = H, port = P, identity = Kp,
               capabilities = Caps} = S) ->
    Target = #{host => H, port => P, timeout_ms => ?HANDSHAKE_TIMEOUT_MS},
    Result = macula_peering:connect(#{
        role            => client,
        identity        => Kp,
        realms          => [],
        capabilities    => Caps,
        controlling_pid => self(),
        target          => Target
    }),
    handle_dial_result(Result, S).

handle_dial_result({ok, ConnPid}, S) ->
    {noreply, S#state{conn_pid = ConnPid}};
handle_dial_result({error, _Reason}, S) ->
    {noreply, schedule_reconnect(reset_conn(S))}.

reset_conn(#state{url = Url} = S) ->
    ok = macula_station_peer_links:clear_peer_node_id(Url),
    S#state{conn_pid = undefined,
            peer_node_id = undefined,
            last_inbound_at = undefined}.

%% Periodic silence-detection. Half-open peering connections occur
%% when the peer's listener-side dedup close-cast killed our inbound
%% counterpart but the close didn't propagate back to us (QUIC stays
%% alive via keep_alive PINGs at the transport layer). The macula_quic
%% keep_alive trick masks app-layer death; we need an app-layer
%% timeout.
%%
%% Trigger conditions (all must hold):
%%   * peer_node_id set (handshake completed at some point)
%%   * conn_pid alive
%%   * last_inbound_at older than ?SILENCE_THRESHOLD_MS
%%
%% Even an entirely-idle peer should emit `_mesh.bloom` at the
%% bloom_exchange rebuild cadence (30s by default) plus inbound
%% station_record announces, SWIM gossip, ADVERTISE, etc. Five
%% minutes of total silence on a station-to-station link is firmly
%% anomalous.
schedule_silence_check() ->
    erlang:send_after(?SILENCE_CHECK_MS, self(), silence_check),
    ok.

maybe_force_reconnect_on_silence(#state{peer_node_id = undefined} = S) ->
    %% Not connected yet — nothing to check.
    S;
maybe_force_reconnect_on_silence(#state{last_inbound_at = undefined} = S) ->
    S;
maybe_force_reconnect_on_silence(#state{conn_pid = undefined} = S) ->
    S;
maybe_force_reconnect_on_silence(#state{conn_pid = ConnPid,
                                        last_inbound_at = LastAt} = S) ->
    case now_ms() - LastAt of
        Silence when Silence >= ?SILENCE_THRESHOLD_MS ->
            macula_diagnostics:event(<<"_macula.peering.silence_reconnect">>, #{
                url           => S#state.url,
                silence_ms    => Silence,
                peer_node_id  => S#state.peer_node_id
            }),
            catch macula_peering:close(ConnPid, app_silence_timeout),
            S1 = fail_all_pending({disconnected, app_silence_timeout}, S),
            schedule_reconnect(reset_conn(S1));
        _ ->
            S
    end.

now_ms() -> erlang:monotonic_time(millisecond).

schedule_reconnect(#state{backoff_ms = Backoff,
                          reconnect_timer = OldTimer} = S) ->
    cancel_timer(OldTimer),
    %% Equal jitter: fire in [Backoff/2, Backoff] rather than exactly Backoff, so
    %% many links dropped by one event (a peer restart, a shared-box stall) do not
    %% reconnect in lockstep. The 25+48 same-second handshake failures of the
    %% 2026-07-24 incident are the signature of an un-jittered shared backoff
    %% ladder. The Backoff/2 floor preserves exponential relief; only the exact
    %% fire time is spread. `rand' auto-seeds per process, so co-spawned links
    %% draw distinct delays. Backoff is only ever INITIAL (>=1000) or a doubling
    %% of it, so `Backoff div 2' >= 500 and rand:uniform is always well-formed.
    Delay = (Backoff div 2) + rand:uniform(Backoff div 2),
    Timer = erlang:send_after(Delay, self(), dial),
    Next  = min(Backoff * 2, ?MAX_BACKOFF_MS),
    S#state{reconnect_timer = Timer, backoff_ms = Next}.

cancel_timer(undefined) -> ok;
cancel_timer(Ref)       -> erlang:cancel_timer(Ref), ok.

parse_url(<<"quic://", Rest/binary>>)  -> parse_host_port(Rest);
parse_url(<<"https://", Rest/binary>>) -> parse_host_port(Rest);
parse_url(B) when is_binary(B)         -> parse_host_port(B).

parse_host_port(B) ->
    case binary:split(B, <<":">>) of
        [H, P] -> {H, binary_to_integer(P)};
        [H]    -> {H, 4433}
    end.
