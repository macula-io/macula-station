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

-include_lib("kernel/include/logger.hrl").

-export([start_link/1, stop/1, peer_node_id/1, conn_pid/1, parse_url/1]).
-export([stats/1]).

-ifdef(TEST).
-export([futility_verdict/3, unverified_from/2]).
-endif.
-export([subscribe/4, unsubscribe/2, publish/4, call/5, is_connected/1]).
%% Dedicated-stream content transfer (PLAN_PER_STREAM_QUIC_ISOLATION.md
%% Phase 2), mirroring `macula_station_link' in the SDK — this module
%% is the CALLING side only (a peer's `_content.*' answer always
%% arrives via the accepting station's `macula_station_peer_observer',
%% never back through here as an inbound stream), so unlike the SDK
%% module there is no `new_dedicated_stream' acceptance clause to add.
-export([open_content_stream/1, call_on_stream/6, close_content_stream/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{
    url             := binary(),
    identity        := macula_identity:key_pair(),
    capabilities    => non_neg_integer(),
    %% TLS peer verification, passed straight through to the SDK dial. Omit for
    %% the secure default (webpki against a real CA), which is what production
    %% wants when dialling a DNS name with a Let's Encrypt cert. `none' is the
    %% SDK's documented escape for self-signed dev/lab peers dialled by IP,
    %% where no CA chain can validate and `expected_node_id' pinning cannot
    %% apply either because the cert SPKI is not the station's Ed25519 pubkey.
    verify          => term()
}.

%% Must stay >= 2: schedule_reconnect jitters the delay with `Backoff div 2', and
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
%% `disconnected') but the peer's application-level peering_conn
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
%% How long a link may go without EVER completing a handshake before it
%% is called futile. Coupled to two other numbers and must stay above
%% both: ?HANDSHAKE_TIMEOUT_MS (30s) + ?MAX_BACKOFF_MS (60s) is the ~90s
%% worst legitimate dial cycle, so 300s is 3.3 cycles of headroom.
%% Raising either of those without raising this manufactures a false
%% futility verdict.
-define(UNVERIFIED_FUTILE_MS, 300_000).

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
    verify          :: term(),
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
    %% Content-transfer dedicated streams — see the moduledoc note at
    %% the export list above. Same shape as the SDK's
    %% `macula_station_link' state fields of the same names.
    content_stream_bufs = #{} :: #{reference() => binary()},
    content_pending     = #{} :: #{reference() => {gen_server:from(), reference()}},
    %% Monotonic ms timestamp of the last inbound peering frame from
    %% this connection. Drives silence-based half-open detection.
    %% `undefined' means no frame received on the current connection
    %% (set on connect, updated on every inbound frame).
    last_inbound_at :: integer() | undefined,
    %% Futility accounting. A dial that never succeeds used to cost
    %% nothing: `handle_dial_result({error, _Reason}, S)' discarded the
    %% reason with no log, no counter and no event, so a link that had
    %% never once completed a handshake was indistinguishable from a
    %% healthy idle one. station-it-milan sat exactly there for 30 hours.
    %%
    %% ⚠ `unverified_since' is set when the link BECOMES unverified and
    %% is cleared ONLY by a successful handshake. A failed dial must
    %% never restart it — milan redialled on a 60s backoff throughout, so
    %% a clock the redial reset would have been re-zeroed forever and
    %% could never have accumulated to a verdict.
    dial_failures   = 0 :: non_neg_integer(),
    last_dial_error     :: term(),
    unverified_since    :: integer() | undefined,
    unverified_alarmed = false :: boolean()
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

%% @doc Open a dedicated QUIC stream for a sequence of related unary
%% CALLs to this peer station — see `macula_station_link:
%% open_content_stream/1' in the SDK, which this mirrors exactly.
-spec open_content_stream(pid()) -> {ok, reference()} | {error, term()}.
open_content_stream(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, open_content_stream, 10_000).

%% @doc Send a CALL on `Stream' (from `open_content_stream/1') and
%% block for its RESULT/ERROR on that same stream. See
%% `macula_station_link:call_on_stream/6' in the SDK.
-spec call_on_stream(pid(), reference(), <<_:256>>, binary(), term(),
                     pos_integer()) -> {ok, term()} | {error, term()}.
call_on_stream(Pid, Stream, Realm, Procedure, Payload, TimeoutMs)
  when is_pid(Pid), is_reference(Stream),
       is_binary(Realm), byte_size(Realm) =:= 32,
       is_binary(Procedure),
       is_integer(TimeoutMs), TimeoutMs > 0 ->
    GenTimeout = TimeoutMs + 500,
    try
        gen_server:call(Pid,
                        {call_on_stream, Stream, Realm, Procedure, Payload,
                         TimeoutMs},
                        GenTimeout)
    catch
        exit:{timeout, _} -> {error, timeout};
        exit:{noproc, _}  -> {error, noproc};
        exit:{normal, _}  -> {error, gone}
    end.

%% @doc Close a content stream opened via `open_content_stream/1'.
-spec close_content_stream(pid(), reference()) -> ok.
close_content_stream(Pid, Stream) when is_pid(Pid), is_reference(Stream) ->
    gen_server:cast(Pid, {close_content_stream, Stream}).

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
        verify       = maps:get(verify, Opts, undefined),
        capabilities = maps:get(capabilities, Opts, 0)
    },
    %% Register in peer_links immediately so the registry can monitor
    %% us; the `node_id' is filled in once the handshake completes.
    ok = macula_station_peer_links:register(Url, self()),
    self() ! dial,
    schedule_silence_check(),
    %% The clock starts here, not on the first failure: a link that never
    %% gets off the ground has been unverified since boot, and that is
    %% the interval the operator needs to see.
    {ok, State#state{unverified_since = now_ms()}}.

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

handle_call(open_content_stream, _From, #state{peer_node_id = undefined} = S) ->
    {reply, {error, not_connected}, S};
handle_call(open_content_stream, _From,
            #state{conn_pid = ConnPid, content_stream_bufs = Bufs} = S) ->
    open_content_stream_result(macula_peering:open_dedicated_stream(ConnPid),
                               Bufs, S);

handle_call({call_on_stream, _Stream, _Realm, _Proc, _Payload, _Tmo}, _From,
            #state{peer_node_id = undefined} = S) ->
    {reply, {error, not_connected}, S};
handle_call({call_on_stream, Stream, Realm, Proc, Payload, Tmo}, From,
            #state{identity = Id, content_pending = CP,
                   content_stream_bufs = Bufs} = S)
        when is_map_key(Stream, Bufs) ->
    Caller = macula_identity:public(Id),
    DeadlineMs = erlang:system_time(millisecond) + Tmo,
    Frame = macula_frame:call(#{
        call_id     => crypto:strong_rand_bytes(16),
        procedure   => Proc,
        realm       => Realm,
        payload     => Payload,
        deadline_ms => DeadlineMs,
        caller      => Caller
    }),
    await_content_call_reply(
      send_on_content_stream(Stream, Frame, Id), Stream, From, Tmo, CP, S);
handle_call({call_on_stream, _Stream, _Realm, _Proc, _Payload, _Tmo}, _From, S) ->
    {reply, {error, invalid_stream}, S};

handle_call(futility_stats, _From, S) ->
    {reply, #{url              => S#state.url,
              peer_node_id     => S#state.peer_node_id,
              dial_failures    => S#state.dial_failures,
              last_dial_error  => S#state.last_dial_error,
              unverified_since => S#state.unverified_since,
              verified         => S#state.peer_node_id =/= undefined}, S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast({close_content_stream, Stream}, S) ->
    {noreply, close_content_stream_state(Stream, S)};
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
    S1 = clear_futility(S#state{peer_node_id    = NodeId,
                                backoff_ms      = ?INITIAL_BACKOFF_MS,
                                last_inbound_at = now_ms()}),
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
handle_info({quic, Bin, Stream, _Flags},
            #state{content_stream_bufs = Bufs} = S)
        when is_binary(Bin), is_map_key(Stream, Bufs) ->
    Buf = maps:get(Stream, Bufs),
    {Frames, Tail} = macula_frame:parse_stream(<<Buf/binary, Bin/binary>>),
    NewS = lists:foldl(fun(F, Acc) -> dispatch_content_frame(F, Stream, Acc) end,
                       S#state{content_stream_bufs = Bufs#{Stream => Tail}},
                       Frames),
    {noreply, NewS};
handle_info({content_call_timeout, Stream}, #state{content_pending = CP} = S) ->
    {noreply, on_content_timeout(maps:take(Stream, CP), S)};
handle_info({'DOWN', Mon, process, _Pid, _Reason}, S) ->
    {noreply, on_subscriber_down(Mon, S)};
handle_info({'EXIT', ConnPid, _Reason}, #state{conn_pid = ConnPid} = S) ->
    S1 = fail_all_pending({disconnected, peering_exit}, S),
    {noreply, schedule_reconnect(reset_conn(S1))};
handle_info(silence_check, S) ->
    schedule_silence_check(),
    %% Two rules on one tick, and only the first one ACTS. The silence
    %% rule can force a reconnect; futility only ever reports. That
    %% asymmetry is deliberate — a futile dial is what a genuine network
    %% partition looks like from every station at once, and a detector
    %% that acted on it would turn a self-healing outage into a
    %% fleet-wide reconnect storm.
    {noreply, check_futility(maybe_force_reconnect_on_silence(S))};
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
%% view stays complete. The local-subscriber delivery in `deliver_event'
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
fail_all_pending(Reason, #state{pending = P,
                                content_pending = ContentP,
                                content_stream_bufs = Bufs} = S) ->
    maps:foreach(fun(_CallId, {From, TRef}) ->
        _ = erlang:cancel_timer(TRef),
        gen_server:reply(From, {error, Reason})
    end, P),
    maps:foreach(fun(_Stream, {From, TRef}) ->
        _ = erlang:cancel_timer(TRef),
        gen_server:reply(From, {error, Reason})
    end, ContentP),
    %% Content streams have no paired process to notify — just reclaim
    %% the QUIC resources.
    maps:foreach(fun(Stream, _Buf) -> catch macula_quic:close_stream(Stream) end,
                Bufs),
    S#state{pending = #{}, content_pending = #{}, content_stream_bufs = #{}}.

%%====================================================================
%% Content-transfer dedicated streams (Phase 2)
%%====================================================================

open_content_stream_result({ok, Stream}, Bufs, S) ->
    {reply, {ok, Stream}, S#state{content_stream_bufs = Bufs#{Stream => <<>>}}};
open_content_stream_result({error, _} = E, _Bufs, S) ->
    {reply, E, S}.

send_on_content_stream(Stream, Frame, Id) ->
    try macula_peering:send_on_stream(Stream, Frame, Id)
    catch C:R -> {error, {C, R}}
    end.

await_content_call_reply(ok, Stream, From, Tmo, Pending, S) ->
    TRef = erlang:send_after(Tmo, self(), {content_call_timeout, Stream}),
    {noreply, S#state{content_pending = Pending#{Stream => {From, TRef}}}};
await_content_call_reply({error, _} = Refused, _Stream, _From, _Tmo, _Pending, S) ->
    {reply, Refused, S}.

on_content_timeout(error, S) ->
    S;
on_content_timeout({{From, _OldTRef}, NewCP}, S) ->
    gen_server:reply(From, {error, timeout}),
    S#state{content_pending = NewCP}.

dispatch_content_frame(#{frame_type := result, payload := Payload}, Stream, S) ->
    deliver_content_reply(Stream, {ok, Payload}, S);
dispatch_content_frame(#{frame_type := error} = Frame, Stream, S) ->
    Code    = maps:get(code, Frame, undefined),
    Message = maps:get(message, Frame, undefined),
    deliver_content_reply(Stream, {error, {call_error, Code, Message}}, S);
dispatch_content_frame(_Frame, _Stream, S) ->
    S.

deliver_content_reply(Stream, Reply, #state{content_pending = CP} = S) ->
    reply_content_pending(maps:take(Stream, CP), Reply, S).

reply_content_pending(error, _Reply, S) ->
    S;
reply_content_pending({{From, TRef}, NewCP}, Reply, S) ->
    _ = erlang:cancel_timer(TRef),
    gen_server:reply(From, Reply),
    S#state{content_pending = NewCP}.

close_content_stream_state(Stream, #state{content_pending = CP,
                                          content_stream_bufs = Bufs} = S) ->
    NewCP = fail_content_pending(maps:take(Stream, CP), CP),
    catch macula_quic:close_stream(Stream),
    S#state{content_pending = NewCP,
            content_stream_bufs = maps:remove(Stream, Bufs)}.

fail_content_pending(error, CP) ->
    CP;
fail_content_pending({{From, TRef}, NewCP}, _CP) ->
    _ = erlang:cancel_timer(TRef),
    gen_server:reply(From, {error, closed}),
    NewCP.

%%====================================================================
%% Internals
%%====================================================================

forward_to_observer(Msg) ->
    case whereis(macula_station_peer_observer) of
        undefined -> ok;
        Pid       -> Pid ! Msg, ok
    end.

do_dial(#state{host = H, port = P, identity = Kp, verify = V,
               capabilities = Caps} = S) ->
    Target = maybe_verify(V, #{host => H, port => P,
                               timeout_ms => ?HANDSHAKE_TIMEOUT_MS}),
    Result = macula_peering:connect(#{
        role            => client,
        identity        => Kp,
        realms          => [],
        capabilities    => Caps,
        controlling_pid => self(),
        target          => Target
    }),
    handle_dial_result(Result, S).

%% Absent means "leave the SDK default alone", which is the secure one. This
%% must never default to `none': a silent downgrade to unverified TLS is the
%% kind of thing that ships once and is never noticed.
maybe_verify(undefined, Target) -> Target;
maybe_verify(V, Target)         -> Target#{verify => V}.

handle_dial_result({ok, ConnPid}, S) ->
    {noreply, S#state{conn_pid = ConnPid}};
%% Counts, and does NOT act. Both prior detectors in this module's
%% history that acted on a negative signal caused outages: the
%% conn-aging sweep closed healthy conns, and the silence probe
%% false-fired under CPU starvation into a handshake storm. An
%% observation cannot do that.
handle_dial_result({error, Reason}, #state{dial_failures = N} = S) ->
    S1 = S#state{dial_failures = N + 1, last_dial_error = Reason},
    {noreply, schedule_reconnect(reset_conn(S1))}.

reset_conn(#state{url = Url} = S) ->
    ok = macula_station_peer_links:clear_peer_node_id(Url),
    S#state{conn_pid = undefined,
            peer_node_id = undefined,
            last_inbound_at = undefined,
            unverified_since = unverified_from(S#state.unverified_since,
                                               now_ms())}.

%% Start the clock on the verified -> unverified transition, and leave a
%% running clock alone. A redial that reset it would re-zero the interval
%% on every backoff cycle, so a link failing forever would look
%% permanently fresh — which is exactly how milan stayed invisible.
-spec unverified_from(integer() | undefined, integer()) -> integer().
unverified_from(undefined, Now) -> Now;
unverified_from(Since, _Now)    -> Since.

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
%% Even an entirely-idle peer should emit `_mesh.bloom' at the
%% bloom_exchange rebuild cadence (30s by default) plus inbound
%% station_record announces, SWIM gossip, ADVERTISE, etc. Five
%% minutes of total silence on a station-to-station link is firmly
%% anomalous.
%%====================================================================
%% Futility — a dial that never succeeds must cost a counter
%%====================================================================

%% @doc Everything this link knows about its own failure to connect.
%%
%% Exists because nothing did. Before this, a link that had NEVER
%% completed a handshake and one that was healthy and idle produced
%% byte-identical output: nothing.
-spec stats(pid()) -> #{atom() => term()}.
stats(Pid) ->
    gen_server:call(Pid, futility_stats, 1_000).

check_futility(#state{unverified_since = Since,
                      unverified_alarmed = Alarmed} = S) ->
    Verdict = futility_verdict(Since, now_ms(), ?UNVERIFIED_FUTILE_MS),
    S#state{unverified_alarmed = futility_edge(Verdict, Alarmed, S)}.

clear_futility(#state{unverified_alarmed = true, url = Url} = S) ->
    ?LOG_NOTICE("[outbound_link] ~s verified at last after ~B failed dials",
                [Url, S#state.dial_failures]),
    S#state{unverified_since = undefined, unverified_alarmed = false,
            dial_failures = 0};
clear_futility(S) ->
    S#state{unverified_since = undefined, dial_failures = 0}.

%% @doc Has this link failed to EVER verify for longer than it should?
%% Pure, so the threshold can be tested without waiting five minutes.
-spec futility_verdict(integer() | undefined, integer(), pos_integer()) ->
    ok | futile.
futility_verdict(undefined, _Now, _Threshold) ->
    %% Verified. Nothing owed.
    ok;
futility_verdict(Since, Now, Threshold) when Now - Since >= Threshold ->
    futile;
futility_verdict(_Since, _Now, _Threshold) ->
    ok.

%% Reports. Never acts. See the silence_check handler for why.
futility_edge(futile, false, #state{url = Url} = S) ->
    ?LOG_ERROR("[outbound_link] ~s has NEVER completed a handshake in ~B ms "
               "across ~B failed dials (last error: ~p). This station is "
               "configured to dial this peer and has no link to it.",
               [Url, now_ms() - S#state.unverified_since,
                S#state.dial_failures, S#state.last_dial_error]),
    catch macula_diagnostics:event(<<"_macula.peering.outbound_futile">>, #{
        url           => Url,
        unverified_ms => now_ms() - S#state.unverified_since,
        dial_failures => S#state.dial_failures
    }),
    true;
futility_edge(_Verdict, Alarmed, _S) ->
    Alarmed.

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

-spec parse_url(binary()) -> {binary(), pos_integer()}.
parse_url(<<"quic://", Rest/binary>>)  -> parse_host_port(Rest);
parse_url(<<"https://", Rest/binary>>) -> parse_host_port(Rest);
parse_url(B) when is_binary(B)         -> parse_host_port(B).

%% @doc Split a peering URL into host and port. Exported because its IPv6
%% handling is subtle enough to deserve tests of its own.
%%
%% ⚠ AN IPv6 LITERAL MUST BE BRACKETED, and this used to crash on one.
%%
%% Splitting on the first colon works for `host:port' and breaks completely for
%% `::1:5000', where the host itself is full of colons: the old code handed
%% `<<":1:5000">>' to `binary_to_integer/1' and died with a badarg. RFC 3986
%% section 3.2.2 requires the brackets precisely to make the port delimiter
%% unambiguous, so honour them.
%%
%% Latent in production, which dials DNS names, and NOT latent for any test
%% cluster: `macula_station_test_cluster' binds every station to `::1', so no
%% in-repo test could build an outbound link at all until this was fixed.
parse_host_port(<<"[", Rest/binary>>) ->
    bracketed(binary:split(Rest, <<"]">>));
parse_host_port(B) ->
    unbracketed(B).

bracketed([Host, <<":", Port/binary>>]) -> {Host, binary_to_integer(Port)};
bracketed([Host, _NoPort])              -> {Host, 4433};
bracketed([Malformed])                  -> {Malformed, 4433}.

%% More than one colon and no brackets is a bare IPv6 literal. It is
%% unparseable by definition -- the port delimiter is ambiguous -- so take the
%% whole thing as the host on the default port rather than crashing the link.
%%
%% Split GLOBALLY to decide that. A non-global split of `<<"::1">>' yields two
%% parts, the same shape as `host:port', and feeds `<<":1">>' to
%% binary_to_integer. That is the original bug in miniature, and it survived my
%% first fix because the comment described the intent while the code kept the
%% two-element clause.
unbracketed(B) ->
    on_colon_count(binary:split(B, <<":">>, [global]), B).

on_colon_count([H],    _B) -> {H, 4433};
on_colon_count([H, P], _B) -> {H, binary_to_integer(P)};
on_colon_count(_Many,   B) -> {B, 4433}.
