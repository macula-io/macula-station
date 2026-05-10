%% @doc Peer observer — glue between peering, DHT, and SWIM.
%%
%% Each station has exactly one observer (when booted via the
%% supervisor). It is the `controlling_pid' for every
%% `macula_peering_conn' worker the listener accepts and for every
%% outbound connect initiated via `macula_station:connect_to/1'. It
%% turns peering events into the mutations the rest of the station
%% expects:
%%
%% <ul>
%%   <li>`{macula_peering, connected, ConnPid, PeerNodeId}' →
%%       `macula_dht:observe/2' with an entry spec carrying
%%       `tier = t0' and `macula_swim:add_peer/3' so the failure
%%       detector starts probing the new member.</li>
%%   <li>`{macula_peering, frame, ConnPid, Frame}' — the observer
%%       verifies the signature and dispatches SWIM frames
%%       (ping / ack / suspect / confirm) to
%%       `macula_swim:handle_frame/3'. Application-layer frames
%%       (HyParView / Plumtree / pubsub / realm admission) are
%%       <em>not</em> a station concern — the station is
%%       realm-agnostic infrastructure per the railroad mental
%%       model; realm identity + overlay live in a separate
%%       `hecate-realm' / `macula-realm' service that dials this
%%       station like any other peer. Unknown frame types are
%%       dropped silently and flow end-to-end between the peering
%%       workers of the realm service and its clients.</li>
%%   <li>`{macula_peering, disconnected, ConnPid, _Reason}' →
%%       `macula_swim:remove_peer/2'.</li>
%% </ul>
%%
%% State is two maps — `peers' for the ConnPid → NodeId reverse
%% lookup used by disconnect handling, and `conns' for the forward
%% NodeId → ConnPid lookup exposed via `conn_for/2' so callers
%% (e.g. the future realm service when co-resident with a station)
%% can send frames to a known peer without re-discovering it.
-module(macula_station_peer_observer).
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

%% Direction of a peering connection from THIS station's perspective:
%%   inbound  — the listener accepted a CONNECT from the peer (server-role).
%%   outbound — `outbound_link' dialled the peer (client-role).
%% Mutual peers (both sides have an outbound_peers entry for the other)
%% materialise BOTH directions; lone-direction peers materialise one.
-type direction() :: inbound | outbound.

%% Per-NodeId conns view. Held as a map so we can carry both directions
%% concurrently without one overwriting the other. `inbound' is the conn
%% that received the peer's SUBSCRIBE / CALL frames (its `controlling_pid'
%% is this peer_observer). `outbound' is the conn we dial out on (its
%% `controlling_pid' is the relevant `macula_station_outbound_link').
%%
%% EVENT delivery to a peer who subscribed via the inbound side MUST go
%% out on `inbound'; the bytes then arrive at the peer's
%% `outbound_link', whose subscriber-fan-out delivers
%% `{macula_event, ...}' messages to local Erlang processes (e.g.
%% `bloom_exchange'). If we mistakenly send via `outbound', the bytes
%% land on the peer's listener-side conn, dispatch into the peer's
%% pubsub_server, and dead-end — the peer has no local subscribers
%% there.
-type peer_conns() :: #{
    inbound  => pid() | undefined,
    outbound => pid() | undefined
}.

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
    %% NodeId → {inbound, outbound} ConnPids. Both can be set for mutual
    %% peers; clears entry-by-entry on `disconnected'.
    conns                :: #{macula_identity:pubkey() => peer_conns()},
    %% Reverse map ConnPid → its direction, populated alongside `peers'.
    %% Lets `on_disconnected' / DOWN-handler clear ONLY the affected
    %% direction in `conns' instead of nuking the whole NodeId entry
    %% (which would lose the still-live other direction).
    direction_of_pid = #{} :: #{pid() => direction()},
    %% In-flight forwarded CALLs. Maps each forwarded call_id to the
    %% origin connection that issued the CALL plus a TTL timer ref
    %% so an entry whose advertiser never replies (advertiser
    %% disconnects mid-call, advertiser process wedges, etc.) is
    %% reaped instead of leaking. The origin's station_link enforces
    %% its own deadline at the SDK level — the relay-side timer is
    %% pure cleanup.
    forwarded = #{}      :: #{<<_:128>> => {pid(), reference()}},
    %% In-flight streaming RPCs. Maps each stream_id to the caller-
    %% origin connection that opened it and the advertiser connection
    %% it was forwarded to, plus a TTL timer ref so a hung stream
    %% (advertiser crashed mid-stream, malformed STREAM_END) is
    %% reaped instead of leaking forever. Cleared on STREAM_END /
    %% STREAM_ERROR / STREAM_REPLY (final frames) or on the timer.
    streams   = #{}      :: #{<<_:128>> => {pid(), pid(), reference()}}
}).

%% Hard upper bound on how long a forwarded CALL entry may live on
%% the relay. Above any reasonable per-call deadline (the longest
%% caller-side timeout in the fleet today is 10 s on
%% `_realm.membership.join_with_token_v1`). Defensive — purges
%% stragglers even when a buggy peer never sends RESULT/ERROR.
-define(FORWARDED_TTL_MS, 60_000).

%% Streaming RPC relay TTL — analogous to FORWARDED_TTL_MS but for
%% streams. Streaming workloads can be longer-lived than unary calls
%% (e.g. a server_stream paginating a large result set), so this
%% bound is generous. A normal stream is closed by an explicit
%% STREAM_END / STREAM_REPLY frame, which removes the entry well
%% before the timer fires.
-define(STREAM_TTL_MS, 300_000).

%% Named ETS table mirroring the `conns' map so external callers
%% (DHT transport, anyone routing wire frames by NodeId) read
%% without serializing against the observer's gen_server mailbox.
-define(CONNS_TABLE, macula_station_peer_observer_conns).

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

%% Reads from the public ETS conns mirror — bypasses the gen_server
%% mailbox so wire-send paths (the DHT's `send_frame' callback fires
%% per outgoing STORE / FIND_VALUE / etc.) do NOT serialize against
%% the observer's frame-handling loop. Under burst load the
%% gen_server:call path queued tens of conn_for lookups behind
%% in-flight ADVERTISE / EVENT processing and timed out at 5s,
%% cascading into supervised-process restarts. The ETS read is
%% O(1) and concurrent.
%%
%% `Pid' is retained in the API for caller-side ergonomics (callers
%% already hold `whereis(macula_station_peer_observer)' or pass it
%% explicitly) but is not used — `?CONNS_TABLE' is named, single
%% per BEAM, so a stale pid does not matter.
-spec conn_for(pid(), macula_identity:pubkey()) ->
    {ok, pid()} | error.
conn_for(Pid, <<_:256>> = NodeId) ->
    on_ets_lookup(ets_find_conns(NodeId), Pid, NodeId).

on_ets_lookup(table_missing, Pid, NodeId) ->
    %% Stub observers in tests don't own the table; defer to their
    %% gen_server:call handler. Production peer_observer always
    %% creates the table in init/1, so this clause is unreachable
    %% in production paths.
    gen_server:call(Pid, {conn_for, NodeId});
on_ets_lookup(Result, _Pid, _NodeId) ->
    primary_conn_lookup(Result).

%%==================================================================
%% gen_server
%%==================================================================

init(#{dht := Dht, swim := Swim} = Opts)
  when is_pid(Dht), is_pid(Swim) ->
    %% `protected' = anyone reads, only owner writes. `set' = unique
    %% NodeId key, last write wins (matches the `conns' map's
    %% per-NodeId semantics). `read_concurrency' biases the BEAM
    %% scheduler for the read-heavy access pattern (every wire frame
    %% routes through one read).
    %%
    %% Idempotent: on supervisor restart the prior peer_observer
    %% process is already gone, taking the table with it; if for
    %% some reason the table still exists, recreate cleanly.
    catch ets:delete(?CONNS_TABLE),
    ?CONNS_TABLE = ets:new(?CONNS_TABLE,
                           [named_table, protected, set,
                            {read_concurrency, true}]),
    State0 = #state{dht              = Dht,
                    swim             = Swim,
                    handler_registry = maps:get(handler_registry, Opts, undefined),
                    pubsub_registry  = maps:get(pubsub_registry, Opts, undefined),
                    remote_advertise = maps:get(remote_advertise, Opts, undefined),
                    self_id          = maps:get(self_id, Opts, undefined),
                    peers            = #{},
                    conns            = #{}},
    %% Self-forming: derive the initial peers/conns view from the
    %% peer_links registry (canonical source of truth for outbound
    %% dials) instead of relying on having received every `connected'
    %% event up to this point. Boot order, observer restart, and
    %% missed notifications all become non-issues because the observer
    %% reconciles whatever is true at init time and uses
    %% `erlang:monitor/2' for ongoing death detection.
    {ok, reconcile_outbound_dials(State0)}.

%% Walk peer_links' current verified entries; for each link with a
%% known peer node-id, query the link for its underlying conn_pid and
%% fold it into our maps via the standard on_connected path. Failures
%% (race with a link about to die, link not responding) are tolerated
%% — anything we miss here will be picked up by a later `connected'
%% notification from outbound_link's forward_to_observer.
reconcile_outbound_dials(S) ->
    Verified = try macula_station_peer_links:verified_peers()
               catch _:_ -> []
               end,
    lists:foldl(fun(#{pid := LinkPid, node_id := NodeId}, Acc) ->
        absorb_known_dial(LinkPid, NodeId, Acc)
    end, S, Verified).

absorb_known_dial(LinkPid, NodeId, S) ->
    try macula_station_outbound_link:conn_pid(LinkPid) of
        undefined -> S;
        ConnPid when is_pid(ConnPid) ->
            logger:info("[peer_observer] reconciled outbound dial "
                        "pid=~p peer=~p", [ConnPid, NodeId]),
            on_connected_directional(outbound, ConnPid, NodeId, S)
    catch
        _:_ -> S
    end.

handle_call(peers, _From, #state{peers = P} = S) ->
    {reply, maps:to_list(P), S};
handle_call({conn_for, NodeId}, _From, #state{conns = C} = S) ->
    %% Kept for direct gen_server callers (tests, debug) — the
    %% public `conn_for/2' export now uses the ETS mirror and never
    %% reaches this clause in production.
    {reply, primary_conn_lookup(maps:find(NodeId, C)), S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({macula_peering, connected, ConnPid, PeerNodeId}, S) ->
    %% Inbound (listener-accepted) handshake — peer dialled us.
    logger:info("[peer_observer] connected pid=~p peer=~p direction=inbound",
                [ConnPid, PeerNodeId]),
    {noreply, on_connected_directional(inbound, ConnPid, PeerNodeId, S)};
handle_info({macula_peering, connected_outbound, ConnPid, PeerNodeId}, S) ->
    %% Outbound (client-side) handshake — `outbound_link' relays
    %% peering events here with the `_outbound' suffix so direction
    %% is unambiguous. Without it the conn map would race between
    %% inbound + outbound for mutual peers and EVENT delivery would
    %% land on the wrong side of the link.
    logger:info("[peer_observer] connected pid=~p peer=~p direction=outbound",
                [ConnPid, PeerNodeId]),
    {noreply, on_connected_directional(outbound, ConnPid, PeerNodeId, S)};
handle_info({macula_peering, frame, ConnPid, Frame}, S) ->
    logger:debug("[peer_observer] frame pid=~p type=~p",
                 [ConnPid, macula_frame:frame_type(Frame)]),
    {noreply, on_frame(ConnPid, Frame, S)};
handle_info({macula_peering, disconnected, ConnPid, _Reason}, S) ->
    {noreply, on_disconnected(ConnPid, S)};
handle_info({macula_peering, disconnected_outbound, ConnPid, _Reason}, S) ->
    {noreply, on_disconnected(ConnPid, S)};
handle_info({'DOWN', _Ref, process, Pid, _Reason},
            #state{peers = P} = S) ->
    %% A conn worker we were monitoring died for any reason — explicit
    %% close, peer disconnect, upstream outbound_link crash, network
    %% drop. on_disconnected is idempotent on unknown pids.
    case maps:is_key(Pid, P) of
        true  -> {noreply, on_disconnected(Pid, S)};
        false -> {noreply, S}
    end;
handle_info({forwarded_timeout, CallId}, #state{forwarded = F} = S) ->
    {noreply, on_forwarded_timeout(maps:take(CallId, F), S)};
handle_info({stream_timeout, Sid}, #state{streams = St} = S) ->
    {noreply, on_stream_timeout(maps:take(Sid, St), S)};
handle_info(_Msg, S) ->
    {noreply, S}.

%% TTL fired with no reply ever arriving. The origin's station_link
%% has already given up at the SDK level (its own deadline timer
%% surfaced `{error, timeout}` to the caller), so we simply clear
%% our entry. Logged at info so a flood of timeouts surfaces in
%% operations dashboards before it becomes a memory leak.
on_forwarded_timeout(error, S) ->
    S;
on_forwarded_timeout({{_Origin, _TRef}, NewF}, S) ->
    logger:info("[peer_observer] forwarded CALL timed out — purging",
                []),
    S#state{forwarded = NewF}.

on_stream_timeout(error, S) ->
    S;
on_stream_timeout({{_Caller, _Adv, _TRef}, NewSt}, S) ->
    logger:info("[peer_observer] stream relay timed out — purging", []),
    S#state{streams = NewSt}.

terminate(_Reason, _S) -> ok.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%==================================================================
%% connected
%%==================================================================

on_connected_directional(Direction, ConnPid, NodeId,
                         #state{dht = Dht, swim = Swim,
                                peers = P, conns = C,
                                direction_of_pid = D} = S) ->
    %% Fire-and-forget admit. `macula_dht:observe/2' is a sync
    %% gen_server:call; under accumulated daemon-conn load the DHT
    %% server gets slow enough that the call times out (5s default)
    %% and crashes peer_observer, taking the named conns ETS table
    %% with it and triggering a fleet-wide cascade as the supervisor
    %% restarts. The admit-result is unused here, so the cast variant
    %% is strictly safer. SWIM is already cast (see `add_peer/3').
    %% Empirical evidence: cascade root-cause investigation 2026-05-10
    %% caught peer_observer dying with mailbox=504, exit=
    %% gen_server:call timeout to macula_dht:observe.
    macula_dht:observe_async(Dht, direct_peer_spec(NodeId)),
    ok = macula_swim:add_peer(Swim, NodeId, ConnPid),
    %% Monitor ConnPid so observer cleans up on death without
    %% depending on a `disconnected' event flowing through some
    %% controlling_pid we may or may not own. Idempotent re-monitoring
    %% is fine — extra DOWN messages hit `on_disconnected' which is
    %% map-remove based.
    _ = erlang:monitor(process, ConnPid),
    Existing = maps:get(NodeId, C, empty_peer_conns()),
    Updated  = Existing#{Direction => ConnPid},
    write_conn_table(NodeId, Updated),
    S#state{peers            = P#{ConnPid => NodeId},
            conns            = C#{NodeId => Updated},
            direction_of_pid = D#{ConnPid => Direction}}.

%% ETS mirror writers — must run from the gen_server process (table
%% is `protected'). Both write the same value the `conns' map sees,
%% so external readers via `conn_for/2' get consistent state.
write_conn_table(NodeId, PeerConns) ->
    ets:insert(?CONNS_TABLE, {NodeId, PeerConns}).

delete_conn_table(NodeId) ->
    ets:delete(?CONNS_TABLE, NodeId).

%% Tolerant of missing table: tests that swap in a stub observer
%% via `macula_station_peer_observer` API expect the gen_server:call
%% fallback path. Fall through to that when the named ETS table
%% isn't present in this BEAM.
ets_find_conns(NodeId) ->
    try ets:lookup(?CONNS_TABLE, NodeId) of
        [{_, PeerConns}] -> {ok, PeerConns};
        []               -> error
    catch
        error:badarg -> table_missing
    end.

empty_peer_conns() ->
    #{inbound => undefined, outbound => undefined}.

%% Pick the conn we want to use for fire-and-forget outbound
%% writes that don't require a specific direction (replies, forwarded
%% CALL trampolines, the public `conn_for/2' API). Inbound is
%% preferred because EVENT-delivery uses the same lookup and
%% inbound is the right answer there; non-EVENT callers don't care
%% which direction they get.
primary_conn_lookup(error) ->
    error;
primary_conn_lookup({ok, #{inbound := Pid}}) when is_pid(Pid) ->
    {ok, Pid};
primary_conn_lookup({ok, #{outbound := Pid}}) when is_pid(Pid) ->
    {ok, Pid};
primary_conn_lookup({ok, _Empty}) ->
    error.

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
classify(ping)         -> dht;
classify(pong)         -> dht;
classify(find_node)    -> dht;
classify(nodes)        -> dht;
classify(find_value)   -> dht;
classify(value)        -> dht;
classify(store)        -> dht;
classify(store_ack)    -> dht;
classify(stream_open)  -> stream;
classify(stream_data)  -> stream;
classify(stream_end)   -> stream;
classify(stream_error) -> stream;
classify(stream_reply) -> stream;
classify(_)            -> other.

claimed_caller(#{caller := <<_:256>> = Pub}) -> Pub;
claimed_caller(_)                            -> <<0:256>>.

claimed_replier(#{responded_by := <<_:256>> = Pub}) -> Pub;
claimed_replier(#{reported_by  := <<_:256>> = Pub}) -> Pub;
claimed_replier(_)                                  -> <<0:256>>.

dispatch(swim, Frame, _ConnPid, NodeId, #state{swim = Swim} = S) ->
    deliver_swim(macula_frame:verify(Frame, NodeId), Frame, NodeId, Swim),
    S;
dispatch(call, Frame, ConnPid, NodeId, S) ->
    %% CALL is signed end-to-end by the original caller and forwarded
    %% as-is across relay hops, so verify against the frame's claimed
    %% caller — NOT the connection's NodeId, which would only match on
    %% a direct dial (caller -> first relay) and fail on every
    %% subsequent hop in a multi-station path.
    deliver_call(macula_frame:verify(Frame, claimed_caller(Frame)),
                 Frame, ConnPid, NodeId, S);
dispatch(reply, Frame, _ConnPid, _NodeId, S) ->
    %% RESULT signed by `responded_by'; call_error signed by
    %% `reported_by'. Both are preserved end-to-end across the relay
    %% chain, same reasoning as CALL above.
    deliver_reply(macula_frame:verify(Frame, claimed_replier(Frame)),
                  Frame, S);
dispatch(advertise, Frame, ConnPid, NodeId, S) ->
    deliver_advertise(macula_frame:verify(Frame, NodeId), Frame, ConnPid, NodeId, S),
    S;
dispatch(pubsub, Frame, _ConnPid, NodeId, S) ->
    deliver_pubsub(macula_frame:verify(Frame, NodeId), Frame, NodeId, S),
    S;
dispatch(dht, Frame, _ConnPid, NodeId, #state{dht = Dht} = S) ->
    ok = macula_dht:handle_frame(Dht, NodeId, Frame),
    S;
dispatch(stream, Frame, ConnPid, NodeId, S) ->
    %% STREAM_OPEN is signed end-to-end by the original caller (same
    %% rationale as CALL); subsequent stream frames are signed by
    %% whichever end emitted them, and for now we verify against the
    %% connection's NodeId — sufficient for the single-station +
    %% direct-edge cases. Multi-hop stream relay would need
    %% claimed-signer extraction analogous to claimed_caller/1, but
    %% the suite only exercises single-station streaming for now.
    deliver_stream(macula_frame:frame_type(Frame), Frame, ConnPid, NodeId, S);
dispatch(other, _Frame, _ConnPid, _NodeId, S) ->
    S.

deliver_swim({ok, _}, Frame, NodeId, Swim) ->
    ok = macula_swim:handle_frame(Swim, NodeId, Frame);
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
%% `macula_handler_dispatch' which traps crashes; the observer
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
    macula_handler_registry:lookup(Registry, Procedure).

on_call_lookup({ok, _Handler}, Frame, _ConnPid, NodeId,
               #state{handler_registry = Registry,
                      self_id = SelfId, conns = C} = S) ->
    Reply = macula_handler_dispatch:dispatch_call(Frame, Registry, SelfId),
    send_reply_to(primary_conn_lookup(maps:find(NodeId, C)), Reply),
    S;
on_call_lookup({error, not_found}, Frame, ConnPid, NodeId, S) ->
    on_remote_lookup(remote_lookup(Frame, S), Frame, ConnPid, NodeId, S).

remote_lookup(_Frame, #state{remote_advertise = undefined}) ->
    {error, not_found};
remote_lookup(Frame, #state{remote_advertise = R}) ->
    Realm     = maps:get(realm,     Frame),
    Procedure = maps:get(procedure, Frame),
    macula_remote_advertise_registry:lookup(R, Realm, Procedure).

%% Remote-advertise miss — synthesize a signed `unknown_next_peer'
%% reply to the caller (origin connection) so they fail fast.
on_remote_lookup({error, not_found}, Frame, _ConnPid, NodeId,
                 #state{self_id = SelfId, conns = C} = S) ->
    Reply = unknown_next_peer_reply(Frame, SelfId),
    send_reply_to(primary_conn_lookup(maps:find(NodeId, C)), Reply),
    S;
on_remote_lookup({ok, #{conn_pid := AdvertiserConn}}, Frame, _ConnPid, NodeId,
                 #state{conns = C, forwarded = F} = S) ->
    %% Forward the CALL frame as-is over the advertiser's connection.
    %% The advertiser's `macula_station_link' will dispatch it to its
    %% local handler and emit a RESULT or call_error back. Track the
    %% origin so we can route the reply.
    macula_peering:send_frame(AdvertiserConn, Frame),
    Origin = origin_for_reply(primary_conn_lookup(maps:find(NodeId, C))),
    track_forwarded(maps:get(call_id, Frame), Origin, F, S).

origin_for_reply({ok, ConnPid}) -> ConnPid;
origin_for_reply(error)         -> undefined.

track_forwarded(_CallId, undefined, _F, S) ->
    %% Origin already gone; the eventual RESULT will be dropped on
    %% arrival because no `forwarded' entry exists. Safe.
    S;
track_forwarded(CallId, Origin, F, S) ->
    %% TTL timer ensures the entry is purged even when no reply ever
    %% arrives (advertiser disconnects mid-call, advertiser process
    %% hangs, malformed reply). Cancelled in `relay_forwarded_reply'
    %% on a normal RESULT/ERROR.
    TRef = erlang:send_after(?FORWARDED_TTL_MS, self(),
                             {forwarded_timeout, CallId}),
    S#state{forwarded = F#{CallId => {Origin, TRef}}}.

unknown_next_peer_reply(#{call_id := CallId}, SelfId) ->
    macula_frame:call_error(#{call_id     => CallId,
                              code        => 16#01,
                              reported_by => SelfId}).

%%==================================================================
%% STREAM dispatch — STREAM_OPEN routes by procedure (mirrors CALL);
%% STREAM_DATA / STREAM_END / STREAM_ERROR / STREAM_REPLY route by
%% stream_id (mirrors how `forwarded' relays RESULT by call_id).
%%==================================================================

deliver_stream(stream_open, Frame, ConnPid, NodeId, S) ->
    on_stream_open_verify(macula_frame:verify(Frame, claimed_caller(Frame)),
                          Frame, ConnPid, NodeId, S);
deliver_stream(_Other, Frame, ConnPid, NodeId, S) ->
    on_stream_relay(macula_frame:verify(Frame, NodeId), Frame, ConnPid, S).

on_stream_open_verify({error, _}, _Frame, _ConnPid, _NodeId, S) ->
    S;
on_stream_open_verify({ok, Frame}, _OrigFrame, ConnPid, NodeId, S) ->
    on_stream_open_lookup(remote_lookup(Frame, S), Frame, ConnPid, NodeId, S).

%% Procedure not advertised on this station — synthesize a
%% `stream_error' back to the caller so the SDK fails fast instead
%% of waiting for the deadline.
on_stream_open_lookup({error, not_found}, Frame, _ConnPid, NodeId,
                      #state{self_id = SelfId, conns = C} = S) ->
    Reply = stream_unknown_reply(Frame, SelfId),
    send_reply_to(primary_conn_lookup(maps:find(NodeId, C)), Reply),
    S;
on_stream_open_lookup({ok, #{conn_pid := AdvertiserConn}}, Frame,
                      _ConnPid, NodeId,
                      #state{conns = C, streams = Streams} = S) ->
    %% Forward STREAM_OPEN as-is to the advertiser; remember the
    %% caller-origin and the advertiser conn so subsequent
    %% STREAM_DATA / STREAM_END frames can route back and forth.
    macula_peering:send_frame(AdvertiserConn, Frame),
    CallerOrigin = origin_for_reply(primary_conn_lookup(maps:find(NodeId, C))),
    track_stream(maps:get(stream_id, Frame),
                 CallerOrigin, AdvertiserConn, Streams, S).

track_stream(_Sid, undefined, _AdvConn, _Streams, S) ->
    %% Caller's origin already gone — the advertiser's eventual
    %% STREAM_DATA cannot route anywhere; drop on arrival via the
    %% missing-streams-entry path.
    S;
track_stream(Sid, CallerOrigin, AdvertiserConn, Streams, S) ->
    TRef = erlang:send_after(?STREAM_TTL_MS, self(),
                             {stream_timeout, Sid}),
    S#state{streams = Streams#{Sid => {CallerOrigin, AdvertiserConn, TRef}}}.

stream_unknown_reply(#{stream_id := Sid}, _SelfId) ->
    macula_frame:stream_error(#{stream_id => Sid,
                                code      => <<"unknown_next_peer">>,
                                message   => <<"procedure not advertised">>}).

%% Bidirectional relay: a stream frame from the caller goes to the
%% advertiser; a frame from the advertiser goes to the caller. We
%% identify the source by comparing the inbound conn against the
%% pair we tracked on STREAM_OPEN. Unknown stream_id (never opened
%% on this station, or already torn down) drops silently.
on_stream_relay({error, _}, _Frame, _ConnPid, S) ->
    S;
on_stream_relay({ok, Frame}, _OrigFrame, ConnPid,
                #state{streams = Streams} = S) ->
    Sid = maps:get(stream_id, Frame),
    on_stream_relay_lookup(maps:find(Sid, Streams), Sid,
                           macula_frame:frame_type(Frame),
                           Frame, ConnPid, Streams, S).

on_stream_relay_lookup(error, _Sid, _Type, _Frame, _ConnPid, _Streams, S) ->
    S;
on_stream_relay_lookup({ok, {CallerOrigin, AdvConn, TRef}}, Sid, Type,
                       Frame, ConnPid, Streams, S) ->
    Target = relay_target(ConnPid, CallerOrigin, AdvConn),
    _ = macula_peering:send_frame(Target, Frame),
    maybe_close_stream(Type, Sid, TRef, Streams, S).

%% Source = caller-side ⇒ forward to advertiser. Source = anything
%% else (advertiser-side, in the typical server_stream path
%% server → caller) ⇒ forward to caller. The routing pair was
%% established when STREAM_OPEN arrived; we identify the caller-
%% origin conn by exact pid match against the tracked pair.
relay_target(SourceConn, CallerOrigin, AdvConn)
  when SourceConn =:= CallerOrigin ->
    AdvConn;
relay_target(_SourceConn, CallerOrigin, _AdvConn) ->
    CallerOrigin.

%% STREAM_END / STREAM_ERROR / STREAM_REPLY are terminal. For
%% server_stream (the only mode our suite exercises today) the
%% server emits STREAM_END(role=send) and the stream is done. Bidi
%% would close on a matched pair of STREAM_END(role=send) frames;
%% a stricter implementation would wait for both. Drop on the
%% first terminal frame for now — the TTL timer catches the bidi
%% case if the second END never arrives.
maybe_close_stream(stream_end,   Sid, TRef, Streams, S) ->
    cancel_and_drop_stream(Sid, TRef, Streams, S);
maybe_close_stream(stream_error, Sid, TRef, Streams, S) ->
    cancel_and_drop_stream(Sid, TRef, Streams, S);
maybe_close_stream(stream_reply, Sid, TRef, Streams, S) ->
    cancel_and_drop_stream(Sid, TRef, Streams, S);
maybe_close_stream(_Other, _Sid, _TRef, _Streams, S) ->
    S.

cancel_and_drop_stream(Sid, TRef, Streams, S) ->
    _ = erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
    S#state{streams = maps:remove(Sid, Streams)}.

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
relay_forwarded_reply({{Origin, TRef}, NewF}, Frame, S) ->
    _ = erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
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
    %% First-write-wins — a direct daemon ADVERTISE registers for
    %% real; subsequent gossip echoes from peer stations (the same
    %% (Realm, Proc) bouncing around the partial mesh via
    %% peering_router's timer-based propagation) hit the existing
    %% entry and skip. Without this guard a gossip echo would
    %% overwrite the direct entry's daemon conn_pid with a peer-
    %% station conn_pid, and CALL forwarding would bounce around
    %% the mesh until the deadline expired instead of hitting the
    %% real handler.
    case macula_remote_advertise_registry:lookup(R, Realm, Proc) of
        {ok, _Existing} ->
            ok;
        {error, not_found} ->
            macula_remote_advertise_registry:register(
              R, Realm, Proc, #{advertiser => Adv, conn_pid => ConnPid})
    end.

on_unadvertise_match(false, _R, _Realm, _Proc) ->
    ok;
on_unadvertise_match(true, R, Realm, Proc) ->
    macula_remote_advertise_registry:unregister(R, Realm, Proc).

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
    %% subscriber's peering connection.
    on_relay_publish(
      hecate_pubsub_registry:relay_publish(Reg, Realm, Verified),
      Conns);
deliver_pubsub_typed(event, Realm, _NodeId, Verified,
                     #state{pubsub_registry = Reg, conns = Conns}) ->
    %% Inbound EVENT — typically a cross-station relay from a peer
    %% station whose pubsub_server fanned out to us as a registered
    %% subscriber. Look up our local pubsub_server, find LOCAL
    %% subscribers (daemons connected to us via SUBSCRIBE inbound),
    %% fan out the EVENT frame to each. This is the second hop of
    %% the cross-station propagation chain — without it, EVENTs
    %% arrived at our listener but never reached the daemons that
    %% subscribed for the topic.
    deliver_inbound_event(safe_lookup(Reg, Realm), Verified, Conns);
deliver_pubsub_typed(subscribe, Realm, NodeId, Verified,
                     #state{pubsub_registry = Reg}) ->
    _ = hecate_pubsub_registry:dispatch_frame(Reg, Realm, NodeId, Verified),
    %% Snap the router to a sync NOW so this fresh subscriber gets
    %% propagated to peer stations within milliseconds rather than
    %% waiting up to ?TICK_MS for the next periodic poll. The router
    %% drives subscribe-on-peer for cross-station fan-out; without
    %% this trigger, e2e probes time out before the router notices.
    notify_router_change(),
    ok;
deliver_pubsub_typed(unsubscribe, Realm, NodeId, Verified,
                     #state{pubsub_registry = Reg}) ->
    _ = hecate_pubsub_registry:dispatch_frame(Reg, Realm, NodeId, Verified),
    notify_router_change(),
    ok;
deliver_pubsub_typed(_Type, Realm, NodeId, Verified,
                     #state{pubsub_registry = Reg}) ->
    _ = hecate_pubsub_registry:dispatch_frame(Reg, Realm, NodeId, Verified),
    ok.

notify_router_change() ->
    case whereis(macula_station_peering_router) of
        undefined -> ok;
        Pid       ->
            %% Async cast so peer_observer doesn't block on router
            %% sync (sync can take seconds when fanning out to many
            %% peers). The router handles a `tick' message identically
            %% to its periodic timer.
            Pid ! tick, ok
    end.

safe_lookup(Reg, Realm) ->
    try hecate_pubsub_registry:lookup(Reg, Realm)
    catch _:_ -> {error, registry_unavailable}
    end.

deliver_inbound_event({ok, Server}, EventFrame, Conns) ->
    Matched = try hecate_pubsub_server:deliver_event(Server, EventFrame)
              catch _:_ -> []
              end,
    fan_out_event(EventFrame, Matched, Conns);
deliver_inbound_event(_Other, _EventFrame, _Conns) ->
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

%% EVENT delivery deliberately PREFERS the inbound conn — the one
%% through which the peer originally sent its SUBSCRIBE. The bytes
%% travel back to the peer's `outbound_link', whose subscriber
%% fan-out delivers `{macula_event, ...}' to local Erlang processes
%% (e.g. cross-station bloom gossip). Sending via outbound would
%% land on the peer's listener-side conn, dispatch into the peer's
%% pubsub_server, and dead-end (the peer has no local subscribers
%% there). Falls back to outbound when inbound is missing — better
%% than dropping the EVENT, even though delivery from there is
%% best-effort and may be silently lost on the peer side.
send_event_to_sub({ok, #{inbound := Pid}}, EventFrame) when is_pid(Pid) ->
    macula_peering:send_frame(Pid, EventFrame);
send_event_to_sub({ok, #{outbound := Pid}}, EventFrame) when is_pid(Pid) ->
    macula_peering:send_frame(Pid, EventFrame);
send_event_to_sub(_, _EventFrame) ->
    %% Subscriber's connection already gone — drop the EVENT.
    %% The conns map cleanup happens in `on_disconnected'.
    ok.

%%==================================================================
%% disconnected
%%==================================================================

on_disconnected(ConnPid, #state{swim = Swim, peers = P, conns = C,
                                direction_of_pid = D,
                                forwarded = F,
                                remote_advertise = R} = S) ->
    NodeId    = maps:get(ConnPid, P, undefined),
    Direction = maps:get(ConnPid, D, undefined),
    NewConns  = drop_directional_conn(NodeId, Direction, ConnPid, C),
    %% Remove the peer from SWIM only when BOTH directions are gone —
    %% a mutual-peer with one direction still alive is still reachable.
    maybe_remove_if_isolated(NodeId, NewConns, Swim),
    maybe_purge_advertise(R, ConnPid),
    S#state{peers            = maps:remove(ConnPid, P),
            conns            = NewConns,
            direction_of_pid = maps:remove(ConnPid, D),
            forwarded        = drop_forwarded_for(ConnPid, F)}.

drop_directional_conn(undefined, _Direction, _ConnPid, C) ->
    C;
drop_directional_conn(NodeId, undefined, ConnPid, C) ->
    %% Direction unknown (DOWN before any `connected' notification
    %% landed, or pre-upgrade entry from an older shape). Best-effort:
    %% scrub the pid from BOTH slots, preserving anything else.
    Existing = maps:get(NodeId, C, empty_peer_conns()),
    Cleared = maps:fold(fun(K, V, Acc) ->
        Acc#{K => clear_if_match(V, ConnPid)}
    end, #{}, Existing),
    on_cleared_peer_conns(NodeId, Cleared, C);
drop_directional_conn(NodeId, Direction, ConnPid, C) ->
    Existing = maps:get(NodeId, C, empty_peer_conns()),
    Cleared = Existing#{Direction => clear_if_match(maps:get(Direction, Existing, undefined),
                                                    ConnPid)},
    on_cleared_peer_conns(NodeId, Cleared, C).

clear_if_match(Pid, Pid)            -> undefined;
clear_if_match(Existing, _OtherPid) -> Existing.

%% Drop the NodeId entry entirely once both slots are empty so the
%% conns map doesn't leak `#{inbound => undefined, outbound => undefined}'
%% husks for every disconnected peer. ETS mirror is updated in
%% lock-step so external `conn_for/2' readers never see a stale
%% pid for a peer whose connection has died.
on_cleared_peer_conns(NodeId, #{inbound := undefined, outbound := undefined}, C) ->
    delete_conn_table(NodeId),
    maps:remove(NodeId, C);
on_cleared_peer_conns(NodeId, Cleared, C) ->
    write_conn_table(NodeId, Cleared),
    C#{NodeId => Cleared}.

maybe_remove_if_isolated(undefined, _NewConns, _Swim) ->
    ok;
maybe_remove_if_isolated(NodeId, NewConns, Swim) ->
    case maps:find(NodeId, NewConns) of
        error                                                            -> maybe_remove(NodeId, Swim);
        {ok, #{inbound := undefined, outbound := undefined}}             -> maybe_remove(NodeId, Swim);
        _                                                                -> ok
    end.

%% Drop in-flight forwarded entries whose origin (or advertiser, for
%% bulk-purge by either endpoint) has just gone. Cancels TTL timers
%% on the entries being dropped so we don't accumulate stale
%% `forwarded_timeout' messages in our mailbox.
drop_forwarded_for(ConnPid, F) ->
    maps:fold(fun(CallId, {Origin, TRef}, Acc) ->
        keep_or_drop(Origin =:= ConnPid, CallId, Origin, TRef, Acc)
    end, #{}, F).

keep_or_drop(true, _CallId, _Origin, TRef, Acc) ->
    _ = erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
    Acc;
keep_or_drop(false, CallId, Origin, TRef, Acc) ->
    Acc#{CallId => {Origin, TRef}}.

maybe_purge_advertise(undefined, _ConnPid) ->
    ok;
maybe_purge_advertise(R, ConnPid) ->
    macula_remote_advertise_registry:purge_conn(R, ConnPid).

maybe_remove(undefined, _Swim) -> ok;
maybe_remove(NodeId,     Swim) -> macula_swim:remove_peer(Swim, NodeId).

