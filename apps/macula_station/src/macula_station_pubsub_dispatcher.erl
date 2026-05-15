%% @doc Pubsub frame dispatcher — receives `subscribe', `unsubscribe',
%% `publish' and `event' frames directly from `macula_peering_conn'
%% (SDK >= 4.4.4 `pubsub_recipient' opt), bypassing
%% `macula_station_peer_observer's mailbox.
%%
%% == Why it exists ==
%%
%% Before 4.4.4 every pubsub frame queued in `peer_observer's
%% gen_server mailbox alongside ADVERTISE / CALL / REPLY / DHT and
%% connection-lifecycle work. Each EVENT carries an Ed25519
%% `publisher_sig' that the relay verifies (~200 µs each) before
%% fanning out, and the multi-publisher e2e cases fire bursts of
%% dozens of EVENTs at once; the observer's mailbox stayed 5-10 k
%% deep through those bursts after the DHT bypass (4.4.3) removed
%% the `store'/`store_ack' chatter. This module owns its own mailbox
%% so EVENT bursts no longer back up the observer's queue, and the
%% observer's frame-dispatch latency stays bounded.
%%
%% == What it does NOT touch ==
%%
%% Connection state, ADVERTISE / CALL / REPLY / STREAM / DHT / SWIM
%% routing — all still owned by `macula_station_peer_observer'. This
%% module is wire-protocol-narrow: pubsub frames in, pubsub work out.
%%
%% == Why a gen_server (not spawn-per-frame) ==
%%
%% Frame ordering matters within a single peering connection — a
%% PUBLISH followed by an UNSUBSCRIBE must process in that order, or
%% the EVENT for the publish lands at a peer whose registry already
%% forgot the subscriber. A gen_server's FIFO mailbox preserves order
%% within a single sender; spawning a worker per frame would not.
%%
%% The same applies to (publisher, seq) dedup: the cache writes must
%% be strictly serialised against the frame stream from each peer.
%%
%% == Conns lookup ==
%%
%% Fan-out (`fan_out_event/3') reads the (NodeId → conn) map from the
%% peer_observer's public ETS mirror
%% (`macula_station_peer_observer_conns'). This is the same lookup
%% path the router uses after the ETS bypass in commit d0f0c8a; cost
%% is microseconds vs the seconds the older `sys:get_state(observer)'
%% path took under load.
-module(macula_station_pubsub_dispatcher).
-behaviour(gen_server).

-export([start_link/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type opts() :: #{
    pubsub_registry => pid() | undefined,
    conns_table     => atom()
}.

-export_type([opts/0]).

-define(CONNS_TABLE, macula_station_peer_observer_conns).

-record(state, {
    pubsub_registry :: pid() | undefined,
    conns_table     :: atom()
}).

%%==================================================================
%% API
%%==================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%%==================================================================
%% gen_server
%%==================================================================

init(Opts) ->
    State = #state{
        pubsub_registry = maps:get(pubsub_registry, Opts, undefined),
        conns_table     = maps:get(conns_table, Opts, ?CONNS_TABLE)
    },
    {ok, State}.

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({macula_peering, pubsub_frame, _ConnPid, NodeId, Frame},
            #state{pubsub_registry = Reg} = S)
        when is_binary(NodeId), byte_size(NodeId) =:= 32,
             is_map(Frame), Reg =/= undefined ->
    Type = macula_frame:frame_type(Frame),
    T0 = erlang:monotonic_time(microsecond),
    handle_pubsub_frame(verify_pubsub(Frame, NodeId), NodeId, Frame, S),
    T1 = erlang:monotonic_time(microsecond),
    macula_station_frame_telemetry:record(Type, dispatch_self, T1 - T0),
    {noreply, S};
%% 6-tuple variant — peering_conn with `timing_enabled=true' (macula
%% >= 4.4.7) stamps `RecvAtUs' at decode-time. Compute mailbox wait
%% first, then time the dispatch body separately.
handle_info({macula_peering, pubsub_frame, _ConnPid, NodeId, Frame, RecvAtUs},
            #state{pubsub_registry = Reg} = S)
        when is_binary(NodeId), byte_size(NodeId) =:= 32,
             is_map(Frame), Reg =/= undefined ->
    Type = macula_frame:frame_type(Frame),
    T0 = erlang:monotonic_time(microsecond),
    macula_station_frame_telemetry:record(Type, recv_to_dispatch,
                                          T0 - RecvAtUs),
    handle_pubsub_frame(verify_pubsub(Frame, NodeId), NodeId, Frame, S),
    T1 = erlang:monotonic_time(microsecond),
    macula_station_frame_telemetry:record(Type, dispatch_self, T1 - T0),
    {noreply, S};
handle_info({macula_peering, pubsub_frame, _ConnPid, _NodeId, _Frame}, S) ->
    %% No registry yet — drop. peer_observer used to log a warning
    %% here; we keep the behaviour quiet because this branch only
    %% fires for a brief boot window or in tests that don't wire
    %% the registry.
    {noreply, S};
handle_info({macula_peering, pubsub_frame, _ConnPid, _NodeId, _Frame, _T}, S) ->
    %% Same fall-through for the 6-tuple shape.
    {noreply, S};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.
code_change(_Old, S, _Extra) -> {ok, S}.

%%==================================================================
%% Dispatch
%%==================================================================

handle_pubsub_frame({error, Reason}, NodeId, Frame, _S) ->
    logger:warning(
      "[pubsub_dispatcher] verify failed: ~p type=~p topic=~s "
      "realm=~s peer_node_id=~s publisher=~s has_publisher_sig=~p",
      [Reason,
       macula_frame:frame_type(Frame),
       maps:get(topic, Frame, <<>>),
       short_hex(maps:get(realm, Frame, <<>>)),
       short_hex(NodeId),
       short_hex(maps:get(publisher, Frame, <<>>)),
       maps:is_key(publisher_sig, Frame)]),
    ok;
handle_pubsub_frame({ok, Verified}, NodeId, _Frame,
                    #state{pubsub_registry = Reg,
                           conns_table     = CT}) ->
    Realm = maps:get(realm, Verified),
    Topic = maps:get(topic, Verified, undefined),
    Type  = macula_frame:frame_type(Verified),
    logger:debug("[pubsub_dispatcher] ~s realm=~p topic=~s",
                 [Type, Realm, Topic]),
    deliver_typed(Type, Realm, NodeId, Verified, Reg, CT).

%% PUBLISH from a remote daemon. Seed (publisher, seq) at the origin
%% so a looped-back EVENT for the same publish is recognised as a
%% duplicate; then build the EVENT frame in the realm's pubsub_server
%% and fan out to each matched local subscriber's peering connection,
%% plus to direct outbound peers whose Bloom filter matches the topic
%% (publisher-side bloom-fan, see `bloom_fan_extras/3').
deliver_typed(publish, Realm, NodeId, Verified, Reg, CT) ->
    record_origin_seq(Verified),
    on_relay_publish(
      hecate_pubsub_registry:relay_publish(Reg, Realm, Verified),
      NodeId, CT);

%% Inbound EVENT — cross-station relay. Find local subscribers and
%% fan out. The (publisher, seq) dedup cache is the loop-kill for
%% publisher-signed EVENTs (they verify end-to-end at every hop, so
%% the older verify-fail accident no longer bounds them). A repeat
%% without a publisher_sig is logged but still delivered — those are
%% still bounded by the verify-fail mismatch one hop out.
%%
%% After local delivery, also bloom-fan to direct outbound peers
%% whose filter matches the topic — minus the source peer
%% (`NodeId') to avoid immediate echo. `event_dedup' at every
%% receiver kills further loops.
deliver_typed(event, Realm, NodeId, Verified, Reg, CT) ->
    case event_dedup_disposition(Verified) of
        drop ->
            ok;
        deliver ->
            deliver_inbound_event(safe_lookup(Reg, Realm), Verified,
                                  NodeId, CT)
    end;

deliver_typed(subscribe, Realm, NodeId, Verified, Reg, _CT) ->
    _ = hecate_pubsub_registry:dispatch_frame(Reg, Realm, NodeId, Verified),
    %% Snap the router to a sync NOW so this fresh subscriber gets
    %% propagated to peer stations within milliseconds rather than
    %% waiting up to ?TICK_MS for the next periodic poll. Same
    %% rationale as the peer_observer path used to have — without
    %% this trigger, e2e probes time out before the router notices.
    notify_router_change(),
    %% Also nudge the bloom-exchange: the local topic set just gained
    %% an entry, so our outgoing Bloom should pick it up on the
    %% debounce (~2s) instead of the periodic 30s tick.
    notify_bloom_change(),
    ok;

deliver_typed(unsubscribe, Realm, NodeId, Verified, Reg, _CT) ->
    _ = hecate_pubsub_registry:dispatch_frame(Reg, Realm, NodeId, Verified),
    notify_router_change(),
    notify_bloom_change(),
    ok;

deliver_typed(_Other, Realm, NodeId, Verified, Reg, _CT) ->
    _ = hecate_pubsub_registry:dispatch_frame(Reg, Realm, NodeId, Verified),
    ok.

%%==================================================================
%% Helpers (lifted from macula_station_peer_observer; keep parity)
%%==================================================================

%% An EVENT carrying a publisher-end-to-end signature is verified
%% against the publisher (so it passes at any relay hop, not just
%% one); loop prevention falls to the (publisher, seq) dedup cache.
%% Everything else — SUBSCRIBE / UNSUBSCRIBE / PUBLISH, and EVENTs
%% from a daemon not yet emitting `publisher_sig' — is verified
%% against the connection's NodeId.
verify_pubsub(#{frame_type := event, publisher_sig := _} = Frame, _NodeId) ->
    macula_frame:verify_publisher(Frame);
verify_pubsub(Frame, NodeId) ->
    macula_frame:verify(Frame, NodeId).

safe_lookup(Reg, Realm) ->
    try hecate_pubsub_registry:lookup(Reg, Realm)
    catch _:_ -> {error, registry_unavailable}
    end.

deliver_inbound_event({ok, Server}, EventFrame, SourceNodeId, CT) ->
    Matched = try hecate_pubsub_server:deliver_event(Server, EventFrame)
              catch _:_ -> []
              end,
    fan_out_event(EventFrame,
                  Matched ++ bloom_fan_extras(EventFrame, Matched,
                                              [SourceNodeId], CT),
                  CT);
deliver_inbound_event(_Other, EventFrame, SourceNodeId, CT) ->
    %% No pubsub_server materialised here — still useful to bloom-fan,
    %% since we may sit on the gossip path between a publisher we have
    %% no chain from and downstream subscribers.
    fan_out_event(EventFrame,
                  bloom_fan_extras(EventFrame, [],
                                   [SourceNodeId], CT), CT).

on_relay_publish({ok, EventFrame, Matched}, _SourceNodeId, CT) ->
    %% Origin station — no source peer to exclude (the PUBLISH came
    %% from a locally-connected daemon, never in `peer_blooms').
    fan_out_event(EventFrame,
                  Matched ++ bloom_fan_extras(EventFrame, Matched, [], CT),
                  CT);
on_relay_publish({error, _Reason}, _SourceNodeId, _CT) ->
    ok.

%% Compute the bloom-fan extras for an EVENT frame: peer NodeIds
%% whose Bloom matches the topic, minus those already in `Matched'
%% (subscribe-on-peer chain), minus any in `Excluded' (the source
%% peer on a relay hop), minus peers we have no live conn to. The
%% conn intersection is critical: `peer_blooms' is transitively
%% merged, so it can name stations we don't directly peer with.
%%
%% Mesh-internal topics (`_mesh.*') are skipped — `bloom_exchange'
%% already broadcasts them directly; a second fan would mess with
%% the gossip cadence (same reason `peering_router' filters them).
bloom_fan_extras(EventFrame, Matched, Excluded, CT) ->
    case maps:get(topic, EventFrame, undefined) of
        undefined -> [];
        <<"_mesh.", _/binary>> -> [];
        Topic when is_binary(Topic) ->
            bloom_fan_extras_for_topic(Topic, Matched, Excluded, CT)
    end.

bloom_fan_extras_for_topic(Topic, Matched, Excluded, CT) ->
    %% ETS-bypass: read the peer_blooms mirror directly. Avoids the
    %% per-event `gen_server:call' to bloom_exchange that would
    %% serialise the entire station's pubsub fan-out path under
    %% sustained load (torture observed pubsub_dispatcher mailbox
    %% backing up to 25k+ when this used the gen_server path).
    Candidates = macula_station_bloom_exchange:peer_matches_ets(Topic),
    filter_fan_candidates(Candidates, Matched, Excluded, CT).

filter_fan_candidates(Candidates, Matched, Excluded, CT) ->
    Skip = sets:from_list(Matched ++ Excluded),
    [NodeId
     || NodeId <- Candidates,
        not sets:is_element(NodeId, Skip),
        has_live_conn(CT, NodeId)].

%% A peer NodeId is fan-eligible only if we hold a peering conn to
%% it. The conns table is owned by `peer_observer'; checking it via
%% ETS is microseconds and keeps the dispatcher off the observer's
%% mailbox.
has_live_conn(CT, NodeId) ->
    try ets:lookup(CT, NodeId) of
        [{_, _PeerConns}] -> true;
        []                -> false
    catch
        _:_ -> false
    end.

%% Fan-out walks the matched subscriber list and looks each one up in
%% the conns ETS table directly. Previously this function received a
%% pre-materialised map, but rebuilding that map for EVERY inbound
%% event (via `ets:tab2list/1' + `maps:from_list/1' over a 40-entry
%% table) added milliseconds-per-event of overhead — under the
%% bypass the dispatcher mailbox climbed to 365k as a result.
%% Per-NodeId `ets:lookup/2' is O(1) and ~µs.
fan_out_event(_EventFrame, [], _CT) ->
    ok;
fan_out_event(EventFrame, [Sub | Rest], CT) ->
    send_event_to_sub(ets_lookup_conn(CT, Sub), EventFrame),
    fan_out_event(EventFrame, Rest, CT).

ets_lookup_conn(CT, NodeId) ->
    try ets:lookup(CT, NodeId) of
        [{_, PeerConns}] -> {ok, PeerConns};
        []               -> error
    catch
        _:_ -> error
    end.

%% EVENT delivery PREFERS the inbound conn — the one the peer originally
%% sent its SUBSCRIBE through. Sending via outbound would land on the
%% peer's listener-side conn and dead-end (the peer has no subscribers
%% there). Falls back to outbound when inbound is missing.
send_event_to_sub({ok, #{inbound := Pid}}, EventFrame) when is_pid(Pid) ->
    macula_peering:send_frame(Pid, EventFrame);
send_event_to_sub({ok, #{outbound := Pid}}, EventFrame) when is_pid(Pid) ->
    macula_peering:send_frame(Pid, EventFrame);
send_event_to_sub(_, _EventFrame) ->
    ok.

%% (publisher, seq) dedup — see macula_station_peer_observer's notes
%% (Phase 2 step 2/3). Decides per-EVENT whether to deliver or drop
%% as a loop-back. Tolerates the cache being momentarily down (boot
%% transient) and frames missing publisher/seq.
event_dedup_disposition(#{publisher := Pub, seq := Seq} = V)
  when is_binary(Pub), is_integer(Seq) ->
    classify_event_dup(macula_station_event_dedup:seen_or_record(Pub, Seq), V);
event_dedup_disposition(_V) ->
    deliver.

classify_event_dup(new, _V) ->
    deliver;
classify_event_dup(duplicate, #{publisher_sig := _} = V) ->
    logger:debug("[event_dedup] dropped loop-back EVENT publisher=~s seq=~p topic=~s",
                 [short_hex(maps:get(publisher, V)), maps:get(seq, V),
                  maps:get(topic, V, <<>>)]),
    drop;
classify_event_dup(duplicate, V) ->
    logger:debug("[event_dedup] repeat EVENT publisher=~s seq=~p topic=~s"
                 " (no publisher_sig — still delivered)",
                 [short_hex(maps:get(publisher, V)), maps:get(seq, V),
                  maps:get(topic, V, <<>>)]),
    deliver.

%% Seed the dedup cache at the origin station so a looped-back EVENT
%% for the same publish is recognised as a duplicate here too.
record_origin_seq(#{publisher := Pub, seq := Seq})
  when is_binary(Pub), is_integer(Seq) ->
    _ = macula_station_event_dedup:seen_or_record(Pub, Seq),
    ok;
record_origin_seq(_Verified) ->
    ok.

notify_router_change() ->
    case whereis(macula_station_peering_router) of
        undefined -> ok;
        Pid       ->
            %% Async send so the dispatcher doesn't block on router
            %% sync (which can take seconds when fanning to many
            %% peers). The router handles a `tick' message identically
            %% to its periodic timer.
            Pid ! tick, ok
    end.

notify_bloom_change() ->
    case whereis(macula_station_bloom_exchange) of
        undefined -> ok;
        Pid       -> macula_station_bloom_exchange:notify_local_change(Pid)
    end.

short_hex(B) when is_binary(B), byte_size(B) > 0 ->
    binary:encode_hex(binary:part(B, 0, min(8, byte_size(B))));
short_hex(_) ->
    <<"?">>.
