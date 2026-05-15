%%% @doc Singleton Bloom filter exchange.
%%%
%%% Ported from V1 `macula_relay_bloom_exchange'. Owns the local
%%% Bloom filter (rebuilt from the station's pubsub_server topics)
%%% and tracks peer filters received via gossip on the
%%% `_mesh.bloom' topic.
%%%
%%% On a 30s tick the manager rebuilds its local filter from
%%% `hecate_pubsub_server:topics/1' and broadcasts the 1KB binary to
%%% every connected peer station via `macula_station_peer_links'.
%%% Peers' inbound Bloom-event handler calls `receive_peer_bloom/3'
%%% which we cache.
%%%
%%% The peering forwarder consults `peer_blooms/1' to skip publishes
%%% to peers whose filter doesn't match — preventing the cross-relay
%%% flooding that would otherwise hit O(N^2) topics × peers.
-module(macula_station_bloom_exchange).
-behaviour(gen_server).

-export([start_link/1, stop/1]).
-export([rebuild_and_broadcast/1, receive_peer_bloom/3,
         notify_local_change/1,
         get_local_bloom/1, peer_blooms/1, peer_matches/2,
         peer_matches_ets/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(REBUILD_INTERVAL_MS, 30_000).

%% Public ETS table mirroring `peer_blooms'. Read-only consumers
%% (notably `macula_station_pubsub_dispatcher's bloom-fan path) use
%% this directly to avoid serialising every inbound EVENT through
%% bloom_exchange's mailbox — a `gen_server:call' per event creates
%% a per-station bottleneck under sustained pubsub load. Same
%% pattern as `macula_station_peer_observer_conns' (commit d0f0c8a).
-define(PEER_BLOOMS_TABLE, macula_station_peer_blooms).

%% Push-on-change debounce. When a peer's bloom CHANGES (new publisher
%% or a different filter from the same publisher), schedule an
%% out-of-band rebuild within ?DEBOUNCE_MS instead of waiting up to
%% ?REBUILD_INTERVAL_MS for the periodic tick. Drops first-delivery
%% latency for multi-hop pubsub from ~3 * REBUILD_INTERVAL (≈90s on a
%% 4-hop mesh) to ~3 * DEBOUNCE (≈6s). Debounce coalesces bursts of
%% peer_bloom updates into one rebuild.
-define(DEBOUNCE_MS, 2_000).

%% Mesh-level events (including bloom gossip) live in the all-zeros
%% realm — protocol infrastructure, not bound to any business realm.
-define(MESH_REALM, <<0:256>>).

-type opts() :: #{
    pubsub_registry := pid(),
    identity        := macula_identity:key_pair()
}.

-export_type([opts/0]).

-record(state, {
    pubsub_registry :: pid(),
    identity        :: macula_identity:key_pair(),
    local_bloom     :: binary(),
    peer_blooms     :: #{binary() => binary()},
    %% Active subscriptions on each peer's station_link for inbound
    %% `_mesh.bloom' events. Keyed by peer hostname so we don't
    %% double-subscribe when peer_links reports the same connection
    %% twice across resync ticks.
    subs            :: #{binary() => {pid(), reference()}},
    timer_ref       :: reference() | undefined,
    %% Token for an in-flight debounced rebuild. `undefined' means no
    %% rebuild is pending; any binding means a `{debounced_rebuild,
    %% Ref}' message is already in flight and further peer_bloom
    %% changes coalesce into it.
    debounce_ref    :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{pubsub_registry := _, identity := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Force an immediate rebuild + broadcast cycle (test hook).
-spec rebuild_and_broadcast(pid()) -> ok.
rebuild_and_broadcast(Pid) ->
    gen_server:cast(Pid, rebuild_and_broadcast).

%% @doc Cache an incoming peer Bloom filter. Called by whatever
%% handler routes inbound `_mesh.bloom' events to this manager.
-spec receive_peer_bloom(pid(), binary(), binary()) -> ok.
receive_peer_bloom(Pid, PeerHostname, BloomBin)
  when is_binary(PeerHostname), is_binary(BloomBin) ->
    gen_server:cast(Pid, {peer_bloom, PeerHostname, BloomBin}).

%% @doc Signal that the LOCAL pubsub topic set may have changed
%% (e.g. an inbound SUBSCRIBE / UNSUBSCRIBE frame registered or
%% removed a topic on a local pubsub_server). Schedules a debounced
%% rebuild so the next outgoing Bloom reflects the new topic set
%% within `?DEBOUNCE_MS' instead of waiting up to
%% `?REBUILD_INTERVAL_MS' for the periodic tick. Idempotent and
%% coalescing.
-spec notify_local_change(pid()) -> ok.
notify_local_change(Pid) ->
    gen_server:cast(Pid, local_change).

%% @doc Snapshot of the current local filter.
-spec get_local_bloom(pid()) -> binary().
get_local_bloom(Pid) ->
    try gen_server:call(Pid, get_local_bloom, 500)
    catch _:_ -> macula_station_bloom:to_binary(macula_station_bloom:new())
    end.

%% @doc Snapshot of all known peer filters.
-spec peer_blooms(pid()) -> #{binary() => binary()}.
peer_blooms(Pid) ->
    try gen_server:call(Pid, peer_blooms, 500)
    catch _:_ -> #{}
    end.

%% @doc List of peer NodeIds (32-byte pubkeys, = the `_mesh.bloom'
%% publisher key) whose Bloom filter matches `Topic'. Drives the
%% pubsub forwarder's bloom-fan to peers without an explicit
%% subscribe-on-peer chain. Peers with no known filter (haven't
%% gossipped yet) are NOT included — callers should treat absence
%% as "unknown, don't forward yet".
%%
%% Note: due to `merge_with_peers/2' (transitive bloom), a returned
%% NodeId may belong to a station that is not directly connected to
%% us — its bloom reached us through an intermediary's outbound
%% merge. Callers MUST intersect the result with their direct-peer
%% conn set before sending; an EVENT to a non-direct NodeId has no
%% conn to land on.
-spec peer_matches(pid(), binary()) -> [<<_:256>>].
peer_matches(Pid, Topic) when is_binary(Topic) ->
    try gen_server:call(Pid, {peer_matches, Topic}, 500)
    catch _:_ -> []
    end.

%% @doc ETS-bypass variant of `peer_matches/2'. Reads the
%% `?PEER_BLOOMS_TABLE' mirror directly — no `gen_server:call', no
%% serialisation through `bloom_exchange's mailbox. Use this on the
%% pubsub hot path; the gen_server variant is for tests + status
%% pages where the lookup rate is bounded.
%%
%% Returns `[]' when the table doesn't exist (boot edge / tests
%% that don't start `bloom_exchange').
-spec peer_matches_ets(binary()) -> [<<_:256>>].
peer_matches_ets(Topic) when is_binary(Topic) ->
    try ets:tab2list(?PEER_BLOOMS_TABLE) of
        Entries ->
            [NodeId
             || {NodeId, BloomBin} <- Entries,
                byte_size(BloomBin) =:= 1024,
                macula_station_bloom:check(
                  Topic, macula_station_bloom:from_binary(BloomBin))]
    catch
        error:badarg -> []
    end.

%%====================================================================
%% gen_server
%%====================================================================

init(#{pubsub_registry := Reg, identity := Kp}) ->
    process_flag(trap_exit, true),
    ensure_peer_blooms_table(),
    State = #state{
        pubsub_registry = Reg,
        identity        = Kp,
        local_bloom     = empty_bloom_bin(),
        peer_blooms     = #{},
        subs            = #{},
        debounce_ref    = undefined
    },
    {ok, schedule_rebuild(State)}.

%% Idempotent: a previous bloom_exchange instance owned the table
%% via `named_table'; on its exit ETS deleted it; we recreate.
%% `read_concurrency=true' biases the table for parallel reads from
%% the dispatcher hot path.
ensure_peer_blooms_table() ->
    case ets:whereis(?PEER_BLOOMS_TABLE) of
        undefined ->
            ets:new(?PEER_BLOOMS_TABLE,
                    [named_table, public, set, {read_concurrency, true}]);
        _ ->
            ?PEER_BLOOMS_TABLE
    end.

handle_call(get_local_bloom, _From, #state{local_bloom = LB} = S) ->
    {reply, LB, S};
handle_call(peer_blooms, _From, #state{peer_blooms = PB} = S) ->
    {reply, PB, S};
handle_call({peer_matches, Topic}, _From, #state{peer_blooms = PB} = S) ->
    %% `peer_blooms' is keyed by the EVENT publisher (= station
    %% NodeId), set in `record_peer_bloom/4'. The returned list is
    %% NodeIds, not hostnames — the name predates the publisher-keying
    %% switch and is preserved for API stability.
    Matches = [NodeId
               || {NodeId, BloomBin} <- maps:to_list(PB),
                  byte_size(BloomBin) =:= 1024,
                  macula_station_bloom:check(
                    Topic, macula_station_bloom:from_binary(BloomBin))],
    {reply, Matches, S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(rebuild_and_broadcast, S) ->
    {noreply, do_rebuild(S)};
handle_cast({peer_bloom, Host, BloomBin}, #state{peer_blooms = PB} = S)
  when byte_size(BloomBin) =:= 1024 ->
    {noreply, record_peer_bloom(Host, BloomBin, PB, S)};
handle_cast({peer_bloom, _, _}, S) ->
    {noreply, S};
handle_cast(local_change, S) ->
    %% Cheap to schedule even if topics haven't actually changed
    %% — the rebuild reads pubsub_registry which is the source of
    %% truth, and the debounce coalesces a burst of subscribe /
    %% unsubscribe frames into a single rebuild.
    {noreply, schedule_debounced_rebuild(S)};
handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({rebuild, Ref}, #state{timer_ref = Ref} = S) ->
    S1 = sync_inbound_subs(do_rebuild(S)),
    %% Periodic rebuild satisfies any pending debounce — queued
    %% `{debounced_rebuild, OldRef}' message arrives later but won't
    %% match the cleared `debounce_ref' field and is dropped.
    S2 = S1#state{debounce_ref = undefined},
    {noreply, schedule_rebuild(S2)};
handle_info({debounced_rebuild, Ref}, #state{debounce_ref = Ref} = S) ->
    %% Debounced rebuild fires. Skip `sync_inbound_subs/1' — only the
    %% periodic tick manages subscriptions, the debounce just refreshes
    %% the outgoing bloom so freshly-received peer interest propagates
    %% to OUR peers without waiting up to 30s for the next periodic.
    S1 = do_rebuild(S),
    {noreply, S1#state{debounce_ref = undefined}};
handle_info({debounced_rebuild, _Stale}, S) ->
    %% Token mismatch — either a periodic rebuild already cleared the
    %% ref, or the field was reset. Drop the stale tick.
    {noreply, S};
%% Inbound EVENT frame for `_mesh.bloom' — outbound_link (and the SDK
%% station_link) deliver events as
%% `{macula_event, SubRef, Topic, Payload, Meta}'.
%%
%% Key the cache by `Meta.publisher` (the originator's pubkey), NOT
%% by SubRef→Host. EVENTs traverse fan-out chains through intermediate
%% stations: a PUBLISH from B sent on B's outbound to A will fan out
%% on A's pubsub_server to every subscriber on A's server (including
%% C, D, ...). C receives the EVENT but the publisher field is B, not
%% A. Keying by SubRef→Host would record `peer_blooms[A] = B's bloom',
%% which is wrong; the publisher is the right cache key. Self-echoes
%% (where publisher = our own identity) are dropped.
handle_info({macula_event, _SubRef, <<"_mesh.bloom">>, Payload,
             #{publisher := Publisher}},
            #state{identity = Kp, peer_blooms = PB} = S)
  when is_binary(Payload), byte_size(Payload) =:= 1024,
       is_binary(Publisher), byte_size(Publisher) =:= 32 ->
    case Publisher =:= macula_identity:public(Kp) of
        true  -> {noreply, S};  % self-echo from a peer's fan-out — ignore
        false -> {noreply, record_peer_bloom(Publisher, Payload, PB, S)}
    end;
handle_info({macula_event, _SubRef, <<"_mesh.bloom">>, _Payload, _Meta}, S) ->
    %% Missing publisher field, wrong-sized payload, or other malformed
    %% event — drop silently.
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Rebuild + broadcast
%%====================================================================

do_rebuild(#state{pubsub_registry = Reg, peer_blooms = PB} = S) ->
    %% Local bloom = topics this station's pubsub_registry holds
    %% subscribers for. Direct interest only.
    Topics = safe_topics(Reg),
    LocalBF = lists:foldl(fun macula_station_bloom:add/2,
                          macula_station_bloom:new(), Topics),
    LocalBin = macula_station_bloom:to_binary(LocalBF),
    %% Transitive bloom = local OR every peer's bloom. Lets topic
    %% interest propagate beyond direct peers without an explicit
    %% subscribe-gossip protocol. Loops are caught at EVENT delivery
    %% by `macula_station_event_dedup' (`(publisher, seq)' cache,
    %% Phase 2). Peer X receiving a transitive bloom that includes
    %% X's own contribution would just forward an EVENT we have
    %% already seen; dedup drops it. Bandwidth cost is one extra
    %% EVENT per loop edge, bounded by mesh diameter and tick
    %% interval. Convergence: log(diameter) ticks for full closure
    %% — ~30-90s on the 10-station Leuven mesh; acceptable.
    OutgoingBin = merge_with_peers(LocalBin, PB),
    broadcast_filter(OutgoingBin),
    %% Diagnostics: also broadcast the LOCAL-only bloom on a sibling
    %% topic. Realm-side observers can compare local vs outgoing to
    %% spot stations that lag the transitive merge (a peer's bloom
    %% arrived late) or that carry interest no other station has
    %% (the "I subscribed here, watch it propagate" demo). Bandwidth
    %% is +1KB per rebuild per station — negligible.
    broadcast_local_filter(LocalBin),
    S#state{local_bloom = LocalBin}.

%% Bitwise-OR the local bloom with every peer bloom we've cached.
%% Both inputs are 1024-byte filters; `macula_station_bloom:merge/2'
%% is just an XOR-free union (binary OR).
merge_with_peers(LocalBin, PeerBlooms) when map_size(PeerBlooms) =:= 0 ->
    LocalBin;
merge_with_peers(LocalBin, PeerBlooms) ->
    Local = macula_station_bloom:from_binary(LocalBin),
    Merged = maps:fold(
        fun(_Pub, Bin, Acc) when is_binary(Bin), byte_size(Bin) =:= 1024 ->
                macula_station_bloom:merge(
                    Acc, macula_station_bloom:from_binary(Bin));
           (_Pub, _BadBin, Acc) ->
                Acc
        end, Local, PeerBlooms),
    macula_station_bloom:to_binary(Merged).

%% Union of every locally-registered realm's topic set. The bloom is
%% used by the cross-station forwarder to decide "does this peer care
%% about a topic I'm about to publish on?" — that decision is realm-
%% blind on the wire (the bloom carries topic strings only), so the
%% bloom must include every topic ANY local realm has subscribers for.
%% Pre-Gap-B versions only queried the mesh-realm server, which left
%% the bloom empty for user-realm pubsub and made the forwarder skip
%% every interested peer.
%%
%% Tolerates registry-down / per-server failures by emitting fewer
%% topics that tick — the next rebuild reconciles.
safe_topics(Reg) ->
    Realms = try hecate_pubsub_registry:list_realms(Reg)
             catch _:_ -> []
             end,
    lists:flatmap(fun(Realm) -> topics_for_realm(Reg, Realm) end, Realms).

topics_for_realm(Reg, Realm) ->
    try hecate_pubsub_registry:lookup(Reg, Realm) of
        {ok, Server} ->
            try hecate_pubsub_server:topics(Server)
            catch _:_ -> []
            end;
        _ -> []
    catch _:_ -> []
    end.

%% Broadcast `_mesh.bloom' to every active outbound station_link.
%% No-op when no live connections — peer caches are updated on the
%% next rebuild tick once outbound dialers reconnect.
broadcast_filter(BloomBin) ->
    Conns = macula_station_peer_links:connections(),
    lists:foreach(
      fun({_Url, LinkPid}) ->
              catch macula_station_link:publish(
                      LinkPid, ?MESH_REALM, <<"_mesh.bloom">>, BloomBin)
      end,
      Conns),
    ok.

%% Broadcast the LOCAL-only filter on a diagnostic sibling topic.
%% Realm-side dashboards use it to render the local-vs-outgoing
%% comparison; peering forwarders MUST NOT consume this topic for
%% routing — only `_mesh.bloom' (transitive) drives interest.
broadcast_local_filter(LocalBin) ->
    Conns = macula_station_peer_links:connections(),
    lists:foreach(
      fun({_Url, LinkPid}) ->
              catch macula_station_link:publish(
                      LinkPid, ?MESH_REALM, <<"_mesh.bloom.local">>, LocalBin)
      end,
      Conns),
    ok.

empty_bloom_bin() ->
    macula_station_bloom:to_binary(macula_station_bloom:new()).

%% Update `peer_blooms[Key] = BloomBin'. If the value actually changed
%% (new entry or different bytes for an existing publisher), schedule
%% a debounced rebuild so the new transitive interest propagates to
%% our peers within ?DEBOUNCE_MS instead of waiting up to
%% ?REBUILD_INTERVAL_MS. Always mirror to the public ETS table so
%% the dispatcher hot path sees the latest bytes within the same
%% wall-clock tick.
record_peer_bloom(Key, BloomBin, PB, S) ->
    Changed = maps:get(Key, PB, undefined) =/= BloomBin,
    catch ets:insert(?PEER_BLOOMS_TABLE, {Key, BloomBin}),
    S1 = S#state{peer_blooms = PB#{Key => BloomBin}},
    case Changed of
        true  -> schedule_debounced_rebuild(S1);
        false -> S1
    end.

%% Idempotent: a second call while a debounce is already scheduled is
%% a no-op (the coalescing behaviour). The handler clears the field on
%% fire, and the periodic rebuild also clears it.
schedule_debounced_rebuild(#state{debounce_ref = undefined} = S) ->
    Ref = make_ref(),
    erlang:send_after(?DEBOUNCE_MS, self(), {debounced_rebuild, Ref}),
    S#state{debounce_ref = Ref};
schedule_debounced_rebuild(S) ->
    S.

%%====================================================================
%% Inbound subscription management
%%====================================================================

%% Each rebuild tick: ensure we have an active `_mesh.bloom'
%% subscription on every peer's station_link. Drops subs for peers
%% no longer reported as connected. Idempotent — re-subscribing on an
%% already-subscribed link is rare (peer_links reports stable
%% connection lists between disconnect events) and tolerable.
sync_inbound_subs(#state{subs = Subs} = S) ->
    Conns   = macula_station_peer_links:connections(),
    Active  = [{hostname_of(Url), LinkPid} || {Url, LinkPid} <- Conns],
    Subs1   = drop_stale_subs(Subs, Active),
    Subs2   = subscribe_new_peers(Subs1, Active),
    S#state{subs = Subs2}.

drop_stale_subs(Subs, Active) ->
    ActiveHosts = sets:from_list([H || {H, _} <- Active]),
    maps:filter(
      fun(Host, {LinkPid, SubRef}) ->
              case sets:is_element(Host, ActiveHosts) of
                  true -> true;
                  false ->
                      catch macula_station_link:unsubscribe(LinkPid, SubRef),
                      false
              end
      end, Subs).

subscribe_new_peers(Subs, Active) ->
    lists:foldl(
      fun({Host, LinkPid}, Acc) ->
              case maps:is_key(Host, Acc) of
                  true  -> Acc;
                  false -> subscribe_one(Acc, Host, LinkPid)
              end
      end, Subs, Active).

%% LinkPid may be a `macula_station_link' SDK client (handles
%% subscribe) OR a `macula_station_outbound_link' worker (does
%% not — returns `{error, unknown_call}'). The wildcard `of'
%% clause is mandatory: `try_clause' from a missing `of' pattern
%% is NOT caught by the `catch' below.
subscribe_one(Acc, Host, LinkPid) ->
    try macula_station_link:subscribe(LinkPid, ?MESH_REALM,
                                       <<"_mesh.bloom">>, self()) of
        {ok, SubRef} -> Acc#{Host => {LinkPid, SubRef}};
        _Other       -> Acc
    catch _:_ -> Acc
    end.

hostname_of(<<"quic://", Rest/binary>>) -> strip_port(Rest);
hostname_of(<<"https://", Rest/binary>>) -> strip_port(Rest);
hostname_of(B) when is_binary(B)         -> strip_port(B).

strip_port(B) ->
    case binary:split(B, <<":">>) of
        [H | _] -> H
    end.

schedule_rebuild(S) ->
    Ref = make_ref(),
    erlang:send_after(?REBUILD_INTERVAL_MS, self(), {rebuild, Ref}),
    S#state{timer_ref = Ref}.
