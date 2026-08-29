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
%%%
%%% == Wildcard patterns (2026-08-29) — a SEPARATE gossip, not folded
%%% into the Bloom ==
%%%
%%% A Bloom filter tests exact-string membership; a `*'-bearing pattern
%%% cannot be usefully summarized into one (see `hecate_pubsub''s own
%%% moduledoc). Patterns are expected to be FEW relative to exact
%%% topics — no station is expected to accumulate thousands of
%%% wildcard subscriptions the way it might exact ones — so instead of
%%% inventing a pattern-aware Bloom variant, the raw pattern SET
%%% (`hecate_pubsub_server:patterns/1', transitively unioned with every
%%% peer's own gossiped set, same shape as the Bloom merge) is gossiped
%%% directly on its own `_mesh.patterns' topic, on the SAME rebuild
%%% tick and debounce as the Bloom. `pattern_matches_ets/1' is the
%%% publish-time counterpart to `peer_matches_ets/1': for a concrete
%%% topic, which peers (by NodeId) hold a pattern that matches it —
%%% checked via `macula_topic_pattern:matches/2' per pattern, not a
%%% single Bloom test, which is the acceptable cost of "few patterns,
%%% checked rarely" the design leans on.
-module(macula_station_bloom_exchange).
-behaviour(gen_server).

-export([start_link/1, stop/1]).
-export([bloom_stats/0]).
-export([rebuild_and_broadcast/1, receive_peer_bloom/3,
         notify_local_change/1,
         get_local_bloom/1, peer_blooms/1, peer_matches/2,
         peer_matches_ets/1]).
-export([receive_peer_patterns/3, get_local_patterns/1, peer_patterns/1,
         pattern_matches_ets/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(REBUILD_INTERVAL_MS, 30_000).

%% Public ETS table mirroring `peer_blooms'. Read-only consumers
%% (notably `macula_station_route_pubsub_frames's bloom-fan path) use
%% this directly to avoid serialising every inbound EVENT through
%% bloom_exchange's mailbox — a `gen_server:call' per event creates
%% a per-station bottleneck under sustained pubsub load. Same
%% pattern as `macula_station_peer_observer_conns' (commit d0f0c8a).
-define(PEER_BLOOMS_TABLE, macula_station_peer_blooms).

%% Same ETS-bypass shape as ?PEER_BLOOMS_TABLE, for patterns. Kept as a
%% SEPARATE table (not folded into the same rows) since the two are
%% independently sized, independently gossiped, and a consumer wanting
%% only exact-match Bloom fan-out (the overwhelmingly common case)
%% should not pay for scanning pattern entries it never needed.
-define(PEER_PATTERNS_TABLE, macula_station_peer_patterns).

%% Wire topic for pattern gossip — deliberately separate from
%% `_mesh.bloom' (see moduledoc): a fixed-size Bloom binary and a
%% variable-length pattern list are different payload shapes, and
%% mixing them would break every existing `_mesh.bloom' consumer's
%% `byte_size(Bin) =:= 1024' assumption.
-define(MESH_PATTERNS_TOPIC, <<"_mesh.patterns">>).

%% Push-on-change debounce. When a peer's bloom CHANGES (new publisher
%% or a different filter from the same publisher), schedule an
%% out-of-band rebuild within ?DEBOUNCE_MS instead of waiting up to
%% ?REBUILD_INTERVAL_MS for the periodic tick. Drops first-delivery
%% latency for multi-hop pubsub from ~3 * REBUILD_INTERVAL (≈90s on a
%% 4-hop mesh) to ~3 * DEBOUNCE (≈6s). Debounce coalesces bursts of
%% peer_bloom updates into one rebuild.
-define(DEBOUNCE_MS, 2_000).

%% How long a peer's Bloom survives without being re-gossiped.
%%
%% The table was INSERT-ONLY: `record_peer_bloom/4' was the single writer and
%% there was no delete anywhere in the module, so a station decommissioned
%% months ago kept contributing bits until this process restarted. That matters
%% CORRECTION (2026-07-26): eviction does NOT bound `n', and an earlier version
%% of this comment claimed it did. `do_rebuild/1' merges each peer's OUTGOING
%% (already-merged) filter, which is what peers publish on `_mesh.bloom'. So for
%% stations A and B, A.outgoing includes B.outgoing includes A.outgoing: a bit
%% set once at A survives at A forever via B, and unsubscribing a topic clears
%% it from A.local only for it to return on the next tick. Eviction fires only
%% when a station stops gossiping ENTIRELY, and by then its bits have been
%% absorbed into every survivor and are rebroadcast every 30s. The bit set is
%% monotone non-decreasing under all conditions.
%%
%% What this DOES buy, and why it stays: it bounds the map/table ENTRY count,
%% and it stops a decommissioned station being returned as a fan candidate by
%% `peer_matches_ets/1'. Bounding `n' requires routing on per-origin LOCAL
%% filters (already on the wire as `_mesh.bloom.local') instead of merged ones.
%%
%% Eviction is by staleness, not by peer liveness, because the merge is
%% TRANSITIVE: we legitimately hold filters for stations we do not peer with
%% directly, so "not in our conns table" would drop live interest. A station
%% that has stopped gossiping for 20 consecutive rebuild ticks is gone.
-define(PEER_BLOOM_TTL_MS, 600_000).

%% Mesh-level events (including bloom gossip) live in the all-zeros
%% realm — protocol infrastructure, not bound to any business realm.
-define(MESH_REALM, <<0:256>>).

%% Registries are REGISTERED NAMES, resolved by gen_server:call/2 on
%% every use. A pid captured at child-spec time is dead the moment the
%% registry restarts, and supervisors reuse the original child spec, so
%% the stale pid would never be replaced.
-type opts() :: #{
    pubsub_registry := atom() | pid(),
    identity        := macula_identity:key_pair()
}.

-export_type([opts/0]).

-record(state, {
    pubsub_registry :: atom() | pid(),
    identity        :: macula_identity:key_pair(),
    local_bloom     :: binary(),
    peer_blooms     :: #{binary() => binary()},
    local_patterns  :: [binary()],
    peer_patterns   :: #{binary() => [binary()]},
    %% Key => last time we saw this peer's bloom OR pattern set
    %% (monotonic ms) — ONE staleness clock for both, since they are
    %% gossiped on the same tick from the same peer; a peer that has
    %% stopped gossiping entirely is equally stale for both.
    peer_seen       :: #{binary() => integer()},
    %% Active subscriptions on each peer's station_link for inbound
    %% `_mesh.bloom' AND `_mesh.patterns' events (one subref each, same
    %% link). Keyed by peer hostname so we don't double-subscribe when
    %% peer_links reports the same connection twice across resync ticks.
    subs            :: #{binary() => {pid(), reference(), reference()}},
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

%% @doc Cache an incoming peer pattern set. Called by whatever handler
%% routes inbound `_mesh.patterns' events to this manager — see
%% `receive_peer_bloom/3', same shape.
-spec receive_peer_patterns(pid(), binary(), [binary()]) -> ok.
receive_peer_patterns(Pid, PeerHostname, Patterns)
  when is_binary(PeerHostname), is_list(Patterns) ->
    gen_server:cast(Pid, {peer_patterns, PeerHostname, Patterns}).

%% @doc Snapshot of the current local pattern set.
-spec get_local_patterns(pid()) -> [binary()].
get_local_patterns(Pid) ->
    try gen_server:call(Pid, get_local_patterns, 500)
    catch _:_ -> []
    end.

%% @doc Snapshot of all known peer pattern sets.
-spec peer_patterns(pid()) -> #{binary() => [binary()]}.
peer_patterns(Pid) ->
    try gen_server:call(Pid, peer_patterns, 500)
    catch _:_ -> #{}
    end.

%% @doc ETS-bypass: peer NodeIds holding a pattern that matches `Topic'.
%% The pattern-fan counterpart to `peer_matches_ets/1' — see moduledoc
%% for why this is a per-pattern `macula_topic_pattern:matches/2' scan
%% rather than a single Bloom test, and why that is an acceptable cost.
%% Same transitive-reach caveat as `peer_matches_ets/1': callers MUST
%% intersect with their direct-peer conn set before sending.
-spec pattern_matches_ets(binary()) -> [<<_:256>>].
pattern_matches_ets(Topic) when is_binary(Topic) ->
    TopicSegments = binary:split(Topic, <<"/">>, [global]),
    try ets:tab2list(?PEER_PATTERNS_TABLE) of
        Entries ->
            [NodeId
             || {NodeId, Patterns} <- Entries,
                is_list(Patterns),
                lists:any(fun(P) -> pattern_matches(P, TopicSegments) end,
                         Patterns)]
    catch
        error:badarg -> []
    end.

pattern_matches(Pattern, TopicSegments) when is_binary(Pattern) ->
    macula_topic_pattern:matches(binary:split(Pattern, <<"/">>, [global]),
                                 TopicSegments);
pattern_matches(_NotABinary, _TopicSegments) ->
    false.

%% @doc Live filter saturation. `count_bits_set/1' and
%% `estimated_elements/1' have been exported from `macula_station_bloom'
%% all along and called NOWHERE in this application, so the only fill
%% number in the programme came from a hand-run docker exec probe.
%%
%% Read `merged_fp' first. At m=8192/k=7 the design point is ~1% and a
%% Bloom hit stops being better than flooding around 5%, which is roughly
%% 1300 topics. `merged_*' is what this station BROADCASTS (local OR every
%% peer, and monotone: see the correction above). `local_*' is this
%% station's own interest, which is the only figure that can actually fall.
%% The gap between them is the transitive union's cost.
-spec bloom_stats() -> #{atom() => term()}.
bloom_stats() ->
    stats_of(whereis(?MODULE)).

stats_of(undefined) ->
    #{error => not_running};
stats_of(Pid) ->
    Local  = get_local_bloom(Pid),
    Merged = merge_with_peers(Local, peer_blooms(Pid)),
    #{peers        => map_size(peer_blooms(Pid)),
      local_bits   => bits(Local),
      local_n      => elements(Local),
      merged_bits  => bits(Merged),
      merged_n     => elements(Merged),
      merged_fill  => fill(Merged),
      merged_fp    => fp(Merged)}.

bits(Bin)     -> macula_station_bloom:count_bits_set(from_bin(Bin)).
elements(Bin) -> macula_station_bloom:estimated_elements(from_bin(Bin)).
fill(Bin)     -> bits(Bin) / 8192.
%% (set/m)^k is the probability a guessed topic's k positions are all set.
fp(Bin)       -> math:pow(fill(Bin), 7).

from_bin(Bin) -> macula_station_bloom:from_binary(Bin).

%%====================================================================
%% gen_server
%%====================================================================

init(#{pubsub_registry := Reg, identity := Kp}) ->
    process_flag(trap_exit, true),
    ensure_peer_blooms_table(),
    ensure_peer_patterns_table(),
    State = #state{
        pubsub_registry = Reg,
        identity        = Kp,
        local_bloom     = empty_bloom_bin(),
        peer_blooms     = #{},
        local_patterns  = [],
        peer_patterns   = #{},
        peer_seen       = #{},
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

%% Same idempotent-recreate shape as `ensure_peer_blooms_table/0'.
ensure_peer_patterns_table() ->
    case ets:whereis(?PEER_PATTERNS_TABLE) of
        undefined ->
            ets:new(?PEER_PATTERNS_TABLE,
                    [named_table, public, set, {read_concurrency, true}]);
        _ ->
            ?PEER_PATTERNS_TABLE
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
handle_call(get_local_patterns, _From, #state{local_patterns = LP} = S) ->
    {reply, LP, S};
handle_call(peer_patterns, _From, #state{peer_patterns = PP} = S) ->
    {reply, PP, S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(rebuild_and_broadcast, S) ->
    {noreply, do_rebuild(S)};
handle_cast({peer_bloom, Host, BloomBin}, #state{peer_blooms = PB} = S)
  when byte_size(BloomBin) =:= 1024 ->
    {noreply, record_peer_bloom(Host, BloomBin, PB, S)};
handle_cast({peer_bloom, _, _}, S) ->
    {noreply, S};
handle_cast({peer_patterns, Host, Patterns}, #state{peer_patterns = PP} = S)
  when is_list(Patterns) ->
    {noreply, record_peer_patterns(Host, Patterns, PP, S)};
handle_cast({peer_patterns, _, _}, S) ->
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
    S1 = sync_inbound_subs(do_rebuild(evict_stale_peer_blooms(S))),
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
%% Key the cache by `Meta.publisher' (the originator's pubkey), NOT
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
%% `_mesh.patterns' — same publisher-keyed, self-echo-dropping shape as
%% `_mesh.bloom' above, decoding the variable-length payload instead of
%% pattern-matching a fixed size.
handle_info({macula_event, _SubRef, ?MESH_PATTERNS_TOPIC, Payload,
             #{publisher := Publisher}},
            #state{identity = Kp, peer_patterns = PP} = S)
  when is_binary(Payload), is_binary(Publisher), byte_size(Publisher) =:= 32 ->
    case Publisher =:= macula_identity:public(Kp) of
        true  -> {noreply, S};
        false -> on_decoded_patterns(safe_decode_patterns(Payload),
                                     Publisher, PP, S)
    end;
handle_info({macula_event, _SubRef, ?MESH_PATTERNS_TOPIC, _Payload, _Meta}, S) ->
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

on_decoded_patterns({ok, Patterns}, Publisher, PP, S) ->
    {noreply, record_peer_patterns(Publisher, Patterns, PP, S)};
on_decoded_patterns({error, _Reason}, _Publisher, _PP, S) ->
    {noreply, S}.

%% try/catch deviation (see this repo's CLAUDE.md): `Payload' is
%% attacker-influenceable wire data from a peer station, not a value
%% this process constructed — `binary_to_term/2' genuinely raises on
%% malformed input, and there is no pattern-match substitute for
%% "parse this variable-length blob" the way `_mesh.bloom' can lean on
%% a fixed `byte_size =:= 1024' guard. `[safe]' additionally refuses to
%% create new atoms or reconstruct funs/pids from the wire, so a
%% malicious peer cannot use this path to grow the atom table.
safe_decode_patterns(Payload) ->
    try binary_to_term(Payload, [safe]) of
        Patterns -> validated_patterns(Patterns)
    catch
        _:_ -> {error, undecodable}
    end.

validated_patterns(Patterns) when is_list(Patterns) ->
    case lists:all(fun is_binary/1, Patterns) of
        true  -> {ok, Patterns};
        false -> {error, not_all_binaries}
    end;
validated_patterns(_NotAList) ->
    {error, not_a_list}.

terminate(_Reason, _S) -> ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Rebuild + broadcast
%%====================================================================

%% Drop peer filters that have not been re-gossiped within
%% `?PEER_BLOOM_TTL_MS'. Runs on the periodic tick only, so a burst of
%% inbound blooms never pays for it.
%% Timestamps live in their OWN map, deliberately. Folding them into
%% `peer_blooms' as `{Bin, SeenAt}' compiles cleanly and then fails SILENTLY:
%% `merge_with_peers/2' guards its fold clause on `when is_binary(Bin)', so
%% every tuple would fall to the `_BadBin -> Acc' branch and the merged filter
%% would collapse to local-only, killing transitive interest with no crash and
%% no log. Same for the `peer_blooms/1' accessor's `#{binary() => binary()}'
%% contract. Keeping the stored shape untouched keeps both correct.
%% Also evicts stale entries from `peer_patterns' -- same `peer_seen'
%% clock (see that field's own comment: gossiped together, equally
%% stale together).
evict_stale_peer_blooms(#state{peer_blooms = PB, peer_seen = Seen} = S) ->
    Cutoff = now_ms() - ?PEER_BLOOM_TTL_MS,
    Stale  = [K || K <- maps:keys(PB), maps:get(K, Seen, 0) < Cutoff],
    evicted(Stale, evict_stale_peer_patterns(Cutoff, S)).

evict_stale_peer_patterns(Cutoff, #state{peer_patterns = PP, peer_seen = Seen} = S) ->
    Stale = [K || K <- maps:keys(PP), maps:get(K, Seen, 0) < Cutoff],
    evicted_patterns(Stale, S).

evicted_patterns([], S) ->
    S;
evicted_patterns(Stale, #state{peer_patterns = PP} = S) ->
    [catch ets:delete(?PEER_PATTERNS_TABLE, K) || K <- Stale],
    S#state{peer_patterns = maps:without(Stale, PP)}.

evicted([], S) ->
    S;
evicted(Stale, #state{peer_blooms = PB, peer_seen = Seen} = S) ->
    [catch ets:delete(?PEER_BLOOMS_TABLE, K) || K <- Stale],
    Live = maps:without(Stale, PB),
    logger:info("[bloom_exchange] evicted ~b stale peer bloom(s); ~b live",
                [length(Stale), maps:size(Live)]),
    S#state{peer_blooms = Live, peer_seen = maps:without(Stale, Seen)}.

now_ms() -> erlang:monotonic_time(millisecond).

do_rebuild(#state{pubsub_registry = Reg, peer_blooms = PB,
                  peer_patterns = PP} = S) ->
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
    %% Same transitive-union shape as the bloom above, just a set
    %% union instead of a bitwise OR — see moduledoc for why patterns
    %% are gossiped raw rather than Bloom-summarized. Same loop-kill
    %% (event_dedup) and convergence reasoning applies identically.
    LocalPatterns = safe_patterns(Reg),
    OutgoingPatterns = merge_patterns_with_peers(LocalPatterns, PP),
    broadcast_patterns(OutgoingPatterns),
    S#state{local_bloom = LocalBin, local_patterns = LocalPatterns}.

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

%% Set-union equivalent of `merge_with_peers/2' for patterns — local
%% patterns OR every peer's gossiped set, deduped. No 1024-byte shape
%% to guard on, so malformed peer entries are guarded structurally
%% instead (a non-list-of-binaries never got stored — see
%% `record_peer_patterns/4' and `validated_patterns/1').
merge_patterns_with_peers(LocalPatterns, PeerPatterns) when map_size(PeerPatterns) =:= 0 ->
    lists:usort(LocalPatterns);
merge_patterns_with_peers(LocalPatterns, PeerPatterns) ->
    lists:usort(
      lists:foldl(fun(Peer, Acc) -> Peer ++ Acc end,
                 LocalPatterns, maps:values(PeerPatterns))).

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
    Lookup = try hecate_pubsub_registry:lookup(Reg, Realm)
             catch _:_ -> error
             end,
    topics_for_lookup(Lookup).

topics_for_lookup({ok, Server}) ->
    try hecate_pubsub_server:topics(Server)
    catch _:_ -> []
    end;
topics_for_lookup(_) ->
    [].

%% Same realm-iteration shape as `safe_topics/1', reading each realm's
%% registered PATTERNS instead of its exact topics. Same realm-blind
%% reasoning applies (see that function's own comment): a station's
%% outgoing pattern gossip must cover every local realm's wildcard
%% subscribers, not just the mesh realm.
safe_patterns(Reg) ->
    Realms = try hecate_pubsub_registry:list_realms(Reg)
             catch _:_ -> []
             end,
    lists:flatmap(fun(Realm) -> patterns_for_realm(Reg, Realm) end, Realms).

patterns_for_realm(Reg, Realm) ->
    Lookup = try hecate_pubsub_registry:lookup(Reg, Realm)
             catch _:_ -> error
             end,
    patterns_for_lookup(Lookup).

patterns_for_lookup({ok, Server}) ->
    try hecate_pubsub_server:patterns(Server)
    catch _:_ -> []
    end;
patterns_for_lookup(_) ->
    [].

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

%% Broadcast `_mesh.patterns' to every active outbound station_link.
%% Same no-op-with-no-connections shape as `broadcast_filter/1'.
%% `term_to_binary/1' (not CBOR): this is a purely internal,
%% station-to-station protocol detail, not a public wire format —
%% `safe_decode_patterns/1' on the receiving end guards it with
%% `binary_to_term(_, [safe])'.
broadcast_patterns(Patterns) ->
    Payload = term_to_binary(Patterns),
    Conns = macula_station_peer_links:connections(),
    lists:foreach(
      fun({_Url, LinkPid}) ->
              catch macula_station_link:publish(
                      LinkPid, ?MESH_REALM, ?MESH_PATTERNS_TOPIC, Payload)
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
record_peer_bloom(Key, BloomBin, PB, #state{peer_seen = Seen} = S) ->
    Changed = maps:get(Key, PB, undefined) =/= BloomBin,
    catch ets:insert(?PEER_BLOOMS_TABLE, {Key, BloomBin}),
    S1 = S#state{peer_blooms = PB#{Key => BloomBin},
                 peer_seen   = Seen#{Key => now_ms()}},
    case Changed of
        true  -> schedule_debounced_rebuild(S1);
        false -> S1
    end.

%% Same shape as `record_peer_bloom/4' for patterns — see that
%% function's own comments.
record_peer_patterns(Key, Patterns, PP, #state{peer_seen = Seen} = S) ->
    Changed = maps:get(Key, PP, undefined) =/= Patterns,
    catch ets:insert(?PEER_PATTERNS_TABLE, {Key, Patterns}),
    S1 = S#state{peer_patterns = PP#{Key => Patterns},
                 peer_seen     = Seen#{Key => now_ms()}},
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
    maps:filter(fun(Host, Sub) -> keep_sub(sets:is_element(Host, ActiveHosts), Sub) end,
                Subs).

keep_sub(true, _Sub) ->
    true;
keep_sub(false, {LinkPid, BloomSubRef, PatternSubRef}) ->
    catch macula_station_link:unsubscribe(LinkPid, BloomSubRef),
    catch macula_station_link:unsubscribe(LinkPid, PatternSubRef),
    false.

subscribe_new_peers(Subs, Active) ->
    lists:foldl(fun maybe_subscribe_peer/2, Subs, Active).

maybe_subscribe_peer({Host, LinkPid}, Acc) ->
    subscribe_peer(maps:is_key(Host, Acc), Acc, Host, LinkPid).

subscribe_peer(true, Acc, _Host, _LinkPid) -> Acc;
subscribe_peer(false, Acc, Host, LinkPid) -> subscribe_one(Acc, Host, LinkPid).

%% LinkPid may be a `macula_station_link' SDK client (handles
%% subscribe) OR a `macula_station_outbound_link' worker (does
%% not — returns `{error, unknown_call}'). The wildcard `of'
%% clause is mandatory: `try_clause' from a missing `of' pattern
%% is NOT caught by the `catch' below.
%%
%% Subscribes to BOTH `_mesh.bloom' and `_mesh.patterns' on the same
%% link — an entry only lands in `Subs' when both succeed, so a host
%% is either "fully wired" or absent and retried whole on the next
%% tick. This was the actual bug behind mesh-wide wildcard pubsub
%% never converging (found live 2026-08-29, verifying slice 5b): only
%% `_mesh.bloom' was ever subscribed here, so `broadcast_patterns/1'
%% published `_mesh.patterns' into a channel nobody had subscribed to
%% receive — every station's `peer_patterns' stayed permanently empty,
%% including a DIRECT peer of a station with an active local pattern.
subscribe_one(Acc, Host, LinkPid) ->
    subscribe_bloom(Acc, Host, LinkPid, subscribe_link(LinkPid, <<"_mesh.bloom">>)).

subscribe_bloom(Acc, Host, LinkPid, {ok, BloomSubRef}) ->
    subscribe_patterns(Acc, Host, LinkPid, BloomSubRef,
                       subscribe_link(LinkPid, ?MESH_PATTERNS_TOPIC));
subscribe_bloom(Acc, _Host, _LinkPid, _Other) ->
    Acc.

subscribe_patterns(Acc, Host, LinkPid, BloomSubRef, {ok, PatternSubRef}) ->
    Acc#{Host => {LinkPid, BloomSubRef, PatternSubRef}};
subscribe_patterns(Acc, _Host, LinkPid, BloomSubRef, _Other) ->
    %% Pattern subscribe failed after bloom succeeded — roll the bloom
    %% subscription back rather than leaving it orphaned and unretried
    %% (an entry in `Subs' is the only thing that stops a retry on the
    %% next tick, so a half-subscribed host must not get one).
    catch macula_station_link:unsubscribe(LinkPid, BloomSubRef),
    Acc.

subscribe_link(LinkPid, Topic) ->
    try macula_station_link:subscribe(LinkPid, ?MESH_REALM, Topic, self()) of
        {ok, SubRef} -> {ok, SubRef};
        Other        -> Other
    catch _:_ -> {error, subscribe_failed}
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
