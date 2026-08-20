%% @doc Station-level content procedure handlers.
%%
%% Advertises four RPC procedures against the station's
%% `macula_handler_registry' so daemons can put and get
%% content-addressed blobs (chunked + Merkle-rooted) over the
%% existing unary CALL infrastructure:
%%
%% <ul>
%%   <li>`_content.put_manifest' — accepts a manifest map, stores it
%%       in the local content store. Manifest = `chunk_size +
%%       chunks (each with hash) + Merkle root + total size'.</li>
%%   <li>`_content.put_block' — accepts `{mcid, payload}', verifies
%%       the payload's BLAKE3 hash matches the MCID, stores the
%%       block locally.</li>
%%   <li>`_content.get_manifest' — accepts `{mcid}', returns the
%%       manifest map or `not_found'. On local miss this is the
%%       point where iterative discovery to other stations could
%%       eventually be wired in (Phase 4+); for now we return
%%       `not_found' and the caller's daemon retries against
%%       other stations on its own.</li>
%%   <li>`_content.get_block' — accepts `{mcid}', returns the
%%       block payload or `not_found'.</li>
%% </ul>
%%
%% This puts content sharing on the same plumbing as `_dht.*' —
%% no new wire frames needed, no new station-to-station relay
%% protocol. Daemon SDK layers an iterate-providers + reassemble
%% loop on top.
-module(macula_station_content_handlers).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-export([fanout_candidates/2, advance_seen/2]).
-endif.

-export_type([opts/0]).

%% Internal — `_content.put_block' / `_content.get_block' / etc. all
%% live under the same realm tag the SDK uses (`?CONTENT_REALM' in
%% macula.erl, `<<0:256>>'). Mirroring the constant here so the
%% station-to-station iteration call uses the same value.
-define(CONTENT_REALM, <<0:256>>).
%% Per-peer timeout for the iterative leg. Smaller than the SDK's
%% `?CONTENT_BLOCK_TIMEOUT_MS' so the daemon's outer call doesn't
%% race the iteration's deadline.
-define(REMOTE_BLOCK_PER_PEER_MS, 5_000).
%% Eager replication fan-out cap on `_content.put_block'. K = 3
%% covers the partial-mesh topology where each station has 3-8
%% peer connections — every cross-station read pair shares at
%% least one peer that received an eager replica, so 1-hop
%% iterative fallback (or even direct-peer hit) reliably finds
%% the block. Larger K = more wire bandwidth per put without
%% meaningful coverage gain at this mesh size.
-define(DEFAULT_REPLICATION_K, 3).
%% Per-peer timeout for the eager replication leg. Generous because
%% the receiving relay does a full BLAKE3 hash check before
%% acking, and 256 KiB blobs over a slow link can take real time.
-define(REPLICATION_TIMEOUT_MS, 10_000).

%% Default hop budget for a daemon's `_content.get_block' /
%% `_content.get_manifest' that doesn't supply its own.
%%
%% Was capped at 1 hop because 2-hop iteration used to overwhelm the
%% fleet's macula_content_store gen_server under torture (each failed
%% lookup amplified to 8² = 64 concurrent inbound calls per relay —
%% unbounded fanout width, EVERY peer connection, with no dedup, so a
%% station with 8 peers asking 8 peers each hits 64). That amplification
%% is why this stayed at 1 despite the partial-mesh topology genuinely
%% needing 2 (a pair with no direct edge but a shared bridge peer needs
%% exactly 2 hops: reader -> bridge -> writer's replica).
%%
%% Fixed at the fanout site instead of by staying at 1 forever:
%% `fanout_candidates/2' now caps width to `?DEFAULT_REPLICATION_K' (the
%% same bound PUT-side eager replication already uses, so GET never
%% explores more of the mesh per hop than PUT already populated) AND
%% threads a `seen' list through every hop so no peer is EVER asked
%% twice across the whole walk, in either direction. Worst case for a
%% 2-hop walk is now bounded to K + K*K = 12 calls, not 64 — see
%% `macula_station_content_fanout_bound_tests' for the property test
%% asserting this holds even against an adversarial (fully-connected,
%% many-peer) topology.
-define(DEFAULT_HOPS, 2).
%% Fanout width per hop — reuses the PUT side's replication fan-out
%% cap. A GET hop exploring MORE of the mesh than a PUT ever replicated
%% to would just be wasted calls; using the same K keeps the two sides
%% symmetric and keeps this one number, not two, as the mesh's fanout
%% budget.
-define(GET_FANOUT_WIDTH, ?DEFAULT_REPLICATION_K).

%% Registries are REGISTERED NAMES, resolved by gen_server:call/2 on
%% every use. A pid captured at child-spec time is dead the moment the
%% registry restarts, and supervisors reuse the original child spec, so
%% the stale pid would never be replaced.
-type opts() :: #{
    handler_registry := atom() | pid()
}.

-record(state, {
    handler_registry :: atom() | pid()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{handler_registry := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

%%====================================================================
%% gen_server
%%====================================================================

init(#{handler_registry := Registry}) ->
    advertise_all(Registry),
    {ok, #state{handler_registry = Registry}}.

handle_call(_Msg, _From, S) -> {reply, {error, unknown_call}, S}.
handle_cast(_Msg, S)        -> {noreply, S}.
handle_info(_Msg, S)        -> {noreply, S}.
terminate(_Reason, _S)      -> ok.
code_change(_OldVsn, S, _)  -> {ok, S}.

%%====================================================================
%% Procedure advertisement
%%====================================================================

advertise_all(Registry) ->
    ok = macula_handler_registry:advertise(
           Registry, <<"_content.put_manifest">>,
           fun handle_put_manifest/1),
    ok = macula_handler_registry:advertise(
           Registry, <<"_content.put_block">>,
           fun handle_put_block/1),
    ok = macula_handler_registry:advertise(
           Registry, <<"_content.get_manifest">>,
           fun handle_get_manifest/1),
    ok = macula_handler_registry:advertise(
           Registry, <<"_content.get_block">>,
           fun handle_get_block/1),
    ok.

%%====================================================================
%% Handler bodies
%%====================================================================

handle_put_manifest(#{manifest := Manifest}) when is_map(Manifest) ->
    ok = macula_content_store:put_manifest(Manifest),
    {ok, ok};
handle_put_manifest(_Other) ->
    {error, bad_request}.

%% Block put validates the payload hashes to the MCID before storing
%% (`macula_content_store:put_block/2' performs the BLAKE3 check and
%% returns `{error, hash_mismatch}' on tampered uploads). The CALL
%% relay path doesn't validate content integrity, so this is the
%% chokepoint that keeps the local store consistent.
handle_put_block(#{mcid := MCID, payload := Payload} = Args)
  when is_binary(MCID), is_binary(Payload) ->
    on_local_put(macula_content_store:put_block(MCID, Payload), MCID, Payload, Args);
handle_put_block(_Other) ->
    {error, bad_request}.

%% Eager replication kicks in on a successful local store, and only
%% when the daemon-facing put — `replicate' arg defaults to true.
%% Peer-to-peer replication calls pass `replicate => false' so the
%% receiving relay stores locally and does not re-fan-out (loop
%% prevention; mirrors the `hops_remaining' shape on get_block).
%%
%% Local-store failure (`hash_mismatch') short-circuits — the
%% replicas would fail the same hash check, no point fanning out.
on_local_put(ok, MCID, Payload, Args) ->
    case maps:get(replicate, Args, true) of
        true  -> fire_eager_block_replication(MCID, Payload);
        false -> ok
    end,
    {ok, ok};
on_local_put({error, hash_mismatch}, _MCID, _Payload, _Args) ->
    {ok, hash_mismatch}.

%% Pick up to ?DEFAULT_REPLICATION_K peer-station LinkPids and
%% spawn one worker per peer that calls the peer's
%% `_content.put_block' RPC with `replicate => false'. Workers are
%% unlinked + fire-and-forget — the daemon's outer put_content
%% RPC returns the moment the local store succeeds; replication
%% acks happen in the background. Bounded by K so a put never
%% generates more than K outbound store calls.
fire_eager_block_replication(MCID, Payload) ->
    Conns = safe_peer_connections(),
    Targets = lists:sublist(Conns, ?DEFAULT_REPLICATION_K),
    [spawn(fun() ->
        _ = safe_replicate(LinkPid, MCID, Payload)
     end) || {_Url, LinkPid} <- Targets],
    ok.

safe_replicate(LinkPid, MCID, Payload) ->
    Args = #{mcid => MCID, payload => Payload, replicate => false},
    safe_content_call(LinkPid, <<"_content.put_block">>, Args,
                      ?REPLICATION_TIMEOUT_MS).

%% Local hit short-circuits. Local miss falls back to a bounded
%% multi-hop relay against peer stations, mirroring `handle_get_block'
%% below exactly (same `hops_remaining'/`seen' shape, same
%% `fanout_candidates/1' bound) — this handler had NO relay at all
%% before: a miss returned `not_found' immediately with a comment
%% saying iterative discovery was a "Phase 4+" concern. For CHUNKED
%% content that is far more costly than it looks: `get_chunked/3'
%% (macula.erl) fetches the manifest FIRST and gives up immediately on
%% `not_found' — no chunk fetch is even attempted without it — so this
%% one missing hop was the dominant reason chunked content above the
%% 256 KiB threshold never arrived cross-station at all, independent of
%% whether every individual chunk was otherwise reachable.
handle_get_manifest(#{mcid := MCID} = Args) when is_binary(MCID) ->
    on_local_manifest(macula_content_store:get_manifest(MCID), MCID, Args);
handle_get_manifest(_Other) ->
    {error, bad_request}.

on_local_manifest({ok, Manifest}, _MCID, _Args) ->
    {ok, Manifest};
on_local_manifest({error, not_found}, MCID, Args) ->
    Hops = maps:get(hops_remaining, Args, ?DEFAULT_HOPS),
    Seen = maps:get(seen, Args, []),
    on_manifest_iterative_decision(Hops, MCID, Seen).

on_manifest_iterative_decision(0, _MCID, _Seen) ->
    {ok, not_found};
on_manifest_iterative_decision(N, MCID, Seen) when N > 0 ->
    fanout_remote_get_manifest(MCID, N - 1, Seen).

fanout_remote_get_manifest(MCID, NextHops, Seen) ->
    Candidates = fanout_candidates(safe_peer_connections(), Seen),
    on_manifest_candidates(Candidates, MCID, NextHops,
                           advance_seen(Seen, Candidates)).

on_manifest_candidates([], _MCID, _NextHops, _NewSeen) ->
    {ok, not_found};
on_manifest_candidates(Candidates, MCID, NextHops, NewSeen) ->
    Tag    = make_ref(),
    Parent = self(),
    Args   = #{mcid => MCID, hops_remaining => NextHops, seen => NewSeen},
    [spawn(fun() ->
        Reply = safe_manifest_call(LinkPid, Args),
        Parent ! {Tag, Reply}
     end) || {_Url, LinkPid} <- Candidates],
    collect_first_manifest(Tag, length(Candidates),
                           ?REMOTE_BLOCK_PER_PEER_MS + 200).

safe_manifest_call(LinkPid, Args) ->
    safe_content_call(LinkPid, <<"_content.get_manifest">>, Args,
                      ?REMOTE_BLOCK_PER_PEER_MS).

collect_first_manifest(_Tag, 0, _DeadlineMs) ->
    {ok, not_found};
collect_first_manifest(Tag, Remaining, DeadlineMs) ->
    Start = erlang:monotonic_time(millisecond),
    receive
        {Tag, {ok, Wire}} when is_map(Wire) ->
            {ok, Wire};
        {Tag, _Other} ->
            Elapsed = erlang:monotonic_time(millisecond) - Start,
            collect_first_manifest(Tag, Remaining - 1,
                                   max(0, DeadlineMs - Elapsed))
    after DeadlineMs ->
        {ok, not_found}
    end.

handle_get_block(#{mcid := MCID} = Args) when is_binary(MCID) ->
    on_local_block(macula_content_store:get_block(MCID), MCID, Args);
handle_get_block(_Other) ->
    {error, bad_request}.

%% Local hit short-circuits. Local miss falls back to a bounded
%% multi-hop relay against peer stations — necessary because the v1
%% put path stores ONLY locally on the writer's relay, and the reader
%% (here) might be a different relay.
%%
%% The `hops_remaining' arg counts down hops the receiving handler is
%% still allowed to walk. A daemon's first call arrives without the
%% field — defaults to `?DEFAULT_HOPS'. Each peer-to-peer iteration
%% decrements the counter; reaching 0 stops at local lookup.
%%
%% `seen' is the OTHER half of what makes 2+ hops safe: every URL this
%% walk has already asked (in EITHER direction — hop 1's askers are in
%% hop 2's `seen' too) is threaded through so no peer is ever queried
%% twice. Without it, two mutually-peered stations would re-ask each
%% other every hop, and on a station with ~8 peers, 2 unbounded hops is
%% 8² = 64 concurrent inbound calls — the exact amplification that
%% overwhelmed `macula_content_store' and cascaded `peer_observer' in
%% the incident `?DEFAULT_HOPS' used to be capped at 1 to avoid. `seen'
%% plus the `?GET_FANOUT_WIDTH' cap on `fanout_candidates/1' bound the
%% SAME 2-hop walk to at most K + K*K calls instead.
on_local_block({ok, Block}, _MCID, _Args) ->
    {ok, Block};
on_local_block({error, not_found}, MCID, Args) ->
    Hops = maps:get(hops_remaining, Args, ?DEFAULT_HOPS),
    Seen = maps:get(seen, Args, []),
    on_iterative_decision(Hops, MCID, Seen).

on_iterative_decision(0, _MCID, _Seen) ->
    {ok, not_found};
on_iterative_decision(N, MCID, Seen) when N > 0 ->
    fanout_remote_get_block(MCID, N - 1, Seen).

fanout_remote_get_block(MCID, NextHops, Seen) ->
    Candidates = fanout_candidates(safe_peer_connections(), Seen),
    on_block_candidates(Candidates, MCID, NextHops, advance_seen(Seen, Candidates)).

on_block_candidates([], _MCID, _NextHops, _NewSeen) ->
    {ok, not_found};
on_block_candidates(Candidates, MCID, NextHops, NewSeen) ->
    Tag    = make_ref(),
    Parent = self(),
    Args   = #{mcid => MCID, hops_remaining => NextHops, seen => NewSeen},
    [spawn(fun() ->
        Reply = safe_call(LinkPid, Args),
        Parent ! {Tag, Reply}
     end) || {_Url, LinkPid} <- Candidates],
    collect_first_block(Tag, length(Candidates),
                        ?REMOTE_BLOCK_PER_PEER_MS + 200).

%% Shared by both `_content.get_block' and `_content.get_manifest'
%% relays: this hop's candidates are this station's own peer
%% connections, minus anything already asked anywhere in the walk so
%% far, capped to `?GET_FANOUT_WIDTH'. Order is whatever
%% `peer_links:connections/0' returns — there is no key-distance
%% concept here, unlike the DHT walk, so "first K not-yet-asked" is as
%% principled a choice as any other and matches the PUT side's own
%% unranked "first K" replication target selection.
%%
%% Pure (takes `Conns' explicitly) so the fanout-bound property can be
%% tested against an adversarial topology without a live peer_links
%% registry — see `macula_station_content_fanout_bound_tests'.
fanout_candidates(Conns, Seen) ->
    lists:sublist(
      [C || {Url, _Pid} = C <- Conns, not lists:member(Url, Seen)],
      ?GET_FANOUT_WIDTH).

%% This hop's chosen candidates join `seen' before being handed to the
%% NEXT hop, so a peer asked here is never asked again further out in
%% the same walk either.
advance_seen(Seen, Candidates) ->
    Seen ++ [Url || {Url, _Pid} <- Candidates].

safe_peer_connections() ->
    try macula_station_peer_links:connections()
    catch _:_ -> []
    end.

safe_call(LinkPid, Args) ->
    safe_content_call(LinkPid, <<"_content.get_block">>, Args,
                      ?REMOTE_BLOCK_PER_PEER_MS).

%% Station-to-station leg of content transfer (eager replication on
%% put, iterative fanout on get) — same dedicated-content-stream
%% primitive the daemon-facing side uses (PLAN_PER_STREAM_QUIC_
%% ISOLATION.md Phase 2), so a big block replicating between two
%% stations doesn't head-of-line-block SWIM/DHT/other traffic on
%% their peering connection either. Each call here is single-shot
%% (one block, not a multi-chunk transfer), so open, call, close —
%% no link-pinning-across-multiple-calls concern the daemon side has.
safe_content_call(LinkPid, Procedure, Args, TimeoutMs) ->
    on_content_stream_opened(
      safe_open_content_stream(LinkPid), LinkPid, Procedure, Args, TimeoutMs).

safe_open_content_stream(LinkPid) ->
    try macula_station_link:open_content_stream(LinkPid)
    catch _:_ -> {error, exception}
    end.

on_content_stream_opened({error, _} = E, _LinkPid, _Procedure, _Args,
                         _TimeoutMs) ->
    E;
on_content_stream_opened({ok, Stream}, LinkPid, Procedure, Args, TimeoutMs) ->
    Result = safe_call_on_stream(LinkPid, Stream, Procedure, Args, TimeoutMs),
    macula_station_link:close_content_stream(LinkPid, Stream),
    Result.

safe_call_on_stream(LinkPid, Stream, Procedure, Args, TimeoutMs) ->
    try macula_station_link:call_on_stream(LinkPid, Stream, ?CONTENT_REALM,
                                           Procedure, Args, TimeoutMs)
    catch _:_ -> {error, exception}
    end.

collect_first_block(_Tag, 0, _DeadlineMs) ->
    {ok, not_found};
collect_first_block(Tag, Remaining, DeadlineMs) ->
    Start = erlang:monotonic_time(millisecond),
    receive
        {Tag, {ok, Bin}} when is_binary(Bin) ->
            {ok, Bin};
        {Tag, _Other} ->
            Elapsed = erlang:monotonic_time(millisecond) - Start,
            collect_first_block(Tag, Remaining - 1,
                                max(0, DeadlineMs - Elapsed))
    after DeadlineMs ->
        {ok, not_found}
    end.
