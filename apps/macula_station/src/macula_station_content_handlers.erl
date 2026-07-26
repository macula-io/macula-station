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

%% Default hop budget for a daemon's `_content.get_block' that
%% doesn't supply its own. 1 hop only — 2-hop iteration was
%% overwhelming the fleet's macula_content_store gen_server under
%% torture (each failed lookup amplified to 8² = 64 concurrent
%% inbound `_content.get_block' calls per relay), and after a few
%% rounds peer_observer crash-recovered, taking the conn_for ETS
%% mirror with it and cascading every other test through stale
%% conn lookups.
%%
%% 1 hop covers the case where writer + reader's relays share at
%% least one peer. For the worst-case partial-mesh pair with no
%% mutual peer, the cross-station `_content.get_block' will return
%% `not_found' until eager content replication lands (Day 3 / chunked
%% manifests). The single-station path is unaffected.
-define(DEFAULT_HOPS, 1).

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
    try macula_station_link:call(LinkPid, ?CONTENT_REALM,
                                  <<"_content.put_block">>,
                                  Args, ?REPLICATION_TIMEOUT_MS)
    catch _:_ -> {error, exception}
    end.

handle_get_manifest(#{mcid := MCID}) when is_binary(MCID) ->
    classify_get_manifest(macula_content_store:get_manifest(MCID));
handle_get_manifest(_Other) ->
    {error, bad_request}.

classify_get_manifest({ok, Manifest})        -> {ok, Manifest};
classify_get_manifest({error, not_found})    -> {ok, not_found}.

handle_get_block(#{mcid := MCID} = Args) when is_binary(MCID) ->
    on_local_block(macula_content_store:get_block(MCID), MCID, Args);
handle_get_block(_Other) ->
    {error, bad_request}.

%% Local hit short-circuits. Local miss falls back to multi-hop
%% iterative fetch against peer stations — necessary because the v1
%% put path stores ONLY locally on the writer's relay, and the
%% reader (here) might be a different relay. Fans
%% `_content.get_block' RPCs out to every peer link in parallel,
%% returns the first peer that holds the block.
%%
%% The `hops_remaining' arg counts down hops the receiving handler
%% is still allowed to walk. A daemon's first call arrives without
%% the field — defaults to `?DEFAULT_HOPS' (2 in our partial-mesh
%% topology, sufficient for the worst-case 2-hop path between
%% centrum and haasrode). Each peer-to-peer iteration decrements
%% the counter; reaching 0 stops at local lookup. Without multi-hop,
%% partial-mesh pairs that lack a direct edge can't reach a block
%% held only on the writer's relay.
on_local_block({ok, Block}, _MCID, _Args) ->
    {ok, Block};
on_local_block({error, not_found}, MCID, Args) ->
    Hops = maps:get(hops_remaining, Args, ?DEFAULT_HOPS),
    on_iterative_decision(Hops, MCID).

on_iterative_decision(0, _MCID) ->
    {ok, not_found};
on_iterative_decision(N, MCID) when N > 0 ->
    fanout_remote_get_block(MCID, N - 1).

fanout_remote_get_block(MCID, NextHops) ->
    Conns = safe_peer_connections(),
    on_peer_count(length(Conns), MCID, NextHops, Conns).

on_peer_count(0, _MCID, _NextHops, _Conns) ->
    {ok, not_found};
on_peer_count(_N, MCID, NextHops, Conns) ->
    Tag    = make_ref(),
    Parent = self(),
    Args   = #{mcid => MCID, hops_remaining => NextHops},
    [spawn(fun() ->
        Reply = safe_call(LinkPid, Args),
        Parent ! {Tag, Reply}
     end) || {_Url, LinkPid} <- Conns],
    collect_first_block(Tag, length(Conns),
                        ?REMOTE_BLOCK_PER_PEER_MS + 200).

safe_peer_connections() ->
    try macula_station_peer_links:connections()
    catch _:_ -> []
    end.

safe_call(LinkPid, Args) ->
    try macula_station_link:call(LinkPid, ?CONTENT_REALM,
                                  <<"_content.get_block">>,
                                  Args, ?REMOTE_BLOCK_PER_PEER_MS)
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
