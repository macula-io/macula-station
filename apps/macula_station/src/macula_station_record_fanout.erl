%% @doc Node-singleton fan-out for DHT record-stored events.
%%
%% Replaces the previous per-identity `macula_station_fact_publisher'
%% pool. Multiple virtual relay identities share a single fan-out
%% process that, on every record stored at any one identity's DHT,
%% classifies the record once and publishes it on EVERY identity's
%% per-identity `hecate_pubsub_server'. Subscribers attached to any
%% single identity therefore see records originating at any other
%% identity on the same box — same delivery semantics as the
%% pg-broadcast path it replaces, without the O(N) cast amplification
%% that drove per-publisher heaps to ~35 MB under load (Helsinki box
%% peaked at 1.8 GB / OOM during the 2026-04-29 outage).
%%
%% == Why a singleton ==
%%
%% Per-identity publishers each carried their own heap and each
%% received a copy of every record from every sibling via `pg'. Total
%% per-record work was O(N²) casts across N publishers. Default
%% `fullsweep_after = 65535' meant minor GCs collected young garbage
%% only — the old generation grew without bound until kernel pressure
%% triggered GC pauses long enough to miss QUIC heartbeats, dropping
%% daemons into a reconnect storm.
%%
%% This module collapses N publishers into one. Total per-record work
%% becomes O(N) `hecate_pubsub_server:publish/3' calls from a single
%% gen_server. The one heap is bounded by:
%%
%%   * `{spawn_opt, [{fullsweep_after, 10}]}' on start — every 10
%%     minor GCs forces a fullsweep that shrinks the heap.
%%   * `{noreply, S, hibernate}' return after each cast — caller-
%%     idiomatic heap-to-min for cast-heavy stateless gen_servers.
%%
%% == Identity discovery ==
%%
%% The fan-out does NOT cache the per-identity Phase-3 info. On every
%% record_stored cast it queries `macula_station_identity_registry'
%% via `phase3_snapshot/0' which returns the live `IdentityKey =>
%% #{pubsub_registry, peer_observer, identity}' map. The registry is
%% the source of truth for which identities exist; querying on
%% demand removes any cache-coherence problem when identities come and
%% go (admin reloads, supervisor restarts) and trades one extra
%% gen_server hop per record for code simplicity.
%%
%% == Test mode ==
%%
%% `start_link/1' accepts `#{initial_identities => Map}' to drive the
%% fan-out from a fixed map without needing the identity registry.
%% Tests use `on_record/3' (Pid form) to dispatch records into the
%% fixture instance. Production uses `start_link/0' (the singleton
%% form) and `on_record/2' (?MODULE form).
-module(macula_station_record_fanout).
-behaviour(gen_server).

-export([start_link/0, start_link/1, stop/1]).
-export([identity_ready/2, identity_gone/1, on_record/2, on_record/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([phase3_info/0]).

-define(MESH_REALM, <<0:256>>).

-type identity_key() :: macula_station_identity_registry:identity_key().
-type phase3_info() :: #{
    pubsub_registry := pid(),
    peer_observer   := pid(),
    identity        := macula_identity:key_pair(),
    sup             => pid()
}.

-record(state, {
    %% Test fixture: identities supplied at start_link/1; bypass the
    %% registry. Production: undefined; query registry on each cast.
    fixed_identities :: undefined | #{identity_key() := phase3_info()}
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, spawn_flags()).

-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) when is_map(Opts) ->
    gen_server:start_link(?MODULE, Opts, spawn_flags()).

spawn_flags() ->
    [{spawn_opt, [{fullsweep_after, 10}]}].

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Record an identity's Phase-3 readiness with the registry. The
%% fan-out itself holds no cache — this just stores the info in
%% `macula_station_identity_registry' so the next `record_stored'
%% cast finds it.
-spec identity_ready(identity_key(), phase3_info()) -> ok.
identity_ready(Key, Info) when is_map(Info) ->
    macula_station_identity_registry:set_phase3(Key, Info).

%% @doc Counterpart to `identity_ready/2'. Used on graceful identity
%% teardown; supervisor crashes drop the entry via the registry's
%% `'EXIT'' handler.
-spec identity_gone(identity_key()) -> ok.
identity_gone(Key) ->
    macula_station_identity_registry:clear_phase3(Key).

%% @doc DHT-side callback. Each per-identity DHT calls this when a
%% record store completes (own put or incoming custodian replicate).
%% `IdentityKey' identifies the source identity — currently advisory
%% (the fan-out broadcasts to all identities regardless), preserved
%% for future per-source filtering / loop suppression.
-spec on_record(identity_key(), macula_record:record()) -> ok.
on_record(IdentityKey, Record) ->
    gen_server:cast(?MODULE, {record_stored, IdentityKey, Record}).

%% @doc Test-friendly variant: target a specific fan-out pid instead
%% of the registered singleton.
-spec on_record(pid(), identity_key(), macula_record:record()) -> ok.
on_record(Pid, IdentityKey, Record) when is_pid(Pid) ->
    gen_server:cast(Pid, {record_stored, IdentityKey, Record}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) when is_map(Opts) ->
    {ok, init_state(maps:get(initial_identities, Opts, undefined)),
     hibernate}.

init_state(undefined) -> #state{fixed_identities = undefined};
init_state(Map) when is_map(Map) -> #state{fixed_identities = Map}.

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S, hibernate}.

handle_cast({record_stored, _IdentityKey, Record}, S) ->
    fan_out(Record, snapshot_identities(S)),
    {noreply, S, hibernate};
handle_cast(_Msg, S) ->
    {noreply, S, hibernate}.

handle_info(_Msg, S) ->
    {noreply, S, hibernate}.

terminate(_Reason, _S) -> ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Identity-set snapshot
%%====================================================================

snapshot_identities(#state{fixed_identities = Map}) when is_map(Map) ->
    maps:to_list(Map);
snapshot_identities(#state{fixed_identities = undefined}) ->
    %% Registry MUST be running in production; if it isn't, the box
    %% is mid-shutdown and dropping records is the right thing.
    try macula_station_identity_registry:phase3_snapshot()
    catch
        exit:{noproc, _}     -> [];
        exit:{shutdown, _}   -> [];
        exit:{normal, _}     -> []
    end.

%%====================================================================
%% Classification — moved verbatim from the deleted
%% `macula_station_fact_publisher'. The sole change is that the
%% classify clauses now feed `fan_out_to_each/3' (a list-of-identities
%% loop) rather than calling `publish_event/3' on a single per-identity
%% server. Everything else — record-type dispatch, payload-kind
%% extraction, the three key/value shape variants, the topic catalog
%% — is identical.
%%====================================================================

fan_out(Record, Identities) ->
    classify(macula_record:type(Record), Record, Identities).

classify(16#01, Record, Identities) ->
    Topic   = node_announce_topic(record_kind(Record)),
    Payload = macula_record:encode(Record),
    fan_out_to_each(Identities, Topic, Payload);
classify(16#0C, Record, Identities) ->
    classify_tombstone(macula_record:payload(Record), Record, Identities);
classify(_Type, _Record, _Identities) ->
    ok.

classify_tombstone(#{{text, <<"superseded_type">>} := 16#01} = P,
                   Record, Identities) ->
    fan_out_depart(P, Record, Identities);
classify_tombstone(#{superseded_type := 16#01} = P, Record, Identities) ->
    fan_out_depart(P, Record, Identities);
classify_tombstone(_, _, _) ->
    ok.

fan_out_depart(P, Record, Identities) ->
    Topic   = node_depart_topic(payload_kind(P)),
    Payload = macula_record:encode(Record),
    fan_out_to_each(Identities, Topic, Payload).

record_kind(Record) -> payload_kind(macula_record:payload(Record)).

payload_kind(#{{text, <<"kind">>} := V}) -> unwrap_kind(V);
payload_kind(#{<<"kind">>          := V}) -> unwrap_kind(V);
payload_kind(#{kind                := V}) -> unwrap_kind(V);
payload_kind(_)                            -> <<"station">>.

unwrap_kind({text, K}) when is_binary(K)                -> K;
unwrap_kind(K)         when is_binary(K)                -> K;
unwrap_kind(K)         when is_atom(K), K =/= undefined -> atom_to_binary(K, utf8);
unwrap_kind(_)                                          -> <<"station">>.

node_announce_topic(<<"daemon">>) -> <<"_mesh.daemon.announced_v1">>;
node_announce_topic(_)            -> <<"_mesh.station.announced_v1">>.

node_depart_topic(<<"daemon">>) -> <<"_mesh.daemon.departed_v1">>;
node_depart_topic(_)            -> <<"_mesh.station.departed_v1">>.

%%====================================================================
%% Fan-out — for each identity on this node, publish on its
%% pubsub_server (signed under that identity) and deliver the EVENT
%% frame to every matched local subscriber via that identity's
%% peer_observer. One Payload binary, N publishes, N×Subs deliveries.
%%====================================================================

fan_out_to_each([], _Topic, _Payload) -> ok;
fan_out_to_each([{_Key, Info} | Rest], Topic, Payload) ->
    publish_on(Info, Topic, Payload),
    fan_out_to_each(Rest, Topic, Payload).

publish_on(#{pubsub_registry := PsReg,
             peer_observer   := Obs,
             identity        := Kp}, Topic, Payload) ->
    do_publish(hecate_pubsub_registry:register(PsReg, ?MESH_REALM, Kp),
               Obs, Topic, Payload);
publish_on(_Malformed, _Topic, _Payload) ->
    ok.

do_publish({ok, Server}, Obs, Topic, Payload) ->
    {Frame, Subs} = hecate_pubsub_server:publish(Server, Topic, Payload),
    deliver(Subs, Frame, Obs);
do_publish({error, _Reason}, _Obs, _Topic, _Payload) ->
    ok.

deliver([], _Frame, _Obs) -> ok;
deliver([NodeId | Rest], Frame, Obs) ->
    deliver_to(macula_station_peer_observer:conn_for(Obs, NodeId), Frame),
    deliver(Rest, Frame, Obs).

deliver_to({ok, ConnPid}, Frame) ->
    catch macula_peering:send_frame(ConnPid, Frame),
    ok;
deliver_to(error, _Frame) ->
    ok.
