%% @doc Per-identity fact publisher — translates records landing in
%% the local DHT into typed business events on the peering pubsub
%% channel.
%%
%% Whenever the per-identity `hecate_dht_server' completes a
%% `store_put' (own `put_record/2' OR an incoming custodian STORE
%% replicated by a peer), it fires the `on_record_stored' callback
%% supplied at start_link time. That callback casts to this
%% gen_server, which classifies the record by `(type, payload.kind)'
%% and produces a typed EVENT frame on a fixed catalog of mesh-level
%% topics:
%%
%% <table>
%%   <tr><th>Record type</th><th>Payload</th><th>Topic</th></tr>
%%   <tr><td>0x01 (node_record)</td><td>`kind = station' (or absent)</td>
%%       <td>`_mesh.station.announced_v1'</td></tr>
%%   <tr><td>0x01 (node_record)</td><td>`kind = daemon'</td>
%%       <td>`_mesh.daemon.announced_v1'</td></tr>
%%   <tr><td>0x0C (tombstone)</td><td>`superseded_type = 0x01' + station</td>
%%       <td>`_mesh.station.departed_v1'</td></tr>
%%   <tr><td>0x0C (tombstone)</td><td>`superseded_type = 0x01' + daemon</td>
%%       <td>`_mesh.daemon.departed_v1'</td></tr>
%% </table>
%%
%% The publisher is realm-agnostic — every event lives in the
%% all-zeros realm, matching the station's "infrastructure, not
%% identity" positioning per Sprint A. Realm-level facts (license,
%% membership) are NOT this module's concern; those classify clauses
%% are reserved for the future realm service.
%%
%% == Fan-out ==
%%
%% Each event goes through the per-identity `hecate_pubsub_registry'
%% (mesh realm key), which builds the signed EVENT frame and returns
%% the matched local subscribers. The publisher then resolves each
%% subscriber pubkey to a `macula_peering' connection pid via the
%% per-identity `hecate_station_peer_observer' and pushes the frame
%% with `macula_peering:send_frame/2'. Subscribers whose connections
%% are gone are dropped silently — their UNSUBSCRIBE will land
%% naturally on reconnect, or the peering layer will reap them.
-module(hecate_station_fact_publisher).
-behaviour(gen_server).

-export([start_link/1, stop/1, on_record_stored/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{
    pubsub_registry := pid(),
    peer_observer   := pid(),
    identity        := macula_identity:key_pair(),
    identity_key    => term()
}.

-record(state, {
    pubsub_registry :: pid(),
    peer_observer   :: pid(),
    identity        :: macula_identity:key_pair()
}).

%% Mesh-level events live in the all-zeros realm. Realm-level facts
%% (membership, license) will use real realm tags when the realm
%% service grows them.
-define(MESH_REALM, <<0:256>>).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{pubsub_registry := _, peer_observer := _, identity := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% @doc Cast a record-stored notification. Bound into the DHT
%% server's `on_record_stored' callback by the identity_sup. Cast
%% (not call) so a slow publisher cannot block the DHT server.
-spec on_record_stored(pid(), macula_record:record()) -> ok.
on_record_stored(Pid, Record) ->
    gen_server:cast(Pid, {record_stored, Record}).

%%====================================================================
%% gen_server
%%====================================================================

init(#{pubsub_registry := Reg, peer_observer := Obs,
       identity := Kp} = Opts) ->
    set_logger_identity(Opts),
    {ok, #state{pubsub_registry = Reg,
                peer_observer   = Obs,
                identity        = Kp}}.

set_logger_identity(#{identity_key := Key}) ->
    logger:set_process_metadata(#{identity_id => Key});
set_logger_identity(_) ->
    ok.

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast({record_stored, Record}, S) ->
    classify(macula_record:type(Record), Record, S),
    {noreply, S};
handle_cast(_, S) ->
    {noreply, S}.

handle_info(_, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%====================================================================
%% Classification
%%====================================================================

%% Record type 0x01 — node_record (station or daemon presence).
classify(16#01, Record, S) ->
    publish_event(node_announce_topic(record_kind(Record)), Record, S);
%% Record type 0x0C — tombstone. Look at the superseded type to
%% decide which depart event to fire.
classify(16#0C, Record, S) ->
    classify_tombstone(macula_record:payload(Record), Record, S);
%% Realm-level record types (0x20+) live with the realm service;
%% the station does not classify them.
classify(_Type, _Record, _S) ->
    ok.

classify_tombstone(#{{text, <<"superseded_type">>} := 16#01} = P, Record, S) ->
    publish_event(node_depart_topic(payload_kind(P)), Record, S);
classify_tombstone(#{superseded_type := 16#01} = P, Record, S) ->
    publish_event(node_depart_topic(payload_kind(P)), Record, S);
classify_tombstone(_, _, _) ->
    ok.

%% Extract the actor kind from a node_record. Defaults to
%% `<<"station">>' for records without the field — node_records
%% predate the kind field and were station-only.
record_kind(Record) ->
    payload_kind(macula_record:payload(Record)).

%% Tagged-text shape per the CBOR canonical form `node_record'
%% emits — `{text, K} => {text, V}'. The atom-key clause covers
%% Elixir-decoded payloads where keys turn into atoms.
payload_kind(#{{text, <<"kind">>} := {text, K}}) when is_binary(K) -> K;
payload_kind(#{<<"kind">>          := K})        when is_binary(K) -> K;
payload_kind(#{kind                := K})        when is_binary(K) -> K;
payload_kind(_)                                                    -> <<"station">>.

node_announce_topic(<<"daemon">>) -> <<"_mesh.daemon.announced_v1">>;
node_announce_topic(_)            -> <<"_mesh.station.announced_v1">>.

node_depart_topic(<<"daemon">>) -> <<"_mesh.daemon.departed_v1">>;
node_depart_topic(_)            -> <<"_mesh.station.departed_v1">>.

%%====================================================================
%% Publish + fan out
%%====================================================================

publish_event(Topic, Record, S) ->
    publish_to(ensure_server(S), Topic, Record, S).

publish_to({ok, Server}, Topic, Record, S) ->
    Payload                = macula_record:encode(Record),
    {Frame, Subscribers}   = hecate_pubsub_server:publish(Server, Topic, Payload),
    fan_out(Subscribers, Frame, S);
publish_to({error, _Reason}, _Topic, _Record, _S) ->
    ok.

ensure_server(#state{pubsub_registry = Reg, identity = Kp}) ->
    hecate_pubsub_registry:register(Reg, ?MESH_REALM, Kp).

fan_out([], _Frame, _S) ->
    ok;
fan_out([NodeId | Rest], Frame, S) ->
    deliver_to(NodeId, Frame, S),
    fan_out(Rest, Frame, S).

deliver_to(NodeId, Frame, #state{peer_observer = Obs}) ->
    deliver_via_conn(hecate_station_peer_observer:conn_for(Obs, NodeId), Frame).

deliver_via_conn({ok, ConnPid}, Frame) ->
    catch macula_peering:send_frame(ConnPid, Frame),
    ok;
deliver_via_conn(error, _Frame) ->
    %% Subscriber's connection is gone. The pubsub_server will learn
    %% via inbound UNSUBSCRIBE on reconnect (or never, if the peer
    %% stays gone — pubsub_server has no liveness tracking yet).
    ok.
