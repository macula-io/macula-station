%% @doc Singleton fan-out for DHT record-stored events.
%%
%% Replaces the previous per-identity `macula_station_fact_publisher'
%% pool. The DHT's `on_record_stored' callback drops every stored
%% record into this gen_server, which classifies the record once and
%% publishes it on the station's `hecate_pubsub_server' for the
%% mesh-realm so local subscribers see `_mesh.station.announced_v1' /
%% `_mesh.daemon.announced_v1' / `_mesh.station.departed_v1' /
%% `_mesh.daemon.departed_v1' events without having to read the DHT.
%%
%% == Heap discipline ==
%%
%% Bounded by:
%%
%%   * `{spawn_opt, [{fullsweep_after, 10}]}' on start — every 10
%%     minor GCs forces a fullsweep that shrinks the heap.
%%   * `{noreply, S, hibernate}' return after each cast — caller-
%%     idiomatic heap-to-min for cast-heavy stateless gen_servers.
%%
%% This avoided the OOM the per-identity `fact_publisher' pool drove
%% during the 2026-04-29 outage; preserved here even though there is
%% only one fan-out process now.
%%
%% == Wiring ==
%%
%% The fan-out is started by `macula_station_sup' before the boot
%% pipeline runs (so the disabled-env smoke tests can assert it sits
%% in the supervisor's child list). It boots in `unwired' mode: any
%% inbound `record_stored' cast is dropped silently. The boot pipeline
%% calls `set_wiring/1' after `pubsub_registry' + `peer_observer' are
%% up, planting the pids the fan-out needs to publish + deliver. From
%% that point onwards casts publish.
%%
%% == Test mode ==
%%
%% `start_link/1' accepts a wiring map directly so tests skip the
%% `set_wiring/1' step. `on_record/2' (Pid form) targets a specific
%% fan-out pid; `on_record/1' (?MODULE form) targets the singleton.
-module(macula_station_record_fanout).
-behaviour(gen_server).

-export([start_link/0, start_link/1, stop/1]).
-export([set_wiring/1, clear_wiring/0, on_record/1, on_record/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([wiring/0]).

-define(MESH_REALM, <<0:256>>).

-type wiring() :: #{
    pubsub_registry := pid(),
    peer_observer   := pid(),
    identity        := macula_identity:key_pair()
}.

-record(state, {
    wiring :: wiring() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, spawn_flags()).

%% @doc Test entry — start unregistered with the wiring already
%% planted. Production uses `start_link/0' + `set_wiring/1'.
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) when is_map(Opts) ->
    gen_server:start_link(?MODULE, Opts, spawn_flags()).

spawn_flags() ->
    [{spawn_opt, [{fullsweep_after, 10}]}].

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Plant the wiring the fan-out needs to publish events. Called
%% by the boot pipeline once `pubsub_registry' + `peer_observer' are
%% up. Cast (not call) so the boot pipeline never blocks on us.
-spec set_wiring(wiring()) -> ok.
set_wiring(Wiring) when is_map(Wiring) ->
    gen_server:cast(?MODULE, {set_wiring, Wiring}).

%% @doc Drop the wiring (graceful teardown).
-spec clear_wiring() -> ok.
clear_wiring() ->
    gen_server:cast(?MODULE, clear_wiring).

%% @doc DHT-side callback. The DHT calls this after every successful
%% `store_put' (own put + incoming custodian STORE).
-spec on_record(macula_record:record()) -> ok.
on_record(Record) ->
    gen_server:cast(?MODULE, {record_stored, Record}).

%% @doc Test-friendly variant: target a specific fan-out pid instead
%% of the registered singleton.
-spec on_record(pid(), macula_record:record()) -> ok.
on_record(Pid, Record) when is_pid(Pid) ->
    gen_server:cast(Pid, {record_stored, Record}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) when is_map(Opts) ->
    {ok, #state{wiring = init_wiring(Opts)}, hibernate}.

init_wiring(#{pubsub_registry := _, peer_observer := _,
              identity := _} = Wiring) ->
    Wiring;
init_wiring(_) ->
    undefined.

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S, hibernate}.

handle_cast({set_wiring, Wiring}, S) ->
    {noreply, S#state{wiring = init_wiring(Wiring)}, hibernate};
handle_cast(clear_wiring, S) ->
    {noreply, S#state{wiring = undefined}, hibernate};
handle_cast({record_stored, Record}, #state{wiring = W} = S) ->
    fan_out(Record, W),
    {noreply, S, hibernate};
handle_cast(_Msg, S) ->
    {noreply, S, hibernate}.

handle_info(_Msg, S) ->
    {noreply, S, hibernate}.

terminate(_Reason, _S) -> ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Classification — moved verbatim from the deleted
%% `macula_station_fact_publisher'. Record-type dispatch, payload-kind
%% extraction, the three key/value shape variants, the topic catalog —
%% all identical.
%%====================================================================

fan_out(_Record, undefined) ->
    ok;
fan_out(Record, #{} = Wiring) ->
    classify(macula_record:type(Record), Record, Wiring).

classify(16#01, Record, Wiring) ->
    Topic   = node_announce_topic(record_kind(Record)),
    Payload = macula_record:encode(Record),
    publish_on(Wiring, Topic, Payload);
classify(16#0C, Record, Wiring) ->
    classify_tombstone(macula_record:payload(Record), Record, Wiring);
classify(_Type, _Record, _Wiring) ->
    ok.

classify_tombstone(#{{text, <<"superseded_type">>} := 16#01} = P,
                   Record, Wiring) ->
    fan_out_depart(P, Record, Wiring);
classify_tombstone(#{superseded_type := 16#01} = P, Record, Wiring) ->
    fan_out_depart(P, Record, Wiring);
classify_tombstone(_, _, _) ->
    ok.

fan_out_depart(P, Record, Wiring) ->
    Topic   = node_depart_topic(payload_kind(P)),
    Payload = macula_record:encode(Record),
    publish_on(Wiring, Topic, Payload).

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
%% Publish — register the mesh-realm pubsub_server (cheap idempotent
%% lookup), publish the topic + payload, deliver the resulting EVENT
%% frame to every matched local subscriber via the peer_observer.
%%====================================================================

publish_on(#{pubsub_registry := PsReg,
             peer_observer   := Obs,
             identity        := Kp}, Topic, Payload) ->
    do_publish(hecate_pubsub_registry:register(PsReg, ?MESH_REALM, Kp),
               Obs, Topic, Payload).

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
