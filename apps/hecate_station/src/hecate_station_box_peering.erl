%%% @doc Per-box peering coordinator.
%%%
%%% V1 macula-relay had a singleton `macula_relay_peering' that
%%% connected the BOX to peer boxes — one outbound QUIC per peer, with
%%% periodic retry. The 100 virtual identities running on the box
%%% piggybacked on this single set of physical connections; identity
%%% mesh formation was BOX-level, not per-identity.
%%%
%%% V2 dropped this — each identity was wired to dial peers directly,
%%% which (a) flooded the relay fleet with 100×N outbound QUIC
%%% sessions per box, and (b) left every box as an isolated pg gossip
%%% island. The realm subscribing to fi-helsinki saw only identities
%%% running on the Helsinki box; cross-box identities never reached it.
%%%
%%% This module restores V1's box-level mesh formation + cross-box
%%% identity gossip:
%%%
%%%   * On boot, derive a box-level identity from `box_secret', open
%%%     ONE `macula_station_link' to each peer box.
%%%   * Subscribe to the four `_mesh.{station,daemon}.{announced,
%%%     departed}_v1' topics on each peer box's mesh-realm pubsub_server.
%%%   * On inbound EVENT: decode the carried `node_record' and bridge
%%%     it into the local `hecate_station_facts' pg gossip group via
%%%     `cross_publish' casts to every local fact_publisher. Each local
%%%     fact_publisher then re-fires the EVENT on its OWN pubsub_server,
%%%     so realm subscribers attached to ANY local identity receive
%%%     records announced on ANY peer box.
%%%
%%% On disconnect, retry every 30s.
-module(hecate_station_box_peering).
-behaviour(gen_server).

-export([start_link/0, stop/1, peer_urls/0, connections/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(RECONNECT_INTERVAL_MS, 30_000).
-define(MESH_REALM,            <<0:256>>).
-define(PG_SCOPE,              hecate_station_facts).
-define(PG_GROUP,              mesh_realm).

%% Topics this box bridges from peer boxes into the local gossip
%% group. Mirrors the realm's subscription set in
%% MaculaRealm.Topology.MeshSubscriber.
-define(BRIDGED_TOPICS, [
    <<"_mesh.station.announced_v1">>,
    <<"_mesh.station.departed_v1">>,
    <<"_mesh.daemon.announced_v1">>,
    <<"_mesh.daemon.departed_v1">>
]).

-record(state, {
    identity  :: macula_identity:key_pair(),
    peers     :: [binary()],
    links     :: #{binary() => pid()},
    subs      :: #{{binary(), binary()} => reference()},
    monitors  :: #{reference() => binary()}
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Configured peer URLs (env-supplied list).
-spec peer_urls() -> [binary()].
peer_urls() ->
    try gen_server:call(?MODULE, peer_urls, 1_000)
    catch _:_ -> []
    end.

%% @doc Live `[{Url, LinkPid}]' connections.
-spec connections() -> [{binary(), pid()}].
connections() ->
    try gen_server:call(?MODULE, connections, 1_000)
    catch _:_ -> []
    end.

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    process_flag(trap_exit, true),
    Identity = box_identity(),
    Peers    = peers_from_env(),
    State    = #state{identity = Identity,
                      peers    = Peers,
                      links    = #{},
                      subs     = #{},
                      monitors = #{}},
    self() ! reconnect_loop,
    {ok, State}.

handle_call(peer_urls, _From, #state{peers = Peers} = S) ->
    {reply, Peers, S};
handle_call(connections, _From, #state{links = Links} = S) ->
    {reply, maps:to_list(Links), S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(reconnect_loop, S) ->
    erlang:send_after(?RECONNECT_INTERVAL_MS, self(), reconnect_loop),
    {noreply, ensure_links(S)};

%% Inbound EVENT from a peer box's pubsub_server. station_link
%% delivers `{macula_event, SubRef, Topic, Payload, Meta}'. Payload
%% is the CBOR-encoded `macula_record:record()'.
handle_info({macula_event, _SubRef, Topic, Payload, _Meta}, S)
  when is_binary(Payload) ->
    on_peer_event(Topic, Payload),
    {noreply, S};
handle_info({macula_event_gone, _SubRef, _Reason}, S) ->
    %% station_link is signaling the subscription went away. We'll
    %% rebuild on the next reconnect_loop tick.
    {noreply, S};
handle_info({'DOWN', Ref, process, _Pid, _Reason},
            #state{monitors = Mons, links = Links, subs = Subs} = S) ->
    case maps:find(Ref, Mons) of
        {ok, Url} ->
            logger:warning("[box_peering] link to ~s dropped — will retry", [Url]),
            Subs1 = maps:filter(fun({U, _T}, _) -> U =/= Url end, Subs),
            {noreply, S#state{links    = maps:remove(Url, Links),
                              subs     = Subs1,
                              monitors = maps:remove(Ref, Mons)}};
        error ->
            {noreply, S}
    end;
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.

code_change(_Old, S, _Extra) -> {ok, S}.

%%====================================================================
%% Reconnect / subscribe
%%====================================================================

ensure_links(#state{peers = Peers} = S) ->
    lists:foldl(fun ensure_one_link/2, S, Peers).

ensure_one_link(Url, #state{links = Links} = S) ->
    case maps:is_key(Url, Links) of
        true  -> ensure_subs(Url, S);
        false -> open_link(Url, S)
    end.

open_link(Url, #state{identity = Kp, links = Links, monitors = Mons} = S) ->
    case macula_station_link:start_link(#{seed => Url, identity => Kp}) of
        {ok, Pid} ->
            unlink(Pid),                  %% don't tie our death to the link
            Ref = erlang:monitor(process, Pid),
            logger:info("[box_peering] connected to ~s pid=~p", [Url, Pid]),
            ensure_subs(Url,
                        S#state{links    = Links#{Url => Pid},
                                monitors = Mons#{Ref => Url}});
        {error, Reason} ->
            logger:warning(
              "[box_peering] dial ~s failed: ~p", [Url, Reason]),
            S
    end.

%% Idempotent: subscribe to BRIDGED_TOPICS on this peer if we haven't
%% already. Subscribe failures are warnings — next reconnect tick
%% retries.
ensure_subs(Url, #state{links = Links, subs = Subs} = S) ->
    LinkPid = maps:get(Url, Links),
    Subs1   = lists:foldl(
                fun(Topic, Acc) ->
                        ensure_one_sub(LinkPid, Url, Topic, Acc)
                end, Subs, ?BRIDGED_TOPICS),
    S#state{subs = Subs1}.

ensure_one_sub(LinkPid, Url, Topic, Subs) ->
    Key = {Url, Topic},
    case maps:is_key(Key, Subs) of
        true  -> Subs;
        false -> subscribe_one(LinkPid, Url, Topic, Subs)
    end.

subscribe_one(LinkPid, Url, Topic, Subs) ->
    try macula_station_link:subscribe(
          LinkPid, ?MESH_REALM, Topic, self()) of
        {ok, SubRef} -> Subs#{{Url, Topic} => SubRef}
    catch _:Reason ->
        logger:warning(
          "[box_peering] subscribe ~s on ~s failed: ~p",
          [Topic, Url, Reason]),
        Subs
    end.

%%====================================================================
%% Bridge inbound peer events into local pg gossip
%%====================================================================

%% The frame's payload is a CBOR-encoded macula_record. Decode, then
%% cross-publish into the local fact-publisher pg group so every
%% local identity's pubsub_server fan-outs to its remote subscribers
%% (incl. the realm's station_link subscription).
on_peer_event(Topic, Payload) ->
    case macula_record:decode(Payload) of
        {ok, Record} ->
            broadcast_to_local_facts(Topic, Record);
        {error, _Reason} ->
            ok
    end.

broadcast_to_local_facts(_Topic, Record) ->
    Members = pg_members(?PG_SCOPE, ?PG_GROUP),
    [gen_server:cast(Pid, {cross_publish, Record}) || Pid <- Members],
    ok.

pg_members(Scope, Group) ->
    try pg:get_members(Scope, Group)
    catch _:_ -> []
    end.

%%====================================================================
%% Identity + config
%%====================================================================

%% Box-level keypair. Derived from the box_secret with a fixed
%% sentinel so it's stable across restarts but distinct from every
%% per-hostname identity. Falls back to an ephemeral generated keypair
%% if box_secret can't be loaded — that path is mainly tests.
box_identity() ->
    case hecate_station_identity_config:load_box_secret() of
        {ok, BoxSecret} ->
            hecate_station_identity_keys:derive(BoxSecret,
                                                <<"__box_peering__">>);
        _ ->
            macula_identity:generate()
    end.

%% Read MACULA_PEER_BOXES (preferred) or MACULA_OVERLAY_SEEDS (legacy
%% from earlier in this fix sequence). Comma-separated URLs.
peers_from_env() ->
    case env_csv("MACULA_PEER_BOXES") of
        []    -> env_csv("MACULA_OVERLAY_SEEDS");
        Other -> Other
    end.

env_csv(Var) ->
    case os:getenv(Var) of
        false -> [];
        ""    -> [];
        S     -> [list_to_binary(string:trim(E))
                  || E <- string:split(S, ",", all),
                     string:trim(E) =/= ""]
    end.
