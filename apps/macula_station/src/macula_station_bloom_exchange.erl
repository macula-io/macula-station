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
         get_local_bloom/1, peer_blooms/1, peer_matches/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(REBUILD_INTERVAL_MS, 30_000).

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
    timer_ref       :: reference() | undefined
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

%% @doc List of peer hostnames whose Bloom filter matches `Topic'.
%% Drives the forwarder's "skip uninterested peers" decision. Peers
%% with no known filter (haven't gossipped yet) are NOT included —
%% callers should treat absence as "unknown, don't forward yet".
-spec peer_matches(pid(), binary()) -> [binary()].
peer_matches(Pid, Topic) when is_binary(Topic) ->
    try gen_server:call(Pid, {peer_matches, Topic}, 500)
    catch _:_ -> []
    end.

%%====================================================================
%% gen_server
%%====================================================================

init(#{pubsub_registry := Reg, identity := Kp}) ->
    process_flag(trap_exit, true),
    State = #state{
        pubsub_registry = Reg,
        identity        = Kp,
        local_bloom     = empty_bloom_bin(),
        peer_blooms     = #{},
        subs            = #{}
    },
    {ok, schedule_rebuild(State)}.

handle_call(get_local_bloom, _From, #state{local_bloom = LB} = S) ->
    {reply, LB, S};
handle_call(peer_blooms, _From, #state{peer_blooms = PB} = S) ->
    {reply, PB, S};
handle_call({peer_matches, Topic}, _From, #state{peer_blooms = PB} = S) ->
    Matches = [Host
               || {Host, BloomBin} <- maps:to_list(PB),
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
    {noreply, S#state{peer_blooms = PB#{Host => BloomBin}}};
handle_cast({peer_bloom, _, _}, S) ->
    {noreply, S};
handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({rebuild, Ref}, #state{timer_ref = Ref} = S) ->
    S1 = sync_inbound_subs(do_rebuild(S)),
    {noreply, schedule_rebuild(S1)};
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
        false -> {noreply, S#state{peer_blooms = PB#{Publisher => Payload}}}
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

do_rebuild(#state{pubsub_registry = Reg} = S) ->
    Topics = safe_topics(Reg),
    BF = lists:foldl(fun macula_station_bloom:add/2,
                     macula_station_bloom:new(), Topics),
    BloomBin = macula_station_bloom:to_binary(BF),
    broadcast_filter(BloomBin),
    S#state{local_bloom = BloomBin}.

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

empty_bloom_bin() ->
    macula_station_bloom:to_binary(macula_station_bloom:new()).

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
