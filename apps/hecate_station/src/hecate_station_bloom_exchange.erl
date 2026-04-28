%%% @doc Per-identity Bloom filter exchange.
%%%
%%% Ported from V1 `macula_relay_bloom_exchange'. Owns the local
%%% Bloom filter (rebuilt from this identity's pubsub_server topics)
%%% and tracks peer filters received via gossip on the
%%% `_mesh.bloom' topic.
%%%
%%% On a 30s tick the manager rebuilds its local filter from
%%% `hecate_pubsub_server:topics/1' and broadcasts the 1KB binary to
%%% every connected peer station via the overlay seeder's outbound
%%% station_links. Peers' inbound Bloom-event handler calls
%%% `receive_peer_bloom/3' which we cache.
%%%
%%% The peering forwarder consults `peer_blooms/1' to skip publishes
%%% to peers whose filter doesn't match — preventing the cross-relay
%%% flooding that would otherwise hit O(N^2) topics × peers.
-module(hecate_station_bloom_exchange).
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
    identity        := macula_identity:key_pair(),
    overlay_seeder  => pid(),
    identity_key    => term()
}.

-export_type([opts/0]).

-record(state, {
    pubsub_registry :: pid(),
    identity        :: macula_identity:key_pair(),
    overlay_seeder  :: pid() | undefined,
    local_bloom     :: binary(),
    peer_blooms     :: #{binary() => binary()},
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
    catch _:_ -> hecate_station_bloom:to_binary(hecate_station_bloom:new())
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

init(#{pubsub_registry := Reg, identity := Kp} = Opts) ->
    process_flag(trap_exit, true),
    set_logger_identity(Opts),
    State = #state{
        pubsub_registry = Reg,
        identity        = Kp,
        overlay_seeder  = maps:get(overlay_seeder, Opts, undefined),
        local_bloom     = empty_bloom_bin(),
        peer_blooms     = #{}
    },
    {ok, schedule_rebuild(State)}.

set_logger_identity(#{identity_key := Key}) ->
    logger:set_process_metadata(#{identity_id => Key});
set_logger_identity(_) -> ok.

handle_call(get_local_bloom, _From, #state{local_bloom = LB} = S) ->
    {reply, LB, S};
handle_call(peer_blooms, _From, #state{peer_blooms = PB} = S) ->
    {reply, PB, S};
handle_call({peer_matches, Topic}, _From, #state{peer_blooms = PB} = S) ->
    Matches = [Host
               || {Host, BloomBin} <- maps:to_list(PB),
                  byte_size(BloomBin) =:= 1024,
                  hecate_station_bloom:check(
                    Topic, hecate_station_bloom:from_binary(BloomBin))],
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
    {noreply, schedule_rebuild(do_rebuild(S))};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Rebuild + broadcast
%%====================================================================

do_rebuild(#state{pubsub_registry = Reg, identity = Kp,
                  overlay_seeder = SeedPid} = S) ->
    Topics = safe_topics(Reg, Kp),
    BF = lists:foldl(fun hecate_station_bloom:add/2,
                     hecate_station_bloom:new(), Topics),
    BloomBin = hecate_station_bloom:to_binary(BF),
    broadcast_filter(SeedPid, BloomBin),
    S#state{local_bloom = BloomBin}.

%% Resolve the per-identity mesh-realm pubsub_server through the
%% registry — fact_publisher already eagerly registers it on init,
%% so the lookup is fast. Any failure (registry down, server gone)
%% is treated as "no topics yet" and we'll retry on the next tick.
safe_topics(Reg, Kp) ->
    try hecate_pubsub_registry:register(Reg, ?MESH_REALM, Kp) of
        {ok, ServerPid} ->
            try hecate_pubsub_server:topics(ServerPid)
            catch _:_ -> []
            end;
        _ -> []
    catch _:_ -> []
    end.

%% Broadcast `_mesh.bloom' to every active outbound station_link.
%% No-op when the seeder has no live connections — peer caches are
%% updated on the next rebuild tick once seeders reconnect.
broadcast_filter(undefined, _BloomBin) -> ok;
broadcast_filter(SeedPid, BloomBin) ->
    Conns = safe_seeder_conns(SeedPid),
    lists:foreach(
      fun({_Url, LinkPid}) ->
              catch macula_station_link:publish(
                      LinkPid, ?MESH_REALM, <<"_mesh.bloom">>, BloomBin)
      end,
      Conns),
    ok.

%% Hidden API on the seeder — see `connections/1' below. Kept here as
%% a `safe_*' wrapper so a hung seeder doesn't take the exchange down.
safe_seeder_conns(SeedPid) ->
    try hecate_station_overlay_seeder:connections(SeedPid)
    catch _:_ -> []
    end.

empty_bloom_bin() ->
    hecate_station_bloom:to_binary(hecate_station_bloom:new()).

schedule_rebuild(S) ->
    Ref = make_ref(),
    erlang:send_after(?REBUILD_INTERVAL_MS, self(), {rebuild, Ref}),
    S#state{timer_ref = Ref}.
