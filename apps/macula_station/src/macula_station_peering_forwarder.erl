%%% @doc Outbound peering forwarder for one (Topic, Peer station-link)
%%% pair. Singleton-station slice — there is one forwarder process per
%%% (Topic, PeerLinkPid) tuple.
%%%
%%% Ported from V1 `macula_relay_peering_forwarder' (which was rooted
%%% in pg group `pg' / `relay_topic'). The pg scope is the constant
%%% `?PG_SCOPE' below — multi-identity-era per-identity scoping was
%%% removed when the relay collapsed to one identity per BEAM.
%%%
%%% On `{relay_publish, Topic, Payload}' messages received from the
%%% local pg group, this forwarder hands the payload off to a single
%%% peer station link via `macula_station_link:publish/4'. Failures
%%% are logged with topic + peer pid for diagnostic trail (the
%%% pre-2026-04 V1 behaviour was a bare `catch' that dropped silently).
-module(macula_station_peering_forwarder).
-behaviour(gen_server).

-export([start_link/1, stop/1, publish/1, pg_scope/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% pg scopes are global per VM. Single-identity per BEAM means a
%% single constant scope is sufficient — multi-identity-era per-pubkey
%% scoping was removed in the rip-out.
-define(PG_SCOPE, macula_station_relay_default).

-type opts() :: #{
    topic     := binary(),
    peer_link := pid(),
    realm     := <<_:256>>
}.

-export_type([opts/0]).

-record(state, {
    topic       :: binary(),
    peer_link   :: pid(),
    peer_ref    :: reference(),
    realm       :: <<_:256>>
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{topic := _, peer_link := _, realm := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid, shutdown, 5_000).

-spec pg_scope() -> atom().
pg_scope() -> ?PG_SCOPE.

%% @doc Push a `{relay_publish, Topic, Payload}' message into the
%% station's pg group. Every alive forwarder for `Topic' receives it
%% and forwards to its peer link.
%%
%% This is the entry point the `hecate_pubsub_server' integration
%% layer calls when a local publish should fan out to peer stations.
-spec publish({Topic :: binary(), Payload :: binary()}) -> ok.
publish({Topic, Payload}) ->
    Members = pg_members({relay_topic, Topic}),
    [Pid ! {relay_publish, Topic, Payload} || Pid <- Members],
    ok.

%%====================================================================
%% gen_server
%%====================================================================

init(#{topic := Topic, peer_link := LinkPid, realm := Realm}) ->
    process_flag(trap_exit, true),
    Ref = erlang:monitor(process, LinkPid),
    ok  = ensure_pg(),
    ok  = pg:join(?PG_SCOPE, {relay_topic, Topic}, self()),
    logger:debug(
      "[peering_fwd] joined ~p for peer ~p", [{relay_topic, Topic}, LinkPid]),
    {ok, #state{topic     = Topic,
                peer_link = LinkPid,
                peer_ref  = Ref,
                realm     = Realm}}.

handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({relay_publish, Topic, Payload},
            #state{topic = Topic} = S) ->
    forward(S, Payload),
    {noreply, S};
handle_info({relay_publish, Topic, Payload, _Trace},
            #state{topic = Topic} = S) ->
    forward(S, Payload),
    {noreply, S};
handle_info({'DOWN', Ref, process, Pid, Reason},
            #state{peer_link = Pid, peer_ref = Ref, topic = Topic} = S) ->
    logger:warning(
      "[peering_fwd] peer link ~p for topic ~s died: ~p — exiting",
      [Pid, Topic, Reason]),
    {stop, normal, S};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(Reason, #state{topic = Topic}) ->
    catch pg:leave(?PG_SCOPE, {relay_topic, Topic}, self()),
    case Reason of
        normal        -> ok;
        shutdown      -> ok;
        {shutdown, _} -> ok;
        _ ->
            logger:warning(
              "[peering_fwd] topic ~s terminating abnormally: ~p",
              [Topic, Reason])
    end,
    ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Forward + helpers
%%====================================================================

forward(#state{peer_link = LinkPid, realm = Realm, topic = Topic}, Payload) ->
    try macula_station_link:publish(LinkPid, Realm, Topic, Payload) of
        ok ->
            ok;
        {error, Reason} ->
            logger:warning(
              "[peering_fwd] publish failed for ~s via ~p: ~p",
              [Topic, LinkPid, Reason]),
            ok
    catch
        Class:ExcReason ->
            logger:warning(
              "[peering_fwd] publish crashed for ~s via ~p: ~p:~p",
              [Topic, LinkPid, Class, ExcReason]),
            ok
    end.

%% pg scope is started on demand by the first forwarder/publisher.
ensure_pg() ->
    case pg:start_link(?PG_SCOPE) of
        {ok, _Pid}                       -> ok;
        {error, {already_started, _Pid}} -> ok
    end.

pg_members(Group) ->
    try pg:get_members(?PG_SCOPE, Group)
    catch _:_ -> []
    end.
