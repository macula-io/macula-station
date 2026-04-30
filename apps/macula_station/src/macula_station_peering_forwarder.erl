%%% @doc Outbound peering forwarder for one (Topic, Peer station-link)
%%% pair. Per-identity slice — there is one forwarder process per
%%% (Identity, Topic, PeerLinkPid) triple.
%%%
%%% Ported from V1 `macula_relay_peering_forwarder' (which was rooted
%%% in pg group `pg' / `relay_topic'). V2 uses a per-identity pg group
%%% so identities running in the same BEAM don't cross-pollinate.
%%%
%%% On `{relay_publish, Topic, Payload}' messages received from the
%%% local pg group, this forwarder hands the payload off to a single
%%% peer station link via `macula_station_link:publish/4'. Failures
%%% are logged with topic + peer pid for diagnostic trail (the
%%% pre-2026-04 V1 behaviour was a bare `catch' that dropped silently).
-module(macula_station_peering_forwarder).
-behaviour(gen_server).

-export([start_link/1, stop/1, publish/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type opts() :: #{
    pg_scope     := atom(),
    topic        := binary(),
    peer_link    := pid(),
    realm        := <<_:256>>,
    identity_key => term()
}.

-export_type([opts/0]).

-record(state, {
    pg_scope    :: atom(),
    topic       :: binary(),
    peer_link   :: pid(),
    peer_ref    :: reference(),
    realm       :: <<_:256>>
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{pg_scope := _, topic := _, peer_link := _, realm := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid, shutdown, 5_000).

%% @doc Push a `{relay_publish, Topic, Payload}' message into the
%% per-identity pg group. Every alive forwarder for `Topic' on this
%% identity receives it and forwards to its peer link.
%%
%% This is the entry point the `hecate_pubsub_server' integration
%% layer calls when a local publish should fan out to peer stations.
-spec publish(atom(), {Topic :: binary(), Payload :: binary()}) -> ok.
publish(PgScope, {Topic, Payload}) ->
    Members = pg_members(PgScope, {relay_topic, Topic}),
    [Pid ! {relay_publish, Topic, Payload} || Pid <- Members],
    ok.

%%====================================================================
%% gen_server
%%====================================================================

init(#{pg_scope := PgScope, topic := Topic, peer_link := LinkPid,
       realm := Realm} = Opts) ->
    process_flag(trap_exit, true),
    set_logger_identity(Opts),
    Ref = erlang:monitor(process, LinkPid),
    ok  = ensure_pg(PgScope),
    ok  = pg:join(PgScope, {relay_topic, Topic}, self()),
    logger:debug(
      "[peering_fwd] joined ~p for peer ~p", [{relay_topic, Topic}, LinkPid]),
    {ok, #state{pg_scope  = PgScope,
                topic     = Topic,
                peer_link = LinkPid,
                peer_ref  = Ref,
                realm     = Realm}}.

set_logger_identity(#{identity_key := Key}) ->
    logger:set_process_metadata(#{identity_id => Key});
set_logger_identity(_) -> ok.

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

terminate(Reason, #state{pg_scope = Scope, topic = Topic}) ->
    catch pg:leave(Scope, {relay_topic, Topic}, self()),
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

%% Forward a local publish to the peer link. Surfaces both `{error, _}'
%% returns AND process crashes so failures aren't silent. The bare
%% `catch' that V1 had pre-2026-04 lost diagnostic data — we keep the
%% explicit try/catch with a WARNING log per failure.
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

%% pg scopes are global per VM but we want a per-identity scope so
%% identities sharing a BEAM don't cross-pollinate. The scope is
%% started on demand by the first forwarder/publisher on this
%% identity's tree.
ensure_pg(Scope) ->
    case pg:start_link(Scope) of
        {ok, _Pid}                       -> ok;
        {error, {already_started, _Pid}} -> ok
    end.

pg_members(Scope, Group) ->
    try pg:get_members(Scope, Group)
    catch _:_ -> []
    end.
