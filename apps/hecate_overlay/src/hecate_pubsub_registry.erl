%% @doc Per-realm-namespace registry for `hecate_pubsub_server' processes.
%%
%% Holds a `RealmTag => pid()' map and acts as the dispatch hub for
%% inbound SUBSCRIBE / UNSUBSCRIBE / EVENT frames. New realms are
%% materialised via `register/2': the registry asks
%% `hecate_pubsub_server_sup' to spawn a child and tracks it via
%% `erlang:monitor/2'. When a server dies the monitor's DOWN clears
%% the entry; a later `register/2' yields a fresh server.
%%
%% == Sprint A invariant ==
%%
%% Realm tags are opaque 32-byte namespace keys. The registry does
%% NOT validate authenticity — multi-tenancy is structural (one
%% server per tag, no cross-realm leakage). Realm authority lives
%% outside the station per `PLAN_DEFERRED_WORK' §6.
%%
%% == Sequencing ==
%%
%% <ul>
%%   <li>Phase 1 (prior commit): `hecate_pubsub_server' standalone.</li>
%%   <li>Phase 2 (this commit): registry + dynamic supervisor under
%%       `hecate_overlay_sup'.</li>
%%   <li>Phase 3 (next commit): wire registry into
%%       `hecate_station_listener' for inbound frame dispatch.</li>
%%   <li>Later: Plumtree fan-out, DHT topic-mesh discovery.</li>
%% </ul>
-module(hecate_pubsub_registry).
-behaviour(gen_server).

-compile({no_auto_import, [register/2]}).

-export([
    start_link/0,
    register/2,
    lookup/1,
    dispatch_frame/3,
    stop/0
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([realm/0, identity/0]).

-type realm()    :: <<_:256>>.
-type identity() :: macula_identity:key_pair().

-record(state, {
    by_realm = #{} :: #{realm() => pid()},
    by_ref   = #{} :: #{reference() => realm()}
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Idempotently start a pubsub_server for `Realm'. If a live
%% server already exists, returns its pid; otherwise spawns a new
%% one under `hecate_pubsub_server_sup' with `Identity' as the
%% signing key. A stale entry pointing at a dead pid is replaced
%% transparently.
-spec register(realm(), identity()) ->
        {ok, pid()} | {error, term()}.
register(<<_:256>> = Realm, Identity) ->
    gen_server:call(?MODULE, {register, Realm, Identity}).

%% @doc Find the pubsub_server pid for `Realm' or report `not_found'.
-spec lookup(realm()) -> {ok, pid()} | {error, not_found}.
lookup(<<_:256>> = Realm) ->
    gen_server:call(?MODULE, {lookup, Realm}).

%% @doc Route a SUBSCRIBE / UNSUBSCRIBE / EVENT frame for `Realm' to
%% the matching pubsub_server. Returns the matched local subscribers
%% (empty list for SUBSCRIBE / UNSUBSCRIBE) or `{error, not_found}'
%% if no server is registered for `Realm'. The realm tag is supplied
%% explicitly — the caller is responsible for extracting it from
%% the frame and confirming the dispatch target.
-spec dispatch_frame(realm(), <<_:256>>, macula_frame:frame()) ->
        {ok, [<<_:256>>]} | {error, not_found}.
dispatch_frame(<<_:256>> = Realm, From, Frame) ->
    gen_server:call(?MODULE, {dispatch_frame, Realm, From, Frame}).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({register, Realm, Identity}, _From, S) ->
    case maps:find(Realm, S#state.by_realm) of
        {ok, Pid} ->
            handle_existing(Realm, Pid, Identity, S);
        error ->
            do_register(Realm, Identity, S)
    end;
handle_call({lookup, Realm}, _From, S) ->
    Reply = case maps:find(Realm, S#state.by_realm) of
                {ok, Pid} -> {ok, Pid};
                error     -> {error, not_found}
            end,
    {reply, Reply, S};
handle_call({dispatch_frame, Realm, From, Frame}, _From, S) ->
    do_dispatch(Realm, From, Frame, S);
handle_call(_Other, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({'DOWN', Ref, process, _Pid, _Reason}, S) ->
    {noreply, drop_ref(Ref, S)};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Helpers
%%====================================================================

handle_existing(Realm, Pid, Identity, S) ->
    case is_process_alive(Pid) of
        true ->
            {reply, {ok, Pid}, S};
        false ->
            do_register(Realm, Identity, drop_realm(Realm, S))
    end.

do_register(Realm, Identity, S) ->
    case hecate_pubsub_server_sup:start_server(
           #{realm => Realm, identity => Identity}) of
        {ok, Pid} ->
            Ref      = erlang:monitor(process, Pid),
            ByRealm2 = maps:put(Realm, Pid, S#state.by_realm),
            ByRef2   = maps:put(Ref, Realm, S#state.by_ref),
            {reply, {ok, Pid},
             S#state{by_realm = ByRealm2, by_ref = ByRef2}};
        {error, _} = Error ->
            {reply, Error, S}
    end.

do_dispatch(Realm, From, Frame, S) ->
    case maps:find(Realm, S#state.by_realm) of
        {ok, Pid} ->
            try hecate_pubsub_server:process_frame(Pid, From, Frame) of
                Subs -> {reply, {ok, Subs}, S}
            catch
                exit:{noproc, _} ->
                    {reply, {error, not_found}, drop_realm(Realm, S)}
            end;
        error ->
            {reply, {error, not_found}, S}
    end.

drop_ref(Ref, S) ->
    case maps:find(Ref, S#state.by_ref) of
        {ok, Realm} ->
            S#state{
                by_realm = maps:remove(Realm, S#state.by_realm),
                by_ref   = maps:remove(Ref, S#state.by_ref)
            };
        error ->
            S
    end.

drop_realm(Realm, S) ->
    S#state{
        by_realm = maps:remove(Realm, S#state.by_realm),
        by_ref   = maps:filter(fun(_, V) -> V =/= Realm end, S#state.by_ref)
    }.
