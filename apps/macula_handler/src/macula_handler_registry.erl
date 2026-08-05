%% @doc Station-level procedure registry.
%%
%% Stations advertise RPC procedures (e.g. `_dht.put_record',
%% `_dht.find_record', `_dht.find_records_by_type') via a
%% station-level registry. When a CALL frame arrives at the station,
%% `macula_handler_dispatch' looks the procedure up here and
%% invokes the registered handler.
%%
%% == Lifecycle ==
%%
%% Started under the station's supervision tree alongside DHT, SWIM,
%% observer, and listener. Resolved via macula_station singleton
%% accessors.
%%
%% == Single-provider invariant ==
%%
%% A procedure can be advertised by at most one handler at a time.
%% Re-advertising the same procedure with a new handler replaces
%% the old one (idempotent for restart-recovery cases). To clear a
%% procedure entirely, call `unadvertise/2'.
%%
%% == Handler shape ==
%%
%% A handler is either a plain `fun((Args :: term()) -> Reply ::
%% term())' or a `{Module, Function}' tuple invoked as
%% `apply(Module, Function, [Args])'. Handlers run inside the
%% dispatch process (NOT the registry); the registry only resolves
%% the function reference. Handlers must return either:
%%
%% <ul>
%%   <li>`{ok, Reply}' — wrapped into a RESULT frame</li>
%%   <li>`{error, Reason}' — wrapped into a `call_error' BOLT#4 frame</li>
%%   <li>any other value — treated as a `{ok, Value}' shorthand</li>
%% </ul>
-module(macula_handler_registry).
-behaviour(gen_server).

-export([start_link/0, start_link/1,
         advertise/3,
         unadvertise/2,
         lookup/2,
         list/1,
         stop/1]).

%% ==================================================================
%% ⚠ WHY THERE IS AN ETS MIRROR: A LOOKUP ON THIS PATH KILLED A STATION
%% ==================================================================
%%
%% `lookup/2' used to be a plain `gen_server:call'. It sits on the CALL
%% hot path: `macula_station_peer_observer' calls it from inside its own
%% `handle_info/2' for every inbound CALL frame on the station. So every
%% CALL on the node serialised through this one process, and if it was
%% slow for five seconds the observer DIED of the timeout.
%%
%% On 2026-08-05 that is exactly what happened to station-de-frankfurt:
%%
%%   exception exit: {timeout, {gen_server,call,
%%                    [macula_handler_registry, {lookup, <<"_macula.ping">>}]}}
%%
%% The supervisor restarted the observer, the restart recreated the public
%% conns ETS mirror EMPTY, and pubsub fan-out — which resolves subscriber
%% connections through that mirror — went permanently blind to every client
%% that connected afterwards. `/health', `/status' and `docker ps' all kept
%% saying healthy. Three public pages were dark for hours.
%%
%% The same medicine was already applied to `macula_dht:observe' in 2026-05
%% (made a cast, see the comment in `on_connected_directional/5') for the
%% same crash with a different callee. This is the class, not the instance:
%% READS ON THE FRAME PATH DO NOT GO THROUGH A MAILBOX.
%%
%% Writes still go through the gen_server, so the single-provider invariant
%% and last-write-wins semantics are unchanged. Reads are a lock-free ETS
%% lookup, the same pattern `macula_station_peer_observer_conns' uses.
-define(TAB, macula_handler_registry_tab).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([procedure/0, handler/0, registry/0]).

%% HOW THIS REGISTRY IS ADDRESSED, and it is not always a pid.
%%
%% `macula_station_sup' registers it under the name `macula_handler_registry'
%% and hands that NAME to the peer observer, whose own record has said
%% `atom() | pid() | undefined' all along. Every function here already accepted
%% it, because none of them guards the first argument and `gen_server:call/2'
%% resolves a registered name perfectly well.
%%
%% The specs said `pid()' anyway, and one consumer believed them: dispatch
%% guarded on `is_pid/1' and crashed a worker per call on the live fleet. The
%% type is written down so the next consumer is told the truth.
-type registry() :: pid() | atom().

-type procedure() :: binary().
-type handler()   :: fun((term()) -> term())
                   | {module(), atom()}.

-type opts() :: #{identity_key => term()}.

-record(state, {
    handlers = #{} :: #{procedure() => handler()},
    %% ⚠ WHETHER THIS REGISTRY OWNS THE MIRROR, AND IT IS NOT ALWAYS TRUE.
    %% The table is named, so it is one per BEAM. In production there is
    %% exactly one registry and it owns it. A second registry — tests do this,
    %% and a stub might — runs MIRRORLESS: it neither writes the table (which
    %% is `protected', so the write would raise) nor reads it (`mirrors/1'
    %% checks the owner). It simply behaves the way this module did before the
    %% mirror existed. Getting this wrong crashed the second registry's `init'.
    mirror = false :: boolean()
}).

%%====================================================================
%% Public API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) when is_map(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

%% @doc Advertise (or replace) a procedure handler.
-spec advertise(registry(), procedure(), handler()) -> ok.
advertise(Pid, Procedure, Handler)
  when is_binary(Procedure),
       (is_function(Handler, 1) orelse
        (is_tuple(Handler) andalso tuple_size(Handler) =:= 2)) ->
    gen_server:call(Pid, {advertise, Procedure, Handler}).

-spec unadvertise(registry(), procedure()) -> ok.
unadvertise(Pid, Procedure) when is_binary(Procedure) ->
    gen_server:call(Pid, {unadvertise, Procedure}).

%% @doc Resolve a procedure to its handler. Never blocks on the registry
%% process when the mirror is available, which is the whole point.
-spec lookup(registry(), procedure()) -> {ok, handler()} | {error, not_found}.
lookup(Registry, Procedure) when is_binary(Procedure) ->
    read(mirrors(Registry), Registry, Procedure).

%% ⚠ THE MIRROR IS ONLY TRUSTED WHEN THIS REGISTRY OWNS IT. The table is
%% named, so it is one per BEAM; a test that starts a second, unnamed
%% registry must not read the first one's handlers. Comparing the table's
%% owner against the registry being asked keeps that honest and costs one
%% `ets:info/2'. When they differ (or the table is absent, as with a stub
%% registry) the call path is used, which is exactly the old behaviour.
mirrors(Registry) ->
    try ets:info(?TAB, owner) =:= resolve(Registry)
    catch _:_ -> false
    end.

resolve(Pid) when is_pid(Pid) -> Pid;
resolve(Name) when is_atom(Name) -> whereis(Name).

read(true, _Registry, Procedure) ->
    lookup_reply(ets_find(ets:lookup(?TAB, Procedure)));
read(false, Registry, Procedure) ->
    gen_server:call(Registry, {lookup, Procedure}).

ets_find([{_Procedure, Handler}]) -> {ok, Handler};
ets_find([]) -> error.

-spec list(registry()) -> [procedure()].
list(Pid) ->
    gen_server:call(Pid, list).

-spec stop(registry()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    set_logger_identity(Opts),
    %% Idempotent: on supervisor restart the prior registry is already gone
    %% and took the table with it. Recreate cleanly if it somehow survived.
    %%
    %% `protected': everyone reads, only this process writes. Losing it on a
    %% crash is safe here in a way it was NOT for the observer's conns mirror,
    %% because the handlers are re-advertised by their owners on restart while
    %% a live QUIC connection has nobody to re-announce it.
    {ok, #state{mirror = claim_table(ets:info(?TAB, owner))}}.

%% No table: this registry makes it and owns it. That is the production path,
%% including after a supervisor restart, because a `protected' named table
%% dies with its owner and `ets:info/2' then answers `undefined'.
claim_table(undefined) ->
    ?TAB = ets:new(?TAB, [named_table, protected, set,
                          {read_concurrency, true}]),
    true;
%% Somebody else's. Do NOT try to delete it — a protected table belongs to its
%% owner and the delete would raise — and do not write to it either.
claim_table(_Owner) ->
    false.

set_logger_identity(#{identity_key := Key}) ->
    logger:set_process_metadata(#{identity_id => Key});
set_logger_identity(_) ->
    ok.

%% The map stays the source of truth and the mirror is written from the same
%% clause, so a reader can never see a handler the registry does not hold.
handle_call({advertise, Procedure, Handler}, _From,
            #state{handlers = H, mirror = M} = S) ->
    mirror_insert(M, Procedure, Handler),
    {reply, ok, S#state{handlers = H#{Procedure => Handler}}};
handle_call({unadvertise, Procedure}, _From,
            #state{handlers = H, mirror = M} = S) ->
    mirror_delete(M, Procedure),
    {reply, ok, S#state{handlers = maps:remove(Procedure, H)}};
handle_call({lookup, Procedure}, _From, #state{handlers = H} = S) ->
    {reply, lookup_reply(maps:find(Procedure, H)), S};
handle_call(list, _From, #state{handlers = H} = S) ->
    {reply, maps:keys(H), S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%====================================================================
%% Internals
%%====================================================================

lookup_reply({ok, Handler}) -> {ok, Handler};
lookup_reply(error)         -> {error, not_found}.

mirror_insert(true, Procedure, Handler) -> true = ets:insert(?TAB, {Procedure, Handler});
mirror_insert(false, _Procedure, _Handler) -> ok.

mirror_delete(true, Procedure) -> true = ets:delete(?TAB, Procedure);
mirror_delete(false, _Procedure) -> ok.
