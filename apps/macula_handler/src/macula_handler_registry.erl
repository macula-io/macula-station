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
    handlers = #{} :: #{procedure() => handler()}
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

-spec lookup(registry(), procedure()) -> {ok, handler()} | {error, not_found}.
lookup(Pid, Procedure) when is_binary(Procedure) ->
    gen_server:call(Pid, {lookup, Procedure}).

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
    {ok, #state{}}.

set_logger_identity(#{identity_key := Key}) ->
    logger:set_process_metadata(#{identity_id => Key});
set_logger_identity(_) ->
    ok.

handle_call({advertise, Procedure, Handler}, _From,
            #state{handlers = H} = S) ->
    {reply, ok, S#state{handlers = H#{Procedure => Handler}}};
handle_call({unadvertise, Procedure}, _From, #state{handlers = H} = S) ->
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
