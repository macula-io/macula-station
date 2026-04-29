%% @doc Identity registry — owns the per-identity supervisors.
%%
%% Multi-identity foundation (PLAN_MULTI_IDENTITY_RELAY §Phase 1).
%% A single hecate-station BEAM can host many identities; this
%% gen_server is the entry point that
%%
%% <ol>
%%   <li>spawns a fresh `hecate_station_identity_sup' linked to
%%       itself (so it is the OTP parent and graceful shutdown
%%       works without `kill' fallbacks),</li>
%%   <li>indexes the resulting pid by an opaque
%%       `identity_key()' (typically the per-identity hostname
%%       binary or Ed25519 public key),</li>
%%   <li>cleans up the index when a supervisor exits, whether by
%%       operator request or by crash.</li>
%% </ol>
%%
%% Phase 1 deliverable: registry exists, top-level station_sup owns
%% it, eunit covers register / lookup / list / terminate plus the
%% crash-cleanup path. The per-identity supervisors come up empty
%% — Phase 2 + Phase 3 wire the actual workers in once the
%% station_server / pubsub / overlay layers are de-singletonised.
%%
%% Why does the registry own the supervisors? Because
%% `exit(SupPid, shutdown)' only propagates as a graceful
%% termination when the sender is the supervisor's parent. Having
%% the registry call `supervisor:start_link/2' inside its own
%% `handle_call' makes the registry the parent of every identity
%% sup, so `terminate/1' can shut them down cleanly without
%% falling back to `exit(Pid, kill)'.
-module(hecate_station_identity_registry).
-behaviour(gen_server).

-export([start_link/0,
         register/2,
         lookup/1,
         lookup_opts/1,
         list/0,
         terminate/1,
         set_phase3/2,
         clear_phase3/1,
         phase3_snapshot/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export_type([identity_key/0]).

-type identity_key() :: term().

-record(state, {
    %% Bidirectional index. By-key for the public lookup API; by-pid
    %% for the EXIT-driven crash cleanup path. Keys + pids are
    %% pairwise unique — `register/2' rejects a second key for the
    %% same pid (would cause a stale entry on cleanup).
    by_key  = #{} :: #{identity_key() := pid()},
    by_pid  = #{} :: #{pid() := identity_key()},
    %% Original opts for each registered identity. Phase 5 needs
    %% this so the admin `/admin/identities/:id/reload' endpoint
    %% can re-register an identity with its original config.
    by_opts = #{} :: #{identity_key()
                       := hecate_station_identity_sup:identity_opts()},
    %% Phase-3 wiring info per identity, recorded by the identity
    %% supervisor once `pubsub_registry' + `peer_observer' are up.
    %% `hecate_station_record_fanout' queries this to broadcast
    %% record-stored events across all box-local identities.
    by_phase3 = #{} :: #{identity_key()
                         := hecate_station_record_fanout:phase3_info()}
}).

%%------------------------------------------------------------------
%% Public API
%%------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Spawn a fresh identity_sup for `Key' under
%% `IdentityOpts'. Returns the supervisor pid on success.
%%
%% Errors: `already_registered' if the key is taken;
%% `{sup_start_failed, Reason}' if the supervisor refused to come
%% up.
-spec register(identity_key(),
               hecate_station_identity_sup:identity_opts()) ->
    {ok, pid()} | {error, already_registered | {sup_start_failed, term()}}.
register(Key, IdentityOpts) when is_map(IdentityOpts) ->
    gen_server:call(?MODULE, {register, Key, IdentityOpts}).

-spec lookup(identity_key()) -> {ok, pid()} | {error, not_found}.
lookup(Key) ->
    gen_server:call(?MODULE, {lookup, Key}).

%% @doc Look up the original `identity_opts()' that the identity was
%% registered with. Used by the admin API `reload' endpoint to
%% restart an identity with the same config.
-spec lookup_opts(identity_key()) ->
    {ok, hecate_station_identity_sup:identity_opts()} | {error, not_found}.
lookup_opts(Key) ->
    gen_server:call(?MODULE, {lookup_opts, Key}).

%% @doc Snapshot every mapping. Used by the admin API + status pages.
-spec list() -> [{identity_key(), pid()}].
list() ->
    gen_server:call(?MODULE, list).

%% @doc Gracefully shut down the supervisor for `Key' and drop the
%% entry. Blocks until the supervised pid is `DOWN'. Returns
%% `{error, not_found}' if the key is unknown.
-spec terminate(identity_key()) -> ok | {error, not_found}.
terminate(Key) ->
    gen_server:call(?MODULE, {terminate, Key}, 10_000).

%% @doc Record an identity's Phase-3 wiring info (pubsub_registry
%% pid, peer_observer pid, identity keypair). Called by the identity
%% supervisor once the per-identity chain has produced both pids.
%%
%% Cast (not call): `register/2' invokes the identity supervisor's
%% Phase-3 chain INSIDE the registry's `handle_call', so a synchronous
%% recursive call back into the registry would deadlock with
%% `calling_self'. The cast queues in the registry's mailbox and is
%% processed after the outer `register/2' call returns. The
%% `record_fanout' reads `phase3_snapshot/0' on every record cast, so
%% the brief window between the cast queueing and being processed only
%% delays fan-out for records that arrive in those microseconds —
%% well-tolerated under steady-state operation.
-spec set_phase3(identity_key(),
                 hecate_station_record_fanout:phase3_info()) -> ok.
set_phase3(Key, Info) when is_map(Info) ->
    gen_server:cast(?MODULE, {set_phase3, Key, Info}).

%% @doc Drop the Phase-3 wiring entry for `Key'. Symmetrical with
%% `set_phase3/2' (also cast for the same reason) for clean identity
%% teardown; supervisor crashes are cleaned up by the registry's
%% `'EXIT'' handler.
-spec clear_phase3(identity_key()) -> ok.
clear_phase3(Key) ->
    gen_server:cast(?MODULE, {clear_phase3, Key}).

%% @doc Snapshot every identity's Phase-3 wiring info as
%% `[{Key, Info}]'. Used by `hecate_station_record_fanout' on each
%% `record_stored' cast to find every identity's pubsub_server +
%% peer_observer for fan-out.
-spec phase3_snapshot() ->
        [{identity_key(),
          hecate_station_record_fanout:phase3_info()}].
phase3_snapshot() ->
    gen_server:call(?MODULE, phase3_snapshot).

%%------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------

%% trap_exit: the registry spawns identity_sups as linked children.
%% When a sup crashes, the EXIT message must NOT take the registry
%% with it — instead it triggers the index cleanup in handle_info/2.
init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call({register, Key, Opts}, _From, S) ->
    do_register(Key, Opts, maps:is_key(Key, S#state.by_key), S);
handle_call({lookup, Key}, _From, S) ->
    {reply, lookup_reply(maps:find(Key, S#state.by_key)), S};
handle_call({lookup_opts, Key}, _From, S) ->
    {reply, lookup_reply(maps:find(Key, S#state.by_opts)), S};
handle_call(list, _From, S) ->
    {reply, snapshot(S#state.by_key), S};
handle_call({terminate, Key}, _From, S) ->
    do_terminate(Key, maps:find(Key, S#state.by_key), S);
handle_call(phase3_snapshot, _From, S) ->
    {reply, maps:to_list(S#state.by_phase3), S}.

handle_cast({set_phase3, Key, Info}, #state{by_phase3 = Ph3} = S) ->
    {noreply, S#state{by_phase3 = Ph3#{Key => Info}}};
handle_cast({clear_phase3, Key}, #state{by_phase3 = Ph3} = S) ->
    {noreply, S#state{by_phase3 = maps:remove(Key, Ph3)}};
handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({'EXIT', Pid, _Reason}, S) ->
    {noreply, drop_pid(Pid, S)};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%------------------------------------------------------------------
%% Internals — flat clauses, no nested case
%%------------------------------------------------------------------

do_register(_Key, _Opts, true, S) ->
    {reply, {error, already_registered}, S};
do_register(Key, Opts, false, S) ->
    on_sup_started(Key, Opts,
                   hecate_station_identity_sup:start_link(Opts),
                   S).

on_sup_started(_Key, _Opts, {error, Reason}, S) ->
    {reply, {error, {sup_start_failed, Reason}}, S};
on_sup_started(Key, Opts, {ok, Pid},
               #state{by_key = K, by_pid = P, by_opts = O} = S) ->
    {reply, {ok, Pid},
     S#state{by_key  = K#{Key  => Pid},
             by_pid  = P#{Pid  => Key},
             by_opts = O#{Key  => Opts}}}.

lookup_reply({ok, Pid}) -> {ok, Pid};
lookup_reply(error)     -> {error, not_found}.

snapshot(ByKey) ->
    maps:to_list(ByKey).

do_terminate(_Key, error, S) ->
    {reply, {error, not_found}, S};
do_terminate(Key, {ok, Pid}, S) ->
    %% Remove the index FIRST so the upcoming `'EXIT'' message
    %% finds nothing left to clean up. The supervisor is our
    %% linked child — `exit/2' from the parent triggers a clean
    %% shutdown via the supervisor's own terminate path.
    S2  = drop_key(Key, Pid, S),
    Ref = erlang:monitor(process, Pid),
    unlink_and_exit(Pid),
    await_down(Pid, Ref),
    {reply, ok, S2}.

unlink_and_exit(Pid) ->
    %% Unlink first so the resulting EXIT does NOT come back as a
    %% link signal we have to filter in handle_info — the monitor
    %% set up in do_terminate is sufficient + race-free.
    true = unlink(Pid),
    %% Drain any EXIT that the link might have already delivered.
    receive {'EXIT', Pid, _} -> ok after 0 -> ok end,
    _ = exit(Pid, shutdown),
    ok.

await_down(Pid, Ref) ->
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 5_000 ->
        _ = exit(Pid, kill),
        receive
            {'DOWN', Ref, process, Pid, _} -> ok
        after 1_000 -> ok
        end
    end.

drop_pid(Pid, #state{by_pid = P} = S) ->
    drop_pid_step(maps:find(Pid, P), Pid, S).

drop_pid_step(error, _Pid, S) ->
    S;
drop_pid_step({ok, Key}, Pid, S) ->
    drop_key(Key, Pid, S).

drop_key(Key, Pid,
         #state{by_key = K, by_pid = P, by_opts = O,
                by_phase3 = Ph3} = S) ->
    S#state{by_key    = maps:remove(Key, K),
            by_pid    = maps:remove(Pid, P),
            by_opts   = maps:remove(Key, O),
            by_phase3 = maps:remove(Key, Ph3)}.
