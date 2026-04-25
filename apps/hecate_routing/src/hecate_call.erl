%% @doc CALL state machine — gen_statem (Part 4 §6.2).
%%
%% Models the per-CALL lifecycle as a five-state pipeline with a
%% bounded deadline at every transition. Missing any deadline is
%% mapped to a BOLT#4 code (`temporary_relay_failure',
%% `unknown_next_peer', or `expiry_too_soon') so the orchestrator
%% can apply the §6.4 retry budget against a structured failure
%% rather than a silent timeout.
%%
%% <pre>
%%   idle ─start─> resolving ─resolved─> selected_target ─selected─>
%%       connecting ─connected─> awaiting_ack ─ack/ack_error─>
%%           succeeded | failed
%% </pre>
%%
%% Timeout map (overridable per-CALL):
%%
%% | Transition                          | Default |
%% | ----------------------------------- | ------: |
%% | idle → resolving                    |  100 ms |
%% | resolving → selected_target         |  500 ms |
%% | selected_target → connecting        |  200 ms |
%% | connecting → awaiting_ack           |  200 ms |
%% | awaiting_ack → succeeded \| failed  | 5_000 ms (caller-set) |
%%
%% The state machine does <em>not</em> perform resolution, path
%% computation, QUIC connection, or wire I/O on its own. It is
%% driven by an orchestrator (Phase 4 Session 4.6+) that pumps in
%% `resolved/3', `selected/2', `connected/1', `ack/2', `ack_error/3'
%% events as the underlying stages complete. Each cast is a no-op
%% if it arrives in the wrong state — the machine simply keeps
%% waiting for the right event or its deadline to fire.
%%
%% Reference: plans/PLAN_MACULA_V2_PART4_LIFECYCLE.md §6.2;
%% plans/PLAN_PHASE_4_BREAKDOWN.md Session 4.5.
-module(hecate_call).
-behaviour(gen_statem).

-export([start_link/1, stop/1, await/2]).
-export([resolved/3, selected/2, connected/1, ack/2, ack_error/3]).
-export([state/1, info/1]).

-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([idle/3, resolving/3, selected_target/3, connecting/3,
         awaiting_ack/3, succeeded/3, failed/3]).

-export_type([opts/0, outcome/0, info/0, state_name/0]).

-define(D_RESOLVING_MS,         100).
-define(D_SELECTED_TARGET_MS,   500).
-define(D_CONNECTING_MS,        200).
-define(D_AWAITING_ACK_MS,    5_000).
-define(DEFAULT_RETRY_BUDGET,     2).

-type state_name() ::
      idle | resolving | selected_target | connecting
    | awaiting_ack | succeeded | failed.

-type opts() :: #{
    caller       := macula_identity:pubkey(),
    procedure    := binary(),
    realm        := <<_:256>>,
    payload      := term(),
    notify_pid   => pid(),
    deadline_ms  => pos_integer(),
    retry_budget => non_neg_integer(),
    call_id      => <<_:128>>,
    resolving_timeout_ms       => pos_integer(),
    selected_target_timeout_ms => pos_integer(),
    connecting_timeout_ms      => pos_integer(),
    awaiting_ack_timeout_ms    => pos_integer()
}.

-type outcome() ::
      {ok, term()}
    | {error, hecate_bolt4:name(), #{atom() => term()}}.

-type info() :: #{
    call_id       := <<_:128>>,
    state         := state_name(),
    target        := macula_identity:pubkey() | undefined,
    path          := [macula_identity:pubkey()] | undefined,
    outcome       := outcome() | undefined,
    elapsed_ms    := non_neg_integer()
}.

-record(data, {
    call_id           :: <<_:128>>,
    caller            :: macula_identity:pubkey(),
    procedure         :: binary(),
    realm             :: <<_:256>>,
    payload           :: term(),
    notify_pid        :: pid() | undefined,
    awaiters = []     :: [gen_statem:from()],
    retry_budget      :: non_neg_integer(),
    t_resolving       :: pos_integer(),
    t_selected        :: pos_integer(),
    t_connecting      :: pos_integer(),
    t_awaiting        :: pos_integer(),
    target            :: macula_identity:pubkey() | undefined,
    path              :: [macula_identity:pubkey()] | undefined,
    outcome           :: outcome() | undefined,
    started_at_mono   :: integer()
}).

%%=====================================================================
%% Public API
%%=====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) when is_map(Opts) ->
    gen_statem:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_statem:stop(Pid).

%% @doc Block until the CALL reaches a terminal state and return
%% its outcome. Multiple awaiters are supported; all receive the
%% same outcome.
-spec await(pid(), timeout()) -> outcome().
await(Pid, TimeoutMs) ->
    gen_statem:call(Pid, await, TimeoutMs).

%% @doc Driving event from the orchestrator: resolution complete.
-spec resolved(pid(), macula_identity:pubkey(),
               [macula_identity:pubkey()]) -> ok.
resolved(Pid, Target, Path) when is_binary(Target), is_list(Path) ->
    gen_statem:cast(Pid, {resolved, Target, Path}).

%% @doc Driving event: target picked from candidates.
-spec selected(pid(), macula_identity:pubkey()) -> ok.
selected(Pid, Target) when is_binary(Target) ->
    gen_statem:cast(Pid, {selected, Target}).

%% @doc Driving event: QUIC connection established to first hop.
-spec connected(pid()) -> ok.
connected(Pid) ->
    gen_statem:cast(Pid, connected).

%% @doc Driving event: RESULT frame received from target.
-spec ack(pid(), term()) -> ok.
ack(Pid, Payload) ->
    gen_statem:cast(Pid, {ack, Payload}).

%% @doc Driving event: ERROR frame received with a BOLT#4 code,
%% optionally identifying the offending hop.
-spec ack_error(pid(), hecate_bolt4:name() | hecate_bolt4:code(),
                macula_identity:pubkey() | undefined) -> ok.
ack_error(Pid, CodeOrName, Hop) ->
    gen_statem:cast(Pid, {ack_error, CodeOrName, Hop}).

-spec state(pid()) -> state_name().
state(Pid) ->
    gen_statem:call(Pid, get_state).

-spec info(pid()) -> info().
info(Pid) ->
    gen_statem:call(Pid, get_info).

%%=====================================================================
%% gen_statem callbacks
%%=====================================================================

callback_mode() -> state_functions.

init(Opts) ->
    Data = build_data(Opts),
    {ok, idle, Data, [{next_event, internal, start}]}.

terminate(_Reason, _State, _Data) -> ok.

code_change(_OldVsn, State, Data, _Extra) -> {ok, State, Data}.

%%=====================================================================
%% State functions
%%=====================================================================

idle(internal, start, Data) ->
    {next_state, resolving, Data,
     [{state_timeout, Data#data.t_resolving, deadline}]};
idle(EventType, Event, Data) ->
    handle_common(idle, EventType, Event, Data).

resolving(cast, {resolved, Target, Path}, Data) ->
    NewData = Data#data{target = Target, path = Path},
    {next_state, selected_target, NewData,
     [{state_timeout, NewData#data.t_selected, deadline}]};
resolving(state_timeout, deadline, Data) ->
    fail(Data, temporary_relay_failure, #{phase => resolving});
resolving(EventType, Event, Data) ->
    handle_common(resolving, EventType, Event, Data).

selected_target(cast, {selected, Target}, Data) ->
    NewData = Data#data{target = Target},
    {next_state, connecting, NewData,
     [{state_timeout, NewData#data.t_connecting, deadline}]};
selected_target(state_timeout, deadline, Data) ->
    fail(Data, unknown_next_peer, #{phase => selected_target});
selected_target(EventType, Event, Data) ->
    handle_common(selected_target, EventType, Event, Data).

connecting(cast, connected, Data) ->
    {next_state, awaiting_ack, Data,
     [{state_timeout, Data#data.t_awaiting, deadline}]};
connecting(state_timeout, deadline, Data) ->
    fail(Data, temporary_relay_failure, #{phase => connecting});
connecting(EventType, Event, Data) ->
    handle_common(connecting, EventType, Event, Data).

awaiting_ack(cast, {ack, Payload}, Data) ->
    succeed(Data#data{outcome = {ok, Payload}});
awaiting_ack(cast, {ack_error, CodeOrName, Hop}, Data) ->
    Code = normalise_code(CodeOrName),
    fail(Data, Code, hop_detail(Hop));
awaiting_ack(state_timeout, deadline, Data) ->
    fail(Data, expiry_too_soon, #{phase => awaiting_ack});
awaiting_ack(EventType, Event, Data) ->
    handle_common(awaiting_ack, EventType, Event, Data).

succeeded(EventType, Event, Data) ->
    handle_terminal(succeeded, EventType, Event, Data).

failed(EventType, Event, Data) ->
    handle_terminal(failed, EventType, Event, Data).

%%=====================================================================
%% Common event handling (non-terminal states)
%%=====================================================================

handle_common(State, {call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, State}]};
handle_common(State, {call, From}, get_info, Data) ->
    {keep_state, Data, [{reply, From, build_info(State, Data)}]};
handle_common(_State, {call, From}, await, #data{awaiters = As} = Data) ->
    {keep_state, Data#data{awaiters = [From | As]}};
handle_common(_State, _EventType, _Event, Data) ->
    {keep_state, Data}.

%%=====================================================================
%% Common event handling (terminal states)
%%=====================================================================

handle_terminal(State, {call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, State}]};
handle_terminal(State, {call, From}, get_info, Data) ->
    {keep_state, Data, [{reply, From, build_info(State, Data)}]};
handle_terminal(_State, {call, From}, await, #data{outcome = O} = Data) ->
    {keep_state, Data, [{reply, From, O}]};
handle_terminal(_State, _EventType, _Event, Data) ->
    {keep_state, Data}.

%%=====================================================================
%% Outcome bookkeeping
%%=====================================================================

succeed(Data) ->
    notify_awaiters(Data),
    {next_state, succeeded, Data#data{awaiters = []}}.

fail(Data, Code, Detail) ->
    NewData = Data#data{outcome = {error, Code, Detail}},
    notify_awaiters(NewData),
    {next_state, failed, NewData#data{awaiters = []}}.

notify_awaiters(#data{awaiters = As, outcome = Outcome,
                       notify_pid = NP}) ->
    [gen_statem:reply(A, Outcome) || A <- As],
    notify_pid(NP, Outcome).

notify_pid(undefined, _Outcome) ->
    ok;
notify_pid(Pid, Outcome) when is_pid(Pid) ->
    Pid ! {hecate_call, self(), Outcome},
    ok.

%%=====================================================================
%% Conversions + builders
%%=====================================================================

normalise_code(Code) when is_atom(Code) ->
    Code;
normalise_code(Code) when is_integer(Code) ->
    hecate_bolt4:name(Code).

hop_detail(undefined) ->
    #{};
hop_detail(Hop) when is_binary(Hop), byte_size(Hop) =:= 32 ->
    #{offending_hop => Hop}.

-spec build_data(opts()) -> #data{}.
build_data(#{caller := Caller, procedure := Proc, realm := Realm,
             payload := Payload} = Opts)
  when is_binary(Caller), byte_size(Caller) =:= 32,
       is_binary(Proc),
       is_binary(Realm),  byte_size(Realm) =:= 32 ->
    #data{
        call_id          = maps:get(call_id, Opts, fresh_call_id()),
        caller           = Caller,
        procedure        = Proc,
        realm            = Realm,
        payload          = Payload,
        notify_pid       = maps:get(notify_pid, Opts, undefined),
        retry_budget     = maps:get(retry_budget, Opts, ?DEFAULT_RETRY_BUDGET),
        t_resolving      = maps:get(resolving_timeout_ms, Opts,
                                    ?D_RESOLVING_MS),
        t_selected       = maps:get(selected_target_timeout_ms, Opts,
                                    ?D_SELECTED_TARGET_MS),
        t_connecting     = maps:get(connecting_timeout_ms, Opts,
                                    ?D_CONNECTING_MS),
        t_awaiting       = maps:get(awaiting_ack_timeout_ms, Opts,
                                    ?D_AWAITING_ACK_MS),
        started_at_mono  = erlang:monotonic_time(millisecond)
    }.

-spec build_info(state_name(), #data{}) -> info().
build_info(State, #data{call_id = Id, target = T, path = P,
                         outcome = O, started_at_mono = Started}) ->
    Now = erlang:monotonic_time(millisecond),
    #{
        call_id    => Id,
        state      => State,
        target     => T,
        path       => P,
        outcome    => O,
        elapsed_ms => max(0, Now - Started)
    }.

-spec fresh_call_id() -> <<_:128>>.
fresh_call_id() ->
    crypto:strong_rand_bytes(16).
