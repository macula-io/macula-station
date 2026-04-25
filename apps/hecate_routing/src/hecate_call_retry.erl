%% @doc Retry budget + disjoint-path rotation (Part 4 §6.4).
%%
%% Wraps a single-attempt CALL function with the §6.4 retry
%% machinery: a bounded budget of retries, an overall wall-clock
%% deadline, and disjoint-path rotation between attempts.
%%
%% The orchestrator does not itself drive a `hecate_call' state
%% machine — that's the integration point that lands later when
%% the full I/O surface (DHT lookup, QUIC handshake, peer-side
%% ack) is wired up. For now the orchestrator accepts a
%% `try_fn :: fun((path()) -> hecate_call:outcome())' callback that
%% performs one attempt against one path and returns its outcome.
%% Callers supply whatever attempt logic they want (a real CALL
%% pipeline in production; a mock in tests).
%%
%% == Retry semantics ==
%%
%% On `{ok, _}' — return immediately.
%%
%% On `{error, Code, _Detail}' — consult
%% `hecate_bolt4:is_retryable(Code)':
%% <ul>
%%   <li>Non-retryable (`ok' / `application' / `crypto_drop'
%%       policies) → return immediately.</li>
%%   <li>Retryable AND budget > 0 AND deadline not exceeded AND
%%       at least one fresh path remains → rotate to the next
%%       disjoint path and try again.</li>
%%   <li>Otherwise → return the latest error.</li>
%% </ul>
%%
%% Path rotation: the retry walks the supplied path list left to
%% right. If the list is shorter than `retry_budget + 1', the
%% orchestrator stops at the last path — the spec mandates "a
%% different disjoint path" rather than re-trying the same one.
%%
%% Reference: plans/PLAN_MACULA_V2_PART4_LIFECYCLE.md §6.4;
%% plans/PLAN_PHASE_4_BREAKDOWN.md Session 4.7.
-module(hecate_call_retry).

-export([
    execute/1,
    interactive_defaults/0,
    background_defaults/0,
    bulk_defaults/0
]).

-export_type([opts/0, path/0, try_fn/0, attempt/0, result/0]).

-define(DEFAULT_RETRY_BUDGET,               2).
-define(DEFAULT_OVERALL_TIMEOUT_MS,     5_000).
-define(BACKGROUND_RETRY_BUDGET,            5).
-define(BACKGROUND_OVERALL_TIMEOUT_MS, 30_000).
-define(BULK_RETRY_BUDGET,                100).
-define(BULK_OVERALL_TIMEOUT_MS,    3_600_000).

-type path()    :: [macula_identity:pubkey()].
-type try_fn()  :: fun((path()) -> hecate_call:outcome()).

-type opts() :: #{
    paths               := [path(), ...],
    try_fn              := try_fn(),
    retry_budget        => non_neg_integer(),
    overall_timeout_ms  => pos_integer()
}.

-type attempt() :: #{
    path    := path(),
    outcome := hecate_call:outcome()
}.

-type result() :: #{
    outcome  := hecate_call:outcome(),
    attempts := [attempt(), ...]
}.

%%=====================================================================
%% Public API
%%=====================================================================

-spec execute(opts()) -> result().
execute(#{paths := [_ | _] = Paths, try_fn := Fn} = Opts) when is_function(Fn, 1) ->
    Budget = maps:get(retry_budget, Opts, ?DEFAULT_RETRY_BUDGET),
    Total  = maps:get(overall_timeout_ms, Opts, ?DEFAULT_OVERALL_TIMEOUT_MS),
    Deadline = erlang:monotonic_time(millisecond) + Total,
    State = #{deadline => Deadline, attempts => []},
    walk(Paths, Fn, Budget, State).

-spec interactive_defaults() -> #{retry_budget := 2,
                                  overall_timeout_ms := 5_000}.
interactive_defaults() ->
    #{retry_budget       => ?DEFAULT_RETRY_BUDGET,
      overall_timeout_ms => ?DEFAULT_OVERALL_TIMEOUT_MS}.

-spec background_defaults() -> #{retry_budget := 5,
                                 overall_timeout_ms := 30_000}.
background_defaults() ->
    #{retry_budget       => ?BACKGROUND_RETRY_BUDGET,
      overall_timeout_ms => ?BACKGROUND_OVERALL_TIMEOUT_MS}.

-spec bulk_defaults() -> #{retry_budget := 100,
                           overall_timeout_ms := 3_600_000}.
bulk_defaults() ->
    #{retry_budget       => ?BULK_RETRY_BUDGET,
      overall_timeout_ms => ?BULK_OVERALL_TIMEOUT_MS}.

%%=====================================================================
%% Walk
%%=====================================================================

-spec walk([path()], try_fn(), non_neg_integer(), map()) -> result().
walk([], _Fn, _Budget, State) ->
    %% No more paths and no terminal outcome captured — synthesise
    %% the canonical "exhausted" failure.
    finalise(no_more_paths(), State);
walk([Path | Rest], Fn, Budget, State) ->
    case past_deadline(State) of
        true  -> finalise(deadline_exceeded(), State);
        false -> attempt(Path, Rest, Fn, Budget, State)
    end.

-spec attempt(path(), [path()], try_fn(),
              non_neg_integer(), map()) -> result().
attempt(Path, Rest, Fn, Budget, State) ->
    Outcome = Fn(Path),
    State1  = record_attempt(Path, Outcome, State),
    decide(Outcome, Rest, Fn, Budget, State1).

-spec decide(hecate_call:outcome(), [path()], try_fn(),
             non_neg_integer(), map()) -> result().
decide({ok, _} = Outcome, _Rest, _Fn, _Budget, State) ->
    finalise(Outcome, State);
decide({error, Code, _Detail} = Outcome, Rest, Fn, Budget, State) ->
    follow_retry(retryable(Code) andalso Budget > 0 andalso Rest =/= [],
                 Outcome, Rest, Fn, Budget, State).

-spec follow_retry(boolean(), hecate_call:outcome(), [path()],
                   try_fn(), non_neg_integer(), map()) -> result().
follow_retry(false, Outcome, _Rest, _Fn, _Budget, State) ->
    finalise(Outcome, State);
follow_retry(true, _Outcome, Rest, Fn, Budget, State) ->
    walk(Rest, Fn, Budget - 1, State).

-spec retryable(hecate_bolt4:name()) -> boolean().
retryable(Code) ->
    hecate_bolt4:is_retryable(Code).

%%=====================================================================
%% State helpers
%%=====================================================================

past_deadline(#{deadline := D}) ->
    erlang:monotonic_time(millisecond) >= D.

record_attempt(Path, Outcome, #{attempts := As} = State) ->
    State#{attempts := As ++ [#{path => Path, outcome => Outcome}]}.

finalise(Outcome, #{attempts := As}) ->
    #{outcome => Outcome, attempts => As}.

no_more_paths() ->
    {error, unknown_next_peer, #{phase => retry, reason => no_more_paths}}.

deadline_exceeded() ->
    {error, expiry_too_soon, #{phase => retry, reason => deadline}}.
