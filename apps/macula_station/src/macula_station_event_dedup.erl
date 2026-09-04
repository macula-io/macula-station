%%% @doc Generational `(publisher, seq)' dedup cache for pubsub events.
%%%
%%% Step 2 of the pubsub Phase 2 redesign (see
%%% `plans/PLAN_PUBSUB_E2E_SIGNED_EVENTS.md'). Today this only
%%% *observes*: `seen_or_record/2' records every `{Publisher, Seq}'
%%% it is shown and reports whether that pair had already been seen
%%% within the dedup window. The caller (`macula_station_peer_observer'
%%% on inbound EVENT frames) logs would-be-dropped duplicates and
%%% bumps `dup_count/0', but still delivers the duplicate — the
%%% accidental signature-verify mismatch (see
%%% `docs/PUBSUB_RESIGN_LOOP_LESSON.md') is still the live loop kill.
%%% Step 3 flips this cache to the *primary* loop kill (and drops the
%%% duplicates) at the same time as switching EVENT verification to
%%% the publisher signature (`macula_frame:verify_publisher/1') and
%%% removing the relay re-sign.
%%%
%%% Bounded: one ETS `set' keyed `{Publisher, Seq}' (or, via the
%%% arity-3 API below, `{Publisher, Seq, Topic}' -- see
%%% `seen_or_record/3''s own doc for why a bare `(Publisher, Seq)' key
%%% turned out to be too coarse), swept every `?SWEEP_MS', entries
%%% evicted once older than `?TTL_MS'. O(1) per record (atomic
%%% `ets:insert_new/2'); no per-frame signing — the per-frame Ed25519
%%% cost is what flaked the reverted attempt 3, and it is simply
%%% absent here. The two key shapes never collide (different tuple
%%% arity) and coexist in the same table; the sweep's match spec only
%%% inspects the outer `{Key, Timestamp}' row, so it evicts both
%%% shapes identically without needing to know which one it's looking
%%% at.
-module(macula_station_event_dedup).
-behaviour(gen_server).

-export([start_link/0, seen_or_record/2, seen_or_record/3, peek/2, peek/3,
         window_size/0, dup_count/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TAB,      ?MODULE).
%% TTL covers the full propagation tail of a sustained publish flood
%% across the mesh. Pre-2026-05-16 default was 120s, which expired
%% mid-flood under sustained 1.5k+/s torture (a 100k-event burst
%% takes ~67s, and slower bloom-fan paths add another ~60s tail);
%% expired entries let the same `(publisher, seq)' re-deliver, with
%% receivers seeing the EVENT twice. 5 min covers any realistic
%% sustained burst with bounded memory growth (set entries are
%% ~100 bytes each — 100k events ≈ 10MB per station, swept regularly).
-define(TTL_MS,   300_000).
-define(SWEEP_MS, 30_000).
-define(PT_COUNTER, {?MODULE, dup_counter}).

-record(state, {}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Record `{Publisher, Seq}'. Returns `new' if this pair has not
%% been recorded within the dedup window, `duplicate' otherwise.
%% Atomic. Callable from any process (the table is public). A
%% malformed key is treated as `new' — never dedup something we
%% cannot key, and never crash the caller.
-spec seen_or_record(macula_identity:pubkey(), non_neg_integer()) ->
    new | duplicate.
seen_or_record(Publisher, Seq)
  when is_binary(Publisher), is_integer(Seq), Seq >= 0 ->
    record(ets:whereis(?TAB), Publisher, Seq);
seen_or_record(_Publisher, _Seq) ->
    new.

%% Table absent (cache process restarting) → don't dedup; never crash
%% the caller.
record(undefined, _Publisher, _Seq) ->
    new;
record(_Tab, Publisher, Seq) ->
    case ets:insert_new(?TAB, {{Publisher, Seq}, now_ms()}) of
        true  -> new;
        false -> bump_dup()
    end.

%% @doc As `seen_or_record/2', additionally scoped by `Topic'.
%%
%% Why: a bare `(Publisher, Seq)' key assumes every SDK maintains ONE
%% collision-free sequence counter per identity across its ENTIRE
%% publish surface. That assumption doesn't hold today -- e.g.
%% macula-go's `Session.Call' auto-publishes RPC telemetry facts
%% (`rpc.sent_v1'/`rpc.completed_v1') under the caller's own identity
%% from a process-wide counter starting at 1, independent of whatever
%% counter an application uses for its own business publishes on a
%% DIFFERENT topic. The first business publish on a fresh process,
%% seeded the same way (seq=1, the SDK's own documented/tested
%% convention), collides with that fact's key and was silently
%% dropped here as a "duplicate" -- a real message lost, not a loop.
%% Scoping by Topic (a genuine loop still repeats the identical
%% (publisher, seq, topic) triple, so loop-kill is unaffected) turns
%% that into two distinct, both-delivered keys. See
%% `macula_station_route_pubsub_frames:event_dedup_disposition/1' and
%% `record_origin_seq/2', the only callers -- both reach this only
%% with a Topic that is already part of what `publisher_sig' (EVENT)
%% or the connection's own attested identity (PUBLISH) covers, so this
%% adds no new trust requirement over the 2-arity form.
%%
%% A distinct key shape (3-tuple vs. the 2-arity form's 2-tuple) --
%% never collides with a bare `{Publisher, Seq}' entry recorded via
%% the arity-2 API, so the two forms coexist safely in one table.
-spec seen_or_record(macula_identity:pubkey(), non_neg_integer(), binary()) ->
    new | duplicate.
seen_or_record(Publisher, Seq, Topic)
  when is_binary(Publisher), is_integer(Seq), Seq >= 0, is_binary(Topic) ->
    record(ets:whereis(?TAB), Publisher, Seq, Topic);
seen_or_record(_Publisher, _Seq, _Topic) ->
    new.

record(undefined, _Publisher, _Seq, _Topic) ->
    new;
record(Tab, Publisher, Seq, Topic) ->
    case ets:insert_new(Tab, {{Publisher, Seq, Topic}, now_ms()}) of
        true  -> new;
        false -> bump_dup()
    end.

%% @doc Read-only peek. Returns `seen' if `{Publisher, Seq}' is in
%% the cache, `absent' otherwise. Does NOT insert and does NOT bump
%% the duplicate counter — used by the dispatcher's pre-verify
%% fast-path to skip Ed25519 work for known-duplicate EVENTs. The
%% post-verify `seen_or_record/2' on first arrival still does the
%% atomic insert + bump.
-spec peek(macula_identity:pubkey(), non_neg_integer()) -> seen | absent.
peek(Publisher, Seq)
  when is_binary(Publisher), is_integer(Seq), Seq >= 0 ->
    peek_lookup(ets:whereis(?TAB), Publisher, Seq);
peek(_Publisher, _Seq) ->
    absent.

peek_lookup(undefined, _Publisher, _Seq) -> absent;
peek_lookup(_Tab, Publisher, Seq) ->
    case ets:lookup(?TAB, {Publisher, Seq}) of
        []    -> absent;
        [_|_] -> seen
    end.

%% @doc As `peek/2', additionally scoped by `Topic' -- the pre-verify
%% fast-path counterpart of `seen_or_record/3'; see its own doc for
%% why. Read-only, same as `peek/2'.
-spec peek(macula_identity:pubkey(), non_neg_integer(), binary()) -> seen | absent.
peek(Publisher, Seq, Topic)
  when is_binary(Publisher), is_integer(Seq), Seq >= 0, is_binary(Topic) ->
    peek_lookup(ets:whereis(?TAB), Publisher, Seq, Topic);
peek(_Publisher, _Seq, _Topic) ->
    absent.

peek_lookup(undefined, _Publisher, _Seq, _Topic) -> absent;
peek_lookup(Tab, Publisher, Seq, Topic) ->
    case ets:lookup(Tab, {Publisher, Seq, Topic}) of
        []    -> absent;
        [_|_] -> seen
    end.

%% @doc Number of `{publisher, seq}' pairs currently in the window.
-spec window_size() -> non_neg_integer().
window_size() ->
    case ets:info(?TAB, size) of
        undefined -> 0;
        N         -> N
    end.

%% @doc Total duplicates observed since the cache started (telemetry —
%% lets an operator gauge loop-back volume without raising log level).
-spec dup_count() -> non_neg_integer().
dup_count() ->
    case persistent_term:get(?PT_COUNTER, undefined) of
        undefined -> 0;
        Ref       -> counters:get(Ref, 1)
    end.

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    ?TAB = ets:new(?TAB, [named_table, public, set,
                          {read_concurrency, true},
                          {write_concurrency, true}]),
    Ref = counters:new(1, [write_concurrency]),
    persistent_term:put(?PT_COUNTER, Ref),
    logger:info("[event_dedup] (publisher,seq) cache installed "
                "(observe-only — step 2; ttl=~bms sweep=~bms)",
                [?TTL_MS, ?SWEEP_MS]),
    schedule_sweep(),
    {ok, #state{}}.

handle_call(_Req, _From, S) -> {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) -> {noreply, S}.

handle_info(sweep, S) ->
    Cutoff = now_ms() - ?TTL_MS,
    _ = ets:select_delete(?TAB, [{{'_', '$1'}, [{'<', '$1', Cutoff}], [true]}]),
    schedule_sweep(),
    {noreply, S};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%====================================================================
%% Internal
%%====================================================================

bump_dup() ->
    case persistent_term:get(?PT_COUNTER, undefined) of
        undefined -> ok;
        Ref       -> counters:add(Ref, 1, 1)
    end,
    duplicate.

now_ms() -> erlang:monotonic_time(millisecond).

schedule_sweep() ->
    _ = erlang:send_after(?SWEEP_MS, self(), sweep),
    ok.
