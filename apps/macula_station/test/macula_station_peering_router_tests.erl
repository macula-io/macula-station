%%% @doc Advertise propagation must reconcile, not just diff.
%%%
%%% The two-service torture found a re-advertise permanently losing its
%%% route on the one core station pair with no direct edge. Root cause
%%% (plans/DESIGN_ADVERTISE_PROPAGATION_RECONCILE.md): propagation diffed
%%% the local advertise set against the SENDER's memory of what it last
%%% sent a peer, and the periodic tick re-diffed the same memory, so once
%%% the sender believed it had sent an entry the peer did not hold, the
%%% two were inconsistent forever.
%%%
%%% The fix is a periodic full re-assert, matching what bloom_exchange
%%% and dht_replicate already do. `advertise_to_send/3' is where the diff
%%% and the reconcile diverge; these pin exactly that divergence.
-module(macula_station_peering_router_tests).
-include_lib("eunit/include/eunit.hrl").

-define(M, macula_station_peering_router).

set(L) -> sets:from_list(L).
sorted(S) -> lists:sort(sets:to_list(S)).

%%====================================================================
%% The bug, and its fix, in one pair of tests
%%====================================================================

%% Sender believes it already sent P (Last has P) and still has P
%% (LocalSet has P). The DIFF sends nothing — this is the exact state in
%% which a peer that dropped P never hears about it again.
diff_skips_an_entry_it_believes_was_sent_test() ->
    Local = set([{r, <<"P">>}]),
    Last  = set([{r, <<"P">>}]),
    {ToAdd, ToDrop} = ?M:advertise_to_send(false, Local, Last),
    ?assertEqual([], sorted(ToAdd)),
    ?assertEqual([], sorted(ToDrop)).

%% The RECONCILE re-asserts P regardless of Last — healing exactly the
%% divergence the diff cannot see.
reconcile_reasserts_what_the_diff_would_skip_test() ->
    Local = set([{r, <<"P">>}]),
    Last  = set([{r, <<"P">>}]),
    {ToAdd, ToDrop} = ?M:advertise_to_send(true, Local, Last),
    ?assertEqual([{r, <<"P">>}], sorted(ToAdd)),
    ?assertEqual([], sorted(ToDrop)).

%%====================================================================
%% Neither mode may break the properties the diff already had
%%====================================================================

diff_sends_only_the_delta_test() ->
    Local = set([{r, <<"P">>}, {r, <<"Q">>}]),
    Last  = set([{r, <<"P">>}]),
    {ToAdd, ToDrop} = ?M:advertise_to_send(false, Local, Last),
    ?assertEqual([{r, <<"Q">>}], sorted(ToAdd)),
    ?assertEqual([], sorted(ToDrop)).

%% A drop the sender knows about (in Last, not in LocalSet) propagates in
%% BOTH modes — reconcile heals missing adds without dropping known
%% removes on the floor.
both_modes_propagate_a_known_drop_test() ->
    Local = set([{r, <<"P">>}]),
    Last  = set([{r, <<"P">>}, {r, <<"gone">>}]),
    {AddD, DropD} = ?M:advertise_to_send(false, Local, Last),
    {AddR, DropR} = ?M:advertise_to_send(true, Local, Last),
    ?assertEqual([{r, <<"gone">>}], sorted(DropD)),
    ?assertEqual([{r, <<"gone">>}], sorted(DropR)),
    %% reconcile re-asserts the survivor; diff, having sent P before,
    %% does not.
    ?assertEqual([{r, <<"P">>}], sorted(AddR)),
    ?assertEqual([], sorted(AddD)).

reconcile_on_empty_local_is_all_drops_test() ->
    Local = set([]),
    Last  = set([{r, <<"P">>}]),
    {ToAdd, ToDrop} = ?M:advertise_to_send(true, Local, Last),
    ?assertEqual([], sorted(ToAdd)),
    ?assertEqual([{r, <<"P">>}], sorted(ToDrop)).

reconcile_to_a_fresh_peer_sends_everything_test() ->
    %% Last empty (never synced this peer): diff and reconcile agree —
    %% send the whole set.
    Local = set([{r, <<"P">>}, {r, <<"Q">>}]),
    {AddD, _} = ?M:advertise_to_send(false, Local, sets:new()),
    {AddR, _} = ?M:advertise_to_send(true, Local, sets:new()),
    ?assertEqual(sorted(Local), sorted(AddD)),
    ?assertEqual(sorted(Local), sorted(AddR)).

%%====================================================================
%% should_periodic_sync/2 — periodic ticks stop being an unconditional
%% O(topics x peers) poll now that every production path that can
%% change the desired set kicks this router directly (see the
%% moduledoc). A periodic tick still needs to sync on exactly two
%% occasions; every other periodic tick is a no-op.
%%====================================================================

first_tick_after_boot_always_syncs_test() ->
    %% SyncedOnce = false: subscriptions/peers may already exist before
    %% this process started, or a kick may have arrived while
    %% `whereis/1' still resolved to `undefined' and was silently lost
    %% -- so the very first periodic tick can't skip, regardless of
    %% Reconcile.
    ?assert(?M:should_periodic_sync(false, false)),
    ?assert(?M:should_periodic_sync(true, false)).

reconcile_tick_always_syncs_test() ->
    %% The periodic drift-healing safety net fires regardless of
    %% whether anything is believed to have changed since the last
    %% sync -- same reasoning as the ADVERTISE reconcile above.
    ?assert(?M:should_periodic_sync(true, true)).

ordinary_tick_after_first_sync_is_a_noop_test() ->
    %% The steady-state case this fix exists for: once the router has
    %% synced at least once and this isn't a reconcile tick, nothing
    %% has changed that a kick wouldn't already have caught -- skip
    %% the full O(topics x peers) recompute.
    ?assertNot(?M:should_periodic_sync(false, true)).

%%====================================================================
%% apply_sub_result/3 -- source B (async subscribe). `subscribe_one/5'
%% marks a Key `pending' the instant it spawns the worker, before the
%% real wire call has even started; these tests pin what happens when
%% that worker's outcome actually arrives, including the two STALE
%% cases (the sync loop moved on before the response came back).
%%====================================================================

test_key() -> {crypto:strong_rand_bytes(32), <<"t">>, self()}.

%% The common case: still waiting on exactly this attempt, and it
%% succeeded -- `subs' gets the real SubRef.
pending_key_accepts_a_successful_result_test() ->
    Key = test_key(),
    SubRef = make_ref(),
    Subs = ?M:apply_sub_result(Key, {ok, SubRef}, #{Key => pending}),
    ?assertEqual(#{Key => SubRef}, Subs).

%% Still waiting, but it failed -- drop the Key entirely rather than
%% leave it stuck `pending' forever; a later sync that still wants it
%% will attempt it fresh.
pending_key_drops_on_a_failed_result_test() ->
    Key = test_key(),
    Subs = ?M:apply_sub_result(Key, {error, timeout}, #{Key => pending}),
    ?assertEqual(#{}, Subs).

%% Stale: a later sync already decided against this Key (dropped from
%% `subs' entirely) before the worker's success arrived. That leaves a
%% real subscription nobody wants -- `subs' itself is untouched (there
%% was nothing to update), and the actual cleanup (an async unsubscribe
%% fires for the stale SubRef) is a side effect this pure function's
%% return value can't show; not independently covered by a test here.
stale_success_after_key_already_dropped_leaves_subs_unchanged_test() ->
    Key = test_key(),
    Subs = ?M:apply_sub_result(Key, {ok, make_ref()}, #{}),
    ?assertEqual(#{}, Subs).

%% Stale failure: nothing to do either way.
stale_failure_after_key_already_dropped_is_a_noop_test() ->
    Key = test_key(),
    Subs = ?M:apply_sub_result(Key, {error, timeout}, #{}),
    ?assertEqual(#{}, Subs).

%% Stale: the Key is present but no longer `pending' (already resolved
%% to a real SubRef by an earlier response, or reassigned) -- this
%% response belongs to an OLD attempt, not the current one. Treated
%% the same as "already dropped": `subs' is left alone here (the real
%% side effect, an async unsubscribe of the STALE SubRef this
%% response carries, is not observable from this pure function).
stale_success_when_key_already_holds_a_different_subref_test() ->
    Key = test_key(),
    Current = make_ref(),
    Subs = ?M:apply_sub_result(Key, {ok, make_ref()}, #{Key => Current}),
    ?assertEqual(#{Key => Current}, Subs).
