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
