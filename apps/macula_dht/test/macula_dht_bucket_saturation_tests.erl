%% What a FULL bucket actually does, as opposed to what its scoring
%% docstring implies.
%%
%% The admission score is
%%   1.0*uptime + 0.8*(novelty/3) + 0.3*incumbency - 0.5*latency_frac
%% and the docstring explains the weighting as preventing Sybils from
%% eclipsing stable peers. That reasoning assumes the four inputs vary.
%% In this tree three of them are constants:
%%
%%   uptime_frac         defaults to 0.0 and NOTHING sets it. No call to
%%                       macula_dht_entry:with_uptime/2 exists outside
%%                       the entry module itself.
%%   observed_latency_ms is never set either, and latency_frac(undefined)
%%                       is 1.0, so every entry carries the same -0.5.
%%   novelty             is computed from asn/country/tier, and
%%                       macula_station_peer_observer:direct_peer_spec/2
%%                       stamps asn => 0, country => <<"??">>, tier => t0
%%                       on every peer it observes, station or daemon.
%%
%% So the score collapses to incumbency, which is age in table. A fresh
%% candidate scores 0 on the only term that varies, and maybe_evict/4
%% rejects on `CandScore =< MinScore'. A full bucket therefore admits
%% NOTHING, ever, whatever the candidate is.
%%
%% That is the opposite of the risk it looks like. The concern worth
%% checking was "a churning daemon evicts a stable station"; the actual
%% behaviour is that a bucket freezes with whatever connected first, and
%% since daemons vastly outnumber stations on this fleet, a long-running
%% core station ends up unable to admit a station it has never met.
%%
%% These tests pin the real behaviour so a future change to the scoring
%% inputs is a deliberate, visible decision rather than a silent one.
-module(macula_dht_bucket_saturation_tests).

-include_lib("eunit/include/eunit.hrl").

-define(K, 20).
-define(DAY_MS, 86_400_000).

%%---------------------------------------------------------------------
%% A full bucket rejects everything
%%---------------------------------------------------------------------

full_bucket_rejects_a_newcomer_test() ->
    %% Incumbents seated at t=0, candidate arrives an hour later, so the
    %% incumbents hold strictly more incumbency than it does.
    B = seated_bucket(0),
    Cand = entry(<<99:256>>),
    ?assertMatch({rejected, _}, macula_dht_bucket:insert(Cand, B, 3_600_000)).

%% THE MECHANISM IS NOT BROKEN — it is starved of inputs.
%%
%% Give a candidate a distinct ASN, country and tier and it DOES evict the
%% weakest incumbent, exactly as the scoring docstring describes. So the
%% saturation above is not a defect in the algorithm; it is a consequence of
%% every production entry carrying identical diversity fields, which makes
%% novelty a constant and leaves incumbency as the only live term.
%%
%% This is the test that will start failing usefully. The day anything
%% populates asn/country for real -- and `macula_dht_lookup:ref_to_spec/1'
%% already propagates whatever a NODES reply carries -- eviction wakes up and
%% full buckets stop being frozen. Pin it now so that transition is visible.
full_bucket_admits_a_diverse_newcomer_by_eviction_test() ->
    B = seated_bucket(0),
    Cand = macula_dht_entry:new(#{node_id => <<98:256>>,
                                  endpoints => [],
                                  asn => 64500, country => <<"SE">>,
                                  tier => t3}, 3_600_000),
    ?assertMatch({replaced, _Evicted, _B1},
                 macula_dht_bucket:insert(Cand, B, 3_600_000)).

%% The reverse of the risk that prompted this: an incumbent is NOT at
%% risk from a newly-arriving peer. Nothing is evicted at all.
full_bucket_evicts_nobody_test() ->
    B0 = seated_bucket(0),
    {rejected, B1} = macula_dht_bucket:insert(entry(<<97:256>>), B0, 3_600_000),
    Before = [macula_dht_entry:node_id(E) || E <- macula_dht_bucket:members(B0)],
    After  = [macula_dht_entry:node_id(E) || E <- macula_dht_bucket:members(B1)],
    ?assertEqual(lists:sort(Before), lists:sort(After)),
    ?assertEqual(?K, length(After)).

%% An entry already present is still refreshed — saturation blocks
%% admission, not liveness tracking.
full_bucket_still_touches_a_known_peer_test() ->
    B = seated_bucket(0),
    Known = macula_dht_entry:node_id(hd(macula_dht_bucket:members(B))),
    ?assertMatch({touched, _},
                 macula_dht_bucket:insert(entry(Known), B, 3_600_000)).

%%---------------------------------------------------------------------
%% Why: the score has only one live term
%%---------------------------------------------------------------------

%% Two entries that differ ONLY in the fields production never sets
%% score identically. If this test ever fails, someone has started
%% populating uptime or latency and the eviction behaviour above will
%% have changed with it — which is exactly when you want to be told.
production_shaped_entries_score_identically_test() ->
    A = entry(<<1:256>>),
    B = entry(<<2:256>>),
    Ref = [A, B],
    ?assertEqual(macula_dht_bucket:score(A, Ref, 0),
                 macula_dht_bucket:score(B, Ref, 0)).

%% Incumbency is the only term that moves, and it is capped at a day.
incumbency_is_the_only_varying_term_test() ->
    E = entry(<<3:256>>),
    Fresh = macula_dht_bucket:score(E, [E], 0),
    Aged  = macula_dht_bucket:score(E, [E], ?DAY_MS),
    ?assert(Aged > Fresh),
    %% 0.3 weight on a term capped at 1.0.
    ?assert(abs((Aged - Fresh) - 0.3) < 0.0001).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

%% Exactly the spec macula_station_peer_observer:direct_peer_spec/2
%% produces for an inbound peer: no endpoints, no asn, no country, t0.
entry(Id) ->
    entry(Id, 0).

entry(Id, Now) ->
    macula_dht_entry:new(#{node_id   => Id,
                           endpoints => [],
                           asn       => 0,
                           country   => <<"??">>,
                           tier      => t0}, Now).

seated_bucket(Now) ->
    lists:foldl(fun(N, B) ->
        {admitted, B1} = macula_dht_bucket:insert(entry(<<N:256>>, Now), B, Now),
        B1
    end, macula_dht_bucket:new(?K), lists:seq(1, ?K)).
