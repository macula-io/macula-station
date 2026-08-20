%% @doc Bound tests for the content relay's fanout selection
%% (`macula_station_content_handlers:fanout_candidates/2' +
%% `advance_seen/2'), the two pure functions that make raising
%% `?DEFAULT_HOPS' from 1 to 2 safe.
%%
%% Exists because the 1-hop cap being fixed here was NOT an arbitrary
%% conservatism — it was a direct response to a real incident: unbounded
%% width (every peer connection) with no de-duplication turned a 2-hop
%% walk into 8² = 64 concurrent inbound `_content.get_block' calls on a
%% station with 8 peers, which overwhelmed `macula_content_store' and
%% cascaded into a `peer_observer' crash-recovery loop (see
%% `docs/CASCADE_INVESTIGATION.md' for the related but distinct
%% conns_tab-accumulation incident this rhymes with). These tests assert
%% the bound holds against an ADVERSARIAL topology — a station with far
%% more peer connections than the fleet has ever run with — not just the
%% handful the fleet happens to have today.
-module(macula_station_content_fanout_bound_tests).
-include_lib("eunit/include/eunit.hrl").

-define(K, 3). %% mirrors ?GET_FANOUT_WIDTH == ?DEFAULT_REPLICATION_K in the module under test.

conn(N) ->
    Url = list_to_binary(io_lib:format("quic://peer-~p.example:4433", [N])),
    {Url, list_to_pid("<0.1." ++ integer_to_list(N) ++ ">")}.

%%%===================================================================
%%% fanout_candidates/2 — width cap
%%%===================================================================

fanout_candidates_caps_to_width_even_with_many_peers_test() ->
    %% 50 peer connections is far beyond anything the fleet has ever
    %% run (docs cite 3-8); the cap must hold regardless.
    Conns = [conn(N) || N <- lists:seq(1, 50)],
    Got = macula_station_content_handlers:fanout_candidates(Conns, []),
    ?assertEqual(?K, length(Got)).

fanout_candidates_returns_all_when_fewer_than_width_test() ->
    Conns = [conn(1), conn(2)],
    Got = macula_station_content_handlers:fanout_candidates(Conns, []),
    ?assertEqual(2, length(Got)).

fanout_candidates_empty_conns_yields_empty_test() ->
    ?assertEqual([], macula_station_content_handlers:fanout_candidates([], [])).

%%%===================================================================
%%% fanout_candidates/2 — seen exclusion
%%%===================================================================

fanout_candidates_excludes_seen_urls_test() ->
    Conns = [conn(1), conn(2), conn(3), conn(4)],
    {SeenUrl, _} = conn(1),
    Got = macula_station_content_handlers:fanout_candidates(Conns, [SeenUrl]),
    ?assertNot(lists:any(fun({Url, _}) -> Url =:= SeenUrl end, Got)),
    ?assertEqual(?K, length(Got)).

fanout_candidates_all_seen_yields_empty_test() ->
    Conns = [conn(1), conn(2)],
    Seen  = [Url || {Url, _} <- Conns],
    ?assertEqual([], macula_station_content_handlers:fanout_candidates(Conns, Seen)).

%%%===================================================================
%%% advance_seen/2
%%%===================================================================

advance_seen_appends_candidate_urls_test() ->
    Candidates = [conn(1), conn(2)],
    Got = macula_station_content_handlers:advance_seen([], Candidates),
    ?assertEqual([U || {U, _} <- Candidates], Got).

advance_seen_preserves_existing_seen_test() ->
    {ExistingUrl, _} = conn(9),
    Candidates = [conn(1)],
    Got = macula_station_content_handlers:advance_seen([ExistingUrl], Candidates),
    ?assertEqual([ExistingUrl, element(1, conn(1))], Got).

%%%===================================================================
%%% The actual bound: worst-case total calls across a 2-hop walk on an
%%% adversarial fully-connected topology, simulated by hand (no live
%%% processes — pure arithmetic over what fanout_candidates/2 +
%%% advance_seen/2 would produce at each simulated hop).
%%%===================================================================

two_hop_worst_case_call_count_is_bounded_test() ->
    %% Fully-connected N-station mesh, N far larger than the fleet
    %% (docs: "each station has 3-8 peer connections"). Every station's
    %% peer list is "every other station" — the worst case for fanout
    %% width, since it never runs dry before the cap does.
    N = 20,
    AllUrls = [element(1, conn(I)) || I <- lists:seq(1, N)],
    Conns = fun(SelfUrl) ->
        [conn(I) || I <- lists:seq(1, N), element(1, conn(I)) =/= SelfUrl]
    end,
    Reader = element(1, conn(1)),

    %% Hop 1: reader picks up to K candidates from its own peers.
    Hop1Candidates = macula_station_content_handlers:fanout_candidates(
                       Conns(Reader), []),
    Hop1Seen = macula_station_content_handlers:advance_seen([], Hop1Candidates),
    ?assertEqual(?K, length(Hop1Candidates)),

    %% Hop 2: EACH hop-1 candidate independently fans out again, using
    %% the SAME inherited seen list (this station's own peers, minus
    %% everything hop 1 already asked, including itself via mutual
    %% connectivity in a fully-connected mesh).
    Hop2Counts = [length(macula_station_content_handlers:fanout_candidates(
                            Conns(HopUrl), Hop1Seen))
                  || {HopUrl, _Pid} <- Hop1Candidates],
    TotalHop2 = lists:sum(Hop2Counts),
    TotalCalls = length(Hop1Candidates) + TotalHop2,

    %% The bound this whole change exists to guarantee: K + K*K, never
    %% the unbounded (N-1)^2 a width-uncapped walk on this topology
    %% would have produced (19*19 = 361 on this fixture, or 64 on the
    %% original incident's 8-peer topology).
    ?assertEqual(?K + ?K * ?K, TotalCalls),
    ?assert(TotalCalls < N * N),

    %% And every hop-2 candidate is genuinely new — seen exclusion
    %% actually did its job, not just the width cap.
    Hop2Urls = lists:append(
                 [[U || {U, _} <- macula_station_content_handlers:fanout_candidates(
                                    Conns(HopUrl), Hop1Seen)]
                  || {HopUrl, _Pid} <- Hop1Candidates]),
    ?assertEqual([], [U || U <- Hop2Urls, lists:member(U, Hop1Seen)]),
    %% Sanity: AllUrls is actually used to build a topology of size N.
    ?assertEqual(N, length(AllUrls)).
