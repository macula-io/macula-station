%% @doc Diversity-constrained replica placement (Part 3 §5.2).
%%
%% Given a list of candidate stations and a target key, pick up to
%% `K' replicas closest-to-key by XOR while enforcing per-set
%% diversity constraints:
%%
%% <ul>
%%   <li>≥8 distinct ASNs</li>
%%   <li>≥5 distinct countries</li>
%%   <li>≥3 tiers represented</li>
%%   <li>≤3 replicas per ASN (soft cap against operator concentration)</li>
%% </ul>
%%
%% == Algorithm ==
%%
%% Candidates are sorted by ascending XOR distance to the target
%% key. We walk the sorted list greedily. A candidate is *admissible*
%% if:
%%
%% <ol>
%%   <li>Its NodeId is not already chosen.</li>
%%   <li>Accepting it would not exceed the per-ASN cap.</li>
%%   <li>After accepting it, the remaining slots are enough to
%%       cover the maximum remaining diversity deficit.</li>
%% </ol>
%%
%% The third rule is the core of §5.2: we only accept a
%% diversity-neutral candidate (same ASN/country/tier as something
%% already chosen) when we have enough slots left to still fill the
%% unmet diversity minimums. This keeps distance primary while
%% guaranteeing diversity when the candidate pool allows it.
%%
%% == Degraded flag ==
%%
%% If the candidate pool cannot satisfy the constraints, the
%% resulting `chosen' set is reported with `degraded = true' and the
%% caller (typically `hecate_dht_store') publishes the record with
%% `diversity_degraded = true' telemetry per Part 3 §5.2.
%%
%% This module is pure: no processes, no ETS, no I/O. Inputs that
%% fully specify the decision → deterministic output.
-module(hecate_dht_placement).

-export([place/2, place/3, default_constraints/0]).
-export_type([constraints/0, placement_result/0, counts/0]).

-define(DEFAULT_K,            20).
-define(DEFAULT_ASN_MIN,       8).
-define(DEFAULT_COUNTRY_MIN,   5).
-define(DEFAULT_TIER_MIN,      3).
-define(DEFAULT_OPERATOR_MAX,  3).

-type constraints() :: #{
    k            := pos_integer(),
    asn_min      := non_neg_integer(),
    country_min  := non_neg_integer(),
    tier_min     := non_neg_integer(),
    operator_max := pos_integer()
}.

-type counts() :: #{
    asns      := non_neg_integer(),
    countries := non_neg_integer(),
    tiers     := non_neg_integer()
}.

-type placement_result() :: #{
    chosen   := [hecate_frame:station_ref()],
    degraded := boolean(),
    counts   := counts()
}.

%%=====================================================================
%% Public API
%%=====================================================================

-spec default_constraints() -> constraints().
default_constraints() ->
    #{k            => ?DEFAULT_K,
      asn_min      => ?DEFAULT_ASN_MIN,
      country_min  => ?DEFAULT_COUNTRY_MIN,
      tier_min     => ?DEFAULT_TIER_MIN,
      operator_max => ?DEFAULT_OPERATOR_MAX}.

-spec place(hecate_dht_xor:id(), [hecate_frame:station_ref()]) ->
        placement_result().
place(Key, Candidates) ->
    place(Key, Candidates, default_constraints()).

-spec place(hecate_dht_xor:id(),
            [hecate_frame:station_ref()],
            constraints()) -> placement_result().
place(<<_:256>> = Key, Candidates, Constraints) ->
    Sorted     = sort_by_distance(Candidates, Key),
    Phase1     = lists:reverse(greedy(Sorted, [], Constraints)),
    Chosen     = fill_remaining(Sorted, Phase1, Constraints),
    #{
        chosen   => Chosen,
        degraded => not constraints_met(Chosen, Constraints),
        counts   => counts_of(Chosen)
    }.

%% @doc Second-pass filler — takes closest unchosen candidates (subject
%% only to the operator cap + dedup, not the diversity deficit gate)
%% up to K. Runs when Phase 1's diversity-aware admission leaves the
%% chosen set under-sized because the pool cannot satisfy the
%% diversity minimums. The resulting set is returned as-is with the
%% `degraded' flag set.
-spec fill_remaining([hecate_frame:station_ref()],
                     [hecate_frame:station_ref()],
                     constraints()) -> [hecate_frame:station_ref()].
fill_remaining(Sorted, Chosen, #{k := K} = Constraints) ->
    Pool = [R || R <- Sorted, not already_chosen(R, Chosen)],
    fill_loop(Pool, Chosen, K - length(Chosen), Constraints).

-spec fill_loop([hecate_frame:station_ref()],
                [hecate_frame:station_ref()],
                non_neg_integer(), constraints()) ->
          [hecate_frame:station_ref()].
fill_loop(_Pool, Chosen, 0, _Constraints) ->
    Chosen;
fill_loop([], Chosen, _Slots, _Constraints) ->
    Chosen;
fill_loop([Cand | Rest], Chosen, Slots, Constraints) ->
    take_or_skip(within_operator_cap(Cand, Chosen, Constraints),
                 Cand, Rest, Chosen, Slots, Constraints).

-spec take_or_skip(boolean(), hecate_frame:station_ref(),
                   [hecate_frame:station_ref()],
                   [hecate_frame:station_ref()],
                   non_neg_integer(), constraints()) ->
          [hecate_frame:station_ref()].
take_or_skip(true,  Cand, Rest, Chosen, Slots, C) ->
    fill_loop(Rest, Chosen ++ [Cand], Slots - 1, C);
take_or_skip(false, _,    Rest, Chosen, Slots, C) ->
    fill_loop(Rest, Chosen, Slots, C).

%%=====================================================================
%% Greedy selection
%%=====================================================================

-spec greedy([hecate_frame:station_ref()],
             [hecate_frame:station_ref()],
             constraints()) -> [hecate_frame:station_ref()].
greedy(_Rest, Chosen, #{k := K}) when length(Chosen) >= K ->
    Chosen;
greedy([], Chosen, _Constraints) ->
    Chosen;
greedy([Cand | Rest], Chosen, Constraints) ->
    maybe_take(admissible(Cand, Chosen, Constraints),
               Cand, Rest, Chosen, Constraints).

-spec maybe_take(boolean(), hecate_frame:station_ref(),
                 [hecate_frame:station_ref()],
                 [hecate_frame:station_ref()],
                 constraints()) -> [hecate_frame:station_ref()].
maybe_take(true,  Cand, Rest, Chosen, C) -> greedy(Rest, [Cand | Chosen], C);
maybe_take(false, _,    Rest, Chosen, C) -> greedy(Rest, Chosen, C).

%%=====================================================================
%% Admissibility
%%=====================================================================

-spec admissible(hecate_frame:station_ref(),
                 [hecate_frame:station_ref()],
                 constraints()) -> boolean().
admissible(Cand, Chosen, Constraints) ->
    not already_chosen(Cand, Chosen)
        andalso within_operator_cap(Cand, Chosen, Constraints)
        andalso room_for_remaining_deficit(Cand, Chosen, Constraints).

-spec already_chosen(hecate_frame:station_ref(),
                     [hecate_frame:station_ref()]) -> boolean().
already_chosen(Cand, Chosen) ->
    Id = id_of(Cand),
    lists:any(fun(X) -> id_of(X) =:= Id end, Chosen).

-spec within_operator_cap(hecate_frame:station_ref(),
                          [hecate_frame:station_ref()],
                          constraints()) -> boolean().
within_operator_cap(Cand, Chosen, #{operator_max := Max}) ->
    count_asn(asn_of(Cand), Chosen) < Max.

%% @doc After tentatively adding Cand, would we still have enough
%% slots left to reach every diversity minimum? A slot can contribute
%% to any dimension, so the gating figure is `max(deficits)'.
-spec room_for_remaining_deficit(hecate_frame:station_ref(),
                                 [hecate_frame:station_ref()],
                                 constraints()) -> boolean().
room_for_remaining_deficit(Cand, Chosen,
                           #{k := K, asn_min := Am, country_min := Cm,
                             tier_min := Tm}) ->
    NextChosen = [Cand | Chosen],
    AsnAfter     = distinct_asns(NextChosen),
    CountryAfter = distinct_countries(NextChosen),
    TierAfter    = distinct_tiers(NextChosen),
    SlotsLeft    = K - length(NextChosen),
    AsnDeficit   = max(0, Am - AsnAfter),
    CntryDeficit = max(0, Cm - CountryAfter),
    TierDeficit  = max(0, Tm - TierAfter),
    MaxDeficit   = lists:max([AsnDeficit, CntryDeficit, TierDeficit]),
    SlotsLeft >= MaxDeficit.

%%=====================================================================
%% Diversity counting
%%=====================================================================

-spec constraints_met([hecate_frame:station_ref()], constraints()) ->
          boolean().
constraints_met(Chosen,
                #{k := K, asn_min := Am, country_min := Cm, tier_min := Tm}) ->
    length(Chosen)       >= K
        andalso distinct_asns(Chosen)      >= Am
        andalso distinct_countries(Chosen) >= Cm
        andalso distinct_tiers(Chosen)     >= Tm.

-spec counts_of([hecate_frame:station_ref()]) -> counts().
counts_of(Chosen) ->
    #{asns      => distinct_asns(Chosen),
      countries => distinct_countries(Chosen),
      tiers     => distinct_tiers(Chosen)}.

-spec distinct_asns([hecate_frame:station_ref()]) -> non_neg_integer().
distinct_asns(Refs) ->
    distinct(fun asn_of/1, Refs).

-spec distinct_countries([hecate_frame:station_ref()]) -> non_neg_integer().
distinct_countries(Refs) ->
    distinct(fun country_of/1, Refs).

-spec distinct_tiers([hecate_frame:station_ref()]) -> non_neg_integer().
distinct_tiers(Refs) ->
    distinct(fun tier_of/1, Refs).

-spec distinct(fun((hecate_frame:station_ref()) -> term()),
               [hecate_frame:station_ref()]) -> non_neg_integer().
distinct(F, Refs) ->
    sets:size(sets:from_list([F(R) || R <- Refs])).

-spec count_asn(term(), [hecate_frame:station_ref()]) -> non_neg_integer().
count_asn(Asn, Refs) ->
    length([1 || R <- Refs, asn_of(R) =:= Asn]).

%%=====================================================================
%% Accessors
%%=====================================================================

-spec id_of(hecate_frame:station_ref())      -> hecate_identity:pubkey().
id_of(#{node_id := V})      -> V.

-spec asn_of(hecate_frame:station_ref())     -> non_neg_integer() | undefined.
asn_of(#{asn := V})         -> V.

-spec country_of(hecate_frame:station_ref()) -> binary().
country_of(#{country := V}) -> V.

-spec tier_of(hecate_frame:station_ref())    -> 0..4.
tier_of(#{tier := V})       -> V.

%%=====================================================================
%% Sorting
%%=====================================================================

-spec sort_by_distance([hecate_frame:station_ref()], hecate_dht_xor:id()) ->
          [hecate_frame:station_ref()].
sort_by_distance(Refs, Key) ->
    Keyed = [{hecate_dht_xor:distance_int(Key, id_of(R)), R} || R <- Refs],
    [R || {_, R} <- lists:keysort(1, Keyed)].
