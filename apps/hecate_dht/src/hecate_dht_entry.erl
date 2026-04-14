%% @doc A single peer entry in the Kademlia routing table.
%%
%% An entry bundles the identifying `NodeId' (Ed25519 pubkey, 32 bytes)
%% with the *diversity metadata* needed for tier-diverse bucket admission
%% (ASN, country, tier) and the *freshness metadata* used during scoring
%% (`first_seen', `last_seen', `observed_latency_ms', `uptime_frac').
%%
%% This module is a pure data library: all functions are deterministic
%% and side-effect free. Wall-clock inputs are injected so tests stay
%% hermetic.
%%
%% Reference: plans/PLAN_MACULA_V2_PART3_DISCOVERY.md §4.3
%% (tier-diverse buckets + scored admission).
-module(hecate_dht_entry).

-export([
    new/1,
    new/2,
    node_id/1,
    endpoints/1,
    asn/1,
    country/1,
    tier/1,
    first_seen/1,
    last_seen/1,
    latency/1,
    uptime/1,
    touch/1,
    touch/2,
    with_latency/2,
    with_uptime/2,
    age_ms/1,
    age_ms/2,
    time_in_table_ms/1,
    time_in_table_ms/2,
    is_entry/1
]).

-export_type([entry/0, tier/0, country/0, spec/0]).

-type tier()    :: t0 | t1 | t2 | t3.
-type country() :: <<_:16>>.

-type spec() :: #{
    node_id             := hecate_dht_xor:id(),
    endpoints           => [term()],
    asn                 := non_neg_integer(),
    country             := country(),
    tier                := tier(),
    observed_latency_ms => non_neg_integer() | undefined,
    uptime_frac         => float()
}.

-type entry() :: #{
    node_id             := hecate_dht_xor:id(),
    endpoints           := [term()],
    asn                 := non_neg_integer(),
    country             := country(),
    tier                := tier(),
    first_seen          := integer(),
    last_seen           := integer(),
    observed_latency_ms := non_neg_integer() | undefined,
    uptime_frac         := float()
}.

%%=====================================================================
%% Construction
%%=====================================================================

%% @doc Build an entry; freshness timestamps default to `monotonic_ms/0'.
-spec new(spec()) -> entry().
new(Spec) ->
    new(Spec, monotonic_ms()).

%% @doc Build an entry with an explicit `now' (millisecond monotonic time).
-spec new(spec(), integer()) -> entry().
new(#{node_id := <<_:256>>,
      asn := Asn,
      country := <<_:16>> = Country,
      tier := Tier} = Spec, Now)
  when is_integer(Asn), Asn >= 0,
       Tier =:= t0 orelse Tier =:= t1 orelse Tier =:= t2 orelse Tier =:= t3 ->
    #{
        node_id             => maps:get(node_id, Spec),
        endpoints           => maps:get(endpoints, Spec, []),
        asn                 => Asn,
        country             => Country,
        tier                => Tier,
        first_seen          => Now,
        last_seen           => Now,
        observed_latency_ms => maps:get(observed_latency_ms, Spec, undefined),
        uptime_frac         => clamp_fraction(maps:get(uptime_frac, Spec, 0.0))
    }.

%%=====================================================================
%% Accessors
%%=====================================================================

-spec node_id(entry())   -> hecate_dht_xor:id().
node_id(#{node_id := V})     -> V.

-spec endpoints(entry()) -> [term()].
endpoints(#{endpoints := V}) -> V.

-spec asn(entry())       -> non_neg_integer().
asn(#{asn := V})             -> V.

-spec country(entry())   -> country().
country(#{country := V})     -> V.

-spec tier(entry())      -> tier().
tier(#{tier := V})           -> V.

-spec first_seen(entry()) -> integer().
first_seen(#{first_seen := V}) -> V.

-spec last_seen(entry())  -> integer().
last_seen(#{last_seen := V})   -> V.

-spec latency(entry())   -> non_neg_integer() | undefined.
latency(#{observed_latency_ms := V}) -> V.

-spec uptime(entry())    -> float().
uptime(#{uptime_frac := V})  -> V.

%%=====================================================================
%% Mutators (return new entries — maps are immutable)
%%=====================================================================

-spec touch(entry()) -> entry().
touch(E) ->
    touch(E, monotonic_ms()).

-spec touch(entry(), integer()) -> entry().
touch(E, Now) ->
    E#{last_seen := Now}.

-spec with_latency(entry(), non_neg_integer()) -> entry().
with_latency(E, Ms) when is_integer(Ms), Ms >= 0 ->
    E#{observed_latency_ms := Ms}.

-spec with_uptime(entry(), number()) -> entry().
with_uptime(E, Frac) ->
    E#{uptime_frac := clamp_fraction(Frac)}.

%%=====================================================================
%% Derived timings
%%=====================================================================

-spec age_ms(entry()) -> non_neg_integer().
age_ms(E) ->
    age_ms(E, monotonic_ms()).

-spec age_ms(entry(), integer()) -> non_neg_integer().
age_ms(#{last_seen := LS}, Now) ->
    max(0, Now - LS).

-spec time_in_table_ms(entry()) -> non_neg_integer().
time_in_table_ms(E) ->
    time_in_table_ms(E, monotonic_ms()).

-spec time_in_table_ms(entry(), integer()) -> non_neg_integer().
time_in_table_ms(#{first_seen := FS}, Now) ->
    max(0, Now - FS).

%%=====================================================================
%% Predicates
%%=====================================================================

-spec is_entry(term()) -> boolean().
is_entry(#{node_id := <<_:256>>,
           asn := _, country := <<_:16>>, tier := _,
           first_seen := _, last_seen := _,
           observed_latency_ms := _, uptime_frac := _,
           endpoints := _}) -> true;
is_entry(_) -> false.

%%=====================================================================
%% Internal
%%=====================================================================

-spec monotonic_ms() -> integer().
monotonic_ms() ->
    erlang:monotonic_time(millisecond).

-spec clamp_fraction(number()) -> float().
clamp_fraction(F) when is_integer(F) ->
    clamp_fraction(float(F));
clamp_fraction(F) when F < 0.0 -> 0.0;
clamp_fraction(F) when F > 1.0 -> 1.0;
clamp_fraction(F) -> F.
