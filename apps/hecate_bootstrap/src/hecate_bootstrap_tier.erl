%% @doc Bootstrap tier behaviour.
%%
%% Each tier in the 5-tier cascade (Part 5 §3) implements this
%% behaviour. The orchestrator spawns one probe per tier at the
%% tier's declared `stagger_ms/0' offset from cascade start; the
%% first tier to yield at least `min_peers' verified peers wins.
%%
%% Probes MUST return already-verified peers — signature and expiry
%% checks are the tier's responsibility because each tier's trust
%% path is different (Tier A trusts foundation pubkeys, Tier B
%% trusts the local segment, Tier E trusts the operator's paste).
%%
%% Reference: plans/PLAN_MACULA_V2_PART5_BOOTSTRAP.md §3.
-module(hecate_bootstrap_tier).

-export_type([tier/0, verified_peer/0, probe_result/0, probe_opts/0]).

-type tier() :: a | b | c | d | e.

-type verified_peer() :: #{
    node_id   := macula_identity:pubkey(),
    record    := macula_record:record(),
    addresses := [map()],
    tier      := tier(),
    via       := module()
}.

-type probe_opts() :: map().

-type probe_result() :: {ok, [verified_peer()]} | {error, term()}.

-callback tier() -> tier().
-callback stagger_ms() -> non_neg_integer().
-callback probe(probe_opts()) -> probe_result().
