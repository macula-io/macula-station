%% @doc Peer-discovery strategy behaviour.
%%
%% Each strategy in the cascade (Part 5 §3) implements this behaviour.
%% The orchestrator spawns one probe per strategy at the strategy's
%% declared `stagger_ms/0' offset from cascade start; the first
%% strategy to yield at least `min_peers' verified peers wins.
%%
%% Probes MUST return already-verified peers — signature and expiry
%% checks are the strategy's responsibility because each strategy's
%% trust path is different (`via_doh' trusts foundation pubkeys,
%% `via_mdns' trusts the local segment via per-peer QUIC handshake,
%% `via_operator_paste' trusts the operator's paste).
%%
%% Reference: plans/PLAN_MACULA_V2_PART5_BOOTSTRAP.md §3.
-module(macula_bootstrap_peer_discoverer).

-export_type([strategy/0, verified_peer/0,
              discover_result/0, discover_opts/0]).

-type strategy() :: via_doh
                  | via_mdns
                  | via_mainline_dht
                  | via_blockchain
                  | via_operator_paste
                  | via_seed_dial.

-type verified_peer() :: #{
    node_id      := macula_identity:pubkey(),
    %% Network strategies (`via_doh' / `via_mdns' / `via_mainline_dht'
    %% / `via_blockchain' / `via_operator_paste') carry a foundation-
    %% or peer-signed `node_record'; `via_seed_dial' (which learns
    %% peers via outbound CONNECT/HELLO handshake only) leaves this
    %% `undefined'. The DHT ingest path consumes `node_id' +
    %% `addresses' regardless and ignores the `record' field.
    record       := macula_record:record() | undefined,
    addresses    := [map()],
    strategy     := strategy(),
    via          := module(),
    %% Station-class tier (3 | 4) carried in foundation seed lists
    %% and surfaced unchanged for downstream DHT routing-table
    %% ingestion. NOT the discovery strategy — that's `strategy'.
    gateway_tier => 3 | 4 | undefined
}.

-type discover_opts() :: map().

-type discover_result() :: {ok, [verified_peer()]} | {error, term()}.

-callback strategy() -> strategy().
-callback stagger_ms() -> non_neg_integer().
-callback discover(discover_opts()) -> discover_result().
