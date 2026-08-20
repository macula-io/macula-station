%% @doc Shared foundation-record trust-boundary helpers.
%%
%% Every automated bootstrap tier (A / C / D) runs the same three-step
%% trust boundary against whatever bytes its transport hands back:
%%
%% <ol>
%%   <li>Decode the raw CBOR as a `macula_record:m_record()'
%%       — <em>safely</em>: external bytes may be arbitrary garbage
%%       and `macula_record_cbor:decode/1' crashes on malformed
%%       input rather than returning `{error, _}'.</li>
%%   <li>Validate the record against the firmware-embedded foundation
%%       trust anchor (`macula_foundation:verify_record/1').</li>
%%   <li>Walk `foundation_seed_list' payload seeds and emit one
%%       `verified_peer()' per seed, stamped with the caller's cascade
%%       tier atom and `via' module name.</li>
%% </ol>
%%
%% This module is the single home for steps (1) and (3). Step (2) is
%% `macula_foundation:verify_record/1' directly — one public trust
%% anchor, no duplication. Tier-specific integrity checks (storage
%% key for via_doh, BEP 44 signature for via_mainline_dht) remain strategy-local.
%%
%% Reference:
%% - PLAN_MACULA_V2_PART5_BOOTSTRAP.md §3, §4, §6, §7
%% - PLAN_MACULA_V2_PART6_PROTOCOL.md §9.14 (foundation_seed_list).
-module(macula_bootstrap_foundation).

-export([decode_record_bytes/1, peers_from_record/3]).

-export_type([decode_error/0]).

-type decode_error() :: bad_record_bytes | term().

%% @doc Decode raw record bytes coming from an untrusted transport.
%% Catches CBOR crashes and normalises them to
%% `{error, bad_record_bytes}' so a single garbage response cannot
%% crash a probe worker and force its tier to time out.
-spec decode_record_bytes(binary()) ->
          {ok, macula_record:m_record()} | {error, decode_error()}.
decode_record_bytes(Bytes) when is_binary(Bytes) ->
    try macula_record:decode(Bytes) of
        {ok, _} = Ok   -> Ok;
        {error, _} = E -> E
    catch
        _:_ -> {error, bad_record_bytes}
    end.

%% @doc Emit one `verified_peer()' per seed carried by an
%% already-foundation-verified `foundation_seed_list' record.
%%
%% The caller stamps `Strategy' (one of `via_doh | via_mdns |
%% via_mainline_dht | via_blockchain | via_operator_paste' per
%% `macula_bootstrap_peer_discoverer:strategy()') and `Via' (the strategy module
%% name) so downstream routing-table ingestion can track provenance.
%%
%% Records of other types (or records whose payload omits `seeds')
%% yield an empty list rather than crashing — defensive because the
%% caller has already performed `macula_foundation:verify_record/1',
%% but foundation may publish record types we don't know how to
%% enumerate seeds from.
-spec peers_from_record(macula_record:m_record(),
                        macula_bootstrap_peer_discoverer:strategy(),
                        module()) ->
          [macula_bootstrap_peer_discoverer:verified_peer()].
peers_from_record(Record, Strategy, Via) ->
    Payload = macula_record:payload(Record),
    Seeds   = maps:get({text, <<"seeds">>}, Payload, []),
    [seed_to_peer(Record, Seed, Strategy, Via) || Seed <- Seeds].

seed_to_peer(Record, Seed, Strategy, Via) ->
    #{
        node_id      => maps:get({text, <<"node_id">>},   Seed),
        record       => Record,
        addresses    => maps:get({text, <<"addresses">>}, Seed, []),
        strategy     => Strategy,
        via          => Via,
        %% Wire field `tier' on the foundation seed = station-class
        %% tier (3|4); kept under `gateway_tier' to avoid name clash
        %% with the discovery `strategy' field.
        gateway_tier => maps:get({text, <<"tier">>}, Seed, undefined)
    }.
