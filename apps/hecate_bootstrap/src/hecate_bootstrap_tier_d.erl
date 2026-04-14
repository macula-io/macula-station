%% @doc Tier D — blockchain anchor (Part 5 §7).
%%
%% Last automated tier before operator paste. Reads the foundation's
%% quarterly-refreshed anchor from one or more public blockchains
%% (Bitcoin OP_RETURN, Ethereum contract event); accepts the first
%% chain to return a foundation-signed `foundation_seed_list'.
%%
%% == Trust model ==
%%
%% The chain is a <em>locator</em>, not a trust anchor. Even a
%% 51%-attacked chain cannot forge the Ed25519 signature of the
%% foundation's threshold key. A successful attack on the chain at
%% worst denies service and forces fall-through to Tier E; it cannot
%% inject hostile peers.
%%
%% == Probe options ==
%%
%% <ul>
%%   <li>`chains' :: `[{module(), chain_opts()}]' — ordered list of
%%       chain adapters to query in parallel. No default: operators
%%       must configure at least one.</li>
%%   <li>`timeout_ms' :: pos_integer() — per-chain deadline
%%       (default 20_000; chains are slow).</li>
%% </ul>
%%
%% == Stagger ==
%%
%% 2000 ms per Part 5 §3 — Tier D is the slowest automated tier.
%% It enters the cascade last so faster tiers (A/B/C) aren't
%% starved by chain-query latency.
-module(hecate_bootstrap_tier_d).
-behaviour(hecate_bootstrap_tier).

-export([tier/0, stagger_ms/0, probe/1]).

-export_type([chain_spec/0, probe_opts/0]).

-type chain_spec() :: {module(),
                       hecate_bootstrap_chain_transport:chain_opts()}.

-type probe_opts() :: #{
    chains     => [chain_spec()],
    timeout_ms => pos_integer()
}.

-define(DEFAULT_TIMEOUT, 20_000).

tier()       -> d.
stagger_ms() -> 2_000.

-spec probe(probe_opts()) -> hecate_bootstrap_tier:probe_result().
probe(Opts) ->
    Chains  = maps:get(chains,     Opts, []),
    Timeout = maps:get(timeout_ms, Opts, ?DEFAULT_TIMEOUT),
    run(Chains, Timeout).

run([], _Timeout) -> {error, no_chains};
run(Chains, Timeout) ->
    Tag    = make_ref(),
    Parent = self(),
    lists:foreach(
      fun(C) -> spawn_worker(Parent, Tag, C, Timeout) end,
      Chains),
    Deadline = erlang:monotonic_time(millisecond) + Timeout + 500,
    collect(length(Chains), Tag, Deadline).

%%------------------------------------------------------------------
%% Fan-out / collect
%%------------------------------------------------------------------

spawn_worker(Parent, Tag, {Mod, ChainOpts}, Timeout) ->
    spawn(fun() ->
                  Parent ! {Tag, fetch_and_verify(Mod, ChainOpts, Timeout)}
          end).

collect(0, _Tag, _Deadline) ->
    {error, all_failed};
collect(N, Tag, Deadline) ->
    Remaining = max(Deadline - erlang:monotonic_time(millisecond), 0),
    receive
        {Tag, {ok, Peers}} when Peers =/= [] ->
            {ok, Peers};
        {Tag, _OkEmptyOrError} ->
            collect(N - 1, Tag, Deadline)
    after Remaining ->
        {error, timeout}
    end.

%%------------------------------------------------------------------
%% Per-chain pipeline
%%------------------------------------------------------------------

fetch_and_verify(Mod, ChainOpts, Timeout) ->
    chain([
        fun() -> Mod:latest_anchor(ChainOpts, Timeout) end,
        fun(Bytes)  -> decode_record(Bytes) end,
        fun(Record) -> macula_foundation:verify_record(Record) end,
        fun(Record) -> {ok, peers(Record)} end
    ]).

%% External-source bytes may be arbitrary garbage; macula_record
%% crashes on malformed CBOR rather than returning `{error, _}', so
%% we catch at the trust boundary here.
decode_record(Bytes) ->
    try macula_record:decode(Bytes) of
        {ok, _} = Ok   -> Ok;
        {error, _} = E -> E
    catch
        _:_ -> {error, bad_record_bytes}
    end.

peers(Record) ->
    Payload = macula_record:payload(Record),
    Seeds   = maps:get({text, <<"seeds">>}, Payload, []),
    [seed_to_peer(Record, S) || S <- Seeds].

seed_to_peer(Record, Seed) ->
    NodeId = maps:get({text, <<"node_id">>}, Seed),
    Addrs  = maps:get({text, <<"addresses">>}, Seed, []),
    #{
        node_id   => NodeId,
        record    => Record,
        addresses => Addrs,
        tier      => d,
        via       => ?MODULE
    }.

%%------------------------------------------------------------------
%% Result chain
%%------------------------------------------------------------------

chain([Step | Rest]) -> chain(Step(), Rest).

chain({error, _} = E, _Rest)         -> E;
chain({ok, V},        [])            -> {ok, V};
chain({ok, V},        [Step | Rest]) -> chain(Step(V), Rest).
