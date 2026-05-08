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
-module(macula_bootstrap_via_blockchain).
-behaviour(macula_bootstrap_peer_discoverer).

-export([strategy/0, stagger_ms/0, discover/1]).

-export_type([chain_spec/0, discover_opts/0]).

-type chain_spec() :: {module(),
                       macula_bootstrap_via_blockchain_transport:chain_opts()}.

-type discover_opts() :: #{
    chains     => [chain_spec()],
    timeout_ms => pos_integer()
}.

-define(DEFAULT_TIMEOUT, 20_000).

strategy()   -> via_blockchain.
stagger_ms() -> 2_000.

-spec discover(discover_opts()) -> macula_bootstrap_peer_discoverer:discover_result().
discover(Opts) ->
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
        fun(Bytes)  -> macula_bootstrap_foundation:decode_record_bytes(Bytes) end,
        fun(Record) -> macula_foundation:verify_record(Record) end,
        fun(Record) ->
                {ok, macula_bootstrap_foundation:peers_from_record(
                       Record, via_blockchain, ?MODULE)}
        end
    ]).

%%------------------------------------------------------------------
%% Result chain
%%------------------------------------------------------------------

chain([Step | Rest]) -> chain(Step(), Rest).

chain({error, _} = E, _Rest)         -> E;
chain({ok, V},        [])            -> {ok, V};
chain({ok, V},        [Step | Rest]) -> chain(Step(V), Rest).
