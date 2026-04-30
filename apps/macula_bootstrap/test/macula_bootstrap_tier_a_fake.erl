%% @doc In-memory `macula_bootstrap_resolver' for tests.
%%
%% Tests stash canned replies keyed by `{Url, Pubkey}'; the orchestrator
%% retrieves them via the behaviour callback. Lets the unit tests
%% exercise corroboration, fall-through, and verification logic
%% without any DoH I/O.
-module(macula_bootstrap_tier_a_fake).
-behaviour(macula_bootstrap_resolver).

-export([init/0, reset/0, set/3, resolve/3]).

-define(TAB, ?MODULE).

init() ->
    case ets:info(?TAB) of
        undefined -> ets:new(?TAB, [named_table, public, set]);
        _         -> reset()
    end,
    ok.

reset() ->
    ets:delete_all_objects(?TAB),
    ok.

set(Url, Pubkey, Reply) ->
    ets:insert(?TAB, {{Url, Pubkey}, Reply}),
    ok.

resolve(Url, Pubkey, _Opts) ->
    deliver(lookup(ets:lookup(?TAB, {Url, Pubkey}))).

lookup([{_, Reply}]) -> Reply;
lookup([])           -> {error, not_set}.

deliver({sleep, Ms, Reply}) ->
    timer:sleep(Ms),
    Reply;
deliver(Other) ->
    Other.
