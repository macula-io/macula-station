%% @doc In-memory `hecate_bootstrap_dht_transport' for tests.
%%
%% Canned results keyed by target id. Tests install an item via
%% `set/2'; tier_c looks it up via the behaviour callback.
-module(hecate_bootstrap_dht_fake).
-behaviour(hecate_bootstrap_dht_transport).

-export([init/0, reset/0, set/2, fail/2, get_mutable/2]).

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

set(Target, Item) when is_binary(Target), byte_size(Target) =:= 20 ->
    ets:insert(?TAB, {Target, {ok, Item}}),
    ok.

fail(Target, Reason) when is_binary(Target), byte_size(Target) =:= 20 ->
    ets:insert(?TAB, {Target, {error, Reason}}),
    ok.

get_mutable(Target, _TimeoutMs) ->
    case ets:lookup(?TAB, Target) of
        [{_, Reply}] -> Reply;
        []           -> {error, not_found}
    end.
