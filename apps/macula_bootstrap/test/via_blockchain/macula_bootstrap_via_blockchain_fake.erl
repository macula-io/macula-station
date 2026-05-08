%% @doc In-memory `macula_bootstrap_via_blockchain_transport' for tests.
%%
%% Canned anchor bytes keyed by a chain label (supplied via
%% `chain_opts' under the `label' key). Tests install an anchor or a
%% failure via `set/2' / `fail/2'; tier_d looks them up through the
%% behaviour callback.
-module(macula_bootstrap_via_blockchain_fake).
-behaviour(macula_bootstrap_via_blockchain_transport).

-export([init/0, reset/0, set/2, fail/2,
         latest_anchor/2]).

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

set(Label, AnchorBytes) when is_binary(AnchorBytes) ->
    ets:insert(?TAB, {Label, {ok, AnchorBytes}}),
    ok.

fail(Label, Reason) ->
    ets:insert(?TAB, {Label, {error, Reason}}),
    ok.

latest_anchor(Opts, _TimeoutMs) ->
    Label = maps:get(label, Opts, default),
    Delay = maps:get(delay_ms, Opts, 0),
    delay(Delay),
    lookup(ets:lookup(?TAB, Label)).

delay(0) -> ok;
delay(N) -> timer:sleep(N).

lookup([{_, Reply}]) -> Reply;
lookup([])           -> {error, not_set}.
