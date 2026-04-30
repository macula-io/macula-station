%% @doc In-memory `macula_bootstrap_mdns_transport' for tests.
%%
%% Tests install a canned reply list; the tier_b orchestrator then
%% sees exactly those `{SrcAddress, PacketBin}' pairs instead of
%% going to the kernel for multicast UDP.
-module(macula_bootstrap_mdns_fake).
-behaviour(macula_bootstrap_mdns_transport).

-export([init/0, reset/0, set_replies/1, query/2]).

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

set_replies(Replies) when is_list(Replies) ->
    ets:insert(?TAB, {replies, Replies}),
    ok.

query(_QueryBin, _Timeout) ->
    lookup(ets:lookup(?TAB, replies)).

lookup([{_, R}]) -> R;
lookup([])       -> [].
