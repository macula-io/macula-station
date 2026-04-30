%% @doc In-memory `macula_bootstrap_http' fake for adapter tests.
%%
%% Canned responses keyed by URL for both `get/2' and `post_json/3'
%% (post responses keyed by URL alone — tests pinning one response
%% per endpoint is sufficient for the adapter suites we ship).
%%
%% Responses can be success (`{ok, Body}') or failure
%% (`{error, Reason}').
-module(macula_bootstrap_http_fake).
-behaviour(macula_bootstrap_http).

-export([init/0, reset/0, set_get/2, set_post/2,
         get/2, post_json/3]).

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

set_get(Url, Reply) ->
    ets:insert(?TAB, {{get, Url}, Reply}),
    ok.

set_post(Url, Reply) ->
    ets:insert(?TAB, {{post, Url}, Reply}),
    ok.

get(Url, _TimeoutMs) ->
    lookup({get, norm(Url)}).

post_json(Url, _Body, _TimeoutMs) ->
    lookup({post, norm(Url)}).

lookup(Key) ->
    case ets:lookup(?TAB, Key) of
        [{_, Reply}] -> Reply;
        []           -> {error, {not_canned, Key}}
    end.

norm(Url) when is_binary(Url) -> Url;
norm(Url) when is_list(Url)   -> iolist_to_binary(Url).
