%% @doc `inets:httpc'-backed `macula_bootstrap_http' implementation.
%%
%% Thin wrapper: build the `httpc:request/4' tuple, issue the call,
%% translate the result into the behaviour's `{ok, Body} | {error, _}'.
%% No JSON or chain-specific concerns — those live in the calling
%% adapters.
-module(macula_bootstrap_http_httpc).
-behaviour(macula_bootstrap_http).

-export([get/2, post_json/3]).

-define(CT_JSON, "application/json").

-spec get(macula_bootstrap_http:url(), pos_integer()) ->
          macula_bootstrap_http:get_result().
get(Url, TimeoutMs) ->
    Request  = {as_list(Url), [{"accept", ?CT_JSON}]},
    HttpOpts = [{timeout, TimeoutMs}, {connect_timeout, TimeoutMs}],
    translate(httpc:request(get, Request, HttpOpts,
                            [{body_format, binary}])).

-spec post_json(macula_bootstrap_http:url(), binary(), pos_integer()) ->
          macula_bootstrap_http:post_result().
post_json(Url, Body, TimeoutMs) ->
    Request  = {as_list(Url), [{"accept", ?CT_JSON}], ?CT_JSON, Body},
    HttpOpts = [{timeout, TimeoutMs}, {connect_timeout, TimeoutMs}],
    translate(httpc:request(post, Request, HttpOpts,
                            [{body_format, binary}])).

%%------------------------------------------------------------------

translate({ok, {{_Ver, 200, _Ph}, _Hs, Body}}) ->
    {ok, Body};
translate({ok, {{_Ver, Code, Ph}, _Hs, _Body}}) ->
    {error, {http_status, Code, iolist_to_binary(Ph)}};
translate({error, _} = E) ->
    E.

as_list(B) when is_binary(B) -> binary_to_list(B);
as_list(L) when is_list(L)   -> L.
