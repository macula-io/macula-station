%% @doc macula_content application callback.
-module(macula_content_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    macula_content_sup:start_link().

stop(_State) ->
    ok.
