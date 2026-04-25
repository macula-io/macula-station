%% @doc hecate_content application callback.
-module(hecate_content_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    hecate_content_sup:start_link().

stop(_State) ->
    ok.
