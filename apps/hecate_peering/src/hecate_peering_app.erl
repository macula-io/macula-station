%% @doc Application callback for hecate_peering.
-module(hecate_peering_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    hecate_peering_sup:start_link().

stop(_State) ->
    ok.
