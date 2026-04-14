%% @doc Application callback. Skeleton only — Phase 0.
-module(hecate_station_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    hecate_station_sup:start_link().

stop(_State) ->
    ok.
