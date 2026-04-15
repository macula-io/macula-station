%% @doc Application callback for `hecate_bootstrap'.
%%
%% Activates `hecate_bootstrap_sup' when
%% `application:ensure_all_started(hecate_bootstrap)' fires — which
%% happens transitively from the station's own dependency list
%% (`hecate_station' declares `hecate_bootstrap' in its `applications').
%%
%% The sup may start with zero children (when the operator hasn't
%% opted in to running the mDNS responder), which is the default
%% library-mode behaviour.
-module(hecate_bootstrap_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    hecate_bootstrap_sup:start_link().

stop(_State) ->
    ok.
