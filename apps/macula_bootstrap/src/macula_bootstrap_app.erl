%% @doc Application callback for `macula_bootstrap'.
%%
%% Activates `macula_bootstrap_sup' when
%% `application:ensure_all_started(macula_bootstrap)' fires — which
%% happens transitively from the station's own dependency list
%% (`macula_station' declares `macula_bootstrap' in its `applications').
%%
%% The sup may start with zero children (when the operator hasn't
%% opted in to running the mDNS responder), which is the default
%% library-mode behaviour.
-module(macula_bootstrap_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    macula_bootstrap_sup:start_link().

stop(_State) ->
    ok.
