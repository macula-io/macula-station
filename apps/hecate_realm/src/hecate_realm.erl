%% @doc Placeholder module for the `hecate_realm' application.
%%
%% Declared as a runtime dependency of `macula_station' (see its
%% `.app.src') under the description "Hecate Station — realm directory
%% cache", but not yet implemented: no modules, no supervision tree.
%% `hecate_realm.app.src' carries no `mod' entry, so `application:
%% ensure_all_started/1' treats it as a library application and starts
%% it trivially — there is nothing running here yet.
%%
%% This module exists only so the app is not entirely empty; an empty
%% app (zero modules) breaks the `rebar3 ex_doc' umbrella-wide doc
%% generation, which expects every project app to produce at least one
%% doc chunk. Delete this moduledoc once the app has real content.
-module(hecate_realm).
