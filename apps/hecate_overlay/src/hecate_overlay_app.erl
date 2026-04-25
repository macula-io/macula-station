%% @doc hecate_overlay application callback.
%%
%% Activates the previously dormant gossip + pubsub modules so they
%% can be wired into the station as substrate primitives. Realm
%% identity / membership / authority remain the responsibility of the
%% separate realm service per Sprint A — what's activated here is the
%% generic *capability* (PubSub state machine + Plumtree gossip),
%% keyed by an opaque 32-byte realm tag treated as a multi-tenant
%% namespace, not as an authority claim.
%%
%% Phase 1 (this commit): the application boots an empty supervisor.
%% Per-realm pubsub_server instances are not yet auto-started — they
%% can be spawned directly by tests and will be managed by a registry
%% (next commit) once the wire-frame integration into hecate_station's
%% listener lands.
-module(hecate_overlay_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    hecate_overlay_sup:start_link().

stop(_State) ->
    ok.
