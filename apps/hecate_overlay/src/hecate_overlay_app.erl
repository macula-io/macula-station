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
%% Phase 2 (current): the application boots `hecate_pubsub_server_sup'
%% (a `simple_one_for_one' pool) and `hecate_pubsub_registry' (a
%% gen_server holding the `RealmTag => pid()' map). Listener
%% integration for inbound frame dispatch lands in the next commit.
-module(hecate_overlay_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    hecate_overlay_sup:start_link().

stop(_State) ->
    ok.
