%% @doc hecate_overlay top-level supervisor.
%%
%% Phase 1 (this commit): empty children list. The sup exists so the
%% application has a root to start; pubsub_server instances are
%% created directly by callers in tests. Subsequent commits will add
%% a per-realm registry that supervises pubsub_server children
%% dynamically as topics get their first subscriber on each realm
%% namespace.
-module(hecate_overlay_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    {ok, {SupFlags, []}}.
