%% @doc hecate_overlay top-level supervisor.
%%
%% Phase 1: empty children list (Sprint A activation only).
%% Phase 2 (this commit): supervises the per-realm pubsub fabric:
%%
%% <ol>
%%   <li>`hecate_pubsub_server_sup' — `simple_one_for_one' pool of
%%       `hecate_pubsub_server' children, one per realm tag.</li>
%%   <li>`hecate_pubsub_registry' — gen_server holding the
%%       `RealmTag => pid()' map and the dispatch hub for inbound
%%       SUBSCRIBE / UNSUBSCRIBE / EVENT frames.</li>
%% </ol>
%%
%% Strategy is `one_for_all': either child going down invalidates the
%% other (the registry's map points at children of the dynamic
%% supervisor; if that supervisor restarts, the entries become stale).
%% Co-restart guarantees a fresh, consistent state.
-module(hecate_overlay_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy  => one_for_all,
                 intensity => 5,
                 period    => 10},
    Children = [
        #{id       => hecate_pubsub_server_sup,
          start    => {hecate_pubsub_server_sup, start_link, []},
          restart  => permanent,
          shutdown => infinity,
          type     => supervisor,
          modules  => [hecate_pubsub_server_sup]},
        #{id       => hecate_pubsub_registry,
          start    => {hecate_pubsub_registry, start_link, []},
          restart  => permanent,
          shutdown => 5000,
          type     => worker,
          modules  => [hecate_pubsub_registry]}
    ],
    {ok, {SupFlags, Children}}.
