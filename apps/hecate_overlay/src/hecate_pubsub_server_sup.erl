%% @doc Dynamic supervisor for `hecate_pubsub_server' children.
%%
%% Holds one server per realm namespace. Started before the registry
%% under `hecate_overlay_sup' so the registry can ask for new
%% children via `start_server/1' (a thin wrapper over
%% `supervisor:start_child/2').
%%
%% Children are `temporary': a crashed pubsub_server is not
%% auto-restarted. The registry's monitor catches the death, removes
%% the entry from its map, and a subsequent `register/2' yields a
%% fresh server.
-module(hecate_pubsub_server_sup).
-behaviour(supervisor).

-export([start_link/0, start_server/1]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_server(hecate_pubsub_server:opts()) ->
        {ok, pid()} | {error, term()}.
start_server(Opts) ->
    supervisor:start_child(?MODULE, [Opts]).

init([]) ->
    SupFlags = #{strategy  => simple_one_for_one,
                 intensity => 0,
                 period    => 1},
    ChildSpec = #{id       => hecate_pubsub_server,
                  start    => {hecate_pubsub_server, start_link, []},
                  restart  => temporary,
                  shutdown => 5000,
                  type     => worker,
                  modules  => [hecate_pubsub_server]},
    {ok, {SupFlags, [ChildSpec]}}.
