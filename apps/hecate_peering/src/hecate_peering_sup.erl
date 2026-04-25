%% @doc Top supervisor for hecate_peering.
%%
%% Hosts the dynamic conn supervisor under which one
%% `hecate_peering_conn' gen_statem is spawned per peer connection.
-module(hecate_peering_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    Children = [
        #{
            id       => hecate_peering_conn_sup,
            start    => {hecate_peering_conn_sup, start_link, []},
            restart  => permanent,
            shutdown => 5_000,
            type     => supervisor,
            modules  => [hecate_peering_conn_sup]
        }
    ],
    {ok, {SupFlags, Children}}.
