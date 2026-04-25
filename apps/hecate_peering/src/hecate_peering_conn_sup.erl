%% @doc simple_one_for_one supervisor for `hecate_peering_conn' workers.
-module(hecate_peering_conn_sup).
-behaviour(supervisor).

-export([start_link/0, start_conn/1, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Spawn a new conn gen_statem with the given options.
-spec start_conn(hecate_peering_conn:opts()) ->
    {ok, pid()} | {error, term()}.
start_conn(Opts) ->
    supervisor:start_child(?MODULE, [Opts]).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 10, period => 10},
    Children = [
        #{
            id       => hecate_peering_conn,
            start    => {hecate_peering_conn, start_link, []},
            restart  => temporary,
            shutdown => 5_000,
            type     => worker,
            modules  => [hecate_peering_conn]
        }
    ],
    {ok, {SupFlags, Children}}.
