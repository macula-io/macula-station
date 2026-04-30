%% @doc Supervisor for the admin HTTP listener.
%%
%% Single child: `macula_station_admin'. A sub-sup rather than a
%% direct child under `macula_station_sup' keeps the admin API's
%% restart intensity independent from the station's core processes
%% (one flaky HTTP handler should not count against the root sup's
%% crash budget for DHT / SWIM / listener).
-module(macula_station_admin_sup).
-behaviour(supervisor).

-export([start_link/1, init/1]).

-spec start_link(macula_station_admin:opts()) -> {ok, pid()} | {error, term()}.
start_link(AdminOpts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, AdminOpts).

init(AdminOpts) ->
    SupFlags = #{strategy  => one_for_one,
                 intensity => 5,
                 period    => 60},
    Admin = #{
        id       => macula_station_admin,
        start    => {macula_station_admin, start_link, [AdminOpts]},
        restart  => permanent,
        shutdown => 5000,
        type     => worker,
        modules  => [macula_station_admin]
    },
    {ok, {SupFlags, [Admin]}}.
