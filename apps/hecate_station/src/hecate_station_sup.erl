%% @doc Top-level supervisor.
%%
%% Session 8.1 surface: one `hecate_station_server' child whose opts come
%% from `hecate_station_config:from_env/0'. When the application env is
%% empty (walking-skeleton / chaos CT modes) the sup starts with no
%% children — those suites drive `hecate_station_server' directly via
%% `hecate_station:start_link/1'.
%%
%% A malformed `sys.config' surfaces as `{stop, {bad_config, Reason}}'
%% per PLAN_STATION_INTEGRATION 8.1 acceptance.
-module(hecate_station_sup).
-behaviour(supervisor).

-include("hecate_station_cfg.hrl").

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    children(hecate_station_config:enabled(), SupFlags).

children(false, SupFlags) ->
    {ok, {SupFlags, []}};
children(true, SupFlags) ->
    build_children(hecate_station_config:from_env(), SupFlags).

build_children({ok, Cfg}, SupFlags) ->
    {ok, {SupFlags, [station_child(Cfg)]}};
build_children({error, Reason}, _SupFlags) ->
    {stop, Reason}.

station_child(#station_cfg{} = Cfg) ->
    Opts = hecate_station_config:to_opts(Cfg),
    #{
        id       => hecate_station_server,
        start    => {hecate_station_server, start_link, [Opts]},
        restart  => permanent,
        shutdown => 10_000,
        type     => worker,
        modules  => [hecate_station_server]
    }.
