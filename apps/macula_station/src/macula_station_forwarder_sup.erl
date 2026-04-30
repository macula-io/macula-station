%%% @doc Per-identity simple_one_for_one supervisor for peering
%%% forwarders. The peering router asks this sup to start/stop
%%% forwarder workers as the (Topic, Peer) cross-product changes.
-module(macula_station_forwarder_sup).
-behaviour(supervisor).

-export([start_link/0, start_forwarder/2, stop_forwarder/2]).
-export([init/1]).

start_link() ->
    supervisor:start_link(?MODULE, []).

%% simple_one_for_one — `start_child(Sup, [Opts])' appends `[Opts]'
%% to the child spec's base args (`[]'), so the forwarder's
%% start_link/1 sees `Opts' as its single argument.
-spec start_forwarder(pid(),
                      macula_station_peering_forwarder:opts()) ->
    {ok, pid()} | {error, term()}.
start_forwarder(Sup, Opts) ->
    supervisor:start_child(Sup, [Opts]).

-spec stop_forwarder(pid(), pid()) -> ok | {error, term()}.
stop_forwarder(Sup, Pid) ->
    supervisor:terminate_child(Sup, Pid).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 10, period => 30},
    Child = #{id       => macula_station_peering_forwarder,
              start    => {macula_station_peering_forwarder, start_link, []},
              restart  => transient,
              shutdown => 5_000,
              type     => worker,
              modules  => [macula_station_peering_forwarder]},
    {ok, {SupFlags, [Child]}}.
