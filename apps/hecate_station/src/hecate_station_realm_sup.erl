%% @doc simple_one_for_one sup holding one `hecate_station_realm'
%% gen_server per realm the station serves.
%%
%% Registered locally under `?MODULE' so the application callback
%% and the station facade can address it without threading a pid
%% through every call site. Children are `transient': a clean exit
%% (`stop/1') removes the realm, a crash restarts it so the realm
%% stays alive across protocol bugs. Crashing one realm does not
%% affect the others — the plan §8.4 acceptance clause.
-module(hecate_station_realm_sup).
-behaviour(supervisor).

-export([start_link/0, start_realm/1, stop_realm/1, children/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Start a realm child under this sup.
-spec start_realm(hecate_station_realm:opts()) ->
    {ok, pid()} | {error, term()}.
start_realm(Opts) ->
    supervisor:start_child(?MODULE, [Opts]).

%% @doc Terminate a realm child. Used on config reload + teardown.
-spec stop_realm(pid()) -> ok | {error, term()}.
stop_realm(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).

%% @doc Enumerate live realm children (pid list).
-spec children() -> [pid()].
children() ->
    [Pid || {_, Pid, worker, _} <- supervisor:which_children(?MODULE),
            is_pid(Pid)].

init([]) ->
    SupFlags = #{strategy  => simple_one_for_one,
                 intensity => 5,
                 period    => 10},
    ChildSpec = #{
        id       => hecate_station_realm,
        start    => {hecate_station_realm, start_link, []},
        restart  => transient,
        shutdown => 5000,
        type     => worker,
        modules  => [hecate_station_realm]
    },
    {ok, {SupFlags, [ChildSpec]}}.
