%% @doc hecate_overlay top-level supervisor.
%%
%% The overlay module-set is pure-library — pubsub state machines,
%% plumtree gossip, OR-sets — plus a `hecate_pubsub_registry'
%% gen_server that is spawned under the station's supervision tree
%% rather than as a singleton under this supervisor.
%%
%% The supervisor stays as an empty OTP shell so the application
%% retains a top-level pid (required by `application:start/1') and
%% can be restarted by the kernel application master if needed. No
%% children are owned here.
-module(hecate_overlay_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy  => one_for_one,
                 intensity => 0,
                 period    => 1},
    {ok, {SupFlags, []}}.
