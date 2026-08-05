%% @doc Tiny stub matching the one call `macula_station_peer_observer' makes
%% into the listener at init: `peers/1'.
%%
%% Registered under the real listener's name, because
%% `macula_station:listener()' resolves by name and that is the path under
%% test. Holds whatever inbound index the test wants it to hold.
-module(stub_listener).
-behaviour(gen_server).

-export([start_link/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-spec start_link([{binary(), pid()}]) -> {ok, pid()}.
start_link(Peers) ->
    gen_server:start_link({local, macula_station_listener}, ?MODULE, Peers, []).

stop(Pid) -> gen_server:stop(Pid).

init(Peers) -> {ok, Peers}.

handle_call(peers, _From, Peers) -> {reply, Peers, Peers};
handle_call(_Other, _From, S) -> {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) -> {noreply, S}.
handle_info(_Msg, S) -> {noreply, S}.
