%% Tiny stub matching the `macula_station_peer_observer:station_view/1'
%% surface, for tests (e.g. `macula_station_peering_redundancy_tests')
%% that need a fixed "how many stations am I connected to" answer
%% without standing up a real observer + real QUIC connections.
-module(stub_station_view_observer).
-behaviour(gen_server).

-export([start_link/1, stop/1, station_view/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-spec start_link(#{stations := non_neg_integer(),
                    station_ids := [macula_identity:pubkey()]}) ->
    {ok, pid()} | {error, term()}.
start_link(View) ->
    gen_server:start_link(?MODULE, View, []).

stop(Pid) ->
    gen_server:stop(Pid).

station_view(Pid) ->
    gen_server:call(Pid, station_view).

init(#{stations := _, station_ids := _} = View) ->
    {ok, View}.

handle_call(station_view, _From, View) ->
    Daemons = maps:get(daemons, View, 0),
    {reply, View#{daemons => Daemons}, View};
handle_call(_, _, S) ->
    {reply, {error, unknown}, S}.

handle_cast(_, S) -> {noreply, S}.
handle_info(_, S) -> {noreply, S}.
terminate(_, _)   -> ok.
code_change(_, S, _) -> {ok, S}.
