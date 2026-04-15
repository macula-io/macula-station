%% @doc Tiny gen_server for `fleet_chaos_tests:pause/1 + resume/1'.
%% Exists solely so a test can `ping/1' a real OTP-shaped process
%% whose state handler can be suspended via `sys:suspend/1'.
-module(echo_server).
-behaviour(gen_server).

-export([start_link/0, stop/1, ping/1]).
-export([init/1, handle_call/3, handle_cast/2]).

start_link() -> gen_server:start_link(?MODULE, [], []).

stop(Pid) -> gen_server:stop(Pid).

ping(Pid) -> gen_server:call(Pid, ping).

init([])                        -> {ok, #{}}.
handle_call(ping, _From, S)     -> {reply, pong, S};
handle_call(_Msg, _From, S)     -> {reply, {error, unknown}, S}.
handle_cast(_Msg, S)            -> {noreply, S}.
