%% @doc Supervisor for a `macula_dht' instance.
%%
%% Session 3.3 supervises a single `macula_dht_server' worker. Later
%% sessions will add children for the ETS record store (3.7), the
%% tReplicate / tRepublish / tExpire custodian timers (3.9 / 3.10),
%% and the observability collector (3.11).
%%
%% == Typical use ==
%%
%% ```
%% {ok, Sup}    = macula_dht_sup:start_link(#{self_id => Self}),
%% {ok, Server} = macula_dht_sup:get_server(Sup),
%% admitted     = macula_dht:observe(Server, Spec).
%% '''
-module(macula_dht_sup).
-behaviour(supervisor).

-export([start_link/1, get_server/1]).
-export([init/1]).

-spec start_link(macula_dht:opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    supervisor:start_link(?MODULE, Opts).

-spec get_server(pid()) -> {ok, pid()} | {error, not_found}.
get_server(SupPid) ->
    select_server(supervisor:which_children(SupPid)).

%%=====================================================================
%% Supervisor callback
%%=====================================================================

init(Opts) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    ChildSpec = #{
        id       => macula_dht_server,
        start    => {macula_dht_server, start_link, [Opts]},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_dht_server]
    },
    {ok, {SupFlags, [ChildSpec]}}.

%%=====================================================================
%% Internal
%%=====================================================================

-spec select_server([tuple()]) -> {ok, pid()} | {error, not_found}.
select_server([])                                                -> {error, not_found};
select_server([{macula_dht_server, Pid, _, _} | _]) when is_pid(Pid) -> {ok, Pid};
select_server([_ | Rest])                                         -> select_server(Rest).
