%% @doc Top-level supervisor for long-running bootstrap services.
%%
%% The cascade itself is a <em>one-shot</em> — `hecate_bootstrap:run/0'
%% spawns probes, collects peers, and returns. Nothing long-running
%% on that path. But the Tier B <em>advertise</em> side
%% (`hecate_bootstrap_mdns_responder') is a steady-state service
%% owning a UDP socket, and a station-wide bootstrap subsystem needs
%% a home for it (plus any future long-running children — periodic
%% re-bootstrap timer, cached-peer maintainer, …).
%%
%% == Conditional children ==
%%
%% The responder is opt-in via `application:get_env(hecate_bootstrap,
%% responder, disabled)':
%% <ul>
%%   <li>`disabled' (default) — no children.</li>
%%   <li>`#{node_id := _, port := _, tier := _}' — responder child
%%       spawned with those opts (plus any extra keys, e.g. a test
%%       `socket_opener' or `silent' flag).</li>
%% </ul>
%%
%% Default-disabled matters: the real responder binds UDP port 5353
%% which conflicts with `avahi-daemon' on typical Linux hosts. The
%% station operator opts in with a working config (or sets
%% `socket_opener' in tests).
-module(hecate_bootstrap_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

-define(RESPONDER_ENV_KEY, responder).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    {ok, {SupFlags, responder_children()}}.

%%------------------------------------------------------------------
%% Child specs
%%------------------------------------------------------------------

responder_children() ->
    responder_spec(application:get_env(hecate_bootstrap,
                                       ?RESPONDER_ENV_KEY)).

responder_spec({ok, disabled})            -> [];
responder_spec({ok, Opts}) when is_map(Opts) -> [child_spec(Opts)];
responder_spec(_)                         -> [].

child_spec(Opts) ->
    #{
        id       => hecate_bootstrap_mdns_responder,
        start    => {hecate_bootstrap_mdns_responder, start_link, [Opts]},
        restart  => permanent,
        shutdown => 5000,
        type     => worker,
        modules  => [hecate_bootstrap_mdns_responder]
    }.
