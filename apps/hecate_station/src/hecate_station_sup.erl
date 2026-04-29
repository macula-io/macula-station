%% @doc Top-level station supervisor.
%%
%% From Session 8.2 onwards the supervisor owns long-lived runtime
%% processes (DHT, SWIM, listener, …), each registered under a fixed
%% local atom so the station API can look them up:
%%
%% <ul>
%%   <li>`hecate_dht' — S/Kademlia routing table server.</li>
%%   <li>`hecate_swim' — SWIM failure detector.</li>
%% </ul>
%%
%% These children are NOT listed in `init/1'. They are added at boot
%% time by `hecate_station_app:start/2', which enforces strict
%% bootstrap ordering (DHT → cascade ingest → SWIM) per
%% PLAN_STATION_INTEGRATION §8.2. Starting them via
%% `supervisor:start_child/2' from the application callback is what
%% lets us refuse to bring SWIM up when the cascade yields zero peers.
%%
%% From the multi-identity refactor (PLAN_MULTI_IDENTITY_RELAY §Phase 1)
%% onwards the supervisor also owns one always-on infrastructure
%% child:
%%
%% <ul>
%%   <li>`hecate_station_identity_registry' — yellow pages
%%       `{IdentityKey =&gt; identity_sup_pid}'.</li>
%% </ul>
%%
%% The registry is config-independent (running it under the disabled
%% station env costs an idle gen_server with an empty map) so it
%% lives in `init/1' rather than the boot pipeline. Subsequent phases
%% will reparent the per-identity workers under
%% `hecate_station_identity_sup' children — which the registry tracks.
%%
%% The walking-skeleton / chaos CT suites drive `hecate_station_server'
%% directly via `hecate_station:start_link/1' and never touch this
%% supervisor; they run in parallel in a single VM without colliding
%% with the registered names because the sup is not started.
-module(hecate_station_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

%% Start wrappers — referenced from child specs built in
%% `hecate_station_app'. They register the child pid under a fixed
%% local name so the station API can find it (`hecate_station:dht/0',
%% `hecate_station:swim/0', `hecate_station:observer/0',
%% `hecate_station:listener/0').
-export([start_dht/1, start_swim/1, start_observer/1, start_listener/1,
         start_cache/1, start_rebootstrap/1]).

-define(DHT_NAME,         hecate_dht).
-define(SWIM_NAME,        hecate_swim).
-define(OBSERVER_NAME,    hecate_station_peer_observer).
-define(LISTENER_NAME,    hecate_station_listener).
-define(CACHE_NAME,       hecate_station_cache).
-define(REBOOTSTRAP_NAME, hecate_station_rebootstrap).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    {ok, {SupFlags, [identity_registry_child(),
                     record_fanout_child()]}}.

identity_registry_child() ->
    #{
        id       => hecate_station_identity_registry,
        start    => {hecate_station_identity_registry, start_link, []},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [hecate_station_identity_registry]
    }.

%% Node-singleton fan-out for DHT record-stored events. Replaces the
%% N per-identity `hecate_station_fact_publisher' processes that
%% previously held N independent heaps; one bounded heap here.
%% Started AFTER the registry so its on-init bootstrap path
%% (`identity_registry:phase3_snapshot/0') has somewhere to read
%% from. See `PLAN_RECORD_FANOUT_REFACTOR.md'.
record_fanout_child() ->
    #{
        id       => hecate_station_record_fanout,
        start    => {hecate_station_record_fanout, start_link, []},
        restart  => permanent,
        shutdown => 5_000,
        type     => worker,
        modules  => [hecate_station_record_fanout]
    }.

%%==================================================================
%% Name-registering start wrappers.
%%==================================================================

-spec start_dht(hecate_dht:opts()) -> {ok, pid()} | {error, term()}.
start_dht(Opts) ->
    register_result(hecate_dht:start_link(Opts), ?DHT_NAME).

-spec start_swim(hecate_swim:opts()) -> {ok, pid()} | {error, term()}.
start_swim(Opts) ->
    register_result(hecate_swim:start_link(Opts), ?SWIM_NAME).

-spec start_observer(hecate_station_peer_observer:opts()) ->
    {ok, pid()} | {error, term()}.
start_observer(Opts) ->
    register_result(hecate_station_peer_observer:start_link(Opts),
                    ?OBSERVER_NAME).

-spec start_listener(hecate_station_listener:opts()) ->
    {ok, pid()} | {error, term()}.
start_listener(Opts) ->
    register_result(hecate_station_listener:start_link(Opts),
                    ?LISTENER_NAME).

-spec start_cache(hecate_station_cache:opts()) ->
    {ok, pid()} | {error, term()}.
start_cache(Opts) ->
    register_result(hecate_station_cache:start_link(Opts), ?CACHE_NAME).

-spec start_rebootstrap(hecate_station_rebootstrap:opts()) ->
    {ok, pid()} | {error, term()}.
start_rebootstrap(Opts) ->
    register_result(hecate_station_rebootstrap:start_link(Opts),
                    ?REBOOTSTRAP_NAME).

register_result({ok, Pid} = Ok, Name) ->
    ensure_registered(Name, Pid),
    Ok;
register_result({error, _} = E, _Name) ->
    E.

%% On restart the old Pid is dead and its registration is already
%% gone, but be defensive so a zombie name does not block the new
%% registration.
ensure_registered(Name, Pid) ->
    _ = catch unregister(Name),
    true = register(Name, Pid),
    ok.
