%% @doc One-shot bootstrap orchestrator.
%%
%% Composes `macula_bootstrap:run/0,1' (the peer-discovery cascade) with
%% `macula_station_bootstrap:ingest/2' (the bridge that seeds the
%% DHT routing table). Called by `macula_station_app:start/2' after
%% the DHT child is up, and before SWIM is started.
%%
%% == Behaviour ==
%%
%% <ul>
%%   <li>Cascade succeeds: returns `{ok, #{peers := Peers, summary :=
%%       IngestSummary}}'.</li>
%%   <li>Cascade returns `{error, no_tiers}': returned verbatim so the
%%       caller can refuse to bring SWIM up on an unseeded DHT (per
%%       PLAN_STATION_INTEGRATION §8.2 acceptance).</li>
%%   <li>Cascade returns any other error: wrapped as
%%       `{error, {bootstrap_failed, Reason}}'.</li>
%% </ul>
%%
%% Not a gen_server: the orchestration is linear and the caller
%% (`macula_station_app') is where boot sequencing lives. Keeping this
%% module data-only makes it cheap to unit test.
-module(macula_station_bootstrap_runner).

-export([run/1, run/2]).

-export_type([result/0, cfg/0]).

-type cfg() :: macula_bootstrap:station_config() | from_app_env.

-type result() :: {ok, #{peers   := [macula_bootstrap_peer_discoverer:verified_peer()],
                         summary := macula_station_bootstrap:ingest_summary()}}
                | {error, no_tiers}
                | {error, {bootstrap_failed, term()}}.

%% @doc Run the cascade with application-env config, then ingest.
-spec run(macula_dht:dht()) -> result().
run(Dht) ->
    run(Dht, from_app_env).

%% @doc Run the cascade with an explicit config, then ingest.
%% `from_app_env' reads tiers + cascade_opts from `macula_bootstrap'
%% application env.
-spec run(macula_dht:dht(), cfg()) -> result().
run(Dht, Cfg) ->
    classify_cascade(cascade(Cfg), Dht).

cascade(from_app_env) -> macula_bootstrap:run();
cascade(Cfg)          -> macula_bootstrap:run(Cfg).

classify_cascade({ok, Peers},              Dht) -> ingest(Dht, Peers);
classify_cascade({error, no_tiers},       _Dht) -> {error, no_tiers};
classify_cascade({error, Reason},         _Dht) -> {error, {bootstrap_failed, Reason}}.

ingest(Dht, Peers) ->
    Summary = macula_station_bootstrap:ingest(Dht, Peers),
    {ok, #{peers => Peers, summary => Summary}}.
