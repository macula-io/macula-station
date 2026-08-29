%% @doc One-shot bootstrap orchestrator.
%%
%% Composes `macula_bootstrap:run/0,1' (the peer-discovery cascade) with
%% `macula_station_bootstrap:ingest/2' (the bridge that seeds the
%% DHT routing table), then performs the self-lookup that turns those
%% seeds into a populated table. Called by `macula_station_app:start/2'
%% after the DHT child is up, and before SWIM is started.
%%
%% The self-lookup is not optional decoration — it is the half of a
%% Kademlia bootstrap that discovers everyone the seeds know. Without it
%% a node's table only ever holds its configured seeds, which is
%% invisible on a mutually-dialled station (inbound traffic fills the
%% table by another route) and fatal on a degree-1 leaf, which has no
%% inbound at all.
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
    ok = self_lookup(Dht, Summary),
    {ok, #{peers => Peers, summary => Summary}}.

%% The second half of a Kademlia bootstrap: having inserted the seeds,
%% ASK THEM WHO ELSE EXISTS.
%%
%% Ingest alone only ever puts the configured seeds in the table. On a
%% mutually-dialled station that is enough, because inbound handshakes
%% and inbound FIND_NODEs then observe their senders and the table fills
%% by itself. A degree-1 LEAF gets no inbound at all, so without this
%% walk it stays pinned at exactly its seed count forever -- observed on
%% station-se-stockholm, which sat at 1 entry indefinitely while its
%% upstream knew all 45 members.
%%
%% Skipped when ingest seeded nothing: with an empty table there is
%% nobody to ask, and `min_peers => 0' means that is a legitimate
%% first-boot state rather than an error.
%%
%% FIRE AND FORGET, for three reasons rather than laziness.
%%
%% Nothing needs the return value: `macula_dht_lookup' now observes what
%% it learns as a side effect, so the routing table fills whether or not
%% anyone reads the result.
%%
%% `dial => fun macula_station_dht_dialer:ensure_dialed/3' (2026-08-29):
%% without it, a peer learned mid-walk (round >= 1, only ever named in a
%% NODES reply, never a round-0 seed we already hold a connection to)
%% could not actually be reached — `macula_dht_lookup''s own moduledoc
%% has the full history. This is what makes THIS self-lookup genuinely
%% multi-hop instead of discovering-but-never-reaching peers beyond the
%% station's initial seed set.
%%
%% Boot must not wait on the network. This runs in
%% `macula_station_app:start/2' before SWIM comes up, and a walk costs
%% up to `overall_timeout_ms' (15s default). A station that cannot
%% complete a walk still has to come up and serve its links.
%%
%% And a synchronous call here made eunit CANCEL two tests: with a stub
%% DHT the walk has nobody to answer it and simply runs out its
%% deadline, which a `catch' cannot rescue because a hang is not an
%% exception.
-spec self_lookup(macula_dht:dht(),
                  macula_station_bootstrap:ingest_summary()) -> ok.
self_lookup(_Dht, #{observed := 0}) ->
    ok;
self_lookup(Dht, _Summary) ->
    _ = spawn(fun() ->
        catch macula_dht:lookup_nodes(
                Dht, macula_dht:self_id(Dht),
                #{dial => fun macula_station_dht_dialer:ensure_dialed/3})
    end),
    ok.
