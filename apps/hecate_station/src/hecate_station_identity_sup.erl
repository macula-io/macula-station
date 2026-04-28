%% @doc Per-identity supervisor.
%%
%% A single hecate-station BEAM process hosts N identities
%% concurrently (PLAN_MULTI_IDENTITY_RELAY). Each identity gets its
%% own supervisor so the per-identity workers share fate: if the
%% DHT or the listener crashes for identity A, the rest of identity
%% A is taken down and restarted in lockstep, but identities B and
%% C are unaffected.
%%
%% Children, by phase:
%%
%% <ul>
%%   <li>Phase 2 (in `init/1' as static specs):
%%       <ul>
%%         <li>`hecate_pubsub_registry' (always-on).</li>
%%         <li>`hecate_content_announcer' (opt-in, when announcer
%%             opts are supplied).</li>
%%       </ul></li>
%%   <li>Phase 3 (added procedurally by `start_link/1' AFTER the
%%       supervisor is up, when listener opts are supplied):
%%       <ul>
%%         <li>`hecate_dht' — per-identity Kademlia routing table.</li>
%%         <li>`hecate_swim' — per-identity SWIM failure detector,
%%             signing membership frames with the identity's
%%             keypair.</li>
%%         <li>`hecate_station_peer_observer' — glue between peering,
%%             DHT, and SWIM. Fed the per-identity DHT + SWIM pids
%%             as opts.</li>
%%         <li>`hecate_station_listener' — bound to the identity's
%%             IPv6 + port, sharing the box's wildcard cert. Sets
%%             the per-identity observer as `controlling_pid'.</li>
%%       </ul></li>
%% </ul>
%%
%% The Phase-3 chain is added procedurally rather than as static
%% `init/1' children because each downstream child needs the pid of
%% an upstream sibling (observer needs DHT + SWIM, listener needs
%% observer). Static specs cannot express that — `supervisor:init/1'
%% has no way to thread pids between siblings. The procedural
%% `start_child' chain in `start_link/1' is the standard OTP idiom
%% for this shape; it is functionally equivalent to the boot
%% pipeline `hecate_station_app' uses today, but executes
%% per-identity.
%%
%% Cascade ingest (seeding the DHT from bootstrap tiers between
%% DHT and SWIM start) is intentionally NOT done here. Phase 4
%% (config loader + boot rewrite) inserts it at the right place;
%% Phase 3 tests dial peers manually.
%%
%% Started by `hecate_station_identity_registry' on `register/2'.
%% Not name-registered (`{local, _}') — N instances must coexist,
%% identified solely by the registry's `identity_key()' map.
-module(hecate_station_identity_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

-export_type([identity_opts/0]).

-type identity_opts() :: #{
    identity_key       := hecate_station_identity_registry:identity_key(),
    identity           => macula_identity:key_pair(),
    bind               => inet:ip_address() | string(),
    port               => inet:port_number(),
    capabilities       => non_neg_integer(),
    realms             => [macula_identity:pubkey()],
    %% content_announcer prerequisites — when present, the
    %% announcer joins the per-identity child list.
    station_id         => macula_identity:pubkey(),
    endpoint           => binary(),
    %% Optional Phase-3 listener prerequisites — when ALL of bind /
    %% port / certfile / keyfile / identity are present, the DHT +
    %% SWIM + observer + listener chain is brought up procedurally.
    certfile           => file:name_all(),
    keyfile            => file:name_all(),
    %% Optional content-announcer DHT pid (Phase 3 will populate
    %% this with the per-identity DHT).
    dht                => hecate_dht:dht() | undefined,
    %% Optional content-announcer record TTL (defaults to 5 min).
    ttl_ms             => pos_integer(),
    %% Phase 4: hard-skip the bootstrap cascade between DHT + SWIM.
    %% Defaults to false. The cascade itself silently skips when no
    %% tiers are configured in app env, so most tests do NOT need
    %% to set this. The flag exists so Phase 3-style tests that
    %% MUST avoid touching the bootstrap path (e.g. when
    %% `hecate_bootstrap' app env is preloaded by another test
    %% case) can opt out cleanly.
    skip_cascade       => boolean(),
    %% Geo + display metadata stamped onto the published `node_record'
    %% by the per-identity announcer. `add_announcer_optionals/2'
    %% reads them when present and silently skips otherwise. Populated
    %% by `hecate_station_identity_config:spec_to_opts/3' from the
    %% MACULA_RELAY_IDENTITIES env-var entries.
    hostname           => binary(),
    city               => binary(),
    country            => binary(),
    lat                => float(),
    lng                => float(),
    display_name       => binary(),
    caps_hint          => binary()
}.

-spec start_link(identity_opts()) -> {ok, pid()} | {error, term()}.
start_link(IdentityOpts) when is_map(IdentityOpts) ->
    on_sup_started(supervisor:start_link(?MODULE, IdentityOpts),
                   IdentityOpts).

init(IdentityOpts) ->
    SupFlags = #{strategy  => one_for_all,
                 intensity => 5,
                 period    => 10},
    {ok, {SupFlags, static_children(IdentityOpts)}}.

%%==================================================================
%% Static children — built once at init.
%%==================================================================

static_children(IdentityOpts) ->
    pubsub_registry_child(IdentityOpts) ++
        content_announcer_child(IdentityOpts).

pubsub_registry_child(IdentityOpts) ->
    Opts = registry_opts(IdentityOpts),
    [
        #{id       => hecate_pubsub_registry,
          start    => {hecate_pubsub_registry, start_link, [Opts]},
          restart  => permanent,
          shutdown => 5_000,
          type     => worker,
          modules  => [hecate_pubsub_registry]}
    ].

%% identity_key is always present in identity_opts (`:=' not `=>')
%% so the registry's logger metadata always has a value to use.
registry_opts(#{identity := Kp, identity_key := Key}) ->
    #{identity => Kp, identity_key => Key};
registry_opts(#{identity_key := Key}) ->
    #{identity_key => Key}.

content_announcer_child(#{identity     := Kp,
                          identity_key := Key,
                          station_id   := SId,
                          endpoint     := Ep} = O) ->
    Opts = #{
        dht          => maps:get(dht, O, undefined),
        identity     => Kp,
        identity_key => Key,
        station_id   => SId,
        endpoint     => Ep,
        ttl_ms       => maps:get(ttl_ms, O, 300_000)
    },
    [
        #{id       => hecate_content_announcer,
          start    => {hecate_content_announcer, start_link, [Opts]},
          restart  => permanent,
          shutdown => 5_000,
          type     => worker,
          modules  => [hecate_content_announcer]}
    ];
content_announcer_child(_) ->
    [].

%%==================================================================
%% Procedural Phase-3 chain — added AFTER the sup is up so each
%% downstream worker can be started with the pid of the previous.
%%==================================================================

on_sup_started({error, _} = E, _Opts) ->
    E;
on_sup_started({ok, Sup}, Opts) ->
    on_phase3_done(Sup, attempt_phase3(Sup, Opts, has_listener_opts(Opts))).

attempt_phase3(_Sup, _Opts, false) ->
    skipped;
attempt_phase3(Sup, Opts, true) ->
    %% pubsub_registry is started by `static_children/1' before
    %% on_sup_started fires; look it up here so every downstream
    %% step that needs it (observer, fact_publisher) gets the pid.
    PsReg = pubsub_registry_pid(Sup),
    seed_handler_registry(Sup, Opts#{pubsub_registry => PsReg}).

pubsub_registry_pid(Sup) ->
    Children = supervisor:which_children(Sup),
    on_lookup(lists:keyfind(hecate_pubsub_registry, 1, Children)).

on_lookup({_, Pid, _, _}) when is_pid(Pid) -> Pid;
on_lookup(_)                               -> undefined.

on_phase3_done(Sup, skipped)        -> {ok, Sup};
on_phase3_done(Sup, ok)             -> {ok, Sup};
on_phase3_done(Sup, {error, _} = E) -> rollback(Sup, E).

rollback(Sup, Error) ->
    %% supervisor:start_link/2 links the sup to the caller. Unlink
    %% before bringing it down so the caller (registry's gen_server)
    %% does not receive a stray EXIT — the caller already has the
    %% error in `Error' and will not reuse the half-built sup.
    _ = unlink(Sup),
    _ = exit(Sup, shutdown),
    Error.

has_listener_opts(#{bind := _, port := _, certfile := _,
                    keyfile := _, identity := _}) -> true;
has_listener_opts(_)                              -> false.

%% Handler registry → DHT → cascade → SWIM → observer → listener.
%% Handler registry runs first so the observer (later in the chain)
%% can be wired to dispatch CALL frames against it.

seed_handler_registry(Sup, Opts) ->
    on_handler_registry(Sup, Opts,
                        supervisor:start_child(Sup,
                                               handler_registry_spec(Opts))).

on_handler_registry(_Sup, _Opts, {error, R}) ->
    {error, {handler_registry_start_failed, R}};
on_handler_registry(Sup, Opts, {ok, HrPid}) ->
    seed_dht(Sup, Opts#{handler_registry => HrPid}).

seed_dht(Sup, Opts) ->
    on_dht(Sup, Opts,
           supervisor:start_child(Sup, dht_spec(Opts))).

on_dht(_Sup, _Opts, {error, R}) ->
    {error, {dht_start_failed, R}};
on_dht(Sup, Opts, {ok, DhtPid}) ->
    seed_dht_handlers(Sup, Opts, DhtPid).

%% Advertises `_dht.*' procedures against the per-identity registry.
%% Lives between DHT start and cascade so `find_records_by_type' and
%% friends are reachable as soon as the DHT is live.
seed_dht_handlers(Sup, #{handler_registry := HrPid} = Opts, DhtPid) ->
    Spec = dht_handlers_spec(HrPid, DhtPid, Opts),
    on_dht_handlers(Sup, Opts, DhtPid,
                    supervisor:start_child(Sup, Spec)).

on_dht_handlers(_Sup, _Opts, _DhtPid, {error, R}) ->
    {error, {dht_handlers_start_failed, R}};
on_dht_handlers(Sup, Opts, DhtPid, {ok, _Pid}) ->
    seed_dht_custodians(Sup, Opts, DhtPid).

%% DHT custodians — periodic background tasks that propagate this
%% identity's view of the mesh:
%%
%%   * `hecate_dht_replicate'  — every interval, push K-nearest copies
%%     of foreign records this identity caches locally so they survive
%%     individual peer churn.
%%   * `hecate_dht_republish'  — every interval, the identity owner
%%     reissues + re-publishes the records it owns (e.g. its own
%%     node_record), so the K-nearest set follows churn.
%%   * `hecate_dht_expire'     — every interval, prune records past
%%     their `expires_at'.
%%
%% Without these the per-identity DHT is local-only — records never
%% reach K-nearest peers, peers never refresh their copies, expired
%% records stay forever. Default 1h cadence is fine for production;
%% tests don't rely on tick timing.
seed_dht_custodians(Sup, Opts, DhtPid) ->
    Specs = [replicate_spec(DhtPid),
             republish_spec(DhtPid, Opts),
             expire_spec(DhtPid)],
    %% Announcer is now started AFTER fact_publisher (which installs
    %% the DHT `on_record_stored' callback). Custodians proceed
    %% straight to cascade → SWIM → observer → fact_publisher →
    %% announcer → listener.
    seed_each(Sup, Specs, Opts, DhtPid, fun seed_cascade/3,
              dht_custodian_start_failed).

seed_each(_Sup, [], Opts, DhtPid, Next, _Tag) ->
    Next(_Sup, Opts, DhtPid);
seed_each(Sup, [Spec | Rest], Opts, DhtPid, Next, Tag) ->
    on_each(Sup, Rest, Opts, DhtPid, Next, Tag,
            supervisor:start_child(Sup, Spec)).

on_each(_Sup, _Rest, _Opts, _DhtPid, _Next, Tag, {error, R}) ->
    {error, {Tag, R}};
on_each(Sup, Rest, Opts, DhtPid, Next, Tag, {ok, _Pid}) ->
    seed_each(Sup, Rest, Opts, DhtPid, Next, Tag).

%% Per-identity announcer publishes this station's `node_record'
%% (Macula V2 type 0x01) into the local DHT on init + refreshes
%% before TTL. Tombstones on graceful shutdown so peers learn we
%% are gone without waiting for TTL.
%%
%% Started AFTER fact_publisher so the boot put_record fires
%% through the now-installed `on_record_stored' callback —
%% otherwise the boot announce is silent on the wire and the realm
%% only sees the next refresh-tick announcement (~7.5 min later).
%% Per-identity overlay seeder runs BEFORE the announcer so its pid
%% can be plumbed into announcer opts and surfaced via caps_hint.
%% Soft-fails: missing/empty env var means no outbound connects,
%% station continues to boot listener as before.
seed_overlay_seeder(Sup, Opts, ObsPid) ->
    on_overlay_seeder(Sup, Opts, ObsPid,
                       supervisor:start_child(Sup,
                                              overlay_seeder_spec(Opts, ObsPid))).

on_overlay_seeder(_Sup, _Opts, _ObsPid, {error, R}) ->
    {error, {overlay_seeder_start_failed, R}};
on_overlay_seeder(Sup, Opts, ObsPid, {ok, SeedPid}) ->
    Opts1 = Opts#{overlay_seeder => SeedPid},
    seed_bloom_exchange(Sup, Opts1, ObsPid).

%% Per-identity Bloom-filter exchange — gossips a 1KB filter of this
%% identity's locally-subscribed topics to peer stations every 30s,
%% caches inbound peer filters. Drives the peering forwarder's
%% "skip uninterested peers" decision so cross-relay pubsub doesn't
%% flood. Ports V1 macula_relay_bloom_exchange.
seed_bloom_exchange(Sup, Opts, ObsPid) ->
    on_bloom_exchange(Sup, Opts, ObsPid,
                       supervisor:start_child(Sup, bloom_exchange_spec(Opts))).

on_bloom_exchange(_Sup, _Opts, _ObsPid, {error, R}) ->
    {error, {bloom_exchange_start_failed, R}};
on_bloom_exchange(Sup, Opts, ObsPid, {ok, _Pid}) ->
    seed_forwarder_sup(Sup, Opts, ObsPid).

bloom_exchange_spec(IdentityOpts) ->
    Opts = #{
        pubsub_registry => maps:get(pubsub_registry, IdentityOpts),
        identity        => maps:get(identity, IdentityOpts),
        overlay_seeder  => maps:get(overlay_seeder, IdentityOpts, undefined),
        identity_key    => maps:get(identity_key, IdentityOpts)
    },
    #{id       => hecate_station_bloom_exchange,
      start    => {hecate_station_bloom_exchange, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_station_bloom_exchange]}.

%% Per-identity simple_one_for_one supervisor for outbound peering
%% forwarders. The peering router asks it to start/stop forwarder
%% workers as the (Topic, Peer) cross-product changes. Decouples
%% lifecycle management (the router) from worker supervision.
seed_forwarder_sup(Sup, Opts, ObsPid) ->
    on_forwarder_sup(Sup, Opts, ObsPid,
                     supervisor:start_child(Sup, forwarder_sup_spec())).

on_forwarder_sup(_Sup, _Opts, _ObsPid, {error, R}) ->
    {error, {forwarder_sup_start_failed, R}};
on_forwarder_sup(Sup, Opts, ObsPid, {ok, FwdSup}) ->
    Opts1 = Opts#{forwarder_sup => FwdSup},
    seed_peering_router(Sup, Opts1, ObsPid).

forwarder_sup_spec() ->
    #{id       => hecate_station_forwarder_sup,
      start    => {hecate_station_forwarder_sup, start_link, []},
      restart  => permanent,
      shutdown => 5_000,
      type     => supervisor,
      modules  => [hecate_station_forwarder_sup]}.

%% Per-identity peering router — periodically polls local topics +
%% peer links, reconciles outbound forwarders + inbound peer
%% subscriptions. Drives the cross-relay pubsub plumbing V1 had built
%% into macula_relay_peering's maybe_subscribe_on_peers cycle.
seed_peering_router(Sup, Opts, ObsPid) ->
    on_peering_router(Sup, Opts, ObsPid,
                       supervisor:start_child(Sup, peering_router_spec(Opts))).

on_peering_router(_Sup, _Opts, _ObsPid, {error, R}) ->
    {error, {peering_router_start_failed, R}};
on_peering_router(Sup, Opts, ObsPid, {ok, _Pid}) ->
    seed_relay_ping(Sup, Opts, ObsPid).

%% Per-identity relay-to-relay ping pinger + responder. Ports the
%% V1 macula_relay_heartbeat ping cycle onto V2's macula_station_link
%% RPC primitive: `_relay.ping' RPC handler returns immediately,
%% periodic call/5 measures RTT, result published as
%% `_mesh.relay.ping' so the realm topology LATENCY toggle has data.
seed_relay_ping(Sup, Opts, ObsPid) ->
    on_relay_ping(Sup, Opts, ObsPid,
                   supervisor:start_child(Sup, relay_ping_spec(Opts))).

on_relay_ping(_Sup, _Opts, _ObsPid, {error, R}) ->
    {error, {relay_ping_start_failed, R}};
on_relay_ping(Sup, Opts, ObsPid, {ok, _Pid}) ->
    seed_announcer(Sup, Opts, ObsPid).

relay_ping_spec(IdentityOpts) ->
    Opts = #{
        handler_registry => maps:get(handler_registry, IdentityOpts),
        pubsub_registry  => maps:get(pubsub_registry, IdentityOpts),
        overlay_seeder   => maps:get(overlay_seeder, IdentityOpts),
        identity         => maps:get(identity, IdentityOpts),
        hostname         => maps:get(hostname, IdentityOpts, <<>>),
        lat              => maps:get(lat, IdentityOpts, undefined),
        lng              => maps:get(lng, IdentityOpts, undefined),
        identity_key     => maps:get(identity_key, IdentityOpts)
    },
    #{id       => hecate_station_relay_ping,
      start    => {hecate_station_relay_ping, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_station_relay_ping]}.

peering_router_spec(IdentityOpts) ->
    Kp = maps:get(identity, IdentityOpts),
    Opts = #{
        pubsub_registry => maps:get(pubsub_registry, IdentityOpts),
        identity        => Kp,
        forwarder_sup   => maps:get(forwarder_sup, IdentityOpts),
        overlay_seeder  => maps:get(overlay_seeder, IdentityOpts),
        pg_scope        => pg_scope_for(Kp),
        realm           => <<0:256>>,
        identity_key    => maps:get(identity_key, IdentityOpts)
    },
    #{id       => hecate_station_peering_router,
      start    => {hecate_station_peering_router, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_station_peering_router]}.

%% Per-identity pg scope so forwarders running for sibling identities
%% on the same BEAM don't cross-pollinate. Hashed off the identity's
%% pubkey for stability across restarts.
pg_scope_for(Kp) ->
    Pub = macula_identity:public(Kp),
    binary_to_atom(<<"hecate_station_relay_", (binary:encode_hex(Pub))/binary>>).

seed_announcer(Sup, Opts, ObsPid) ->
    DhtPid = maps:get(dht_pid, Opts),
    on_announcer(Sup, Opts, ObsPid,
                 supervisor:start_child(Sup, announcer_spec(DhtPid, Opts))).

on_announcer(_Sup, _Opts, _ObsPid, {error, R}) ->
    {error, {announcer_start_failed, R}};
on_announcer(Sup, Opts, ObsPid, {ok, _Pid}) ->
    seed_listener(Sup, Opts, ObsPid).

overlay_seeder_spec(IdentityOpts, ObsPid) ->
    Opts = #{
        identity      => maps:get(identity, IdentityOpts),
        peer_observer => ObsPid,
        realms        => maps:get(realms, IdentityOpts, []),
        capabilities  => maps:get(capabilities, IdentityOpts, 0),
        identity_key  => maps:get(identity_key, IdentityOpts)
    },
    #{id       => hecate_station_overlay_seeder,
      start    => {hecate_station_overlay_seeder, start_link, [Opts]},
      restart  => transient,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_station_overlay_seeder]}.

%% Bootstrap cascade — runs between DHT start and SWIM start so the
%% routing table is seeded before SWIM begins probing peers. The
%% cascade reads `hecate_bootstrap' app env; when no tiers are
%% configured it returns `{error, no_tiers}' which we treat as a
%% silent skip — the same identity may run in unit-test contexts
%% with an empty bootstrap config without aborting boot. Any other
%% cascade error IS fatal: a misconfigured tier should not be
%% papered over.
seed_cascade(Sup, #{skip_cascade := true} = Opts, DhtPid) ->
    seed_swim(Sup, Opts, DhtPid);
seed_cascade(Sup, Opts, DhtPid) ->
    on_cascade(Sup, Opts, DhtPid,
               hecate_station_bootstrap_runner:run(DhtPid)).

on_cascade(Sup, Opts, DhtPid, {ok, _Summary}) ->
    seed_swim(Sup, Opts, DhtPid);
on_cascade(Sup, Opts, DhtPid, {error, no_tiers}) ->
    seed_swim(Sup, Opts, DhtPid);
on_cascade(_Sup, _Opts, _DhtPid, {error, R}) ->
    {error, {cascade_failed, R}}.

seed_swim(Sup, Opts, DhtPid) ->
    on_swim(Sup, Opts, DhtPid,
            supervisor:start_child(Sup, swim_spec(Opts, Sup))).

on_swim(_Sup, _Opts, _DhtPid, {error, R}) ->
    {error, {swim_start_failed, R}};
on_swim(Sup, Opts, DhtPid, {ok, SwimPid}) ->
    seed_observer(Sup, Opts, DhtPid, SwimPid).

seed_observer(Sup, Opts, DhtPid, SwimPid) ->
    on_observer(Sup, Opts, DhtPid,
                supervisor:start_child(Sup,
                                       observer_spec(DhtPid, SwimPid, Opts))).

on_observer(_Sup, _Opts, _DhtPid, {error, R}) ->
    {error, {observer_start_failed, R}};
on_observer(Sup, Opts, DhtPid, {ok, ObsPid}) ->
    seed_fact_publisher(Sup, Opts, DhtPid, ObsPid).

%% Fact publisher translates record-store events into typed business
%% events on the peering pubsub channel. Started AFTER observer
%% (needs ObsPid) and AFTER pubsub_registry (in static children) but
%% BEFORE listener — so the very first inbound peer that subscribes
%% sees a live publisher.
seed_fact_publisher(Sup, Opts, DhtPid, ObsPid) ->
    on_fact_publisher(Sup, Opts, DhtPid, ObsPid,
                      supervisor:start_child(
                        Sup,
                        fact_publisher_spec(Opts, ObsPid))).

on_fact_publisher(_Sup, _Opts, _DhtPid, _ObsPid, {error, R}) ->
    {error, {fact_publisher_start_failed, R}};
on_fact_publisher(Sup, Opts, DhtPid, ObsPid, {ok, FpPid}) ->
    logger:info(
      "[identity_sup] fact_publisher started pid=~p — installing DHT callback",
      [FpPid]),
    install_dht_callback(DhtPid, FpPid),
    %% Boot order: overlay_seeder -> announcer -> listener.
    %% Seeder runs first so its pid can be plumbed into the
    %% announcer's opts; the announcer reads connected_hostnames/1
    %% on every refresh and surfaces them in caps_hint, giving the
    %% realm topology view edges to draw even before peer node_records
    %% replicate into local DHT.
    Opts1 = Opts#{peer_observer => ObsPid, dht_pid => DhtPid},
    seed_overlay_seeder(Sup, Opts1, ObsPid).

%% Bind the DHT's `on_record_stored' callback to the publisher. The
%% DHT server stores the function and invokes it after every
%% successful `store_put' (own put + incoming custodian STORE).
install_dht_callback(DhtPid, FpPid) ->
    ok = hecate_dht:set_on_record_stored(
           DhtPid,
           fun(Record) ->
               hecate_station_fact_publisher:on_record_stored(FpPid, Record)
           end),
    logger:info("[identity_sup] DHT callback installed: dht=~p fp=~p",
                [DhtPid, FpPid]).

seed_listener(Sup, Opts, ObsPid) ->
    on_listener(supervisor:start_child(Sup, listener_spec(Opts, ObsPid))).

on_listener({error, R}) -> {error, {listener_start_failed, R}};
on_listener({ok, _Pid}) -> ok.

%%==================================================================
%% Phase-3 child specs.
%%==================================================================

dht_spec(#{identity := Kp}) ->
    Self = macula_identity:public(Kp),
    #{id       => hecate_dht,
      start    => {hecate_dht, start_link, [#{self_id => Self}]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_dht]}.

swim_spec(#{identity := Kp}, Sup) ->
    Opts = #{
        self_node_id    => macula_identity:public(Kp),
        identity        => Kp,
        %% The identity_sup pid is a long-lived safe sink for SWIM
        %% membership notifications. Phase 4 / 5 will route them to
        %% the per-identity status reporter.
        controlling_pid => Sup
    },
    #{id       => hecate_swim,
      start    => {hecate_swim, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_swim]}.

handler_registry_spec(IdentityOpts) ->
    %% Identity_key is always present in identity_opts (`:=' not `=>')
    %% so the registry's logger metadata always has a value.
    Opts = #{identity_key => maps:get(identity_key, IdentityOpts)},
    #{id       => hecate_handler_registry,
      start    => {hecate_handler_registry, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_handler_registry]}.

dht_handlers_spec(HrPid, DhtPid, IdentityOpts) ->
    Opts = #{
        handler_registry => HrPid,
        dht              => DhtPid,
        identity_key     => maps:get(identity_key, IdentityOpts)
    },
    #{id       => hecate_station_dht_handlers,
      start    => {hecate_station_dht_handlers, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_station_dht_handlers]}.

announcer_spec(DhtPid, IdentityOpts) ->
    Opts = announcer_opts(DhtPid, IdentityOpts),
    #{id       => hecate_station_announcer,
      start    => {hecate_station_announcer, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_station_announcer]}.

announcer_opts(DhtPid, IdentityOpts) ->
    Base0 = #{
        dht          => DhtPid,
        identity     => maps:get(identity, IdentityOpts),
        identity_key => maps:get(identity_key, IdentityOpts),
        realms       => maps:get(realms, IdentityOpts, []),
        capabilities => maps:get(capabilities, IdentityOpts, 0)
    },
    %% Forward the peer_observer pid (planted by `on_fact_publisher')
    %% so the announcer can query the live overlay-peer set on each
    %% refresh and stamp it onto the node_record's `peers' field.
    Base1 = forward_pid(peer_observer, IdentityOpts, Base0),
    %% Forward the overlay_seeder pid so the announcer can include
    %% explicit dial targets (which may not yet be in local DHT) in
    %% the caps_hint peers list.
    Base = forward_pid(overlay_seeder, IdentityOpts, Base1),
    add_announcer_optionals(Base, IdentityOpts).

forward_pid(Key, Src, Acc) ->
    case maps:find(Key, Src) of
        {ok, Pid} -> Acc#{Key => Pid};
        error     -> Acc
    end.

add_announcer_optionals(Base, Src) ->
    %% `hostname' / `bind' added 2026-04 to fix V2 regressions:
    %%   * hostname: realm relay tooltip + country aggregate regex
    %%   * bind:     IPv6 listener address surfaced as `ipv6' in the
    %%               topology view
    Optional = [hostname, city, country, lat, lng, endpoint, ttl_ms,
                display_name, caps_hint, bind],
    lists:foldl(fun(K, Acc) ->
        case maps:find(K, Src) of
            {ok, V} -> Acc#{K => V};
            error   -> Acc
        end
    end, Base, Optional).

replicate_spec(DhtPid) ->
    #{id       => hecate_dht_replicate,
      start    => {hecate_dht_replicate, start_link, [#{dht => DhtPid}]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_dht_replicate]}.

republish_spec(DhtPid, #{identity := Kp}) ->
    Opts = #{dht => DhtPid, identity => Kp},
    #{id       => hecate_dht_republish,
      start    => {hecate_dht_republish, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_dht_republish]}.

expire_spec(DhtPid) ->
    #{id       => hecate_dht_expire,
      start    => {hecate_dht_expire, start_link, [#{dht => DhtPid}]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_dht_expire]}.

observer_spec(DhtPid, SwimPid, IdentityOpts) ->
    Opts = #{
        dht              => DhtPid,
        swim             => SwimPid,
        handler_registry => maps:get(handler_registry, IdentityOpts, undefined),
        pubsub_registry  => maps:get(pubsub_registry, IdentityOpts, undefined),
        self_id          => self_id_of(IdentityOpts)
    },
    #{id       => hecate_station_peer_observer,
      start    => {hecate_station_peer_observer, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_station_peer_observer]}.

fact_publisher_spec(IdentityOpts, ObsPid) ->
    Opts = #{
        pubsub_registry => maps:get(pubsub_registry, IdentityOpts),
        peer_observer   => ObsPid,
        identity        => maps:get(identity, IdentityOpts),
        identity_key    => maps:get(identity_key, IdentityOpts)
    },
    #{id       => hecate_station_fact_publisher,
      start    => {hecate_station_fact_publisher, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_station_fact_publisher]}.

%% observer_spec/3 only fires through the Phase-3 chain, which is
%% gated by `has_listener_opts/1' — `identity' is therefore always
%% present at this call site.
self_id_of(#{identity := Kp}) -> macula_identity:public(Kp).

listener_spec(#{bind := Bind, port := Port,
                certfile := Cert, keyfile := Key,
                identity := Kp} = O, ObsPid) ->
    Opts = #{
        bind         => Bind,
        port         => Port,
        certfile     => Cert,
        keyfile      => Key,
        identity     => Kp,
        realms       => maps:get(realms, O, []),
        capabilities => maps:get(capabilities, O, 0),
        observer     => ObsPid
    },
    #{id       => hecate_station_listener,
      start    => {hecate_station_listener, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_station_listener]}.
