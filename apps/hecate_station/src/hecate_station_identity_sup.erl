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
    skip_cascade       => boolean()
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

registry_opts(#{identity := Kp}) -> #{identity => Kp};
registry_opts(_)                 -> #{}.

content_announcer_child(#{identity   := Kp,
                          station_id := SId,
                          endpoint   := Ep} = O) ->
    Opts = #{
        dht        => maps:get(dht, O, undefined),
        identity   => Kp,
        station_id => SId,
        endpoint   => Ep,
        ttl_ms     => maps:get(ttl_ms, O, 300_000)
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
    seed_dht(Sup, Opts).

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

%% DHT → SWIM → observer → listener, flat clauses.

seed_dht(Sup, Opts) ->
    on_dht(Sup, Opts,
           supervisor:start_child(Sup, dht_spec(Opts))).

on_dht(_Sup, _Opts, {error, R}) ->
    {error, {dht_start_failed, R}};
on_dht(Sup, Opts, {ok, DhtPid}) ->
    seed_cascade(Sup, Opts, DhtPid).

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
    on_observer(Sup, Opts,
                supervisor:start_child(Sup, observer_spec(DhtPid, SwimPid))).

on_observer(_Sup, _Opts, {error, R}) ->
    {error, {observer_start_failed, R}};
on_observer(Sup, Opts, {ok, ObsPid}) ->
    seed_listener(Sup, Opts, ObsPid).

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

observer_spec(DhtPid, SwimPid) ->
    Opts = #{dht => DhtPid, swim => SwimPid},
    #{id       => hecate_station_peer_observer,
      start    => {hecate_station_peer_observer, start_link, [Opts]},
      restart  => permanent,
      shutdown => 5_000,
      type     => worker,
      modules  => [hecate_station_peer_observer]}.

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
