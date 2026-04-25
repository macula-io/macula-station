%% @doc Per-identity supervisor.
%%
%% A single hecate-station BEAM process hosts N identities
%% concurrently (PLAN_MULTI_IDENTITY_RELAY). Each identity gets its
%% own supervisor so the per-identity workers share fate: if the
%% DHT or the listener crashes for identity A, the rest of identity
%% A is taken down and restarted in lockstep, but identities B and
%% C are unaffected.
%%
%% Phase 2 (this commit) wires the de-singletonised pubsub +
%% content-announcer pair under the supervisor:
%%
%% <ul>
%%   <li>`hecate_pubsub_registry' — per-identity per-realm pubsub
%%       fabric. The registry spawn-links its own
%%       `hecate_pubsub_server' workers when a realm is first
%%       referenced.</li>
%%   <li>`hecate_content_announcer' — per-identity content
%%       provider record publisher. Subscribes to the SHARED
%%       `hecate_content_store' event group and publishes records
%%       signed with the identity's keypair into the identity's
%%       (Phase-3) DHT instance.</li>
%% </ul>
%%
%% Optional children: `content_announcer' is only added when
%% `IdentityOpts' carries the announcer prerequisites
%% (`identity', `station_id', `endpoint'). Absent any of these,
%% the supervisor comes up with just the pubsub_registry — the
%% Phase 1 lifecycle tests still hit this code path with
%% `IdentityOpts = #{identity_key =&gt; _}' and must stay green.
%%
%% Phase 3 will add: `hecate_station_server', listener,
%% per-identity DHT + SWIM. Those layers still hit `{local, _}'
%% via `hecate_station_sup' so they cannot share identity_sup yet.
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
    %% Optional Phase-3 hook — once the per-identity DHT lands,
    %% this carries its pid; until then the announcer runs with
    %% `dht = undefined' (no-op publish path).
    dht                => hecate_dht:dht() | undefined,
    %% Optional content-announcer record TTL (defaults to 5 min).
    ttl_ms             => pos_integer()
}.

-spec start_link(identity_opts()) -> {ok, pid()} | {error, term()}.
start_link(IdentityOpts) when is_map(IdentityOpts) ->
    supervisor:start_link(?MODULE, IdentityOpts).

init(IdentityOpts) ->
    SupFlags = #{strategy  => one_for_all,
                 intensity => 5,
                 period    => 10},
    {ok, {SupFlags, build_children(IdentityOpts)}}.

%%==================================================================
%% Children — order matters for `one_for_all' restart cascades.
%% pubsub_registry first (no deps); content_announcer second
%% (independent, but conceptually layered on top).
%%==================================================================

build_children(IdentityOpts) ->
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

%% Announcer is optional. Phase-1 lifecycle tests pass
%% `#{identity_key => _}' with no other keys; in that mode the
%% announcer is omitted and the sup's child list is just the
%% registry. Phase 4 (config loader) will always supply the full
%% announcer opts at boot.
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
