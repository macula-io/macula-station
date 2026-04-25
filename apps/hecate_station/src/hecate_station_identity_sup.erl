%% @doc Per-identity supervisor — Phase 1 skeleton.
%%
%% A single hecate-station BEAM process is being reshaped to host
%% N identities concurrently (PLAN_MULTI_IDENTITY_RELAY). Each
%% identity gets its own supervisor so the per-identity workers
%% share fate: if the DHT or the listener crashes for identity A,
%% the rest of identity A is taken down and restarted in lockstep,
%% but identities B and C are unaffected.
%%
%% Phase 1 (this commit) lays the OTP shape only. The supervisor
%% starts with an empty children list — Phase 2 + Phase 3 wire the
%% actual per-identity workers in once they are de-singletonised:
%%
%% <ul>
%%   <li>Phase 2: `hecate_station_server' (anonymous),
%%       `hecate_pubsub_registry', `hecate_pubsub_server_sup',
%%       `hecate_content_announcer'.</li>
%%   <li>Phase 3: `hecate_station_listener' bound to the identity's
%%       IPv6, plus per-identity `hecate_swim' / `hecate_dht_server'
%%       (already pid-based — just need an identity-aware spec).</li>
%%   <li>Phase 3: `hecate_overlay_sup' (per-realm pubsub fabric)
%%       reparented under each identity_sup.</li>
%% </ul>
%%
%% Started by `hecate_station_identity_registry' on
%% `register/2'. Not name-registered (`{local, _}') — N instances
%% must coexist, identified solely by the registry's
%% `identity_key()' map.
-module(hecate_station_identity_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

-export_type([identity_opts/0]).

-type identity_opts() :: #{
    identity_key       := hecate_station_identity_registry:identity_key(),
    %% Phase 2+ will require these. Phase 1 tolerates either being
    %% absent — the empty child list does not consume them.
    identity           => macula_identity:key_pair(),
    bind               => inet:ip_address() | string(),
    port               => inet:port_number(),
    capabilities       => non_neg_integer(),
    realms             => [macula_identity:pubkey()]
}.

-spec start_link(identity_opts()) -> {ok, pid()} | {error, term()}.
start_link(IdentityOpts) when is_map(IdentityOpts) ->
    supervisor:start_link(?MODULE, IdentityOpts).

init(IdentityOpts) ->
    SupFlags = #{strategy  => one_for_all,
                 intensity => 5,
                 period    => 10},
    {ok, {SupFlags, build_children(IdentityOpts)}}.

%% @doc Per-identity child specs. Phase 1 is intentionally empty —
%% see module docs for the Phase-2/3 plan.
build_children(_IdentityOpts) ->
    [].
