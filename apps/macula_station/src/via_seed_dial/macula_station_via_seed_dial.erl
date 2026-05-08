%% @doc Seed-dial bootstrap tier — reads verified peers from the
%% local `macula_station_peer_links' registry.
%%
%% Unlike the foundation-anchored / mDNS / chain / explicit-URL
%% tiers (A-E), `seed_dial' relies on the operator's outbound peer
%% list having already been spawned as live
%% `macula_station_outbound_link' workers BEFORE the cascade runs (see
%% `macula_station_app:boot_outbound_links/2').
%%
%% At probe time the tier waits a configurable stagger window for
%% in-flight handshakes to complete, then snapshots the registry's
%% `verified_peers/0' list and returns it as `verified_peer()' maps.
%%
%% == Probe options ==
%% <ul>
%%   <li>`wait_ms' :: non_neg_integer() — total wait time before
%%       snapshotting (default 3000 ms). Larger values trade boot
%%       latency for more chances of catching a slow handshake.</li>
%% </ul>
%%
%% == Stagger ==
%%
%% Tier seed_dial enters the cascade at zero stagger — outbound link
%% workers are already up by the time the cascade starts.
-module(macula_station_via_seed_dial).
-behaviour(macula_bootstrap_peer_discoverer).

-export([strategy/0, stagger_ms/0, discover/1]).

-define(DEFAULT_WAIT_MS, 3_000).

strategy()   -> via_seed_dial.
stagger_ms() -> 0.

-spec discover(macula_bootstrap_peer_discoverer:discover_opts()) ->
          macula_bootstrap_peer_discoverer:discover_result().
discover(Opts) ->
    WaitMs = maps:get(wait_ms, Opts, ?DEFAULT_WAIT_MS),
    timer:sleep(WaitMs),
    %% Always returns `{ok, _}'. Empty list is valid: a fresh-fleet
    %% station whose siblings haven't booted yet cannot dial them
    %% successfully, but the listener still needs to come up so the
    %% siblings can dial in. Pair with `cascade_opts: #{min_peers => 0}'
    %% so the cascade accepts the empty result and boot proceeds.
    {ok, [to_verified_peer(P) || P <- macula_station_peer_links:verified_peers()]}.

to_verified_peer(#{node_id := N, host := H, port := P}) ->
    #{
        node_id   => N,
        record    => undefined,
        addresses => [#{host => H, port => P, transport => quic}],
        strategy  => via_seed_dial,
        via       => ?MODULE
    }.
