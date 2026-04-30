%% @doc Deterministic bootstrap tier stub for integration tests.
%%
%% Implements `macula_bootstrap_tier' so `macula_bootstrap:run/0,1'
%% can drive it. The probe returns the peers stashed in the tier
%% options map, letting tests feed exact verified_peer() records
%% into the cascade without network I/O.
%%
%% Usage:
%% ```
%% Peers = [stub_peer(<<1:256>>), stub_peer(<<2:256>>), ...],
%% Tiers = [{macula_station_stub_tier, #{peers => Peers}}],
%% {ok, Out} = macula_bootstrap:run(#{tiers => Tiers,
%%                                    cascade_opts => #{min_peers => 1}}).
%% '''
-module(macula_station_stub_tier).
-behaviour(macula_bootstrap_tier).

-export([tier/0, stagger_ms/0, probe/1]).
-export([stub_peer/1, stub_peers/1]).

tier() -> a.
stagger_ms() -> 0.

probe(#{peers := Peers}) -> {ok, Peers};
probe(#{error := Err})   -> {error, Err};
probe(_)                 -> {ok, []}.

%%==================================================================
%% Peer constructors — convenience for tests.
%%==================================================================

stub_peer(NodeId) when is_binary(NodeId), byte_size(NodeId) =:= 32 ->
    #{
        node_id   => NodeId,
        record    => fake_record(NodeId),
        addresses => [],
        tier      => a,
        via       => ?MODULE
    }.

stub_peers(N) when is_integer(N), N > 0 ->
    [stub_peer(<<I:256>>) || I <- lists:seq(1, N)].

fake_record(Key) ->
    #{type => 16#0D, key => Key, version => <<0:128>>,
      created_at => 1, expires_at => 2, payload => #{}}.
