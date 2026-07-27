%% Liveness probing of full buckets.
%%
%% The test that matters most here is the NEGATIVE one. On this fleet most
%% routing-table entries carry no dialable address, and station entries are
%% deliberately kept across disconnect, so a perfectly healthy station that
%% is simply not connected right now answers `{error, no_route}'. If this
%% worker treated that as death it would reap exactly the peers the freeze
%% fix exists to protect, and it would do so quietly.
-module(macula_dht_liveness_tests).

-include_lib("eunit/include/eunit.hrl").

-define(K, 20).

%%---------------------------------------------------------------------
%% Target selection
%%---------------------------------------------------------------------

only_full_buckets_are_probed_test() ->
    {ok, D} = start_dht(),
    %% One peer: the bucket holding it is nowhere near capacity.
    admitted = macula_dht:observe(D, spec(<<1:256>>)),
    {ok, L} = macula_dht_liveness:start_link(#{dht => D}),
    ?assertEqual(0, macula_dht_liveness:tick(L)),
    macula_dht_liveness:stop(L),
    macula_dht:stop(D).

%% With no transport wired, every ping answers {error, no_transport}, which
%% is `unreachable' — so a full bucket is probed but nothing is evicted.
a_full_bucket_is_probed_test() ->
    {ok, D} = start_dht(),
    fill_one_bucket(D),
    {ok, L} = macula_dht_liveness:start_link(#{dht => D, ping_timeout_ms => 50}),
    N = macula_dht_liveness:tick(L),
    ?assert(N >= 1),
    macula_dht_liveness:stop(L),
    macula_dht:stop(D).

%%---------------------------------------------------------------------
%% The negative case: unreachable is NOT dead
%%---------------------------------------------------------------------

unreachable_peer_is_not_evicted_test() ->
    {ok, D} = start_dht(),
    Ids = fill_one_bucket(D),
    Before = macula_dht:size(D),
    {ok, L} = macula_dht_liveness:start_link(#{dht => D, ping_timeout_ms => 50}),
    _ = macula_dht_liveness:tick(L),
    timer:sleep(300),
    #{unreachable := U, evicted := E} = macula_dht_liveness:stats(L),
    %% No transport is configured, so every probe is a statement about us.
    ?assert(U >= 1),
    ?assertEqual(0, E),
    ?assertEqual(Before, macula_dht:size(D)),
    [?assertMatch({ok, _}, macula_dht:find(D, I)) || I <- Ids],
    macula_dht_liveness:stop(L),
    macula_dht:stop(D).

%%---------------------------------------------------------------------
%% A timeout IS death, and frees the slot
%%---------------------------------------------------------------------

timed_out_peer_is_evicted_and_slot_frees_test() ->
    %% send_frame that swallows the frame: the peer is reachable as far as
    %% the transport is concerned, and simply never answers. That is the one
    %% verdict about the PEER rather than about us.
    Kp = macula_identity:generate(),
    {ok, D} = macula_dht:start_link(#{self_id    => macula_identity:public(Kp),
                                      identity   => Kp,
                                      send_frame => fun(_, _) -> ok end,
                                      ping_timeout_ms => 60}),
    Ids = fill_one_bucket(D),
    ?assertEqual(?K, macula_dht:size(D)),

    %% Before: the bucket is saturated and rejects a newcomer outright.
    ?assertEqual(rejected, macula_dht:observe(D, spec(<<250:256>>))),

    {ok, L} = macula_dht_liveness:start_link(#{dht => D, ping_timeout_ms => 60}),
    _ = macula_dht_liveness:tick(L),
    timer:sleep(500),
    #{evicted := E} = macula_dht_liveness:stats(L),
    ?assert(E >= 1),
    ?assert(macula_dht:size(D) < ?K),

    %% After: the freed slot admits the newcomer that was locked out.
    ?assertEqual(admitted, macula_dht:observe(D, spec(<<251:256>>))),
    ?assert(lists:any(fun(I) -> macula_dht:find(D, I) =:= error end, Ids)),

    macula_dht_liveness:stop(L),
    macula_dht:stop(D).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

start_dht() ->
    Kp = macula_identity:generate(),
    macula_dht:start_link(#{self_id => macula_identity:public(Kp)}).

%% Production-shaped spec: exactly what direct_peer_spec/2 produces.
spec(Id) ->
    #{node_id   => Id,
      endpoints => [],
      asn       => 0,
      country   => <<"??">>,
      tier      => t0}.

%% Ids sharing a long prefix land in the same bucket, so K of them saturate
%% it. The low byte varies to keep them distinct.
fill_one_bucket(D) ->
    Ids = [<<0:248, N:8>> || N <- lists:seq(1, ?K)],
    [admitted = macula_dht:observe(D, spec(I)) || I <- Ids],
    Ids.
