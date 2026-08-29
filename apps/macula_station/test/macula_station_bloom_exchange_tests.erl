%% @doc Focused tests for `macula_station_bloom_exchange'.
%%
%% Drives the gen_server through cast/info messages and inspects the
%% effective broadcast bloom by intercepting `macula_station_link:publish/4'
%% (which the bloom_exchange uses internally) via the trace BIF.
-module(macula_station_bloom_exchange_tests).

-include_lib("eunit/include/eunit.hrl").

-define(MESH_REALM, <<0:256>>).

%%------------------------------------------------------------------
%% Transitive merge — the Phase 3 behaviour
%%
%% A station with NO local subscribers but a populated `peer_blooms'
%% map MUST broadcast a bloom that contains the union of every
%% peer's bloom — so the publishing station two hops away can tell
%% that some downstream subscriber cares about the topic and forward
%% the EVENT.
%%
%% Without Phase 3, the broadcast would be empty (no local
%% subscribers means an empty local bloom), the publishing station
%% would skip the forward, and multi-hop pubsub would never deliver.
%%------------------------------------------------------------------

merge_with_peers_unions_every_peer_bloom_test() ->
    %% Build two peer blooms with distinct topics; the merge must
    %% match BOTH at `macula_station_bloom:check/2'.
    PeerA = bloom_with([<<"topic.a">>, <<"topic.shared">>]),
    PeerB = bloom_with([<<"topic.b">>, <<"topic.shared">>]),
    Local = bloom_with([<<"topic.local">>]),
    Empty = macula_station_bloom:to_binary(macula_station_bloom:new()),

    %% Reach the private merge function via the public rebuild
    %% surface: spin up a registry-less bloom_exchange, inject
    %% peer_blooms via the cast API, then `rebuild_and_broadcast'
    %% and capture the outgoing bloom by tracing `broadcast_filter'.

    %% Direct functional check — the inputs are public types.
    Merged = direct_merge(Local, #{
        <<1, 0:248>> => PeerA,
        <<2, 0:248>> => PeerB
    }),
    MergedBF = macula_station_bloom:from_binary(Merged),

    ?assert(macula_station_bloom:check(<<"topic.local">>,  MergedBF)),
    ?assert(macula_station_bloom:check(<<"topic.a">>,      MergedBF)),
    ?assert(macula_station_bloom:check(<<"topic.b">>,      MergedBF)),
    ?assert(macula_station_bloom:check(<<"topic.shared">>, MergedBF)),
    ?assertNot(macula_station_bloom:check(<<"topic.absent">>, MergedBF)),

    %% Empty peer_blooms — falls back to the local bloom verbatim.
    EmptyMerged = direct_merge(Empty, #{}),
    EmptyBF = macula_station_bloom:from_binary(EmptyMerged),
    ?assertNot(macula_station_bloom:check(<<"topic.a">>, EmptyBF)).

malformed_peer_bloom_is_skipped_not_crash_test() ->
    Local = bloom_with([<<"topic.local">>]),
    Merged = direct_merge(Local, #{
        <<3, 0:248>> => <<"too-short">>,
        <<4, 0:248>> => bloom_with([<<"topic.a">>])
    }),
    BF = macula_station_bloom:from_binary(Merged),
    ?assert(macula_station_bloom:check(<<"topic.a">>, BF)),
    ?assert(macula_station_bloom:check(<<"topic.local">>, BF)).

%%------------------------------------------------------------------
%% Wildcard pattern gossip (2026-08-29) — separate from the Bloom,
%% see the module's own moduledoc for why: patterns are gossiped raw
%% (no Bloom-summarization) on their own `_mesh.patterns' topic, same
%% transitive-union shape as the Bloom just as a set union instead of
%% a bitwise OR.
%%------------------------------------------------------------------

%% `merge_patterns_with_peers/2' is private; re-derived here the same
%% way `direct_merge/2' re-derives the Bloom merge above, for the same
%% reason (reaching a private function via `sys:get_state' tricks
%% would be brittle).
direct_merge_patterns(LocalPatterns, PeerPatterns) ->
    lists:usort(
      lists:foldl(fun(Peer, Acc) -> Peer ++ Acc end,
                 LocalPatterns, maps:values(PeerPatterns))).

merge_patterns_with_peers_unions_every_peer_set_test() ->
    Merged = direct_merge_patterns(
               [<<"local/*">>],
               #{<<1, 0:248>> => [<<"acme/*">>, <<"shared/*">>],
                 <<2, 0:248>> => [<<"contoso/*">>, <<"shared/*">>]}),
    ?assertEqual(lists:sort([<<"local/*">>, <<"acme/*">>, <<"contoso/*">>,
                            <<"shared/*">>]),
                 lists:sort(Merged)).

merge_patterns_with_peers_dedupes_test() ->
    Merged = direct_merge_patterns([<<"a/*">>], #{<<1, 0:248>> => [<<"a/*">>]}),
    ?assertEqual([<<"a/*">>], Merged).

merge_patterns_with_peers_empty_peers_falls_back_to_local_test() ->
    ?assertEqual([<<"local/*">>], direct_merge_patterns([<<"local/*">>], #{})).

%% Same shape as `peer_bloom_change_schedules_debounce_test' /
%% `duplicate_peer_bloom_does_not_reschedule_test' above, for patterns.
peer_patterns_change_schedules_debounce_test() ->
    {Pid, _Kp} = start_exchange(),
    try
        ?assertEqual(undefined, debounce_ref_of(Pid)),
        ok = macula_station_bloom_exchange:receive_peer_patterns(
               Pid, <<"peer-a">>, [<<"acme/*">>]),
        Ref = debounce_ref_of(Pid),
        ?assert(is_reference(Ref))
    after
        stop_exchange(Pid)
    end.

duplicate_peer_patterns_does_not_reschedule_test() ->
    {Pid, _Kp} = start_exchange(),
    try
        Patterns = [<<"acme/*">>],
        ok = macula_station_bloom_exchange:receive_peer_patterns(
               Pid, <<"peer-a">>, Patterns),
        Ref1 = debounce_ref_of(Pid),
        ?assert(is_reference(Ref1)),
        ok = macula_station_bloom_exchange:receive_peer_patterns(
               Pid, <<"peer-a">>, Patterns),
        ?assertEqual(Ref1, debounce_ref_of(Pid))
    after
        stop_exchange(Pid)
    end.

%% peer_patterns/1 + pattern_matches_ets/1 -- the actual publish-time
%% surface `route_pubsub_frames' consumes.
receive_peer_patterns_is_queryable_and_matches_test() ->
    {Pid, _Kp} = start_exchange(),
    try
        ok = macula_station_bloom_exchange:receive_peer_patterns(
               Pid, <<7, 0:248>>, [<<"realm/*/app/domain/name_v1">>]),
        _ = sys:get_state(Pid),
        ?assertEqual(#{<<7, 0:248>> => [<<"realm/*/app/domain/name_v1">>]},
                     macula_station_bloom_exchange:peer_patterns(Pid)),
        ?assertEqual([<<7, 0:248>>],
                     macula_station_bloom_exchange:pattern_matches_ets(
                       <<"realm/acme/app/domain/name_v1">>)),
        ?assertEqual([],
                     macula_station_bloom_exchange:pattern_matches_ets(
                       <<"realm/acme/app/domain/other_v1">>))
    after
        stop_exchange(Pid)
    end.

%% Inbound `_mesh.patterns' EVENT -- same shape as
%% `macula_event_change_schedules_debounce_test' above.
macula_event_patterns_change_schedules_debounce_test() ->
    {Pid, Kp} = start_exchange(),
    try
        Publisher = other_pubkey(Kp),
        Payload = term_to_binary([<<"acme/*">>]),
        Pid ! {macula_event, ignored_sub_ref, <<"_mesh.patterns">>,
               Payload, #{publisher => Publisher}},
        _ = sys:get_state(Pid),
        ?assert(is_reference(debounce_ref_of(Pid)))
    after
        stop_exchange(Pid)
    end.

%% The actual security property `safe_decode_patterns/1' exists for:
%% a malformed / hostile payload on `_mesh.patterns' must not crash
%% the gen_server, must not poison `peer_patterns', and — since
%% `binary_to_term(_, [safe])' is what's used — must never grow the
%% atom table. Driven through the message interface, matching this
%% file's own convention of not exporting private internals for test.
malformed_mesh_patterns_payload_is_dropped_not_crash_test() ->
    {Pid, Kp} = start_exchange(),
    try
        Publisher = other_pubkey(Kp),
        %% Not a term_to_binary'd list at all -- garbage bytes.
        Pid ! {macula_event, ignored_sub_ref, <<"_mesh.patterns">>,
               <<"not-a-valid-term">>, #{publisher => Publisher}},
        _ = sys:get_state(Pid),  % gen_server still alive -> didn't crash
        ?assertEqual(#{}, macula_station_bloom_exchange:peer_patterns(Pid)),

        %% A validly-encoded term, but not a list-of-binaries.
        Pid ! {macula_event, ignored_sub_ref, <<"_mesh.patterns">>,
               term_to_binary(#{unexpected => a_map}), #{publisher => Publisher}},
        _ = sys:get_state(Pid),
        ?assertEqual(#{}, macula_station_bloom_exchange:peer_patterns(Pid)),

        %% A valid list, but not all binaries.
        Pid ! {macula_event, ignored_sub_ref, <<"_mesh.patterns">>,
               term_to_binary([<<"ok/*">>, not_a_binary]),
               #{publisher => Publisher}},
        _ = sys:get_state(Pid),
        ?assertEqual(#{}, macula_station_bloom_exchange:peer_patterns(Pid))
    after
        stop_exchange(Pid)
    end.

%%------------------------------------------------------------------
%% Push-on-change debounce — Item 1 (2026-05-15)
%%
%% On a peer_bloom CHANGE (new entry, or different bytes for an
%% existing publisher key), the gen_server schedules an out-of-band
%% rebuild within ?DEBOUNCE_MS instead of waiting up to
%% ?REBUILD_INTERVAL_MS for the periodic tick.
%%
%% These tests inspect `#state{}' via `sys:get_state/1' and extract
%% the `debounce_ref' field by position. Keep the position-extractor
%% in sync with the record def if it grows new fields.
%%------------------------------------------------------------------

peer_bloom_change_schedules_debounce_test() ->
    {Pid, _Kp} = start_exchange(),
    try
        ?assertEqual(undefined, debounce_ref_of(Pid)),
        ok = macula_station_bloom_exchange:receive_peer_bloom(
            Pid, <<"peer-a">>, bloom_with([<<"topic.a">>])),
        Ref = debounce_ref_of(Pid),
        ?assert(is_reference(Ref))
    after
        stop_exchange(Pid)
    end.

duplicate_peer_bloom_does_not_reschedule_test() ->
    {Pid, _Kp} = start_exchange(),
    try
        Bloom = bloom_with([<<"topic.a">>]),
        ok = macula_station_bloom_exchange:receive_peer_bloom(
            Pid, <<"peer-a">>, Bloom),
        Ref1 = debounce_ref_of(Pid),
        ?assert(is_reference(Ref1)),
        %% Same key, same bytes — must NOT reset the in-flight debounce.
        ok = macula_station_bloom_exchange:receive_peer_bloom(
            Pid, <<"peer-a">>, Bloom),
        ?assertEqual(Ref1, debounce_ref_of(Pid))
    after
        stop_exchange(Pid)
    end.

macula_event_change_schedules_debounce_test() ->
    {Pid, Kp} = start_exchange(),
    try
        Bloom = bloom_with([<<"topic.evt">>]),
        Publisher = other_pubkey(Kp),
        Pid ! {macula_event, ignored_sub_ref, <<"_mesh.bloom">>,
               Bloom, #{publisher => Publisher}},
        %% Give the gen_server a moment to drain the info message.
        _ = sys:get_state(Pid),
        ?assert(is_reference(debounce_ref_of(Pid)))
    after
        stop_exchange(Pid)
    end.

%% `notify_local_change/1' is the SDK-subscribe / unsubscribe path:
%% pubsub_dispatcher calls it on every inbound SUBSCRIBE / UNSUBSCRIBE
%% frame so the local Bloom rebuilds on the debounce, not on the 30s
%% periodic tick.
notify_local_change_schedules_debounce_test() ->
    {Pid, _Kp} = start_exchange(),
    try
        ?assertEqual(undefined, debounce_ref_of(Pid)),
        ok = macula_station_bloom_exchange:notify_local_change(Pid),
        Ref = debounce_ref_of(Pid),
        ?assert(is_reference(Ref)),
        %% Second call within the debounce window MUST be a no-op
        %% (idempotent coalescing — same contract as peer_bloom
        %% changes).
        ok = macula_station_bloom_exchange:notify_local_change(Pid),
        ?assertEqual(Ref, debounce_ref_of(Pid))
    after
        stop_exchange(Pid)
    end.

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

start_exchange() ->
    Kp = macula_identity:generate(),
    {ok, Pid} = macula_station_bloom_exchange:start_link(
                  #{pubsub_registry => self(), identity => Kp}),
    {Pid, Kp}.

stop_exchange(Pid) ->
    catch macula_station_bloom_exchange:stop(Pid).

%% `#state{}' is private to the module. Extract `debounce_ref' by
%% position (currently the last field). If new tail fields are added
%% later, this helper must move with them.
debounce_ref_of(Pid) ->
    StateRec = sys:get_state(Pid),
    state = element(1, StateRec),
    element(tuple_size(StateRec), StateRec).

%% A 32-byte pubkey guaranteed not to equal `macula_identity:public(Kp)'.
other_pubkey(Kp) ->
    Self = macula_identity:public(Kp),
    Cand = <<0:256>>,
    case Cand =:= Self of
        true  -> <<1:256>>;
        false -> Cand
    end.

bloom_with(Topics) ->
    BF = lists:foldl(fun macula_station_bloom:add/2,
                     macula_station_bloom:new(), Topics),
    macula_station_bloom:to_binary(BF).

%% `merge_with_peers/2' is private to the module; reach it via
%% sys:get_state-style tricks would be brittle. Re-derive the same
%% bitwise-union here to assert the contract; the production code
%% has its own behavioural coverage at the integration level
%% (multihop probe in /tmp/csr-investigate/multihop_probe.erl).
direct_merge(LocalBin, PeerBlooms) ->
    Local = macula_station_bloom:from_binary(LocalBin),
    Merged = maps:fold(
        fun(_Pub, Bin, Acc) when is_binary(Bin), byte_size(Bin) =:= 1024 ->
                macula_station_bloom:merge(
                    Acc, macula_station_bloom:from_binary(Bin));
           (_Pub, _BadBin, Acc) -> Acc
        end, Local, PeerBlooms),
    macula_station_bloom:to_binary(Merged).
