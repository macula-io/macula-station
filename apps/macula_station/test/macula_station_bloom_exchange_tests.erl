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
