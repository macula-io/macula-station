%% EUnit tests for hecate_pubsub_server.
-module(hecate_pubsub_server_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Setup helpers
%%---------------------------------------------------------------------

realm()   -> crypto:strong_rand_bytes(32).
id(N)     -> <<N:256>>.
keypair() -> hecate_identity:generate().

start() ->
    {ok, Pid} = hecate_pubsub_server:start_link(
                  #{realm => realm(), identity => keypair()}),
    Pid.

stop_(Pid) ->
    hecate_pubsub_server:stop(Pid).

%%---------------------------------------------------------------------
%% Construction + inspection
%%---------------------------------------------------------------------

start_link_creates_empty_state_test() ->
    Pid = start(),
    ?assertEqual(0, hecate_pubsub_server:topic_count(Pid)),
    ?assertEqual([], hecate_pubsub_server:topics(Pid)),
    ?assertEqual(0, hecate_pubsub_server:subscriber_count(Pid)),
    stop_(Pid).

realm_returns_configured_realm_test() ->
    R = realm(),
    {ok, Pid} = hecate_pubsub_server:start_link(
                  #{realm => R, identity => keypair()}),
    ?assertEqual(R, hecate_pubsub_server:realm(Pid)),
    hecate_pubsub_server:stop(Pid).

%%---------------------------------------------------------------------
%% Subscribe / unsubscribe
%%---------------------------------------------------------------------

subscribe_records_subscriber_test() ->
    Pid = start(),
    Sub = id(1),
    ok = hecate_pubsub_server:subscribe(Pid, <<"news">>, Sub),
    ?assert(hecate_pubsub_server:is_subscribed(Pid, <<"news">>, Sub)),
    ?assertEqual([Sub], hecate_pubsub_server:subscribers(Pid, <<"news">>)),
    ?assertEqual([<<"news">>], hecate_pubsub_server:topics(Pid)),
    ?assertEqual(1, hecate_pubsub_server:subscriber_count(Pid)),
    stop_(Pid).

subscribe_idempotent_test() ->
    Pid = start(),
    Sub = id(1),
    ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, Sub),
    ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, Sub),
    ?assertEqual(1, hecate_pubsub_server:subscriber_count(Pid)),
    stop_(Pid).

multiple_subscribers_per_topic_test() ->
    Pid = start(),
    [ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, id(N))
     || N <- [1, 2, 3]],
    ?assertEqual(3, hecate_pubsub_server:subscriber_count(Pid)),
    ?assertEqual(3, length(hecate_pubsub_server:subscribers(Pid, <<"t">>))),
    stop_(Pid).

unsubscribe_drops_subscriber_test() ->
    Pid = start(),
    Sub = id(2),
    ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, Sub),
    ok = hecate_pubsub_server:unsubscribe(Pid, <<"t">>, Sub),
    ?assertNot(hecate_pubsub_server:is_subscribed(Pid, <<"t">>, Sub)),
    ?assertEqual(0, hecate_pubsub_server:subscriber_count(Pid)),
    stop_(Pid).

unsubscribe_unknown_topic_is_noop_test() ->
    Pid = start(),
    ok = hecate_pubsub_server:unsubscribe(Pid, <<"nope">>, id(1)),
    ?assertEqual(0, hecate_pubsub_server:topic_count(Pid)),
    stop_(Pid).

%%---------------------------------------------------------------------
%% Publish
%%---------------------------------------------------------------------

publish_returns_local_matched_subscribers_test() ->
    Pid = start(),
    [ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, id(N))
     || N <- [1, 2]],
    {Frame, Matched} = hecate_pubsub_server:publish(Pid, <<"t">>, <<"hello">>),
    ?assertEqual(event, hecate_frame:frame_type(Frame)),
    ?assertEqual(lists:sort([id(1), id(2)]), lists:sort(Matched)),
    stop_(Pid).

publish_with_no_subscribers_returns_empty_match_test() ->
    Pid = start(),
    {_Frame, Matched} = hecate_pubsub_server:publish(Pid, <<"empty">>, <<"x">>),
    ?assertEqual([], Matched),
    stop_(Pid).

publish_increments_seq_test() ->
    Pid = start(),
    ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, id(1)),
    {F1, _} = hecate_pubsub_server:publish(Pid, <<"t">>, <<"a">>),
    {F2, _} = hecate_pubsub_server:publish(Pid, <<"t">>, <<"b">>),
    ?assertNotEqual(maps:get(seq, F1), maps:get(seq, F2)),
    stop_(Pid).

publish_signs_with_server_identity_test() ->
    R   = realm(),
    Kp  = keypair(),
    Pub = hecate_identity:public(Kp),
    {ok, Pid} = hecate_pubsub_server:start_link(
                  #{realm => R, identity => Kp}),
    ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, id(1)),
    {Frame, _} = hecate_pubsub_server:publish(Pid, <<"t">>, <<"hello">>),
    ?assertMatch({ok, _}, hecate_frame:verify(Frame, Pub)),
    hecate_pubsub_server:stop(Pid).

%%---------------------------------------------------------------------
%% deliver_event for inbound frames
%%---------------------------------------------------------------------

deliver_event_returns_subscribers_test() ->
    R = realm(),
    Kp = keypair(),
    {ok, Pid} = hecate_pubsub_server:start_link(
                  #{realm => R, identity => Kp}),
    ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, id(1)),
    %% Build an event frame externally with the same realm.
    External = hecate_frame:sign(hecate_frame:event(#{
        topic         => <<"t">>,
        realm         => R,
        publisher     => hecate_identity:public(Kp),
        seq           => 42,
        payload       => <<"x">>,
        delivered_via => plumtree
    }), Kp),
    Matched = hecate_pubsub_server:deliver_event(Pid, External),
    ?assertEqual([id(1)], Matched),
    hecate_pubsub_server:stop(Pid).

deliver_event_for_other_realm_returns_empty_test() ->
    R1 = realm(),
    R2 = realm(),
    Kp = keypair(),
    {ok, Pid} = hecate_pubsub_server:start_link(
                  #{realm => R1, identity => Kp}),
    ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, id(1)),
    %% Event frame tagged with a different realm — must NOT deliver.
    External = hecate_frame:sign(hecate_frame:event(#{
        topic         => <<"t">>,
        realm         => R2,
        publisher     => hecate_identity:public(Kp),
        seq           => 1,
        payload       => <<"x">>,
        delivered_via => plumtree
    }), Kp),
    ?assertEqual([], hecate_pubsub_server:deliver_event(Pid, External)),
    hecate_pubsub_server:stop(Pid).
