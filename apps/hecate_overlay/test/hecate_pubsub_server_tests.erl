%% EUnit tests for hecate_pubsub_server.
-module(hecate_pubsub_server_tests).

-include_lib("eunit/include/eunit.hrl").

%%---------------------------------------------------------------------
%% Setup helpers
%%---------------------------------------------------------------------

realm()   -> crypto:strong_rand_bytes(32).
id(N)     -> <<N:256>>.
keypair() -> macula_identity:generate().

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
    ?assertEqual(event, macula_frame:frame_type(Frame)),
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
    Pub = macula_identity:public(Kp),
    {ok, Pid} = hecate_pubsub_server:start_link(
                  #{realm => R, identity => Kp}),
    ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, id(1)),
    {Frame, _} = hecate_pubsub_server:publish(Pid, <<"t">>, <<"hello">>),
    ?assertMatch({ok, _}, macula_frame:verify(Frame, Pub)),
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
    External = macula_frame:sign(macula_frame:event(#{
        topic         => <<"t">>,
        realm         => R,
        publisher     => macula_identity:public(Kp),
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
    External = macula_frame:sign(macula_frame:event(#{
        topic         => <<"t">>,
        realm         => R2,
        publisher     => macula_identity:public(Kp),
        seq           => 1,
        payload       => <<"x">>,
        delivered_via => plumtree
    }), Kp),
    ?assertEqual([], hecate_pubsub_server:deliver_event(Pid, External)),
    hecate_pubsub_server:stop(Pid).

%%---------------------------------------------------------------------
%% relay_event — per-hop re-sign + dedup
%%---------------------------------------------------------------------

relay_event_re_signs_with_server_identity_test() ->
    R           = realm(),
    SelfKp      = keypair(),
    SelfPub     = macula_identity:public(SelfKp),
    UpstreamKp  = keypair(),
    UpstreamPub = macula_identity:public(UpstreamKp),
    OriginPub   = id(7),
    {ok, Pid} = hecate_pubsub_server:start_link(
                  #{realm => R, identity => SelfKp}),
    ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, id(1)),
    Inbound = macula_frame:sign(macula_frame:event(#{
        topic         => <<"t">>,
        realm         => R,
        publisher     => OriginPub,
        seq           => 99,
        payload       => <<"x">>,
        delivered_via => plumtree
    }), UpstreamKp),
    {ReSigned, Matched} = hecate_pubsub_server:relay_event(Pid, Inbound),
    ?assertEqual([id(1)], Matched),
    ?assertMatch({ok, _}, macula_frame:verify(ReSigned, SelfPub)),
    ?assertMatch({error, signature_invalid},
                 macula_frame:verify(ReSigned, UpstreamPub)),
    ?assertEqual(OriginPub, maps:get(publisher,     ReSigned)),
    ?assertEqual(99,        maps:get(seq,           ReSigned)),
    ?assertEqual(plumtree,  maps:get(delivered_via, ReSigned)),
    hecate_pubsub_server:stop(Pid).

relay_event_dedups_on_publisher_seq_test() ->
    R   = realm(),
    Kp  = keypair(),
    {ok, Pid} = hecate_pubsub_server:start_link(
                  #{realm => R, identity => Kp}),
    ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, id(1)),
    Frame = macula_frame:sign(macula_frame:event(#{
        topic         => <<"t">>,
        realm         => R,
        publisher     => id(42),
        seq           => 7,
        payload       => <<"x">>,
        delivered_via => direct
    }), keypair()),
    %% First arrival: process normally.
    ?assertMatch({_ReSigned, [_]}, hecate_pubsub_server:relay_event(Pid, Frame)),
    %% Second arrival of same {publisher, seq}: dropped as duplicate.
    ?assertEqual({error, duplicate}, hecate_pubsub_server:relay_event(Pid, Frame)),
    %% Different seq for same publisher: not a duplicate.
    Frame2 = macula_frame:sign(macula_frame:event(#{
        topic         => <<"t">>,
        realm         => R,
        publisher     => id(42),
        seq           => 8,
        payload       => <<"y">>,
        delivered_via => direct
    }), keypair()),
    ?assertMatch({_ReSigned, [_]}, hecate_pubsub_server:relay_event(Pid, Frame2)),
    hecate_pubsub_server:stop(Pid).

relay_publish_dedups_on_publisher_seq_test() ->
    R = realm(),
    {ok, Pid} = hecate_pubsub_server:start_link(
                  #{realm => R, identity => keypair()}),
    ok = hecate_pubsub_server:subscribe(Pid, <<"t">>, id(1)),
    Frame = macula_frame:sign(macula_frame:publish(#{
        topic           => <<"t">>,
        realm           => R,
        publisher       => id(42),
        seq             => 0,
        payload         => <<"x">>,
        published_at_ms => 0
    }), keypair()),
    ?assertMatch({_EventFrame, [_]}, hecate_pubsub_server:relay_publish(Pid, Frame)),
    ?assertEqual({error, duplicate},
                 hecate_pubsub_server:relay_publish(Pid, Frame)),
    hecate_pubsub_server:stop(Pid).

relay_event_dedup_evicts_after_ttl_test() ->
    R = realm(),
    {ok, Pid} = hecate_pubsub_server:start_link(
                  #{realm => R,
                    identity => keypair(),
                    seen_ttl_ms => 50}),
    Frame = macula_frame:sign(macula_frame:event(#{
        topic         => <<"t">>,
        realm         => R,
        publisher     => id(42),
        seq           => 1,
        payload       => <<"x">>,
        delivered_via => direct
    }), keypair()),
    ?assertMatch({_, _}, hecate_pubsub_server:relay_event(Pid, Frame)),
    ?assertEqual({error, duplicate}, hecate_pubsub_server:relay_event(Pid, Frame)),
    %% Drive a manual sweep; the periodic timer is 30s so we trigger
    %% it directly to keep the test fast.
    timer:sleep(80),
    Pid ! seen_sweep,
    timer:sleep(20),
    %% After eviction, the same frame is fresh again.
    ?assertMatch({_, _}, hecate_pubsub_server:relay_event(Pid, Frame)),
    hecate_pubsub_server:stop(Pid).

relay_event_for_other_realm_returns_error_test() ->
    R1 = realm(),
    R2 = realm(),
    {ok, Pid} = hecate_pubsub_server:start_link(
                  #{realm => R1, identity => keypair()}),
    Inbound = macula_frame:sign(macula_frame:event(#{
        topic         => <<"t">>,
        realm         => R2,
        publisher     => id(7),
        seq           => 1,
        payload       => <<"x">>,
        delivered_via => direct
    }), keypair()),
    ?assertEqual({error, realm_mismatch},
                 hecate_pubsub_server:relay_event(Pid, Inbound)),
    hecate_pubsub_server:stop(Pid).
