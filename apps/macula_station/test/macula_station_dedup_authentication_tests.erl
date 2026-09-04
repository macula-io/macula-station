%%% @doc Only an ATTESTED publisher may touch the (publisher, seq) dedup cache.
%%%
%%% The attack this closes. The cache was keyed off the raw `publisher' and
%%% `seq' fields of any verified frame. For an EVENT carrying no
%%% `publisher_sig', `verify_pubsub/2' checks the frame signature against the
%%% SENDING peer's node id and never compares `publisher' to it. So a peer
%%% could name a victim's pubkey with a chosen `seq', sign the frame with its
%%% own key, pass verification, and insert that pair. The victim's genuine
%%% publisher-signed EVENT then arrived, found the pair present, and was
%%% dropped as a duplicate.
%%%
%%% Sequence numbers are trivially predictable: `macula_client' seeds
%%% `publish_seq' from `erlang:system_time(microsecond)' and increments by one,
%%% so one observed event yields every future key for that publisher. That made
%%% it a per-publisher, per-topic mute button lasting the full 300s dedup
%%% window, at a cost of one frame per message suppressed.
%%%
%%% A PUBLISH is a separate case: its `publisher_sig' is NOT verified on that
%%% path, so the only publisher the station can attest to is the connected
%%% daemon itself.
-module(macula_station_dedup_authentication_tests).

-include_lib("eunit/include/eunit.hrl").

-define(VICTIM,   <<9:256>>).
-define(ATTACKER, <<66:256>>).
-define(TOPIC,       <<"io.macula/test/dedup_auth">>).
-define(OTHER_TOPIC, <<"io.macula/test/dedup_auth/other">>).

%%====================================================================
%% Fixture
%%====================================================================

dedup_authentication_test_() ->
    {foreach,
     fun setup/0,
     fun teardown/1,
     [
      fun signed_event_is_deduped/1,
      fun unsigned_event_never_writes_the_cache/1,
      fun unsigned_event_cannot_suppress_a_signed_one/1,
      fun unsigned_event_is_always_delivered/1,
      fun origin_seeds_only_for_the_connected_daemon/1,
      fun origin_refuses_a_third_party_publisher/1,
      fun same_seq_different_topic_is_not_deduped/1,
      fun same_seq_same_topic_is_still_deduped/1,
      fun origin_seed_on_one_topic_does_not_suppress_another/1
     ]}.

setup() ->
    macula_station_route_pubsub_frames:install_counters(),
    {ok, Pid} = macula_station_event_dedup:start_link(),
    Pid.

teardown(Pid) ->
    gen_server:stop(Pid),
    ok.

%%====================================================================
%% EVENT path
%%====================================================================

signed_event_is_deduped(_) ->
    fun() ->
        Seq = 1,
        ?assertEqual(deliver, disposition(signed_event(?VICTIM, Seq))),
        ?assertEqual(drop,    disposition(signed_event(?VICTIM, Seq)))
    end.

%% The write is what the attack needs; deny it.
unsigned_event_never_writes_the_cache(_) ->
    fun() ->
        Seq = 2,
        ?assertEqual(absent, peek(?VICTIM, Seq)),
        _ = disposition(unsigned_event(?VICTIM, Seq)),
        ?assertEqual(absent, peek(?VICTIM, Seq))
    end.

%% The attack, end to end at this layer. Pre-fix the second assertion was
%% `drop' and the victim's event died.
unsigned_event_cannot_suppress_a_signed_one(_) ->
    fun() ->
        Seq = 3,
        %% Attacker predicts the victim's next seq and claims it.
        _ = disposition(unsigned_event(?VICTIM, Seq)),
        %% Victim's genuine, publisher-signed event for the same key.
        ?assertEqual(deliver, disposition(signed_event(?VICTIM, Seq)))
    end.

unsigned_event_is_always_delivered(_) ->
    fun() ->
        Before = stats(),
        ?assertEqual(deliver, disposition(unsigned_event(?VICTIM, 4))),
        ?assertEqual(deliver, disposition(unsigned_event(?VICTIM, 4))),
        ?assertEqual(2, delta(unauth_event, Before, stats()))
    end.

%%====================================================================
%% PUBLISH origin seeding
%%====================================================================

origin_seeds_only_for_the_connected_daemon(_) ->
    fun() ->
        Seq = 10,
        ok = macula_station_route_pubsub_frames:record_origin_seq(
               publish(?VICTIM, Seq), ?VICTIM),
        ?assertEqual(seen, peek(?VICTIM, Seq))
    end.

origin_refuses_a_third_party_publisher(_) ->
    fun() ->
        Seq = 11,
        Before = stats(),
        %% Attacker's connection, naming the victim as publisher.
        ok = macula_station_route_pubsub_frames:record_origin_seq(
               publish(?VICTIM, Seq), ?ATTACKER),
        ?assertEqual(absent, peek(?VICTIM, Seq)),
        ?assertEqual(1, delta(unauth_origin, Before, stats()))
    end.

%%====================================================================
%% Topic scoping (2026-09 fix) -- the actual bug: a bare (publisher,
%% seq) key can't tell a genuinely new publish on a different topic
%% apart from a repeat of an earlier one, when both share a seq value.
%% Real-world trigger: macula-go's Session.Call auto-publishes
%% rpc.sent_v1/rpc.completed_v1 under the caller's own identity from a
%% process-wide counter starting at 1, independent of whatever counter
%% an application uses for its own business publishes -- the first
%% business publish on a fresh process, seeded seq=1 (macula-go's own
%% documented/tested convention), collided with that fact's key and
%% was silently dropped as a "duplicate": a real message lost, not a
%% loop. Live-verified against the production fleet: identical in
%% every way except the seq value, seq=1 missed 3/3 fresh-process
%% trials, a wall-clock-unique seq delivered 3/3.
%%====================================================================

%% The regression test for the bug itself, through the real call path
%% (event_dedup_disposition/1, not the dedup module directly): same
%% publisher, same seq, two different topics -- an rpc.sent_v1-shaped
%% EVENT followed by a business EVENT on an unrelated topic. Pre-fix
%% the second `disposition/1' here returned `drop'.
same_seq_different_topic_is_not_deduped(_) ->
    fun() ->
        Seq = 1,
        ?assertEqual(deliver, disposition(signed_event(?VICTIM, Seq, <<"rpc.sent_v1">>))),
        ?assertEqual(deliver, disposition(signed_event(?VICTIM, Seq, ?OTHER_TOPIC)))
    end.

%% Loop-kill is not weakened by topic-scoping: the SAME (publisher,
%% seq, topic) triple repeated is still a duplicate, exactly like
%% `signed_event_is_deduped/1' already covers for the default topic --
%% pinned again here explicitly alongside the cross-topic case above,
%% so the two properties are asserted side by side.
same_seq_same_topic_is_still_deduped(_) ->
    fun() ->
        Seq = 12,
        ?assertEqual(deliver, disposition(signed_event(?VICTIM, Seq, ?OTHER_TOPIC))),
        ?assertEqual(drop,    disposition(signed_event(?VICTIM, Seq, ?OTHER_TOPIC)))
    end.

%% Same property as the EVENT path, exercised through the PUBLISH
%% origin-seeding path instead: seeding the cache for a publish on one
%% topic must not make a LOOPED-BACK event for a different topic (same
%% publisher, same seq) look like a duplicate.
origin_seed_on_one_topic_does_not_suppress_another(_) ->
    fun() ->
        Seq = 13,
        ok = macula_station_route_pubsub_frames:record_origin_seq(
               publish(?VICTIM, Seq, ?TOPIC), ?VICTIM),
        ?assertEqual(seen,   peek(?VICTIM, Seq, ?TOPIC)),
        ?assertEqual(absent, peek(?VICTIM, Seq, ?OTHER_TOPIC)),
        ?assertEqual(deliver, disposition(signed_event(?VICTIM, Seq, ?OTHER_TOPIC)))
    end.

%%====================================================================
%% Helpers
%%====================================================================

disposition(Frame) ->
    macula_station_route_pubsub_frames:event_dedup_disposition(Frame).

%% `event_dedup_disposition/1' and `record_origin_seq/2' both now key
%% via the arity-3 (publisher, seq, topic) form -- see
%% `macula_station_event_dedup:seen_or_record/3''s own doc for why a
%% bare (publisher, seq) key was too coarse. Every frame helper below
%% uses the same fixed `?TOPIC', so this helper just needs to query
%% the matching arity-3 key to see what those calls actually wrote.
peek(Pub, Seq) ->
    peek(Pub, Seq, ?TOPIC).

peek(Pub, Seq, Topic) ->
    macula_station_event_dedup:peek(Pub, Seq, Topic).

stats() ->
    macula_station_route_pubsub_frames:delivery_stats().

delta(Key, Before, After) ->
    maps:get(Key, After) - maps:get(Key, Before).

%% Frames reach this code already verified, so the fields are what matter,
%% not real signatures. Presence of `publisher_sig' is exactly what
%% `verify_pubsub/2' has already checked against the publisher's key.
signed_event(Pub, Seq) ->
    signed_event(Pub, Seq, ?TOPIC).

signed_event(Pub, Seq, Topic) ->
    (unsigned_event(Pub, Seq, Topic))#{publisher_sig => <<0:512>>}.

unsigned_event(Pub, Seq) ->
    unsigned_event(Pub, Seq, ?TOPIC).

unsigned_event(Pub, Seq, Topic) ->
    #{frame_type => event,
      publisher  => Pub,
      seq        => Seq,
      topic      => Topic}.

publish(Pub, Seq) ->
    publish(Pub, Seq, ?TOPIC).

publish(Pub, Seq, Topic) ->
    #{frame_type => publish,
      publisher  => Pub,
      seq        => Seq,
      topic      => Topic}.
