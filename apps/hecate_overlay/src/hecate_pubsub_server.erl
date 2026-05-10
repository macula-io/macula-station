%% @doc PubSub gen_server wrapping `hecate_pubsub' state for one realm
%% namespace.
%%
%% Activates the dormant `hecate_pubsub' pure-state module by giving
%% it a process identity. One server instance owns the
%% topic-to-subscriber index for a single realm tag; the realm is an
%% opaque 32-byte namespace key, not validated against any authority.
%% Multi-tenancy comes from running multiple servers under different
%% realm tags — the station does not arbitrate which realm tags are
%% "real" (Sprint A: realm identity lives outside infrastructure).
%%
%% == Phase 1 scope (this commit) ==
%%
%% State mutations + frame processing only. The publish path builds
%% the signed EVENT frame and returns the matched LOCAL subscribers,
%% but does NOT fan out across the cluster — that requires the
%% Plumtree wire layer (`hecate_plumtree') and the DHT topic-discovery
%% integration which land in subsequent commits.
%%
%% == Sequencing ==
%%
%% <ul>
%%   <li>This commit: server in isolation, no integration with
%%       station listener or DHT.</li>
%%   <li>Next: per-realm-namespace registry under
%%       `hecate_overlay_sup' so the listener can route inbound
%%       SUBSCRIBE / UNSUBSCRIBE / EVENT frames to the right
%%       server.</li>
%%   <li>Then: Plumtree fan-out for cross-station delivery.</li>
%%   <li>Then: DHT integration for topic-mesh discovery.</li>
%% </ul>
-module(hecate_pubsub_server).
-behaviour(gen_server).

-export([
    start_link/1,
    subscribe/3, unsubscribe/3, is_subscribed/3,
    subscribers/2, topics/1, topic_count/1, subscriber_count/1,
    realm/1,
    publish/3, deliver_event/2, process_frame/3,
    relay_publish/2, relay_event/2,
    stop/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([opts/0]).

%% TTL on the seen-message dedup cache. Sized larger than any
%% realistic propagation across the partial mesh (3 outbound peers,
%% 3-4 hops max) so loop-back duplicates always hit a live entry.
-define(DEFAULT_SEEN_TTL_MS, 60_000).
-define(SEEN_SWEEP_INTERVAL_MS, 30_000).

-type opts() :: #{
    realm        := <<_:256>>,
    identity     := macula_identity:key_pair(),
    seen_ttl_ms  => non_neg_integer()
}.

-record(state, {
    realm        :: <<_:256>>,
    identity     :: macula_identity:key_pair(),
    self_id      :: <<_:256>>,
    pubsub       :: hecate_pubsub:state(),
    next_seq     :: non_neg_integer(),
    %% Seen-message dedup cache used only by `relay_event/2'. Keyed
    %% by `{Publisher, Seq}' (preserved end-to-end across per-hop
    %% re-sign). Required for the bloom-mesh `_mesh.bloom' relay
    %% path: mutual cross-station SUBSCRIBEs would otherwise let
    %% re-signed EVENTs loop forever (see
    %% project_pubsub_resign_loop_lesson). Caller (peer_observer)
    %% restricts `relay_event/2' to the mesh realm so user-realm
    %% pubsub keeps the cheaper `deliver_event/2' path.
    seen         :: ets:tid(),
    seen_ttl_ms  :: non_neg_integer()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{realm := <<_:256>>, identity := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec subscribe(pid(), binary(), <<_:256>>) -> ok.
subscribe(Pid, Topic, Sub) ->
    gen_server:call(Pid, {subscribe, Topic, Sub}).

-spec unsubscribe(pid(), binary(), <<_:256>>) -> ok.
unsubscribe(Pid, Topic, Sub) ->
    gen_server:call(Pid, {unsubscribe, Topic, Sub}).

-spec is_subscribed(pid(), binary(), <<_:256>>) -> boolean().
is_subscribed(Pid, Topic, Sub) ->
    gen_server:call(Pid, {is_subscribed, Topic, Sub}).

-spec subscribers(pid(), binary()) -> [<<_:256>>].
subscribers(Pid, Topic) ->
    gen_server:call(Pid, {subscribers, Topic}).

-spec topics(pid()) -> [binary()].
topics(Pid) ->
    gen_server:call(Pid, topics).

-spec topic_count(pid()) -> non_neg_integer().
topic_count(Pid) ->
    gen_server:call(Pid, topic_count).

-spec subscriber_count(pid()) -> non_neg_integer().
subscriber_count(Pid) ->
    gen_server:call(Pid, subscriber_count).

-spec realm(pid()) -> <<_:256>>.
realm(Pid) ->
    gen_server:call(Pid, realm).

%% @doc Build a signed EVENT frame for `Topic'/`Payload' and return it
%% together with the set of LOCAL subscribers that match. The caller
%% is responsible for handing the frame to the cross-station delivery
%% layer (Plumtree, future commit) and for delivering to the matched
%% local subscribers via the application channel.
-spec publish(pid(), binary(), binary()) ->
        {macula_frame:frame(), [<<_:256>>]}.
publish(Pid, Topic, Payload) ->
    gen_server:call(Pid, {publish, Topic, Payload}).

%% @doc Process an inbound EVENT frame received from the wire. Returns
%% the matched local subscribers; the caller delivers.
-spec deliver_event(pid(), macula_frame:frame()) -> [<<_:256>>].
deliver_event(Pid, Frame) ->
    gen_server:call(Pid, {deliver_event, Frame}).

%% @doc Generic frame dispatch — handles subscribe / unsubscribe /
%% event uniformly. Returns the matched subscribers for event frames,
%% empty list for subscribe / unsubscribe.
-spec process_frame(pid(), <<_:256>>, macula_frame:frame()) -> [<<_:256>>].
process_frame(Pid, From, Frame) ->
    gen_server:call(Pid, {process_frame, From, Frame}).

%% @doc Relay an inbound PUBLISH frame from a remote daemon. The
%% server builds an EVENT frame signed by THIS server's identity
%% (intermediate hop re-signing — Phase 1 simplification; Phase 2
%% tightens to publisher-end-to-end auth via UCAN), preserves the
%% original publisher pubkey + seq inside the EVENT, and returns
%% the matched local subscribers. The caller (typically the
%% peer observer) is responsible for sending `EventFrame' on each
%% subscriber's peering connection.
%%
%% Returns `{error, realm_mismatch}' when the publish frame's realm
%% does not match this server's realm. The registry routes by realm
%% so this should never fire in practice — defensive check.
-spec relay_publish(pid(), macula_frame:frame()) ->
        {macula_frame:frame(), [<<_:256>>]} | {error, realm_mismatch}.
relay_publish(Pid, Frame) ->
    gen_server:call(Pid, {relay_publish, Frame}).

%% @doc Relay an inbound EVENT frame received from a peer station.
%% Re-signs with this server's identity (preserving the original
%% `publisher' / `seq' / `payload' / `delivered_via') and dedupes on
%% `{publisher, seq}' before fanning out. Returns `{error, duplicate}'
%% on cache hit.
%%
%% Per-hop re-sign keeps `peer_observer''s NodeId-based verify valid
%% across multi-hop relay chains; dedup terminates the bloom-mesh
%% mutual-SUBSCRIBE loop that the per-hop re-sign would otherwise
%% feed indefinitely (see project_pubsub_resign_loop_lesson).
%%
%% Callers should restrict this path to the mesh realm; user-realm
%% pubsub keeps the cheaper `deliver_event/2' path because the loop
%% topology only exists for the bloom subscribers.
-spec relay_event(pid(), macula_frame:frame()) ->
        {macula_frame:frame(), [<<_:256>>]}
        | {error, realm_mismatch | duplicate}.
relay_event(Pid, Frame) ->
    gen_server:call(Pid, {relay_event, Frame}).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(#{realm := Realm, identity := Kp} = Opts) ->
    Seen = ets:new(seen_messages, [set, private]),
    erlang:send_after(?SEEN_SWEEP_INTERVAL_MS, self(), seen_sweep),
    {ok, #state{
        realm       = Realm,
        identity    = Kp,
        self_id     = macula_identity:public(Kp),
        pubsub      = hecate_pubsub:new(Realm),
        next_seq    = 0,
        seen        = Seen,
        seen_ttl_ms = maps:get(seen_ttl_ms, Opts, ?DEFAULT_SEEN_TTL_MS)
    }}.

handle_call({subscribe, Topic, Sub}, _From, S) ->
    PS2 = hecate_pubsub:subscribe(S#state.pubsub, Topic, Sub),
    {reply, ok, S#state{pubsub = PS2}};
handle_call({unsubscribe, Topic, Sub}, _From, S) ->
    PS2 = hecate_pubsub:unsubscribe(S#state.pubsub, Topic, Sub),
    {reply, ok, S#state{pubsub = PS2}};
handle_call({is_subscribed, Topic, Sub}, _From, S) ->
    {reply, hecate_pubsub:is_subscribed(S#state.pubsub, Topic, Sub), S};
handle_call({subscribers, Topic}, _From, S) ->
    {reply, hecate_pubsub:subscribers(S#state.pubsub, Topic), S};
handle_call(topics, _From, S) ->
    {reply, hecate_pubsub:topics(S#state.pubsub), S};
handle_call(topic_count, _From, S) ->
    {reply, hecate_pubsub:topic_count(S#state.pubsub), S};
handle_call(subscriber_count, _From, S) ->
    {reply, hecate_pubsub:subscriber_count(S#state.pubsub), S};
handle_call(realm, _From, S) ->
    {reply, S#state.realm, S};
handle_call({publish, Topic, Payload}, _From, S) ->
    Spec = #{topic           => Topic,
             realm           => S#state.realm,
             publisher       => S#state.self_id,
             seq             => S#state.next_seq,
             payload         => Payload,
             published_at_ms => erlang:system_time(millisecond)},
    Frame   = hecate_pubsub:build_event(S#state.pubsub, Spec, S#state.identity),
    Matched = hecate_pubsub:deliver_event(S#state.pubsub, Frame),
    {reply, {Frame, Matched}, S#state{next_seq = S#state.next_seq + 1}};
handle_call({deliver_event, Frame}, _From, S) ->
    {reply, hecate_pubsub:deliver_event(S#state.pubsub, Frame), S};
handle_call({process_frame, From, Frame}, _From, S) ->
    {PS2, Subs} = hecate_pubsub:process(S#state.pubsub, From, Frame),
    {reply, Subs, S#state{pubsub = PS2}};
handle_call({relay_publish, Frame}, _From, S) ->
    {reply, do_relay_publish(Frame, S), S};
handle_call({relay_event, Frame}, _From, S) ->
    {reply, do_relay_event(Frame, S), S};
handle_call(_Request, _From, S) ->
    {reply, {error, unknown_call}, S}.

%%====================================================================
%% Internals — publish relay
%%====================================================================

do_relay_publish(#{frame_type := publish, realm := R} = Frame,
                 #state{realm = R} = S) ->
    EventFrame = build_relay_event(Frame, S),
    Matched    = hecate_pubsub:deliver_event(S#state.pubsub, EventFrame),
    {EventFrame, Matched};
do_relay_publish(_Frame, _S) ->
    {error, realm_mismatch}.

build_relay_event(#{topic := T, realm := R, publisher := Pub,
                    seq := Seq, payload := Pl},
                  #state{identity = Id}) ->
    macula_frame:sign(macula_frame:event(#{
        topic         => T,
        realm         => R,
        publisher     => Pub,
        seq           => Seq,
        payload       => Pl,
        delivered_via => direct
    }), Id).

%%====================================================================
%% Internals — event relay (per-hop re-sign + dedup, mesh realm only)
%%====================================================================

do_relay_event(#{frame_type := event, realm := R} = Frame,
               #state{realm = R} = S) ->
    case check_seen(Frame, S) of
        seen ->
            {error, duplicate};
        fresh ->
            Re_signed = re_sign_event(Frame, S),
            Matched   = hecate_pubsub:deliver_event(
                            S#state.pubsub, Re_signed),
            {Re_signed, Matched}
    end;
do_relay_event(_Frame, _S) ->
    {error, realm_mismatch}.

re_sign_event(#{topic := T, realm := R, publisher := Pub,
                seq := Seq, payload := Pl, delivered_via := Via},
              #state{identity = Id}) ->
    macula_frame:sign(macula_frame:event(#{
        topic         => T,
        realm         => R,
        publisher     => Pub,
        seq           => Seq,
        payload       => Pl,
        delivered_via => Via
    }), Id).

%% (publisher, seq) is preserved end-to-end across hops by the
%% per-hop re-sign and across PUBLISH→EVENT conversion, so the
%% pair uniquely identifies an EVENT in the seen-cache.
check_seen(#{publisher := Pub, seq := Seq}, #state{seen = Seen})
  when is_binary(Pub), is_integer(Seq) ->
    Now = erlang:monotonic_time(millisecond),
    case ets:insert_new(Seen, {{Pub, Seq}, Now}) of
        true  -> fresh;
        false -> seen
    end;
check_seen(_Frame, _S) ->
    fresh.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(seen_sweep, #state{seen = Seen,
                                seen_ttl_ms = TtlMs} = S) ->
    sweep_seen(Seen, TtlMs),
    erlang:send_after(?SEEN_SWEEP_INTERVAL_MS, self(), seen_sweep),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

sweep_seen(Seen, TtlMs) ->
    Cutoff = erlang:monotonic_time(millisecond) - TtlMs,
    ets:select_delete(Seen,
        [{{'_', '$1'}, [{'<', '$1', Cutoff}], [true]}]).

terminate(_Reason, _State) ->
    ok.
