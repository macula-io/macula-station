%% @doc CT helper for the Phase 5 acceptance suite.
%%
%% Each station is a process that holds, per realm:
%% <ul>
%%   <li>a `hecate_overlay_view' (HyParView active + passive)</li>
%%   <li>a `hecate_plumtree' state (eager + lazy push)</li>
%%   <li>a `hecate_pubsub' state (topic → subscriber set)</li>
%% </ul>
%%
%% Stations share an in-VM router that delivers frames between
%% station inboxes. On receiving a frame the station dispatches to
%% whichever module owns the frame type:
%% <ul>
%%   <li>`hyparview_*' → `hecate_overlay_proto:process/4' (emits
%%       outbound frames via the router and mutates the overlay
%%       view).</li>
%%   <li>`plumtree_*' → `hecate_plumtree:process/3' (delivers
%%       payloads locally; we then forward delivered payloads to
%%       `hecate_pubsub:process/3' so local subscribers fire).</li>
%%   <li>`subscribe' / `unsubscribe' / `event' →
%%       `hecate_pubsub:process/3' directly; `event' frames also
%%       enter the plumtree dispatcher via the local publish path.</li>
%% </ul>
%%
%% `join/5' drives the realm-join handshake: joiner signs a JOIN
%% frame, attaches the admin-signed endorsement bytes, sends to a
%% seed; the seed verifies via `hecate_realm_join:verify_endorsement/3'
%% and admits the joiner into its active view. Non-endorsed joiners
%% are silently dropped (defensive).
-module(phase5_helper).

-export([
    start_fleet/3,
    stop_fleet/1,
    connect/4,
    subscribe/4,
    publish/5,
    deliveries/3,
    active_view/3,
    endorse/4,
    join/5,
    pubkey_of/2
]).

-record(station, {
    name   :: atom(),
    pid    :: pid(),
    kp     :: macula_identity:key_pair(),
    pubkey :: macula_identity:pubkey()
}).

%%=====================================================================
%% Fleet lifecycle
%%=====================================================================

%% @doc Build a fleet of stations whose trust root for `Realms'
%% maps each realm id to an admin pubkey. The realm id IS the admin
%% pubkey in our model — same as the SDK constructors.
start_fleet(Names, Realms, _Opts) when is_list(Names), is_list(Realms) ->
    Router = spawn_router(),
    Stations = [build_station(N, Realms, Router) || N <- Names],
    Map = maps:from_list([{S#station.pubkey, S#station.pid}
                          || S <- Stations]),
    Router ! {table, Map},
    NameMap = maps:from_list([{S#station.name, S} || S <- Stations]),
    #{router => Router, stations => NameMap, realms => Realms}.

stop_fleet(#{router := Router, stations := Stations}) ->
    maps:foreach(fun(_N, S) -> S#station.pid ! stop end, Stations),
    Router ! stop,
    ok.

build_station(Name, Realms, Router) ->
    Kp = macula_identity:generate(),
    Pub = macula_identity:public(Kp),
    Pid = spawn(fun() ->
        station_loop(init_state(Name, Kp, Pub, Realms, Router))
    end),
    #station{name = Name, pid = Pid, kp = Kp, pubkey = Pub}.

init_state(Name, Kp, Pub, Realms, Router) ->
    Per = maps:from_list(
            [{R, #{view    => hecate_overlay_view:new(Pub),
                   plum    => hecate_plumtree:new(Kp, R),
                   pubsub  => hecate_pubsub:new(R)}}
             || R <- Realms]),
    #{
        name   => Name,
        kp     => Kp,
        pubkey => Pub,
        router => Router,
        realms => Per,
        delivered => #{},   %% {realm, topic} → [payload]
        msg_log   => []     %% debugging
    }.

%%=====================================================================
%% Public helpers
%%=====================================================================

pubkey_of(#{stations := Map}, Name) ->
    (maps:get(Name, Map))#station.pubkey.

endorse(#{stations := Map}, Admin, Realm, Name) ->
    #station{pubkey = Pub} = maps:get(Name, Map),
    R = hecate_record:realm_member_endorsement(
          Realm,
          #{realm => Realm, member_node => Pub, roles => [<<"peer">>]}),
    hecate_record:sign(R, Admin).

%% @doc Wire A→B and B→A into each other's active view for `Realm'
%% without going through the full hyparview handshake. Used by
%% tests that want to exercise plumtree/pubsub over a known mesh.
connect(#{stations := Map} = _Net, NameA, NameB, Realm) ->
    #station{pid = PidA, pubkey = PubA} = maps:get(NameA, Map),
    #station{pid = PidB, pubkey = PubB} = maps:get(NameB, Map),
    call_station(PidA, {wire_peer, Realm, PubB}),
    call_station(PidB, {wire_peer, Realm, PubA}),
    ok.

subscribe(#{stations := Map} = _Net, Name, Realm, Topic) ->
    #station{pid = Pid, pubkey = Pub} = maps:get(Name, Map),
    call_station(Pid, {local_subscribe, Realm, Topic, Pub}).

publish(#{stations := Map} = _Net, Name, Realm, Topic, Payload) ->
    #station{pid = Pid} = maps:get(Name, Map),
    call_station(Pid, {local_publish, Realm, Topic, Payload}).

deliveries(#{stations := Map} = _Net, Name, {Realm, Topic}) ->
    #station{pid = Pid} = maps:get(Name, Map),
    call_station(Pid, {deliveries, Realm, Topic}).

active_view(#{stations := Map} = _Net, Name, Realm) ->
    #station{pid = Pid} = maps:get(Name, Map),
    call_station(Pid, {active_view, Realm}).

%% @doc Run the admission handshake: `JoinerName' sends a signed
%% `hyparview_join' frame to `SeedName' bundled with `Endorsement'
%% (admin-signed member endorsement). Seed verifies via
%% `hecate_realm_join' and either admits or drops.
join(#{stations := Map} = _Net, JoinerName, SeedName, Realm, Endorsement) ->
    #station{pid = JPid}    = maps:get(JoinerName, Map),
    #station{pubkey = SPub} = maps:get(SeedName,   Map),
    call_station(JPid, {send_join, Realm, SPub, Endorsement}),
    %% Handshake is a chain of one-shot messages; give the router
    %% a brief window to fan out NEIGHBOR(high) replies.
    timer:sleep(20),
    ok.

%%=====================================================================
%% Station loop
%%=====================================================================

station_loop(State) ->
    receive
        stop -> ok;
        {frame, Frame}             -> station_loop(dispatch_frame(Frame, State));
        {control, From, Ref, Msg}  ->
            {Reply, State2} = control(Msg, State),
            From ! {Ref, Reply},
            station_loop(State2);
        _Other ->
            station_loop(State)
    end.

%%---------------------------------------------------------------------
%% Control messages (synchronous)
%%---------------------------------------------------------------------

control({wire_peer, Realm, Peer}, State) ->
    {ok, update_realm(State, Realm, fun(#{view := V, plum := Pl} = RS) ->
        V1  = hecate_overlay_view:add_active(V, Peer),
        Pl1 = hecate_plumtree:add_peer(Pl, Peer),
        RS#{view := V1, plum := Pl1}
    end)};

control({local_subscribe, Realm, Topic, Sub}, State) ->
    {ok, update_realm(State, Realm, fun(#{pubsub := PS} = RS) ->
        RS#{pubsub := hecate_pubsub:subscribe(PS, Topic, Sub)}
    end)};

control({local_publish, Realm, Topic, Payload}, State) ->
    RS = realm_state(State, Realm),
    PubId   = maps:get(pubkey, State),
    EventF  = hecate_pubsub:build_event(
                maps:get(pubsub, RS),
                #{topic => Topic, realm => Realm,
                  publisher => PubId, seq => 0,
                  payload => Payload, published_at_ms => 0},
                maps:get(kp, State)),
    MsgId   = crypto:strong_rand_bytes(16),
    {Plum1, Sends, Delivered} =
        hecate_plumtree:publish(maps:get(plum, RS), MsgId, EventF),
    %% "Local delivered" — feed into pubsub for local subscribers too.
    State2 = deliver_payloads(Delivered, Realm, State, RS),
    emit_plumtree_sends(Sends, Realm, State2),
    State3 = update_realm(State2, Realm, fun(R) -> R#{plum := Plum1} end),
    {ok, State3};

control({send_join, Realm, SeedPub, Endorsement}, State) ->
    JKp  = maps:get(kp, State),
    JPub = maps:get(pubkey, State),
    Frame0 = hecate_frame:hyparview_join(
               #{realm => Realm, new_member => JPub}),
    %% Attach endorsement in a test-only side channel so the seed can
    %% verify it — the production wire format would piggyback in the
    %% JOIN frame's endorsement field (not part of the minimal JOIN
    %% record in Phase 5.2).
    Frame = hecate_frame:sign(Frame0, JKp),
    Env   = #{frame => Frame, endorsement => Endorsement,
              realm => Realm, joiner => JPub},
    route(State, SeedPub, {join_envelope, Env}),
    {ok, State};

control({deliveries, Realm, Topic}, State) ->
    {maps:get({Realm, Topic}, maps:get(delivered, State), []), State};

control({active_view, Realm}, State) ->
    RS = realm_state(State, Realm),
    {hecate_overlay_view:active(maps:get(view, RS)), State}.

%%---------------------------------------------------------------------
%% Frame dispatch
%%---------------------------------------------------------------------

dispatch_frame({join_envelope, #{frame := Frame, endorsement := End,
                                 realm := Realm, joiner := Joiner}}, State) ->
    handle_join_envelope(Frame, End, Realm, Joiner, State);
dispatch_frame(#{frame_type := FT} = Frame, State) ->
    case FT of
        hyparview_join          -> dispatch_overlay(Frame, State);
        hyparview_forward_join  -> dispatch_overlay(Frame, State);
        hyparview_neighbor      -> dispatch_overlay(Frame, State);
        hyparview_disconnect    -> dispatch_overlay(Frame, State);
        hyparview_shuffle       -> dispatch_overlay(Frame, State);
        hyparview_shuffle_reply -> dispatch_overlay(Frame, State);
        plumtree_gossip         -> dispatch_plumtree(Frame, State);
        plumtree_ihave          -> dispatch_plumtree(Frame, State);
        plumtree_graft          -> dispatch_plumtree(Frame, State);
        plumtree_prune          -> dispatch_plumtree(Frame, State);
        subscribe               -> dispatch_pubsub(Frame, State);
        unsubscribe             -> dispatch_pubsub(Frame, State);
        event                   -> dispatch_pubsub(Frame, State);
        _                       -> State
    end.

handle_join_envelope(Frame, Endorsement, Realm, Joiner, State) ->
    case hecate_realm_join:verify_endorsement(Endorsement, Realm, Joiner) of
        {ok, _Roles} ->
            admit_joiner(Frame, Realm, Joiner, State);
        {error, _} ->
            State   %% silently drop
    end.

admit_joiner(_Frame, Realm, Joiner, State) ->
    %% Admit joiner into active view and reply NEIGHBOR(high).
    State1 = update_realm(State, Realm, fun(#{view := V, plum := Pl} = RS) ->
        V1  = hecate_overlay_view:add_active(V, Joiner),
        Pl1 = hecate_plumtree:add_peer(Pl, Joiner),
        RS#{view := V1, plum := Pl1}
    end),
    Reply = hecate_frame:sign(hecate_frame:hyparview_neighbor(
                                #{realm => Realm, priority => high}),
                              maps:get(kp, State1)),
    route(State1, Joiner, Reply),
    State1.

%%---------------------------------------------------------------------
%% Overlay dispatch
%%---------------------------------------------------------------------

dispatch_overlay(#{realm := Realm} = Frame, State) ->
    RS = realm_state(State, Realm),
    Ctx = #{
        self_id  => maps:get(pubkey, State),
        identity => maps:get(kp, State),
        arwl     => 6, prwl => 3,
        shuffle_ttl => 4
    },
    From = maps:get(new_member, Frame, undefined),
    {NewView, Actions} = hecate_overlay_proto:process(
                           maps:get(view, RS), From, Frame, Ctx),
    apply_overlay_actions(Actions, State,
                          fun(ST) ->
                              update_realm(ST, Realm,
                                           fun(R) -> R#{view := NewView} end)
                          end).

apply_overlay_actions([], State, Finalize) ->
    Finalize(State);
apply_overlay_actions([{send, Peer, Frame} | Rest], State, Finalize) ->
    route(State, Peer, Frame),
    apply_overlay_actions(Rest, State, Finalize);
apply_overlay_actions([_ | Rest], State, Finalize) ->
    apply_overlay_actions(Rest, State, Finalize).

%%---------------------------------------------------------------------
%% Plumtree dispatch
%%---------------------------------------------------------------------

dispatch_plumtree(#{realm := Realm} = Frame, State) ->
    RS = realm_state(State, Realm),
    From = maps:get(sender, Frame, undefined),
    {Plum1, Sends, Delivered} =
        hecate_plumtree:process(maps:get(plum, RS), From, Frame),
    State1 = deliver_payloads(Delivered, Realm, State, RS),
    emit_plumtree_sends(Sends, Realm, State1),
    update_realm(State1, Realm, fun(R) -> R#{plum := Plum1} end).

emit_plumtree_sends([], _Realm, _State) -> ok;
emit_plumtree_sends([{send, Peer, Frame} | Rest], Realm, State) ->
    route(State, Peer, frame_with_sender(Frame, Realm, State)),
    emit_plumtree_sends(Rest, Realm, State).

frame_with_sender(Frame, _Realm, State) ->
    Frame#{sender => maps:get(pubkey, State)}.

deliver_payloads([], _Realm, State, _RS) -> State;
deliver_payloads([{_MsgId, Payload} | Rest], Realm, State, RS) ->
    State1 = feed_pubsub(Payload, Realm, State, RS),
    deliver_payloads(Rest, Realm, State1, RS).

feed_pubsub(#{frame_type := event, topic := T} = Frame, Realm, State, RS) ->
    {_, Subs} = hecate_pubsub:process(maps:get(pubsub, RS),
                                      undefined, Frame),
    log_delivery(State, Realm, T, maps:get(payload, Frame), Subs);
feed_pubsub(_Other, _Realm, State, _RS) ->
    State.

log_delivery(State, Realm, Topic, Payload, Subs) when Subs =/= [] ->
    Key = {Realm, Topic},
    Delivered = maps:get(delivered, State),
    Existing  = maps:get(Key, Delivered, []),
    State#{delivered := Delivered#{Key => [Payload | Existing]}};
log_delivery(State, _Realm, _Topic, _Payload, _Subs) ->
    State.

%%---------------------------------------------------------------------
%% Pubsub dispatch (direct SUBSCRIBE/UNSUBSCRIBE/EVENT — rare in tests)
%%---------------------------------------------------------------------

dispatch_pubsub(#{realm := Realm} = Frame, State) ->
    RS = realm_state(State, Realm),
    {PS1, _} = hecate_pubsub:process(maps:get(pubsub, RS), undefined, Frame),
    update_realm(State, Realm, fun(R) -> R#{pubsub := PS1} end).

%%=====================================================================
%% Internals
%%=====================================================================

realm_state(State, Realm) ->
    maps:get(Realm, maps:get(realms, State)).

update_realm(State, Realm, Fun) ->
    Realms = maps:get(realms, State),
    RS  = maps:get(Realm, Realms),
    RS1 = Fun(RS),
    State#{realms := Realms#{Realm => RS1}}.

route(State, DstPubKey, Msg) ->
    maps:get(router, State) ! {route, DstPubKey, Msg},
    ok.

call_station(Pid, Msg) ->
    Ref = make_ref(),
    Pid ! {control, self(), Ref, Msg},
    receive {Ref, Reply} -> Reply
    after 500 -> {error, station_timeout}
    end.

%%=====================================================================
%% Router
%%=====================================================================

spawn_router() ->
    spawn(fun() -> router_loop(undefined) end).

router_loop(Table) ->
    receive
        {table, T}             -> router_loop(T);
        {route, Dst, Msg}      -> route_to(Table, Dst, Msg), router_loop(Table);
        stop                   -> ok
    end.

route_to(undefined, _, _) -> ok;
route_to(Table, Dst, Msg) ->
    case maps:find(Dst, Table) of
        {ok, Pid} -> Pid ! {frame, Msg};
        error     -> ok
    end.
