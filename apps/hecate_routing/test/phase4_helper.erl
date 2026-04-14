%% @doc CT helper for the Phase 4 acceptance suite.
%%
%% Builds an in-VM fleet of "stations" wired through a synchronous
%% dispatch router. Each station is a small process that:
%% <ul>
%%   <li>Holds a key pair and a mutable "alive set" of 16-byte
%%       NodeId prefixes (set membership is the `is_alive'
%%       predicate handed to `hecate_relay').</li>
%%   <li>On receiving a CALL frame, runs `hecate_relay:process_call/2'
%%       and either forwards (via the router), delivers locally
%%       (echoes the payload back as a signed RESULT), or
%%       generates a signed BOLT#4 ERROR and routes it to the
%%       originator.</li>
%%   <li>On receiving a RESULT or ERROR frame, looks up its
%%       listeners map and fires `{call_outcome, CallId, Outcome}'
%%       to the registered awaiter.</li>
%% </ul>
%%
%% This is intentionally a CT-only helper — it's not the
%% production CALL pipeline but it exercises every Phase 4 module
%% end to end (`hecate_routing` for paths, `macula_source_route`
%% for the wire header, `hecate_relay` for per-hop processing,
%% `hecate_call_retry` for budget + path rotation,
%% `macula_bolt4` + `macula_frame` for codes + envelopes).
-module(phase4_helper).

-export([
    start_fleet/1,
    stop_fleet/1,
    set_alive/3,
    name_to_pubkey/2,
    name_to_prefix/2,
    call_via_path/4,
    call_via_paths/5
]).

-record(station, {
    name        :: atom(),
    pid         :: pid(),
    kp          :: macula_identity:key_pair(),
    pubkey      :: macula_identity:pubkey(),
    prefix      :: <<_:128>>
}).

%%=====================================================================
%% Fleet lifecycle
%%=====================================================================

start_fleet(Names) when is_list(Names), Names =/= [] ->
    Router = spawn_router(),
    Stations = [build_station(N, Router) || N <- Names],
    AliveAll = sets:from_list([S#station.prefix || S <- Stations]),
    Map = maps:from_list([{S#station.prefix, S#station.pid}
                          || S <- Stations]),
    Router ! {table, Map},
    [station_send(S#station.pid, {init_alive, AliveAll}) || S <- Stations],
    NameMap = maps:from_list([{S#station.name, S} || S <- Stations]),
    #{router => Router, stations => NameMap}.

stop_fleet(#{router := Router, stations := Stations}) ->
    maps:foreach(fun(_N, S) -> station_send(S#station.pid, stop) end,
                 Stations),
    Router ! stop.

build_station(Name, Router) ->
    Kp = macula_identity:generate(),
    Pub = macula_identity:public(Kp),
    Prefix = macula_source_route:truncate_hop(Pub),
    Pid = spawn(fun() ->
        State = init_state(Kp, Pub, Prefix, Router),
        station_loop(State)
    end),
    #station{name = Name, pid = Pid, kp = Kp,
             pubkey = Pub, prefix = Prefix}.

%%=====================================================================
%% Public helpers
%%=====================================================================

name_to_pubkey(#{stations := M}, Name) ->
    (maps:get(Name, M))#station.pubkey.

name_to_prefix(#{stations := M}, Name) ->
    (maps:get(Name, M))#station.prefix.

%% @doc Toggle the live-state flag for `Name' across every
%% station's `is_alive' set.
set_alive(#{stations := M} = Net, Name, Bool) ->
    Prefix = name_to_prefix(Net, Name),
    maps:foreach(fun(_N, S) ->
        station_send(S#station.pid, {set_alive, Prefix, Bool})
    end, M).

%% @doc Issue a CALL from `From' along the explicit `PathNames' list.
%% `PathNames' is `[FromName | Hops]' — the originator followed by
%% every hop including the destination. Per Part 6 §11 the
%% source-route header lists hops from the originator's perspective
%% (i.e. excluding the originator itself), so the helper splits the
%% input accordingly. Blocks until RESULT or ERROR arrives or
%% the timeout fires. Returns a `hecate_call:outcome()'.
call_via_path(Net, FromName, [FromName | HopNames] = _PathNames,
              #{procedure := _, payload := _, realm := _,
                timeout_ms := T} = Spec)
  when HopNames =/= [] ->
    From     = maps:get(FromName, maps:get(stations, Net)),
    HopIds   = [name_to_prefix(Net, N) || N <- HopNames],
    Deadline = erlang:system_time(millisecond) + T,
    SR       = macula_source_route:new(HopIds, Deadline),
    CallId   = crypto:strong_rand_bytes(16),
    SrBin    = macula_source_route:encode(SR),
    Frame    = macula_frame:sign(macula_frame:call(#{
                 call_id      => CallId,
                 procedure    => maps:get(procedure, Spec),
                 realm        => maps:get(realm, Spec),
                 payload      => maps:get(payload, Spec),
                 deadline_ms  => Deadline,
                 caller       => From#station.pubkey,
                 source_route => SrBin
               }), From#station.kp),
    Listener = self(),
    [FirstHop | _] = HopIds,
    station_send(From#station.pid,
                 {originate, CallId, Listener, FirstHop, Frame}),
    receive
        {call_outcome, CallId, Outcome} -> Outcome
    after T + 500 ->
        {error, expiry_too_soon, #{phase => helper_timeout}}
    end.

%% @doc Wrap `call_via_path' in `hecate_call_retry:execute/1' for
%% disjoint-path rotation tests.
call_via_paths(Net, FromName, PathLists, Spec, RetryOpts) ->
    TryFn = fun(PathNames) ->
        call_via_path(Net, FromName, PathNames, Spec)
    end,
    hecate_call_retry:execute(maps:merge(#{
        paths  => PathLists,
        try_fn => TryFn
    }, RetryOpts)).

%%=====================================================================
%% Station process
%%=====================================================================

init_state(Kp, Pub, Prefix, Router) ->
    #{
        kp        => Kp,
        pubkey    => Pub,
        prefix    => Prefix,
        router    => Router,
        alive     => sets:new(),
        listeners => #{}
    }.

station_loop(State) ->
    receive
        stop ->
            ok;
        {init_alive, Set} ->
            station_loop(State#{alive := Set});
        {set_alive, Prefix, true} ->
            station_loop(State#{alive := sets:add_element(
                                          Prefix, maps:get(alive, State))});
        {set_alive, Prefix, false} ->
            station_loop(State#{alive := sets:del_element(
                                          Prefix, maps:get(alive, State))});
        {originate, CallId, Listener, FirstHop, Frame} ->
            send_via_router(State, FirstHop, Frame),
            station_loop(register_listener(State, CallId, Listener));
        {frame, _From, Frame} ->
            station_loop(handle_frame(Frame, State));
        Other ->
            ct:pal("station ~p unhandled: ~p", [maps:get(prefix, State), Other]),
            station_loop(State)
    end.

handle_frame(#{frame_type := call} = Frame, State) ->
    Ctx = #{
        self_id  => maps:get(pubkey, State),
        identity => maps:get(kp, State),
        is_alive => fun(P) ->
            sets:is_element(P, maps:get(alive, State))
        end
    },
    Action = hecate_relay:process_call(Frame, Ctx),
    apply_action(Action, Frame, State);
handle_frame(#{frame_type := result} = Frame, State) ->
    deliver_outcome(Frame, {ok, maps:get(payload, Frame)}, State);
handle_frame(#{frame_type := error} = Frame, State) ->
    Code   = maps:get(name, Frame),
    Detail = maps:without([call_id, code, name, signature, version,
                            frame_id, sent_at_ms, capabilities, realm,
                            call_id, source_route], Frame),
    deliver_outcome(Frame, {error, Code, Detail}, State).

apply_action({forward, NextHop, NewFrame}, _Original, State) ->
    send_via_router(State, NextHop, NewFrame),
    State;
apply_action({deliver_local, Frame}, _Original, State) ->
    %% Echo handler: build a signed RESULT addressed to the caller.
    CallId  = maps:get(call_id, Frame),
    Caller  = maps:get(caller, Frame),
    Payload = #{echoed => maps:get(payload, Frame)},
    ResultFrame = macula_frame:sign(macula_frame:result(#{
                    call_id      => CallId,
                    payload      => Payload,
                    responded_by => maps:get(pubkey, State)
                  }), maps:get(kp, State)),
    send_via_router(State, macula_source_route:truncate_hop(Caller),
                    ResultFrame),
    State;
apply_action({reply_error, ErrFrame, Originator}, _Original, State) ->
    case Originator of
        undefined -> ok;
        OrigKey   -> send_via_router(State,
                                     macula_source_route:truncate_hop(OrigKey),
                                     ErrFrame)
    end,
    State.

deliver_outcome(Frame, Outcome, State) ->
    CallId = maps:get(call_id, Frame),
    case maps:find(CallId, maps:get(listeners, State)) of
        {ok, Listener} ->
            Listener ! {call_outcome, CallId, Outcome},
            State#{listeners := maps:remove(CallId, maps:get(listeners, State))};
        error ->
            State
    end.

register_listener(State, CallId, Listener) ->
    Listeners = maps:get(listeners, State),
    State#{listeners := Listeners#{CallId => Listener}}.

send_via_router(#{prefix := Self, router := Router}, Dst, Frame) ->
    Router ! {route, Self, Dst, Frame},
    ok.

station_send(Pid, Msg) ->
    Pid ! Msg, ok.

%%=====================================================================
%% Router
%%=====================================================================

spawn_router() ->
    spawn(fun() -> router_loop(undefined) end).

router_loop(Table) ->
    receive
        {table, T}                 -> router_loop(T);
        {route, From, Dst, Frame}  ->
            route_frame(Table, From, Dst, Frame),
            router_loop(Table);
        stop                       -> ok
    end.

route_frame(undefined, _, _, _) -> ok;
route_frame(Table, From, Dst, Frame) ->
    case maps:find(Dst, Table) of
        {ok, Pid} -> Pid ! {frame, From, Frame};
        error     -> ok
    end.
