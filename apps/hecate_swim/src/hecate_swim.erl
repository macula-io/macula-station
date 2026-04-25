%% @doc SWIM-Lifeguard failure detector.
%%
%% Phase 2 first pass — classic SWIM with direct-only probes:
%% <ul>
%%   <li>Every `period_ms' (default 2 s): pick one random alive member,
%%       send a signed PING, wait `ping_timeout_ms' (default 500 ms) for
%%       a signed ACK.</li>
%%   <li>No ACK ⇒ member transitions `alive → suspect'.</li>
%%   <li>`suspect_timeout_ms' (default 6 s) later without refutation ⇒
%%       `suspect → confirmed_failed'.</li>
%%   <li>Any received PING or matching ACK refutes suspicion instantly.</li>
%% </ul>
%%
%% Lifeguard extensions (indirect-ping via K buddies, self-awareness,
%% refutation-buddy, NACK) land in a follow-up session — see plan Part 4 §5.
%%
%% Membership changes are pushed to the controlling pid (the station's
%% gen_server) as `{hecate_swim, member_state, NodeId, State}'.
-module(hecate_swim).
-behaviour(gen_server).

-export([
    start_link/1,
    stop/1,
    add_peer/3,
    remove_peer/2,
    handle_frame/3,
    members/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, member/0, member_state/0]).

-define(DEFAULT_PERIOD_MS,          2_000).
-define(DEFAULT_PING_TIMEOUT_MS,      500).
-define(DEFAULT_SUSPECT_TIMEOUT_MS, 6_000).

-type opts() :: #{
    self_node_id       := hecate_identity:pubkey(),
    identity           := hecate_identity:key_pair(),
    controlling_pid    := pid(),
    period_ms          => pos_integer(),
    ping_timeout_ms    => pos_integer(),
    suspect_timeout_ms => pos_integer()
}.

-type member_state() :: alive | suspect | confirmed_failed.

-type member() :: #{
    node_id   := hecate_identity:pubkey(),
    state     := member_state(),
    last_seen := pos_integer(),
    since     := pos_integer(),
    conn_pid  := pid() | undefined
}.

-type probe() :: #{
    round     := non_neg_integer(),
    target    := hecate_identity:pubkey(),
    timer_ref := reference()
}.

-type config() :: #{
    period_ms          := pos_integer(),
    ping_timeout_ms    := pos_integer(),
    suspect_timeout_ms := pos_integer()
}.

-record(state, {
    self_node_id     :: hecate_identity:pubkey(),
    identity         :: hecate_identity:key_pair(),
    controlling_pid  :: pid(),
    round = 0        :: non_neg_integer(),
    members = #{}    :: #{hecate_identity:pubkey() => member()},
    probes = #{}     :: #{non_neg_integer() => probe()},
    suspect_timers = #{} :: #{hecate_identity:pubkey() => reference()},
    config           :: config()
}).

%%------------------------------------------------------------------
%% Public API
%%------------------------------------------------------------------

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

-spec add_peer(pid(), hecate_identity:pubkey(), pid()) -> ok.
add_peer(Pid, NodeId, ConnPid)
  when is_binary(NodeId), byte_size(NodeId) =:= 32, is_pid(ConnPid) ->
    gen_server:cast(Pid, {add_peer, NodeId, ConnPid}).

-spec remove_peer(pid(), hecate_identity:pubkey()) -> ok.
remove_peer(Pid, NodeId)
  when is_binary(NodeId), byte_size(NodeId) =:= 32 ->
    gen_server:cast(Pid, {remove_peer, NodeId}).

-spec handle_frame(pid(), hecate_identity:pubkey(), hecate_frame:frame()) -> ok.
handle_frame(Pid, FromNodeId, Frame)
  when is_binary(FromNodeId), byte_size(FromNodeId) =:= 32, is_map(Frame) ->
    gen_server:cast(Pid, {swim_frame, FromNodeId, Frame}).

-spec members(pid()) -> [member()].
members(Pid) ->
    gen_server:call(Pid, members).

%%------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------

init(#{self_node_id := Self, identity := Id, controlling_pid := Ctrl} = Opts) ->
    Config = #{
        period_ms          => maps:get(period_ms,          Opts, ?DEFAULT_PERIOD_MS),
        ping_timeout_ms    => maps:get(ping_timeout_ms,    Opts, ?DEFAULT_PING_TIMEOUT_MS),
        suspect_timeout_ms => maps:get(suspect_timeout_ms, Opts, ?DEFAULT_SUSPECT_TIMEOUT_MS)
    },
    State = #state{
        self_node_id    = Self,
        identity        = Id,
        controlling_pid = Ctrl,
        config          = Config
    },
    _ = schedule_period(State),
    {ok, State}.

handle_call(members, _From, #state{members = M} = S) ->
    {reply, maps:values(M), S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast({add_peer, NodeId, ConnPid}, S) ->
    {noreply, upsert_alive(NodeId, ConnPid, S)};
handle_cast({remove_peer, NodeId}, S) ->
    {noreply, drop_member(NodeId, S)};
handle_cast({swim_frame, From, Frame}, S) ->
    {noreply, dispatch_frame(From, Frame, S)};
handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(period_tick, S) ->
    {noreply, run_round(S)};
handle_info({ping_timeout, Round, Target}, S) ->
    {noreply, on_ping_timeout(Round, Target, S)};
handle_info({suspect_timeout, NodeId}, S) ->
    {noreply, on_suspect_timeout(NodeId, S)};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%------------------------------------------------------------------
%% Membership mutations
%%------------------------------------------------------------------

upsert_alive(NodeId, ConnPid, #state{members = M} = S) ->
    Now = erlang:system_time(millisecond),
    Prev = maps:get(NodeId, M, undefined),
    Since = preserved_since(Prev, alive, Now),
    Entry = #{
        node_id   => NodeId,
        state     => alive,
        last_seen => Now,
        since     => Since,
        conn_pid  => ConnPid
    },
    NewMembers = M#{NodeId => Entry},
    cancel_suspect_timer(NodeId, S#state{members = NewMembers}).

drop_member(NodeId, #state{members = M} = S) ->
    S1 = cancel_suspect_timer(NodeId, S),
    S1#state{members = maps:remove(NodeId, M)}.

touch_alive(NodeId, Member, S) ->
    Now = erlang:system_time(millisecond),
    Since = preserved_since(Member, alive, Now),
    Updated = Member#{state => alive, last_seen => Now, since => Since},
    NewMembers = (S#state.members)#{NodeId => Updated},
    S1 = S#state{members = NewMembers},
    case maps:get(state, Member) of
        alive -> S1;
        _     -> notify_and_cancel_suspect(NodeId, S1)
    end.

notify_and_cancel_suspect(NodeId, S) ->
    notify_state_change(NodeId, alive, S),
    cancel_suspect_timer(NodeId, S).

preserved_since(#{state := S0, since := Since}, TargetState, _Now)
  when S0 =:= TargetState ->
    Since;
preserved_since(_Prev, _TargetState, Now) ->
    Now.

cancel_suspect_timer(NodeId, #state{suspect_timers = T} = S) ->
    case maps:take(NodeId, T) of
        {Ref, T2} ->
            _ = erlang:cancel_timer(Ref),
            S#state{suspect_timers = T2};
        error ->
            S
    end.

mark_suspect(NodeId, #state{members = M, suspect_timers = T,
                            config = #{suspect_timeout_ms := Ms}} = S) ->
    case maps:get(NodeId, M, undefined) of
        #{state := alive} = Member ->
            Now = erlang:system_time(millisecond),
            Updated = Member#{state => suspect, since => Now},
            NewM = M#{NodeId => Updated},
            Ref = erlang:send_after(Ms, self(), {suspect_timeout, NodeId}),
            notify_state_change(NodeId, suspect, S),
            S#state{members = NewM, suspect_timers = T#{NodeId => Ref}};
        _ ->
            S
    end.

mark_confirmed(NodeId, #state{members = M, suspect_timers = T} = S) ->
    case maps:get(NodeId, M, undefined) of
        #{state := suspect} = Member ->
            Now = erlang:system_time(millisecond),
            Updated = Member#{state => confirmed_failed, since => Now},
            NewMembers = M#{NodeId => Updated},
            notify_state_change(NodeId, confirmed_failed, S),
            S#state{members = NewMembers, suspect_timers = maps:remove(NodeId, T)};
        _ ->
            S#state{suspect_timers = maps:remove(NodeId, T)}
    end.

%%------------------------------------------------------------------
%% Probing
%%------------------------------------------------------------------

schedule_period(#state{config = #{period_ms := Ms}}) ->
    erlang:send_after(Ms, self(), period_tick).

run_round(#state{round = R} = S) ->
    _ = schedule_period(S),
    NewRound = R + 1,
    S1 = S#state{round = NewRound},
    case pick_alive_target(S1) of
        {ok, Target, ConnPid} ->
            send_ping(NewRound, Target, ConnPid, S1);
        none ->
            S1
    end.

pick_alive_target(#state{members = M}) ->
    Alive = [{NodeId, ConnPid}
             || {NodeId, #{state := alive, conn_pid := ConnPid}}
                    <- maps:to_list(M),
                is_pid(ConnPid)],
    pick_one(Alive).

pick_one([]) -> none;
pick_one(L)  ->
    {NodeId, ConnPid} = lists:nth(rand:uniform(length(L)), L),
    {ok, NodeId, ConnPid}.

send_ping(Round, Target, ConnPid,
          #state{identity = Id, config = #{ping_timeout_ms := Ms},
                 probes = P} = S) ->
    Ping = hecate_frame:swim_ping(#{round => Round, incarnation => 0,
                                    piggyback => []}),
    Signed = hecate_frame:sign(Ping, Id),
    _ = hecate_peering:send_frame(ConnPid, Signed),
    Ref = erlang:send_after(Ms, self(), {ping_timeout, Round, Target}),
    Probe = #{round => Round, target => Target, timer_ref => Ref},
    S#state{probes = P#{Round => Probe}}.

on_ping_timeout(Round, Target, #state{probes = P} = S) ->
    case maps:take(Round, P) of
        {_Probe, NewP} ->
            mark_suspect(Target, S#state{probes = NewP});
        error ->
            S
    end.

on_suspect_timeout(NodeId, S) ->
    mark_confirmed(NodeId, S).

%%------------------------------------------------------------------
%% Frame dispatch
%%------------------------------------------------------------------

dispatch_frame(From, #{frame_type := swim_ping} = Ping, S) ->
    on_ping(From, Ping, S);
dispatch_frame(From, #{frame_type := swim_ack} = Ack, S) ->
    on_ack(From, Ack, S);
dispatch_frame(_From, _Frame, S) ->
    S.

%% Replying to a PING also confirms the sender alive (refutes suspicion).
on_ping(From, #{round := Round}, #state{identity = Id} = S) ->
    S1 = maybe_touch_alive(From, S),
    case maps:get(From, S1#state.members, undefined) of
        #{conn_pid := ConnPid} when is_pid(ConnPid) ->
            Ack = hecate_frame:swim_ack(#{
                round       => Round,
                responder   => S1#state.self_node_id,
                incarnation => 0,
                piggyback   => []
            }),
            Signed = hecate_frame:sign(Ack, Id),
            _ = hecate_peering:send_frame(ConnPid, Signed),
            S1;
        _ ->
            S1
    end.

%% Matching ACK cancels the in-flight probe and marks sender alive.
on_ack(From, #{round := Round}, #state{probes = P} = S) ->
    case maps:take(Round, P) of
        {#{target := Target, timer_ref := Ref}, NewP}
          when Target =:= From ->
            _ = erlang:cancel_timer(Ref),
            maybe_touch_alive(From, S#state{probes = NewP});
        _ ->
            S
    end.

maybe_touch_alive(NodeId, #state{members = M} = S) ->
    case maps:get(NodeId, M, undefined) of
        undefined -> S;
        Member    -> touch_alive(NodeId, Member, S)
    end.

%%------------------------------------------------------------------
%% Notifications
%%------------------------------------------------------------------

notify_state_change(NodeId, State, #state{controlling_pid = Pid}) ->
    Pid ! {hecate_swim, member_state, NodeId, State},
    ok.
