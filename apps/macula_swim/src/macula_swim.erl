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
%% Membership changes are pushed to the `controlling_pid' as
%% `{macula_swim, member_state, NodeId, State}'. `controlling_pid'
%% may be either a `pid()' (resolved once at start) or an `atom()'
%% (resolved on every notification via `whereis/1', dropping the
%% notification if the registered name is not yet up). The atom
%% form lets SWIM start before its consumer in the boot order
%% without losing the supervisor-warning-spam fight.
-module(macula_swim).
-behaviour(gen_server).

-export([
    start_link/1,
    stop/1,
    add_peer/3,
    remove_peer/2,
    handle_frame/3,
    members/1,
    stats/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, member/0, member_state/0]).

-define(DEFAULT_PERIOD_MS,          2_000).
-define(DEFAULT_PING_TIMEOUT_MS,      500).
-define(DEFAULT_SUSPECT_TIMEOUT_MS, 6_000).

-type opts() :: #{
    self_node_id       := macula_identity:pubkey(),
    identity           := macula_identity:key_pair(),
    controlling_pid    := pid() | atom(),
    period_ms          => pos_integer(),
    ping_timeout_ms    => pos_integer(),
    suspect_timeout_ms => pos_integer()
}.

-type member_state() :: alive | suspect | confirmed_failed.

-type member() :: #{
    node_id   := macula_identity:pubkey(),
    state     := member_state(),
    last_seen := pos_integer(),
    since     := pos_integer(),
    conn_pid  := pid() | undefined
}.

-type probe() :: #{
    round     := non_neg_integer(),
    target    := macula_identity:pubkey(),
    timer_ref := reference()
}.

-type config() :: #{
    period_ms          := pos_integer(),
    ping_timeout_ms    := pos_integer(),
    suspect_timeout_ms := pos_integer()
}.

-record(state, {
    self_node_id     :: macula_identity:pubkey(),
    identity         :: macula_identity:key_pair(),
    controlling_pid  :: pid() | atom(),
    round = 0        :: non_neg_integer(),
    members = #{}    :: #{macula_identity:pubkey() => member()},
    probes = #{}     :: #{non_neg_integer() => probe()},
    suspect_timers = #{} :: #{macula_identity:pubkey() => reference()},
    config           :: config(),
    %% MECHANISM COUNTERS. Without these a quiet fleet is unreadable: a
    %% detector with nothing to detect and a detector that cannot fire look
    %% identical from outside, and a fix whose code path never executes in
    %% production has been validated by nothing.
    %%
    %% `probes_sent' vs `suspected' separates those two. `refuted' counts the
    %% late-ACK rescue added in `on_ack/3' -- if it reads zero forever, that
    %% branch is dead code on this fleet whatever the unit tests say.
    probes_sent = 0  :: non_neg_integer(),
    acks_matched = 0 :: non_neg_integer(),
    refuted = 0      :: non_neg_integer(),
    suspected = 0    :: non_neg_integer()
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

-spec add_peer(pid(), macula_identity:pubkey(), pid()) -> ok.
add_peer(Pid, NodeId, ConnPid)
  when is_binary(NodeId), byte_size(NodeId) =:= 32, is_pid(ConnPid) ->
    gen_server:cast(Pid, {add_peer, NodeId, ConnPid}).

-spec remove_peer(pid(), macula_identity:pubkey()) -> ok.
remove_peer(Pid, NodeId)
  when is_binary(NodeId), byte_size(NodeId) =:= 32 ->
    gen_server:cast(Pid, {remove_peer, NodeId}).

-spec handle_frame(pid(), macula_identity:pubkey(), macula_frame:frame()) -> ok.
handle_frame(Pid, FromNodeId, Frame)
  when is_binary(FromNodeId), byte_size(FromNodeId) =:= 32, is_map(Frame) ->
    gen_server:cast(Pid, {swim_frame, FromNodeId, Frame}).

%% @doc Cumulative mechanism counters since boot. See the state record for
%% why these exist rather than what they count.
-spec stats(pid()) -> #{atom() => non_neg_integer()}.
stats(Pid) ->
    gen_server:call(Pid, stats).

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
handle_call(stats, _From, #state{probes_sent = P, acks_matched = A,
                                 refuted = R, suspected = Su} = S) ->
    {reply, #{probes_sent => P, acks_matched => A,
              refuted => R, suspected => Su}, S};
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
            S#state{members = NewM, suspect_timers = T#{NodeId => Ref},
                    suspected = S#state.suspected + 1};
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

%% ⚠ `is_pid/1' IS TRUE FOR A DEAD PID. Guarding on it alone selects probe
%% targets whose conn died, sends every ping into the void, and converts a
%% perfectly healthy station to `confirmed_failed' on a timeout that says
%% nothing about the peer.
%%
%% That is not hypothetical here. A mutual station pair holds TWO conns,
%% inbound and outbound, but SWIM stores exactly ONE `conn_pid', handed to it
%% once by `macula_station_peer_observer:apply_station_flag/4' at capability
%% resolution. When that one direction dies while the other survives, the peer
%% is NOT isolated, so it is never removed from SWIM and no fresh capability
%% resolution re-adds it — SWIM keeps the dead pid for the lifetime of the
%% process. `touch_alive/3' does not refresh `conn_pid' either, so even a
%% rescue leaves the stale pid in place and the member re-enters the suspect
%% cycle immediately: rescue becomes a metronome rather than a recovery.
%%
%% This is the same pathogen as the 2026-07-26 multihop pubsub root cause,
%% where a stale bypass recipient pid silenced every pre-existing connection
%% totally and silently, for exactly this reason. The observer now re-points
%% SWIM at the surviving conn (`resync_swim_after_conn_loss/3'), which is the
%% real fix; this liveness check is the defence in depth, because a pid can
%% die at any moment between that resync and this selection.
%%
%% A comprehension FILTER may be an arbitrary boolean expression, unlike a
%% guard, so `is_process_alive/1' is legal here. These conns are always local.
pick_alive_target(#state{members = M}) ->
    Alive = [{NodeId, ConnPid}
             || {NodeId, #{state := alive, conn_pid := ConnPid}}
                    <- maps:to_list(M),
                is_pid(ConnPid),
                erlang:is_process_alive(ConnPid)],
    pick_one(Alive).

pick_one([]) -> none;
pick_one(L)  ->
    {NodeId, ConnPid} = lists:nth(rand:uniform(length(L)), L),
    {ok, NodeId, ConnPid}.

send_ping(Round, Target, ConnPid,
          #state{identity = Id, config = #{ping_timeout_ms := Ms},
                 probes = P} = S) ->
    Ping = macula_frame:swim_ping(#{round => Round, incarnation => 0,
                                    piggyback => []}),
    Signed = macula_frame:sign(Ping, Id),
    _ = macula_peering:send_frame(ConnPid, Signed),
    Ref = erlang:send_after(Ms, self(), {ping_timeout, Round, Target}),
    Probe = #{round => Round, target => Target, timer_ref => Ref},
    S#state{probes = P#{Round => Probe},
            probes_sent = S#state.probes_sent + 1}.

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
            Ack = macula_frame:swim_ack(#{
                round       => Round,
                responder   => S1#state.self_node_id,
                incarnation => 0,
                piggyback   => []
            }),
            Signed = macula_frame:sign(Ack, Id),
            _ = macula_peering:send_frame(ConnPid, Signed),
            S1;
        _ ->
            S1
    end.

%% Matching ACK cancels the in-flight probe and marks sender alive.
%%
%% A NON-matching ACK still proves life, and used to be dropped on the floor.
%% `on_ping_timeout/3' has already done `maps:take' on the round, so an ACK
%% arriving even microseconds late matches nothing — and the fall-through
%% discarded the one verified, signed, freshly-arrived frame proving the peer
%% we just suspected is alive. Production routes every frame through
%% `deliver_swim' verification before it reaches here, so its arrival is
%% evidence, not a rumour.
%%
%% This is the SWIM Lifeguard refutation rule: any verified message from a
%% suspect refutes the suspicion. It matters because a suspect is otherwise
%% unreachable by design — `pick_alive_target/1' selects only `alive' members,
%% so we never re-probe it, and the sole remaining rescue path is the suspect
%% happening to pick US out of its own membership. That gives a conversion
%% probability of (1 - 1/M)^3, which RISES with mesh size; see
%% `macula_swim_conversion_tests'. Refuting on a late ACK adds a rescue channel
%% proportional to actual traffic rather than to luck.
on_ack(From, #{round := Round}, #state{probes = P} = S) ->
    case maps:take(Round, P) of
        {#{target := Target, timer_ref := Ref}, NewP}
          when Target =:= From ->
            _ = erlang:cancel_timer(Ref),
            maybe_touch_alive(From, S#state{probes = NewP,
                                            acks_matched =
                                                S#state.acks_matched + 1});
        _ ->
            refute_if_suspect(From, S)
    end.

%% Only a SUSPECT is refuted. An ACK from a member already `alive' says nothing
%% new, and one from `confirmed_failed' must NOT silently resurrect it: that
%% verdict has already been published to the consumer, which has acted on it.
%% Recovery from a confirmed failure is a re-add through `add_peer/3', where it
%% is visible, not a side effect of a stray frame.
refute_if_suspect(From, #state{members = M} = S) ->
    resolve_refutation(maps:get(From, M, undefined), From, S).

resolve_refutation(#{state := suspect} = Member, From, S) ->
    touch_alive(From, Member, S#state{refuted = S#state.refuted + 1});
resolve_refutation(_Other, _From, S) ->
    S.

maybe_touch_alive(NodeId, #state{members = M} = S) ->
    case maps:get(NodeId, M, undefined) of
        undefined -> S;
        Member    -> touch_alive(NodeId, Member, S)
    end.

%%------------------------------------------------------------------
%% Notifications
%%------------------------------------------------------------------

notify_state_change(NodeId, State, #state{controlling_pid = Ctrl}) ->
    send_to_controller(resolve_pid(Ctrl), NodeId, State).

resolve_pid(Pid) when is_pid(Pid) -> Pid;
resolve_pid(Name) when is_atom(Name) -> erlang:whereis(Name).

send_to_controller(undefined, _NodeId, _State) ->
    %% Atom controller name not yet registered (e.g. observer hasn't
    %% booted). Drop the notification — SWIM keeps tracking; the next
    %% state change will surface once the consumer is up.
    ok;
send_to_controller(Pid, NodeId, State) when is_pid(Pid) ->
    Pid ! {macula_swim, member_state, NodeId, State},
    ok.
