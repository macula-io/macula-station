%% @doc Chaos-harness primitives for CT + eunit + remsh.
%%
%% A minimal, composable toolkit for Phase 7 resilience scenarios.
%% Each function is synchronous, bounded, and safe to compose;
%% nothing here touches iptables or the kernel — host-level chaos
%% (partition via iptables, clock skew, BGP simulation) belongs to
%% the fleet-side runner on beam00–03 (see `scripts/fleet-ct.sh
%% beam'). The in-VM primitives here cover the scenarios we can
%% exercise across `peer' nodes without root:
%%
%% <ul>
%%   <li>`kill_pid/1' — `exit(Pid, kill)' + wait on a DOWN monitor.</li>
%%   <li>`stop_peer/1' — `peer:stop/1' wrapper that ignores the
%%       common "already-dead" races.</li>
%%   <li>`pause/1' / `resume/1' — `sys:suspend' / `sys:resume'
%%       around a gen_server. Pausing is a softer failure than kill:
%%       the process keeps its socket + state, it just stops
%%       servicing calls — a SWIM ping times out, a SWIM ack never
%%       gets built.</li>
%%   <li>`wait_until/2' — poll a predicate with a 50 ms cadence
%%       until it returns true or the budget expires.</li>
%%   <li>`wait_alive/3' / `wait_confirmed_failed/3' — specialised
%%       waits against a `hecate_swim' member list. The resilience
%%       scenarios always end with a check against one of these.</li>
%% </ul>
%%
%% The primitives are stateless library functions; compose them in
%% test bodies rather than setting up a shared chaos driver gen_server.
-module(fleet_chaos).

-export([
    kill_pid/1,
    stop_peer/1,
    pause/1,
    resume/1,
    wait_until/2,
    wait_alive/3,
    wait_confirmed_failed/3,
    member_state/2
]).

%%==================================================================
%% Liveness — stop or suspend a participant.
%%==================================================================

%% @doc Kill a local pid and wait up to 5 s for the DOWN message.
%% Returns `ok' whether the pid is already dead, dies on request, or
%% the wait times out — the caller is never blocked indefinitely.
-spec kill_pid(pid()) -> ok | timeout.
kill_pid(Pid) when is_pid(Pid) ->
    kill_alive(Pid, is_process_alive(Pid)).

kill_alive(_Pid, false) ->
    ok;
kill_alive(Pid, true) ->
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 5_000 -> timeout
    end.

%% @doc Stop a `peer'-module BEAM node. `peer:stop/1' on an already
%% stopped node returns an error shape we treat as benign here — a
%% previous test's teardown may have raced us.
-spec stop_peer(pid()) -> ok.
stop_peer(Ctl) when is_pid(Ctl) ->
    _ = catch peer:stop(Ctl),
    ok.

%% @doc Suspend a gen_server / gen_statem. The process keeps its
%% state + sockets; it just stops pulling messages. Useful to
%% simulate a frozen peer that answers nothing — SWIM will move it
%% through alive → suspect → confirmed_failed within the detector's
%% budget.
-spec pause(pid()) -> ok.
pause(Pid) when is_pid(Pid) ->
    sys:suspend(Pid).

-spec resume(pid()) -> ok.
resume(Pid) when is_pid(Pid) ->
    sys:resume(Pid).

%%==================================================================
%% Waits — bounded polls against observable station state.
%%==================================================================

%% @doc Poll `Pred/0' every 50 ms; return `ok' the first time it
%% is true, `timeout' if the budget expires. Never blocks longer
%% than `Ms'.
-spec wait_until(fun(() -> boolean()), non_neg_integer()) -> ok | timeout.
wait_until(Pred, Ms) when is_function(Pred, 0), is_integer(Ms), Ms >= 0 ->
    wait_step(safe_pred(Pred), Pred, Ms).

wait_step(true,  _Pred, _Ms)                -> ok;
wait_step(false, _Pred, Ms) when Ms =< 0    -> timeout;
wait_step(false, Pred, Ms)                  ->
    timer:sleep(50),
    wait_step(safe_pred(Pred), Pred, Ms - 50).

%% A predicate that raises (because a peer gen_server just died,
%% say) shouldn't abort the wait — treat it as "not yet true".
safe_pred(Pred) ->
    try Pred()
    catch _:_ -> false
    end.

%% @doc Wait for `NodeId' to appear as `alive' in the SWIM member
%% list of `Swim'.
-spec wait_alive(pid(), macula_identity:pubkey(), non_neg_integer()) ->
    ok | timeout.
wait_alive(Swim, NodeId, Ms) ->
    wait_until(fun() -> member_state(Swim, NodeId) =:= alive end, Ms).

%% @doc Wait for `NodeId' to be marked `confirmed_failed' in the
%% SWIM member list.
-spec wait_confirmed_failed(pid(), macula_identity:pubkey(),
                            non_neg_integer()) -> ok | timeout.
wait_confirmed_failed(Swim, NodeId, Ms) ->
    wait_until(fun() ->
        member_state(Swim, NodeId) =:= confirmed_failed
    end, Ms).

%% @doc Current SWIM state of `NodeId' on the given SWIM pid, or
%% `missing' if the node is not on the member list.
-spec member_state(pid(), macula_identity:pubkey()) ->
    hecate_swim:member_state() | missing.
member_state(Swim, NodeId) ->
    Match = [S || #{node_id := N, state := S} <- hecate_swim:members(Swim),
                  N =:= NodeId],
    case Match of
        [State] -> State;
        []      -> missing
    end.
