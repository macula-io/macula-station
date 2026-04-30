#!/usr/bin/env bash
# Read-only forensics on a running macula-station container.
# Usage: forensics.sh <hostname>
set -eu

HOST="${1:?Usage: $0 <hostname>}"
KEY="${SSH_KEY:-$HOME/.ssh/id_hetzner}"
SSH="ssh -i $KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@$HOST"

run_eval() {
  local label="$1"; shift
  local expr="$1"
  echo "=== $label ==="
  $SSH "docker exec macula-station bin/macula_station eval '$expr'"
  echo
}

echo "=== Container ==="
$SSH "docker ps --filter name=macula-station --format '{{.Names}}\t{{.Image}}\t{{.Status}}'"
echo

echo "=== Host top ==="
$SSH "top -bn1 | head -10"
echo

run_eval "VM memory" 'erlang:memory().'

run_eval "Process count + ports" '{erlang:system_info(process_count), erlang:system_info(port_count)}.'

run_eval "Scheduler online + utilization sample" '
  Schedulers = erlang:system_info(schedulers_online),
  S1 = erlang:statistics(scheduler_wall_time),
  timer:sleep(500),
  S2 = erlang:statistics(scheduler_wall_time),
  Util = case {S1, S2} of
    {undefined, _} ->
      erlang:system_flag(scheduler_wall_time, true),
      timer:sleep(500),
      A = erlang:statistics(scheduler_wall_time),
      timer:sleep(500),
      B = erlang:statistics(scheduler_wall_time),
      [{Id, (Bb-Ab) / max(1,(Bt-At))} || {Id,Ab,At} <- lists:sort(A), {Id2,Bb,Bt} <- lists:sort(B), Id =:= Id2];
    _ ->
      [{Id, (Bb-Ab) / max(1,(Bt-At))} || {Id,Ab,At} <- lists:sort(S1), {Id2,Bb,Bt} <- lists:sort(S2), Id =:= Id2]
  end,
  {schedulers, Schedulers, util, Util}.
'

run_eval "Top 10 by message_queue_len" '
  Ps = [P || P <- erlang:processes()],
  Q = [{element(2, process_info(P, message_queue_len)), P} || P <- Ps, process_info(P, message_queue_len) =/= undefined],
  Top = lists:sublist(lists:reverse(lists:sort(Q)), 10),
  [{Len, P, process_info(P, [registered_name, current_function, initial_call, status, total_heap_size])} || {Len, P} <- Top].
'

run_eval "Top 10 by total memory" '
  Ps = [P || P <- erlang:processes()],
  M = [{element(2, process_info(P, memory)), P} || P <- Ps, process_info(P, memory) =/= undefined],
  Top = lists:sublist(lists:reverse(lists:sort(M)), 10),
  [{Mem, P, process_info(P, [registered_name, current_function, initial_call, message_queue_len])} || {Mem, P} <- Top].
'

run_eval "Top 10 by reductions" '
  Ps = [P || P <- erlang:processes()],
  R = [{element(2, process_info(P, reductions)), P} || P <- Ps, process_info(P, reductions) =/= undefined],
  Top = lists:sublist(lists:reverse(lists:sort(R)), 10),
  [{Red, P, process_info(P, [registered_name, current_function, initial_call])} || {Red, P} <- Top].
'

run_eval "ETS tables top 15 by memory (words)" '
  T = [{N, ets:info(N, size), ets:info(N, memory)} || N <- ets:all()],
  T2 = [{N, S, M} || {N, S, M} <- T, M =/= undefined],
  lists:sublist(lists:reverse(lists:keysort(3, T2)), 15).
'
