#!/usr/bin/env bash
# Deep inspect of the top-memory anonymous gen_servers on a station.
set -eu

HOST="${1:?Usage: $0 <hostname> [N]}"
N="${2:-3}"
KEY="${SSH_KEY:-$HOME/.ssh/id_hetzner}"
SSH="ssh -i $KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@$HOST"

EXPR='
  Ps = erlang:processes(),
  M = [{element(2, process_info(P, memory)), P} || P <- Ps, process_info(P, memory) =/= undefined],
  Top = lists:sublist(lists:reverse(lists:sort(M)), '"$N"'),
  [begin
    Bins = case process_info(P, binary) of
      {binary, BL} ->
        BTotal = lists:sum([Sz || {_,Sz,_} <- BL]),
        {count, length(BL), total_bytes, BTotal};
      _ -> {none}
    end,
    Stack = case process_info(P, current_stacktrace) of
      {current_stacktrace, ST} -> lists:sublist(ST, 8);
      _ -> []
    end,
    Status = try sys:get_status(P, 1000) catch C:E -> {failed, C, E} end,
    Dict = element(2, process_info(P, dictionary)),
    Links = element(2, process_info(P, links)),
    Monitors = element(2, process_info(P, monitors)),
    Mod = case Status of
      {status, _, {module, M0}, _} -> M0;
      _ -> unknown
    end,
    {{pid, P},
     {memory_bytes, Mem},
     {bin_refs, Bins},
     {dictionary, Dict},
     {links, length(Links), Links},
     {monitors, length(Monitors)},
     {behaviour_module, Mod},
     {stack, Stack}}
  end || {Mem, P} <- Top].
'

echo "=== Top $N by memory (deep) on $HOST ==="
$SSH "docker exec macula-station bin/macula_station eval '$EXPR'"
