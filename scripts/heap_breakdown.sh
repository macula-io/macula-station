#!/usr/bin/env bash
set -eu
HOST="${1:?Usage: $0 <hostname>}"
KEY="${SSH_KEY:-$HOME/.ssh/id_hetzner}"
SSH="ssh -i $KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@$HOST"

EXPR='
  Top = lists:sublist(lists:reverse(lists:sort(
    [{element(2, process_info(P, memory)), P} || P <- erlang:processes(),
     process_info(P, memory) =/= undefined])), 3),
  [begin
    Info = process_info(P, [memory, total_heap_size, heap_size, stack_size,
                            min_heap_size, garbage_collection,
                            message_queue_len, status, registered_name]),
    {pid, P, Info}
  end || {_, P} <- Top].
'

echo "=== Heap breakdown of top 3 memory hogs on $HOST ==="
$SSH "docker exec macula-station bin/macula_station eval '$EXPR'"
