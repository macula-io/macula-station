#!/bin/bash
# One-shot snapshot of macula-station memory + fanout pid info.
# Usage: watch_memory.sh <hostname>
set -eu
HOST="${1:?Usage: $0 <hostname>}"
KEY="${SSH_KEY:-$HOME/.ssh/id_hetzner}"

ssh -i "$KEY" -o ConnectTimeout=10 -o BatchMode=yes \
    -o StrictHostKeyChecking=no "root@$HOST" \
    "docker exec macula-station bin/macula_station eval '
        M = erlang:memory(),
        F = case whereis(macula_station_record_fanout) of
                undefined -> no_fanout;
                P -> {fanout,
                      element(2, process_info(P, memory)),
                      element(2, process_info(P, message_queue_len))}
            end,
        {total, proplists:get_value(total, M),
         procs, proplists:get_value(processes, M),
         pcount, erlang:system_info(process_count),
         F}.
    '"
