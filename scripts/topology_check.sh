#!/bin/bash
# One-shot topology + fanout snapshot across realm + 3 stations.
# Single line of output suitable for piping into Monitor.
set -eu

# Realm topology counts via Elixir RPC
REALM=$(ssh -i ~/.ssh/id_rsa -o ConnectTimeout=10 -o BatchMode=yes \
        -o StrictHostKeyChecking=no root@macula.io \
  "docker exec macula-realm /app/bin/macula_realm rpc \"
    s = :sys.get_state(MaculaRealm.Topology.MeshSubscriber)
    count = fn v -> cond do is_map(v) -> map_size(v); is_list(v) -> length(v); true -> 0 end end
    IO.write(
      \\\"R=#{count.(Map.get(s, :stations))} \\\" <>
      \\\"D=#{count.(Map.get(s, :daemons))} \\\" <>
      \\\"sites=#{count.(Map.get(s, :sites))}\\\"
    )
  \" 2>&1" 2>&1 | grep -E '^R=' | head -1)

# Each station: total mem MB + fanout heap bytes + fanout mailbox
station_mem() {
    local host=$1 key=$2
    ssh -i ~/.ssh/$key -o ConnectTimeout=10 -o BatchMode=yes \
        -o StrictHostKeyChecking=no root@$host \
        "docker exec hecate-station bin/hecate_station eval '
            M = erlang:memory(),
            T = proplists:get_value(total, M) div (1024*1024),
            F = case whereis(hecate_station_record_fanout) of
                undefined -> 0;
                P -> {element(2, process_info(P, memory)),
                      element(2, process_info(P, message_queue_len))}
            end,
            {T, F}.
        '" 2>/dev/null | tr -d '\r\n'
}

H=$(station_mem relays-hetzner-helsinki.macula.io id_hetzner)
N=$(station_mem relays-hetzner-nuremberg.macula.io id_hetzner)
P=$(station_mem relays-linode-paris.macula.io id_ed25519)

echo "$REALM | helsinki=$H | nuremberg=$N | paris=$P"
