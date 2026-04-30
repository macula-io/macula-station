#!/usr/bin/env bash
# fleet-ct.sh — run the fleet Common Test suite in distributed mode.
#
# Two modes:
#
#   ./scripts/fleet-ct.sh local
#     Runs the `fleet_SUITE` peer-node tests on this workstation
#     (two BEAM VMs spawned locally). The suite skips under plain
#     `rebar3 ct` because it needs Erlang distribution on the parent
#     — this wrapper adds `--name` so the peers have somewhere to
#     dial home.
#
#   ./scripts/fleet-ct.sh beam
#     Runs the fuller fleet scenarios against beam00–03. Still a
#     template — see `docs/fleet-ct-beam.md` (TODO) for the
#     end-to-end wiring. The core moving parts are:
#       1. `scripts/fleet-deploy.sh` to commit the image tag to
#          `hecate-social/hecate-gitops' and wait for podman
#          auto-update to pick it up.
#       2. SSH into each beam node, run the station's self-test
#          binary (`macula-station self-test`).
#       3. Collect the CT report from `/bulk0/.hecate/reports/`.
#
# Exit 0 on success; non-zero on any failure.

set -euo pipefail

cd "$(dirname "$0")/.."

MODE="${1:-local}"

case "$MODE" in
    local)
        echo ">> Running fleet_SUITE locally with distribution enabled"
        exec rebar3 ct --suite=test/fleet_SUITE --name=ct_main
        ;;
    beam)
        echo ">> Fleet CT against beam cluster is a template — operator runs:"
        echo ">>   1. scripts/fleet-deploy.sh <version>"
        echo ">>   2. for node in beam00 beam01 beam02 beam03; do"
        echo ">>        ssh rl@\${node}.lab 'systemctl --user status hecate-daemon'"
        echo ">>      done"
        echo ">>   3. Run chaos scenarios in fleet_chaos.erl via remsh."
        echo ">> This path lands in 8.8.x alongside the beam cluster migration"
        echo ">> (see PLAN_DEFERRED_WORK.md)."
        exit 0
        ;;
    *)
        echo "usage: $0 [local|beam]" >&2
        exit 64
        ;;
esac
