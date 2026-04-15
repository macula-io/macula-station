#!/usr/bin/env bash
# fleet-deploy.sh — bump the station image tag in hecate-gitops.
#
# Commits + pushes a tag update so Podman's auto-update pulls it on
# each beam node's next reconciliation window. Waits until every
# node's `podman auto-update` run has completed before returning;
# fails fast if any node rejects the new image.
#
# Usage:
#   ./scripts/fleet-deploy.sh <version>
#   ./scripts/fleet-deploy.sh main         # floating tag
#
# This is a template. The full end-to-end flow depends on:
#   - A clean workspace clone of hecate-social/hecate-gitops/.
#   - SSH access to beam00-03 as user rl (key-based, no password).
#   - hecate-daemon systemd user service with auto-update = registry.
#
# Until the beam migration is complete (PLAN_DEFERRED_WORK.md), this
# script prints the commands an operator would run by hand.

set -euo pipefail

VERSION="${1:-main}"

cat <<EOF
fleet-deploy.sh template — run each step by hand:

1) Bump the image tag in hecate-gitops:

   cd ~/work/github.com/hecate-social/hecate-gitops
   sed -i "s|hecate-station:[^\\s]*|hecate-station:${VERSION}|" \\
       beam-cluster/hecate-station.container
   git add beam-cluster/hecate-station.container
   git commit -m "deploy(hecate-station): ${VERSION}"
   git push

2) Watch beam nodes pick up the new image (Podman auto-update
   polls every 60 s):

   for n in beam00 beam01 beam02 beam03; do
       echo "== \${n} =="
       ssh rl@\${n}.lab 'journalctl --user -u podman-auto-update -n 30'
   done

3) Confirm the running container matches the requested tag:

   for n in beam00 beam01 beam02 beam03; do
       ssh rl@\${n}.lab \\
           'podman inspect hecate-station --format "{{.ImageName}}"'
   done
EOF
