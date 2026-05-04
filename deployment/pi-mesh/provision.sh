#!/usr/bin/env bash
#
# Off-grid Pi mesh provisioning script.
#
# Configures a Raspberry Pi running Raspberry Pi OS Bookworm to participate
# in a BATMAN-adv wifi mesh with an IPv6 ULA derived from the wifi MAC.
#
# Designed to be idempotent: safe to re-run.
#
# Run as root: sudo ./provision.sh
#
# See RECIPE.md in the same directory for the full deployment recipe and
# rationale.

set -euo pipefail

# ============================================================================
# Configuration (override via env vars)
# ============================================================================

WIFI_IFACE="${WIFI_IFACE:-wlan1}"          # USB wifi adapter; built-in is wlan0
MESH_SSID="${MESH_SSID:-macula-mesh}"
MESH_CELL="${MESH_CELL:-02:00:00:00:00:01}"
MESH_CHANNEL="${MESH_CHANNEL:-36}"          # 5 GHz; pick 6 GHz channel if adapter supports
MESH_PREFIX="${MESH_PREFIX:-fd60:6d65:7368::}"   # /48 ULA prefix for the mesh
BAT_IFACE="${BAT_IFACE:-bat0}"
SYSTEMD_UNIT="${SYSTEMD_UNIT:-macula-pi-mesh.service}"

# ============================================================================
# Logging
# ============================================================================

log()  { printf '\033[1;34m[provision]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[provision]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[provision]\033[0m %s\n' "$*" >&2; exit 1; }

# ============================================================================
# Pre-flight checks
# ============================================================================

[[ $EUID -eq 0 ]] || die "Must run as root."

command -v batctl >/dev/null || die "batctl not installed; apt install batctl iw rfkill"
command -v iw     >/dev/null || die "iw not installed; apt install iw"

modprobe batman-adv 2>/dev/null || die "Could not load batman-adv kernel module."

ip link show "$WIFI_IFACE" >/dev/null 2>&1 \
  || die "Wifi interface $WIFI_IFACE not found. Set WIFI_IFACE env var."

# ============================================================================
# Compute the per-Pi IPv6 suffix from the wifi MAC
# ============================================================================

WIFI_MAC=$(cat "/sys/class/net/$WIFI_IFACE/address")
[[ -n "$WIFI_MAC" ]] || die "Could not read MAC address for $WIFI_IFACE."

# Strip colons; take last 12 hex chars (full MAC); reformat as IPv6 suffix.
# This produces e.g. mac=aa:bb:cc:dd:ee:ff -> suffix=aabb:ccdd:eeff
SUFFIX=$(echo "$WIFI_MAC" \
  | tr -d ':' \
  | sed -E 's/(.{4})(.{4})(.{4})/\1:\2:\3/')

MY_ADDR="${MESH_PREFIX}${SUFFIX}/64"

log "Wifi MAC:        $WIFI_MAC"
log "IPv6 address:    $MY_ADDR"

# ============================================================================
# Take wifi interface down and configure into IBSS mode
# ============================================================================

log "Bringing $WIFI_IFACE down..."
ip link set "$WIFI_IFACE" down

# Some Pi wifi chipsets need to be unblocked first.
rfkill unblock wifi || true

log "Configuring $WIFI_IFACE in ad-hoc (IBSS) mode on channel $MESH_CHANNEL..."

# The interface must be DOWN to set its type and to join an IBSS.
iw dev "$WIFI_IFACE" set type ibss
ip link set "$WIFI_IFACE" up

# Join the IBSS network. This is idempotent if the cell + ssid match.
iw dev "$WIFI_IFACE" ibss join "$MESH_SSID" \
    $((2412 + (MESH_CHANNEL - 1) * 5)) \
    fixed-freq "$MESH_CELL" \
    || warn "ibss join returned non-zero (already in IBSS?); continuing."

# ============================================================================
# Add wifi interface to BATMAN-adv mesh
# ============================================================================

log "Adding $WIFI_IFACE to BATMAN-adv mesh ($BAT_IFACE)..."
batctl if add "$WIFI_IFACE" || warn "$WIFI_IFACE already in BATMAN mesh."

ip link set "$BAT_IFACE" up

# ============================================================================
# Assign IPv6 ULA address to bat0
# ============================================================================

log "Assigning $MY_ADDR to $BAT_IFACE..."

# Remove any prior address in our prefix (idempotency).
ip -6 addr show "$BAT_IFACE" \
  | awk -v p="${MESH_PREFIX%::}" '/inet6/ && $2 ~ p {print $2}' \
  | while read -r OLD; do
      ip -6 addr del "$OLD" dev "$BAT_IFACE" || true
    done

ip -6 addr add "$MY_ADDR" dev "$BAT_IFACE"

# ============================================================================
# Install systemd unit so the mesh comes up at boot
# ============================================================================

UNIT_PATH="/etc/systemd/system/$SYSTEMD_UNIT"

log "Installing systemd unit at $UNIT_PATH..."

cat >"$UNIT_PATH" <<UNIT
[Unit]
Description=macula off-grid Pi mesh
After=network-pre.target
Before=network-online.target
DefaultDependencies=no

[Service]
Type=oneshot
RemainAfterExit=yes
Environment=WIFI_IFACE=$WIFI_IFACE
Environment=MESH_SSID=$MESH_SSID
Environment=MESH_CELL=$MESH_CELL
Environment=MESH_CHANNEL=$MESH_CHANNEL
Environment=MESH_PREFIX=$MESH_PREFIX
Environment=BAT_IFACE=$BAT_IFACE
ExecStart=$(realpath "$0")

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable "$SYSTEMD_UNIT"

# ============================================================================
# Verify
# ============================================================================

log "Mesh interface state:"
ip -6 addr show "$BAT_IFACE"

log "BATMAN-adv neighbours (may be empty if no other Pis are powered on yet):"
batctl o || true

log "Done. The Pi will now rejoin the mesh on every boot."
log "Next: install macula-station and configure it to bind to $MY_ADDR."
log "See RECIPE.md in this directory for the macula-station configuration steps."
