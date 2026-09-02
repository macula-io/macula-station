# Off-grid Pi mesh deployment recipe

**Status**: v0.1-draft
**Created**: 2026-05-03
**Audience**: anyone provisioning a Raspberry Pi to participate in an off-grid macula-station mesh

## What this recipe gives you

A Raspberry Pi running a macula-station, joined to a wifi mesh of other Pis, addressable over IPv6, operating with no internet connectivity, no ISP, no DNS, no DHCP server.

What it explicitly does NOT change: the macula-station Erlang code, the macula-net QUIC transport, the wire protocol. Everything above the link layer is unchanged from the helsinki/nuremberg/paris fleet deployment. This is purely an **operational recipe** for putting a macula-station on top of a self-organising wifi mesh instead of on top of public-internet IPv4/IPv6.

## How the layers stack

```
Application (hecate-daemon, plugins, etc.)
    ↑
macula-net identity-addressable substrate
    ↑      (cryptographic addresses, per-realm DHT, route_packet)
QUIC over IPv6
    ↑
IPv6 ULA on bat0 (or mesh0)
    ↑
BATMAN-adv (or 802.11s) — L2 mesh routing across multiple wifi hops
    ↑
wifi link (6 GHz / 2.4-5 GHz / 60 GHz depending on adapter)
    ↑
hardware: Pi + USB or PCIe wifi adapter
```

The bottom three layers (hardware, wifi, BATMAN-adv, IPv6 ULA) are what this recipe sets up. Everything from QUIC up is unchanged from the existing fleet deployment.

## Hardware

### Recommended (practical, available now)

- **Pi 5** (4 GB or 8 GB) — adequate CPU and RAM for macula-station + a few hecate-daemons
- **Pi 4** (4 GB) acceptable for relay-only nodes (no hecate-daemons attached)
- **MicroSD**: 32 GB minimum, A2 class for I/O performance; or NVMe via the Pi 5 PCIe HAT for production
- **Wifi adapter**:
  - **6 GHz (Wifi 6E)** recommended for general-purpose mesh. Adapter examples: Comfast CF-953AX, Mercusys MA86XE. USB 3.0.
  - **5 GHz fallback** acceptable; mature drivers, lower throughput
- **Power**: official Pi 5 PSU minimum (5 V / 5 A); for off-grid solar/battery rigs see future addendum

### Aspirational (not in this recipe)

- **60 GHz (802.11ay/WiGig)** — bleeding-edge, line-of-sight, very high throughput, sparse driver support on Pi-class hardware. Add as a separate addendum once a known-good adapter exists.
- **Long-range directional antennas** for multi-kilometre links — out of scope here; covered by point-to-point bridge configurations layered on top of this recipe.

## Operating system

- **Raspberry Pi OS Bookworm 64-bit (Lite)** is the reference base. No desktop; minimal services.
- Kernel 6.6 or later (BATMAN-adv is in-tree; modern wifi 6E driver support).
- **NixOS for ARM64** is an acceptable alternative for operators who want declarative provisioning; equivalent steps but expressed as a Nix module. Out of scope for this recipe; build it once we have an operator who needs it.

## Network architecture

### IPv6 addressing

Each Pi assigns itself an IPv6 ULA address from a `fd00::/8` prefix. This recipe defines:

```
Mesh prefix:        fd60:6d65:7368::/48      (chosen for the mesh; "60 mesh" mnemonic)
Per-Pi address:     fd60:6d65:7368::<48-bit suffix derived from the wifi MAC>
```

The 48-bit suffix is the lower 48 bits of the wifi adapter's MAC address (EUI-48 truncation). Deterministic, stable across reboots, no allocation needed.

This prefix is **private to the mesh**. It is not routed to the public internet (which would refuse to route ULAs anyway). Other Pi meshes in other locations can choose other ULA prefixes; the address derivation does not require global coordination.

### Wifi mesh routing

**BATMAN-adv** is the default L2 mesh routing protocol. It runs in the kernel (module `batman-adv`), exposes a virtual interface (`bat0`) that behaves like a normal ethernet interface, and routes frames across multi-hop wifi paths transparently. From userspace the mesh looks like a single broadcast domain.

Each Pi:
1. Puts its wifi adapter into ad-hoc (IBSS) mode on a chosen channel + SSID + cell ID
2. Adds the wifi interface to the BATMAN-adv mesh on `bat0`
3. Configures `bat0` with its IPv6 ULA address
4. Starts `batadv-vis` for diagnostics (optional)

After this, all Pis on the mesh can reach each other at L3 over `bat0` regardless of how many wifi hops separate them. macula-station sees `bat0` as a normal IPv6 interface.

### Channel + SSID

```
Wifi channel:    36 (5 GHz) or 1/2/3 (6 GHz, depending on adapter capability)
Mesh SSID:       macula-mesh
Cell ID:         02:00:00:00:00:01
```

These are defaults. Operators standing up a separate mesh should pick a different SSID + cell ID to avoid colliding with other macula meshes that might be in radio range.

## Provisioning steps

### 1. Flash and boot

Flash Raspberry Pi OS Bookworm Lite 64-bit to MicroSD using the official imager. Pre-configure SSH (key-based; no password) via the imager's advanced options. First boot, ssh in.

### 2. Install dependencies

```sh
sudo apt update
sudo apt install -y batctl iw rfkill iproute2 wireless-tools
sudo modprobe batman-adv
echo batman-adv | sudo tee /etc/modules-load.d/batman-adv.conf
```

### 3. Run the provisioning script

```sh
git clone https://github.com/macula-io/macula-station.git
cd macula-station/deployment/pi-mesh
sudo ./provision.sh
```

The script does:
- Configures the wifi adapter into IBSS mode on the configured channel + SSID + cell ID
- Adds the wifi interface to BATMAN-adv on `bat0`
- Computes the ULA suffix from the wifi MAC
- Assigns the ULA address to `bat0`
- Writes a systemd unit so the mesh comes up at boot
- Verifies the Pi can see at least one BATMAN-adv neighbor (if any others are powered on)

### 4. Install macula-station

If not pre-installed, build the macula-station Erlang release on the Pi (slow, ~20 minutes) or cross-compile from a faster host and copy the release. The standard `_build/prod/rel/` shape applies.

### 5. Configure macula-station

Edit `~/macula-station/releases/<vsn>/sys.config` to bind to the Pi's mesh address:

```erlang
{macula_transport, [
    {bind_addr, "fd60:6d65:7368::<this-pi-suffix>"},
    {bind_port, 4433}
]}.
```

For the first Pi (the bootstrap node), no peer config is needed — it waits for others to dial in. For subsequent Pis, add at least one bootstrap peer:

```erlang
{macula_bootstrap, [
    {seeds, ["fd60:6d65:7368::<bootstrap-pi-suffix>:4433"]}
]}.
```

After two or three Pis are up, the per-realm DHT bootstraps itself and further Pis joining can use any existing peer as a seed.

### 6. Start macula-station

```sh
sudo systemctl enable --now macula-station
journalctl -u macula-station -f
```

## Verification

### Mesh connectivity

```sh
batctl o                  # Show originator table; should list neighbours
ip -6 addr show bat0      # Should show the assigned ULA
ping6 fd60:6d65:7368::<other-pi-suffix>   # Should succeed
```

### macula-station

```sh
systemctl status macula-station
journalctl -u macula-station --since="5 min ago"
```

Look for `transport_quic` startup messages binding to the mesh ULA + port. No `connection refused` or `network unreachable` errors after the bootstrap interval.

### End-to-end identity addressing

If at least two Pis are running macula-station and have at least one hecate-daemon attached each, the standard alice ↔ bob round-trip from the lan-demo scripts works unchanged: `bash scripts/lan-demo.sh auto-alice-bob` against the Pi mesh produces the same green output as it does on the helsinki/nuremberg fleet.

## What this recipe does NOT yet cover

- **One-flash Pi image** (a pre-built image with all the above baked in). Future addendum once the recipe stabilises.
- **Solar / battery power** for off-grid hardware. Out of scope here.
- **60 GHz adapters**. Future addendum when a Pi-compatible 60 GHz adapter is selected.
- **Mesh-to-internet bridging** (one Pi has internet uplink, others don't). Out of scope; can be added later via a standard NAT64 + gateway config without changes to macula-station.
- **Field-deploy security**: physical tamper-resistance, full-disk encryption, secure-boot. Add when an actual deployment requires it.
- **Realm management** for the mesh. The mesh as a substrate is realm-agnostic; if a Pi-mesh deployment wants to use a specific realm, the realm's hecate-realm service runs separately (probably on one of the Pis or on an internet-connected node) and Pis attach hecate-daemons holding that realm's identities. Standard macula-net pattern; nothing mesh-specific.

## Validation roadmap

To validate this recipe is real (not just paper):

1. **Two-Pi bench test**: two Pis on a desk, mesh up, ping6 across, macula-station up on both, alice ↔ bob demo green over the mesh. ~1 day of work once hardware is on hand.
2. **Three-Pi multi-hop test**: three Pis, middle one routes for the other two. Verifies BATMAN-adv multi-hop works and macula-net is transparent to the multi-hop. ~1 day.
3. **Field test**: Pis in different rooms / floors / line-of-sight outdoor short-range. Validates real wifi propagation behaviour. ~1-2 days.
4. **Long-haul soak**: leave a 3-Pi mesh running for 24 hours with the macula-net soak harness pointed at it. Validates stability under non-ideal radio conditions. ~1 day setup, 24 hours running, ~1 day analysis.

Total to "we have a proven Pi mesh deployment recipe": roughly 5-7 days of work spread across whenever hardware is procured. None of it requires further macula-net code work.

## References

- BATMAN-adv documentation: https://www.open-mesh.org/projects/batman-adv/wiki
- Raspberry Pi OS: https://www.raspberrypi.com/software/operating-systems/
- macula-station: `macula-internal/macula-station/`
- Existing fleet deployment patterns: `macula-internal/macula-station/scripts/fleet-deploy.sh` (the operational shape this recipe parallels for off-grid)
