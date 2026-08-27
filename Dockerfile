# Hecate Station — production Dockerfile.
#
# Builds a self-contained OTP 28 release of macula-station and
# packages it in a slim Debian runtime. The macula SDK includes a
# Quinn-based QUIC NIF written in Rust, so the builder stage needs
# the Rust toolchain alongside Erlang.
#
# Build: docker build -t ghcr.io/macula-io/macula-station:latest .
# Run:   docker run --network=host \
#          -e MACULA_STATION_CONFIG=/etc/macula-station/config.json \
#          -v /path/to/config.json:/etc/macula-station/config.json:ro \
#          -v /path/to/certs:/certs:ro \
#          -v station_data:/var/lib/macula/station \
#          ghcr.io/macula-io/macula-station:latest
#
# config.json shape (data_dir, bind, port, certfile, keyfile required;
# everything else optional — see macula_station_config.erl):
#   {
#     "data_dir": "/var/lib/macula/station",
#     "bind":     "2600:3c1a:e001:19::be:01",
#     "port":     4433,
#     "certfile": "/certs/.../wildcard_.macula.io.crt",
#     "keyfile":  "/certs/.../wildcard_.macula.io.key",
#     "geo": { "hostname": "station-be-brussels.macula.io",
#              "city": "Brussels", "country": "BE",
#              "lat": 50.8503, "lng": 4.3517 }
#   }

ARG OTP_VERSION=28.1
ARG DEBIAN_VERSION=bookworm-20241016-slim
ARG BUILDER_IMAGE="docker.io/library/erlang:${OTP_VERSION}-slim"
ARG RUNNER_IMAGE="docker.io/library/debian:${DEBIAN_VERSION}"

# =============================================================================
# BUILD STAGE
# =============================================================================
FROM ${BUILDER_IMAGE} AS builder

RUN apt-get update -y && apt-get install -y \
    build-essential \
    git \
    curl \
    libssl-dev \
    pkg-config \
    && apt-get clean && rm -f /var/lib/apt/lists/*_*

# Quinn QUIC NIF in macula needs a Rust toolchain to build.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

# Build the macula_quic NIF FROM SOURCE instead of downloading the precompiled
# libmacula_quic.so from the GitHub release. The fetched release artifact has
# bitten us before (the v4.8.0 one hung on every connect and took the realm
# dark); its provenance is decoupled from this build. Rust is installed above,
# so compile it here. Matches macula-realm/Dockerfile.prod. See fetch-nif.sh.
ENV MACULA_FORCE_SOURCE_BUILD=1

WORKDIR /build

# Copy build config first so the deps layer caches cleanly when only
# application source changes.
COPY rebar.config rebar.lock* ./

# rebar.lock is deliberately gitignored (this repo floats on `~>` ranges,
# not exact pins) and is never present in a fresh checkout, so this layer
# is keyed on rebar.config's content alone. `upgrade` (not just
# `get-deps`) is meant to force a real check against the current hex.pm
# index every build — but that only happens if this RUN instruction
# actually executes. Docker's build cache (cache-from/cache-to: type=gha
# in ci.yml) skips re-running a RUN layer whenever its preceding layers
# and its own command text are both unchanged, which they are for as
# long as rebar.config's dependency line stays the same — so without the
# CACHEBUST below, `upgrade`'s hex.pm check silently never ran on a
# cache-hit build, shipping whatever hex release was fetched on the
# FIRST such build forever after. Confirmed live: a published macula
# patch release sat un-picked-up through a full rebuild+redeploy cycle
# on 2026-08-27 because of exactly this. ci.yml passes a fresh value
# (the commit SHA) on every build, so this layer's cache is genuinely
# invalidated instead of merely appearing to be.
ARG CACHEBUST=1
RUN echo "cachebust=${CACHEBUST}" && rebar3 get-deps && rebar3 upgrade --all

COPY apps   ./apps
COPY config ./config

RUN rebar3 as prod release

# =============================================================================
# RUNTIME STAGE
# =============================================================================
FROM ${RUNNER_IMAGE}

RUN apt-get update -y && \
    apt-get install -y \
    libstdc++6 \
    openssl \
    libncurses5 \
    locales \
    ca-certificates \
    curl \
    && apt-get clean && rm -f /var/lib/apt/lists/*_*

RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

WORKDIR /opt/macula_station

RUN useradd --create-home --shell /bin/bash app && \
    mkdir -p /var/lib/macula/station /etc/macula-station && \
    chown -R app:app /var/lib/macula /etc/macula-station

COPY --from=builder --chown=app:app \
     /build/_build/prod/rel/macula_station ./

USER app

# Single-identity boot. Operators mount a JSON config file and point
# MACULA_STATION_CONFIG at it. Multi-identity (MACULA_RELAY_IDENTITIES)
# was removed 2026-05-04 and the loader hard-rejects it at boot.
ENV MACULA_STATION_CONFIG=/etc/macula-station/config.json

# Erlang node name — substituted into vm.args at release start.
# Override per-container when multiple stations share an EPMD on the
# same host (e.g. network_mode: host with two containers per box).
ENV MACULA_NODE_NAME=macula_station
ENV RELX_REPLACE_OS_VARS=true

# Expose QUIC port (UDP) + admin/health port (TCP).
EXPOSE 4433/udp
EXPOSE 8443

# Health check uses the unauthenticated `/wire' route on the admin port.
# Bearer auth is only required for `/admin/*' management routes; the
# readout routes stay open for liveness probes.
#
# ⚠ It used to target `/status', which could not fail. `curl -f' keys off
# the HTTP STATUS CODE, and `/status' is hardcoded 200 — so a station
# reporting `healthy: false' in its own body was still GREEN to Docker.
# That is not hypothetical: station-it-milan passed this check every 30
# seconds for 30 hours on 2026-08-13 while receiving every packet sent to
# it and answering none.
#
# `/wire' returns 503 when the kernel is holding datagrams on our
# listener socket that we are not dispatching. See
# plans/PLAN_WIRE_LIVENESS_TRIPWIRE.md.
#
# ⚠ Going red is NOT sufficient on its own: the restart policy on the
# station boxes is `unless-stopped', which restarts on EXIT and not on
# UNHEALTHY, and watchtower only chases registry digests. This makes the
# fault VISIBLE in `docker ps' and to the fleet scripts; commit 3 of the
# plan is what makes it act.
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
    CMD curl -sf http://localhost:8443/wire || exit 1

CMD ["./bin/macula_station", "foreground"]
