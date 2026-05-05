# Hecate Station — production Dockerfile.
#
# Builds a self-contained OTP 27 release of macula-station and
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

ARG OTP_VERSION=27.1.2
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

WORKDIR /build

# Copy build config first so the deps layer caches cleanly when only
# application source changes.
COPY rebar.config rebar.lock* ./

RUN rebar3 get-deps

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

# Health check uses the unauthenticated `/status' route on the admin
# port. Bearer auth is only required for `/admin/*' management
# routes; `/status' stays open for liveness probes.
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
    CMD curl -sf http://localhost:8443/status || exit 1

CMD ["./bin/macula_station", "foreground"]
