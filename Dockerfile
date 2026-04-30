# Hecate Station — production Dockerfile.
#
# Builds a self-contained OTP 27 release of macula-station and
# packages it in a slim Debian runtime. The macula SDK includes a
# Quinn-based QUIC NIF written in Rust, so the builder stage needs
# the Rust toolchain alongside Erlang.
#
# Build: docker build -t ghcr.io/macula-io/macula-station:latest .
# Run:   docker run --network=host \
#          -e MACULA_RELAY_IDENTITIES="..." \
#          -e MACULA_QUIC_PORT=4433 \
#          -e MACULA_TLS_CERTFILE=/certs/cert.pem \
#          -e MACULA_TLS_KEYFILE=/certs/key.pem \
#          -e MACULA_ADMIN_TOKEN=... \
#          -v /path/to/certs:/certs:ro \
#          ghcr.io/macula-io/macula-station:latest

ARG OTP_VERSION=27.1.2
ARG DEBIAN_VERSION=bookworm-20241016-slim
ARG BUILDER_IMAGE="erlang:${OTP_VERSION}-slim"
ARG RUNNER_IMAGE="debian:${DEBIAN_VERSION}"

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
    mkdir -p /var/lib/hecate && chown app:app /var/lib/hecate

COPY --from=builder --chown=app:app \
     /build/_build/prod/rel/macula_station ./

USER app

# Box-secret (per-identity keypair derivation) lives in /var/lib/hecate
# by default; operators can override via the `box_secret_path' env
# var pair (`HECATE_STATION_BOX_SECRET_PATH' if exposed).
ENV HECATE_STATION_BOX_SECRET_PATH=/var/lib/hecate/box-secret

# Multi-identity boot is opt-in: operators set MACULA_RELAY_IDENTITIES
# in their docker-compose env. Single-identity legacy boot still works
# but is unconfigured at the image level — sys.config provides only
# the supervision-tree skeleton.

# Expose QUIC port (UDP) + admin/health port (TCP).
EXPOSE 4433/udp
EXPOSE 8443

# Health check uses the unauthenticated `/status' route on the admin
# port. Bearer auth is only required for `/admin/*' management
# routes; `/status' stays open for liveness probes.
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
    CMD curl -sf http://localhost:8443/status || exit 1

CMD ["./bin/macula_station", "foreground"]
