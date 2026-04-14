# Hecate Station — Phase 0 skeleton Dockerfile.
# No runtime behaviour yet. Builds an OTP 27 release of the empty skeleton.

ARG OTP_VSN=27.0
ARG ALPINE_VSN=3.20

# ---- builder ----
FROM erlang:${OTP_VSN}-alpine AS builder

RUN apk add --no-cache git build-base

WORKDIR /build
COPY rebar.config ./
COPY apps ./apps
COPY config ./config

RUN rebar3 as prod release

# ---- runtime ----
FROM alpine:${ALPINE_VSN}

RUN apk add --no-cache libstdc++ ncurses openssl

WORKDIR /opt/hecate_station
COPY --from=builder /build/_build/prod/rel/hecate_station ./

ENV PATH=/opt/hecate_station/bin:$PATH
CMD ["hecate_station", "foreground"]
