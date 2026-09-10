# syntax=docker/dockerfile:1

ARG NODE_IMAGE=docker.io/library/node:22-bookworm-slim
ARG GO_IMAGE=docker.io/library/golang:1.26-trixie
ARG RUNTIME_IMAGE=docker.io/library/debian:trixie-slim
ARG VERSION=dev

# Build the dashboard that onderzeeerd embeds at compile time.
FROM ${NODE_IMAGE} AS web-build
WORKDIR /src/web

COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY web/ ./
RUN npm run build

# Build both binaries; the client is also used for the health check.
FROM ${GO_IMAGE} AS go-build
ARG VERSION
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
COPY --from=web-build /src/internal/webui/dist/ ./internal/webui/dist/

RUN mkdir -p /out \
    && go build -mod=readonly -trimpath -buildvcs=false \
        -ldflags="-s -w -X main.Version=${VERSION}" \
        -o /out/onderzeeerd ./cmd/onderzeeerd \
    && go build -mod=readonly -trimpath -buildvcs=false \
        -ldflags="-s -w -X main.Version=${VERSION}" \
        -o /out/onderzeeer ./cmd/onderzeeer

# Keep a normal Linux userspace for command-executor pipelines.
# Add workload-specific executables here, or in a derived image.
FROM ${RUNTIME_IMAGE} AS runtime
ARG VERSION
ARG ONDERZEEER_UID=1000
ARG ONDERZEEER_GID=1000

LABEL org.opencontainers.image.title="onderzeeerd" \
      org.opencontainers.image.description="Onderzeeer file-triggered job daemon" \
      org.opencontainers.image.source="https://github.com/GerhardOfRivia/onderzeeer" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="MIT"

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        passwd \
        tini \
        tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid "${ONDERZEEER_GID}" onderzeeer \
    && useradd --uid "${ONDERZEEER_UID}" --gid "${ONDERZEEER_GID}" \
        --create-home --home-dir /home/onderzeeer \
        --shell /usr/sbin/nologin --no-log-init onderzeeer \
    && install -d -m 0700 -o onderzeeer -g onderzeeer /run/onderzeeer \
    && install -d -m 0750 -o onderzeeer -g onderzeeer /workspace

COPY --from=go-build /out/onderzeeerd /out/onderzeeer /usr/local/bin/

ENV HOME=/home/onderzeeer \
    ONDERZEEER_SOCKET=/run/onderzeeer/onderzeeer.sock

WORKDIR /workspace
USER ${ONDERZEEER_UID}:${ONDERZEEER_GID}

# This probes the control API, not individual pipeline health.
# Override ONDERZEEER_SOCKET with an environment variable, not just a daemon flag,
# to keep the daemon and this client-based probe pointed at the same socket.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/onderzeeer", "ps"]

STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/onderzeeerd"]
CMD []
