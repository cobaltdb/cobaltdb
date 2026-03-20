# Multi-stage build for CobaltDB
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build server binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -X main.version=$(git describe --tags --always) -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o /build/cobaltdb-server \
    ./cmd/cobaltdb-server

# Build CLI binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o /build/cobaltdb-cli \
    ./cmd/cobaltdb-cli

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata netcat-openbsd

# Create non-root user
RUN addgroup -g 1000 -S cobaltdb && \
    adduser -u 1000 -S cobaltdb -G cobaltdb

# Create data and certs directories
RUN mkdir -p /data/cobaltdb /etc/cobaltdb/certs && \
    chown -R cobaltdb:cobaltdb /data/cobaltdb

# Copy binaries from builder
COPY --from=builder /build/cobaltdb-server /usr/local/bin/
COPY --from=builder /build/cobaltdb-cli /usr/local/bin/

# Copy default config
COPY --from=builder /build/config/cobaltdb.conf /etc/cobaltdb/

# Create entrypoint script to fix permissions
RUN printf '%s\n' \
    '#!/bin/sh' \
    '# Fix ownership of data directory (for volume mounts)' \
    'chown -R cobaltdb:cobaltdb /data/cobaltdb 2>/dev/null || true' \
    'chmod 755 /data/cobaltdb 2>/dev/null || true' \
    '' \
    '# Switch to cobaltdb user and run server' \
    'exec su-exec cobaltdb:cobaltdb cobaltdb-server "$@"' \
    > /entrypoint.sh \
    && chmod +x /entrypoint.sh

# Install su-exec for user switching
RUN apk add --no-cache su-exec

# Expose ports: 4200=wire protocol, 3307=MySQL protocol, 8420=health checks
EXPOSE 4200 3307 8420

# Volume for data persistence
VOLUME ["/data/cobaltdb"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD nc -z localhost 4200 || exit 1

# Default command
ENTRYPOINT ["/entrypoint.sh"]
CMD ["-addr", ":4200", "-mysql-addr", ":3307", "-data", "/data/cobaltdb", "-cache", "1024"]
