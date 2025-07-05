# Multi-stage build for KaflowSQL
# Support for multiple architectures: linux/amd64, linux/arm64, linux/arm/v7

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH

# Build stage - use Ubuntu for better ARM64 library support  
FROM golang:1.24.1 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    git \
    make \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the engine for the target platform
RUN echo "Building for $(uname -m) with CGO"; \
    CGO_ENABLED=1 go build -ldflags="-w -s" -o bin/engine cmd/engine/main.go

# Runtime stage - use Ubuntu for consistent ARM64 support
FROM ubuntu:24.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -g 1001 kaflowsql && \
    useradd -u 1001 -g kaflowsql -m -s /bin/bash kaflowsql

# Create necessary directories
RUN mkdir -p /data/rocksdb /app/pipelines /app/configs && \
    chown -R kaflowsql:kaflowsql /data /app

# Copy binary from builder
COPY --from=builder /app/bin/engine /usr/local/bin/

# Set working directory
WORKDIR /app

# Switch to non-root user
USER kaflowsql

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD pgrep engine || exit 1

# Default command
CMD ["engine"]