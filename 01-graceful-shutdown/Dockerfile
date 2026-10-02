# Build Stage
FROM rust:1.80-slim as builder
WORKDIR /app
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

# Production Minimal Stage
FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/graceful_shutdown /usr/local/bin/graceful_shutdown

# Expose port and configure Kubernetes health probes
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=2s --retries=3 CMD curl -f http://localhost:8080/healthz || exit 1

# Run as non-root user
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/graceful_shutdown"]
