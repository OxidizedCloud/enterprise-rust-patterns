# Enterprise Rust Patterns: 01. Graceful Shutdown in Async Tokio

[![YouTube Deep Dive](https://img.shields.io/badge/YouTube-Video%20Breakdown-FF0000?logo=youtube)](https://youtube.com/@OxidizedCloud)
[![Substack ADR](https://img.shields.io/badge/Substack-Architectural%20Decision%20Record-FF6719?logo=substack)](https://oxidizedcloud.substack.com)
[![GitHub Monorepo](https://img.shields.io/badge/GitHub-Enterprise%20Rust%20Patterns-181717?logo=github)](https://github.com/OxidizedCloud/enterprise-rust-patterns)

> **Episode 01 of the Enterprise Rust Series by [Oxidized Cloud](https://youtube.com/@OxidizedCloud)**  
> *Production-Grade Cloud Architecture & Distributed Systems in Rust*

---

## The Distributed Systems Problem

During Kubernetes rolling deployments or autoscaling events, a pod receives a `SIGTERM` signal from the kubelet. By default:
1. In Java/Spring Boot systems, developers rely on container managed thread pools (`Tomcat` or `Jetty`) and `@PreDestroy` hooks that drain active servlets.
2. In **Tokio and async Rust**, tasks are cooperatively scheduled. When the root `main()` function exits, **all spawned async tasks are aborted instantly**, terminating in-flight database transactions, dropping active HTTP connections, and generating `502 Bad Gateway` errors at your ingress controller.

---

## The Solution: Two-Phase Drain Barrier

This crate demonstrates the idiomatic, battle-tested pattern combining:
1. **Immediate `/readyz` 503 Rejection:** Signals Kubernetes Ingress and kube-proxy to evict this pod from the backend pool immediately.
2. **`tokio_util::sync::CancellationToken`:** Propagates cancellation down to long-running background tasks.
3. **`tokio_util::task::TaskTracker`:** Acts as a synchronization barrier, awaiting in-flight transactional tasks up to a safe timeout (e.g. 25 seconds).

```
[K8s SIGTERM] ──► wait_for_shutdown_signal()
                         │
                         ├──► is_shutting_down = true  (readiness probe /readyz returns 503)
                         ├──► cancellation_token.cancel()
                         │
                         ▼
             axum::serve.with_graceful_shutdown()
                         │ (HTTP listener stops accepting new connections)
                         ▼
             task_tracker.close()
                         │
                         ▼
             task_tracker.wait().await  (Drains active background jobs)
                         │
                         ▼
             Process exits cleanly with code 0
```

---

## Running and Verifying

### Run the Service Locally
```bash
cargo run
```

### Run the Integration Tests
```bash
# Tests that in-flight requests complete cleanly while shutdown occurs concurrently
cargo test
```

### Run Benchmarks / Load Test
```bash
# Simulates traffic during a rolling restart
k6 run benchmarks/load_test.js
```
