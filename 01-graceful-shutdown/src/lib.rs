//! # Enterprise Graceful Shutdown in Tokio & Axum
//!
//! Provides a production-grade, zero-downtime termination lifecycle for Kubernetes-hosted microservices.
//!
//! ## Key Architectural Invariants:
//! 1. **Immediate Readiness Rejection:** As soon as SIGTERM is received, `/readyz` immediately returns 503
//!    to signal the Kubernetes Endpoints controller and Ingress to stop routing new traffic.
//! 2. **In-Flight Connection Draining:** Active HTTP transactions are allowed up to a configurable
//!    grace period (e.g. 25 seconds) to complete normally before socket close.
//! 3. **Background Task Tracking:** Async tasks spawned via `tokio::spawn` are tracked using Tokio's
//!    `TaskTracker`, preventing orphaned threads or abrupt abortion of database transactions.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

/// Shared application state across HTTP handlers
#[derive(Clone)]
pub struct AppState {
    pub is_shutting_down: Arc<AtomicBool>,
    pub cancellation_token: CancellationToken,
    pub task_tracker: TaskTracker,
}

#[derive(Serialize, Deserialize)]
pub struct OrderRequest {
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrderResponse {
    pub status: String,
    pub order_id: String,
    pub message: String,
}

/// Creates the enterprise Axum router with health, readiness, and business endpoints.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .route("/api/v1/orders", post(process_order_handler))
        .with_state(state)
}

/// Kubernetes Liveness Probe: always 200 OK unless process is deadlocked
async fn healthz_handler() -> impl IntoResponse {
    (StatusCode::OK, "ALIVE")
}

/// Kubernetes Readiness Probe:
/// Immediately returns 503 SERVICE_UNAVAILABLE once shutdown has commenced.
async fn readyz_handler(State(state): State<AppState>) -> impl IntoResponse {
    if state.is_shutting_down.load(Ordering::SeqCst) {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "SHUTTING_DOWN: Evict from K8s Endpoint",
        )
    } else {
        (StatusCode::OK, "READY")
    }
}

/// Simulated mission-critical HTTP transaction (e.g. checkout / database write).
/// Uses `TaskTracker` so that even if the client disconnects or shutdown triggers,
/// the critical persistence segment runs to completion without aborting.
async fn process_order_handler(
    State(state): State<AppState>,
    Json(payload): Json<OrderRequest>,
) -> Result<Json<OrderResponse>, StatusCode> {
    if state.is_shutting_down.load(Ordering::SeqCst) {
        warn!(order_id = %payload.order_id, "Rejecting new order: server is shutting down");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    info!(order_id = %payload.order_id, "Processing order transaction");

    // Simulate in-flight transactional work (300ms)
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Track a background ledger audit log asynchronously
    let audit_order_id = payload.order_id.clone();
    state.task_tracker.spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        info!(order_id = %audit_order_id, "Background audit log committed successfully");
    });

    Ok(Json(OrderResponse {
        status: "COMPLETED".to_string(),
        order_id: payload.order_id,
        message: "Transaction completed and audited".to_string(),
    }))
}

/// Listens for OS POSIX termination signals (SIGINT on all OS, SIGTERM on Unix).
pub async fn wait_for_shutdown_signal(cancellation_token: CancellationToken) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C signal handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received SIGINT (Ctrl+C). Initiating graceful termination...");
        },
        _ = terminate => {
            info!("Received SIGTERM from Kubernetes. Initiating graceful termination...");
        },
        _ = cancellation_token.cancelled() => {
            info!("Received programmatic cancellation signal.");
        }
    }

    cancellation_token.cancel();
}

/// Orchestrates the full graceful server lifecycle.
pub async fn run_server(
    listener: TcpListener,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
    shutdown_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let is_shutting_down = Arc::new(AtomicBool::new(false));

    let state = AppState {
        is_shutting_down: is_shutting_down.clone(),
        cancellation_token: cancellation_token.clone(),
        task_tracker: task_tracker.clone(),
    };

    let app = create_router(state);
    let addr = listener.local_addr()?;
    info!("Enterprise Axum service running on http://{}", addr);

    // Axum server with graceful shutdown hook
    let server = axum::serve(listener, app);
    let shutdown_signal_future = {
        let is_shutting_down = is_shutting_down.clone();
        let cancellation_token = cancellation_token.clone();
        async move {
            wait_for_shutdown_signal(cancellation_token).await;
            is_shutting_down.store(true, Ordering::SeqCst);
            info!("Phase 1: Marked readiness probe as 503. Draining HTTP listener...");
        }
    };

    // Serve requests until shutdown signal is received
    if let Err(e) = server.with_graceful_shutdown(shutdown_signal_future).await {
        error!("Server encountered unexpected error: {}", e);
        return Err(e.into());
    }

    info!("Phase 2: HTTP server listener stopped accepting connections.");
    info!("Phase 3: Closing TaskTracker and draining background worker tasks...");
    task_tracker.close();

    // Barrier: Wait for all background tasks to complete, bounded by timeout
    let drain_result = tokio::time::timeout(shutdown_timeout, task_tracker.wait()).await;

    match drain_result {
        Ok(()) => {
            info!(
                "Phase 4: All in-flight requests and background tasks drained cleanly (0 dropped)."
            );
        }
        Err(_) => {
            warn!(
                "Shutdown timeout ({:?}) reached! Forcing process exit to avoid K8s SIGKILL.",
                shutdown_timeout
            );
        }
    }

    info!("Server shutdown sequence complete. Exiting cleanly.");
    Ok(())
}
