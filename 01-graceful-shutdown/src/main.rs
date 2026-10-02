use std::{env, time::Duration};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Initialize structured JSON telemetry / log subscriber
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "graceful_shutdown=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Initializing Oxidized Cloud Enterprise Graceful Shutdown Service...");

    // 2. Read configuration
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);
    let shutdown_timeout = Duration::from_secs(25); // Safe inside K8s default 30s grace window

    // 3. Bind TCP socket
    let listener = TcpListener::bind(&bind_addr).await?;

    // 4. Construct synchronization primitives
    let cancellation_token = CancellationToken::new();
    let task_tracker = TaskTracker::new();

    // 5. Execute server lifecycle
    graceful_shutdown::run_server(listener, cancellation_token, task_tracker, shutdown_timeout)
        .await?;

    Ok(())
}
