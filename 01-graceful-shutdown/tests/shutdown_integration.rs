use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use graceful_shutdown::{run_server, OrderRequest, OrderResponse};

#[tokio::test]
async fn test_in_flight_request_completes_during_shutdown() {
    // 1. Bind to an ephemeral port
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral test socket");
    let server_addr = listener.local_addr().expect("Failed to get local addr");

    let cancellation_token = CancellationToken::new();
    let task_tracker = TaskTracker::new();
    let shutdown_timeout = Duration::from_secs(5);

    // 2. Spawn server in background task
    let server_token = cancellation_token.clone();
    let server_tracker = task_tracker.clone();
    let server_handle = tokio::spawn(async move {
        run_server(listener, server_token, server_tracker, shutdown_timeout)
            .await
            .expect("Server failed during execution");
    });

    let client = reqwest::Client::new();
    let base_url = format!("http://{}", server_addr);

    // 3. Verify server starts healthy and ready
    let health_res = client
        .get(format!("{}/healthz", base_url))
        .send()
        .await
        .expect("Healthz request failed");
    assert_eq!(health_res.status(), reqwest::StatusCode::OK);

    let ready_res = client
        .get(format!("{}/readyz", base_url))
        .send()
        .await
        .expect("Readyz request failed");
    assert_eq!(ready_res.status(), reqwest::StatusCode::OK);

    // 4. Fire an in-flight order transaction (takes 300ms)
    let in_flight_order_future = {
        let client = client.clone();
        let base_url = base_url.clone();
        tokio::spawn(async move {
            let res = client
                .post(format!("{}/api/v1/orders", base_url))
                .json(&OrderRequest {
                    order_id: "order-k8s-9941".to_string(),
                    customer_id: "cust-ent-77".to_string(),
                    amount_cents: 49900,
                })
                .send()
                .await
                .expect("Failed to send order request");

            assert_eq!(res.status(), reqwest::StatusCode::OK);
            let body: OrderResponse = res.json().await.expect("Failed to parse response");
            assert_eq!(body.status, "COMPLETED");
            assert_eq!(body.order_id, "order-k8s-9941");
        })
    };

    // 5. Wait 50ms so the request is actively inside the handler, then trigger SIGTERM / Cancellation
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancellation_token.cancel();

    // 6. Assert that in-flight request finishes successfully despite cancellation!
    in_flight_order_future
        .await
        .expect("In-flight order future panicked or failed");

    // 7. Verify that the server task terminates cleanly
    server_handle
        .await
        .expect("Server background task panicked");
}

#[tokio::test]
async fn test_readiness_probe_immediately_rejects_on_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral test socket");
    let server_addr = listener.local_addr().expect("Failed to get local addr");

    let cancellation_token = CancellationToken::new();
    let task_tracker = TaskTracker::new();

    let server_token = cancellation_token.clone();
    let server_tracker = task_tracker.clone();
    let _server_handle = tokio::spawn(async move {
        run_server(
            listener,
            server_token,
            server_tracker,
            Duration::from_secs(5),
        )
        .await
        .expect("Server failed");
    });

    let client = reqwest::Client::new();
    let base_url = format!("http://{}", server_addr);

    // Initial check: ready
    let res = client
        .get(format!("{}/readyz", base_url))
        .send()
        .await
        .expect("Initial readiness check failed");
    assert_eq!(res.status(), reqwest::StatusCode::OK);

    // Trigger shutdown
    cancellation_token.cancel();
    tokio::time::sleep(Duration::from_millis(30)).await;

    // After shutdown initiation, readiness MUST fail or connection closed
    let maybe_res = client.get(format!("{}/readyz", base_url)).send().await;
    if let Ok(res) = maybe_res {
        // If connection still accepted, status must be 503 SERVICE_UNAVAILABLE
        assert_eq!(res.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    }
}
