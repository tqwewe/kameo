#![cfg(all(feature = "remote", feature = "remote_integration_tests"))]
//! Regression test for socket outages: an in-flight ask must not hang forever if the
//! connection is cancelled (IO task aborted).
//!
//! This is a transport-layer precondition for application-level exactly-once retries:
//! the client must reliably observe a failure (timeout/closed) so it can retry with
//! the same RequestId.

use bytes::Bytes;
use kameo::remote::transport::RemoteTransport;
use tokio::time::{Duration, timeout};

#[tokio::test]
#[serial_test::serial]
async fn test_inflight_ask_returns_promptly_when_connection_is_aborted() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn")
        .try_init();

    let server_addr: std::net::SocketAddr = "127.0.0.1:9470".parse().unwrap();
    let client_addr: std::net::SocketAddr = "127.0.0.1:9471".parse().unwrap();

    let server_keypair = kameo::remote::v2_bootstrap::test_keypair(9470);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = kameo::remote::v2_bootstrap::test_keypair(9471);

    let mut server_transport = kameo::remote::v2_bootstrap::bootstrap_on(server_addr, server_keypair)
        .await
        .expect("server bootstrap failed");
    let mut client_transport = kameo::remote::v2_bootstrap::bootstrap_on(client_addr, client_keypair)
        .await
        .expect("client bootstrap failed");

    // Connect client to server and obtain a direct connection handle.
    let client_handle = client_transport.handle().expect("client missing handle");
    let peer = client_handle.add_peer(&server_peer_id).await;
    peer.connect(&server_addr).await.expect("connect failed");

    let peer_ref = client_handle.lookup_peer(&server_peer_id).await.expect("lookup_peer failed");
    let conn = std::sync::Arc::as_ref(peer_ref.connection_ref().expect("no connection")).clone();

    // Start an ask that will never receive a response (unknown actor_id), then abort the connection.
    let ask_task = tokio::spawn(async move {
        conn.ask_actor_frame(
            0x0123_4567_89ab_cdef,
            0xdead_beef,
            Bytes::from_static(b"ping"),
            Duration::from_secs(5),
        )
        .await
    });

    // Give the ask a moment to allocate its correlation slot and start waiting.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Abort the IO task via pool disconnect; this triggers correlation cancellation.
    client_handle
        .registry
        .connection_pool
        .disconnect_connection_by_peer_id(&server_peer_id);

    let result = timeout(Duration::from_secs(2), ask_task)
        .await
        .expect("ask hung after connection abort")
        .expect("ask task panicked");

    assert!(
        result.is_err(),
        "expected ask to fail after connection abort"
    );

    server_transport.shutdown().await.ok();
    client_transport.shutdown().await.ok();
}
