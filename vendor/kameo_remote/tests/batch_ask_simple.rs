use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_batch_ask_simple() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,batch_ask_simple=debug")
        .try_init()
        .ok();

    // Start gossip registry client
    let key_pair = KeyPair::new_for_testing("batch_ask_simple_client");
    let registry = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        key_pair,
        Some(GossipConfig::default()),
    )
    .await?;

    // Start gossip registry server
    let server_key_pair = KeyPair::new_for_testing("batch_ask_simple_server");
    let server = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        server_key_pair,
        Some(GossipConfig::default()),
    )
    .await?;

    // Give servers time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get connection
    let server_peer_id = server.registry.peer_id.clone();
    let server_addr = server.registry.bind_addr;
    let peer = registry.add_peer(&server_peer_id).await;
    peer.connect(&server_addr).await?;

    let conn = registry.get_connection_to_peer(&server_peer_id).await?;
    info!("Connected to server via TLS");

    // Test 1: Single ask
    {
        let request = 42u32.to_be_bytes();
        let start = Instant::now();
        let response = conn.ask(&request).await?;
        let elapsed = start.elapsed();

        assert_eq!(response.len(), 4);
        let value = u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
        assert_eq!(value, 43);
        info!("Single ask: {} -> {} in {:?}", 42, value, elapsed);
    }

    // Test 2: Small batch
    {
        let requests: Vec<Vec<u8>> = (0..10u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let request_refs: Vec<&[u8]> = requests.iter().map(|r| r.as_slice()).collect();

        let start = Instant::now();
        let receivers = conn.ask_batch(&request_refs).await?;

        let mut responses = Vec::new();
        for receiver in receivers {
            match receiver.await {
                Ok(response) => responses.push(response),
                Err(_) => panic!("Failed to receive response"),
            }
        }
        let elapsed = start.elapsed();

        assert_eq!(responses.len(), 10);
        for (i, response) in responses.iter().enumerate() {
            assert_eq!(response.len(), 4);
            let value = u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
            assert_eq!(value, i as u32 + 1);
        }
        info!("Batch ask (10): completed in {:?}", elapsed);
    }

    // Test 3: Large batch
    {
        const BATCH_SIZE: usize = 100;
        let requests: Vec<Vec<u8>> = (0..BATCH_SIZE as u32)
            .map(|i| i.to_be_bytes().to_vec())
            .collect();
        let request_refs: Vec<&[u8]> = requests.iter().map(|r| r.as_slice()).collect();

        let start = Instant::now();
        let receivers = conn.ask_batch(&request_refs).await?;

        let mut responses = Vec::new();
        for receiver in receivers {
            match receiver.await {
                Ok(response) => responses.push(response),
                Err(_) => panic!("Failed to receive response"),
            }
        }
        let elapsed = start.elapsed();

        assert_eq!(responses.len(), BATCH_SIZE);
        let mut success_count = 0;
        for (i, response) in responses.iter().enumerate() {
            if response.len() == 4 {
                let value =
                    u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
                if value == i as u32 + 1 {
                    success_count += 1;
                }
            }
        }
        assert_eq!(success_count, BATCH_SIZE);
        info!(
            "Batch ask ({}): completed in {:?} ({:.2} req/sec)",
            BATCH_SIZE,
            elapsed,
            BATCH_SIZE as f64 / elapsed.as_secs_f64()
        );
    }

    // Test 4: Concurrent batches
    {
        const NUM_CONCURRENT: usize = 5;
        const BATCH_SIZE: usize = 20;

        let conn = Arc::new(conn);
        let mut tasks = Vec::new();

        let start = Instant::now();

        for task_id in 0..NUM_CONCURRENT {
            let conn_clone = conn.clone();
            let base = (task_id * BATCH_SIZE) as u32;

            let task = tokio::spawn(async move {
                let requests: Vec<Vec<u8>> = (base..base + BATCH_SIZE as u32)
                    .map(|i| i.to_be_bytes().to_vec())
                    .collect();
                let request_refs: Vec<&[u8]> = requests.iter().map(|r| r.as_slice()).collect();

                let receivers = conn_clone.ask_batch(&request_refs).await?;

                let mut responses = Vec::new();
                for receiver in receivers {
                    match receiver.await {
                        Ok(response) => responses.push(response),
                        Err(_) => {
                            return Err(kameo_remote::GossipError::Network(std::io::Error::other(
                                "Failed to receive response",
                            )))
                        }
                    }
                }

                // Verify responses
                let mut success = 0;
                for (i, response) in responses.iter().enumerate() {
                    if response.len() == 4 {
                        let value = u32::from_be_bytes([
                            response[0],
                            response[1],
                            response[2],
                            response[3],
                        ]);
                        if value == base + i as u32 + 1 {
                            success += 1;
                        }
                    }
                }

                Ok::<usize, kameo_remote::GossipError>(success)
            });

            tasks.push(task);
        }

        let mut total_success = 0;
        for task in tasks {
            total_success += task.await.unwrap()?;
        }

        let elapsed = start.elapsed();
        let total_requests = NUM_CONCURRENT * BATCH_SIZE;

        assert_eq!(total_success, total_requests);
        info!(
            "Concurrent batch asks ({} tasks x {} requests): completed in {:?} ({:.2} req/sec)",
            NUM_CONCURRENT,
            BATCH_SIZE,
            elapsed,
            total_requests as f64 / elapsed.as_secs_f64()
        );
    }

    // Cleanup
    registry.shutdown().await;
    server.shutdown().await;

    Ok(())
}

#[tokio::test]
async fn test_batch_ask_with_timeout() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init()
        .ok();

    // Start registry client
    let key_pair = KeyPair::new_for_testing("batch_ask_simple_alt_client");
    let registry = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        key_pair,
        Some(GossipConfig::default()),
    )
    .await?;

    // Start registry server
    let server_key_pair = KeyPair::new_for_testing("batch_ask_simple_alt_server");
    let server = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        server_key_pair,
        Some(GossipConfig::default()),
    )
    .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get connection
    let server_peer_id = server.registry.peer_id.clone();
    let server_addr = server.registry.bind_addr;
    let peer = registry.add_peer(&server_peer_id).await;
    peer.connect(&server_addr).await?;
    let conn = registry.get_connection_to_peer(&server_peer_id).await?;

    // Test batch with timeout
    let requests: Vec<Vec<u8>> = (0..5u32).map(|i| i.to_be_bytes().to_vec()).collect();
    let request_refs: Vec<&[u8]> = requests.iter().map(|r| r.as_slice()).collect();

    let results = conn
        .ask_batch_with_timeout(&request_refs, Duration::from_secs(1))
        .await?;

    assert_eq!(results.len(), 5);
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(response) => {
                assert_eq!(response.len(), 4);
                let value =
                    u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
                assert_eq!(value, i as u32 + 1);
            }
            Err(e) => panic!("Request {} failed: {}", i, e),
        }
    }

    info!("Batch ask with timeout: all requests succeeded");

    // Cleanup
    registry.shutdown().await;
    server.shutdown().await;

    Ok(())
}
