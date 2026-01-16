use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info};

const NUM_REQUESTS: usize = 1000;
const BATCH_SIZE: usize = 100; // Send requests in batches of 100

#[tokio::test]
async fn test_batch_ask_performance() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug,test=debug")
        .try_init()
        .ok();

    // Start server node that will handle ask requests
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server_keypair = KeyPair::new_for_testing("batch_perf_server");
    let server_registry = GossipRegistryHandle::new_with_keypair(
        server_addr,
        server_keypair,
        Some(GossipConfig::default()),
    )
    .await?;
    let actual_server_addr = server_registry.registry.bind_addr;
    info!("Server started on {}", actual_server_addr);

    // Start client node
    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client_keypair = KeyPair::new_for_testing("batch_perf_client");
    let client_registry = GossipRegistryHandle::new_with_keypair(
        client_addr,
        client_keypair,
        Some(GossipConfig::default()),
    )
    .await?;
    let actual_client_addr = client_registry.registry.bind_addr;
    info!("Client started on {}", actual_client_addr);

    // Set up echo server that responds with i+1
    let echo_server_handle = tokio::spawn(async move {
        // Accept connections and handle ask requests
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;

            // In a real implementation, we'd need to handle incoming ask messages
            // For now, this is a placeholder
            // The actual handling would be in the connection pool's message handler
        }
    });

    // Give servers time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get connection from client to server
    let connection = client_registry.get_connection(actual_server_addr).await?;

    // Prepare requests - each request is just the integer as bytes
    let mut all_requests = Vec::with_capacity(NUM_REQUESTS);
    for i in 0..NUM_REQUESTS {
        let request = (i as u32).to_be_bytes().to_vec();
        all_requests.push(request);
    }

    info!(
        "Starting batch ask performance test with {} requests",
        NUM_REQUESTS
    );

    // Test 1: Single asks (baseline)
    {
        info!("Test 1: Individual asks (baseline)");
        let start = Instant::now();
        let mut responses = Vec::new();

        for (i, request) in all_requests.iter().enumerate() {
            match connection.ask(request).await {
                Ok(response) => responses.push(response),
                Err(e) => error!("Ask {} failed: {}", i, e),
            }
        }

        let elapsed = start.elapsed();
        info!(
            "Individual asks: {} requests in {:?} ({:.2} req/sec)",
            NUM_REQUESTS,
            elapsed,
            NUM_REQUESTS as f64 / elapsed.as_secs_f64()
        );
    }

    // Test 2: Batch asks
    {
        info!("Test 2: Batch asks");
        let start = Instant::now();
        let mut all_responses = Vec::new();

        // Process in batches
        for batch_idx in 0..(NUM_REQUESTS / BATCH_SIZE) {
            let batch_start = batch_idx * BATCH_SIZE;
            let batch_end = batch_start + BATCH_SIZE;

            // Prepare batch request slice
            let batch_requests: Vec<&[u8]> = all_requests[batch_start..batch_end]
                .iter()
                .map(|req| req.as_slice())
                .collect();

            // Send batch
            match connection.ask_batch(&batch_requests).await {
                Ok(receivers) => {
                    // Collect all responses from this batch
                    for (i, receiver) in receivers.into_iter().enumerate() {
                        match receiver.await {
                            Ok(response) => {
                                // Verify response is i+1
                                if response.len() >= 4 {
                                    let value = u32::from_be_bytes([
                                        response[0],
                                        response[1],
                                        response[2],
                                        response[3],
                                    ]);
                                    let expected = (batch_start + i + 1) as u32;
                                    if value != expected {
                                        error!(
                                            "Response mismatch: got {} expected {}",
                                            value, expected
                                        );
                                    }
                                }
                                all_responses.push(response);
                            }
                            Err(e) => error!("Response {} failed: {:?}", batch_start + i, e),
                        }
                    }
                }
                Err(e) => error!("Batch {} failed: {}", batch_idx, e),
            }
        }

        let elapsed = start.elapsed();
        info!(
            "Batch asks: {} requests in {:?} ({:.2} req/sec, batch size: {})",
            NUM_REQUESTS,
            elapsed,
            NUM_REQUESTS as f64 / elapsed.as_secs_f64(),
            BATCH_SIZE
        );
    }

    // Test 3: Optimized batch asks with pre-allocated buffers
    {
        info!("Test 3: Optimized batch asks");
        let start = Instant::now();
        let mut response_buffer = Vec::with_capacity(BATCH_SIZE);
        let mut all_responses = Vec::new();

        for batch_idx in 0..(NUM_REQUESTS / BATCH_SIZE) {
            let batch_start = batch_idx * BATCH_SIZE;
            let batch_end = batch_start + BATCH_SIZE;

            // Prepare batch request slice
            let batch_requests: Vec<&[u8]> = all_requests[batch_start..batch_end]
                .iter()
                .map(|req| req.as_slice())
                .collect();

            // Send batch with optimized API
            match connection
                .ask_batch_optimized(&batch_requests, &mut response_buffer)
                .await
            {
                Ok(()) => {
                    // Collect responses
                    for receiver in response_buffer.drain(..) {
                        match receiver.await {
                            Ok(response) => all_responses.push(response),
                            Err(e) => error!("Response failed: {:?}", e),
                        }
                    }
                }
                Err(e) => error!("Optimized batch {} failed: {}", batch_idx, e),
            }
        }

        let elapsed = start.elapsed();
        info!(
            "Optimized batch asks: {} requests in {:?} ({:.2} req/sec)",
            NUM_REQUESTS,
            elapsed,
            NUM_REQUESTS as f64 / elapsed.as_secs_f64()
        );
    }

    // Test 4: Concurrent batch asks
    {
        info!("Test 4: Concurrent batch asks");
        let start = Instant::now();
        let connection = Arc::new(connection);
        let mut tasks = Vec::new();

        // Launch multiple concurrent batch operations
        let num_concurrent = 10;
        let batch_per_task = NUM_REQUESTS / num_concurrent;

        for task_idx in 0..num_concurrent {
            let task_start = task_idx * batch_per_task;
            let task_end = task_start + batch_per_task;
            let connection_clone = connection.clone();
            let requests = all_requests[task_start..task_end].to_vec();

            let task = tokio::spawn(async move {
                let mut responses = Vec::new();

                // Process in smaller batches within each task
                for chunk in requests.chunks(BATCH_SIZE / 10) {
                    let batch_requests: Vec<&[u8]> =
                        chunk.iter().map(|req| req.as_slice()).collect();

                    match connection_clone.ask_batch(&batch_requests).await {
                        Ok(receivers) => {
                            for receiver in receivers {
                                if let Ok(response) = receiver.await {
                                    responses.push(response);
                                }
                            }
                        }
                        Err(e) => error!("Concurrent batch failed: {}", e),
                    }
                }

                responses
            });

            tasks.push(task);
        }

        // Wait for all tasks
        let mut total_responses = 0;
        for task in tasks {
            match task.await {
                Ok(responses) => total_responses += responses.len(),
                Err(e) => error!("Task failed: {}", e),
            }
        }

        let elapsed = start.elapsed();
        info!(
            "Concurrent batch asks: {} requests in {:?} ({:.2} req/sec, {} concurrent tasks)",
            total_responses,
            elapsed,
            total_responses as f64 / elapsed.as_secs_f64(),
            num_concurrent
        );
    }

    // Cleanup
    echo_server_handle.abort();
    server_registry.shutdown().await;
    client_registry.shutdown().await;

    Ok(())
}

#[tokio::test]
async fn test_batch_ask_with_timeout() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init()
        .ok();

    // Start nodes
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server_keypair = KeyPair::new_for_testing("batch_perf_server_alt");
    let server = GossipRegistryHandle::new_with_keypair(
        server_addr,
        server_keypair,
        Some(GossipConfig::default()),
    )
    .await?;
    let actual_server_addr = server.registry.bind_addr;

    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client_keypair = KeyPair::new_for_testing("batch_perf_client_alt");
    let client = GossipRegistryHandle::new_with_keypair(
        client_addr,
        client_keypair,
        Some(GossipConfig::default()),
    )
    .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get connection
    let connection = client.get_connection(actual_server_addr).await?;

    // Test batch ask with timeout
    let requests: Vec<Vec<u8>> = (0..10).map(|i| (i as u32).to_be_bytes().to_vec()).collect();

    let request_refs: Vec<&[u8]> = requests.iter().map(|r| r.as_slice()).collect();

    let results = connection
        .ask_batch_with_timeout(&request_refs, Duration::from_secs(1))
        .await?;

    info!(
        "Batch ask with timeout completed: {} results",
        results.len()
    );

    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(response) => info!("Request {} got response of {} bytes", i, response.len()),
            Err(e) => error!("Request {} failed: {}", i, e),
        }
    }

    // Cleanup
    server.shutdown().await;
    client.shutdown().await;

    Ok(())
}
