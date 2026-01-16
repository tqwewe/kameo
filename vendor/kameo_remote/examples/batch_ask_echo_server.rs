use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair, MessageType, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

/// Simple echo server that responds to ask messages with value + 1
async fn run_echo_server(bind_addr: SocketAddr) -> Result<SocketAddr> {
    let listener = TcpListener::bind(bind_addr).await?;
    let actual_addr = listener.local_addr()?;
    info!("Echo server listening on {}", actual_addr);

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("Echo server accepted connection from {}", peer_addr);
                    tokio::spawn(handle_echo_connection(stream, peer_addr));
                }
                Err(e) => error!("Accept error: {}", e),
            }
        }
    });

    Ok(actual_addr)
}

async fn handle_echo_connection(mut stream: TcpStream, peer_addr: SocketAddr) {
    info!("Handling echo connection from {}", peer_addr);

    loop {
        // Read length prefix
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                info!("Echo connection closed by peer {}", peer_addr);
                break;
            }
            Err(e) => {
                error!("Error reading length from {}: {}", peer_addr, e);
                break;
            }
        }

        let msg_len = u32::from_be_bytes(len_buf) as usize;
        debug!("Reading message of {} bytes", msg_len);

        // Read message
        let mut msg_buf = vec![0u8; msg_len];
        if let Err(e) = stream.read_exact(&mut msg_buf).await {
            error!("Error reading message: {}", e);
            break;
        }

        // Parse message header
        if msg_buf.len() < 8 {
            error!("Message too short");
            continue;
        }

        let msg_type = msg_buf[0];
        let correlation_id = u16::from_be_bytes([msg_buf[1], msg_buf[2]]);
        // Skip 5 reserved bytes
        let payload = &msg_buf[8..];

        if msg_type == MessageType::Ask as u8 {
            debug!(
                "Received ASK with correlation_id={}, payload_len={}",
                correlation_id,
                payload.len()
            );

            // Parse integer from payload
            if payload.len() >= 4 {
                let value = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let response_value = value + 1;
                debug!("Echo: {} -> {}", value, response_value);

                // Build response
                let response_payload = response_value.to_be_bytes();
                let response_size = 8 + response_payload.len();
                let mut response = Vec::with_capacity(4 + response_size);

                // Length prefix
                response.extend_from_slice(&(response_size as u32).to_be_bytes());

                // Header: [type:1][correlation_id:2][reserved:5]
                response.push(MessageType::Response as u8);
                response.extend_from_slice(&correlation_id.to_be_bytes());
                response.extend_from_slice(&[0u8; 5]);

                // Payload
                response.extend_from_slice(&response_payload);

                // Send response
                if let Err(e) = stream.write_all(&response).await {
                    error!("Error sending response: {}", e);
                    break;
                }

                if let Err(e) = stream.flush().await {
                    error!("Error flushing: {}", e);
                }
            }
        } else {
            debug!("Ignoring non-ASK message type: {}", msg_type);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug,batch_ask_echo_server=info")
        .init();

    // Start the echo server
    let echo_addr = run_echo_server("127.0.0.1:0".parse().unwrap()).await?;
    info!("Echo server started on {}", echo_addr);

    // Start a gossip registry (for the connection pool)
    let registry_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let key_pair = KeyPair::new_for_testing("batch_ask_echo_server");
    let registry = GossipRegistryHandle::new_with_keypair(
        registry_addr,
        key_pair,
        Some(GossipConfig::default()),
    )
    .await?;
    info!("Registry started on {}", registry.registry.bind_addr);

    // Get connection to echo server
    let connection = registry.get_connection(echo_addr).await?;
    info!("Connected to echo server");

    const NUM_REQUESTS: usize = 1000;
    const BATCH_SIZE: usize = 100;

    // Prepare all requests
    let all_requests: Vec<Vec<u8>> = (0..NUM_REQUESTS)
        .map(|i| (i as u32).to_be_bytes().to_vec())
        .collect();

    // Test 1: Individual asks (baseline)
    {
        info!("\n=== Test 1: Individual Asks ===");
        let start = Instant::now();
        let mut success_count = 0;

        for (i, request) in all_requests.iter().enumerate().take(NUM_REQUESTS.min(100)) {
            // Limit individual test to 100 for speed
            match connection.ask(request).await {
                Ok(response) => {
                    if response.len() >= 4 {
                        let value = u32::from_be_bytes([
                            response[0],
                            response[1],
                            response[2],
                            response[3],
                        ]);
                        if value == (i as u32 + 1) {
                            success_count += 1;
                        } else {
                            error!("Wrong response: expected {} got {}", i + 1, value);
                        }
                    }
                }
                Err(e) => error!("Ask {} failed: {}", i, e),
            }
        }

        let elapsed = start.elapsed();
        let count = NUM_REQUESTS.min(100);
        info!(
            "Individual asks: {}/{} successful in {:?} ({:.2} req/sec)",
            success_count,
            count,
            elapsed,
            count as f64 / elapsed.as_secs_f64()
        );
    }

    // Test 2: Batch asks
    {
        info!("\n=== Test 2: Batch Asks ===");
        let start = Instant::now();
        let mut success_count = 0;
        let mut total_responses = 0;

        for batch_idx in 0..(NUM_REQUESTS / BATCH_SIZE) {
            let batch_start = batch_idx * BATCH_SIZE;
            let batch_end = batch_start + BATCH_SIZE;

            let batch_requests: Vec<&[u8]> = all_requests[batch_start..batch_end]
                .iter()
                .map(|req| req.as_slice())
                .collect();

            match connection.ask_batch(&batch_requests).await {
                Ok(receivers) => {
                    for (i, receiver) in receivers.into_iter().enumerate() {
                        match receiver.await {
                            Ok(response) => {
                                total_responses += 1;
                                if response.len() >= 4 {
                                    let value = u32::from_be_bytes([
                                        response[0],
                                        response[1],
                                        response[2],
                                        response[3],
                                    ]);
                                    let expected = (batch_start + i + 1) as u32;
                                    if value == expected {
                                        success_count += 1;
                                    } else {
                                        error!(
                                            "Wrong response: expected {} got {}",
                                            expected, value
                                        );
                                    }
                                }
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
            "Batch asks: {}/{} successful in {:?} ({:.2} req/sec, batch_size={})",
            success_count,
            total_responses,
            elapsed,
            NUM_REQUESTS as f64 / elapsed.as_secs_f64(),
            BATCH_SIZE
        );
    }

    // Test 3: Concurrent batch asks
    {
        info!("\n=== Test 3: Concurrent Batch Asks ===");
        let start = Instant::now();
        let connection = Arc::new(connection);
        let num_concurrent = 10;
        let requests_per_task = NUM_REQUESTS / num_concurrent;

        let mut tasks = Vec::new();
        for task_idx in 0..num_concurrent {
            let task_start = task_idx * requests_per_task;
            let task_end = task_start + requests_per_task;
            let connection_clone = connection.clone();
            let task_requests = all_requests[task_start..task_end].to_vec();

            let task = tokio::spawn(async move {
                let mut success_count = 0;
                let mut total_count = 0;

                // Process in smaller batches
                for (chunk_idx, chunk) in task_requests.chunks(50).enumerate() {
                    let batch_requests: Vec<&[u8]> =
                        chunk.iter().map(|req| req.as_slice()).collect();

                    match connection_clone.ask_batch(&batch_requests).await {
                        Ok(receivers) => {
                            for (i, receiver) in receivers.into_iter().enumerate() {
                                if let Ok(response) = receiver.await {
                                    total_count += 1;
                                    if response.len() >= 4 {
                                        let value = u32::from_be_bytes([
                                            response[0],
                                            response[1],
                                            response[2],
                                            response[3],
                                        ]);
                                        let expected = (task_start + chunk_idx * 50 + i + 1) as u32;
                                        if value == expected {
                                            success_count += 1;
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => error!("Concurrent batch failed: {}", e),
                    }
                }

                (success_count, total_count)
            });

            tasks.push(task);
        }

        let mut total_success = 0;
        let mut total_responses = 0;
        for task in tasks {
            match task.await {
                Ok((success, total)) => {
                    total_success += success;
                    total_responses += total;
                }
                Err(e) => error!("Task failed: {}", e),
            }
        }

        let elapsed = start.elapsed();
        info!(
            "Concurrent batch asks: {}/{} successful in {:?} ({:.2} req/sec, {} tasks)",
            total_success,
            total_responses,
            elapsed,
            total_responses as f64 / elapsed.as_secs_f64(),
            num_concurrent
        );
    }

    info!("\n=== Performance Test Complete ===");

    // Keep running for manual testing
    info!("Server continues running. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c().await?;

    registry.shutdown().await;
    Ok(())
}
