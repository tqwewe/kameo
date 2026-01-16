use kameo_remote::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Comprehensive TellMessage API demonstration with lookup() by actor name
/// Tests individual, sequential, and batch message sending with performance comparisons
#[tokio::test]
async fn test_tell_with_lookup_and_performance_comparison() {
    println!("🚀 TellMessage with Lookup() Performance Test");
    println!("=============================================");

    // Setup three nodes with gossip
    let config = GossipConfig::default();

    println!("📡 Setting up 3-node cluster...");
    let node1_keypair = KeyPair::new_for_testing("node1");
    let node2_keypair = KeyPair::new_for_testing("node2");
    let node3_keypair = KeyPair::new_for_testing("node3");
    let node1_id = node1_keypair.peer_id();
    let node2_id = node2_keypair.peer_id();
    let node3_id = node3_keypair.peer_id();

    let node1 = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:29001".parse().unwrap(),
        node1_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();

    let node2 = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:29002".parse().unwrap(),
        node2_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();

    let node3 = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:29003".parse().unwrap(),
        node3_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();

    // Connect nodes in a mesh topology
    println!("\n🔗 Establishing peer connections...");

    // Node1 connects to Node2 and Node3
    let peer2 = node1.add_peer(&node2_id).await;
    let peer3 = node1.add_peer(&node3_id).await;
    peer2
        .connect(&"127.0.0.1:29002".parse().unwrap())
        .await
        .unwrap();
    peer3
        .connect(&"127.0.0.1:29003".parse().unwrap())
        .await
        .unwrap();

    // Node2 connects to Node1 and Node3
    let peer1_from_2 = node2.add_peer(&node1_id).await;
    let peer3_from_2 = node2.add_peer(&node3_id).await;
    peer1_from_2
        .connect(&"127.0.0.1:29001".parse().unwrap())
        .await
        .unwrap();
    peer3_from_2
        .connect(&"127.0.0.1:29003".parse().unwrap())
        .await
        .unwrap();

    // Node3 connects to Node1 and Node2
    let peer1_from_3 = node3.add_peer(&node1_id).await;
    let peer2_from_3 = node3.add_peer(&node2_id).await;
    peer1_from_3
        .connect(&"127.0.0.1:29001".parse().unwrap())
        .await
        .unwrap();
    peer2_from_3
        .connect(&"127.0.0.1:29002".parse().unwrap())
        .await
        .unwrap();

    // Wait for connection establishment
    sleep(Duration::from_millis(200)).await;
    println!("✅ All nodes connected in mesh topology");

    // Register actors on each node
    println!("\n📋 Registering actors on each node...");

    // Node1 registers chat_service
    node1
        .register_urgent(
            "chat_service".to_string(),
            "127.0.0.1:39001".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    println!("   ✅ Node1: registered 'chat_service' at 127.0.0.1:39001");

    // Node2 registers auth_service
    node2
        .register_urgent(
            "auth_service".to_string(),
            "127.0.0.1:39002".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    println!("   ✅ Node2: registered 'auth_service' at 127.0.0.1:39002");

    // Node3 registers storage_service
    node3
        .register_urgent(
            "storage_service".to_string(),
            "127.0.0.1:39003".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    println!("   ✅ Node3: registered 'storage_service' at 127.0.0.1:39003");

    // Wait for gossip propagation
    println!("\n⏳ Waiting for gossip propagation...");
    sleep(Duration::from_millis(100)).await;

    // Verify all actors are visible from all nodes
    let mut all_propagated = false;
    let propagation_start = Instant::now();
    while !all_propagated && propagation_start.elapsed() < Duration::from_secs(2) {
        let chat_on_2 = node2.lookup("chat_service").await.is_some();
        let auth_on_1 = node1.lookup("auth_service").await.is_some();
        let storage_on_1 = node1.lookup("storage_service").await.is_some();

        all_propagated = chat_on_2 && auth_on_1 && storage_on_1;
        if !all_propagated {
            sleep(Duration::from_millis(10)).await;
        }
    }

    let propagation_time = propagation_start.elapsed();
    println!(
        "✅ All actors propagated in: {:?} ({:.3} ms)",
        propagation_time,
        propagation_time.as_millis() as f64
    );

    // ===========================================
    // PART 1: LOOKUP AND CREATE ACTOR REFS
    // ===========================================
    println!("\n📊 PART 1: ACTOR LOOKUP AND REFERENCE CREATION");
    println!("==============================================");

    // Create actor references by looking up once
    println!("\n🔸 Looking up actors and creating references...");

    let lookup_start = Instant::now();

    // Lookup each actor and create a reference (location + connection)
    let chat_location = node1
        .lookup("chat_service")
        .await
        .expect("chat_service not found");
    let chat_conn = node1
        .get_connection(if chat_location.peer_id == node1_id {
            "127.0.0.1:29001".parse().unwrap()
        } else if chat_location.peer_id == node2_id {
            "127.0.0.1:29002".parse().unwrap()
        } else if chat_location.peer_id == node3_id {
            "127.0.0.1:29003".parse().unwrap()
        } else {
            panic!("Unknown peer")
        })
        .await
        .unwrap();

    let auth_location = node1
        .lookup("auth_service")
        .await
        .expect("auth_service not found");
    let auth_conn = node1
        .get_connection(if auth_location.peer_id == node1_id {
            "127.0.0.1:29001".parse().unwrap()
        } else if auth_location.peer_id == node2_id {
            "127.0.0.1:29002".parse().unwrap()
        } else if auth_location.peer_id == node3_id {
            "127.0.0.1:29003".parse().unwrap()
        } else {
            panic!("Unknown peer")
        })
        .await
        .unwrap();

    let storage_location = node1
        .lookup("storage_service")
        .await
        .expect("storage_service not found");
    let storage_conn = node1
        .get_connection(if storage_location.peer_id == node1_id {
            "127.0.0.1:29001".parse().unwrap()
        } else if storage_location.peer_id == node2_id {
            "127.0.0.1:29002".parse().unwrap()
        } else if storage_location.peer_id == node3_id {
            "127.0.0.1:29003".parse().unwrap()
        } else {
            panic!("Unknown peer")
        })
        .await
        .unwrap();

    let total_lookup_time = lookup_start.elapsed();

    println!("   ✅ Created actor references:");
    println!(
        "     - chat_service at {} (peer: {})",
        chat_location.address, chat_location.peer_id
    );
    println!(
        "     - auth_service at {} (peer: {})",
        auth_location.address, auth_location.peer_id
    );
    println!(
        "     - storage_service at {} (peer: {})",
        storage_location.address, storage_location.peer_id
    );
    println!(
        "   📈 Total lookup and connection time: {:?} ({:.3} μs)",
        total_lookup_time,
        total_lookup_time.as_nanos() as f64 / 1000.0
    );

    // Define a simple ActorRef struct for convenience
    struct ActorRef {
        name: &'static str,
        #[allow(dead_code)]
        location: RemoteActorLocation,
        conn: kameo_remote::connection_pool::ConnectionHandle,
    }

    let chat_ref = ActorRef {
        name: "chat_service",
        location: chat_location,
        conn: chat_conn,
    };
    let auth_ref = ActorRef {
        name: "auth_service",
        location: auth_location,
        conn: auth_conn,
    };
    let storage_ref = ActorRef {
        name: "storage_service",
        location: storage_location,
        conn: storage_conn,
    };

    // ===========================================
    // PART 2: MESSAGE SENDING PERFORMANCE
    // ===========================================
    println!("\n📊 PART 2: MESSAGE SENDING PERFORMANCE");
    println!("=====================================");

    // Prepare test messages
    let test_messages = vec![
        ("chat_service", "New message from user123".as_bytes()),
        ("auth_service", "Validate token abc123xyz".as_bytes()),
        ("storage_service", "Store file document.pdf".as_bytes()),
        ("chat_service", "User456 joined the room".as_bytes()),
        ("auth_service", "Refresh token requested".as_bytes()),
        ("storage_service", "Retrieve file image.jpg".as_bytes()),
    ];

    println!("\n🔸 Test 2A: Individual Message Sending (using saved actor refs)");
    println!("   Messages to send:");
    for (i, (actor, msg)) in test_messages.iter().enumerate() {
        println!(
            "     {}. {} <- {:?} ({} bytes)",
            i + 1,
            actor,
            std::str::from_utf8(msg).unwrap(),
            msg.len()
        );
    }

    let mut individual_send_times = Vec::new();
    let individual_start = Instant::now();

    for (i, (actor_name, message)) in test_messages.iter().enumerate() {
        let send_start = Instant::now();

        // Use the pre-created actor reference
        let actor_ref = match *actor_name {
            "chat_service" => &chat_ref,
            "auth_service" => &auth_ref,
            "storage_service" => &storage_ref,
            _ => panic!("Unknown actor"),
        };

        // Send message using the saved connection
        actor_ref.conn.tell(*message).await.unwrap();

        let send_time = send_start.elapsed();
        individual_send_times.push(send_time);

        println!(
            "     Message {} to {}: {:?} ({:.3} μs)",
            i + 1,
            actor_ref.name,
            send_time,
            send_time.as_nanos() as f64 / 1000.0
        );
    }

    let individual_total = individual_start.elapsed();
    let individual_avg = individual_total / test_messages.len() as u32;

    println!("   📈 Individual Sending Results:");
    println!(
        "     - Total time: {:?} ({:.3} μs)",
        individual_total,
        individual_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Average per message: {:?} ({:.3} μs)",
        individual_avg,
        individual_avg.as_nanos() as f64 / 1000.0
    );

    // Test 2B: Sequential sending (send all messages one by one using actor refs)
    println!("\n🔸 Test 2B: Sequential Sending (using actor refs, no lookups)");

    let sequential_start = Instant::now();

    // Send all messages using the saved actor refs
    for (actor_name, message) in &test_messages {
        let actor_ref = match *actor_name {
            "chat_service" => &chat_ref,
            "auth_service" => &auth_ref,
            "storage_service" => &storage_ref,
            _ => panic!("Unknown actor"),
        };
        actor_ref.conn.tell(*message).await.unwrap();
    }

    let sequential_total = sequential_start.elapsed();
    let sequential_avg = sequential_total / test_messages.len() as u32;

    println!("   📈 Sequential Sending Results:");
    println!(
        "     - Total time: {:?} ({:.3} μs)",
        sequential_total,
        sequential_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Average per message: {:?} ({:.3} μs)",
        sequential_avg,
        sequential_avg.as_nanos() as f64 / 1000.0
    );

    // Test 2C: True batch sending (group messages by actor)
    println!("\n🔸 Test 2C: True Batch Sending (group messages by destination)");

    let batch_start = Instant::now();

    // Group messages by actor
    let chat_messages: Vec<&[u8]> = test_messages
        .iter()
        .filter(|(actor, _)| *actor == "chat_service")
        .map(|(_, msg)| *msg)
        .collect();

    let auth_messages: Vec<&[u8]> = test_messages
        .iter()
        .filter(|(actor, _)| *actor == "auth_service")
        .map(|(_, msg)| *msg)
        .collect();

    let storage_messages: Vec<&[u8]> = test_messages
        .iter()
        .filter(|(actor, _)| *actor == "storage_service")
        .map(|(_, msg)| *msg)
        .collect();

    println!("   Message groups:");
    println!("     - chat_service: {} messages", chat_messages.len());
    println!("     - auth_service: {} messages", auth_messages.len());
    println!(
        "     - storage_service: {} messages",
        storage_messages.len()
    );

    // Send batches using actor refs
    chat_ref
        .conn
        .tell(kameo_remote::connection_pool::TellMessage::batch(
            chat_messages,
        ))
        .await
        .unwrap();
    auth_ref
        .conn
        .tell(kameo_remote::connection_pool::TellMessage::batch(
            auth_messages,
        ))
        .await
        .unwrap();
    storage_ref
        .conn
        .tell(kameo_remote::connection_pool::TellMessage::batch(
            storage_messages,
        ))
        .await
        .unwrap();

    let batch_total = batch_start.elapsed();
    let batch_avg = batch_total / test_messages.len() as u32;

    println!("   📈 True Batch Results:");
    println!(
        "     - Total time: {:?} ({:.3} ms)",
        batch_total,
        batch_total.as_millis() as f64
    );
    println!(
        "     - Average per message: {:?} ({:.3} μs)",
        batch_avg,
        batch_avg.as_nanos() as f64 / 1000.0
    );

    // ===========================================
    // PERFORMANCE COMPARISON
    // ===========================================
    println!("\n🎯 PERFORMANCE COMPARISON");
    println!("=========================");

    let batch_vs_individual = individual_total.as_nanos() as f64 / batch_total.as_nanos() as f64;
    let batch_vs_sequential = sequential_total.as_nanos() as f64 / batch_total.as_nanos() as f64;

    println!("\n📊 Results Summary:");
    println!("   Individual (one by one with actor refs):");
    println!(
        "     - Total: {:?} ({:.3} μs)",
        individual_total,
        individual_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Per message: {:?} ({:.3} μs)",
        individual_avg,
        individual_avg.as_nanos() as f64 / 1000.0
    );

    println!("   Sequential (same as individual, just timed differently):");
    println!(
        "     - Total: {:?} ({:.3} μs)",
        sequential_total,
        sequential_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Per message: {:?} ({:.3} μs)",
        sequential_avg,
        sequential_avg.as_nanos() as f64 / 1000.0
    );

    println!("   Batch (group by destination):");
    println!(
        "     - Total: {:?} ({:.3} μs)",
        batch_total,
        batch_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Per message: {:?} ({:.3} μs)",
        batch_avg,
        batch_avg.as_nanos() as f64 / 1000.0
    );
    println!("     - Speedup vs individual: {:.2}x", batch_vs_individual);
    println!("     - Speedup vs sequential: {:.2}x", batch_vs_sequential);

    println!("\n💡 Key Insights:");
    println!("   1. Actor refs eliminate lookup overhead - store refs after first lookup");
    println!(
        "   2. Batch sending provides {:.1}x-{:.1}x speedup by reducing network overhead",
        batch_vs_sequential, batch_vs_individual
    );
    println!(
        "   3. Initial lookup + connection time: {:.3} μs per actor",
        total_lookup_time.as_nanos() as f64 / 3000.0
    );
    println!(
        "   4. Once actor refs are created, sending is very fast (~{:.1} μs per message)",
        individual_avg.as_nanos() as f64 / 1000.0
    );

    // ===========================================
    // HIGH VOLUME TEST
    // ===========================================
    println!("\n📊 PART 3: HIGH VOLUME PERFORMANCE TEST");
    println!("======================================");

    let volume_count = 300; // 100 messages per service
    let _services = ["chat_service", "auth_service", "storage_service"];

    println!(
        "\n🔸 Test 3A: High Volume Individual ({}x3 = {} total messages)",
        volume_count,
        volume_count * 3
    );

    let vol_individual_start = Instant::now();
    for i in 0..volume_count {
        // Send to each service using actor refs
        let msg1 = format!("Message {} to chat_service", i);
        chat_ref.conn.tell(msg1.as_bytes()).await.unwrap();

        let msg2 = format!("Message {} to auth_service", i);
        auth_ref.conn.tell(msg2.as_bytes()).await.unwrap();

        let msg3 = format!("Message {} to storage_service", i);
        storage_ref.conn.tell(msg3.as_bytes()).await.unwrap();

        if i % 50 == 0 && i > 0 {
            println!("   - Sent {}/{} rounds", i, volume_count);
        }
    }
    let vol_individual_total = vol_individual_start.elapsed();
    let vol_individual_per_msg = vol_individual_total / (volume_count * 3) as u32;

    println!("   📈 High Volume Individual Results:");
    println!(
        "     - Total: {:?} ({:.3} ms)",
        vol_individual_total,
        vol_individual_total.as_millis() as f64
    );
    println!(
        "     - Per message: {:?} ({:.3} μs)",
        vol_individual_per_msg,
        vol_individual_per_msg.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Messages/sec: {:.0}",
        (volume_count * 3) as f64 / vol_individual_total.as_secs_f64()
    );

    println!("\n🔸 Test 3B: High Volume Batch (pre-lookup + batch by destination)");

    // Pre-generate all messages
    let mut chat_batch = Vec::new();
    let mut auth_batch = Vec::new();
    let mut storage_batch = Vec::new();

    for i in 0..volume_count {
        chat_batch.push(format!("Message {} to chat_service", i).into_bytes());
        auth_batch.push(format!("Message {} to auth_service", i).into_bytes());
        storage_batch.push(format!("Message {} to storage_service", i).into_bytes());
    }

    let vol_batch_start = Instant::now();

    // Send all batches
    let chat_batch_refs: Vec<&[u8]> = chat_batch.iter().map(|v| v.as_slice()).collect();
    let auth_batch_refs: Vec<&[u8]> = auth_batch.iter().map(|v| v.as_slice()).collect();
    let storage_batch_refs: Vec<&[u8]> = storage_batch.iter().map(|v| v.as_slice()).collect();

    chat_ref
        .conn
        .tell(kameo_remote::connection_pool::TellMessage::batch(
            chat_batch_refs,
        ))
        .await
        .unwrap();
    auth_ref
        .conn
        .tell(kameo_remote::connection_pool::TellMessage::batch(
            auth_batch_refs,
        ))
        .await
        .unwrap();
    storage_ref
        .conn
        .tell(kameo_remote::connection_pool::TellMessage::batch(
            storage_batch_refs,
        ))
        .await
        .unwrap();

    let vol_batch_total = vol_batch_start.elapsed();
    let vol_batch_per_msg = vol_batch_total / (volume_count * 3) as u32;

    println!("   📈 High Volume Batch Results:");
    println!(
        "     - Total: {:?} ({:.3} ms)",
        vol_batch_total,
        vol_batch_total.as_millis() as f64
    );
    println!(
        "     - Per message: {:?} ({:.3} μs)",
        vol_batch_per_msg,
        vol_batch_per_msg.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Messages/sec: {:.0}",
        (volume_count * 3) as f64 / vol_batch_total.as_secs_f64()
    );

    let vol_improvement =
        vol_individual_total.as_nanos() as f64 / vol_batch_total.as_nanos() as f64;
    println!(
        "   🎯 High volume improvement: {:.1}x faster with batching",
        vol_improvement
    );

    // ===========================================
    // RECOMMENDATIONS
    // ===========================================
    println!("\n📋 RECOMMENDATIONS");
    println!("==================");

    println!("\n✅ Best Practices:");
    println!("   1. Do lookup() once and save actor refs for repeated communication");
    println!("   2. Group messages by destination actor for batch sending");
    println!("   3. Use tell() for fire-and-forget, ask() for request-response");
    println!(
        "   4. For high-throughput scenarios, batching provides {:.0}x-{:.0}x improvement",
        batch_vs_individual.min(vol_improvement),
        batch_vs_individual.max(vol_improvement)
    );

    println!("\n🔧 Code Examples:");
    println!("   // Create actor ref once:");
    println!("   let location = registry.lookup(\"chat_service\").await?;");
    println!("   let conn = registry.get_connection(peer_addr).await?;");
    println!("   let chat_ref = ActorRef {{ location, conn }};");
    println!();
    println!("   // Use actor ref for all messages:");
    println!("   chat_ref.conn.tell(message1).await?;");
    println!("   chat_ref.conn.tell(message2).await?;");
    println!();
    println!("   // Or batch messages:");
    println!("   let messages = vec![msg1, msg2, msg3];");
    println!("   chat_ref.conn.tell(TellMessage::batch(messages)).await?;");

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;
    node3.shutdown().await;

    println!("\n✅ Test completed successfully!");
}

/// Test error handling when actors are not found
#[tokio::test]
async fn test_lookup_error_handling() {
    println!("🔍 Testing lookup error handling");

    let config = GossipConfig::default();
    let keypair = KeyPair::new_for_testing("lookup_error_node");
    let node = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:29100".parse().unwrap(),
        keypair,
        Some(config),
    )
    .await
    .unwrap();

    // Try to lookup non-existent actor
    let result = node.lookup("non_existent_actor").await;
    assert!(
        result.is_none(),
        "Lookup should return None for non-existent actors"
    );

    println!("✅ Lookup correctly returns None for non-existent actors");

    node.shutdown().await;
}
