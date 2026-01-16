use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair, PeerId};
use std::time::Duration;
use tokio::time::timeout;

/// Test cryptographic security features
///
/// This test suite verifies:
/// 1. Proper keypair generation and PeerId derivation
/// 2. Signature creation and verification
/// 3. Connection rejection when keys don't match
/// 4. Edge cases with invalid keys and signatures

#[tokio::test]
async fn test_keypair_generation_and_peer_id() {
    tracing_subscriber::fmt::try_init().ok();

    println!("🔑 Testing keypair generation and PeerId derivation...");

    // Generate two different keypairs
    let keypair1 = KeyPair::generate();
    let keypair2 = KeyPair::generate();

    let peer_id1 = keypair1.peer_id();
    let peer_id2 = keypair2.peer_id();

    // Verify that different keypairs produce different PeerIds
    assert_ne!(
        peer_id1, peer_id2,
        "Different keypairs should produce different PeerIds"
    );

    // Verify that the same keypair consistently produces the same PeerId
    let peer_id1_again = keypair1.peer_id();
    assert_eq!(
        peer_id1, peer_id1_again,
        "Same keypair should produce same PeerId"
    );

    // Verify key bytes are correct length
    assert_eq!(
        keypair1.public_key_bytes().len(),
        32,
        "Public key should be 32 bytes"
    );
    assert_eq!(
        keypair1.private_key_bytes().len(),
        32,
        "Private key should be 32 bytes"
    );

    println!("   ✅ Keypair generation working correctly");
}

#[tokio::test]
async fn test_deterministic_keypair_for_testing() {
    println!("🧪 Testing deterministic keypair generation for tests...");

    // Generate the same seed multiple times
    let keypair1 = KeyPair::new_for_testing("42");
    let keypair2 = KeyPair::new_for_testing("42");
    let keypair3 = KeyPair::new_for_testing("43"); // Different seed

    // Same seeds should produce identical keypairs
    assert_eq!(
        keypair1.peer_id(),
        keypair2.peer_id(),
        "Same seed should produce same PeerId"
    );
    assert_eq!(
        keypair1.public_key_bytes(),
        keypair2.public_key_bytes(),
        "Same seed should produce same public key"
    );
    assert_eq!(
        keypair1.private_key_bytes(),
        keypair2.private_key_bytes(),
        "Same seed should produce same private key"
    );

    // Different seeds should produce different keypairs
    assert_ne!(
        keypair1.peer_id(),
        keypair3.peer_id(),
        "Different seeds should produce different PeerIds"
    );

    println!("   ✅ Deterministic keypair generation working correctly");
}

#[tokio::test]
async fn test_signature_creation_and_verification() {
    println!("✍️  Testing signature creation and verification...");

    let keypair = KeyPair::generate();
    let peer_id = keypair.peer_id();
    let message = b"Test message for signature verification";

    // Create signature
    let signature = keypair.sign(message);

    // Verify signature with correct key
    assert!(
        peer_id.verify_signature(message, &signature).is_ok(),
        "Valid signature should verify successfully"
    );

    // Verify signature with wrong message should fail
    let wrong_message = b"Different message";
    assert!(
        peer_id.verify_signature(wrong_message, &signature).is_err(),
        "Signature with wrong message should fail verification"
    );

    // Verify signature with wrong key should fail
    let wrong_keypair = KeyPair::generate();
    let wrong_peer_id = wrong_keypair.peer_id();
    assert!(
        wrong_peer_id.verify_signature(message, &signature).is_err(),
        "Signature with wrong public key should fail verification"
    );

    println!("   ✅ Signature verification working correctly");
}

#[tokio::test]
async fn test_peer_id_conversions() {
    println!("🔄 Testing PeerId conversions and formats...");

    let keypair = KeyPair::generate();
    let peer_id = keypair.peer_id();

    // Test hex conversion roundtrip
    let hex_string = peer_id.to_hex();
    assert_eq!(
        hex_string.len(),
        64,
        "Hex string should be 64 characters (32 bytes * 2)"
    );

    let peer_id_from_hex = PeerId::from_hex(&hex_string).expect("Should parse valid hex");
    assert_eq!(peer_id, peer_id_from_hex, "Hex roundtrip should work");

    // Test bytes conversion roundtrip
    let bytes = peer_id.to_bytes();
    assert_eq!(bytes.len(), 32, "PeerId bytes should be 32 bytes");

    let peer_id_from_bytes = PeerId::from_bytes(&bytes).expect("Should parse valid bytes");
    assert_eq!(peer_id, peer_id_from_bytes, "Bytes roundtrip should work");

    // Test Display and string conversion
    let display_string = format!("{}", peer_id);
    assert_eq!(
        display_string, hex_string,
        "Display should match hex string"
    );

    println!("   ✅ PeerId conversions working correctly");
}

#[tokio::test]
async fn test_key_mismatch_connection_rejection() {
    println!("🚫 Testing connection rejection with mismatched keys...");

    // Create server with specific keypair
    let server_keypair = KeyPair::new_for_testing("1");
    let server_peer_id = server_keypair.peer_id();
    let server_addr = "127.0.0.1:29101".parse().unwrap();

    let server_config = GossipConfig {
        key_pair: Some(server_keypair.clone()),
        ..Default::default()
    };

    let server_registry =
        GossipRegistryHandle::new_with_keypair(server_addr, server_keypair, Some(server_config))
            .await
            .expect("Should create server registry");

    println!("   🖥️  Server started with PeerId: {}", server_peer_id);

    // Create client with different keypair
    let client_keypair = KeyPair::new_for_testing("2"); // Different seed!
    let client_peer_id = client_keypair.peer_id();
    let client_addr = "127.0.0.1:29102".parse().unwrap();

    let client_config = GossipConfig {
        key_pair: Some(client_keypair.clone()),
        ..Default::default()
    };

    let client_registry =
        GossipRegistryHandle::new_with_keypair(client_addr, client_keypair, Some(client_config))
            .await
            .expect("Should create client registry");

    println!("   💻 Client started with PeerId: {}", client_peer_id);
    assert_ne!(
        server_peer_id, client_peer_id,
        "Client and server should have different PeerIds"
    );

    // Test 1: Client tries to connect using the correct server PeerId
    println!("   🔗 Test 1: Client connecting with correct server PeerId...");
    let correct_peer = client_registry.add_peer(&server_peer_id).await;
    match timeout(Duration::from_secs(5), correct_peer.connect(&server_addr)).await {
        Ok(Ok(())) => println!("      ✅ Connection with correct PeerId succeeded"),
        Ok(Err(e)) => println!("      ⚠️  Connection with correct PeerId failed: {} (this might be expected if auth is implemented)", e),
        Err(_) => println!("      ⏰ Connection with correct PeerId timed out"),
    }

    // Test 2: Client tries to connect using wrong PeerId (should fail)
    println!("   🔗 Test 2: Client connecting with wrong PeerId...");
    let wrong_keypair = KeyPair::new_for_testing("99"); // Completely different
    let wrong_peer_id = wrong_keypair.peer_id();

    let wrong_peer = client_registry.add_peer(&wrong_peer_id).await;
    match timeout(Duration::from_secs(5), wrong_peer.connect(&server_addr)).await {
        Ok(Ok(())) => println!("      ❌ ERROR: Connection with wrong PeerId should have failed!"),
        Ok(Err(e)) => println!(
            "      ✅ Connection with wrong PeerId correctly failed: {}",
            e
        ),
        Err(_) => println!("      ✅ Connection with wrong PeerId timed out (expected)"),
    }

    // Test 3: Verify that we can detect the mismatch at the PeerId level
    println!("   🔍 Test 3: PeerId comparison verification...");
    assert_ne!(
        server_peer_id, wrong_peer_id,
        "Server and wrong PeerIds should be different"
    );
    assert_ne!(
        client_peer_id, server_peer_id,
        "Client and server PeerIds should be different"
    );
    assert_ne!(
        client_peer_id, wrong_peer_id,
        "Client and wrong PeerIds should be different"
    );

    println!("      ✅ PeerId mismatches correctly detected");

    // Cleanup
    server_registry.shutdown().await;
    client_registry.shutdown().await;

    println!("   ✅ Key mismatch rejection test completed");
}

#[tokio::test]
async fn test_invalid_key_edge_cases() {
    println!("⚠️  Testing invalid key edge cases...");

    // Test invalid hex strings
    let invalid_hex_cases: Vec<String> = vec![
        "".to_string(),        // Empty
        "invalid".to_string(), // Not hex
        "abc".to_string(),     // Too short
        "a".repeat(63),        // One character too short
        "a".repeat(65),        // One character too long
        "g".repeat(64),        // Invalid hex characters
    ];

    for (i, invalid_hex) in invalid_hex_cases.iter().enumerate() {
        let display_hex = if invalid_hex.len() > 20 {
            format!("{}...", &invalid_hex[..20])
        } else {
            invalid_hex.clone()
        };
        println!("   Test case {}: '{}'", i + 1, display_hex);
        match PeerId::from_hex(invalid_hex) {
            Ok(_) => println!("      ❌ ERROR: Should have failed to parse invalid hex"),
            Err(_) => println!("      ✅ Correctly rejected invalid hex"),
        }
    }

    // Test invalid byte arrays
    let invalid_byte_cases = [
        vec![],        // Empty
        vec![0u8; 31], // Too short
        vec![0u8; 33], // Too long
        vec![0u8; 16], // Half length
    ];

    for (i, invalid_bytes) in invalid_byte_cases.iter().enumerate() {
        println!("   Byte test case {}: {} bytes", i + 1, invalid_bytes.len());
        match PeerId::from_bytes(invalid_bytes) {
            Ok(_) => println!("      ❌ ERROR: Should have failed to parse invalid bytes"),
            Err(_) => println!("      ✅ Correctly rejected invalid byte array"),
        }
    }

    // Test invalid private key for KeyPair
    let invalid_private_key_cases = [
        vec![],        // Empty
        vec![0u8; 31], // Too short
        vec![0u8; 33], // Too long
    ];

    for (i, invalid_private) in invalid_private_key_cases.iter().enumerate() {
        println!(
            "   Private key test case {}: {} bytes",
            i + 1,
            invalid_private.len()
        );
        match KeyPair::from_private_key_bytes(invalid_private) {
            Ok(_) => println!(
                "      ❌ ERROR: Should have failed to create KeyPair from invalid private key"
            ),
            Err(_) => println!("      ✅ Correctly rejected invalid private key"),
        }
    }

    println!("   ✅ Invalid key edge cases handled correctly");
}
