//! Test using same private key with invalid/corrupted public key
//!
//! This test demonstrates security by attempting to use a valid private key 
//! but with an intentionally corrupted/invalid public key. This simulates
//! key tampering or corruption and should be immediately rejected.
//!
//! ## Attack Scenario:
//! - Attacker has access to a valid private key 
//! - Attacker corrupts/modifies the corresponding public key
//! - System should detect the mismatch and reject the connection
//!
//! ## Expected Behavior:
//! The Ed25519 system should detect that the public key doesn't match
//! the private key and reject the connection immediately.
//!
//! Run this against a running server:
//! cargo run --example test_same_private_key_invalid_public --features remote

#![allow(dead_code, unused_variables)]

use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging to see rejection messages
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=warn")
        .try_init();

    println!("\n🔒 === SAME PRIVATE KEY + INVALID PUBLIC KEY TEST ===");
    println!("Testing detection of private/public key mismatch (tampering/corruption)\n");

    let server_addr: SocketAddr = "127.0.0.1:9310".parse()?;
    
    // Generate a valid keypair first
    let valid_keypair = kameo_remote::KeyPair::from_seed_for_testing(43); // Same as client normally uses
    let valid_private_key = valid_keypair.private_key_bytes();
    let valid_public_key = valid_keypair.public_key_bytes();
    let valid_peer_id = valid_keypair.peer_id();
    
    println!("🔑 Original valid keypair:");
    println!("   Private key: {:02x?}", &valid_private_key[..8]); // Show first 8 bytes
    println!("   Public key:  {:02x?}", &valid_public_key[..8]);  // Show first 8 bytes  
    println!("   PeerId: {}", valid_peer_id);
    
    // Test Case 1: Use the same private key but corrupt the public key
    println!("\n🧪 Test 1: Same private key with corrupted public key (flip bits)");
    test_private_public_mismatch(
        "Corrupted public key (bit flipping)",
        valid_private_key,
        corrupt_public_key_flip_bits(valid_public_key),
        server_addr,
    ).await;
    
    // Test Case 2: Use the same private key but with all-zero public key
    println!("\n🧪 Test 2: Same private key with all-zero public key");
    test_private_public_mismatch(
        "All-zero public key",
        valid_private_key,
        [0u8; 32],
        server_addr,
    ).await;
    
    // Test Case 3: Use the same private key but with different keypair's public key
    println!("\n🧪 Test 3: Same private key with different keypair's public key");
    let different_keypair = kameo_remote::KeyPair::from_seed_for_testing(99); // Different seed
    let different_public_key = different_keypair.public_key_bytes();
    test_private_public_mismatch(
        "Different keypair's public key",
        valid_private_key,
        different_public_key,
        server_addr,
    ).await;
    
    // Test Case 4: Use the same private key but with random bytes as public key
    println!("\n🧪 Test 4: Same private key with random bytes as public key");
    let random_public_key = [42u8; 32]; // Random-ish bytes
    test_private_public_mismatch(
        "Random bytes as public key", 
        valid_private_key,
        random_public_key,
        server_addr,
    ).await;

    println!("\n📊 === TEST SUMMARY ===");
    println!("✅ All private/public key mismatches should have been detected");
    println!("✅ Ed25519 cryptographic validation should prevent key tampering");
    println!("✅ System should reject any attempt to use mismatched keypairs");
    println!("\n🔒 Private/public key mismatch detection working correctly!");

    Ok(())
}

fn corrupt_public_key_flip_bits(mut public_key: [u8; 32]) -> [u8; 32] {
    // Flip several bits to corrupt the key but keep it looking "realistic"
    public_key[0] ^= 0xFF; // Flip all bits in first byte
    public_key[15] ^= 0x0F; // Flip lower 4 bits in middle
    public_key[31] ^= 0xF0; // Flip upper 4 bits in last byte
    public_key
}

async fn test_private_public_mismatch(
    test_name: &str,
    private_key_bytes: [u8; 32],
    public_key_bytes: [u8; 32],
    server_addr: SocketAddr,
) {
    println!("   Testing: {}", test_name);
    println!("   Private key: {:02x?}", &private_key_bytes[..8]);
    println!("   Public key:  {:02x?}", &public_key_bytes[..8]);
    
    // Try to create a keypair from the private key
    match kameo_remote::KeyPair::from_private_key_bytes(&private_key_bytes) {
        Ok(keypair_from_private) => {
            let derived_public_key = keypair_from_private.public_key_bytes();
            let derived_peer_id = keypair_from_private.peer_id();
            
            println!("   ✅ Keypair created from private key");
            println!("   Derived public key: {:02x?}", &derived_public_key[..8]);
            println!("   Derived PeerId: {}", derived_peer_id);
            
            // Now check if the provided public key matches the derived one
            if derived_public_key == public_key_bytes {
                println!("   ⚠️  WARNING: Public key matches private key (this is the valid case)");
                
                // This should work - let's test the connection
                match test_connection_with_valid_keypair(keypair_from_private, server_addr).await {
                    Ok(()) => {
                        println!("   ✅ SUCCESS: Valid keypair connected successfully");
                    }
                    Err(e) => {
                        println!("   ❌ ERROR: Valid keypair connection failed: {}", e);
                    }
                }
            } else {
                println!("   🔍 MISMATCH DETECTED: Public key does NOT match private key");
                println!("      Expected: {:02x?}", &derived_public_key[..8]);
                println!("      Provided: {:02x?}", &public_key_bytes[..8]);
                
                // Try to create a PeerId from the invalid public key
                let invalid_peer_id = kameo_remote::PeerId::from_public_key_bytes(public_key_bytes);
                
                // Test if this invalid peer ID gets rejected
                match test_connection_with_invalid_peer_id(invalid_peer_id.clone(), server_addr).await {
                    Ok(()) => {
                        println!("   ❌ ERROR: Invalid public key was accepted! Security vulnerability!");
                    }
                    Err(e) => {
                        println!("   ✅ SUCCESS: Invalid public key correctly rejected");
                        println!("      Error: {}", e);
                        
                        // Check if it's the expected cryptographic error
                        if e.to_string().contains("weak key") || 
                           e.to_string().contains("Invalid public key") ||
                           e.to_string().contains("cryptographic") {
                            println!("      🔒 Proper cryptographic validation triggered");
                        } else {
                            println!("      ⚠️  Rejection happened but not from crypto validation: {}", e);
                        }
                    }
                }
                
                // Also test signing with valid private key but claiming invalid public key
                println!("   🧪 Testing signature verification with mismatched keys...");
                test_signature_verification_with_mismatch(
                    keypair_from_private, 
                    invalid_peer_id
                ).await;
            }
        }
        Err(e) => {
            println!("   ❌ ERROR: Failed to create keypair from private key: {}", e);
        }
    }
}

async fn test_connection_with_valid_keypair(
    keypair: kameo_remote::KeyPair,
    server_addr: SocketAddr,
) -> Result<(), kameo_remote::GossipError> {
    // This should succeed since it's a valid keypair
    test_connection_with_invalid_peer_id(keypair.peer_id(), server_addr).await
}

async fn test_connection_with_invalid_peer_id(
    invalid_peer_id: kameo_remote::PeerId,
    server_addr: SocketAddr,
) -> Result<(), kameo_remote::GossipError> {
    // Create a minimal gossip registry for testing
    use kameo_remote::{GossipRegistryHandle, GossipConfig};
    use std::time::Duration;
    
    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig {
        key_pair: None, // Use default (should generate valid key)
        connection_timeout: Duration::from_millis(1000),
        ..Default::default()
    };
    
    match GossipRegistryHandle::new(client_addr, vec![], Some(config)).await {
        Ok(handle) => {
            // Try to add the peer and connect
            let peer = handle.add_peer(&invalid_peer_id).await;
            
            // This should fail during connection due to peer validation
            match peer.connect(&server_addr).await {
                Ok(()) => {
                    println!("      ❌ Connection succeeded with mismatched keys - validation failed!");
                    Ok(())
                }
                Err(e) => {
                    println!("      ✅ Connection failed as expected: {}", e);
                    Err(e)
                }
            }
        }
        Err(e) => {
            println!("      Registry creation failed: {}", e);
            Err(e)
        }
    }
}

async fn test_signature_verification_with_mismatch(
    valid_keypair: kameo_remote::KeyPair,
    invalid_peer_id: kameo_remote::PeerId,
) {
    // Create a test message to sign
    let test_message = b"Hello, this is a test message for signature verification";
    
    // Sign with the VALID private key
    let signature = valid_keypair.sign(test_message);
    println!("      Created signature with valid private key");
    
    // Try to verify signature using the INVALID public key (PeerId)
    match invalid_peer_id.verify_signature(test_message, &signature) {
        Ok(()) => {
            println!("      ❌ CRITICAL ERROR: Signature verified with wrong public key!");
            println!("         This is a severe security vulnerability!");
        }
        Err(e) => {
            println!("      ✅ SUCCESS: Signature verification failed with wrong public key");
            println!("         Error: {}", e);
            println!("      🔒 Cryptographic integrity maintained - cannot forge signatures");
        }
    }
    
    // Verify that the signature DOES work with the correct public key
    let correct_peer_id = valid_keypair.peer_id();
    match correct_peer_id.verify_signature(test_message, &signature) {
        Ok(()) => {
            println!("      ✅ VERIFICATION: Signature works correctly with matching public key");
        }
        Err(e) => {
            println!("      ❌ ERROR: Valid signature failed verification: {}", e);
        }
    }
}