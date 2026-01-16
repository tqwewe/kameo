use anyhow::Result;
use std::fs;
use std::path::Path;
use kameo_remote::{SecretKey, NodeId};

/// Example showing how to generate and manage Ed25519 keypairs for TLS communication
#[tokio::main]
async fn main() -> Result<()> {
    // Install crypto provider for TLS
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    
    println!("=== Kameo Remote TLS Keypair Generation Example ===\n");
    
    // 1. Generate a new random TLS keypair
    println!("1. Generating new random TLS keypair...");
    let secret_key1 = SecretKey::generate();
    let node_id1 = secret_key1.public();
    
    println!("   Public Key (NodeId): {}", node_id1.fmt_short());
    println!("   Public Key Hex: {}", hex::encode(node_id1.as_bytes()));
    println!("   Private Key: [HIDDEN - 32 bytes]\n");
    
    // 2. Save keypair to file and load it back
    println!("2. Saving and loading keypair from file...");
    let keypair_path = "/tmp/test_tls_keypair.key";
    
    // Save private key to file
    fs::write(keypair_path, hex::encode(secret_key1.to_bytes()))?;
    println!("   Saved private key to: {}", keypair_path);
    
    // Load private key from file
    let key_hex = fs::read_to_string(keypair_path)?;
    let key_bytes = hex::decode(key_hex.trim())?;
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&key_bytes);
    let secret_key2 = SecretKey::from_bytes(&arr)?;
    let node_id2 = secret_key2.public();
    
    println!("   Loaded keypair from file");
    println!("   Node IDs match: {}\n", node_id1 == node_id2);
    
    // 3. Demonstrate TLS certificate properties
    println!("3. Demonstrating TLS certificate properties...");
    let tls_key1 = SecretKey::generate();
    let tls_key2 = SecretKey::generate();
    let tls_node1 = tls_key1.public();
    let tls_node2 = tls_key2.public();
    
    println!("   TLS key 1 NodeId: {}", tls_node1.fmt_short());
    println!("   TLS key 2 NodeId: {}", tls_node2.fmt_short());
    println!("   NodeIds are unique: {}\n", tls_node1 != tls_node2);
    
    // 4. Demonstrate TLS identity verification
    println!("4. Demonstrating TLS identity verification...");
    
    // In TLS, identity is verified through the handshake process
    println!("   TLS uses Ed25519 certificates for:");
    println!("   - Server authentication during handshake");
    println!("   - Mutual TLS authentication (mTLS)");
    println!("   - Perfect forward secrecy with ephemeral keys");
    println!("   ");
    println!("   Each NodeId represents a unique TLS certificate");
    println!("   Connections are verified automatically during TLS handshake\n");
    
    // 5. Show NodeId conversions for TLS
    println!("5. Demonstrating NodeId conversions for TLS...");
    
    // NodeId represents the public key of a TLS certificate
    let tls_node = secret_key1.public();
    println!("   TLS NodeId: {}", tls_node.fmt_short());
    println!("   Full hex: {}", hex::encode(tls_node.as_bytes()));
    
    // Convert between formats
    let node_from_bytes = NodeId::from_bytes(&tls_node.as_bytes())?;
    println!("   Roundtrip conversion successful: {}\n", tls_node == node_from_bytes);
    
    // 6. Generate multiple TLS keypairs for a distributed setup
    println!("6. Generating TLS keypairs for distributed setup...");
    let server_key = SecretKey::generate();
    let client1_key = SecretKey::generate();
    let client2_key = SecretKey::generate();
    
    println!("   Server NodeId:  {}", server_key.public().fmt_short());
    println!("   Client1 NodeId: {}", client1_key.public().fmt_short());
    println!("   Client2 NodeId: {}\n", client2_key.public().fmt_short());
    
    // Save all keypairs
    let keys_dir = "/tmp/kameo_tls_keys";
    fs::create_dir_all(keys_dir)?;
    
    fs::write(format!("{}/server.key", keys_dir), hex::encode(server_key.to_bytes()))?;
    fs::write(format!("{}/client1.key", keys_dir), hex::encode(client1_key.to_bytes()))?;
    fs::write(format!("{}/client2.key", keys_dir), hex::encode(client2_key.to_bytes()))?;
    
    // Save public keys for easy sharing
    fs::write(format!("{}/server.pub", keys_dir), hex::encode(server_key.public().as_bytes()))?;
    fs::write(format!("{}/client1.pub", keys_dir), hex::encode(client1_key.public().as_bytes()))?;
    fs::write(format!("{}/client2.pub", keys_dir), hex::encode(client2_key.public().as_bytes()))?;
    
    println!("   Saved all TLS keypairs to: {}", keys_dir);
    println!("   Private keys: *.key files (keep secret!)");
    println!("   Public keys: *.pub files (safe to share)\n");
    
    // 7. Show how to use TLS keys with the registry
    println!("7. How to use these keys with TLS:");
    println!("   // Server setup:");
    println!("   let secret_key = load_key(\"/tmp/kameo_tls_keys/server.key\")?;");
    println!("   let registry = GossipRegistryHandle::new_with_tls(");
    println!("       bind_addr, secret_key, config");
    println!("   ).await?;");
    println!();
    println!("   // Client setup with server verification:");
    println!("   let client_key = load_key(\"/tmp/kameo_tls_keys/client1.key\")?;");
    println!("   let server_node_id = load_node_id(\"/tmp/kameo_tls_keys/server.pub\")?;");
    println!("   registry.add_peer_with_node_id(server_addr, Some(server_node_id)).await;");
    
    // Clean up
    if Path::new(keypair_path).exists() {
        fs::remove_file(keypair_path)?;
    }
    
    println!("\n=== TLS Keypair Generation Example Complete ===");
    println!("Use these keypairs for secure TLS communication!");
    
    Ok(())
}