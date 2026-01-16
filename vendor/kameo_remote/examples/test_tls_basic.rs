use kameo_remote::tls::{name, TlsConfig};
use kameo_remote::SecretKey;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install default crypto provider (ring)
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Enable debug logging
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug")
        .init();

    // Generate keys for both nodes
    let server_key = SecretKey::generate();
    let server_node_id = server_key.public();
    println!("Server NodeId: {}", server_node_id.fmt_short());

    let client_key = SecretKey::generate();
    let client_node_id = client_key.public();
    println!("Client NodeId: {}", client_node_id.fmt_short());

    // Create TLS configurations
    let server_tls = TlsConfig::new(server_key)?;
    let client_tls = TlsConfig::new(client_key)?;

    // Start server
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("Server listening on: {}", addr);

    let server_handle = tokio::spawn(async move {
        let (stream, peer_addr) = listener.accept().await.unwrap();
        println!("Server: Accepted connection from {}", peer_addr);

        // Perform TLS handshake
        let acceptor = server_tls.acceptor();
        let mut tls_stream = acceptor.accept(stream).await.unwrap();
        println!("Server: TLS handshake complete");

        // Read message
        let mut buf = [0u8; 100];
        let n = tls_stream.read(&mut buf).await.unwrap();
        let msg = String::from_utf8_lossy(&buf[..n]);
        println!("Server received: {}", msg);

        // Send response
        tls_stream.write_all(b"Hello from server!").await.unwrap();
        println!("Server: Sent response");
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client connects
    let stream = TcpStream::connect(addr).await?;
    println!("Client: Connected to server");

    // Create DNS name for the server
    let server_name = name::encode(&server_node_id);
    let dns_name = rustls::pki_types::ServerName::try_from(server_name.clone())?;

    // Perform TLS handshake
    let connector = client_tls.connector();
    let mut tls_stream = connector.connect(dns_name, stream).await?;
    println!("Client: TLS handshake complete");

    // Send message
    tls_stream.write_all(b"Hello from client!").await?;
    println!("Client: Sent message");

    // Read response
    let mut buf = [0u8; 100];
    let n = tls_stream.read(&mut buf).await?;
    let msg = String::from_utf8_lossy(&buf[..n]);
    println!("Client received: {}", msg);

    // Wait for server to finish
    server_handle.await?;

    println!("\nTLS test successful!");
    println!("✓ Generated Ed25519 keypairs");
    println!("✓ Created self-signed certificates");
    println!("✓ Completed TLS 1.3 handshake");
    println!("✓ Exchanged encrypted messages");

    Ok(())
}
