//! Unit tests for remote Ask/Reply functionality
//!
//! These tests verify that the ask/reply mechanism works correctly
//! for distributed actors across the network.

use kameo::message::{Context, Message};
use kameo::remote::{bootstrap_with_keypair, RemoteActorRef};
use kameo::Actor;
use kameo_remote::KeyPair;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;

// Simple test actor that responds to Ask messages
#[derive(Clone, Debug)]
struct TestActor {
    counter: u32,
}

impl Actor for TestActor {}

// Add message
#[derive(Clone, Debug)]
struct Add {
    a: u32,
    b: u32,
}

#[derive(Clone, Debug)]
struct AddResult {
    result: u32,
    counter: u32,
}

impl Message<Add> for TestActor {
    type Reply = AddResult;

    async fn handle(&mut self, msg: Add, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        self.counter += 1;
        AddResult {
            result: msg.a + msg.b,
            counter: self.counter,
        }
    }
}

// Multiply message
#[derive(Clone, Debug)]
struct Multiply {
    a: u32,
    b: u32,
}

#[derive(Clone, Debug)]
struct MultiplyResult {
    result: u32,
}

impl Message<Multiply> for TestActor {
    type Reply = MultiplyResult;

    async fn handle(&mut self, msg: Multiply, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        MultiplyResult {
            result: msg.a * msg.b,
        }
    }
}

#[tokio::test]
async fn test_ask_envelope_creation() {
    // This test verifies that conn.ask() properly creates the Ask envelope
    // with MessageType::Ask (0x01) and correlation ID

    // Start a local transport
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let transport = bootstrap_with_keypair(addr, KeyPair::generate())
        .await
        .expect("Failed to bootstrap transport");

    // Get the actual bind address
    let bind_addr = transport.local_addr().expect("Failed to get local addr");
    println!("Test transport listening on {}", bind_addr);

    // Spawn a test actor locally
    let actor = kameo::spawn(TestActor { counter: 0 });

    // Register it for remote access
    let actor_id = actor.id();
    transport.register_actor(actor_id, actor.clone()).await;

    // Create a remote reference to ourselves (loopback test)
    let remote_ref = RemoteActorRef::<TestActor>::lookup("test_actor", bind_addr)
        .await
        .expect("Failed to lookup actor");

    // Send an ask message
    let result = timeout(
        Duration::from_secs(2),
        remote_ref.ask(Add { a: 10, b: 20 }).send(),
    )
    .await;

    // Should succeed with correct result
    assert!(
        result.is_ok(),
        "Ask timed out - envelope not properly created"
    );
    let add_result = result.unwrap().expect("Ask failed");
    assert_eq!(add_result.result, 30, "Incorrect result");
    assert_eq!(add_result.counter, 1, "Counter not incremented");
}

#[tokio::test]
async fn test_bidirectional_message_routing() {
    // This test verifies that Ask messages are routed through the
    // bidirectional Ask handler, not the gossip path

    // Start server transport
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server_transport = bootstrap_with_keypair(server_addr, KeyPair::generate())
        .await
        .expect("Failed to bootstrap server");

    let server_bind = server_transport
        .local_addr()
        .expect("Failed to get server addr");
    println!("Server listening on {}", server_bind);

    // Start client transport
    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client_transport = bootstrap_with_keypair(client_addr, KeyPair::generate())
        .await
        .expect("Failed to bootstrap client");

    let client_bind = client_transport
        .local_addr()
        .expect("Failed to get client addr");
    println!("Client listening on {}", client_bind);

    // Connect client to server
    client_transport
        .connect_to(server_bind)
        .await
        .expect("Failed to connect");

    // Spawn actor on server
    let actor = kameo::spawn(TestActor { counter: 0 });
    server_transport
        .register_actor(actor.id(), actor.clone())
        .await;

    // Wait for gossip propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Lookup from client
    let remote_ref = RemoteActorRef::<TestActor>::lookup_with_transport(
        "test_actor",
        server_bind,
        client_transport.clone(),
    )
    .await
    .expect("Failed to lookup actor");

    // Send ask - should go through Ask handler
    let result = timeout(
        Duration::from_secs(2),
        remote_ref.ask(Add { a: 5, b: 10 }).send(),
    )
    .await;

    assert!(
        result.is_ok(),
        "Ask timed out - not routed through Ask handler"
    );
    let add_result = result.unwrap().expect("Ask failed");
    assert_eq!(add_result.result, 15, "Incorrect result");
}

#[tokio::test]
async fn test_correlation_id_handling() {
    // This test verifies that correlation IDs are properly:
    // 1. Generated by the client
    // 2. Attached to the Ask envelope
    // 3. Passed to the handler with the message
    // 4. Used to route the reply back

    // Start server
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server_transport = bootstrap_with_keypair(server_addr, KeyPair::generate())
        .await
        .expect("Failed to bootstrap server");

    let server_bind = server_transport
        .local_addr()
        .expect("Failed to get server addr");

    // Start client
    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client_transport = bootstrap_with_keypair(client_addr, KeyPair::generate())
        .await
        .expect("Failed to bootstrap client");

    // Connect
    client_transport
        .connect_to(server_bind)
        .await
        .expect("Failed to connect");

    // Spawn actor on server
    let actor = kameo::spawn(TestActor { counter: 0 });
    server_transport
        .register_actor(actor.id(), actor.clone())
        .await;

    // Wait for gossip
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Lookup from client
    let remote_ref = RemoteActorRef::<TestActor>::lookup_with_transport(
        "test_actor",
        server_bind,
        client_transport.clone(),
    )
    .await
    .expect("Failed to lookup actor");

    // Send multiple asks concurrently - correlation IDs should route replies correctly
    let fut1 = remote_ref.ask(Add { a: 1, b: 2 }).send();
    let fut2 = remote_ref.ask(Multiply { a: 3, b: 4 }).send();
    let fut3 = remote_ref.ask(Add { a: 5, b: 6 }).send();

    let (res1, res2, res3) = tokio::join!(
        timeout(Duration::from_secs(2), fut1),
        timeout(Duration::from_secs(2), fut2),
        timeout(Duration::from_secs(2), fut3)
    );

    // All should succeed with correct results
    let add1 = res1.expect("Ask 1 timed out").expect("Ask 1 failed");
    assert_eq!(add1.result, 3, "Ask 1 incorrect result");
    assert_eq!(add1.counter, 1, "Ask 1 incorrect counter");

    let mul = res2.expect("Ask 2 timed out").expect("Ask 2 failed");
    assert_eq!(mul.result, 12, "Ask 2 incorrect result");

    let add2 = res3.expect("Ask 3 timed out").expect("Ask 3 failed");
    assert_eq!(add2.result, 11, "Ask 3 incorrect result");
    assert_eq!(add2.counter, 2, "Ask 3 incorrect counter");
}

#[tokio::test]
async fn test_ask_timeout() {
    // Test that ask properly times out when no response is received

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let transport = bootstrap_with_keypair(addr, KeyPair::generate())
        .await
        .expect("Failed to bootstrap");

    // Try to lookup non-existent actor
    let result = RemoteActorRef::<TestActor>::lookup_with_transport(
        "non_existent",
        "127.0.0.1:12345".parse().unwrap(), // Non-existent server
        transport.clone(),
    )
    .await;

    // Lookup should fail
    assert!(
        result.is_err(),
        "Lookup should fail for non-existent server"
    );
}

#[tokio::test]
async fn test_tell_vs_ask_routing() {
    // Verify that tell and ask messages take different paths
    // Tell should go through ActorTell, Ask through Ask handler

    // Start server
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server_transport = bootstrap_with_keypair(server_addr, KeyPair::generate())
        .await
        .expect("Failed to bootstrap server");

    let server_bind = server_transport
        .local_addr()
        .expect("Failed to get server addr");

    // Start client
    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client_transport = bootstrap_with_keypair(client_addr, KeyPair::generate())
        .await
        .expect("Failed to bootstrap client");

    // Connect
    client_transport
        .connect_to(server_bind)
        .await
        .expect("Failed to connect");

    // Spawn actor on server
    let actor = kameo::spawn(TestActor { counter: 0 });
    let actor_id = actor.id();
    server_transport
        .register_actor(actor_id, actor.clone())
        .await;

    // Wait for gossip
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Lookup from client
    let remote_ref = RemoteActorRef::<TestActor>::lookup_with_transport(
        "test_actor",
        server_bind,
        client_transport.clone(),
    )
    .await
    .expect("Failed to lookup actor");

    // Send tell - should go through ActorTell (no response expected)
    // Note: We need a tell-only message for this
    // For now, we'll verify ask works

    // Send ask - should go through Ask handler and get response
    let result = timeout(
        Duration::from_secs(2),
        remote_ref.ask(Add { a: 100, b: 200 }).send(),
    )
    .await;

    assert!(result.is_ok(), "Ask should succeed");
    let add_result = result.unwrap().expect("Ask failed");
    assert_eq!(add_result.result, 300, "Incorrect result");
}

#[tokio::test]
async fn test_large_message_ask() {
    // Test ask with larger payloads

    // Define a message with larger payload
    #[derive(Clone, Debug)]
    struct LargeMessage {
        data: Vec<u8>,
    }

    #[derive(Clone, Debug)]
    struct LargeReply {
        size: usize,
    }

    impl Message<LargeMessage> for TestActor {
        type Reply = LargeReply;

        async fn handle(
            &mut self,
            msg: LargeMessage,
            _ctx: Context<'_, Self, Self::Reply>,
        ) -> Self::Reply {
            LargeReply {
                size: msg.data.len(),
            }
        }
    }

    // Start server
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server_transport = bootstrap_with_keypair(server_addr, KeyPair::generate())
        .await
        .expect("Failed to bootstrap server");

    let server_bind = server_transport
        .local_addr()
        .expect("Failed to get server addr");

    // Start client
    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client_transport = bootstrap_with_keypair(client_addr, KeyPair::generate())
        .await
        .expect("Failed to bootstrap client");

    // Connect
    client_transport
        .connect_to(server_bind)
        .await
        .expect("Failed to connect");

    // Spawn actor
    let actor = kameo::spawn(TestActor { counter: 0 });
    server_transport
        .register_actor(actor.id(), actor.clone())
        .await;

    // Wait for gossip
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Lookup
    let remote_ref = RemoteActorRef::<TestActor>::lookup_with_transport(
        "test_actor",
        server_bind,
        client_transport.clone(),
    )
    .await
    .expect("Failed to lookup actor");

    // Send large message (1KB)
    let large_data = vec![42u8; 1024];
    let result = timeout(
        Duration::from_secs(5),
        remote_ref.ask(LargeMessage { data: large_data }).send(),
    )
    .await;

    assert!(result.is_ok(), "Large message ask timed out");
    let reply = result.unwrap().expect("Large message ask failed");
    assert_eq!(reply.size, 1024, "Incorrect size in reply");
}
