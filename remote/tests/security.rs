mod common;

use std::time::Duration;

use common::{Counter, GOSSIP_DEADLINE, Inc, eventually};
use kameo::prelude::*;
use kameo_remote::{ClusterKey, RemoteNode, RemoteNodeConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn test_config(seed_nodes: Vec<String>, cluster_key: Option<ClusterKey>) -> RemoteNodeConfig {
    RemoteNodeConfig {
        cluster_key,
        ..common::test_config(seed_nodes)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn psk_cluster_end_to_end() {
    let key = ClusterKey::generate();
    let node_a = RemoteNode::bootstrap(test_config(vec![], Some(key.clone())))
        .await
        .unwrap();
    let node_b = RemoteNode::bootstrap(test_config(
        vec![node_a.gossip_addr().to_string()],
        Some(key),
    ))
    .await
    .unwrap();

    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "counter").await.unwrap();

    let remote = eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<Counter>("counter").await.unwrap()
    })
    .await;
    assert_eq!(remote.ask(&Inc { amount: 2 }).await.unwrap(), 2);
    remote.tell(&Inc { amount: 3 }).await.unwrap();
    assert_eq!(remote.ask(&Inc { amount: 0 }).await.unwrap(), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_key_nodes_never_join() {
    let node_a = RemoteNode::bootstrap(test_config(vec![], Some(ClusterKey::generate())))
        .await
        .unwrap();
    let _node_b = RemoteNode::bootstrap(test_config(
        vec![node_a.gossip_addr().to_string()],
        Some(ClusterKey::generate()),
    ))
    .await
    .unwrap();

    // At a 50ms gossip interval, 3s is dozens of rejected rounds.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(node_a.live_nodes().await.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn keyless_node_never_joins_keyed_cluster() {
    let node_a = RemoteNode::bootstrap(test_config(vec![], Some(ClusterKey::generate())))
        .await
        .unwrap();
    let _node_b = RemoteNode::bootstrap(test_config(vec![node_a.gossip_addr().to_string()], None))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(node_a.live_nodes().await.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn server_survives_garbage_connection() {
    let key = ClusterKey::generate();
    let node_a = RemoteNode::bootstrap(test_config(vec![], Some(key.clone())))
        .await
        .unwrap();
    let node_b = RemoteNode::bootstrap(test_config(
        vec![node_a.gossip_addr().to_string()],
        Some(key),
    ))
    .await
    .unwrap();

    // A garbage framed blob must get the connection closed, not crash the server.
    let mut stream = tokio::net::TcpStream::connect(node_a.messaging_addr())
        .await
        .unwrap();
    let garbage = b"not a handshake frame";
    stream
        .write_all(&(garbage.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(garbage).await.unwrap();
    let mut buf = [0u8; 64];
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("server should close the connection")
        .unwrap();
    assert_eq!(read, 0, "server should send nothing and close");

    // The cluster still works afterwards.
    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "counter").await.unwrap();
    let remote = eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<Counter>("counter").await.unwrap()
    })
    .await;
    assert_eq!(remote.ask(&Inc { amount: 1 }).await.unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn stalled_handshake_times_out() {
    let config = RemoteNodeConfig {
        handshake_timeout: Duration::from_millis(200),
        ..test_config(vec![], None)
    };
    let node = RemoteNode::bootstrap(config).await.unwrap();

    let mut stream = tokio::net::TcpStream::connect(node.messaging_addr())
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("server should close a stalled connection")
        .unwrap();
    assert_eq!(read, 0);
}

#[cfg(feature = "tls")]
mod tls {
    use kameo_remote::{RemoteSendError, TlsConfig};

    use super::*;

    struct TestCa {
        key: rcgen::KeyPair,
        cert: rcgen::Certificate,
    }

    impl TestCa {
        fn generate() -> Self {
            let key = rcgen::KeyPair::generate().unwrap();
            let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let cert = params.self_signed(&key).unwrap();
            TestCa { key, cert }
        }

        /// Issues a node certificate and builds a [`TlsConfig`] trusting this CA.
        fn node_tls_config(&self) -> TlsConfig {
            let key = rcgen::KeyPair::generate().unwrap();
            let params = rcgen::CertificateParams::new(vec!["node".to_string()]).unwrap();
            let cert = params.signed_by(&key, &self.cert, &self.key).unwrap();
            TlsConfig::from_pem(
                self.cert.pem().as_bytes(),
                cert.pem().as_bytes(),
                key.serialize_pem().as_bytes(),
            )
            .unwrap()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tls_psk_cluster_end_to_end() {
        let ca = TestCa::generate();
        let key = ClusterKey::generate();

        let config_a = test_config(vec![], Some(key.clone())).with_tls(ca.node_tls_config());
        let node_a = RemoteNode::bootstrap(config_a).await.unwrap();
        let config_b = test_config(vec![node_a.gossip_addr().to_string()], Some(key))
            .with_tls(ca.node_tls_config());
        let node_b = RemoteNode::bootstrap(config_b).await.unwrap();

        let counter = Counter::spawn(Counter { count: 0 });
        node_a.register(&counter, "counter").await.unwrap();

        let remote = eventually(GOSSIP_DEADLINE, || async {
            node_b.lookup::<Counter>("counter").await.unwrap()
        })
        .await;
        assert_eq!(remote.ask(&Inc { amount: 2 }).await.unwrap(), 2);
        remote.tell(&Inc { amount: 3 }).await.unwrap();
        assert_eq!(remote.ask(&Inc { amount: 0 }).await.unwrap(), 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tls_rejects_plaintext_peer() {
        let ca = TestCa::generate();

        // Both keyless so gossip converges; only node A requires TLS on messaging.
        let config_a = test_config(vec![], None).with_tls(ca.node_tls_config());
        let node_a = RemoteNode::bootstrap(config_a).await.unwrap();
        let node_b =
            RemoteNode::bootstrap(test_config(vec![node_a.gossip_addr().to_string()], None))
                .await
                .unwrap();

        let counter = Counter::spawn(Counter { count: 0 });
        node_a.register(&counter, "counter").await.unwrap();

        let remote = eventually(GOSSIP_DEADLINE, || async {
            node_b.lookup::<Counter>("counter").await.unwrap()
        })
        .await;
        let err = remote.ask(&Inc { amount: 1 }).await.unwrap_err();
        assert!(
            matches!(
                err,
                RemoteSendError::Connect(_)
                    | RemoteSendError::ConnectionClosed
                    | RemoteSendError::Handshake(_)
            ),
            "expected a connection-level failure, got: {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tls_rejects_untrusted_ca() {
        let ca_a = TestCa::generate();
        let ca_b = TestCa::generate();

        let config_a = test_config(vec![], None).with_tls(ca_a.node_tls_config());
        let node_a = RemoteNode::bootstrap(config_a).await.unwrap();
        // Node B trusts and presents certs from a different CA.
        let config_b = test_config(vec![node_a.gossip_addr().to_string()], None)
            .with_tls(ca_b.node_tls_config());
        let node_b = RemoteNode::bootstrap(config_b).await.unwrap();

        let counter = Counter::spawn(Counter { count: 0 });
        node_a.register(&counter, "counter").await.unwrap();

        let remote = eventually(GOSSIP_DEADLINE, || async {
            node_b.lookup::<Counter>("counter").await.unwrap()
        })
        .await;
        let err = remote.ask(&Inc { amount: 1 }).await.unwrap_err();
        assert!(
            matches!(
                err,
                RemoteSendError::Connect(_)
                    | RemoteSendError::ConnectionClosed
                    | RemoteSendError::Handshake(_)
            ),
            "expected a connection-level failure, got: {err:?}"
        );
    }
}
