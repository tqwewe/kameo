//! The hello exchange every messaging connection performs before carrying requests.
//!
//! The handshake pins the protocol version, verifies both peers belong to the same
//! cluster, and, when a cluster key is configured, mutually proves possession of it
//! via an HMAC challenge-response — the key itself never crosses the wire.
//!
//! ```text
//! client                                server
//!   | ClientHello { version, cluster_id,  |
//!   |               client_nonce }        |
//!   |------------------------------------>|
//!   | ServerHello { version,              |  or Reject + close
//!   |               server_nonce, mac? }  |
//!   |<------------------------------------|
//!   | ClientAuth { mac? }                 |
//!   |------------------------------------>|
//!   | Accept                              |  or Reject + close
//!   |<------------------------------------|
//! ```

use chacha20poly1305::aead::{OsRng, rand_core::RngCore};
use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::security::{ConnSecurity, HandshakeKey};

pub(crate) const PROTOCOL_VERSION: u16 = 1;

const SERVER_MAC_LABEL: &[u8] = b"kameo-remote v1 server";
const CLIENT_MAC_LABEL: &[u8] = b"kameo-remote v1 client";

#[derive(Debug, Serialize, Deserialize)]
enum HandshakeFrame {
    ClientHello(ClientHello),
    ServerHello(ServerHello),
    ClientAuth(ClientAuth),
    Accept,
    Reject(RejectReason),
}

#[derive(Debug, Serialize, Deserialize)]
struct ClientHello {
    protocol_version: u16,
    cluster_id: String,
    nonce: ByteBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct ServerHello {
    protocol_version: u16,
    nonce: ByteBuf,
    /// Present iff the server has a cluster key.
    mac: Option<ByteBuf>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClientAuth {
    /// Present iff the client has a cluster key.
    mac: Option<ByteBuf>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum RejectReason {
    ProtocolVersion { supported: u16 },
    ClusterMismatch,
    AuthRequired,
    AuthFailed,
}

/// An error which can occur during the connection handshake.
#[derive(Debug, thiserror::Error)]
pub(crate) enum HandshakeError {
    #[error("cluster id mismatch")]
    ClusterMismatch,
    #[error("peer requires a cluster key")]
    AuthRequired,
    #[error("cluster key authentication failed")]
    AuthFailed,
    #[error("unsupported protocol version {0}")]
    ProtocolVersion(u16),
    #[error("handshake protocol error: {0}")]
    Protocol(String),
}

/// The MAC proving key possession, bound to this connection's nonces and cluster.
/// Direction labels prevent reflecting a peer's MAC back at it; the length-prefixed
/// cluster id prevents boundary ambiguity with the nonces.
fn compute_mac(
    key: &HandshakeKey,
    label: &[u8],
    cluster_id: &str,
    first_nonce: &[u8],
    second_nonce: &[u8],
) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.0).expect("hmac accepts any key length");
    mac.update(label);
    mac.update(&(cluster_id.len() as u32).to_le_bytes());
    mac.update(cluster_id.as_bytes());
    mac.update(first_nonce);
    mac.update(second_nonce);
    mac.finalize().into_bytes().to_vec()
}

/// Constant-time MAC verification.
fn verify_mac(
    key: &HandshakeKey,
    label: &[u8],
    cluster_id: &str,
    first_nonce: &[u8],
    second_nonce: &[u8],
    provided: &[u8],
) -> bool {
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.0).expect("hmac accepts any key length");
    mac.update(label);
    mac.update(&(cluster_id.len() as u32).to_le_bytes());
    mac.update(cluster_id.as_bytes());
    mac.update(first_nonce);
    mac.update(second_nonce);
    mac.verify_slice(provided).is_ok()
}

fn nonce() -> ByteBuf {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    ByteBuf::from(bytes.to_vec())
}

async fn send_frame<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    frame: &HandshakeFrame,
) -> Result<(), HandshakeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let bytes = rmp_serde::to_vec_named(frame)
        .map_err(|err| HandshakeError::Protocol(format!("encode: {err}")))?;
    framed
        .send(bytes.into())
        .await
        .map_err(|err| HandshakeError::Protocol(format!("send: {err}")))
}

async fn recv_frame<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
) -> Result<HandshakeFrame, HandshakeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let bytes = framed
        .next()
        .await
        .ok_or_else(|| HandshakeError::Protocol("connection closed".to_string()))?
        .map_err(|err| HandshakeError::Protocol(format!("recv: {err}")))?;
    rmp_serde::from_slice(&bytes).map_err(|err| HandshakeError::Protocol(format!("decode: {err}")))
}

impl From<RejectReason> for HandshakeError {
    fn from(reason: RejectReason) -> Self {
        match reason {
            RejectReason::ProtocolVersion { supported } => {
                HandshakeError::ProtocolVersion(supported)
            }
            RejectReason::ClusterMismatch => HandshakeError::ClusterMismatch,
            RejectReason::AuthRequired => HandshakeError::AuthRequired,
            RejectReason::AuthFailed => HandshakeError::AuthFailed,
        }
    }
}

/// Runs the dialer's side of the handshake.
pub(crate) async fn client<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    security: &ConnSecurity,
) -> Result<(), HandshakeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let client_nonce = nonce();
    send_frame(
        framed,
        &HandshakeFrame::ClientHello(ClientHello {
            protocol_version: PROTOCOL_VERSION,
            cluster_id: security.cluster_id.clone(),
            nonce: client_nonce.clone(),
        }),
    )
    .await?;

    let server_hello = match recv_frame(framed).await? {
        HandshakeFrame::ServerHello(hello) => hello,
        HandshakeFrame::Reject(reason) => return Err(reason.into()),
        frame => {
            return Err(HandshakeError::Protocol(format!(
                "unexpected frame: {frame:?}"
            )));
        }
    };
    if server_hello.protocol_version != PROTOCOL_VERSION {
        return Err(HandshakeError::ProtocolVersion(
            server_hello.protocol_version,
        ));
    }

    // A keyed client only talks to servers that prove key possession; a missing or
    // bad MAC fails here so a keyless peer cannot downgrade the connection.
    let client_mac = match &security.handshake_key {
        Some(key) => {
            let valid = server_hello.mac.as_ref().is_some_and(|mac| {
                verify_mac(
                    key,
                    SERVER_MAC_LABEL,
                    &security.cluster_id,
                    &client_nonce,
                    &server_hello.nonce,
                    mac,
                )
            });
            if !valid {
                return Err(HandshakeError::AuthFailed);
            }
            Some(ByteBuf::from(compute_mac(
                key,
                CLIENT_MAC_LABEL,
                &security.cluster_id,
                &server_hello.nonce,
                &client_nonce,
            )))
        }
        None => None,
    };

    send_frame(
        framed,
        &HandshakeFrame::ClientAuth(ClientAuth { mac: client_mac }),
    )
    .await?;

    match recv_frame(framed).await? {
        HandshakeFrame::Accept => Ok(()),
        HandshakeFrame::Reject(reason) => Err(reason.into()),
        frame => Err(HandshakeError::Protocol(format!(
            "unexpected frame: {frame:?}"
        ))),
    }
}

/// Runs the accepting side of the handshake. Rejections are sent to the peer
/// best-effort before returning the error.
pub(crate) async fn server<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    security: &ConnSecurity,
) -> Result<(), HandshakeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let client_hello = match recv_frame(framed).await? {
        HandshakeFrame::ClientHello(hello) => hello,
        frame => {
            return Err(HandshakeError::Protocol(format!(
                "unexpected frame: {frame:?}"
            )));
        }
    };
    if client_hello.protocol_version != PROTOCOL_VERSION {
        let reason = RejectReason::ProtocolVersion {
            supported: PROTOCOL_VERSION,
        };
        let _ = send_frame(framed, &HandshakeFrame::Reject(reason)).await;
        return Err(HandshakeError::ProtocolVersion(
            client_hello.protocol_version,
        ));
    }
    if client_hello.cluster_id != security.cluster_id {
        let _ = send_frame(
            framed,
            &HandshakeFrame::Reject(RejectReason::ClusterMismatch),
        )
        .await;
        return Err(HandshakeError::ClusterMismatch);
    }

    // Answering with a MAC before the client authenticates reveals nothing useful:
    // it is an HMAC over random nonces, worthless without the key.
    let server_nonce = nonce();
    let server_mac = security.handshake_key.as_ref().map(|key| {
        ByteBuf::from(compute_mac(
            key,
            SERVER_MAC_LABEL,
            &security.cluster_id,
            &client_hello.nonce,
            &server_nonce,
        ))
    });
    send_frame(
        framed,
        &HandshakeFrame::ServerHello(ServerHello {
            protocol_version: PROTOCOL_VERSION,
            nonce: server_nonce.clone(),
            mac: server_mac,
        }),
    )
    .await?;

    let client_auth = match recv_frame(framed).await? {
        HandshakeFrame::ClientAuth(auth) => auth,
        HandshakeFrame::Reject(reason) => return Err(reason.into()),
        frame => {
            return Err(HandshakeError::Protocol(format!(
                "unexpected frame: {frame:?}"
            )));
        }
    };

    if let Some(key) = &security.handshake_key {
        let Some(mac) = &client_auth.mac else {
            let _ = send_frame(framed, &HandshakeFrame::Reject(RejectReason::AuthRequired)).await;
            return Err(HandshakeError::AuthRequired);
        };
        if !verify_mac(
            key,
            CLIENT_MAC_LABEL,
            &security.cluster_id,
            &server_nonce,
            &client_hello.nonce,
            mac,
        ) {
            let _ = send_frame(framed, &HandshakeFrame::Reject(RejectReason::AuthFailed)).await;
            return Err(HandshakeError::AuthFailed);
        }
    }

    send_frame(framed, &HandshakeFrame::Accept).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::security::{ClusterKey, derive_keys};

    fn security(cluster_id: &str, key: Option<&ClusterKey>) -> ConnSecurity {
        ConnSecurity {
            cluster_id: cluster_id.to_string(),
            handshake_key: key.map(|key| derive_keys(key).1),
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Runs both sides as separate tasks so a side that aborts mid-handshake drops
    /// its stream, and the other side observes EOF instead of waiting forever.
    async fn run_handshake(
        client_security: ConnSecurity,
        server_security: ConnSecurity,
    ) -> (Result<(), HandshakeError>, Result<(), HandshakeError>) {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let client_task = tokio::spawn(async move {
            let mut framed = LengthDelimitedCodec::builder().new_framed(client_io);
            client(&mut framed, &client_security).await
        });
        let server_task = tokio::spawn(async move {
            let mut framed = LengthDelimitedCodec::builder().new_framed(server_io);
            server(&mut framed, &server_security).await
        });
        (client_task.await.unwrap(), server_task.await.unwrap())
    }

    #[tokio::test]
    async fn keyless_peers_connect() {
        let (client_result, server_result) =
            run_handshake(security("kameo", None), security("kameo", None)).await;
        client_result.unwrap();
        server_result.unwrap();
    }

    #[tokio::test]
    async fn matching_keys_connect() {
        let key = ClusterKey::new([1u8; 32]);
        let (client_result, server_result) =
            run_handshake(security("kameo", Some(&key)), security("kameo", Some(&key))).await;
        client_result.unwrap();
        server_result.unwrap();
    }

    #[tokio::test]
    async fn different_keys_fail() {
        let key_a = ClusterKey::new([1u8; 32]);
        let key_b = ClusterKey::new([2u8; 32]);
        let (client_result, _server_result) = run_handshake(
            security("kameo", Some(&key_a)),
            security("kameo", Some(&key_b)),
        )
        .await;
        assert!(matches!(client_result, Err(HandshakeError::AuthFailed)));
    }

    #[tokio::test]
    async fn keyed_client_rejects_keyless_server() {
        let key = ClusterKey::new([1u8; 32]);
        let (client_result, _server_result) =
            run_handshake(security("kameo", Some(&key)), security("kameo", None)).await;
        assert!(matches!(client_result, Err(HandshakeError::AuthFailed)));
    }

    #[tokio::test]
    async fn keyed_server_rejects_keyless_client() {
        let key = ClusterKey::new([1u8; 32]);
        let (client_result, server_result) =
            run_handshake(security("kameo", None), security("kameo", Some(&key))).await;
        assert!(matches!(server_result, Err(HandshakeError::AuthRequired)));
        assert!(matches!(client_result, Err(HandshakeError::AuthRequired)));
    }

    #[tokio::test]
    async fn cluster_mismatch_fails_both_sides() {
        let (client_result, server_result) =
            run_handshake(security("kameo-a", None), security("kameo-b", None)).await;
        assert!(matches!(
            client_result,
            Err(HandshakeError::ClusterMismatch)
        ));
        assert!(matches!(
            server_result,
            Err(HandshakeError::ClusterMismatch)
        ));
    }

    #[tokio::test]
    async fn unsupported_protocol_version_rejected() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let mut client_framed = LengthDelimitedCodec::builder().new_framed(client_io);
        let mut server_framed = LengthDelimitedCodec::builder().new_framed(server_io);
        let server_security = security("kameo", None);

        let client = async {
            send_frame(
                &mut client_framed,
                &HandshakeFrame::ClientHello(ClientHello {
                    protocol_version: 2,
                    cluster_id: "kameo".to_string(),
                    nonce: nonce(),
                }),
            )
            .await
            .unwrap();
            recv_frame(&mut client_framed).await.unwrap()
        };
        let (reply, server_result) =
            tokio::join!(client, server(&mut server_framed, &server_security));
        assert!(matches!(
            reply,
            HandshakeFrame::Reject(RejectReason::ProtocolVersion { supported: 1 })
        ));
        assert!(matches!(
            server_result,
            Err(HandshakeError::ProtocolVersion(2))
        ));
    }

    #[tokio::test]
    async fn garbage_first_frame_fails() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let mut client_framed = LengthDelimitedCodec::builder().new_framed(client_io);
        let mut server_framed = LengthDelimitedCodec::builder().new_framed(server_io);
        let server_security = security("kameo", None);

        let client = async {
            client_framed
                .send(Bytes::from_static(b"not a handshake frame"))
                .await
                .unwrap();
        };
        let (_, server_result) = tokio::join!(client, server(&mut server_framed, &server_security));
        assert!(matches!(server_result, Err(HandshakeError::Protocol(_))));
    }

    #[tokio::test]
    async fn tampered_client_mac_rejected() {
        let key = ClusterKey::new([1u8; 32]);
        let client_security = security("kameo", Some(&key));
        let server_security = security("kameo", Some(&key));

        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let mut client_framed = LengthDelimitedCodec::builder().new_framed(client_io);
        let mut server_framed = LengthDelimitedCodec::builder().new_framed(server_io);

        // A hand-driven client that authenticates with a corrupted MAC.
        let client = async {
            let client_nonce = nonce();
            send_frame(
                &mut client_framed,
                &HandshakeFrame::ClientHello(ClientHello {
                    protocol_version: PROTOCOL_VERSION,
                    cluster_id: client_security.cluster_id.clone(),
                    nonce: client_nonce.clone(),
                }),
            )
            .await
            .unwrap();
            let HandshakeFrame::ServerHello(server_hello) =
                recv_frame(&mut client_framed).await.unwrap()
            else {
                panic!("expected server hello");
            };
            let mut mac = compute_mac(
                client_security.handshake_key.as_ref().unwrap(),
                CLIENT_MAC_LABEL,
                &client_security.cluster_id,
                &server_hello.nonce,
                &client_nonce,
            );
            mac[0] ^= 1;
            send_frame(
                &mut client_framed,
                &HandshakeFrame::ClientAuth(ClientAuth {
                    mac: Some(ByteBuf::from(mac)),
                }),
            )
            .await
            .unwrap();
            recv_frame(&mut client_framed).await.unwrap()
        };
        let (reply, server_result) =
            tokio::join!(client, server(&mut server_framed, &server_security));
        assert!(matches!(
            reply,
            HandshakeFrame::Reject(RejectReason::AuthFailed)
        ));
        assert!(matches!(server_result, Err(HandshakeError::AuthFailed)));
    }
}
