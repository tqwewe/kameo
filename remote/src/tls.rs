//! Mutual TLS for the messaging layer.
//!
//! Both sides of every connection authenticate against a cluster CA: the server
//! requires a CA-signed client certificate, and the client verifies the server's
//! chain against the same CA. Hostname verification is intentionally skipped, since
//! nodes are dialed by gossip-advertised IPs; possession of a CA-signed certificate
//! is what makes a peer a cluster member. Use [`TlsConfig::from_rustls`] to bring a
//! standard verifier if certificates carry IP SANs and stricter checking is wanted.

use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use rustls::{
    ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme,
    client::{
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        verify_server_cert_signed_by_trust_anchor,
    },
    crypto::{WebPkiSupportedAlgorithms, ring, verify_tls13_signature},
    server::{ParsedCertificate, VerifierBuilderError, WebPkiClientVerifier},
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// An error which can occur when building a [`TlsConfig`].
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// Failed to read a certificate or key file.
    #[error("failed to read {path}: {source}")]
    Io {
        /// The file that could not be read.
        path: PathBuf,
        /// The underlying io error.
        source: io::Error,
    },
    /// Failed to parse PEM data.
    #[error("invalid pem: {0}")]
    Pem(#[from] rustls_pki_types::pem::Error),
    /// The PEM data contained no certificates.
    #[error("no certificates found in pem")]
    NoCertificates,
    /// The TLS configuration is invalid (bad certificate, key mismatch, ...).
    #[error("tls: {0}")]
    Rustls(#[from] rustls::Error),
    /// The client certificate verifier could not be built.
    #[error("verifier: {0}")]
    Verifier(#[from] VerifierBuilderError),
}

/// Mutual TLS configuration for the messaging layer.
///
/// Built from a cluster CA plus this node's certificate and key: connections are
/// encrypted with TLS 1.3 and both peers must present a certificate signed by the
/// CA. See the module docs for the verification model.
#[derive(Clone)]
pub struct TlsConfig {
    acceptor: TlsAcceptor,
    connector: TlsConnector,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TlsConfig { .. }")
    }
}

impl TlsConfig {
    /// Builds a config from PEM data: the cluster CA certificate(s), this node's
    /// certificate chain, and its private key.
    pub fn from_pem(ca_pem: &[u8], cert_pem: &[u8], key_pem: &[u8]) -> Result<Self, TlsError> {
        let ca_certs = CertificateDer::pem_slice_iter(ca_pem).collect::<Result<Vec<_>, _>>()?;
        if ca_certs.is_empty() {
            return Err(TlsError::NoCertificates);
        }
        let cert_chain = CertificateDer::pem_slice_iter(cert_pem).collect::<Result<Vec<_>, _>>()?;
        if cert_chain.is_empty() {
            return Err(TlsError::NoCertificates);
        }
        let key = PrivateKeyDer::from_pem_slice(key_pem)?;

        let mut roots = RootCertStore::empty();
        for cert in ca_certs {
            roots.add(cert)?;
        }
        let roots = Arc::new(roots);
        // An explicit provider, so nothing here depends on (or conflicts with) the
        // process-level default the host application may install.
        let provider = Arc::new(ring::default_provider());

        let client_verifier =
            WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone()).build()?;
        let server_config = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(cert_chain.clone(), key.clone_key())?;

        let server_verifier = Arc::new(NoHostnameVerifier {
            roots,
            supported_algs: provider.signature_verification_algorithms,
        });
        let client_config = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .dangerous()
            .with_custom_certificate_verifier(server_verifier)
            .with_client_auth_cert(cert_chain, key)?;

        Ok(TlsConfig::from_rustls(
            Arc::new(server_config),
            Arc::new(client_config),
        ))
    }

    /// Builds a config from PEM files; see [`from_pem`](TlsConfig::from_pem).
    pub fn from_pem_files(
        ca: impl AsRef<Path>,
        cert: impl AsRef<Path>,
        key: impl AsRef<Path>,
    ) -> Result<Self, TlsError> {
        let read = |path: &Path| {
            std::fs::read(path).map_err(|source| TlsError::Io {
                path: path.to_path_buf(),
                source,
            })
        };
        let ca_pem = read(ca.as_ref())?;
        let cert_pem = read(cert.as_ref())?;
        let key_pem = read(key.as_ref())?;
        TlsConfig::from_pem(&ca_pem, &cert_pem, &key_pem)
    }

    /// Builds a config from raw rustls configs, for full control over verification,
    /// revocation, providers, or certificate resolution.
    ///
    /// The server config is used for inbound connections and the client config for
    /// outbound ones. For a closed cluster the server should require client
    /// certificates and the client should verify the server, otherwise inbound
    /// connections are effectively unauthenticated.
    pub fn from_rustls(server: Arc<ServerConfig>, client: Arc<ClientConfig>) -> Self {
        TlsConfig {
            acceptor: TlsAcceptor::from(server),
            connector: TlsConnector::from(client),
        }
    }

    pub(crate) fn acceptor(&self) -> &TlsAcceptor {
        &self.acceptor
    }

    pub(crate) fn connector(&self) -> &TlsConnector {
        &self.connector
    }
}

/// Verifies the server's chain and validity against the cluster CA, skipping
/// hostname verification: nodes are dialed by gossip-advertised IPs, and a CA-signed
/// certificate is what proves cluster membership.
#[derive(Debug)]
struct NoHostnameVerifier {
    roots: Arc<RootCertStore>,
    supported_algs: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let cert = ParsedCertificate::try_from(end_entity)?;
        verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            self.supported_algs.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        // Unreachable: the configs are TLS 1.3 only.
        Err(rustls::Error::General("tls 1.2 is not supported".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.supported_algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported_algs.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster_pems() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec!["node".to_string()]).unwrap();
        let cert = params.signed_by(&key, &ca_cert, &ca_key).unwrap();

        (
            ca_cert.pem().into_bytes(),
            cert.pem().into_bytes(),
            key.serialize_pem().into_bytes(),
        )
    }

    #[test]
    fn from_pem_accepts_ca_signed_cert() {
        let (ca, cert, key) = cluster_pems();
        TlsConfig::from_pem(&ca, &cert, &key).unwrap();
    }

    #[test]
    fn from_pem_rejects_garbage() {
        let err = TlsConfig::from_pem(b"garbage", b"garbage", b"garbage").unwrap_err();
        assert!(matches!(err, TlsError::Pem(_) | TlsError::NoCertificates));
    }

    #[test]
    fn from_pem_rejects_missing_key() {
        let (ca, cert, _) = cluster_pems();
        let err = TlsConfig::from_pem(&ca, &cert, b"").unwrap_err();
        assert!(matches!(err, TlsError::Pem(_)));
    }

    #[test]
    fn from_pem_files_reports_missing_file() {
        let err = TlsConfig::from_pem_files(
            "/nonexistent/ca.pem",
            "/nonexistent/cert.pem",
            "/nonexistent/key.pem",
        )
        .unwrap_err();
        assert!(matches!(err, TlsError::Io { .. }));
    }
}
