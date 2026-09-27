//! TLS certificates.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

use crate::TransportError;

/// The `[tls]` section of a configuration file. Its presence enables mTLS mode.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM certificate of the component.
    pub certificate: PathBuf,
    /// PEM private key of the certificate.
    pub private_key: PathBuf,
    /// PEM certificate of the cluster CA.
    pub ca: PathBuf,
}

impl TlsConfig {
    pub async fn load(&self) -> Result<TlsMaterial, TransportError> {
        Ok(TlsMaterial {
            certificate: read(&self.certificate).await?,
            private_key: read(&self.private_key).await?,
            ca: read(&self.ca).await?,
        })
    }
}

async fn read(path: &Path) -> Result<Vec<u8>, TransportError> {
    tokio::fs::read(path)
        .await
        .map_err(|source| TransportError::Read {
            path: path.to_owned(),
            source,
        })
}

/// Loaded certificates, PEM encoded.
#[derive(Clone)]
pub struct TlsMaterial {
    pub certificate: Vec<u8>,
    pub private_key: Vec<u8>,
    pub ca: Vec<u8>,
}

impl TlsMaterial {
    fn identity(&self) -> Identity {
        Identity::from_pem(&self.certificate, &self.private_key)
    }

    fn ca(&self) -> Certificate {
        Certificate::from_pem(&self.ca)
    }

    /// Client configuration presenting our certificate and trusting the cluster CA.
    /// `domain` overrides the name checked against the server certificate.
    pub fn client_config(&self, domain: Option<&str>) -> ClientTlsConfig {
        let config = ClientTlsConfig::new()
            .ca_certificate(self.ca())
            .identity(self.identity());
        match domain {
            Some(domain) => config.domain_name(domain),
            None => config,
        }
    }

    /// Server configuration requiring client certificates signed by the cluster CA.
    pub fn server_config(&self) -> ServerTlsConfig {
        ServerTlsConfig::new()
            .identity(self.identity())
            .client_ca_root(self.ca())
    }
}
