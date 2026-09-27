//! gRPC transport shared by the Orchid services.
//!
//! A cluster runs either in cleartext or in mTLS mode, the mode of a component
//! is chosen by the presence of TLS files in its configuration:
//! - [`url`]: parsing and validation of URLs against the mode,
//! - [`tls`]: loading of certificates,
//! - [`client`] and [`server`]: channels and servers in either mode,
//! - [`identity`]: identity of a caller from its client certificate,
//! - [`errors`]: machine readable error reasons attached to statuses.

pub mod client;
pub mod errors;
pub mod identity;
pub mod server;
#[cfg(feature = "testing")]
pub mod testing;
pub mod tls;
pub mod url;

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("failed to read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid URL {url:?}: {reason}")]
    InvalidUrl { url: String, reason: String },

    #[error("failed to bind {address}: {source}")]
    Bind {
        address: String,
        #[source]
        source: std::io::Error,
    },

    #[error(transparent)]
    Tonic(#[from] tonic::transport::Error),
}
