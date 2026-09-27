//! Client side helpers of the Orchid API.
//!
//! - [`connect`]: a channel to Labellum from configuration values,
//! - [`informer`]: keeps an up to date view of a collection (list, then watch,
//!   resuming or listing again on failures).

pub mod informer;

use orchid_transport::TransportError;
use orchid_transport::tls::TlsMaterial;
use tonic::transport::Channel;

/// A channel to the Labellum instances reachable at `urls`, in mTLS mode if
/// `tls` is set.
pub fn connect(urls: &[String], tls: Option<&TlsMaterial>) -> Result<Channel, TransportError> {
    let urls = orchid_transport::url::parse_server_urls(urls, tls.is_some())?;
    orchid_transport::client::connect(&urls, tls)
}
