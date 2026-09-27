//! Channels to a server.

use std::time::Duration;

use hyper_util::rt::TokioIo;
use tonic::transport::{Channel, Endpoint, Uri};

use crate::TransportError;
use crate::tls::TlsMaterial;
use crate::url::ServerUrl;

/// Name checked against the server certificate when connecting over a unix
/// socket in mTLS mode: the server certificate needs a `localhost` SAN.
pub const UNIX_TLS_DOMAIN: &str = "localhost";

/// Creates a channel to a server reachable at `urls`, using mTLS if `tls` is set.
///
/// The channel connects lazily and reconnects on failures. With several URLs,
/// requests are balanced over the reachable ones.
pub fn connect(urls: &[ServerUrl], tls: Option<&TlsMaterial>) -> Result<Channel, TransportError> {
    if let [ServerUrl::Unix(path)] = urls {
        // The URI is only used by TLS (scheme and server name), the connector
        // always dials the socket.
        let uri = if tls.is_some() {
            "https://localhost"
        } else {
            "http://localhost"
        };
        let mut endpoint = configure(Endpoint::from_static(uri));
        if let Some(tls) = tls {
            endpoint = endpoint.tls_config(tls.client_config(Some(UNIX_TLS_DOMAIN)))?;
        }
        let path = path.clone();
        return Ok(
            endpoint.connect_with_connector_lazy(tower::service_fn(move |_: Uri| {
                let path = path.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            })),
        );
    }

    let endpoints = urls
        .iter()
        .map(|url| {
            let ServerUrl::Tcp(url) = url else {
                return Err(TransportError::InvalidUrl {
                    url: format!("{url:?}"),
                    reason: "a unix socket must be the only URL".to_owned(),
                });
            };
            let mut endpoint = configure(Endpoint::from_shared(url.clone())?);
            if let Some(tls) = tls {
                endpoint = endpoint.tls_config(tls.client_config(None))?;
            }
            Ok(endpoint)
        })
        .collect::<Result<Vec<_>, _>>()?;

    match endpoints.as_slice() {
        [] => Err(TransportError::InvalidUrl {
            url: String::new(),
            reason: "at least one URL is required".to_owned(),
        }),
        [endpoint] => Ok(endpoint.connect_lazy()),
        _ => Ok(Channel::balance_list(endpoints.into_iter())),
    }
}

fn configure(endpoint: Endpoint) -> Endpoint {
    endpoint
        .connect_timeout(Duration::from_secs(5))
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true)
}
