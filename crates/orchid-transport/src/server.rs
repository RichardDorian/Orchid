//! Serving gRPC services.

use std::future::Future;
use std::time::Duration;

use tokio::net::{TcpListener, UnixListener};
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tonic::transport::Server;
use tonic::transport::server::Router;
use tracing::info;

use crate::TransportError;
use crate::tls::TlsMaterial;
use crate::url::ListenAddr;

/// A server builder configured for the mode: requires client certificates
/// signed by the cluster CA if `tls` is set.
pub fn builder(tls: Option<&TlsMaterial>) -> Result<Server, TransportError> {
    let mut server = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(Duration::from_secs(20)));
    if let Some(tls) = tls {
        server = server.tls_config(tls.server_config())?;
    }
    Ok(server)
}

/// A bound listener.
pub enum Listener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

impl Listener {
    /// Binds `address`. A stale unix socket file is removed first.
    pub async fn bind(address: &ListenAddr) -> Result<Self, TransportError> {
        let error = |source| TransportError::Bind {
            address: address.to_string(),
            source,
        };
        match address {
            ListenAddr::Tcp(address) => TcpListener::bind(address)
                .await
                .map(Self::Tcp)
                .map_err(error),
            ListenAddr::Unix(path) => {
                match tokio::fs::remove_file(path).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(error(e)),
                }
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await.map_err(error)?;
                }
                UnixListener::bind(path).map(Self::Unix).map_err(error)
            }
        }
    }

    /// Local address of a TCP listener.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match self {
            Self::Tcp(listener) => listener.local_addr().ok(),
            Self::Unix(_) => None,
        }
    }
}

/// Serves `router` on every listener until `shutdown` completes.
pub async fn serve(
    router: Router,
    listeners: Vec<Listener>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), TransportError> {
    let (stop, stopped) = tokio::sync::watch::channel(());
    let mut tasks = tokio::task::JoinSet::new();
    for listener in listeners {
        let router = router.clone();
        let mut stopped = stopped.clone();
        let signal = async move {
            let _ = stopped.changed().await;
        };
        match listener {
            Listener::Tcp(listener) => {
                info!(address = ?listener.local_addr().ok(), "listening");
                tasks.spawn(
                    router.serve_with_incoming_shutdown(TcpListenerStream::new(listener), signal),
                );
            }
            Listener::Unix(listener) => {
                info!(address = ?listener.local_addr().ok(), "listening");
                tasks.spawn(
                    router.serve_with_incoming_shutdown(UnixListenerStream::new(listener), signal),
                );
            }
        }
    }
    tokio::spawn(async move {
        shutdown.await;
        let _ = stop.send(());
    });

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(result) => result?,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(_) => {}
        }
    }
    Ok(())
}
