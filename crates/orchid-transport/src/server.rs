//! Serving gRPC services.

use std::future::Future;
use std::time::Duration;

use tokio::net::{TcpListener, UnixListener};
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tonic::transport::Server;
use tonic::transport::server::Router;
use tracing::{info, warn};

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

/// How long in-flight requests get to finish after the shutdown signal.
/// Watch streams never finish on their own: they are closed after it.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Serves `router` on every listener until `shutdown` completes.
///
/// On shutdown, the listeners are closed right away (new connections are
/// refused, so clients fail over to another instance), in-flight requests get
/// [`SHUTDOWN_GRACE`] to finish, then the remaining connections are closed.
pub async fn serve(
    router: Router,
    listeners: Vec<Listener>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), TransportError> {
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let signal = |mut stopped: tokio::sync::watch::Receiver<bool>| async move {
        let _ = stopped.wait_for(|stopped| *stopped).await;
    };

    let mut tasks = tokio::task::JoinSet::new();
    for listener in listeners {
        let router = router.clone();
        match listener {
            Listener::Tcp(listener) => {
                info!(address = ?listener.local_addr().ok(), "listening");
                let incoming =
                    Closing::new(TcpListenerStream::new(listener), signal(stopped.clone()));
                tasks.spawn(router.serve_with_incoming_shutdown(incoming, signal(stopped.clone())));
            }
            Listener::Unix(listener) => {
                info!(address = ?listener.local_addr().ok(), "listening");
                let incoming =
                    Closing::new(UnixListenerStream::new(listener), signal(stopped.clone()));
                tasks.spawn(router.serve_with_incoming_shutdown(incoming, signal(stopped.clone())));
            }
        }
    }
    tokio::spawn(async move {
        shutdown.await;
        let _ = stop.send(true);
    });

    let deadline = async {
        signal(stopped.clone()).await;
        tokio::time::sleep(SHUTDOWN_GRACE).await;
    };
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            result = tasks.join_next() => match result {
                None => return Ok(()),
                Some(Ok(result)) => result?,
                Some(Err(error)) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Some(Err(_)) => {}
            },
            () = &mut deadline => {
                warn!("closing the remaining connections");
                tasks.abort_all();
                return Ok(());
            }
        }
    }
}

/// A stream of connections whose listener is dropped as soon as `stop`
/// completes, even if the server doesn't poll the stream anymore (tonic keeps
/// its incoming stream alive while waiting for the open connections).
struct Closing<S> {
    listener: std::sync::Arc<std::sync::Mutex<Option<S>>>,
}

impl<S: Send + 'static> Closing<S> {
    fn new(listener: S, stop: impl Future<Output = ()> + Send + 'static) -> Self {
        let listener = std::sync::Arc::new(std::sync::Mutex::new(Some(listener)));
        let closed = listener.clone();
        tokio::spawn(async move {
            stop.await;
            closed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        });
        Self { listener }
    }
}

impl<S: futures_core::Stream + Unpin> futures_core::Stream for Closing<S> {
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut listener = self
            .listener
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match listener.as_mut() {
            Some(listener) => std::pin::Pin::new(listener).poll_next(cx),
            None => std::task::Poll::Ready(None),
        }
    }
}
