//! Connections to a server.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper_util::rt::TokioIo;
use tokio::sync::{mpsc, watch};
use tonic::body::Body;
use tonic::codegen::http;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::Service;
use tracing::{debug, warn};

use crate::TransportError;
use crate::tls::TlsMaterial;
use crate::url::ServerUrl;

/// Name checked against the server certificate when connecting over a unix
/// socket in mTLS mode: the server certificate needs a `localhost` SAN.
pub const UNIX_TLS_DOMAIN: &str = "localhost";

/// Creates a connection to a server reachable at `urls`, using mTLS if `tls` is set.
///
/// With a single URL, the connection is established lazily and re-established
/// on failures. With several URLs, see [`Failover`]. Must be called from
/// within a Tokio runtime.
pub fn connect(
    urls: &[ServerUrl],
    tls: Option<&TlsMaterial>,
) -> Result<Connection, TransportError> {
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
        let channel = endpoint.connect_with_connector_lazy(tower::service_fn(move |_: Uri| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }));
        return Ok(Connection::Single(channel));
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
        [endpoint] => Ok(Connection::Single(endpoint.connect_lazy())),
        _ => Ok(Connection::Failover(Failover::new(endpoints))),
    }
}

fn configure(endpoint: Endpoint) -> Endpoint {
    endpoint
        .connect_timeout(Duration::from_secs(5))
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true)
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type ResponseFuture = Pin<Box<dyn Future<Output = Result<http::Response<Body>, BoxError>> + Send>>;

/// A connection usable by every generated gRPC client.
#[derive(Clone)]
pub enum Connection {
    Single(Channel),
    Failover(Failover),
}

impl Service<http::Request<Body>> for Connection {
    type Response = http::Response<Body>;
    type Error = BoxError;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self {
            Self::Single(channel) => channel.poll_ready(cx).map_err(Into::into),
            Self::Failover(failover) => failover.poll_ready(cx),
        }
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        match self {
            Self::Single(channel) => {
                let response = channel.call(request);
                Box::pin(async move { response.await.map_err(Into::into) })
            }
            Self::Failover(failover) => failover.call(request),
        }
    }
}

#[derive(Clone)]
enum FailoverState {
    /// No connection attempt finished yet.
    Connecting,
    /// Connected to one of the servers. The generation identifies the connection.
    Connected(Channel, u64),
    /// No server could be reached, calls fail until one can.
    Unavailable(String),
}

/// A connection to one of several servers.
///
/// A background task keeps a connection to one server, trying the servers in
/// turn. Requests wait for a connection instead of being sent to a server that
/// is down; when a request fails because of the transport (the server died),
/// the task moves to the next server. The request that saw the failure fails,
/// the next ones use the next server. When no server can be reached, requests
/// fail right away while the task keeps trying.
pub struct Failover {
    state: watch::Receiver<FailoverState>,
    failures: mpsc::UnboundedSender<u64>,
    /// Latest connection generation known to be dead, shared by every handle
    /// so that none of them sends requests to it while the task fails over.
    failed: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// The connection this handle sends requests to.
    channel: Option<(Channel, u64)>,
    /// Behind a mutex (only used through `get_mut`) to keep `Failover` `Sync`.
    waiting: std::sync::Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
}

impl Clone for Failover {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            failures: self.failures.clone(),
            failed: self.failed.clone(),
            channel: None,
            waiting: std::sync::Mutex::new(None),
        }
    }
}

impl Failover {
    fn new(endpoints: Vec<Endpoint>) -> Self {
        let (state, receiver) = watch::channel(FailoverState::Connecting);
        let (failures, failure_reports) = mpsc::unbounded_channel();
        tokio::spawn(manage(endpoints, state, failure_reports));
        Self {
            state: receiver,
            failures,
            failed: std::sync::Arc::default(),
            channel: None,
            waiting: std::sync::Mutex::new(None),
        }
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        loop {
            let failed = self.failed.load(std::sync::atomic::Ordering::Acquire);
            // The task moved to another server, or the connection is dead.
            if self.state.has_changed().unwrap_or(true)
                || self
                    .channel
                    .as_ref()
                    .is_some_and(|(_, generation)| *generation <= failed)
            {
                self.channel = None;
            }
            if let Some((channel, generation)) = &mut self.channel {
                return match channel.poll_ready(cx) {
                    Poll::Ready(Err(error)) => {
                        report_failure(&self.failed, &self.failures, *generation);
                        self.channel = None;
                        Poll::Ready(Err(error.into()))
                    }
                    other => other.map_err(Into::into),
                };
            }

            let state = self.state.borrow_and_update().clone();
            match state {
                FailoverState::Connected(channel, generation) if generation > failed => {
                    self.channel = Some((channel, generation));
                }
                FailoverState::Unavailable(message) => return Poll::Ready(Err(message.into())),
                // Connecting, or still connected to a dead server: wait for the task.
                _ => {
                    let state = &self.state;
                    let waiting = self
                        .waiting
                        .get_mut()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let future = waiting.get_or_insert_with(|| {
                        let mut state = state.clone();
                        Box::pin(async move {
                            let _ = state.changed().await;
                        })
                    });
                    match future.as_mut().poll(cx) {
                        Poll::Ready(()) => *waiting = None,
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }

    fn call(&mut self, request: http::Request<Body>) -> ResponseFuture {
        let (channel, generation) = self
            .channel
            .as_mut()
            .expect("poll_ready must succeed before call");
        let generation = *generation;
        let response = channel.call(request);
        let failures = self.failures.clone();
        let failed = self.failed.clone();
        Box::pin(async move {
            response.await.map_err(|error| {
                // Errors at this level come from the transport, not from gRPC.
                report_failure(&failed, &failures, generation);
                error.into()
            })
        })
    }
}

fn report_failure(
    failed: &std::sync::atomic::AtomicU64,
    failures: &mpsc::UnboundedSender<u64>,
    generation: u64,
) {
    failed.fetch_max(generation, std::sync::atomic::Ordering::AcqRel);
    let _ = failures.send(generation);
}

/// Keeps a connection to one of the endpoints, until every handle is dropped.
async fn manage(
    endpoints: Vec<Endpoint>,
    state: watch::Sender<FailoverState>,
    mut failures: mpsc::UnboundedReceiver<u64>,
) {
    let mut next = 0;
    let mut generation = 0;
    let mut backoff = Duration::from_millis(200);
    loop {
        let mut connected = None;
        for _ in 0..endpoints.len() {
            let endpoint = &endpoints[next];
            next = (next + 1) % endpoints.len();
            tokio::select! {
                () = state.closed() => return,
                result = endpoint.connect() => match result {
                    Ok(channel) => {
                        connected = Some((channel, endpoint.uri().clone()));
                        break;
                    }
                    Err(error) => debug!(uri = %endpoint.uri(), %error, "server unreachable"),
                }
            }
        }

        let Some((channel, uri)) = connected else {
            let uris: Vec<String> = endpoints.iter().map(|e| e.uri().to_string()).collect();
            state.send_replace(FailoverState::Unavailable(format!(
                "no server reachable at {}",
                uris.join(", ")
            )));
            tokio::select! {
                () = state.closed() => return,
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(5));
            continue;
        };

        backoff = Duration::from_millis(200);
        generation += 1;
        debug!(%uri, "connected");
        // Failures of previous connections don't count.
        while failures.try_recv().is_ok() {}
        state.send_replace(FailoverState::Connected(channel, generation));
        loop {
            tokio::select! {
                () = state.closed() => return,
                failure = failures.recv() => match failure {
                    Some(failed) if failed == generation => {
                        warn!(%uri, "connection to the server lost, failing over");
                        break;
                    }
                    Some(_) => {}
                    None => return,
                }
            }
        }
    }
}
