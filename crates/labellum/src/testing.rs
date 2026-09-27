//! An in-process Labellum on an in-memory store, reached over gRPC on
//! localhost. For tests of Labellum and of the other components.

use std::time::Duration;

use orchid_api::ClusterConfig;
use orchid_store::{MemoryStore, Store, codec, keys};
use orchid_transport::server::{self, Listener};
use orchid_transport::tls::TlsMaterial;
use tonic::transport::Channel;

use crate::{Labellum, Options};

/// Cluster configuration with durations short enough for tests.
pub fn fast_config() -> ClusterConfig {
    ClusterConfig {
        node_lease_ttl: Duration::from_secs(1),
        pod_eviction_delay: Duration::from_secs(1),
        leader_lease_ttl: Duration::from_secs(1),
        ..ClusterConfig::default()
    }
}

pub struct TestServer<S = MemoryStore> {
    pub store: S,
    port: u16,
    tls: bool,
    labellum: Option<Labellum<S>>,
}

impl TestServer {
    /// Starts a cleartext server with [`fast_config`].
    pub async fn start() -> Self {
        Self::start_with(fast_config(), None).await
    }

    /// Starts a server, in mTLS mode if `tls` is set (the certificate must be
    /// valid for `localhost`).
    pub async fn start_with(config: ClusterConfig, tls: Option<&TlsMaterial>) -> Self {
        Self::start_on(MemoryStore::new(), config, tls).await
    }
}

impl<S: Store + Clone> TestServer<S> {
    /// Starts a server on `store`, which must not be used by another cluster.
    pub async fn start_on(store: S, config: ClusterConfig, tls: Option<&TlsMaterial>) -> Self {
        store
            .put(keys::CLUSTER_CONFIG, codec::encode(&config))
            .await
            .expect("in-memory store");
        let labellum = Labellum::start(
            store.clone(),
            Options {
                candidate: "test".into(),
            },
        )
        .await
        .expect("labellum starts");
        let listener = Listener::bind(&"tcp://127.0.0.1:0".parse().expect("valid address"))
            .await
            .expect("bind localhost");
        let port = listener.local_addr().expect("TCP listener").port();
        let router = labellum.router(server::builder(tls).expect("valid TLS configuration"));
        tokio::spawn(server::serve(
            router,
            vec![listener],
            std::future::pending(),
        ));
        Self {
            store,
            port,
            tls: tls.is_some(),
            labellum: Some(labellum),
        }
    }

    pub fn url(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{scheme}://localhost:{}", self.port)
    }

    /// A channel to the server, presenting `tls` in mTLS mode.
    pub fn channel(&self, tls: Option<&TlsMaterial>) -> Channel {
        let urls = orchid_transport::url::parse_server_urls(&[self.url()], tls.is_some())
            .expect("valid URL");
        orchid_transport::client::connect(&urls, tls).expect("valid channel")
    }

    /// Stops the caches and the controller.
    pub async fn shutdown(&mut self) {
        if let Some(labellum) = self.labellum.take() {
            labellum.shutdown().await;
        }
    }
}
