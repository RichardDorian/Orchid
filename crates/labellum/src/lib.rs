//! Labellum, the API server of Orchid.
//!
//! Labellum is stateless: every piece of state is stored in etcd (through
//! [`Store`]), so any request can be served by any instance. Each instance
//! keeps an in-memory cache of pods and nodes ([`cache`]) to serve lists and
//! watches, and one instance at a time runs the [`controller`].

pub mod cache;
pub mod config;
mod controller;
mod election;
mod ops;
mod records;
mod services;
#[cfg(feature = "testing")]
pub mod testing;
mod watch;

use std::sync::Arc;

use orchid_api::{ClusterConfig, Node, Pod, Revision};
use orchid_proto::v1::cluster_service_server::ClusterServiceServer;
use orchid_proto::v1::leadership_service_server::LeadershipServiceServer;
use orchid_proto::v1::node_agent_service_server::NodeAgentServiceServer;
use orchid_proto::v1::node_service_server::NodeServiceServer;
use orchid_proto::v1::pod_service_server::PodServiceServer;
use orchid_proto::v1::scheduler_service_server::SchedulerServiceServer;
use orchid_store::{Compare, Op, Store, Txn, codec, keys};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tonic::transport::Server;
use tonic::transport::server::Router;

use crate::cache::ObjectCache;
use crate::records::{Versioned, read, store_error};

/// State shared by the services and the controller.
pub(crate) struct State<S> {
    store: S,
    pods: ObjectCache<Pod>,
    nodes: ObjectCache<Node>,
}

impl<S: Store> State<S> {
    /// Current revision of etcd.
    async fn current_revision(&self) -> Result<Revision, Status> {
        Ok(self
            .store
            .get(keys::ROOT)
            .await
            .map_err(store_error)?
            .revision)
    }

    /// Waits until the pod cache reflects every write committed before the call.
    async fn sync_pods(&self) -> Result<(), Status> {
        let revision = self.current_revision().await?;
        self.pods.wait_for(revision).await
    }

    /// Waits until the node cache reflects every write committed before the call.
    async fn sync_nodes(&self) -> Result<(), Status> {
        let revision = self.current_revision().await?;
        self.nodes.wait_for(revision).await
    }

    /// The pod as of `revision` or later.
    async fn pod_at(&self, name: &str, revision: Revision) -> Result<Pod, Status> {
        self.pods.wait_for(revision).await?;
        self.pods
            .get(name)
            .ok_or_else(|| Status::not_found(format!("pod {name} not found")))
    }

    /// The node as of `revision` or later.
    async fn node_at(&self, name: &str, revision: Revision) -> Result<Node, Status> {
        self.nodes.wait_for(revision).await?;
        self.nodes
            .get(name)
            .ok_or_else(|| Status::not_found(format!("node {name} not found")))
    }

    /// The cluster configuration, and its key if it exists.
    async fn cluster_config(
        &self,
    ) -> Result<(ClusterConfig, Option<Versioned<ClusterConfig>>), Status> {
        let versioned: Option<Versioned<ClusterConfig>> =
            read(&self.store, keys::CLUSTER_CONFIG).await?;
        let config = versioned
            .as_ref()
            .map(|v| v.value.clone())
            .unwrap_or_default();
        Ok((config, versioned))
    }
}

#[derive(Clone, Debug)]
pub struct Options {
    /// Name of this instance in the controller leader election.
    pub candidate: String,
}

/// A running Labellum instance.
pub struct Labellum<S> {
    state: Arc<State<S>>,
    shutdown: CancellationToken,
    controller: JoinHandle<()>,
}

impl<S: Store + Clone> Labellum<S> {
    /// Initializes the cluster configuration if needed, starts the caches and
    /// the controller election.
    pub async fn start(store: S, options: Options) -> Result<Self, Status> {
        // Concurrent instances agree on the defaults thanks to put-if-absent.
        store
            .txn(
                Txn::new()
                    .when([Compare::absent(keys::CLUSTER_CONFIG)])
                    .then([Op::put(
                        keys::CLUSTER_CONFIG,
                        codec::encode(&ClusterConfig::default()),
                    )]),
            )
            .await
            .map_err(store_error)?;

        let shutdown = CancellationToken::new();
        let state = Arc::new(State {
            pods: ObjectCache::start(store.clone(), shutdown.clone()),
            nodes: ObjectCache::start(store.clone(), shutdown.clone()),
            store,
        });
        let controller = tokio::spawn(election::run_controller(
            state.clone(),
            options.candidate,
            shutdown.clone(),
        ));
        Ok(Self {
            state,
            shutdown,
            controller,
        })
    }

    /// Adds every service to `server`.
    pub fn router(&self, mut server: Server) -> Router {
        let state = &self.state;
        server
            .add_service(ClusterServiceServer::new(services::ClusterApi::new(
                state.clone(),
            )))
            .add_service(PodServiceServer::new(services::PodApi::new(state.clone())))
            .add_service(NodeServiceServer::new(services::NodeApi::new(
                state.clone(),
            )))
            .add_service(NodeAgentServiceServer::new(services::AgentApi::new(
                state.clone(),
            )))
            .add_service(SchedulerServiceServer::new(services::SchedulerApi::new(
                state.clone(),
            )))
            .add_service(LeadershipServiceServer::new(services::LeadershipApi::new(
                state.clone(),
            )))
    }

    /// Stops the caches and the controller, releasing the controller leadership.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        let _ = self.controller.await;
    }
}
