//! An in-process Labellum on an in-memory store, reached over gRPC.

#![allow(dead_code)]

use std::time::Duration;

use labellum::testing::TestServer;
use orchid_api::ClusterConfig;
use orchid_proto::v1 as pb;
use orchid_proto::v1::leadership_service_client::LeadershipServiceClient;
use orchid_proto::v1::node_agent_service_client::NodeAgentServiceClient;
use orchid_proto::v1::node_service_client::NodeServiceClient;
use orchid_proto::v1::pod_service_client::PodServiceClient;
use orchid_proto::v1::scheduler_service_client::SchedulerServiceClient;
use orchid_transport::client::Connection;
use orchid_transport::tls::TlsMaterial;
use tokio::task::JoinHandle;

pub use labellum::testing::fast_config;

/// A test server with typed clients.
pub struct Cluster {
    server: TestServer,
}

impl Cluster {
    pub async fn start() -> Self {
        Self::start_with(fast_config(), None).await
    }

    pub async fn start_with(config: ClusterConfig, tls: Option<&TlsMaterial>) -> Self {
        Self {
            server: TestServer::start_with(config, tls).await,
        }
    }

    pub fn channel(&self) -> Connection {
        self.server.channel(None)
    }

    pub fn channel_as(&self, tls: &TlsMaterial) -> Connection {
        self.server.channel(Some(tls))
    }

    pub fn pods(&self) -> PodServiceClient<Connection> {
        PodServiceClient::new(self.channel())
    }

    pub fn nodes(&self) -> NodeServiceClient<Connection> {
        NodeServiceClient::new(self.channel())
    }

    pub fn agent(&self) -> NodeAgentServiceClient<Connection> {
        NodeAgentServiceClient::new(self.channel())
    }

    pub fn scheduler(&self) -> SchedulerServiceClient<Connection> {
        SchedulerServiceClient::new(self.channel())
    }

    pub fn leadership(&self) -> LeadershipServiceClient<Connection> {
        LeadershipServiceClient::new(self.channel())
    }
}

pub fn resources(cpu_millis: u64, memory_mib: u64) -> pb::Resources {
    pb::Resources {
        cpu_millis,
        memory_bytes: memory_mib << 20,
    }
}

pub fn pod_spec(cpu_millis: u64, memory_mib: u64) -> pb::PodSpec {
    pb::PodSpec {
        containers: vec![pb::Container {
            name: "app".into(),
            image: "nginx".into(),
            resources: Some(resources(cpu_millis, memory_mib)),
        }],
        ..Default::default()
    }
}

pub fn register_request(node: &str, pod_cidr: &str, cpu_millis: u64) -> pb::RegisterRequest {
    pb::RegisterRequest {
        node: node.into(),
        role: pb::NodeRole::Worker.into(),
        schedulable: None,
        runtimes: vec!["io.containerd.runc.v2".into()],
        pod_cidr: pod_cidr.into(),
        capacity: Some(resources(cpu_millis, 16 << 10)),
    }
}

/// Registers a node and keeps it alive with heartbeats until the returned
/// task is aborted.
pub async fn ready_node(
    cluster: &Cluster,
    node: &str,
    pod_cidr: &str,
    cpu_millis: u64,
) -> JoinHandle<()> {
    let mut agent = cluster.agent();
    agent
        .register(register_request(node, pod_cidr, cpu_millis))
        .await
        .unwrap();
    let node = node.to_owned();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let _ = agent
                .heartbeat(pb::HeartbeatRequest {
                    node: node.clone(),
                    health: pb::NodeHealth::Healthy.into(),
                    message: String::new(),
                    usage: None,
                })
                .await;
        }
    })
}

pub async fn create_pod(cluster: &Cluster, name: &str, cpu_millis: u64) -> pb::Pod {
    cluster
        .pods()
        .create_pod(pb::CreatePodRequest {
            name: name.into(),
            spec: Some(pod_spec(cpu_millis, 64)),
        })
        .await
        .unwrap()
        .into_inner()
}

pub async fn get_pod(cluster: &Cluster, name: &str) -> Result<pb::Pod, tonic::Status> {
    cluster
        .pods()
        .get_pod(pb::GetPodRequest { name: name.into() })
        .await
        .map(tonic::Response::into_inner)
}

pub async fn get_node(cluster: &Cluster, name: &str) -> pb::Node {
    cluster
        .nodes()
        .get_node(pb::GetNodeRequest { name: name.into() })
        .await
        .unwrap()
        .into_inner()
}

/// Acquires the Ikebana leadership, returns the token.
pub async fn leader_token(cluster: &Cluster) -> i64 {
    let response = cluster
        .leadership()
        .acquire_leadership(pb::AcquireLeadershipRequest {
            election: "ikebana".into(),
            candidate: "test".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.acquired);
    response.leader.unwrap().token
}

pub async fn bind(
    cluster: &Cluster,
    pod: &str,
    node: &str,
    token: i64,
) -> Result<pb::BindResponse, tonic::Status> {
    let revision = get_pod(cluster, pod).await?.revision;
    cluster
        .scheduler()
        .bind(pb::BindRequest {
            pod: pod.into(),
            pod_revision: revision,
            node: node.into(),
            leader_token: token,
        })
        .await
        .map(tonic::Response::into_inner)
}

/// Polls `check` until it returns `Some`, for up to 15 seconds.
pub async fn eventually<T, F: Future<Output = Option<T>>>(mut check: impl FnMut() -> F) -> T {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn phase(pod: &pb::Pod) -> pb::PodPhase {
    pb::PodPhase::try_from(pod.status.as_ref().unwrap().phase).unwrap()
}

pub fn node_of(pod: &pb::Pod) -> &str {
    pod.binding.as_ref().map_or("", |b| b.node.as_str())
}
