//! Ikebana scheduling pods through an in-process Labellum.

use std::time::Duration;

use labellum::testing::TestServer;
use orchid_proto::v1 as pb;
use orchid_proto::v1::leadership_service_client::LeadershipServiceClient;
use orchid_proto::v1::node_agent_service_client::NodeAgentServiceClient;
use orchid_proto::v1::pod_service_client::PodServiceClient;
use orchid_transport::client::Connection;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Registers a node and keeps it alive until the task is aborted.
async fn node(channel: Connection, name: &str, index: u8, cpu_millis: u64) -> JoinHandle<()> {
    let mut agent = NodeAgentServiceClient::new(channel);
    agent
        .register(pb::RegisterRequest {
            node: name.into(),
            role: pb::NodeRole::Worker.into(),
            schedulable: None,
            runtimes: vec!["io.containerd.runc.v2".into()],
            pod_cidr: format!("10.244.{index}.0/24"),
            capacity: Some(pb::Resources {
                cpu_millis,
                memory_bytes: 16 << 30,
            }),
        })
        .await
        .unwrap();
    let name = name.to_owned();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let _ = agent
                .heartbeat(pb::HeartbeatRequest {
                    node: name.clone(),
                    health: pb::NodeHealth::Healthy.into(),
                    ..Default::default()
                })
                .await;
        }
    })
}

async fn create_pod(channel: Connection, name: &str, cpu_millis: u64) {
    PodServiceClient::new(channel)
        .create_pod(pb::CreatePodRequest {
            name: name.into(),
            spec: Some(pb::PodSpec {
                containers: vec![pb::Container {
                    name: "app".into(),
                    image: "nginx".into(),
                    resources: Some(pb::Resources {
                        cpu_millis,
                        memory_bytes: 64 << 20,
                    }),
                }],
                ..Default::default()
            }),
        })
        .await
        .unwrap();
}

async fn pods(channel: Connection) -> Vec<pb::Pod> {
    PodServiceClient::new(channel)
        .list_pods(pb::ListPodsRequest::default())
        .await
        .unwrap()
        .into_inner()
        .pods
}

fn node_of(pod: &pb::Pod) -> &str {
    pod.binding.as_ref().map_or("", |b| b.node.as_str())
}

/// Waits until `count` pods are bound, returns every pod.
async fn wait_bound(channel: Connection, count: usize) -> Vec<pb::Pod> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let pods = pods(channel.clone()).await;
        if pods.iter().filter(|p| !node_of(p).is_empty()).count() >= count {
            return pods;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pods not scheduled in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn start_ikebana(channel: Connection, candidate: &str) -> (CancellationToken, JoinHandle<()>) {
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(ikebana::run(channel, candidate.into(), shutdown.clone()));
    (shutdown, task)
}

#[tokio::test]
async fn spreads_pods_on_the_least_loaded_nodes() {
    let server = TestServer::start().await;
    let channel = server.channel(None);
    let _a = node(channel.clone(), "a", 0, 2000).await;
    let _b = node(channel.clone(), "b", 1, 2000).await;
    let (shutdown, task) = start_ikebana(channel.clone(), "ikebana-1");

    for i in 0..4 {
        create_pod(channel.clone(), &format!("pod-{i}"), 500).await;
    }
    let pods = wait_bound(channel.clone(), 4).await;
    let on_a = pods.iter().filter(|p| node_of(p) == "a").count();
    assert_eq!(on_a, 2, "pods are balanced: {pods:?}");

    shutdown.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn waits_for_capacity() {
    let server = TestServer::start().await;
    let channel = server.channel(None);
    let _a = node(channel.clone(), "a", 0, 1000).await;
    let (shutdown, task) = start_ikebana(channel.clone(), "ikebana-1");

    create_pod(channel.clone(), "small", 800).await;
    create_pod(channel.clone(), "big", 900).await;
    // The pod waiting for the longest time goes first, the other one doesn't fit.
    wait_bound(channel.clone(), 1).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let bound: Vec<_> = pods(channel.clone())
        .await
        .into_iter()
        .filter(|p| !node_of(p).is_empty())
        .collect();
    assert_eq!(bound.len(), 1, "a node never gets overcommitted");

    // A new node lets the waiting pod be scheduled.
    let _b = node(channel.clone(), "b", 1, 1000).await;
    wait_bound(channel.clone(), 2).await;

    shutdown.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn fails_over_to_another_instance() {
    let server = TestServer::start().await;
    let channel = server.channel(None);
    let _a = node(channel.clone(), "a", 0, 4000).await;

    let (first, first_task) = start_ikebana(channel.clone(), "ikebana-1");
    // Let the first instance win the election.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (second, second_task) = start_ikebana(channel.clone(), "ikebana-2");

    create_pod(channel.clone(), "before", 100).await;
    wait_bound(channel.clone(), 1).await;
    let leader = |channel: Connection| async move {
        LeadershipServiceClient::new(channel)
            .get_leader(pb::GetLeaderRequest {
                election: "ikebana".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .leader
            .map(|l| l.candidate)
    };
    assert_eq!(leader(channel.clone()).await.as_deref(), Some("ikebana-1"));

    // Stopping the leader releases the leadership: the follower takes over.
    first.cancel();
    first_task.await.unwrap();
    create_pod(channel.clone(), "after", 100).await;
    wait_bound(channel.clone(), 2).await;
    assert_eq!(leader(channel.clone()).await.as_deref(), Some("ikebana-2"));

    second.cancel();
    second_task.await.unwrap();
}
