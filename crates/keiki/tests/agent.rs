//! Keiki on a fake runtime, with an in-process Labellum (and Ikebana for the
//! end to end test).

use std::sync::Arc;
use std::time::Duration;

use keiki::runtime::fake::FakeRuntime;
use keiki::runtime::{ContainerState, PodRef};
use keiki::{Options, Registration};
use labellum::testing::TestServer;
use orchid_api::{Bytes, MilliCpu, NodeRole, Resources};
use orchid_proto::v1 as pb;
use orchid_proto::v1::leadership_service_client::LeadershipServiceClient;
use orchid_proto::v1::node_service_client::NodeServiceClient;
use orchid_proto::v1::pod_service_client::PodServiceClient;
use orchid_proto::v1::scheduler_service_client::SchedulerServiceClient;
use orchid_transport::client::Connection;
use tokio_util::sync::CancellationToken;

struct Harness {
    server: TestServer,
    runtime: FakeRuntime,
    shutdown: CancellationToken,
}

impl Harness {
    async fn start() -> Self {
        Self::start_with(FakeRuntime::new()).await
    }

    async fn start_with(runtime: FakeRuntime) -> Self {
        let server = TestServer::start().await;
        let shutdown = CancellationToken::new();
        let registration = Registration {
            node: Some("phoenix".into()),
            role: NodeRole::Worker,
            schedulable: None,
            runtimes: vec!["io.containerd.runc.v2".into()],
            pod_cidr: "10.244.0.0/24".parse().unwrap(),
            capacity: Resources::new(MilliCpu(4000), Bytes(8 << 30)),
        };
        let options = Options {
            restart_backoff: Duration::from_millis(200),
            cleanup_interval: Duration::from_millis(200),
        };
        tokio::spawn(keiki::run(
            Arc::new(runtime.clone()),
            server.channel(None),
            registration,
            options,
            shutdown.clone(),
        ));
        let harness = Self {
            server,
            runtime,
            shutdown,
        };
        eventually(|| async {
            let node = harness.node().await?;
            (node.status?.condition == i32::from(pb::NodeCondition::Ready)).then_some(())
        })
        .await;
        harness
    }

    fn channel(&self) -> Connection {
        self.server.channel(None)
    }

    async fn node(&self) -> Option<pb::Node> {
        NodeServiceClient::new(self.channel())
            .get_node(pb::GetNodeRequest {
                name: "phoenix".into(),
            })
            .await
            .ok()
            .map(tonic::Response::into_inner)
    }

    async fn pod(&self, name: &str) -> Option<pb::Pod> {
        PodServiceClient::new(self.channel())
            .get_pod(pb::GetPodRequest { name: name.into() })
            .await
            .ok()
            .map(tonic::Response::into_inner)
    }

    async fn create_pod(&self, name: &str, restart_policy: pb::RestartPolicy) {
        PodServiceClient::new(self.channel())
            .create_pod(pb::CreatePodRequest {
                name: name.into(),
                spec: Some(pb::PodSpec {
                    restart_policy: restart_policy.into(),
                    containers: vec![pb::Container {
                        name: "app".into(),
                        image: "nginx".into(),
                        resources: Some(pb::Resources {
                            cpu_millis: 300,
                            memory_bytes: 64 << 20,
                        }),
                    }],
                    ..Default::default()
                }),
            })
            .await
            .unwrap();
    }

    /// Binds a pod to the node, playing the scheduler.
    async fn bind(&self, name: &str) {
        let token = LeadershipServiceClient::new(self.channel())
            .acquire_leadership(pb::AcquireLeadershipRequest {
                election: "ikebana".into(),
                candidate: "test".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .leader
            .unwrap()
            .token;
        let revision = self.pod(name).await.unwrap().revision;
        SchedulerServiceClient::new(self.channel())
            .bind(pb::BindRequest {
                pod: name.into(),
                pod_revision: revision,
                node: "phoenix".into(),
                leader_token: token,
            })
            .await
            .unwrap();
    }

    async fn wait_phase(&self, name: &str, phase: pb::PodPhase) -> pb::Pod {
        eventually(|| async {
            let pod = self.pod(name).await?;
            (pod.status.as_ref()?.phase == i32::from(phase)).then_some(pod)
        })
        .await
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn eventually<T, F: Future<Output = Option<T>>>(mut check: impl FnMut() -> F) -> T {
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

fn container(pod: &pb::Pod) -> pb::ContainerStatus {
    pod.status.as_ref().unwrap().containers[0].clone()
}

#[tokio::test]
async fn runs_bound_pods() {
    let harness = Harness::start().await;
    harness
        .create_pod("my-app", pb::RestartPolicy::Always)
        .await;
    harness.bind("my-app").await;

    let pod = harness.wait_phase("my-app", pb::PodPhase::Running).await;
    assert_eq!(
        container(&pod).state,
        i32::from(pb::ContainerState::Running)
    );
    assert!(container(&pod).started_at.is_some());
    let pods = harness.runtime.pods();
    let containers = pods.values().next().unwrap();
    assert_eq!(containers["app"], ContainerState::Running);
}

#[tokio::test]
async fn restarts_containers_according_to_the_policy() {
    let harness = Harness::start().await;
    harness
        .create_pod("my-app", pb::RestartPolicy::Failure)
        .await;
    harness.bind("my-app").await;
    harness.wait_phase("my-app", pb::PodPhase::Running).await;

    harness.runtime.exit("my-app", "app", 1);
    let pod = eventually(|| async {
        let pod = harness.pod("my-app").await?;
        (container(&pod).restart_count == 1).then_some(pod)
    })
    .await;
    assert_eq!(harness.runtime.starts("my-app", "app"), 2);
    assert_eq!(container(&pod).exit_code, None, "running again");

    harness.runtime.exit("my-app", "app", 0);
    let pod = harness.wait_phase("my-app", pb::PodPhase::Succeeded).await;
    assert_eq!(container(&pod).exit_code, Some(0));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        harness.runtime.starts("my-app", "app"),
        2,
        "not restarted after success"
    );
}

#[tokio::test]
async fn terminates_deleted_pods() {
    let harness = Harness::start().await;
    harness
        .create_pod("my-app", pb::RestartPolicy::Always)
        .await;
    harness.bind("my-app").await;
    harness.wait_phase("my-app", pb::PodPhase::Running).await;

    PodServiceClient::new(harness.channel())
        .delete_pod(pb::DeletePodRequest {
            name: "my-app".into(),
            uid: None,
        })
        .await
        .unwrap();
    eventually(|| async { harness.pod("my-app").await.is_none().then_some(()) }).await;
    assert!(harness.runtime.pods().is_empty());
}

#[tokio::test]
async fn removes_pods_not_bound_to_the_node() {
    let runtime = FakeRuntime::new();
    runtime.insert(
        PodRef {
            uid: "gone".into(),
            attempt: 1,
            name: "old".into(),
        },
        &["app"],
    );
    let harness = Harness::start_with(runtime).await;
    eventually(|| async { harness.runtime.pods().is_empty().then_some(()) }).await;
}

#[tokio::test]
async fn registers_again_when_the_node_is_deleted() {
    let harness = Harness::start().await;
    let uid = harness.node().await.unwrap().uid;
    NodeServiceClient::new(harness.channel())
        .delete_node(pb::DeleteNodeRequest {
            name: "phoenix".into(),
            uid: None,
        })
        .await
        .unwrap();
    let node = eventually(|| async { harness.node().await }).await;
    assert_ne!(node.uid, uid, "a new node was registered");
}

#[tokio::test]
async fn reports_an_unhealthy_runtime() {
    let harness = Harness::start().await;
    harness.runtime.set_unhealthy(Some("containerd is down"));
    let node = eventually(|| async {
        let node = harness.node().await?;
        (node.status.as_ref()?.condition == i32::from(pb::NodeCondition::Unhealthy)).then_some(node)
    })
    .await;
    assert!(node.status.unwrap().message.contains("containerd is down"));
}

#[tokio::test]
async fn reports_creation_failures() {
    let harness = Harness::start().await;
    harness.runtime.fail_creations(Some("image not found"));
    harness
        .create_pod("my-app", pb::RestartPolicy::Always)
        .await;
    harness.bind("my-app").await;
    let pod = eventually(|| async {
        let pod = harness.pod("my-app").await?;
        (pod.status.as_ref()?.reason == "CreateFailed").then_some(pod)
    })
    .await;
    assert!(pod.status.unwrap().message.contains("image not found"));

    // Our own status reports must not shorten the backoff (200ms, then
    // 400ms, 800ms...): a hot loop would make dozens of attempts.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let attempts = harness.runtime.creation_attempts();
    assert!(
        attempts <= 5,
        "{attempts} creation attempts in about 2 seconds"
    );

    harness.runtime.fail_creations(None);
    harness.wait_phase("my-app", pb::PodPhase::Running).await;
}

#[tokio::test]
async fn runs_pods_end_to_end() {
    let harness = Harness::start().await;
    let shutdown = CancellationToken::new();
    tokio::spawn(ikebana::run(
        harness.channel(),
        "ikebana".into(),
        shutdown.clone(),
    ));

    harness
        .create_pod("my-app", pb::RestartPolicy::Always)
        .await;
    let pod = harness.wait_phase("my-app", pb::PodPhase::Running).await;
    assert_eq!(pod.binding.unwrap().node, "phoenix");
    shutdown.cancel();
}
