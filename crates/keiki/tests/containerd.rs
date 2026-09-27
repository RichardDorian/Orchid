//! The containerd runtime against a real containerd.
//!
//! Ignored by default: needs a running containerd, root, and network access to
//! pull `registry.k8s.io/pause`:
//!
//! ```sh
//! sudo -E cargo test -p keiki --test containerd -- --ignored
//! ```
//!
//! `ORCHID_TEST_CONTAINERD` overrides the socket path.

use std::time::Duration;

use keiki::runtime::containerd::{ContainerdOptions, ContainerdRuntime};
use keiki::runtime::{ContainerState, PodRef, Runtime};
use orchid_api::{
    Bytes, Container, MilliCpu, Pod, PodBinding, PodSpec, PodStatus, Resources, RestartPolicy,
    Timestamp,
};

#[tokio::test]
#[ignore = "needs root and a running containerd"]
async fn runs_a_pod_in_containerd() {
    let socket = std::env::var("ORCHID_TEST_CONTAINERD")
        .unwrap_or_else(|_| "/run/containerd/containerd.sock".to_owned());
    let logs = std::env::temp_dir().join("orchid-test-logs");
    let runtime = ContainerdRuntime::new(ContainerdOptions {
        namespace: "orchid-test".into(),
        log_dir: logs,
        ..ContainerdOptions::new(socket.into())
    })
    .unwrap();
    runtime.health().await.expect("containerd reachable");

    let uid = format!("test-{}", std::process::id());
    let pod = Pod {
        name: "keiki-test".into(),
        uid: uid.clone(),
        revision: 1,
        created_at: Timestamp::now(),
        spec: PodSpec {
            runtime: "io.containerd.runc.v2".into(),
            priority: 0,
            restart_policy: RestartPolicy::Always,
            termination_grace_period: Duration::from_secs(2),
            containers: vec![Container {
                name: "app".into(),
                // Runs until killed.
                image: "registry.k8s.io/pause:3.10".into(),
                resources: Resources::new(MilliCpu(100), Bytes(32 << 20)),
            }],
        },
        binding: Some(PodBinding {
            node: "test".into(),
            attempt: 1,
        }),
        status: PodStatus::pending(Timestamp::now()),
    };
    let reference = PodRef::of(&pod).unwrap();

    runtime.create_pod(&pod, 1).await.expect("pod created");
    assert!(runtime.list_pods().await.unwrap().contains(&reference));
    let status = runtime.pod_status(&reference).await.unwrap();
    assert_eq!(status[0].state, ContainerState::Created);

    runtime
        .start_container(&reference, "app")
        .await
        .expect("container started");
    let status = runtime.pod_status(&reference).await.unwrap();
    assert_eq!(status[0].state, ContainerState::Running);

    runtime
        .remove_pod(&reference, Duration::from_secs(2))
        .await
        .expect("pod removed");
    assert!(!runtime.list_pods().await.unwrap().contains(&reference));
}
