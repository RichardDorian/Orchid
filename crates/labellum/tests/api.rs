//! API behavior of Labellum, through gRPC.

mod common;

use common::*;
use futures_util::StreamExt;
use orchid_proto::v1 as pb;
use orchid_transport::errors;
use tonic::Code;

fn reason(status: &tonic::Status) -> String {
    errors::reason(status).unwrap_or_default()
}

#[tokio::test]
async fn creates_lists_and_deletes_pods() {
    let cluster = Cluster::start().await;
    let pod = create_pod(&cluster, "my-app", 300).await;
    assert_eq!(phase(&pod), pb::PodPhase::Pending);
    assert_eq!(pod.spec.as_ref().unwrap().runtime, "io.containerd.runc.v2");
    assert!(!pod.uid.is_empty());
    assert!(pod.binding.is_none());

    let list = cluster
        .pods()
        .list_pods(pb::ListPodsRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.pods.len(), 1);
    assert!(list.revision >= pod.revision);

    let deleted = cluster
        .pods()
        .delete_pod(pb::DeletePodRequest {
            name: "my-app".into(),
            uid: Some(pod.uid.clone()),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(phase(&deleted), pb::PodPhase::Terminating);
    assert_eq!(
        get_pod(&cluster, "my-app").await.unwrap_err().code(),
        Code::NotFound
    );
}

#[tokio::test]
async fn rejects_invalid_and_duplicate_pods() {
    let cluster = Cluster::start().await;
    create_pod(&cluster, "my-app", 300).await;

    let duplicate = cluster
        .pods()
        .create_pod(pb::CreatePodRequest {
            name: "my-app".into(),
            spec: Some(pod_spec(300, 64)),
        })
        .await
        .unwrap_err();
    assert_eq!(duplicate.code(), Code::AlreadyExists);

    let invalid = cluster
        .pods()
        .create_pod(pb::CreatePodRequest {
            name: "Invalid_Name".into(),
            spec: Some(pod_spec(0, 64)),
        })
        .await
        .unwrap_err();
    assert_eq!(invalid.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn registers_nodes() {
    let cluster = Cluster::start().await;
    let _heartbeats = ready_node(&cluster, "phoenix", "10.244.0.0/24", 4000).await;

    let node = get_node(&cluster, "phoenix").await;
    let status = node.status.unwrap();
    assert_eq!(status.condition, i32::from(pb::NodeCondition::Ready));
    assert!(node.spec.unwrap().schedulable);
    assert_eq!(node.info.unwrap().pod_cidr, "10.244.0.0/24");

    // Overlapping pod CIDR.
    let error = cluster
        .agent()
        .register(register_request("dragon", "10.244.0.0/16", 4000))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);

    // Missing default runtime.
    let mut request = register_request("dragon", "10.244.1.0/24", 4000);
    request.runtimes = vec!["io.containerd.kata.v2".into()];
    let error = cluster.agent().register(request).await.unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);

    // Registering again keeps the node spec.
    cluster
        .nodes()
        .update_node_spec(pb::UpdateNodeSpecRequest {
            name: "phoenix".into(),
            schedulable: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    cluster
        .agent()
        .register(register_request("phoenix", "10.244.0.0/24", 4000))
        .await
        .unwrap();
    assert!(
        !get_node(&cluster, "phoenix")
            .await
            .spec
            .unwrap()
            .schedulable
    );
}

#[tokio::test]
async fn nodes_become_unreachable_without_heartbeats() {
    let cluster = Cluster::start().await;
    cluster
        .agent()
        .register(register_request("phoenix", "10.244.0.0/24", 4000))
        .await
        .unwrap();

    eventually(|| async {
        let node = get_node(&cluster, "phoenix").await;
        (node.status.unwrap().condition == i32::from(pb::NodeCondition::Unreachable)).then_some(())
    })
    .await;

    let error = cluster
        .agent()
        .heartbeat(pb::HeartbeatRequest {
            node: "phoenix".into(),
            health: pb::NodeHealth::Healthy.into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
}

#[tokio::test]
async fn binds_pods_without_overcommit() {
    let cluster = Cluster::start().await;
    let _heartbeats = ready_node(&cluster, "phoenix", "10.244.0.0/24", 1000).await;
    let token = leader_token(&cluster).await;
    create_pod(&cluster, "a", 600).await;
    create_pod(&cluster, "b", 600).await;

    let response = bind(&cluster, "a", "phoenix", token).await.unwrap();
    assert_eq!(response.binding.unwrap().attempt, 1);
    let pod = get_pod(&cluster, "a").await.unwrap();
    assert_eq!(phase(&pod), pb::PodPhase::Creating);
    assert_eq!(node_of(&pod), "phoenix");
    let node = get_node(&cluster, "phoenix").await;
    assert_eq!(node.status.unwrap().allocated.unwrap().cpu_millis, 600);

    let error = bind(&cluster, "b", "phoenix", token).await.unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(reason(&error), "BIND_FAILURE_INSUFFICIENT_RESOURCES");

    let error = bind(&cluster, "a", "phoenix", token).await.unwrap_err();
    assert_eq!(reason(&error), "BIND_FAILURE_POD_CHANGED");

    let error = bind(&cluster, "b", "phoenix", token + 1).await.unwrap_err();
    assert_eq!(error.code(), Code::Aborted);
    assert_eq!(reason(&error), "BIND_FAILURE_STALE_LEADER_TOKEN");

    let error = bind(&cluster, "b", "unknown", token).await.unwrap_err();
    assert_eq!(reason(&error), "BIND_FAILURE_NODE_UNAVAILABLE");
}

#[tokio::test]
async fn concurrent_binds_never_overcommit() {
    let cluster = Cluster::start().await;
    let _heartbeats = ready_node(&cluster, "phoenix", "10.244.0.0/24", 1000).await;
    let token = leader_token(&cluster).await;
    for i in 0..10 {
        create_pod(&cluster, &format!("pod-{i}"), 300).await;
    }

    let binds = (0..10).map(|i| {
        let cluster = &cluster;
        async move { bind(cluster, &format!("pod-{i}"), "phoenix", token).await }
    });
    let results = futures_util::future::join_all(binds).await;
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 3);

    let node = get_node(&cluster, "phoenix").await;
    assert_eq!(node.status.unwrap().allocated.unwrap().cpu_millis, 900);
}

#[tokio::test]
async fn watches_follow_filters_and_resume() {
    let cluster = Cluster::start().await;
    let _heartbeats = ready_node(&cluster, "phoenix", "10.244.0.0/24", 1000).await;
    let token = leader_token(&cluster).await;

    let list = cluster
        .pods()
        .list_pods(pb::ListPodsRequest {
            filter: Some(pb::PodFilter {
                node: Some("phoenix".into()),
                unbound: false,
            }),
        })
        .await
        .unwrap()
        .into_inner();
    let watch = |revision| {
        let mut pods = cluster.pods();
        async move {
            pods.watch_pods(pb::WatchPodsRequest {
                filter: Some(pb::PodFilter {
                    node: Some("phoenix".into()),
                    unbound: false,
                }),
                from_revision: revision,
            })
            .await
            .unwrap()
            .into_inner()
            .filter(|event| {
                // Skip bookmarks.
                std::future::ready(
                    event
                        .as_ref()
                        .map_or(true, |e| e.r#type != i32::from(pb::EventType::Bookmark)),
                )
            })
        }
    };
    let mut events = Box::pin(watch(list.revision).await);

    create_pod(&cluster, "my-app", 300).await;
    bind(&cluster, "my-app", "phoenix", token).await.unwrap();
    let added = events.next().await.unwrap().unwrap();
    assert_eq!(added.r#type, i32::from(pb::EventType::Added));
    assert_eq!(added.pod.unwrap().name, "my-app");

    cluster
        .pods()
        .delete_pod(pb::DeletePodRequest {
            name: "my-app".into(),
            uid: None,
        })
        .await
        .unwrap();
    let modified = events.next().await.unwrap().unwrap();
    assert_eq!(modified.r#type, i32::from(pb::EventType::Modified));
    assert_eq!(phase(&modified.pod.unwrap()), pb::PodPhase::Terminating);

    // A new watch from the first event replays what followed.
    let mut resumed = Box::pin(watch(added.revision).await);
    let replayed = resumed.next().await.unwrap().unwrap();
    assert_eq!(replayed.revision, modified.revision);

    // Too old revisions must be listed again.
    let error = cluster
        .pods()
        .watch_pods(pb::WatchPodsRequest {
            filter: None,
            from_revision: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::OutOfRange);
}

#[tokio::test]
async fn agents_report_status_and_finalize_pods() {
    let cluster = Cluster::start().await;
    let _heartbeats = ready_node(&cluster, "phoenix", "10.244.0.0/24", 1000).await;
    let token = leader_token(&cluster).await;
    let pod = create_pod(&cluster, "my-app", 300).await;
    bind(&cluster, "my-app", "phoenix", token).await.unwrap();

    let update = |attempt| pb::UpdatePodStatusRequest {
        pod: "my-app".into(),
        uid: pod.uid.clone(),
        attempt,
        phase: pb::PodPhase::Running.into(),
        reason: "Started".into(),
        message: String::new(),
        containers: vec![pb::ContainerStatus {
            name: "app".into(),
            state: pb::ContainerState::Running.into(),
            ..Default::default()
        }],
    };
    cluster.agent().update_pod_status(update(1)).await.unwrap();
    assert_eq!(
        phase(&get_pod(&cluster, "my-app").await.unwrap()),
        pb::PodPhase::Running
    );

    let error = cluster
        .agent()
        .update_pod_status(update(2))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);

    let finalize = pb::FinalizePodRequest {
        pod: "my-app".into(),
        uid: pod.uid.clone(),
        attempt: 1,
    };
    let error = cluster
        .agent()
        .finalize_pod(finalize.clone())
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Code::FailedPrecondition,
        "not terminating yet"
    );

    cluster
        .pods()
        .delete_pod(pb::DeletePodRequest {
            name: "my-app".into(),
            uid: None,
        })
        .await
        .unwrap();
    cluster.agent().finalize_pod(finalize).await.unwrap();
    assert_eq!(
        get_pod(&cluster, "my-app").await.unwrap_err().code(),
        Code::NotFound
    );
    let node = get_node(&cluster, "phoenix").await;
    assert_eq!(node.status.unwrap().allocated.unwrap().cpu_millis, 0);
}

#[tokio::test]
async fn controller_reschedules_pods_of_lost_nodes() {
    let cluster = Cluster::start().await;
    let heartbeats = ready_node(&cluster, "phoenix", "10.244.0.0/24", 1000).await;
    let token = leader_token(&cluster).await;
    create_pod(&cluster, "restartable", 300).await;
    cluster
        .pods()
        .create_pod(pb::CreatePodRequest {
            name: "one-shot".into(),
            spec: Some(pb::PodSpec {
                restart_policy: pb::RestartPolicy::Never.into(),
                ..pod_spec(300, 64)
            }),
        })
        .await
        .unwrap();
    bind(&cluster, "restartable", "phoenix", token)
        .await
        .unwrap();
    bind(&cluster, "one-shot", "phoenix", token).await.unwrap();

    heartbeats.abort();

    let pod = eventually(|| async {
        let pod = get_pod(&cluster, "restartable").await.unwrap();
        (phase(&pod) == pb::PodPhase::Pending).then_some(pod)
    })
    .await;
    assert_eq!(pod.status.as_ref().unwrap().reason, "NodeLost");
    assert_eq!(node_of(&pod), "");
    assert_eq!(pod.binding.unwrap().attempt, 1, "the attempt is kept");

    let pod = eventually(|| async {
        let pod = get_pod(&cluster, "one-shot").await.unwrap();
        (phase(&pod) == pb::PodPhase::Failed).then_some(pod)
    })
    .await;
    assert_eq!(node_of(&pod), "");

    let node = get_node(&cluster, "phoenix").await;
    assert_eq!(node.status.unwrap().allocated.unwrap().cpu_millis, 0);
}

#[tokio::test]
async fn controller_drains_nodes() {
    let cluster = Cluster::start().await;
    let _heartbeats = ready_node(&cluster, "phoenix", "10.244.0.0/24", 1000).await;
    let token = leader_token(&cluster).await;
    let pod = create_pod(&cluster, "my-app", 300).await;
    bind(&cluster, "my-app", "phoenix", token).await.unwrap();

    cluster
        .nodes()
        .update_node_spec(pb::UpdateNodeSpecRequest {
            name: "phoenix".into(),
            draining: Some(true),
            ..Default::default()
        })
        .await
        .unwrap();

    let evicted = eventually(|| async {
        let pod = get_pod(&cluster, "my-app").await.unwrap();
        (phase(&pod) == pb::PodPhase::Terminating).then_some(pod)
    })
    .await;
    assert_eq!(evicted.status.unwrap().reason, "Evicted");

    // The node stopped the pod: it is rescheduled, not deleted.
    cluster
        .agent()
        .finalize_pod(pb::FinalizePodRequest {
            pod: "my-app".into(),
            uid: pod.uid,
            attempt: 1,
        })
        .await
        .unwrap();
    let pod = get_pod(&cluster, "my-app").await.unwrap();
    assert_eq!(phase(&pod), pb::PodPhase::Pending);
    assert_eq!(node_of(&pod), "");

    let spec = eventually(|| async {
        let spec = get_node(&cluster, "phoenix").await.spec.unwrap();
        (!spec.draining).then_some(spec)
    })
    .await;
    assert!(!spec.schedulable, "a drained node stays cordoned");
}

#[tokio::test]
async fn leadership_is_exclusive() {
    let cluster = Cluster::start().await;
    let token = leader_token(&cluster).await;

    let second = cluster
        .leadership()
        .acquire_leadership(pb::AcquireLeadershipRequest {
            election: "ikebana".into(),
            candidate: "other".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!second.acquired);
    assert_eq!(second.leader.unwrap().candidate, "test");

    cluster
        .leadership()
        .renew_leadership(pb::RenewLeadershipRequest {
            election: "ikebana".into(),
            token,
        })
        .await
        .unwrap();
    let error = cluster
        .leadership()
        .renew_leadership(pb::RenewLeadershipRequest {
            election: "ikebana".into(),
            token: token + 1,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::NotFound);

    cluster
        .leadership()
        .release_leadership(pb::ReleaseLeadershipRequest {
            election: "ikebana".into(),
            token,
        })
        .await
        .unwrap();
    assert_ne!(leader_token(&cluster).await, token);

    let error = cluster
        .leadership()
        .acquire_leadership(pb::AcquireLeadershipRequest {
            election: "controller".into(),
            candidate: "intruder".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
}
