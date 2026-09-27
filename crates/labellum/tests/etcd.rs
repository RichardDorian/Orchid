//! Labellum on a real etcd.
//!
//! Skipped unless `ORCHID_TEST_ETCD` contains the endpoints of an etcd that is
//! not used by anything else (the tests write under `/orchid/`):
//!
//! ```sh
//! ORCHID_TEST_ETCD=http://127.0.0.1:2379 cargo test -p labellum --test etcd
//! ```

use std::time::Duration;

use futures_util::StreamExt;
use labellum::testing::{TestServer, fast_config};
use orchid_proto::v1 as pb;
use orchid_proto::v1::leadership_service_client::LeadershipServiceClient;
use orchid_proto::v1::node_agent_service_client::NodeAgentServiceClient;
use orchid_proto::v1::node_service_client::NodeServiceClient;
use orchid_proto::v1::pod_service_client::PodServiceClient;
use orchid_proto::v1::scheduler_service_client::SchedulerServiceClient;
use orchid_store::{EtcdStore, Op, Store, Txn, keys};

#[tokio::test]
async fn serves_the_api_on_etcd() {
    let Some(endpoints) = std::env::var("ORCHID_TEST_ETCD")
        .ok()
        .filter(|e| !e.is_empty())
    else {
        eprintln!("skipped: ORCHID_TEST_ETCD is not set");
        return;
    };
    let endpoints: Vec<&str> = endpoints.split(',').collect();
    let store = EtcdStore::connect(&endpoints, None).await.unwrap();
    store
        .txn(Txn::new().then([Op::delete_prefix(keys::ROOT)]))
        .await
        .unwrap();

    let server = TestServer::start_on(store, fast_config(), None).await;
    let channel = server.channel(None);
    let mut pods = PodServiceClient::new(channel.clone());

    // Register a node and keep it alive.
    let mut agent = NodeAgentServiceClient::new(channel.clone());
    agent
        .register(pb::RegisterRequest {
            node: "phoenix".into(),
            role: pb::NodeRole::Worker.into(),
            schedulable: None,
            runtimes: vec!["io.containerd.runc.v2".into()],
            pod_cidr: "10.244.0.0/24".into(),
            capacity: Some(pb::Resources {
                cpu_millis: 1000,
                memory_bytes: 1 << 30,
            }),
        })
        .await
        .unwrap();
    let heartbeats = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let _ = agent
                .heartbeat(pb::HeartbeatRequest {
                    node: "phoenix".into(),
                    health: pb::NodeHealth::Healthy.into(),
                    ..Default::default()
                })
                .await;
        }
    });

    // Watch the pods of the node, with bookmarks.
    let list = pods
        .list_pods(pb::ListPodsRequest {
            filter: Some(pb::PodFilter {
                node: Some("phoenix".into()),
                unbound: false,
            }),
        })
        .await
        .unwrap()
        .into_inner();
    let mut watch = pods
        .watch_pods(pb::WatchPodsRequest {
            filter: Some(pb::PodFilter {
                node: Some("phoenix".into()),
                unbound: false,
            }),
            from_revision: list.revision,
        })
        .await
        .unwrap()
        .into_inner();

    pods.create_pod(pb::CreatePodRequest {
        name: "my-app".into(),
        spec: Some(pb::PodSpec {
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
    let token = LeadershipServiceClient::new(channel.clone())
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
    let pod = pods
        .get_pod(pb::GetPodRequest {
            name: "my-app".into(),
        })
        .await
        .unwrap()
        .into_inner();
    SchedulerServiceClient::new(channel.clone())
        .bind(pb::BindRequest {
            pod: "my-app".into(),
            pod_revision: pod.revision,
            node: "phoenix".into(),
            leader_token: token,
        })
        .await
        .unwrap();

    let mut added = None;
    let mut bookmark = None;
    while added.is_none() || bookmark.is_none() {
        let event = tokio::time::timeout(Duration::from_secs(15), watch.next())
            .await
            .expect("watch event")
            .unwrap()
            .unwrap();
        match pb::EventType::try_from(event.r#type).unwrap() {
            pb::EventType::Added => added = Some(event),
            pb::EventType::Bookmark => bookmark = Some(event),
            _ => {}
        }
    }
    assert_eq!(added.unwrap().pod.unwrap().name, "my-app");

    let node = NodeServiceClient::new(channel)
        .get_node(pb::GetNodeRequest {
            name: "phoenix".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let status = node.status.unwrap();
    assert_eq!(status.condition, i32::from(pb::NodeCondition::Ready));
    assert_eq!(status.allocated.unwrap().cpu_millis, 300);
    heartbeats.abort();
}
