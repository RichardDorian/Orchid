//! Permissions of each identity in mTLS mode.

mod common;

use common::*;
use orchid_proto::v1 as pb;
use orchid_proto::v1::node_agent_service_client::NodeAgentServiceClient;
use orchid_proto::v1::pod_service_client::PodServiceClient;
use orchid_transport::testing::TestPki;
use tonic::Code;

#[tokio::test]
async fn enforces_permissions_in_mtls_mode() {
    let pki = TestPki::new();
    let cluster = Cluster::start_with(fast_config(), Some(&pki.issue("labellum"))).await;
    let user = pki.issue("user:alice");
    let keiki = pki.issue("keiki:phoenix");
    let ikebana = pki.issue("ikebana");

    // Keiki registers its own node only.
    let mut agent = NodeAgentServiceClient::new(cluster.channel_as(&keiki));
    let response = agent
        .register(register_request("", "10.244.0.0/24", 1000))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.node, "phoenix",
        "the name comes from the certificate"
    );
    let error = agent
        .register(register_request("dragon", "10.244.1.0/24", 1000))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);

    // Only users create pods.
    let spec = || pb::CreatePodRequest {
        name: "my-app".into(),
        spec: Some(pod_spec(300, 64)),
    };
    let error = PodServiceClient::new(cluster.channel_as(&ikebana))
        .create_pod(spec())
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
    PodServiceClient::new(cluster.channel_as(&user))
        .create_pod(spec())
        .await
        .unwrap();

    // Keiki only sees the pods of its node.
    let mut keiki_pods = PodServiceClient::new(cluster.channel_as(&keiki));
    let error = keiki_pods
        .list_pods(pb::ListPodsRequest::default())
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
    let own = keiki_pods
        .list_pods(pb::ListPodsRequest {
            filter: Some(pb::PodFilter {
                node: Some("phoenix".into()),
                unbound: false,
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(own.pods.is_empty());
    let error = keiki_pods
        .get_pod(pb::GetPodRequest {
            name: "my-app".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Code::NotFound,
        "unbound pods are hidden from agents"
    );

    // Ikebana sees every pod.
    let all = PodServiceClient::new(cluster.channel_as(&ikebana))
        .list_pods(pb::ListPodsRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(all.pods.len(), 1);
}
