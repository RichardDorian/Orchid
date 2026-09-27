//! Connections in both modes, over TCP and unix sockets.

use orchid_proto::v1 as pb;
use orchid_proto::v1::cluster_service_client::ClusterServiceClient;
use orchid_proto::v1::cluster_service_server::{ClusterService, ClusterServiceServer};
use orchid_transport::identity::Identity;
use orchid_transport::server::{self, Listener};
use orchid_transport::testing::TestPki;
use orchid_transport::tls::TlsMaterial;
use orchid_transport::url::{ListenAddr, ServerUrl};
use tonic::{Request, Response, Status};

/// Answers with the identity of the caller in `default_runtime`.
struct WhoAmI;

#[tonic::async_trait]
impl ClusterService for WhoAmI {
    async fn get_cluster_config(
        &self,
        request: Request<pb::GetClusterConfigRequest>,
    ) -> Result<Response<pb::ClusterConfig>, Status> {
        let identity = Identity::of(&request)?;
        Ok(Response::new(pb::ClusterConfig {
            default_runtime: identity.to_string(),
            ..Default::default()
        }))
    }

    async fn update_cluster_config(
        &self,
        _: Request<pb::UpdateClusterConfigRequest>,
    ) -> Result<Response<pb::ClusterConfig>, Status> {
        Err(Status::unimplemented(""))
    }

    type WatchClusterConfigStream = tokio_stream::Empty<Result<pb::ClusterConfigEvent, Status>>;

    async fn watch_cluster_config(
        &self,
        _: Request<pb::WatchClusterConfigRequest>,
    ) -> Result<Response<Self::WatchClusterConfigStream>, Status> {
        Err(Status::unimplemented(""))
    }
}

/// Starts a server, returns the URL to reach it.
async fn start(address: ListenAddr, tls: Option<&TlsMaterial>) -> ServerUrl {
    let listener = Listener::bind(&address).await.unwrap();
    let url = match (&address, listener.local_addr()) {
        (ListenAddr::Unix(path), _) => ServerUrl::Unix(path.clone()),
        (ListenAddr::Tcp(_), Some(local)) => {
            let scheme = if tls.is_some() { "https" } else { "http" };
            // The test certificates are valid for "localhost".
            ServerUrl::Tcp(format!("{scheme}://localhost:{}", local.port()))
        }
        _ => unreachable!(),
    };
    let router = server::builder(tls)
        .unwrap()
        .add_service(ClusterServiceServer::new(WhoAmI));
    tokio::spawn(server::serve(
        router,
        vec![listener],
        std::future::pending(),
    ));
    url
}

async fn who_am_i(url: &ServerUrl, tls: Option<&TlsMaterial>) -> Result<String, Status> {
    let channel = orchid_transport::client::connect(std::slice::from_ref(url), tls).unwrap();
    let response = ClusterServiceClient::new(channel)
        .get_cluster_config(pb::GetClusterConfigRequest {})
        .await?;
    Ok(response.into_inner().default_runtime)
}

fn tcp() -> ListenAddr {
    "tcp://127.0.0.1:0".parse().unwrap()
}

#[tokio::test]
async fn cleartext_callers_are_anonymous() {
    let dir = tempfile::tempdir().unwrap();
    for address in [tcp(), ListenAddr::Unix(dir.path().join("server.sock"))] {
        let url = start(address, None).await;
        assert_eq!(who_am_i(&url, None).await.unwrap(), "anonymous");
    }
}

#[tokio::test]
async fn mtls_callers_are_identified_by_their_certificate() {
    let pki = TestPki::new();
    let server_tls = pki.issue("labellum");
    let dir = tempfile::tempdir().unwrap();
    for address in [tcp(), ListenAddr::Unix(dir.path().join("server.sock"))] {
        let url = start(address, Some(&server_tls)).await;
        assert_eq!(
            who_am_i(&url, Some(&pki.issue("keiki:phoenix")))
                .await
                .unwrap(),
            "keiki:phoenix"
        );
        assert_eq!(
            who_am_i(&url, Some(&pki.issue("user:alice")))
                .await
                .unwrap(),
            "user:alice"
        );
    }
}

#[tokio::test]
async fn mtls_rejects_unknown_identities() {
    let pki = TestPki::new();
    let url = start(tcp(), Some(&pki.issue("labellum"))).await;
    let error = who_am_i(&url, Some(&pki.issue("admin"))).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn mtls_rejects_certificates_of_another_ca() {
    let pki = TestPki::new();
    let url = start(tcp(), Some(&pki.issue("labellum"))).await;
    let other = TestPki::new().issue("user:mallory");
    assert!(who_am_i(&url, Some(&other)).await.is_err());
}

#[tokio::test]
async fn mtls_rejects_cleartext_clients() {
    let pki = TestPki::new();
    let url = start(tcp(), Some(&pki.issue("labellum"))).await;
    let ServerUrl::Tcp(https) = url else {
        unreachable!()
    };
    let http = ServerUrl::Tcp(https.replace("https://", "http://"));
    assert!(who_am_i(&http, None).await.is_err());
}

#[tokio::test]
async fn shutdown_closes_the_listener_and_open_streams() {
    use std::time::{Duration, Instant};

    use orchid_proto::v1::pod_service_client::PodServiceClient;
    use orchid_proto::v1::pod_service_server::{PodService, PodServiceServer};

    /// A pod service whose watches never end.
    struct Endless;

    #[tonic::async_trait]
    impl PodService for Endless {
        async fn create_pod(
            &self,
            _: Request<pb::CreatePodRequest>,
        ) -> Result<Response<pb::Pod>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn get_pod(
            &self,
            _: Request<pb::GetPodRequest>,
        ) -> Result<Response<pb::Pod>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn list_pods(
            &self,
            _: Request<pb::ListPodsRequest>,
        ) -> Result<Response<pb::ListPodsResponse>, Status> {
            Ok(Response::new(pb::ListPodsResponse::default()))
        }
        type WatchPodsStream = tokio_stream::Pending<Result<pb::PodEvent, Status>>;
        async fn watch_pods(
            &self,
            _: Request<pb::WatchPodsRequest>,
        ) -> Result<Response<Self::WatchPodsStream>, Status> {
            Ok(Response::new(tokio_stream::pending()))
        }
        async fn delete_pod(
            &self,
            _: Request<pb::DeletePodRequest>,
        ) -> Result<Response<pb::Pod>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn update_pod_priority(
            &self,
            _: Request<pb::UpdatePodPriorityRequest>,
        ) -> Result<Response<pb::Pod>, Status> {
            Err(Status::unimplemented(""))
        }
    }

    let listener = Listener::bind(&tcp()).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let router = server::builder(None)
        .unwrap()
        .add_service(PodServiceServer::new(Endless));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(server::serve(router, vec![listener], async move {
        let _ = stopped.await;
    }));

    let url = ServerUrl::Tcp(format!("http://127.0.0.1:{port}"));
    let channel = orchid_transport::client::connect(std::slice::from_ref(&url), None).unwrap();
    let _stream = PodServiceClient::new(channel)
        .watch_pods(pb::WatchPodsRequest::default())
        .await
        .unwrap();

    let started = Instant::now();
    stop.send(()).unwrap();
    // New connections are refused right away.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_err()
    );
    // The server returns despite the open watch.
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("server stopped")
        .unwrap()
        .unwrap();
    assert!(started.elapsed() >= server::SHUTDOWN_GRACE);
}
