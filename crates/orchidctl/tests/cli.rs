//! orchidctl against an in-process Labellum.

use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Parser;
use labellum::testing::{TestServer, fast_config};
use orchid_proto::v1 as pb;
use orchid_proto::v1::node_agent_service_client::NodeAgentServiceClient;
use orchid_transport::testing::TestPki;
use orchid_transport::tls::TlsMaterial;
use orchidctl::Cli;
use tempfile::TempDir;
use tokio::task::JoinHandle;

struct Harness {
    server: TestServer,
    dir: TempDir,
    config: PathBuf,
}

impl Harness {
    async fn start() -> Self {
        let server = TestServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("orchidctl.toml");
        std::fs::write(&config, format!("labellum = [\"{}\"]\n", server.url())).unwrap();
        Self {
            server,
            dir,
            config,
        }
    }

    /// Runs orchidctl, returns its output or its error.
    async fn ctl(&self, args: &[&str]) -> Result<String, String> {
        run(&self.config, args).await
    }

    async fn ok(&self, args: &[&str]) -> String {
        self.ctl(args)
            .await
            .unwrap_or_else(|e| panic!("orchidctl {args:?} failed: {e}"))
    }

    fn file(&self, name: &str, content: &str) -> String {
        let path = self.dir.path().join(name);
        std::fs::write(&path, content).unwrap();
        path.display().to_string()
    }

    /// Registers a node and keeps it alive.
    async fn node(&self, name: &str) -> JoinHandle<()> {
        let mut agent = NodeAgentServiceClient::new(self.server.channel(None));
        agent
            .register(pb::RegisterRequest {
                node: name.into(),
                role: pb::NodeRole::Worker.into(),
                schedulable: None,
                runtimes: vec!["io.containerd.runc.v2".into()],
                pod_cidr: "10.244.0.0/24".into(),
                capacity: Some(pb::Resources {
                    cpu_millis: 4000,
                    memory_bytes: 8 << 30,
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
}

async fn run(config: &Path, args: &[&str]) -> Result<String, String> {
    let config = config.display().to_string();
    let mut argv = vec!["orchidctl", "--config", config.as_str()];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    orchidctl::run(cli, &mut out)
        .await
        .map_err(|e| e.to_string())?;
    Ok(String::from_utf8(out).unwrap())
}

#[tokio::test]
async fn runs_gets_describes_and_deletes_pods() {
    let harness = Harness::start().await;
    assert_eq!(harness.ok(&["get", "pods"]).await, "No pods found.\n");

    let output = harness
        .ok(&[
            "run",
            "web",
            "--image",
            "nginx:alpine",
            "--cpu",
            "250m",
            "--memory",
            "128Mi",
            "--priority",
            "-2",
        ])
        .await;
    assert_eq!(output, "pod/web created\n");

    let table = harness.ok(&["get", "pods"]).await;
    let mut lines = table.lines();
    assert!(
        lines
            .next()
            .unwrap()
            .starts_with("NAME   READY   PHASE     RESTARTS   NODE     AGE")
    );
    let row = lines.next().unwrap();
    assert!(
        row.starts_with("web    0/1     Pending   0          <none>"),
        "{row}"
    );

    let wide = harness.ok(&["get", "po", "web", "-o", "wide"]).await;
    assert!(
        wide.contains("250m") && wide.contains("128Mi") && wide.contains("io.containerd.runc.v2"),
        "{wide}"
    );
    assert!(wide.contains("-2"), "{wide}");

    assert_eq!(
        harness.ok(&["get", "pods", "-o", "name"]).await,
        "pod/web\n"
    );
    let json: serde_json::Value =
        serde_json::from_str(&harness.ok(&["get", "pod", "web", "-o", "json"]).await).unwrap();
    assert_eq!(json["spec"]["container"][0]["image"], "nginx:alpine");

    let description = harness.ok(&["describe", "pod", "web"]).await;
    for expected in [
        "Name:             web",
        "Phase:            Pending",
        "Containers:",
        "  web:",
        "Image:",
    ] {
        assert!(
            description.contains(expected),
            "{expected:?} not in:\n{description}"
        );
    }

    // The TOML output can be used to create the pod again.
    let toml = harness.ok(&["get", "pod", "web", "-o", "toml"]).await;
    let file = harness.file("web.toml", &toml);
    assert_eq!(
        harness.ok(&["delete", "pod", "web"]).await,
        "pod/web deleted\n"
    );
    assert_eq!(harness.ok(&["get", "pods"]).await, "No pods found.\n");
    assert_eq!(
        harness.ok(&["create", "-f", &file]).await,
        "pod/web created\n"
    );
    assert_eq!(
        harness.ok(&["delete", "-f", &file]).await,
        "pod/web deleted\n"
    );
}

#[tokio::test]
async fn creates_and_applies_pod_files() {
    let harness = Harness::start().await;
    let example = concat!(env!("CARGO_MANIFEST_DIR"), "/../../resources/pod.toml");
    assert_eq!(
        harness.ok(&["create", "-f", example]).await,
        "pod/my-app created\n"
    );
    let error = harness.ctl(&["create", "-f", example]).await.unwrap_err();
    assert!(error.contains("already exists"), "{error}");
    assert_eq!(
        harness.ok(&["apply", "-f", example]).await,
        "pod/my-app unchanged\n"
    );

    let original = std::fs::read_to_string(example).unwrap();
    let higher = harness.file(
        "higher.toml",
        &original.replace("priority = 0", "priority = 5"),
    );
    assert_eq!(
        harness.ok(&["apply", "-f", &higher]).await,
        "pod/my-app configured\n"
    );
    let json: serde_json::Value =
        serde_json::from_str(&harness.ok(&["get", "pod", "my-app", "-o", "json"]).await).unwrap();
    assert_eq!(json["spec"]["priority"], 5);

    let other_image = harness.file(
        "image.toml",
        &original.replace("my-app:latest", "my-app:v2"),
    );
    let error = harness
        .ctl(&["apply", "-f", &other_image])
        .await
        .unwrap_err();
    assert!(error.contains("--force"), "{error}");
    assert_eq!(
        harness.ok(&["apply", "--force", "-f", &other_image]).await,
        "pod/my-app replaced\n"
    );
    let json: serde_json::Value =
        serde_json::from_str(&harness.ok(&["get", "pod", "my-app", "-o", "json"]).await).unwrap();
    assert_eq!(
        json["spec"]["container"][0]["image"],
        "ghcr.io/acme/my-app:v2"
    );

    let invalid = harness.file("invalid.toml", "name = \"x\"\n[spec]\ncontainer = []\n");
    let error = harness.ctl(&["create", "-f", &invalid]).await.unwrap_err();
    assert!(error.contains("at least one container"), "{error}");
}

#[tokio::test]
async fn manages_nodes() {
    let harness = Harness::start().await;
    let _heartbeats = harness.node("phoenix").await;

    let table = harness.ok(&["get", "nodes"]).await;
    assert!(
        table.contains("phoenix   Ready    worker   0/4 (0%)   0/8Gi (0%)"),
        "{table}"
    );
    harness
        .ok(&[
            "wait",
            "node",
            "phoenix",
            "--for",
            "condition=ready",
            "--timeout",
            "5s",
        ])
        .await;

    assert_eq!(
        harness.ok(&["cordon", "phoenix"]).await,
        "node/phoenix cordoned\n"
    );
    assert!(
        harness
            .ok(&["get", "node", "phoenix"])
            .await
            .contains("Ready,SchedulingDisabled")
    );
    assert_eq!(
        harness.ok(&["uncordon", "phoenix"]).await,
        "node/phoenix uncordoned\n"
    );

    let description = harness.ok(&["describe", "node", "phoenix"]).await;
    for expected in [
        "Name:             phoenix",
        "Pod CIDR:         10.244.0.0/24",
        "Resources:",
        "Pods (0):",
    ] {
        assert!(
            description.contains(expected),
            "{expected:?} not in:\n{description}"
        );
    }

    let output = harness.ok(&["drain", "phoenix", "--timeout", "30s"]).await;
    assert!(
        output.starts_with("node/phoenix draining\n") && output.ends_with("node/phoenix drained\n"),
        "{output}"
    );
    assert!(
        harness
            .ok(&["get", "node", "phoenix"])
            .await
            .contains("SchedulingDisabled")
    );
}

#[tokio::test]
async fn sets_the_cluster_configuration() {
    let harness = Harness::start().await;
    let table = harness.ok(&["get", "cluster-config"]).await;
    assert!(
        table.contains("default_runtime      io.containerd.runc.v2"),
        "{table}"
    );

    assert_eq!(
        harness
            .ok(&[
                "set",
                "cluster-config",
                "pod_eviction_delay=45s",
                "node_lease_ttl=20s"
            ])
            .await,
        "cluster-config updated\n"
    );
    let toml = harness.ok(&["get", "cluster-config", "-o", "toml"]).await;
    assert!(
        toml.contains("pod_eviction_delay = \"45s\"") && toml.contains("node_lease_ttl = \"20s\""),
        "{toml}"
    );

    let error = harness
        .ctl(&["set", "cluster-config", "unknown=1"])
        .await
        .unwrap_err();
    assert!(error.contains("unknown key"), "{error}");
    let error = harness
        .ctl(&[
            "set",
            "cluster-config",
            "default_runtime=io.containerd.kata.v2",
        ])
        .await;
    assert!(error.is_ok(), "no node lacks the runtime yet: {error:?}");
}

#[tokio::test]
async fn reports_errors() {
    let harness = Harness::start().await;
    harness.ok(&["run", "web", "--image", "nginx"]).await;

    let error = harness
        .ctl(&["get", "pods", "web", "nope"])
        .await
        .unwrap_err();
    assert_eq!(error, "pods not found: nope");
    let error = harness
        .ctl(&["describe", "node", "nope"])
        .await
        .unwrap_err();
    assert_eq!(error, "node nope not found");
    let error = harness.ctl(&["get", "things"]).await.unwrap_err();
    assert!(error.contains("unknown resource type"), "{error}");
    let error = harness
        .ctl(&[
            "wait",
            "pod",
            "web",
            "--for",
            "phase=running",
            "--timeout",
            "500ms",
        ])
        .await
        .unwrap_err();
    assert!(error.contains("timed out"), "{error}");
    let error = harness
        .ctl(&["run", "other", "--image", "nginx", "--cpu", "lots"])
        .await
        .unwrap_err();
    assert!(error.contains("--cpu"), "{error}");
}

#[tokio::test]
async fn authenticates_with_the_configured_certificate() {
    let pki = TestPki::new();
    let server = TestServer::start_with(fast_config(), Some(&pki.issue("labellum"))).await;
    let dir = tempfile::tempdir().unwrap();
    let config_for = |name: &str, tls: &TlsMaterial| {
        let write = |file: &str, content: &[u8]| {
            let path = dir.path().join(format!("{name}-{file}"));
            std::fs::write(&path, content).unwrap();
            path.display().to_string()
        };
        let config = dir.path().join(format!("{name}.toml"));
        std::fs::write(
            &config,
            format!(
                "labellum = [\"{}\"]\n[tls]\ncertificate = \"{}\"\nprivate_key = \"{}\"\nca = \"{}\"\n",
                server.url(),
                write("cert.pem", &tls.certificate),
                write("key.pem", &tls.private_key),
                write("ca.pem", &tls.ca),
            ),
        )
        .unwrap();
        config
    };
    let admin = config_for("admin", &pki.issue("user:admin"));
    let agent = config_for("agent", &pki.issue("keiki:phoenix"));

    assert_eq!(
        run(&admin, &["run", "web", "--image", "nginx"])
            .await
            .unwrap(),
        "pod/web created\n"
    );
    let error = run(&agent, &["run", "other", "--image", "nginx"])
        .await
        .unwrap_err();
    assert!(error.starts_with("permission denied"), "{error}");
}
