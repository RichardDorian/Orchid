//! Runtime backed by containerd.
//!
//! Every pod gets a sandbox container running the pause image, which owns the
//! network, IPC and UTS namespaces of the pod. The containers of the pod join
//! them. Everything lives in a dedicated containerd namespace, and containers
//! carry labels identifying their pod, so Keiki can find them after a restart.
//!
//! Images are pulled with the transfer service and unpacked for the local
//! platform. An image already present is not pulled again.

mod image;
mod spec;

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::time::Duration;

use containerd_client::services::v1::container::Runtime as ContainerRuntime;
use containerd_client::services::v1::containers_client::ContainersClient;
use containerd_client::services::v1::content_client::ContentClient;
use containerd_client::services::v1::images_client::ImagesClient;
use containerd_client::services::v1::snapshots::snapshots_client::SnapshotsClient;
use containerd_client::services::v1::snapshots::{
    MountsRequest, PrepareSnapshotRequest, RemoveSnapshotRequest,
};
use containerd_client::services::v1::tasks_client::TasksClient;
use containerd_client::services::v1::transfer_client::TransferClient;
use containerd_client::services::v1::version_client::VersionClient;
use containerd_client::services::v1::{
    Container, CreateContainerRequest, CreateTaskRequest, DeleteContainerRequest,
    DeleteTaskRequest, GetImageRequest, GetRequest, KillRequest, ListContainersRequest,
    ReadContentRequest, StartRequest, TransferRequest,
};
use containerd_client::to_any;
use containerd_client::types::Platform;
use containerd_client::types::transfer::{ImageStore, OciRegistry, UnpackConfiguration};
use containerd_client::types::v1::Status as TaskStatus;
use orchid_api::{Pod, Resources, Timestamp};
use orchid_transport::client::Connection;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tonic::{Code, Request};
use tracing::{debug, info};

use self::image::{ImageConfig, Index, Manifest};
use self::spec::{ContainerSpec, Role};
use super::{ContainerInfo, ContainerState, PodRef, Runtime, RuntimeError};

const LABEL_UID: &str = "io.orchid.pod.uid";
const LABEL_ATTEMPT: &str = "io.orchid.pod.attempt";
const LABEL_POD: &str = "io.orchid.pod.name";
const LABEL_CONTAINER: &str = "io.orchid.container";
/// Container name of the sandbox. Not a valid container name for users.
const SANDBOX: &str = "_sandbox";
const SPEC_TYPE_URL: &str = "types.containerd.io/opencontainers/runtime-spec/1/Spec";
const SIGTERM: u32 = 15;
const SIGKILL: u32 = 9;
/// How long to wait for processes to die after SIGKILL.
const KILL_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct ContainerdOptions {
    /// Path of the containerd socket.
    pub socket: PathBuf,
    /// containerd namespace of the Orchid containers.
    pub namespace: String,
    pub snapshotter: String,
    /// Image of the sandbox containers.
    pub pause_image: String,
    /// Directory of the container logs.
    pub log_dir: PathBuf,
}

impl ContainerdOptions {
    pub fn new(socket: PathBuf) -> Self {
        Self {
            socket,
            namespace: "orchid".to_owned(),
            snapshotter: "overlayfs".to_owned(),
            pause_image: "registry.k8s.io/pause:3.10".to_owned(),
            log_dir: PathBuf::from("/var/log/orchid"),
        }
    }
}

#[derive(Clone)]
pub struct ContainerdRuntime {
    channel: Connection,
    options: ContainerdOptions,
}

impl ContainerdRuntime {
    /// Creates the runtime. The connection to containerd is established lazily.
    pub fn new(options: ContainerdOptions) -> Result<Self, RuntimeError> {
        let url = orchid_transport::url::ServerUrl::Unix(options.socket.clone());
        let channel = orchid_transport::client::connect(&[url], None)
            .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
        Ok(Self { channel, options })
    }

    /// A request in the Orchid namespace.
    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        if let Ok(namespace) = self.options.namespace.parse() {
            request
                .metadata_mut()
                .insert("containerd-namespace", namespace);
        }
        request
    }

    fn platform() -> Platform {
        Platform {
            os: "linux".to_owned(),
            architecture: image::architecture().to_owned(),
            ..Default::default()
        }
    }

    /// Pulls and unpacks the image if it is not present. Returns its name.
    async fn ensure_image(&self, reference: &str) -> Result<String, RuntimeError> {
        let name = image::normalize(reference);
        let image_error = |message: String| RuntimeError::Image {
            image: name.clone(),
            message,
        };
        let existing = ImagesClient::new(self.channel.clone())
            .get(self.request(GetImageRequest { name: name.clone() }))
            .await;
        match existing {
            Ok(_) => return Ok(name),
            Err(status) if status.code() == Code::NotFound => {}
            Err(status) => return Err(status.into()),
        }

        info!(image = name, "pulling image");
        let source = OciRegistry {
            reference: name.clone(),
            resolver: None,
        };
        let destination = ImageStore {
            name: name.clone(),
            platforms: vec![Self::platform()],
            unpacks: vec![UnpackConfiguration {
                platform: Some(Self::platform()),
                snapshotter: self.options.snapshotter.clone(),
            }],
            ..Default::default()
        };
        TransferClient::new(self.channel.clone())
            .transfer(self.request(TransferRequest {
                source: Some(to_any(&source)),
                destination: Some(to_any(&destination)),
                options: None,
            }))
            .await
            .map_err(|status| image_error(status.message().to_owned()))?;
        Ok(name)
    }

    async fn read_blob(&self, digest: &str) -> Result<Vec<u8>, RuntimeError> {
        let mut stream = ContentClient::new(self.channel.clone())
            .read(self.request(ReadContentRequest {
                digest: digest.to_owned(),
                offset: 0,
                size: 0,
            }))
            .await?
            .into_inner();
        let mut data = Vec::new();
        while let Some(chunk) = stream.message().await? {
            data.extend(chunk.data);
        }
        Ok(data)
    }

    async fn read_json<T: DeserializeOwned>(
        &self,
        image: &str,
        digest: &str,
    ) -> Result<T, RuntimeError> {
        let data = self.read_blob(digest).await?;
        serde_json::from_slice(&data).map_err(|e| RuntimeError::Image {
            image: image.to_owned(),
            message: format!("invalid blob {digest}: {e}"),
        })
    }

    /// The configuration of a pulled image, for this platform.
    async fn image_config(&self, name: &str) -> Result<ImageConfig, RuntimeError> {
        let image = ImagesClient::new(self.channel.clone())
            .get(self.request(GetImageRequest {
                name: name.to_owned(),
            }))
            .await?
            .into_inner()
            .image
            .and_then(|image| image.target)
            .ok_or_else(|| RuntimeError::Image {
                image: name.to_owned(),
                message: "image without target".to_owned(),
            })?;

        let mut manifest_digest = image.digest;
        if image::INDEX_MEDIA_TYPES.contains(&image.media_type.as_str()) {
            let index: Index = self.read_json(name, &manifest_digest).await?;
            manifest_digest = image::select_manifest(&index)
                .ok_or_else(|| RuntimeError::Image {
                    image: name.to_owned(),
                    message: format!("no manifest for linux/{}", image::architecture()),
                })?
                .digest
                .clone();
        }
        let manifest: Manifest = self.read_json(name, &manifest_digest).await?;
        self.read_json(name, &manifest.config.digest).await
    }

    /// Creates a container and its snapshot. Existing ones are kept.
    async fn create_container(
        &self,
        pod: &Pod,
        attempt: u32,
        container: &str,
        image_reference: &str,
        role: Role,
        resources: Option<Resources>,
    ) -> Result<String, RuntimeError> {
        let image_name = self.ensure_image(image_reference).await?;
        let config = self.image_config(&image_name).await?;
        let chain_id = config.chain_id().ok_or_else(|| RuntimeError::Image {
            image: image_name.clone(),
            message: "image without layers".to_owned(),
        })?;
        let id = container_id(&pod.uid, attempt, container);

        let prepared = SnapshotsClient::new(self.channel.clone())
            .prepare(self.request(PrepareSnapshotRequest {
                snapshotter: self.options.snapshotter.clone(),
                key: id.clone(),
                parent: chain_id,
                labels: HashMap::new(),
            }))
            .await;
        match prepared {
            Ok(_) => {}
            Err(status) if status.code() == Code::AlreadyExists => {}
            Err(status) => return Err(status.into()),
        }

        let spec = spec::build(&ContainerSpec {
            role,
            hostname: &pod.name,
            image: &config,
            resources,
            cgroups_path: cgroups_path(&self.options.namespace, &pod.uid, attempt, container),
        });
        let labels = HashMap::from([
            (LABEL_UID.to_owned(), pod.uid.clone()),
            (LABEL_ATTEMPT.to_owned(), attempt.to_string()),
            (LABEL_POD.to_owned(), pod.name.clone()),
            (LABEL_CONTAINER.to_owned(), container.to_owned()),
        ]);
        let created =
            ContainersClient::new(self.channel.clone())
                .create(self.request(CreateContainerRequest {
                    container:
                        Some(
                            Container {
                                id: id.clone(),
                                labels,
                                image: image_name,
                                runtime: Some(ContainerRuntime {
                                    name: pod.spec.runtime.clone(),
                                    options: None,
                                }),
                                spec:
                                    Some(
                                        prost_types::Any {
                                            type_url: SPEC_TYPE_URL.to_owned(),
                                            value:
                                                serde_json::to_vec(&spec).map_err(|e| {
                                                    RuntimeError::Failed(e.to_string())
                                                })?,
                                        },
                                    ),
                                snapshotter: self.options.snapshotter.clone(),
                                snapshot_key: id.clone(),
                                ..Default::default()
                            },
                        ),
                }))
                .await;
        match created {
            Ok(_) => Ok(id),
            Err(status) if status.code() == Code::AlreadyExists => Ok(id),
            Err(status) => Err(status.into()),
        }
    }

    /// The task of a container, `None` if it has none.
    async fn task(
        &self,
        id: &str,
    ) -> Result<Option<containerd_client::types::v1::Process>, RuntimeError> {
        let response = TasksClient::new(self.channel.clone())
            .get(self.request(GetRequest {
                container_id: id.to_owned(),
                exec_id: String::new(),
            }))
            .await;
        match response {
            Ok(response) => Ok(response.into_inner().process),
            Err(status) if status.code() == Code::NotFound => Ok(None),
            Err(status) => Err(status.into()),
        }
    }

    /// Creates and starts the task of a container. Returns its pid.
    async fn start_task(&self, pod: &PodRef, container: &str) -> Result<u32, RuntimeError> {
        let id = container_id(&pod.uid, pod.attempt, container);
        let mut tasks = TasksClient::new(self.channel.clone());

        match self.task(&id).await? {
            Some(process) if process.status == TaskStatus::Running as i32 => return Ok(process.pid),
            Some(process) if process.status == TaskStatus::Created as i32 => {}
            Some(_) => {
                // Stopped: the task must be deleted before a new one is created.
                ignore_not_found(
                    tasks
                        .delete(self.request(DeleteTaskRequest {
                            container_id: id.clone(),
                        }))
                        .await,
                )?;
                self.create_task(&id, pod, container).await?;
            }
            None => self.create_task(&id, pod, container).await?,
        }
        let response = tasks
            .start(self.request(StartRequest {
                container_id: id.clone(),
                exec_id: String::new(),
            }))
            .await?;
        Ok(response.into_inner().pid)
    }

    async fn create_task(
        &self,
        id: &str,
        pod: &PodRef,
        container: &str,
    ) -> Result<(), RuntimeError> {
        let mounts = SnapshotsClient::new(self.channel.clone())
            .mounts(self.request(MountsRequest {
                snapshotter: self.options.snapshotter.clone(),
                key: id.to_owned(),
            }))
            .await?
            .into_inner()
            .mounts;

        // The sandbox has no output worth keeping.
        let log = if container == SANDBOX {
            String::new()
        } else {
            let directory = self
                .options
                .log_dir
                .join(format!("{}_{}", pod.name, pod.uid));
            tokio::fs::create_dir_all(&directory).await.map_err(|e| {
                RuntimeError::Failed(format!("failed to create {}: {e}", directory.display()))
            })?;
            format!(
                "file://{}",
                directory.join(format!("{container}.log")).display()
            )
        };
        TasksClient::new(self.channel.clone())
            .create(self.request(CreateTaskRequest {
                container_id: id.to_owned(),
                rootfs: mounts,
                stdout: log.clone(),
                stderr: log,
                ..Default::default()
            }))
            .await?;
        Ok(())
    }

    /// Containers of every pod, or of one pod.
    async fn containers(&self, pod: Option<&PodRef>) -> Result<Vec<Container>, RuntimeError> {
        let filter = match pod {
            Some(pod) => format!(
                "labels.\"{LABEL_UID}\"=={},labels.\"{LABEL_ATTEMPT}\"=={}",
                pod.uid, pod.attempt
            ),
            None => format!("labels.\"{LABEL_UID}\""),
        };
        let response = ContainersClient::new(self.channel.clone())
            .list(self.request(ListContainersRequest {
                filters: vec![filter],
            }))
            .await?;
        Ok(response.into_inner().containers)
    }

    async fn signal(&self, id: &str, signal: u32) -> Result<(), RuntimeError> {
        let result = TasksClient::new(self.channel.clone())
            .kill(self.request(KillRequest {
                container_id: id.to_owned(),
                exec_id: String::new(),
                signal,
                all: true,
            }))
            .await;
        match result {
            Ok(_) => Ok(()),
            // No task, or a task that already exited.
            Err(status) if matches!(status.code(), Code::NotFound | Code::FailedPrecondition) => {
                Ok(())
            }
            Err(status) => Err(status.into()),
        }
    }

    /// Waits until no task of `ids` is running, for up to `timeout`.
    async fn wait_stopped(&self, ids: &[String], timeout: Duration) -> Result<bool, RuntimeError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let mut running = false;
            for id in ids {
                if self
                    .task(id)
                    .await?
                    .is_some_and(|p| p.status != TaskStatus::Stopped as i32)
                {
                    running = true;
                    break;
                }
            }
            if !running {
                return Ok(true);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

impl Runtime for ContainerdRuntime {
    async fn health(&self) -> Result<(), RuntimeError> {
        VersionClient::new(self.channel.clone())
            .version(Request::new(()))
            .await
            .map_err(|status| RuntimeError::Unavailable(status.message().to_owned()))?;
        Ok(())
    }

    async fn list_pods(&self) -> Result<Vec<PodRef>, RuntimeError> {
        let pods: BTreeSet<PodRef> = self
            .containers(None)
            .await?
            .iter()
            .filter_map(|container| pod_ref(&container.labels))
            .collect();
        Ok(pods.into_iter().collect())
    }

    async fn create_pod(&self, pod: &Pod, attempt: u32) -> Result<(), RuntimeError> {
        let reference = PodRef {
            uid: pod.uid.clone(),
            attempt,
            name: pod.name.clone(),
        };
        let pause = self.options.pause_image.clone();
        self.create_container(pod, attempt, SANDBOX, &pause, Role::Sandbox, None)
            .await?;
        let sandbox_pid = self.start_task(&reference, SANDBOX).await?;
        debug!(pod = pod.name, sandbox_pid, "sandbox started");

        for container in &pod.spec.containers {
            self.create_container(
                pod,
                attempt,
                &container.name,
                &container.image,
                Role::Container { sandbox_pid },
                Some(container.resources),
            )
            .await?;
        }
        Ok(())
    }

    async fn start_container(&self, pod: &PodRef, container: &str) -> Result<(), RuntimeError> {
        self.start_task(pod, container).await.map(|_| ())
    }

    async fn pod_status(&self, pod: &PodRef) -> Result<Vec<ContainerInfo>, RuntimeError> {
        let containers = self.containers(Some(pod)).await?;
        if containers.is_empty() {
            return Err(RuntimeError::NotFound(pod.name.clone()));
        }
        let mut infos = Vec::new();
        for container in containers {
            let Some(name) = container
                .labels
                .get(LABEL_CONTAINER)
                .filter(|n| *n != SANDBOX)
            else {
                continue;
            };
            let state = match self.task(&container.id).await? {
                None => ContainerState::Created,
                Some(process) => match TaskStatus::try_from(process.status) {
                    Ok(TaskStatus::Running | TaskStatus::Paused | TaskStatus::Pausing) => {
                        ContainerState::Running
                    }
                    Ok(TaskStatus::Stopped) => ContainerState::Exited {
                        code: i32::try_from(process.exit_status).unwrap_or(i32::MAX),
                        at: process
                            .exited_at
                            .and_then(|t| Timestamp::new(t.seconds, t.nanos).ok()),
                    },
                    _ => ContainerState::Created,
                },
            };
            infos.push(ContainerInfo {
                name: name.clone(),
                state,
            });
        }
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(infos)
    }

    async fn remove_pod(&self, pod: &PodRef, grace: Duration) -> Result<(), RuntimeError> {
        let containers = self.containers(Some(pod)).await?;
        // The containers first, the sandbox last.
        let (sandbox, others): (Vec<_>, Vec<_>) = containers
            .into_iter()
            .partition(|c| c.labels.get(LABEL_CONTAINER).is_some_and(|n| n == SANDBOX));

        for group in [others, sandbox] {
            let ids: Vec<String> = group.iter().map(|c| c.id.clone()).collect();
            for id in &ids {
                self.signal(id, SIGTERM).await?;
            }
            if !self.wait_stopped(&ids, grace).await? {
                for id in &ids {
                    self.signal(id, SIGKILL).await?;
                }
                self.wait_stopped(&ids, KILL_TIMEOUT).await?;
            }
            for id in &ids {
                ignore_not_found(
                    TasksClient::new(self.channel.clone())
                        .delete(self.request(DeleteTaskRequest {
                            container_id: id.clone(),
                        }))
                        .await,
                )?;
                ignore_not_found(
                    ContainersClient::new(self.channel.clone())
                        .delete(self.request(DeleteContainerRequest { id: id.clone() }))
                        .await,
                )?;
                ignore_not_found(
                    SnapshotsClient::new(self.channel.clone())
                        .remove(self.request(RemoveSnapshotRequest {
                            snapshotter: self.options.snapshotter.clone(),
                            key: id.clone(),
                        }))
                        .await,
                )?;
            }
        }
        info!(
            pod = pod.name,
            attempt = pod.attempt,
            "pod removed from containerd"
        );
        Ok(())
    }
}

/// The cgroup of a container. Distinct per attempt: the containers of a
/// previous attempt may still exist when the pod runs again on the same host,
/// and per containerd namespace, for agents sharing a host.
fn cgroups_path(namespace: &str, uid: &str, attempt: u32, container: &str) -> String {
    format!("/{namespace}/{uid}/{attempt}/{container}")
}

/// A containerd container ID: short, and unique per pod, attempt and container.
fn container_id(uid: &str, attempt: u32, container: &str) -> String {
    let digest = Sha256::digest(format!("{uid}/{attempt}/{container}"));
    let hex: String = digest.iter().take(20).map(|b| format!("{b:02x}")).collect();
    format!("orchid-{hex}")
}

fn pod_ref(labels: &HashMap<String, String>) -> Option<PodRef> {
    Some(PodRef {
        uid: labels.get(LABEL_UID)?.clone(),
        attempt: labels.get(LABEL_ATTEMPT)?.parse().ok()?,
        name: labels.get(LABEL_POD)?.clone(),
    })
}

fn ignore_not_found<T>(result: Result<T, tonic::Status>) -> Result<(), RuntimeError> {
    match result {
        Ok(_) => Ok(()),
        Err(status) if status.code() == Code::NotFound => Ok(()),
        Err(status) => Err(status.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn container_ids_are_short_and_distinct() {
        let a = container_id("01926f3e-8b1c-7c2a-9d4e-3f5a6b7c8d9e", 1, "app");
        let b = container_id("01926f3e-8b1c-7c2a-9d4e-3f5a6b7c8d9e", 2, "app");
        assert_ne!(a, b);
        assert!(a.len() <= 76, "containerd limit");
        assert!(a.starts_with("orchid-"));
    }

    #[test]
    fn cgroups_are_distinct_per_attempt_and_namespace() {
        assert_eq!(cgroups_path("orchid", "uid", 1, "app"), "/orchid/uid/1/app");
        assert_ne!(
            cgroups_path("orchid", "uid", 1, "app"),
            cgroups_path("orchid", "uid", 2, "app")
        );
        assert_ne!(
            cgroups_path("a", "uid", 1, "app"),
            cgroups_path("b", "uid", 1, "app")
        );
    }

    #[test]
    fn parses_pod_labels() {
        let labels = HashMap::from([
            (LABEL_UID.to_owned(), "uid".to_owned()),
            (LABEL_ATTEMPT.to_owned(), "3".to_owned()),
            (LABEL_POD.to_owned(), "my-app".to_owned()),
        ]);
        assert_eq!(
            pod_ref(&labels),
            Some(PodRef {
                uid: "uid".into(),
                attempt: 3,
                name: "my-app".into()
            })
        );
    }
}
