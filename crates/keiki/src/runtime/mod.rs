//! Container runtimes.
//!
//! [`Runtime`] is what the agent needs from a runtime: create the sandbox and
//! the containers of a pod, start them, report their state and remove them.
//! [`containerd::ContainerdRuntime`] is the real implementation,
//! [`fake::FakeRuntime`] an in-memory one for tests.

pub mod containerd;
pub mod fake;

use std::future::Future;
use std::time::Duration;

use orchid_api::{Pod, Timestamp};

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("image {image}: {message}")]
    Image { image: String, message: String },
    #[error("{0}")]
    Invalid(String),
    #[error("pod {0} not found in the runtime")]
    NotFound(String),
    #[error("runtime unavailable: {0}")]
    Unavailable(String),
    #[error("{0}")]
    Failed(String),
}

impl From<tonic::Status> for RuntimeError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::Unavailable => Self::Unavailable(status.message().to_owned()),
            _ => Self::Failed(format!("{}: {}", status.code(), status.message())),
        }
    }
}

/// A pod in the runtime, identified by its uid and binding attempt.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PodRef {
    pub uid: String,
    pub attempt: u32,
    pub name: String,
}

impl PodRef {
    /// The pod as bound to this node. `None` if it is not bound.
    pub fn of(pod: &Pod) -> Option<Self> {
        let binding = pod.binding.as_ref()?;
        binding.node()?;
        Some(Self {
            uid: pod.uid.clone(),
            attempt: binding.attempt,
            name: pod.name.clone(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContainerState {
    /// Created, never started or waiting to be restarted.
    Created,
    Running,
    Exited {
        code: i32,
        at: Option<Timestamp>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerInfo {
    pub name: String,
    pub state: ContainerState,
}

pub trait Runtime: Send + Sync + 'static {
    /// Fails if the runtime cannot be used.
    fn health(&self) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    /// Every pod created by Keiki.
    fn list_pods(&self) -> impl Future<Output = Result<Vec<PodRef>, RuntimeError>> + Send;

    /// Pulls the images and creates the sandbox and the containers of the pod,
    /// without starting the containers. A partially created pod must be removed
    /// before trying again.
    fn create_pod(
        &self,
        pod: &Pod,
        attempt: u32,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    /// Starts a created container, or restarts an exited one.
    fn start_container(
        &self,
        pod: &PodRef,
        container: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    /// State of the containers of the pod.
    fn pod_status(
        &self,
        pod: &PodRef,
    ) -> impl Future<Output = Result<Vec<ContainerInfo>, RuntimeError>> + Send;

    /// Stops the containers (SIGTERM, then SIGKILL after `grace`) and removes
    /// everything created for the pod. Succeeds if the pod doesn't exist.
    fn remove_pod(
        &self,
        pod: &PodRef,
        grace: Duration,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;
}
