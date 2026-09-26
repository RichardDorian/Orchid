use std::collections::HashSet;
use std::time::Duration;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::{Resources, Revision, ValidationErrors, validate_name};

/// Default time between SIGTERM and SIGKILL when stopping containers.
pub const DEFAULT_TERMINATION_GRACE_PERIOD: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pod {
    /// Unique in the cluster, DNS label.
    pub name: String,
    /// Assigned by Labellum at creation.
    pub uid: String,
    /// Highest etcd revision of the pod keys.
    pub revision: Revision,
    pub created_at: Timestamp,
    pub spec: PodSpec,
    /// `None` until the pod is scheduled for the first time.
    pub binding: Option<PodBinding>,
    pub status: PodStatus,
}

impl Pod {
    /// The node the pod is bound to, if any.
    pub fn node(&self) -> Option<&str> {
        self.binding.as_ref().and_then(PodBinding::node)
    }
}

/// Written by the user. Immutable after creation, except `priority`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodSpec {
    /// Full containerd runtime name. Empty means the cluster default runtime,
    /// Labellum replaces it at creation.
    #[serde(default)]
    pub runtime: String,
    /// Higher priorities are scheduled first. Can be negative.
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub restart_policy: RestartPolicy,
    #[serde(
        with = "crate::serde_duration",
        default = "default_termination_grace_period"
    )]
    pub termination_grace_period: Duration,
    #[serde(rename = "container")]
    pub containers: Vec<Container>,
}

fn default_termination_grace_period() -> Duration {
    DEFAULT_TERMINATION_GRACE_PERIOD
}

impl PodSpec {
    /// Sum of the resources of every container. `None` on overflow.
    pub fn resources(&self) -> Option<Resources> {
        self.containers
            .iter()
            .try_fold(Resources::ZERO, |total, c| total.checked_add(c.resources))
    }

    /// Validates a spec submitted by a user.
    pub fn validate(&self) -> Result<(), ValidationErrors> {
        let mut errors = ValidationErrors::new();

        if self.runtime.chars().any(char::is_whitespace) {
            errors.push("runtime", "must not contain whitespaces");
        }

        if self.containers.is_empty() {
            errors.push("containers", "at least one container is required");
        }
        let mut names = HashSet::new();
        for (i, container) in self.containers.iter().enumerate() {
            let mut container_errors = container.validate();
            if !names.insert(container.name.as_str()) {
                container_errors.push("name", "duplicate container name");
            }
            errors.extend_prefixed(&format!("containers[{i}]"), container_errors);
        }
        if self.resources().is_none() {
            errors.push("containers", "total resources overflow");
        }

        errors.into_result()
    }
}

/// Applies to each container independently.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicy {
    #[default]
    Always,
    /// Restart if the container exited with a non zero code.
    Failure,
    Never,
}

impl RestartPolicy {
    /// Whether a container that exited with `exit_code` must be restarted.
    pub fn should_restart(self, exit_code: i32) -> bool {
        match self {
            Self::Always => true,
            Self::Failure => exit_code != 0,
            Self::Never => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Container {
    /// Unique in the pod.
    pub name: String,
    /// OCI image reference.
    pub image: String,
    /// Reserved on the node and enforced as limits.
    pub resources: Resources,
}

impl Container {
    fn validate(&self) -> ValidationErrors {
        let mut errors = ValidationErrors::new();
        if let Err(reason) = validate_name(&self.name) {
            errors.push("name", reason);
        }
        if self.image.trim().is_empty() {
            errors.push("image", "must not be empty");
        }
        if self.resources.cpu.is_zero() {
            errors.push("resources.cpu", "must be greater than 0");
        }
        if self.resources.memory.is_zero() {
            errors.push("resources.memory", "must be greater than 0");
        }
        errors
    }
}

/// Never removed once created: rescheduling clears `node` and keeps `attempt`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodBinding {
    /// Empty while the pod is waiting to be rescheduled.
    pub node: String,
    /// Starts at 1, incremented by every bind.
    pub attempt: u32,
}

impl PodBinding {
    /// The node the pod is bound to, `None` if `node` is empty.
    pub fn node(&self) -> Option<&str> {
        (!self.node.is_empty()).then_some(self.node.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodStatus {
    pub phase: PodPhase,
    /// Machine readable reason of the last transition, e.g. `NodeLost`.
    #[serde(default)]
    pub reason: String,
    /// Human readable details.
    #[serde(default)]
    pub message: String,
    /// When the pod last became pending. Orders pods of the same priority.
    pub pending_since: Timestamp,
    #[serde(default)]
    pub deletion_requested_at: Option<Timestamp>,
    #[serde(default, rename = "container")]
    pub containers: Vec<ContainerStatus>,
}

impl PodStatus {
    /// Status of a pod waiting to be scheduled since `now`.
    pub fn pending(now: Timestamp) -> Self {
        Self {
            phase: PodPhase::Pending,
            reason: String::new(),
            message: String::new(),
            pending_since: now,
            deletion_requested_at: None,
            containers: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PodPhase {
    /// Waiting to be scheduled.
    Pending,
    /// Bound to a node, images are being pulled or the sandbox is being created.
    Creating,
    /// The sandbox exists and at least one container is running or restarting.
    Running,
    /// All containers exited with code 0 and none will be restarted.
    Succeeded,
    /// All containers exited, at least one with a non zero code, and none will
    /// be restarted. Also used when the pod is lost with its node.
    Failed,
    /// Deleted or evicted, containers are being stopped.
    Terminating,
}

impl PodPhase {
    /// Whether no container will run anymore.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerStatus {
    pub name: String,
    pub state: ContainerState,
    #[serde(default)]
    pub restart_count: u32,
    #[serde(default)]
    pub started_at: Option<Timestamp>,
    /// Exit code of the last termination, if any.
    #[serde(default)]
    pub exit_code: Option<i32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainerState {
    /// Being created, or waiting for its restart backoff.
    Waiting,
    Running,
    Exited,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Bytes, MilliCpu};

    fn container(name: &str) -> Container {
        Container {
            name: name.to_owned(),
            image: "ghcr.io/acme/my-app:latest".to_owned(),
            resources: Resources::new(MilliCpu(300), Bytes(500 << 20)),
        }
    }

    fn spec(containers: Vec<Container>) -> PodSpec {
        PodSpec {
            runtime: String::new(),
            priority: 0,
            restart_policy: RestartPolicy::Always,
            termination_grace_period: DEFAULT_TERMINATION_GRACE_PERIOD,
            containers,
        }
    }

    #[test]
    fn accepts_valid_spec() {
        assert_eq!(
            spec(vec![container("app"), container("sidecar")]).validate(),
            Ok(())
        );
    }

    #[test]
    fn sums_container_resources() {
        let spec = spec(vec![container("app"), container("sidecar")]);
        assert_eq!(
            spec.resources(),
            Some(Resources::new(MilliCpu(600), Bytes(1000 << 20)))
        );
    }

    #[test]
    fn reports_every_invalid_field() {
        let mut bad = container("app");
        bad.image = String::new();
        bad.resources.cpu = MilliCpu::ZERO;
        let mut spec = spec(vec![container("app"), bad]);
        spec.runtime = "io.containerd runc".to_owned();

        let fields: Vec<String> = spec
            .validate()
            .unwrap_err()
            .into_iter()
            .map(|e| e.field)
            .collect();
        assert_eq!(
            fields,
            [
                "runtime",
                "containers[1].image",
                "containers[1].resources.cpu",
                "containers[1].name",
            ]
        );
    }

    #[test]
    fn requires_a_container() {
        assert!(spec(vec![]).validate().is_err());
    }

    #[test]
    fn binding_with_empty_node_is_unbound() {
        let binding = PodBinding {
            node: String::new(),
            attempt: 2,
        };
        assert_eq!(binding.node(), None);
    }

    #[test]
    fn restart_policy() {
        assert!(RestartPolicy::Always.should_restart(0));
        assert!(RestartPolicy::Failure.should_restart(1));
        assert!(!RestartPolicy::Failure.should_restart(0));
        assert!(!RestartPolicy::Never.should_restart(1));
    }

    #[test]
    fn deserializes_spec_with_defaults() {
        let spec: PodSpec = serde_json::from_str(
            r#"{"container": [{"name": "app", "image": "nginx", "resources": {"cpu": "300m", "memory": "500Mi"}}]}"#,
        )
        .unwrap();
        assert_eq!(spec.restart_policy, RestartPolicy::Always);
        assert_eq!(
            spec.termination_grace_period,
            DEFAULT_TERMINATION_GRACE_PERIOD
        );
        assert_eq!(spec.containers[0].resources.cpu, MilliCpu(300));
    }
}
