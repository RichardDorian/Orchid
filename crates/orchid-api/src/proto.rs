//! Conversions between the domain types and the protobuf types.
//!
//! Domain to protobuf conversions are infallible ([`From`]).
//! Protobuf to domain conversions ([`TryFrom`]) only check the structure of the
//! message (required fields, known enum values, parsable CIDRs...): objects
//! submitted by users must still be validated (e.g. [`PodSpec::validate`]).

use std::time::Duration;

use jiff::Timestamp;
use orchid_proto::prost_types;
use orchid_proto::v1 as pb;

use crate::{
    Bytes, ClusterConfig, Container, ContainerState, ContainerStatus, MilliCpu, Node,
    NodeCondition, NodeHealth, NodeInfo, NodeRole, NodeSpec, NodeStatus, Pod, PodBinding, PodPhase,
    PodSpec, PodStatus, Resources, RestartPolicy, Revision, ValidationError,
};

type Result<T> = std::result::Result<T, ValidationError>;

// ---------------------------------------------------------------------------
// Scalars
// ---------------------------------------------------------------------------

pub fn timestamp_to_proto(timestamp: Timestamp) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: timestamp.as_second(),
        nanos: timestamp.subsec_nanosecond(),
    }
}

pub fn timestamp_from_proto(timestamp: prost_types::Timestamp) -> Result<Timestamp> {
    Timestamp::new(timestamp.seconds, timestamp.nanos)
        .map_err(|e| ValidationError::new("", format!("invalid timestamp: {e}")))
}

pub fn duration_to_proto(duration: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        // Always lower than 1e9.
        nanos: duration.subsec_nanos() as i32,
    }
}

pub fn duration_from_proto(duration: prost_types::Duration) -> Result<Duration> {
    Duration::try_from(duration)
        .map_err(|e| ValidationError::new("", format!("invalid duration: {e}")))
}

fn required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.ok_or_else(|| ValidationError::new(field, "is required"))
}

fn at(field: &str) -> impl FnOnce(ValidationError) -> ValidationError + '_ {
    move |e: ValidationError| e.prefixed(field)
}

fn required_timestamp(value: Option<prost_types::Timestamp>, field: &str) -> Result<Timestamp> {
    timestamp_from_proto(required(value, field)?).map_err(at(field))
}

fn optional_timestamp(
    value: Option<prost_types::Timestamp>,
    field: &str,
) -> Result<Option<Timestamp>> {
    value
        .map(timestamp_from_proto)
        .transpose()
        .map_err(at(field))
}

fn unknown_enum(field: &str, value: i32) -> ValidationError {
    ValidationError::new(field, format!("unknown value {value}"))
}

fn unspecified(field: &str) -> ValidationError {
    ValidationError::new(field, "must be specified")
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

impl From<Resources> for pb::Resources {
    fn from(resources: Resources) -> Self {
        Self {
            cpu_millis: resources.cpu.0,
            memory_bytes: resources.memory.0,
        }
    }
}

impl From<pb::Resources> for Resources {
    fn from(resources: pb::Resources) -> Self {
        Self {
            cpu: MilliCpu(resources.cpu_millis),
            memory: Bytes(resources.memory_bytes),
        }
    }
}

/// Missing resources are treated as zero, validation rejects them where needed.
fn resources_from_proto(resources: Option<pb::Resources>) -> Resources {
    resources.map(Resources::from).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Pods
// ---------------------------------------------------------------------------

impl From<RestartPolicy> for pb::RestartPolicy {
    fn from(policy: RestartPolicy) -> Self {
        match policy {
            RestartPolicy::Always => Self::Always,
            RestartPolicy::Failure => Self::Failure,
            RestartPolicy::Never => Self::Never,
        }
    }
}

/// `UNSPECIFIED` means `ALWAYS`.
pub fn restart_policy_from_proto(value: i32) -> Result<RestartPolicy> {
    match pb::RestartPolicy::try_from(value) {
        Ok(pb::RestartPolicy::Unspecified | pb::RestartPolicy::Always) => Ok(RestartPolicy::Always),
        Ok(pb::RestartPolicy::Failure) => Ok(RestartPolicy::Failure),
        Ok(pb::RestartPolicy::Never) => Ok(RestartPolicy::Never),
        Err(_) => Err(unknown_enum("restart_policy", value)),
    }
}

impl From<PodPhase> for pb::PodPhase {
    fn from(phase: PodPhase) -> Self {
        match phase {
            PodPhase::Pending => Self::Pending,
            PodPhase::Creating => Self::Creating,
            PodPhase::Running => Self::Running,
            PodPhase::Succeeded => Self::Succeeded,
            PodPhase::Failed => Self::Failed,
            PodPhase::Terminating => Self::Terminating,
        }
    }
}

pub fn pod_phase_from_proto(value: i32) -> Result<PodPhase> {
    match pb::PodPhase::try_from(value) {
        Ok(pb::PodPhase::Unspecified) => Err(unspecified("phase")),
        Ok(pb::PodPhase::Pending) => Ok(PodPhase::Pending),
        Ok(pb::PodPhase::Creating) => Ok(PodPhase::Creating),
        Ok(pb::PodPhase::Running) => Ok(PodPhase::Running),
        Ok(pb::PodPhase::Succeeded) => Ok(PodPhase::Succeeded),
        Ok(pb::PodPhase::Failed) => Ok(PodPhase::Failed),
        Ok(pb::PodPhase::Terminating) => Ok(PodPhase::Terminating),
        Err(_) => Err(unknown_enum("phase", value)),
    }
}

impl From<ContainerState> for pb::ContainerState {
    fn from(state: ContainerState) -> Self {
        match state {
            ContainerState::Waiting => Self::Waiting,
            ContainerState::Running => Self::Running,
            ContainerState::Exited => Self::Exited,
        }
    }
}

pub fn container_state_from_proto(value: i32) -> Result<ContainerState> {
    match pb::ContainerState::try_from(value) {
        Ok(pb::ContainerState::Unspecified) => Err(unspecified("state")),
        Ok(pb::ContainerState::Waiting) => Ok(ContainerState::Waiting),
        Ok(pb::ContainerState::Running) => Ok(ContainerState::Running),
        Ok(pb::ContainerState::Exited) => Ok(ContainerState::Exited),
        Err(_) => Err(unknown_enum("state", value)),
    }
}

impl From<Container> for pb::Container {
    fn from(container: Container) -> Self {
        Self {
            name: container.name,
            image: container.image,
            resources: Some(container.resources.into()),
        }
    }
}

impl From<pb::Container> for Container {
    fn from(container: pb::Container) -> Self {
        Self {
            name: container.name,
            image: container.image,
            resources: resources_from_proto(container.resources),
        }
    }
}

impl From<PodSpec> for pb::PodSpec {
    fn from(spec: PodSpec) -> Self {
        Self {
            runtime: spec.runtime,
            priority: spec.priority,
            restart_policy: pb::RestartPolicy::from(spec.restart_policy).into(),
            termination_grace_period: Some(duration_to_proto(spec.termination_grace_period)),
            containers: spec.containers.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<pb::PodSpec> for PodSpec {
    type Error = ValidationError;

    /// A missing termination grace period means the default one.
    fn try_from(spec: pb::PodSpec) -> Result<Self> {
        let termination_grace_period = match spec.termination_grace_period {
            Some(duration) => {
                duration_from_proto(duration).map_err(at("termination_grace_period"))?
            }
            None => crate::DEFAULT_TERMINATION_GRACE_PERIOD,
        };
        Ok(Self {
            runtime: spec.runtime,
            priority: spec.priority,
            restart_policy: restart_policy_from_proto(spec.restart_policy)?,
            termination_grace_period,
            containers: spec.containers.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<PodBinding> for pb::PodBinding {
    fn from(binding: PodBinding) -> Self {
        Self {
            node: binding.node,
            attempt: binding.attempt,
        }
    }
}

impl From<pb::PodBinding> for PodBinding {
    fn from(binding: pb::PodBinding) -> Self {
        Self {
            node: binding.node,
            attempt: binding.attempt,
        }
    }
}

impl From<ContainerStatus> for pb::ContainerStatus {
    fn from(status: ContainerStatus) -> Self {
        Self {
            name: status.name,
            state: pb::ContainerState::from(status.state).into(),
            restart_count: status.restart_count,
            started_at: status.started_at.map(timestamp_to_proto),
            exit_code: status.exit_code,
        }
    }
}

impl TryFrom<pb::ContainerStatus> for ContainerStatus {
    type Error = ValidationError;

    fn try_from(status: pb::ContainerStatus) -> Result<Self> {
        Ok(Self {
            name: status.name,
            state: container_state_from_proto(status.state)?,
            restart_count: status.restart_count,
            started_at: optional_timestamp(status.started_at, "started_at")?,
            exit_code: status.exit_code,
        })
    }
}

/// Converts container statuses, prefixing errors with `containers[i]`.
pub fn container_statuses_from_proto(
    statuses: Vec<pb::ContainerStatus>,
) -> Result<Vec<ContainerStatus>> {
    statuses
        .into_iter()
        .enumerate()
        .map(|(i, status)| {
            ContainerStatus::try_from(status)
                .map_err(|e| e.prefixed(&format!("[{i}]")).prefixed("containers"))
        })
        .collect()
}

impl From<PodStatus> for pb::PodStatus {
    fn from(status: PodStatus) -> Self {
        Self {
            phase: pb::PodPhase::from(status.phase).into(),
            reason: status.reason,
            message: status.message,
            pending_since: Some(timestamp_to_proto(status.pending_since)),
            deletion_requested_at: status.deletion_requested_at.map(timestamp_to_proto),
            containers: status.containers.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<pb::PodStatus> for PodStatus {
    type Error = ValidationError;

    fn try_from(status: pb::PodStatus) -> Result<Self> {
        Ok(Self {
            phase: pod_phase_from_proto(status.phase)?,
            reason: status.reason,
            message: status.message,
            pending_since: required_timestamp(status.pending_since, "pending_since")?,
            deletion_requested_at: optional_timestamp(
                status.deletion_requested_at,
                "deletion_requested_at",
            )?,
            containers: container_statuses_from_proto(status.containers)?,
        })
    }
}

impl From<Pod> for pb::Pod {
    fn from(pod: Pod) -> Self {
        Self {
            name: pod.name,
            uid: pod.uid,
            revision: pod.revision,
            created_at: Some(timestamp_to_proto(pod.created_at)),
            spec: Some(pod.spec.into()),
            binding: pod.binding.map(Into::into),
            status: Some(pod.status.into()),
        }
    }
}

impl TryFrom<pb::Pod> for Pod {
    type Error = ValidationError;

    fn try_from(pod: pb::Pod) -> Result<Self> {
        Ok(Self {
            name: pod.name,
            uid: pod.uid,
            revision: pod.revision,
            created_at: required_timestamp(pod.created_at, "created_at")?,
            spec: PodSpec::try_from(required(pod.spec, "spec")?).map_err(at("spec"))?,
            binding: pod.binding.map(Into::into),
            status: PodStatus::try_from(required(pod.status, "status")?).map_err(at("status"))?,
        })
    }
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

impl From<NodeRole> for pb::NodeRole {
    fn from(role: NodeRole) -> Self {
        match role {
            NodeRole::Worker => Self::Worker,
            NodeRole::ControlPlane => Self::ControlPlane,
        }
    }
}

pub fn node_role_from_proto(value: i32) -> Result<NodeRole> {
    match pb::NodeRole::try_from(value) {
        Ok(pb::NodeRole::Unspecified) => Err(unspecified("role")),
        Ok(pb::NodeRole::Worker) => Ok(NodeRole::Worker),
        Ok(pb::NodeRole::ControlPlane) => Ok(NodeRole::ControlPlane),
        Err(_) => Err(unknown_enum("role", value)),
    }
}

impl From<NodeCondition> for pb::NodeCondition {
    fn from(condition: NodeCondition) -> Self {
        match condition {
            NodeCondition::Ready => Self::Ready,
            NodeCondition::Unhealthy => Self::Unhealthy,
            NodeCondition::Unreachable => Self::Unreachable,
        }
    }
}

pub fn node_condition_from_proto(value: i32) -> Result<NodeCondition> {
    match pb::NodeCondition::try_from(value) {
        Ok(pb::NodeCondition::Unspecified) => Err(unspecified("condition")),
        Ok(pb::NodeCondition::Ready) => Ok(NodeCondition::Ready),
        Ok(pb::NodeCondition::Unhealthy) => Ok(NodeCondition::Unhealthy),
        Ok(pb::NodeCondition::Unreachable) => Ok(NodeCondition::Unreachable),
        Err(_) => Err(unknown_enum("condition", value)),
    }
}

impl From<NodeHealth> for pb::NodeHealth {
    fn from(health: NodeHealth) -> Self {
        match health {
            NodeHealth::Healthy => Self::Healthy,
            NodeHealth::Unhealthy => Self::Unhealthy,
        }
    }
}

pub fn node_health_from_proto(value: i32) -> Result<NodeHealth> {
    match pb::NodeHealth::try_from(value) {
        Ok(pb::NodeHealth::Unspecified) => Err(unspecified("health")),
        Ok(pb::NodeHealth::Healthy) => Ok(NodeHealth::Healthy),
        Ok(pb::NodeHealth::Unhealthy) => Ok(NodeHealth::Unhealthy),
        Err(_) => Err(unknown_enum("health", value)),
    }
}

impl From<NodeSpec> for pb::NodeSpec {
    fn from(spec: NodeSpec) -> Self {
        Self {
            schedulable: spec.schedulable,
            draining: spec.draining,
        }
    }
}

impl From<pb::NodeSpec> for NodeSpec {
    fn from(spec: pb::NodeSpec) -> Self {
        Self {
            schedulable: spec.schedulable,
            draining: spec.draining,
        }
    }
}

impl From<NodeInfo> for pb::NodeInfo {
    fn from(info: NodeInfo) -> Self {
        Self {
            role: pb::NodeRole::from(info.role).into(),
            runtimes: info.runtimes,
            pod_cidr: info.pod_cidr.to_string(),
            capacity: Some(info.capacity.into()),
        }
    }
}

impl TryFrom<pb::NodeInfo> for NodeInfo {
    type Error = ValidationError;

    fn try_from(info: pb::NodeInfo) -> Result<Self> {
        Ok(Self {
            role: node_role_from_proto(info.role)?,
            runtimes: info.runtimes,
            pod_cidr: parse_cidr(&info.pod_cidr)?,
            capacity: resources_from_proto(info.capacity),
        })
    }
}

pub fn parse_cidr(cidr: &str) -> Result<ipnet::IpNet> {
    cidr.parse()
        .map_err(|_| ValidationError::new("pod_cidr", format!("invalid CIDR {cidr:?}")))
}

impl From<NodeStatus> for pb::NodeStatus {
    fn from(status: NodeStatus) -> Self {
        Self {
            condition: pb::NodeCondition::from(status.condition).into(),
            message: status.message,
            last_heartbeat: status.last_heartbeat.map(timestamp_to_proto),
            allocated: Some(status.allocated.into()),
            allocated_revision: status.allocated_revision,
            usage: Some(status.usage.into()),
        }
    }
}

impl TryFrom<pb::NodeStatus> for NodeStatus {
    type Error = ValidationError;

    fn try_from(status: pb::NodeStatus) -> Result<Self> {
        Ok(Self {
            condition: node_condition_from_proto(status.condition)?,
            message: status.message,
            last_heartbeat: optional_timestamp(status.last_heartbeat, "last_heartbeat")?,
            allocated: resources_from_proto(status.allocated),
            allocated_revision: status.allocated_revision,
            usage: resources_from_proto(status.usage),
        })
    }
}

impl From<Node> for pb::Node {
    fn from(node: Node) -> Self {
        Self {
            name: node.name,
            uid: node.uid,
            revision: node.revision,
            spec: Some(node.spec.into()),
            info: Some(node.info.into()),
            status: Some(node.status.into()),
        }
    }
}

impl TryFrom<pb::Node> for Node {
    type Error = ValidationError;

    fn try_from(node: pb::Node) -> Result<Self> {
        Ok(Self {
            name: node.name,
            uid: node.uid,
            revision: node.revision,
            spec: required(node.spec, "spec")?.into(),
            info: NodeInfo::try_from(required(node.info, "info")?).map_err(at("info"))?,
            status: NodeStatus::try_from(required(node.status, "status")?).map_err(at("status"))?,
        })
    }
}

// ---------------------------------------------------------------------------
// Cluster configuration
// ---------------------------------------------------------------------------

pub fn cluster_config_to_proto(config: ClusterConfig, revision: Revision) -> pb::ClusterConfig {
    pb::ClusterConfig {
        revision,
        default_runtime: config.default_runtime,
        node_lease_ttl: Some(duration_to_proto(config.node_lease_ttl)),
        pod_eviction_delay: Some(duration_to_proto(config.pod_eviction_delay)),
        leader_lease_ttl: Some(duration_to_proto(config.leader_lease_ttl)),
    }
}

impl TryFrom<pb::ClusterConfig> for ClusterConfig {
    type Error = ValidationError;

    /// The revision is ignored.
    fn try_from(config: pb::ClusterConfig) -> Result<Self> {
        let duration = |value: Option<prost_types::Duration>, field: &str| {
            duration_from_proto(required(value, field)?).map_err(at(field))
        };
        Ok(Self {
            default_runtime: config.default_runtime,
            node_lease_ttl: duration(config.node_lease_ttl, "node_lease_ttl")?,
            pod_eviction_delay: duration(config.pod_eviction_delay, "pod_eviction_delay")?,
            leader_lease_ttl: duration(config.leader_lease_ttl, "leader_lease_ttl")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod() -> Pod {
        let now = Timestamp::new(1_790_000_000, 123).unwrap();
        Pod {
            name: "my-app".into(),
            uid: "01926f3e-8b1c-7c2a-9d4e-3f5a6b7c8d9e".into(),
            revision: 42,
            created_at: now,
            spec: PodSpec {
                runtime: "io.containerd.runsc.v1".into(),
                priority: -3,
                restart_policy: RestartPolicy::Failure,
                termination_grace_period: Duration::from_millis(1_500),
                containers: vec![Container {
                    name: "app".into(),
                    image: "nginx".into(),
                    resources: Resources::new(MilliCpu(300), Bytes(500 << 20)),
                }],
            },
            binding: Some(PodBinding {
                node: "phoenix".into(),
                attempt: 2,
            }),
            status: PodStatus {
                phase: PodPhase::Running,
                reason: "Started".into(),
                message: String::new(),
                pending_since: now,
                deletion_requested_at: Some(now),
                containers: vec![ContainerStatus {
                    name: "app".into(),
                    state: ContainerState::Exited,
                    restart_count: 3,
                    started_at: Some(now),
                    exit_code: Some(137),
                }],
            },
        }
    }

    fn node() -> Node {
        Node {
            name: "phoenix".into(),
            uid: "01926f3a-11d2-7f40-8a3b-5c6d7e8f9a0b".into(),
            revision: 7,
            spec: NodeSpec {
                schedulable: true,
                draining: true,
            },
            info: NodeInfo {
                role: NodeRole::ControlPlane,
                runtimes: vec!["io.containerd.runc.v2".into()],
                pod_cidr: "10.244.0.0/24".parse().unwrap(),
                capacity: Resources::new(MilliCpu(12_000), Bytes(16 << 30)),
            },
            status: NodeStatus {
                condition: NodeCondition::Unhealthy,
                message: "containerd unreachable".into(),
                last_heartbeat: Some(Timestamp::new(1_790_000_000, 0).unwrap()),
                allocated: Resources::new(MilliCpu(300), Bytes(500 << 20)),
                allocated_revision: 6,
                usage: Resources::new(MilliCpu(120), Bytes(310 << 20)),
            },
        }
    }

    #[test]
    fn pod_round_trips() {
        let pod = pod();
        assert_eq!(Pod::try_from(pb::Pod::from(pod.clone())), Ok(pod));
    }

    #[test]
    fn node_round_trips() {
        let node = node();
        assert_eq!(Node::try_from(pb::Node::from(node.clone())), Ok(node));
    }

    #[test]
    fn cluster_config_round_trips() {
        let config = ClusterConfig::default();
        let proto = cluster_config_to_proto(config.clone(), 12);
        assert_eq!(proto.revision, 12);
        assert_eq!(ClusterConfig::try_from(proto), Ok(config));
    }

    #[test]
    fn spec_defaults() {
        let spec = PodSpec::try_from(pb::PodSpec::default()).unwrap();
        assert_eq!(spec.restart_policy, RestartPolicy::Always);
        assert_eq!(
            spec.termination_grace_period,
            crate::DEFAULT_TERMINATION_GRACE_PERIOD
        );
    }

    #[test]
    fn reports_path_of_invalid_fields() {
        let mut proto = pb::Pod::from(pod());
        proto.status.as_mut().unwrap().containers[0].state = 42;
        assert_eq!(
            Pod::try_from(proto).unwrap_err().field,
            "status.containers[0].state"
        );

        let mut proto = pb::Pod::from(pod());
        proto.spec = None;
        assert_eq!(Pod::try_from(proto).unwrap_err().field, "spec");

        let mut proto = pb::Node::from(node());
        proto.info.as_mut().unwrap().pod_cidr = "nope".into();
        assert_eq!(Node::try_from(proto).unwrap_err().field, "info.pod_cidr");
    }

    #[test]
    fn rejects_unspecified_phase() {
        let mut proto = pb::Pod::from(pod());
        proto.status.as_mut().unwrap().phase = pb::PodPhase::Unspecified.into();
        assert_eq!(Pod::try_from(proto).unwrap_err().field, "status.phase");
    }

    #[test]
    fn rejects_negative_durations() {
        let spec = pb::PodSpec {
            termination_grace_period: Some(prost_types::Duration {
                seconds: -1,
                nanos: 0,
            }),
            ..Default::default()
        };
        assert_eq!(
            PodSpec::try_from(spec).unwrap_err().field,
            "termination_grace_period"
        );
    }
}
