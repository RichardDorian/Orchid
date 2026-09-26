use std::collections::HashSet;

use ipnet::IpNet;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::{Resources, Revision, ValidationErrors};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    pub uid: String,
    /// Highest etcd revision of the node keys.
    pub revision: Revision,
    pub spec: NodeSpec,
    pub info: NodeInfo,
    pub status: NodeStatus,
}

/// Written by users (and by the controller at the end of a drain).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSpec {
    /// `false` when the node is cordoned.
    pub schedulable: bool,
    /// When `true`, the controller evicts every pod of the node.
    pub draining: bool,
}

impl NodeSpec {
    /// Spec of a node registered for the first time. `schedulable` defaults to
    /// `true` for workers and `false` for control planes.
    pub fn initial(role: NodeRole, schedulable: Option<bool>) -> Self {
        Self {
            schedulable: schedulable.unwrap_or(role == NodeRole::Worker),
            draining: false,
        }
    }
}

/// Reported by Keiki on registration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub role: NodeRole,
    /// Full containerd runtime names.
    pub runtimes: Vec<String>,
    /// Must not overlap the pod CIDR of another node.
    pub pod_cidr: IpNet,
    pub capacity: Resources,
}

impl NodeInfo {
    pub fn validate(&self) -> Result<(), ValidationErrors> {
        let mut errors = ValidationErrors::new();

        if self.runtimes.is_empty() {
            errors.push("runtimes", "at least one runtime is required");
        }
        let mut seen = HashSet::new();
        for (i, runtime) in self.runtimes.iter().enumerate() {
            if runtime.is_empty() || runtime.chars().any(char::is_whitespace) {
                errors.push(format!("runtimes[{i}]"), "must be a runtime name");
            } else if !seen.insert(runtime) {
                errors.push(format!("runtimes[{i}]"), "duplicate runtime");
            }
        }

        if self.pod_cidr.trunc() != self.pod_cidr {
            errors.push("pod_cidr", "must not have host bits set");
        }

        if self.capacity.cpu.is_zero() {
            errors.push("capacity.cpu", "must be greater than 0");
        }
        if self.capacity.memory.is_zero() {
            errors.push("capacity.memory", "must be greater than 0");
        }

        errors.into_result()
    }

    pub fn has_runtime(&self, runtime: &str) -> bool {
        self.runtimes.iter().any(|r| r == runtime)
    }
}

/// Whether two CIDRs share at least one address.
pub fn cidrs_overlap(a: &IpNet, b: &IpNet) -> bool {
    a.contains(&b.network()) || b.contains(&a.network())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeRole {
    Worker,
    ControlPlane,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    /// Derived when the node is read.
    pub condition: NodeCondition,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub last_heartbeat: Option<Timestamp>,
    /// Sum of the resources of the pods bound to the node.
    pub allocated: Resources,
    /// etcd revision of the last `allocated` change.
    #[serde(default)]
    pub allocated_revision: Revision,
    /// Actual usage measured by Keiki. Informational only.
    #[serde(default)]
    pub usage: Resources,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeCondition {
    /// The heartbeat lease is alive and Keiki reports healthy.
    Ready,
    /// The heartbeat lease is alive but Keiki reports unhealthy.
    Unhealthy,
    /// The heartbeat lease expired.
    Unreachable,
}

impl NodeCondition {
    /// Condition of a node from the presence of its lease key and its last
    /// reported health.
    pub fn derive(lease_alive: bool, health: NodeHealth) -> Self {
        match (lease_alive, health) {
            (false, _) => Self::Unreachable,
            (true, NodeHealth::Healthy) => Self::Ready,
            (true, NodeHealth::Unhealthy) => Self::Unhealthy,
        }
    }
}

/// Health reported by Keiki in its heartbeats.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHealth {
    Healthy,
    /// e.g. containerd is unreachable.
    Unhealthy,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Bytes, MilliCpu};

    fn info() -> NodeInfo {
        NodeInfo {
            role: NodeRole::Worker,
            runtimes: vec![
                "io.containerd.runc.v2".into(),
                "io.containerd.kata.v2".into(),
            ],
            pod_cidr: "10.244.0.0/24".parse().unwrap(),
            capacity: Resources::new(MilliCpu(12_000), Bytes(16 << 30)),
        }
    }

    #[test]
    fn accepts_valid_info() {
        assert_eq!(info().validate(), Ok(()));
    }

    #[test]
    fn rejects_invalid_info() {
        let mut info = info();
        info.runtimes.push("io.containerd.runc.v2".into());
        info.pod_cidr = "10.244.0.5/24".parse().unwrap();
        info.capacity.memory = Bytes::ZERO;

        let fields: Vec<String> = info
            .validate()
            .unwrap_err()
            .into_iter()
            .map(|e| e.field)
            .collect();
        assert_eq!(fields, ["runtimes[2]", "pod_cidr", "capacity.memory"]);
    }

    #[test]
    fn detects_overlapping_cidrs() {
        let net = |s: &str| s.parse::<IpNet>().unwrap();
        assert!(cidrs_overlap(&net("10.244.0.0/24"), &net("10.244.0.0/24")));
        assert!(cidrs_overlap(&net("10.244.0.0/16"), &net("10.244.3.0/24")));
        assert!(cidrs_overlap(&net("10.244.3.0/24"), &net("10.244.0.0/16")));
        assert!(!cidrs_overlap(&net("10.244.0.0/24"), &net("10.244.1.0/24")));
        assert!(!cidrs_overlap(&net("10.244.0.0/24"), &net("fd00::/64")));
    }

    #[test]
    fn initial_spec_depends_on_role() {
        assert!(NodeSpec::initial(NodeRole::Worker, None).schedulable);
        assert!(!NodeSpec::initial(NodeRole::ControlPlane, None).schedulable);
        assert!(NodeSpec::initial(NodeRole::ControlPlane, Some(true)).schedulable);
    }

    #[test]
    fn derives_condition() {
        assert_eq!(
            NodeCondition::derive(true, NodeHealth::Healthy),
            NodeCondition::Ready
        );
        assert_eq!(
            NodeCondition::derive(true, NodeHealth::Unhealthy),
            NodeCondition::Unhealthy
        );
        assert_eq!(
            NodeCondition::derive(false, NodeHealth::Healthy),
            NodeCondition::Unreachable
        );
    }
}
