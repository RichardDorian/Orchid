use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::ValidationErrors;

/// Runtime used by default when the cluster configuration is created.
pub const DEFAULT_RUNTIME: &str = "io.containerd.runc.v2";

/// Cluster wide configuration, stored in etcd.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Runtime used by pods that don't specify one.
    pub default_runtime: String,
    /// TTL of the node heartbeat leases. Keiki sends a heartbeat every TTL / 3.
    #[serde(with = "crate::serde_duration")]
    pub node_lease_ttl: Duration,
    /// Time between a node becoming unreachable and its pods being rescheduled.
    #[serde(with = "crate::serde_duration")]
    pub pod_eviction_delay: Duration,
    /// TTL of the leader election leases (Labellum controller, Ikebana).
    #[serde(with = "crate::serde_duration")]
    pub leader_lease_ttl: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            default_runtime: DEFAULT_RUNTIME.to_owned(),
            node_lease_ttl: Duration::from_secs(15),
            pod_eviction_delay: Duration::from_secs(30),
            leader_lease_ttl: Duration::from_secs(15),
        }
    }
}

impl ClusterConfig {
    pub fn validate(&self) -> Result<(), ValidationErrors> {
        let mut errors = ValidationErrors::new();
        if self.default_runtime.is_empty() || self.default_runtime.chars().any(char::is_whitespace)
        {
            errors.push("default_runtime", "must be a runtime name");
        }
        // etcd leases have a granularity of one second.
        if self.node_lease_ttl < Duration::from_secs(1) {
            errors.push("node_lease_ttl", "must be at least 1s");
        }
        if self.leader_lease_ttl < Duration::from_secs(1) {
            errors.push("leader_lease_ttl", "must be at least 1s");
        }
        errors.into_result()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_valid() {
        assert_eq!(ClusterConfig::default().validate(), Ok(()));
    }

    #[test]
    fn rejects_short_ttls() {
        let config = ClusterConfig {
            node_lease_ttl: Duration::from_millis(500),
            ..ClusterConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn serializes_durations_as_strings() {
        let json = serde_json::to_value(ClusterConfig::default()).unwrap();
        assert_eq!(json["node_lease_ttl"], "15s");
        assert_eq!(json["pod_eviction_delay"], "30s");
    }
}
