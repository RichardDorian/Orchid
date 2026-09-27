//! Values stored in etcd, and reads of the keys of an object.
//!
//! Every object is split into several keys (see [`orchid_store::keys`]), each
//! key having a single kind of writer. Writes always read the keys they
//! depend on first ([`RawPod`], [`RawNode`]), then commit a transaction that
//! checks those keys have not changed since.

use std::collections::BTreeMap;

use ipnet::IpNet;
use jiff::Timestamp;
use orchid_api::{
    Node, NodeCondition, NodeHealth, NodeInfo, NodeSpec, NodeStatus, Pod, PodBinding, PodSpec,
    PodStatus, Resources, Revision,
};
use orchid_store::{Compare, KeyValue, Store, StoreError, codec, keys};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tonic::Status;

/// `/orchid/pods/<name>/spec`
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodSpecRecord {
    pub uid: String,
    pub created_at: Timestamp,
    pub spec: PodSpec,
}

/// `/orchid/nodes/<name>/spec`
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSpecRecord {
    pub uid: String,
    #[serde(flatten)]
    pub spec: NodeSpec,
}

/// `/orchid/nodes/<name>/status`
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatusRecord {
    pub health: NodeHealth,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub last_heartbeat: Option<Timestamp>,
    #[serde(default)]
    pub usage: Resources,
}

/// `/orchid/nodes/<name>/lease`, attached to the heartbeat lease of the node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub registered_at: Timestamp,
}

/// `/orchid/leader/<election>`, attached to the lease of the leader.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderRecord {
    pub candidate: String,
}

/// `/orchid/cluster/pod-cidrs`
pub type PodCidrs = BTreeMap<String, IpNet>;

/// A decoded value and the revision of its last write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Versioned<T> {
    pub value: T,
    pub mod_revision: Revision,
    pub create_revision: Revision,
}

impl<T: DeserializeOwned> Versioned<T> {
    pub fn decode(kv: &KeyValue) -> Result<Self, StoreError> {
        Ok(Self {
            value: codec::decode(kv)?,
            mod_revision: kv.mod_revision,
            create_revision: kv.create_revision,
        })
    }
}

/// Reads and decodes a single key.
pub async fn read<S: Store, T: DeserializeOwned>(
    store: &S,
    key: &str,
) -> Result<Option<Versioned<T>>, Status> {
    let response = store.get(key).await.map_err(store_error)?;
    response
        .kv
        .as_ref()
        .map(Versioned::decode)
        .transpose()
        .map_err(store_error)
}

/// A compare checking that `key` is still at `versioned`, or still absent.
pub fn unchanged<T>(key: String, versioned: Option<&Versioned<T>>) -> Compare {
    match versioned {
        Some(versioned) => Compare::unchanged(key, versioned.mod_revision),
        None => Compare::absent(key),
    }
}

pub fn store_error(error: StoreError) -> Status {
    match error {
        StoreError::LeaseNotFound => Status::not_found("lease not found"),
        StoreError::Compacted { compact_revision } => Status::out_of_range(format!(
            "revision has been compacted, oldest available revision is {compact_revision}"
        )),
        StoreError::InvalidRequest(message) => Status::internal(message),
        StoreError::Decode { .. } => Status::internal(error.to_string()),
        StoreError::WatchClosed(_) | StoreError::Backend(_) => {
            Status::unavailable(error.to_string())
        }
    }
}

/// The name of the object a key belongs to.
pub fn object_name<'a>(prefix: &str, key: &'a str) -> Option<&'a str> {
    let (name, _) = key.strip_prefix(prefix)?.split_once('/')?;
    (!name.is_empty()).then_some(name)
}

/// The keys of a pod, as stored in etcd.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawPod {
    pub name: String,
    pub spec: Versioned<PodSpecRecord>,
    pub binding: Option<Versioned<PodBinding>>,
    pub status: Versioned<PodStatus>,
}

impl RawPod {
    pub async fn read<S: Store>(store: &S, name: &str) -> Result<Option<Self>, Status> {
        let response = store
            .list(&keys::pod::prefix(name))
            .await
            .map_err(store_error)?;
        Self::from_kvs(name, response.kvs.iter()).map_err(store_error)
    }

    /// `None` if the spec or status key is missing.
    pub fn from_kvs<'a>(
        name: &str,
        kvs: impl IntoIterator<Item = &'a KeyValue>,
    ) -> Result<Option<Self>, StoreError> {
        let (mut spec, mut binding, mut status) = (None, None, None);
        for kv in kvs {
            match keys::parse(&kv.key) {
                Some(keys::Key::Pod(n, keys::PodKey::Spec)) if n == name => {
                    spec = Some(Versioned::decode(kv)?);
                }
                Some(keys::Key::Pod(n, keys::PodKey::Binding)) if n == name => {
                    binding = Some(Versioned::decode(kv)?);
                }
                Some(keys::Key::Pod(n, keys::PodKey::Status)) if n == name => {
                    status = Some(Versioned::decode(kv)?);
                }
                _ => {}
            }
        }
        Ok(match (spec, status) {
            (Some(spec), Some(status)) => Some(Self {
                name: name.to_owned(),
                spec,
                binding,
                status,
            }),
            _ => None,
        })
    }

    /// Highest revision of the keys of the pod.
    pub fn revision(&self) -> Revision {
        self.spec
            .mod_revision
            .max(self.status.mod_revision)
            .max(self.binding.as_ref().map_or(0, |b| b.mod_revision))
    }

    /// The node the pod is bound to.
    pub fn node(&self) -> Option<&str> {
        self.binding.as_ref().and_then(|b| b.value.node())
    }

    pub fn attempt(&self) -> u32 {
        self.binding.as_ref().map_or(0, |b| b.value.attempt)
    }

    /// Resources reserved by the pod on its node.
    pub fn resources(&self) -> Resources {
        self.spec.value.spec.resources().unwrap_or_default()
    }

    /// Compares checking that no key of the pod changed since it was read.
    pub fn unchanged(&self) -> Vec<Compare> {
        vec![
            Compare::unchanged(keys::pod::spec(&self.name), self.spec.mod_revision),
            unchanged(keys::pod::binding(&self.name), self.binding.as_ref()),
            Compare::unchanged(keys::pod::status(&self.name), self.status.mod_revision),
        ]
    }

    pub fn to_pod(&self) -> Pod {
        Pod {
            name: self.name.clone(),
            uid: self.spec.value.uid.clone(),
            revision: self.revision(),
            created_at: self.spec.value.created_at,
            spec: self.spec.value.spec.clone(),
            binding: self.binding.as_ref().map(|b| b.value.clone()),
            status: self.status.value.clone(),
        }
    }
}

/// The keys of a node, as stored in etcd.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawNode {
    pub name: String,
    pub spec: Versioned<NodeSpecRecord>,
    pub info: Versioned<NodeInfo>,
    pub status: Option<Versioned<NodeStatusRecord>>,
    pub allocated: Option<Versioned<Resources>>,
    /// The heartbeat key. Absent when the node is unreachable.
    pub lease: Option<KeyValue>,
}

impl RawNode {
    pub async fn read<S: Store>(store: &S, name: &str) -> Result<Option<Self>, Status> {
        let response = store
            .list(&keys::node::prefix(name))
            .await
            .map_err(store_error)?;
        Self::from_kvs(name, response.kvs.iter()).map_err(store_error)
    }

    /// `None` if the spec or info key is missing.
    pub fn from_kvs<'a>(
        name: &str,
        kvs: impl IntoIterator<Item = &'a KeyValue>,
    ) -> Result<Option<Self>, StoreError> {
        let (mut spec, mut info, mut status, mut allocated, mut lease) =
            (None, None, None, None, None);
        for kv in kvs {
            match keys::parse(&kv.key) {
                Some(keys::Key::Node(n, part)) if n == name => match part {
                    keys::NodeKey::Spec => spec = Some(Versioned::decode(kv)?),
                    keys::NodeKey::Info => info = Some(Versioned::decode(kv)?),
                    keys::NodeKey::Status => status = Some(Versioned::decode(kv)?),
                    keys::NodeKey::Allocated => allocated = Some(Versioned::decode(kv)?),
                    keys::NodeKey::Lease => lease = Some(kv.clone()),
                },
                _ => {}
            }
        }
        Ok(match (spec, info) {
            (Some(spec), Some(info)) => Some(Self {
                name: name.to_owned(),
                spec,
                info,
                status,
                allocated,
                lease,
            }),
            _ => None,
        })
    }

    /// Highest revision of the keys of the node.
    pub fn revision(&self) -> Revision {
        [
            Some(self.spec.mod_revision),
            Some(self.info.mod_revision),
            self.status.as_ref().map(|s| s.mod_revision),
            self.allocated.as_ref().map(|a| a.mod_revision),
            self.lease.as_ref().map(|l| l.mod_revision),
        ]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(0)
    }

    pub fn allocated(&self) -> Resources {
        self.allocated.as_ref().map(|a| a.value).unwrap_or_default()
    }

    /// Compares checking that no key of the node changed since it was read.
    pub fn unchanged(&self) -> Vec<Compare> {
        vec![
            Compare::unchanged(keys::node::spec(&self.name), self.spec.mod_revision),
            Compare::unchanged(keys::node::info(&self.name), self.info.mod_revision),
            unchanged(keys::node::status(&self.name), self.status.as_ref()),
            unchanged(keys::node::allocated(&self.name), self.allocated.as_ref()),
            Compare::unchanged(
                keys::node::lease(&self.name),
                self.lease.as_ref().map_or(0, |l| l.mod_revision),
            ),
        ]
    }

    pub fn health(&self) -> NodeHealth {
        self.status
            .as_ref()
            .map_or(NodeHealth::Healthy, |s| s.value.health)
    }

    pub fn condition(&self) -> NodeCondition {
        NodeCondition::derive(self.lease.is_some(), self.health())
    }

    pub fn to_node(&self) -> Node {
        let status = self.status.as_ref().map(|s| &s.value);
        Node {
            name: self.name.clone(),
            uid: self.spec.value.uid.clone(),
            revision: self.revision(),
            spec: self.spec.value.spec,
            info: self.info.value.clone(),
            status: NodeStatus {
                condition: self.condition(),
                message: status.map(|s| s.message.clone()).unwrap_or_default(),
                last_heartbeat: status.and_then(|s| s.last_heartbeat),
                allocated: self.allocated(),
                allocated_revision: self.allocated.as_ref().map_or(0, |a| a.mod_revision),
                usage: status.map(|s| s.usage).unwrap_or_default(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_object_names() {
        assert_eq!(
            object_name(keys::PODS, "/orchid/pods/my-app/spec"),
            Some("my-app")
        );
        assert_eq!(object_name(keys::PODS, "/orchid/pods/my-app"), None);
        assert_eq!(object_name(keys::PODS, "/orchid/nodes/a/spec"), None);
    }
}
