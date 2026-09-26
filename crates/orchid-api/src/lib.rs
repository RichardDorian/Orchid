//! Domain types of the Orchid API.
//!
//! This crate is shared by every Orchid component. It contains:
//! - the resource model (pods, nodes, cluster configuration),
//! - resource quantities ([`MilliCpu`], [`Bytes`]) and their string format,
//! - validation of user provided objects,
//! - conversions from and to the protobuf types of [`orchid_proto`].

mod cluster;
mod error;
mod name;
mod node;
mod pod;
mod quantity;
mod resources;

pub mod proto;
pub mod serde_duration;

pub use cluster::{ClusterConfig, DEFAULT_RUNTIME};
pub use error::{ValidationError, ValidationErrors};
pub use name::{MAX_NAME_LEN, validate_name};
pub use node::{
    Node, NodeCondition, NodeHealth, NodeInfo, NodeRole, NodeSpec, NodeStatus, cidrs_overlap,
};
pub use pod::{
    Container, ContainerState, ContainerStatus, DEFAULT_TERMINATION_GRACE_PERIOD, Pod, PodBinding,
    PodPhase, PodSpec, PodStatus, RestartPolicy,
};
pub use quantity::{Bytes, MilliCpu, ParseQuantityError};
pub use resources::Resources;

/// etcd revision.
pub type Revision = i64;

pub use jiff::Timestamp;
