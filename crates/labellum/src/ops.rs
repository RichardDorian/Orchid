//! Writes shared by the API services and the controller.

use jiff::Timestamp;
use orchid_api::{PodBinding, PodPhase, PodStatus, Resources};
use orchid_store::{Compare, Op, Store, Txn, codec, keys};
use tonic::Status;

use crate::records::{RawPod, Versioned, read, store_error, unchanged};

/// Maximum number of attempts of a read-check-write cycle before giving up.
pub const MAX_ATTEMPTS: usize = 10;

pub fn conflict() -> Status {
    Status::aborted("too many concurrent updates, retry")
}

/// What happens to a pod leaving its node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Release {
    /// The pod is removed.
    Delete,
    /// The pod goes back to `pending` to be scheduled again.
    Reschedule {
        reason: &'static str,
        message: String,
    },
    /// The pod is unbound and marked `failed`.
    Fail {
        reason: &'static str,
        message: String,
    },
    /// The pod is unbound, its status is kept (terminal pods).
    Unbind,
}

impl Release {
    /// Release of a terminating pod that is done: deleted pods are removed,
    /// evicted pods are rescheduled.
    pub fn finalize(pod: &RawPod) -> Self {
        if pod.status.value.deletion_requested_at.is_some() {
            Self::Delete
        } else {
            Self::Reschedule {
                reason: "Evicted",
                message: String::new(),
            }
        }
    }
}

/// Removes `pod` from its node in a single transaction: its resources are
/// subtracted from the node `allocated` key, and it is deleted or unbound.
///
/// Returns `false` if the pod or the node allocation changed since they were
/// read, or if one of the `extra` compares doesn't hold: the caller must read
/// the pod again and retry.
pub async fn release<S: Store>(
    store: &S,
    pod: &RawPod,
    release: Release,
    extra: Vec<Compare>,
) -> Result<bool, Status> {
    let mut compares = pod.unchanged();
    compares.extend(extra);
    let mut ops = Vec::new();

    if let Some(node) = pod.node() {
        let key = keys::node::allocated(node);
        let allocated: Option<Versioned<Resources>> = read(store, &key).await?;
        compares.push(unchanged(key.clone(), allocated.as_ref()));
        if let Some(allocated) = allocated {
            let remaining = allocated.value.saturating_sub(pod.resources());
            ops.push(Op::put(key, codec::encode(&remaining)));
        }
    }

    let unbound = PodBinding {
        node: String::new(),
        attempt: pod.attempt(),
    };
    let status = &pod.status.value;
    match release {
        Release::Delete => ops.push(Op::delete_prefix(keys::pod::prefix(&pod.name))),
        Release::Reschedule { reason, message } => {
            ops.push(Op::put(
                keys::pod::binding(&pod.name),
                codec::encode(&unbound),
            ));
            let status = PodStatus {
                reason: reason.to_owned(),
                message,
                ..PodStatus::pending(Timestamp::now())
            };
            ops.push(Op::put(
                keys::pod::status(&pod.name),
                codec::encode(&status),
            ));
        }
        Release::Fail { reason, message } => {
            ops.push(Op::put(
                keys::pod::binding(&pod.name),
                codec::encode(&unbound),
            ));
            let status = PodStatus {
                phase: PodPhase::Failed,
                reason: reason.to_owned(),
                message,
                ..status.clone()
            };
            ops.push(Op::put(
                keys::pod::status(&pod.name),
                codec::encode(&status),
            ));
        }
        Release::Unbind => {
            ops.push(Op::put(
                keys::pod::binding(&pod.name),
                codec::encode(&unbound),
            ));
        }
    }

    let response = store
        .txn(Txn::new().when(compares).then(ops))
        .await
        .map_err(store_error)?;
    Ok(response.succeeded)
}
