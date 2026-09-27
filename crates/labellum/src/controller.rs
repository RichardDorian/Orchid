//! The controller: repairs the cluster when nodes are lost or drained.
//!
//! It runs on the leader instance only. Decisions are taken from the caches,
//! every write reads the keys it depends on again and checks them in its
//! transaction, along with the leader fencing token.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use orchid_api::{
    Node, NodeCondition, Pod, PodPhase, PodStatus, Resources, RestartPolicy, Revision,
};
use orchid_store::{Compare, CompareOp, Op, Store, Txn, codec, keys};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{info, warn};

use crate::State;
use crate::ops::{self, MAX_ATTEMPTS, Release, conflict};
use crate::records::{RawNode, RawPod, read, store_error, unchanged};

const RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
const RESYNC_INTERVAL: Duration = Duration::from_secs(60);

/// Runs the controller until `cancel` is cancelled.
pub async fn run<S: Store>(state: Arc<State<S>>, token: Revision, cancel: CancellationToken) {
    let mut controller = Controller {
        fence: Compare::create_revision(
            keys::leader(keys::CONTROLLER_ELECTION),
            CompareOp::Equal,
            token,
        ),
        state,
        unreachable_since: HashMap::new(),
        last_resync: Instant::now(),
    };
    let mut ticker = tokio::time::interval(RECONCILE_INTERVAL);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {}
        }
        if let Err(status) = controller.reconcile().await {
            warn!(%status, "reconciliation failed");
        }
    }
}

struct Controller<S> {
    state: Arc<State<S>>,
    /// Checks that this instance is still the leader.
    fence: Compare,
    /// When each unreachable node was first seen unreachable by this leader.
    unreachable_since: HashMap<String, Instant>,
    last_resync: Instant,
}

impl<S: Store> Controller<S> {
    async fn reconcile(&mut self) -> Result<(), Status> {
        // Both caches must be at the same revision, or a pod could be seen
        // bound to a node the node cache doesn't know yet.
        let revision = self.state.current_revision().await?;
        self.state.pods.wait_for(revision).await?;
        self.state.nodes.wait_for(revision).await?;

        let (config, _) = self.state.cluster_config().await?;
        let nodes: HashMap<String, Node> = self
            .state
            .nodes
            .list(|_| true)
            .0
            .into_iter()
            .map(|node| (node.name.clone(), node))
            .collect();
        let (pods, _) = self.state.pods.list(|pod| pod.node().is_some());

        let now = Instant::now();
        self.unreachable_since.retain(|name, _| {
            nodes
                .get(name)
                .is_some_and(|n| n.status.condition == NodeCondition::Unreachable)
        });
        for node in nodes.values() {
            if node.status.condition == NodeCondition::Unreachable {
                self.unreachable_since
                    .entry(node.name.clone())
                    .or_insert(now);
            }
        }
        let lost: HashSet<&str> = self
            .unreachable_since
            .iter()
            .filter(|(_, since)| now.duration_since(**since) >= config.pod_eviction_delay)
            .map(|(name, _)| name.as_str())
            .collect();

        for pod in &pods {
            let Some(node_name) = pod.node() else {
                continue;
            };
            let result = match nodes.get(node_name) {
                None => self.release_lost(pod, node_name, "deleted").await,
                Some(_) if lost.contains(node_name) => {
                    self.release_lost(pod, node_name, "unreachable").await
                }
                Some(node) if node.spec.draining && pod.status.phase != PodPhase::Terminating => {
                    self.evict(pod, node_name).await
                }
                Some(_) => Ok(()),
            };
            if let Err(status) = result {
                warn!(%status, pod = pod.name, "failed to repair pod");
            }
        }

        for node in nodes.values().filter(|n| n.spec.draining) {
            if !pods
                .iter()
                .any(|pod| pod.node() == Some(node.name.as_str()))
                && let Err(status) = self.finish_drain(&node.name).await
            {
                warn!(%status, node = node.name, "failed to finish drain");
            }
        }

        if self.last_resync.elapsed() >= RESYNC_INTERVAL {
            self.last_resync = Instant::now();
            for node in nodes.keys() {
                if let Err(status) = self.resync_allocation(node).await {
                    warn!(%status, node, "failed to resync allocation");
                }
            }
        }
        Ok(())
    }

    /// Releases a pod bound to a lost (unreachable or deleted) node.
    async fn release_lost(&self, pod: &Pod, node: &str, why: &str) -> Result<(), Status> {
        let store = &self.state.store;
        for _ in 0..MAX_ATTEMPTS {
            let Some(raw) = RawPod::read(store, &pod.name).await? else {
                return Ok(());
            };
            if raw.node() != Some(node) {
                return Ok(());
            }
            let raw_node = RawNode::read(store, node).await?;
            if raw_node.as_ref().is_some_and(|n| n.lease.is_some()) {
                // The node came back.
                return Ok(());
            }

            let status = &raw.status.value;
            let message = format!("node {node} is {why}");
            let release = if status.phase == PodPhase::Terminating {
                Release::finalize(&raw)
            } else if status.phase.is_terminal() {
                Release::Unbind
            } else if raw.spec.value.spec.restart_policy == RestartPolicy::Never {
                Release::Fail {
                    reason: "NodeLost",
                    message,
                }
            } else {
                Release::Reschedule {
                    reason: "NodeLost",
                    message,
                }
            };
            let extra = vec![self.fence.clone(), Compare::absent(keys::node::lease(node))];
            if ops::release(store, &raw, release.clone(), extra).await? {
                info!(
                    pod = raw.name,
                    node,
                    ?release,
                    "released pod of a lost node"
                );
                return Ok(());
            }
        }
        Err(conflict())
    }

    /// Starts the termination of a pod of a draining node.
    async fn evict(&self, pod: &Pod, node: &str) -> Result<(), Status> {
        let store = &self.state.store;
        for _ in 0..MAX_ATTEMPTS {
            let Some(raw) = RawPod::read(store, &pod.name).await? else {
                return Ok(());
            };
            if raw.node() != Some(node) || raw.status.value.phase == PodPhase::Terminating {
                return Ok(());
            }
            let Some(raw_node) = RawNode::read(store, node).await? else {
                return Ok(());
            };
            if !raw_node.spec.value.spec.draining {
                return Ok(());
            }

            let status = PodStatus {
                phase: PodPhase::Terminating,
                reason: "Evicted".to_owned(),
                message: format!("node {node} is draining"),
                ..raw.status.value.clone()
            };
            let mut compares = raw.unchanged();
            compares.push(self.fence.clone());
            compares.push(unchanged(keys::node::spec(node), Some(&raw_node.spec)));
            let response = store
                .txn(Txn::new().when(compares).then([Op::put(
                    keys::pod::status(&raw.name),
                    codec::encode(&status),
                )]))
                .await
                .map_err(store_error)?;
            if response.succeeded {
                info!(pod = raw.name, node, "evicting pod");
                return Ok(());
            }
        }
        Err(conflict())
    }

    /// Ends the drain of an empty node: it stays cordoned.
    async fn finish_drain(&self, node: &str) -> Result<(), Status> {
        let store = &self.state.store;
        let Some(raw) = RawNode::read(store, node).await? else {
            return Ok(());
        };
        if !raw.spec.value.spec.draining {
            return Ok(());
        }
        let mut record = raw.spec.value.clone();
        record.spec.draining = false;
        record.spec.schedulable = false;
        // Binds check the node spec too: no pod can be bound to the node
        // between the check of the caches and this write.
        let response = store
            .txn(
                Txn::new()
                    .when([
                        unchanged(keys::node::spec(node), Some(&raw.spec)),
                        self.fence.clone(),
                    ])
                    .then([Op::put(keys::node::spec(node), codec::encode(&record))]),
            )
            .await
            .map_err(store_error)?;
        if response.succeeded {
            info!(node, "node drained");
        }
        Ok(())
    }

    /// Recomputes the allocation of a node from the pods bound to it.
    async fn resync_allocation(&self, node: &str) -> Result<(), Status> {
        let store = &self.state.store;
        let key = keys::node::allocated(node);
        let Some(allocated) = read::<_, Resources>(store, &key).await? else {
            return Ok(());
        };
        // Every bind or release up to the read is reflected in the cache.
        // Later ones change the key, which fails the transaction.
        self.state.pods.wait_for(allocated.mod_revision).await?;
        let (pods, _) = self.state.pods.list(|pod| pod.node() == Some(node));
        let expected = pods
            .iter()
            .filter_map(|pod| pod.spec.resources())
            .try_fold(Resources::ZERO, Resources::checked_add)
            .unwrap_or(allocated.value);
        if expected == allocated.value {
            return Ok(());
        }
        let response = store
            .txn(
                Txn::new()
                    .when([unchanged(key.clone(), Some(&allocated)), self.fence.clone()])
                    .then([Op::put(key, codec::encode(&expected))]),
            )
            .await
            .map_err(store_error)?;
        if response.succeeded {
            warn!(node, ?expected, previous = ?allocated.value, "fixed allocation drift");
        }
        Ok(())
    }
}
