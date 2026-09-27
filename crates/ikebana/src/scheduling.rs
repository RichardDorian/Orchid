//! Scheduling decisions: which pod next, on which node.

use std::cmp::Reverse;

use orchid_api::{Node, NodeCondition, Pod, Resources, Revision};

/// Resources reserved by a successful bind that the node cache doesn't show
/// yet (its `allocated_revision` is older than the bind).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Assumed {
    pub revision: Revision,
    pub resources: Resources,
}

/// A node and the pods bound to it that its cached state doesn't reflect yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeEntry {
    pub node: Node,
    pub assumed: Vec<Assumed>,
}

impl NodeEntry {
    pub fn new(node: Node) -> Self {
        Self {
            node,
            assumed: Vec::new(),
        }
    }

    /// Replaces the node, forgetting the binds it now reflects.
    pub fn update(&mut self, node: Node) {
        let revision = node.status.allocated_revision;
        self.assumed.retain(|a| a.revision > revision);
        self.node = node;
    }

    /// Resources allocated on the node, including assumed binds.
    pub fn allocated(&self) -> Resources {
        self.assumed
            .iter()
            .try_fold(self.node.status.allocated, |total, a| {
                total.checked_add(a.resources)
            })
            .unwrap_or(self.node.info.capacity)
    }

    /// Whether the node can receive new pods at all.
    pub fn schedulable(&self) -> bool {
        let node = &self.node;
        node.status.condition == NodeCondition::Ready
            && node.spec.schedulable
            && !node.spec.draining
    }
}

/// Whether a change of a node may let an unschedulable pod fit somewhere.
pub fn relevant_change(old: &Node, new: &Node) -> bool {
    old.status.condition != new.status.condition
        || old.spec != new.spec
        || old.info.capacity != new.info.capacity
        || old.info.runtimes != new.info.runtimes
        || old.status.allocated != new.status.allocated
}

/// The node with the lowest `max(cpu%, mem%)` once the pod is placed, among
/// the schedulable nodes providing the runtime and with enough resources left.
/// Ties are broken by node name.
pub fn choose_node<'a>(
    pod: &Pod,
    nodes: impl IntoIterator<Item = &'a NodeEntry>,
) -> Option<&'a str> {
    let resources = pod.spec.resources()?;
    nodes
        .into_iter()
        .filter(|entry| entry.schedulable() && entry.node.info.has_runtime(&pod.spec.runtime))
        .filter_map(|entry| {
            let capacity = entry.node.info.capacity;
            let total = entry.allocated().checked_add(resources)?;
            total
                .fits_in(capacity)
                .then(|| (score(total, capacity), entry.node.name.as_str()))
        })
        .min_by(|(a, a_name), (b, b_name)| a.total_cmp(b).then_with(|| a_name.cmp(b_name)))
        .map(|(_, name)| name)
}

/// `max(cpu%, mem%)` of `total` over `capacity`.
pub fn score(total: Resources, capacity: Resources) -> f64 {
    let ratio = |used: u64, capacity: u64| {
        if capacity == 0 {
            f64::INFINITY
        } else {
            used as f64 / capacity as f64
        }
    };
    ratio(total.cpu.0, capacity.cpu.0).max(ratio(total.memory.0, capacity.memory.0))
}

/// Order of the queue: highest priority first, then the pod waiting for the
/// longest time, then by name.
pub fn queue_key(pod: &Pod) -> (i32, Reverse<orchid_api::Timestamp>, Reverse<&str>) {
    (
        pod.spec.priority,
        Reverse(pod.status.pending_since),
        Reverse(pod.name.as_str()),
    )
}

#[cfg(test)]
mod tests {
    use orchid_api::{
        Bytes, Container, MilliCpu, NodeInfo, NodeRole, NodeSpec, NodeStatus, PodSpec, PodStatus,
        RestartPolicy, Timestamp,
    };

    use super::*;

    const RUNC: &str = "io.containerd.runc.v2";

    fn node(
        name: &str,
        cpu: u64,
        memory_gib: u64,
        allocated_cpu: u64,
        allocated_gib: u64,
    ) -> NodeEntry {
        NodeEntry::new(Node {
            name: name.into(),
            uid: String::new(),
            revision: 1,
            spec: NodeSpec {
                schedulable: true,
                draining: false,
            },
            info: NodeInfo {
                role: NodeRole::Worker,
                runtimes: vec![RUNC.into()],
                pod_cidr: "10.244.0.0/24".parse().unwrap(),
                capacity: Resources::new(MilliCpu(cpu), Bytes(memory_gib << 30)),
            },
            status: NodeStatus {
                condition: NodeCondition::Ready,
                message: String::new(),
                last_heartbeat: None,
                allocated: Resources::new(MilliCpu(allocated_cpu), Bytes(allocated_gib << 30)),
                allocated_revision: 1,
                usage: Resources::ZERO,
            },
        })
    }

    fn pod(name: &str, cpu: u64, memory_gib: u64) -> Pod {
        Pod {
            name: name.into(),
            uid: String::new(),
            revision: 1,
            created_at: Timestamp::UNIX_EPOCH,
            spec: PodSpec {
                runtime: RUNC.into(),
                priority: 0,
                restart_policy: RestartPolicy::Always,
                termination_grace_period: orchid_api::DEFAULT_TERMINATION_GRACE_PERIOD,
                containers: vec![Container {
                    name: "app".into(),
                    image: "nginx".into(),
                    resources: Resources::new(MilliCpu(cpu), Bytes(memory_gib << 30)),
                }],
            },
            binding: None,
            status: PodStatus::pending(Timestamp::UNIX_EPOCH),
        }
    }

    #[test]
    fn picks_the_least_loaded_node() {
        // After placing 1 core / 1 GiB:
        // a: max(50%, 12.5%) = 50%, b: max(25%, 75%) = 75%, c: max(12.5%, 25%) = 25%.
        let nodes = [
            node("a", 4000, 8, 1000, 0),
            node("b", 8000, 4, 1000, 2),
            node("c", 8000, 4, 0, 0),
        ];
        assert_eq!(choose_node(&pod("p", 1000, 1), &nodes), Some("c"));
    }

    #[test]
    fn breaks_ties_by_name() {
        let nodes = [node("b", 4000, 4, 0, 0), node("a", 4000, 4, 0, 0)];
        assert_eq!(choose_node(&pod("p", 1000, 1), &nodes), Some("a"));
    }

    #[test]
    fn filters_nodes() {
        let pod = pod("p", 1000, 1);
        let mut full = node("full", 4000, 4, 3500, 0);
        let mut unready = node("unready", 4000, 4, 0, 0);
        unready.node.status.condition = NodeCondition::Unreachable;
        let mut cordoned = node("cordoned", 4000, 4, 0, 0);
        cordoned.node.spec.schedulable = false;
        let mut draining = node("draining", 4000, 4, 0, 0);
        draining.node.spec.draining = true;
        let mut kata = node("kata", 4000, 4, 0, 0);
        kata.node.info.runtimes = vec!["io.containerd.kata.v2".into()];
        assert_eq!(
            choose_node(&pod, [&full, &unready, &cordoned, &draining, &kata]),
            None
        );

        full.node.status.allocated.cpu = MilliCpu(3000);
        assert_eq!(choose_node(&pod, [&full]), Some("full"), "exactly fits");
    }

    #[test]
    fn counts_assumed_binds_until_the_node_reflects_them() {
        let mut entry = node("a", 2000, 4, 0, 0);
        entry.assumed.push(Assumed {
            revision: 5,
            resources: Resources::new(MilliCpu(1500), Bytes(0)),
        });
        assert_eq!(choose_node(&pod("p", 1000, 1), [&entry]), None);

        let mut updated = entry.node.clone();
        updated.status.allocated_revision = 5;
        updated.status.allocated.cpu = MilliCpu(1500);
        entry.update(updated);
        assert!(entry.assumed.is_empty());
        assert_eq!(entry.allocated().cpu, MilliCpu(1500));
    }

    #[test]
    fn orders_the_queue() {
        let mut high = pod("high", 1, 1);
        high.spec.priority = 10;
        let mut old = pod("old", 1, 1);
        old.status.pending_since = Timestamp::UNIX_EPOCH;
        let mut new = pod("new", 1, 1);
        new.status.pending_since = Timestamp::new(60, 0).unwrap();
        let mut negative = pod("negative", 1, 1);
        negative.spec.priority = -1;

        let mut pods = [&new, &negative, &old, &high];
        pods.sort_by_key(|p| Reverse(queue_key(p)));
        let names: Vec<&str> = pods.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["high", "old", "new", "negative"]);
    }
}
