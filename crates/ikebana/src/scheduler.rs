//! The scheduling loop.

use std::collections::HashMap;
use std::time::Duration;

use orchid_api::{Node, Pod, PodPhase};
use orchid_client::informer::{Change, InformerEvent};
use orchid_proto::v1 as pb;
use orchid_proto::v1::scheduler_service_client::SchedulerServiceClient;
use orchid_transport::client::Connection;
use orchid_transport::errors;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::scheduling::{Assumed, NodeEntry, choose_node, queue_key, relevant_change};

/// Unschedulable pods are retried at least this often.
const UNSCHEDULABLE_RETRY: Duration = Duration::from_secs(60);
/// Pause after a bind failed because of a stale node, to let the watch catch up.
const STALE_NODE_PAUSE: Duration = Duration::from_millis(200);
/// Pause after an unexpected bind failure.
const ERROR_PAUSE: Duration = Duration::from_secs(1);

struct Queued {
    pod: Pod,
    /// When the pod could not be placed on any node.
    unschedulable_since: Option<Instant>,
}

pub struct Scheduler {
    client: SchedulerServiceClient<Connection>,
    nodes: HashMap<String, NodeEntry>,
    queue: HashMap<String, Queued>,
    paused_until: Option<Instant>,
}

impl Scheduler {
    pub fn new(channel: Connection) -> Self {
        Self {
            client: SchedulerServiceClient::new(channel),
            nodes: HashMap::new(),
            queue: HashMap::new(),
            paused_until: None,
        }
    }

    /// Schedules pods while leader, until `shutdown` is cancelled.
    pub async fn run(
        mut self,
        mut nodes: mpsc::Receiver<InformerEvent<Node>>,
        mut pods: mpsc::Receiver<InformerEvent<Pod>>,
        mut leader: watch::Receiver<Option<i64>>,
        stale: mpsc::Sender<()>,
        shutdown: CancellationToken,
    ) {
        loop {
            // Apply every pending update before taking a decision.
            while let Ok(event) = nodes.try_recv() {
                self.apply_node(event);
            }
            while let Ok(event) = pods.try_recv() {
                self.apply_pod(event);
            }
            self.retry_unschedulable();

            let token = *leader.borrow();
            let paused = self
                .paused_until
                .is_some_and(|until| Instant::now() < until);
            if let Some(token) = token
                && !paused
                && let Some(name) = self.next_pod()
            {
                self.schedule(&name, token, &stale).await;
                continue;
            }

            let wake_up = self.next_wake_up();
            tokio::select! {
                () = shutdown.cancelled() => return,
                Some(event) = nodes.recv() => self.apply_node(event),
                Some(event) = pods.recv() => self.apply_pod(event),
                Ok(()) = leader.changed() => {}
                () = sleep_until(wake_up) => {}
            }
        }
    }

    fn apply_node(&mut self, event: InformerEvent<Node>) {
        match event {
            InformerEvent::Synced(nodes) => {
                let mut previous = std::mem::take(&mut self.nodes);
                for node in nodes {
                    let entry = match previous.remove(&node.name) {
                        Some(mut entry) => {
                            entry.update(node);
                            entry
                        }
                        None => NodeEntry::new(node),
                    };
                    self.nodes.insert(entry.node.name.clone(), entry);
                }
                self.requeue_unschedulable();
            }
            InformerEvent::Changed(Change::Added(node) | Change::Modified(node)) => {
                let relevant = match self.nodes.get_mut(&node.name) {
                    Some(entry) => {
                        let relevant = relevant_change(&entry.node, &node);
                        entry.update(node);
                        relevant
                    }
                    None => {
                        self.nodes.insert(node.name.clone(), NodeEntry::new(node));
                        true
                    }
                };
                if relevant {
                    self.requeue_unschedulable();
                }
            }
            InformerEvent::Changed(Change::Deleted(node)) => {
                self.nodes.remove(&node.name);
            }
        }
    }

    fn apply_pod(&mut self, event: InformerEvent<Pod>) {
        match event {
            InformerEvent::Synced(pods) => {
                let mut previous = std::mem::take(&mut self.queue);
                for pod in pods.into_iter().filter(waiting) {
                    let unschedulable_since = previous
                        .remove(&pod.name)
                        .and_then(|queued| queued.unschedulable_since);
                    self.queue.insert(
                        pod.name.clone(),
                        Queued {
                            pod,
                            unschedulable_since,
                        },
                    );
                }
            }
            InformerEvent::Changed(Change::Added(pod) | Change::Modified(pod)) => {
                if !waiting(&pod) {
                    self.queue.remove(&pod.name);
                    return;
                }
                match self.queue.get_mut(&pod.name) {
                    Some(queued) => queued.pod = pod,
                    None => {
                        self.queue.insert(
                            pod.name.clone(),
                            Queued {
                                pod,
                                unschedulable_since: None,
                            },
                        );
                    }
                }
            }
            InformerEvent::Changed(Change::Deleted(pod)) => {
                self.queue.remove(&pod.name);
            }
        }
    }

    fn requeue_unschedulable(&mut self) {
        for queued in self.queue.values_mut() {
            queued.unschedulable_since = None;
        }
    }

    fn retry_unschedulable(&mut self) {
        let now = Instant::now();
        for queued in self.queue.values_mut() {
            if queued
                .unschedulable_since
                .is_some_and(|since| now.duration_since(since) >= UNSCHEDULABLE_RETRY)
            {
                queued.unschedulable_since = None;
            }
        }
    }

    /// The active pod to schedule first.
    fn next_pod(&self) -> Option<String> {
        self.queue
            .values()
            .filter(|queued| queued.unschedulable_since.is_none())
            .max_by(|a, b| queue_key(&a.pod).cmp(&queue_key(&b.pod)))
            .map(|queued| queued.pod.name.clone())
    }

    /// When the loop must wake up without events: end of a pause, or retry
    /// of the unschedulable pods.
    fn next_wake_up(&self) -> Option<Instant> {
        let retry = self
            .queue
            .values()
            .filter_map(|queued| queued.unschedulable_since)
            .min()
            .map(|since| since + UNSCHEDULABLE_RETRY);
        [
            retry,
            self.paused_until.filter(|until| *until > Instant::now()),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    async fn schedule(&mut self, name: &str, token: i64, stale: &mpsc::Sender<()>) {
        let Some(queued) = self.queue.get_mut(name) else {
            return;
        };
        let pod = queued.pod.clone();
        let Some(node) = choose_node(&pod, self.nodes.values()).map(str::to_owned) else {
            debug!(pod = name, "no node fits the pod");
            queued.unschedulable_since = Some(Instant::now());
            return;
        };

        let result = self
            .client
            .bind(pb::BindRequest {
                pod: pod.name.clone(),
                pod_revision: pod.revision,
                node: node.clone(),
                leader_token: token,
            })
            .await;
        match result {
            Ok(response) => {
                let response = response.into_inner();
                info!(pod = name, node, "pod scheduled");
                self.queue.remove(name);
                if let (Some(entry), Some(resources)) =
                    (self.nodes.get_mut(&node), pod.spec.resources())
                {
                    entry.assumed.push(Assumed {
                        revision: response.revision,
                        resources,
                    });
                }
            }
            Err(status) => {
                let reason = errors::reason(&status);
                match reason.as_deref() {
                    Some(
                        "BIND_FAILURE_INSUFFICIENT_RESOURCES" | "BIND_FAILURE_NODE_UNAVAILABLE",
                    ) => {
                        debug!(pod = name, node, %status, "stale node, retrying");
                        self.paused_until = Some(Instant::now() + STALE_NODE_PAUSE);
                    }
                    Some("BIND_FAILURE_POD_CHANGED" | "BIND_FAILURE_POD_ALREADY_BOUND") => {
                        // The watch delivers the new version of the pod if it
                        // still needs to be scheduled.
                        debug!(pod = name, %status, "pod changed");
                        self.queue.remove(name);
                    }
                    Some("BIND_FAILURE_STALE_LEADER_TOKEN") => {
                        warn!("leader token rejected, stepping down");
                        let _ = stale.try_send(());
                    }
                    _ => {
                        warn!(pod = name, node, %status, "failed to bind");
                        self.paused_until = Some(Instant::now() + ERROR_PAUSE);
                    }
                }
            }
        }
    }
}

/// Whether the pod is waiting to be scheduled.
fn waiting(pod: &Pod) -> bool {
    pod.node().is_none() && pod.status.phase == PodPhase::Pending
}

async fn sleep_until(instant: Option<Instant>) {
    match instant {
        Some(instant) => tokio::time::sleep_until(instant).await,
        None => std::future::pending().await,
    }
}
