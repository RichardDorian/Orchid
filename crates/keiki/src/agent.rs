//! The agent: registers the node, sends heartbeats, and runs a worker for
//! every pod bound to the node.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ipnet::IpNet;
use orchid_api::proto::duration_from_proto;
use orchid_api::{NodeRole, Pod, Resources};
use orchid_client::informer::{self, Change, InformerEvent, PodSource};
use orchid_proto::v1 as pb;
use orchid_proto::v1::node_agent_service_client::NodeAgentServiceClient;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use crate::runtime::{PodRef, Runtime};
use crate::usage::UsageSampler;
use crate::worker::Worker;

/// Grace period of pods removed because they are not bound to the node anymore.
const ORPHAN_GRACE: Duration = Duration::from_secs(30);

/// Tunables of the agent.
#[derive(Clone, Debug)]
pub struct Options {
    /// First delay before restarting an exited container or retrying a failed
    /// pod creation. Doubled on each attempt, up to 5 minutes.
    pub restart_backoff: Duration,
    /// How often pods left in the runtime are looked for.
    pub cleanup_interval: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            restart_backoff: crate::status::INITIAL_BACKOFF,
            cleanup_interval: Duration::from_secs(10),
        }
    }
}

/// What the node reports when it registers.
#[derive(Clone, Debug)]
pub struct Registration {
    /// Required in cleartext mode. In mTLS mode, the name comes from the certificate.
    pub node: Option<String>,
    pub role: NodeRole,
    pub schedulable: Option<bool>,
    pub runtimes: Vec<String>,
    pub pod_cidr: IpNet,
    pub capacity: Resources,
}

impl Registration {
    fn request(&self) -> pb::RegisterRequest {
        pb::RegisterRequest {
            node: self.node.clone().unwrap_or_default(),
            role: pb::NodeRole::from(self.role).into(),
            schedulable: self.schedulable,
            runtimes: self.runtimes.clone(),
            pod_cidr: self.pod_cidr.to_string(),
            capacity: Some(self.capacity.into()),
        }
    }
}

/// Runs the agent until `shutdown` is cancelled. Running containers are left
/// untouched when the agent stops.
pub async fn run<R: Runtime>(
    runtime: Arc<R>,
    channel: Channel,
    registration: Registration,
    options: Options,
    shutdown: CancellationToken,
) {
    let mut client = NodeAgentServiceClient::new(channel.clone());
    let mut backoff = Duration::from_secs(1);
    while !shutdown.is_cancelled() {
        let response = tokio::select! {
            () = shutdown.cancelled() => return,
            response = client.register(registration.request()) => response,
        };
        match response {
            Ok(response) => {
                backoff = Duration::from_secs(1);
                let response = response.into_inner();
                let ttl = response
                    .lease_ttl
                    .and_then(|ttl| duration_from_proto(ttl).ok())
                    .unwrap_or(Duration::from_secs(15));
                info!(node = response.node, ?ttl, "node registered");
                session(
                    runtime.clone(),
                    channel.clone(),
                    response.node,
                    ttl,
                    &options,
                    &shutdown,
                )
                .await;
            }
            Err(status) => {
                if matches!(
                    status.code(),
                    Code::FailedPrecondition | Code::PermissionDenied | Code::InvalidArgument
                ) {
                    error!(%status, "registration rejected");
                } else {
                    warn!(%status, "registration failed");
                }
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

struct WorkerHandle {
    reference: PodRef,
    desired: watch::Sender<Pod>,
    task: JoinHandle<()>,
}

/// Runs while the registration is valid: until the heartbeat lease expires or
/// `shutdown` is cancelled.
async fn session<R: Runtime>(
    runtime: Arc<R>,
    channel: Channel,
    node: String,
    ttl: Duration,
    options: &Options,
    shutdown: &CancellationToken,
) {
    let expired = shutdown.child_token();
    let heartbeats = tokio::spawn(heartbeats(
        runtime.clone(),
        NodeAgentServiceClient::new(channel.clone()),
        node.clone(),
        ttl,
        expired.clone(),
    ));

    let (events_sender, mut events) = mpsc::channel(256);
    let filter = pb::PodFilter {
        node: Some(node.clone()),
        unbound: false,
    };
    let informer = tokio::spawn(informer::run(
        PodSource::new(channel.clone(), filter),
        events_sender,
    ));

    let mut desired: HashMap<String, Pod> = HashMap::new();
    let mut workers: HashMap<String, WorkerHandle> = HashMap::new();
    let removing = Arc::new(Mutex::new(HashSet::new()));
    // Nothing is removed from the runtime before the first list of the pods.
    let mut synced = false;
    let mut cleanup = tokio::time::interval(options.cleanup_interval);

    loop {
        tokio::select! {
            () = expired.cancelled() => break,
            event = events.recv() => {
                let Some(event) = event else { break };
                match event {
                    InformerEvent::Synced(pods) => {
                        desired = pods.into_iter().map(|p| (p.name.clone(), p)).collect();
                        synced = true;
                    }
                    InformerEvent::Changed(Change::Added(pod) | Change::Modified(pod)) => {
                        if pod.node() == Some(node.as_str()) {
                            desired.insert(pod.name.clone(), pod);
                        } else {
                            desired.remove(&pod.name);
                        }
                    }
                    InformerEvent::Changed(Change::Deleted(pod)) => {
                        desired.remove(&pod.name);
                    }
                }
                sync_workers(&runtime, &channel, options, &desired, &mut workers);
            }
            _ = cleanup.tick(), if synced => {
                remove_orphans(&runtime, &desired, &removing);
            }
        }
    }

    informer.abort();
    heartbeats.abort();
    for worker in workers.into_values() {
        worker.task.abort();
    }
}

/// Starts, updates and stops workers to match the desired pods.
fn sync_workers<R: Runtime>(
    runtime: &Arc<R>,
    channel: &Channel,
    options: &Options,
    desired: &HashMap<String, Pod>,
    workers: &mut HashMap<String, WorkerHandle>,
) {
    workers.retain(|name, worker| {
        let keep = desired
            .get(name)
            .and_then(PodRef::of)
            .is_some_and(|reference| reference == worker.reference);
        if !keep {
            worker.task.abort();
        }
        keep
    });

    for (name, pod) in desired {
        let Some(reference) = PodRef::of(pod) else {
            continue;
        };
        match workers.get(name) {
            Some(worker) => {
                worker.desired.send_replace(pod.clone());
            }
            None => {
                let (sender, receiver) = watch::channel(pod.clone());
                let worker = Worker::new(
                    runtime.clone(),
                    channel.clone(),
                    reference.clone(),
                    options.restart_backoff,
                );
                workers.insert(
                    name.clone(),
                    WorkerHandle {
                        reference,
                        desired: sender,
                        task: tokio::spawn(worker.run(receiver)),
                    },
                );
            }
        }
    }
}

/// Removes the runtime pods that are not bound to the node anymore (a pod may
/// have been rescheduled while the node was unreachable).
fn remove_orphans<R: Runtime>(
    runtime: &Arc<R>,
    desired: &HashMap<String, Pod>,
    removing: &Arc<Mutex<HashSet<PodRef>>>,
) {
    let expected: HashSet<PodRef> = desired.values().filter_map(PodRef::of).collect();
    let runtime = runtime.clone();
    let removing = removing.clone();
    tokio::spawn(async move {
        let pods = match runtime.list_pods().await {
            Ok(pods) => pods,
            Err(error) => {
                warn!(%error, "failed to list the runtime pods");
                return;
            }
        };
        for pod in pods.into_iter().filter(|p| !expected.contains(p)) {
            if !removing
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(pod.clone())
            {
                continue;
            }
            info!(
                pod = pod.name,
                attempt = pod.attempt,
                "removing pod not bound to this node"
            );
            if let Err(error) = runtime.remove_pod(&pod, ORPHAN_GRACE).await {
                warn!(%error, pod = pod.name, "failed to remove pod");
            }
            removing
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&pod);
        }
    });
}

/// Sends a heartbeat every third of the TTL. Cancels `expired` when Labellum
/// doesn't know the registration anymore.
async fn heartbeats<R: Runtime>(
    runtime: Arc<R>,
    mut client: NodeAgentServiceClient<Channel>,
    node: String,
    ttl: Duration,
    expired: CancellationToken,
) {
    let mut sampler = UsageSampler::default();
    loop {
        tokio::select! {
            () = expired.cancelled() => return,
            () = tokio::time::sleep(ttl / 3) => {}
        }
        let (health, message) = match runtime.health().await {
            Ok(()) => (pb::NodeHealth::Healthy, String::new()),
            Err(error) => (pb::NodeHealth::Unhealthy, error.to_string()),
        };
        let request = pb::HeartbeatRequest {
            node: node.clone(),
            health: health.into(),
            message,
            usage: Some(sampler.sample().into()),
        };
        match client.heartbeat(request).await {
            Ok(_) => {}
            Err(status) if status.code() == Code::NotFound => {
                warn!("registration expired, registering again");
                expired.cancel();
                return;
            }
            Err(status) => warn!(%status, "heartbeat failed"),
        }
    }
}
