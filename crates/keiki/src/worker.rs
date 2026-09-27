//! A worker drives one pod: creates it, starts and restarts its containers,
//! reports its status, and removes it once it is terminating.
//!
//! The desired state of the pod arrives on a watch channel. The worker stops
//! when the channel is closed: the pod is not bound to this node anymore (the
//! agent removes it from the runtime).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use orchid_api::{Pod, PodPhase, Timestamp};
use orchid_proto::v1 as pb;
use orchid_proto::v1::node_agent_service_client::NodeAgentServiceClient;
use orchid_transport::client::Connection;
use tokio::sync::watch;
use tokio::time::Instant;
use tonic::Code;
use tracing::{debug, info, warn};

use crate::runtime::{ContainerState, PodRef, Runtime, RuntimeError};
use crate::status::{self, BACKOFF_RESET, ContainerRecord, MAX_BACKOFF};

/// How often running containers are checked.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

pub struct Worker<R> {
    runtime: Arc<R>,
    client: NodeAgentServiceClient<Connection>,
    reference: PodRef,
    created: bool,
    containers: HashMap<String, ContainerRecord>,
    /// First delay of the restart and creation backoffs.
    initial_backoff: Duration,
    /// Delay before retrying a failed creation.
    create_backoff: Duration,
    /// No creation is attempted before this instant. Wakeups caused by our own
    /// status reports must not shorten the backoff.
    create_after: Option<Instant>,
    last_report: Option<pb::UpdatePodStatusRequest>,
}

impl<R: Runtime> Worker<R> {
    pub fn new(
        runtime: Arc<R>,
        channel: Connection,
        reference: PodRef,
        initial_backoff: Duration,
    ) -> Self {
        Self {
            runtime,
            client: NodeAgentServiceClient::new(channel),
            reference,
            created: false,
            containers: HashMap::new(),
            initial_backoff,
            create_backoff: initial_backoff,
            create_after: None,
            last_report: None,
        }
    }

    pub async fn run(mut self, mut desired: watch::Receiver<Pod>) {
        // The pod may already exist, e.g. created before Keiki restarted.
        self.created = match self.runtime.list_pods().await {
            Ok(pods) => pods.contains(&self.reference),
            Err(error) => {
                warn!(%error, pod = self.reference.name, "failed to list the runtime pods");
                false
            }
        };

        loop {
            let pod = desired.borrow_and_update().clone();
            if pod.status.phase == PodPhase::Terminating {
                self.terminate(&pod, &mut desired).await;
                return;
            }

            let now = Instant::now();
            let wait = if self.created {
                self.supervise(&pod).await;
                POLL_INTERVAL
            } else if let Some(after) = self.create_after.filter(|after| now < *after) {
                after - now
            } else {
                match self.create(&pod).await {
                    Ok(()) => Duration::ZERO,
                    Err(error) => {
                        warn!(%error, pod = pod.name, "failed to create pod");
                        self.report(
                            &pod,
                            PodPhase::Creating,
                            "CreateFailed",
                            error.to_string(),
                            Vec::new(),
                        )
                        .await;
                        let wait = self.create_backoff;
                        self.create_after = Some(Instant::now() + wait);
                        self.create_backoff = (self.create_backoff * 2).min(MAX_BACKOFF);
                        wait
                    }
                }
            };

            tokio::select! {
                changed = desired.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                () = tokio::time::sleep(wait) => {}
            }
        }
    }

    async fn create(&mut self, pod: &Pod) -> Result<(), RuntimeError> {
        // After a failure, the failure stays visible until the next attempt succeeds.
        if self.create_after.is_none() {
            self.report(
                pod,
                PodPhase::Creating,
                "Creating",
                String::new(),
                Vec::new(),
            )
            .await;
        }
        info!(
            pod = pod.name,
            attempt = self.reference.attempt,
            "creating pod"
        );
        if let Err(error) = self.runtime.create_pod(pod, self.reference.attempt).await {
            // Start from scratch on the next attempt.
            let _ = self
                .runtime
                .remove_pod(&self.reference, Duration::ZERO)
                .await;
            return Err(error);
        }
        self.created = true;
        self.create_backoff = self.initial_backoff;
        self.create_after = None;
        Ok(())
    }

    /// Starts containers that need it and reports the status of the pod.
    async fn supervise(&mut self, pod: &Pod) {
        let infos = match self.runtime.pod_status(&self.reference).await {
            Ok(infos) => infos,
            Err(RuntimeError::NotFound(_)) => {
                warn!(
                    pod = pod.name,
                    "pod disappeared from the runtime, creating it again"
                );
                self.created = false;
                return;
            }
            Err(error) => {
                warn!(%error, pod = pod.name, "failed to get the pod status");
                return;
            }
        };

        let policy = pod.spec.restart_policy;
        let now = Instant::now();
        for info in &infos {
            let initial_backoff = self.initial_backoff;
            let record =
                self.containers
                    .entry(info.name.clone())
                    .or_insert_with(|| ContainerRecord {
                        backoff: initial_backoff,
                        ..ContainerRecord::default()
                    });
            let start = match info.state {
                ContainerState::Created => true,
                ContainerState::Running => {
                    let since = *record.running_since.get_or_insert(now);
                    if now.duration_since(since) >= BACKOFF_RESET {
                        record.backoff = initial_backoff;
                    }
                    record.started_at.get_or_insert_with(Timestamp::now);
                    false
                }
                ContainerState::Exited { code, .. } => {
                    record.running_since = None;
                    policy.should_restart(code)
                        && now >= *record.restart_at.get_or_insert(now + record.backoff)
                }
            };
            if !start {
                continue;
            }
            match self
                .runtime
                .start_container(&self.reference, &info.name)
                .await
            {
                Ok(()) => {
                    if record.started_at.is_some() {
                        record.restart_count += 1;
                        record.backoff = (record.backoff * 2).min(MAX_BACKOFF);
                        info!(
                            pod = pod.name,
                            container = info.name,
                            restarts = record.restart_count,
                            "container restarted"
                        );
                    }
                    record.started_at = Some(Timestamp::now());
                    record.running_since = Some(now);
                    record.restart_at = None;
                }
                Err(error) => {
                    warn!(%error, pod = pod.name, container = info.name, "failed to start container")
                }
            }
        }

        // Report the state after the starts.
        let infos = match self.runtime.pod_status(&self.reference).await {
            Ok(infos) => infos,
            Err(_) => infos,
        };
        let default = ContainerRecord::default();
        let records: Vec<&ContainerRecord> = infos
            .iter()
            .map(|info| self.containers.get(&info.name).unwrap_or(&default))
            .collect();
        let phase = status::phase(policy, &infos, &records);
        let containers = infos
            .iter()
            .zip(&records)
            .map(|(info, record)| status::container_status(policy, info, record))
            .collect();
        let reason = match phase {
            PodPhase::Running => "Started",
            PodPhase::Succeeded => "Completed",
            PodPhase::Failed => "Error",
            _ => "Creating",
        };
        self.report(pod, phase, reason, String::new(), containers)
            .await;
    }

    /// Stops and removes the pod, then finalizes it.
    async fn terminate(&mut self, pod: &Pod, desired: &mut watch::Receiver<Pod>) {
        let mut backoff = Duration::from_secs(1);
        info!(pod = pod.name, "terminating pod");
        loop {
            let removed = self
                .runtime
                .remove_pod(&self.reference, pod.spec.termination_grace_period)
                .await;
            let result = match removed {
                Ok(()) => self
                    .client
                    .finalize_pod(pb::FinalizePodRequest {
                        pod: pod.name.clone(),
                        uid: pod.uid.clone(),
                        attempt: self.reference.attempt,
                    })
                    .await
                    .map(|_| ())
                    .map_err(|status| status.to_string()),
                Err(error) => Err(error.to_string()),
            };
            match result {
                Ok(()) => {
                    info!(pod = pod.name, "pod finalized");
                    return;
                }
                Err(error) => warn!(%error, pod = pod.name, "failed to terminate pod"),
            }
            tokio::select! {
                changed = desired.changed() => {
                    // The pod left the node: nothing left to finalize.
                    if changed.is_err() {
                        return;
                    }
                }
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }

    /// Reports the status if it changed since the last report.
    async fn report(
        &mut self,
        pod: &Pod,
        phase: PodPhase,
        reason: &str,
        message: String,
        containers: Vec<orchid_api::ContainerStatus>,
    ) {
        let request = pb::UpdatePodStatusRequest {
            pod: pod.name.clone(),
            uid: pod.uid.clone(),
            attempt: self.reference.attempt,
            phase: pb::PodPhase::from(phase).into(),
            reason: reason.to_owned(),
            message,
            containers: containers.into_iter().map(Into::into).collect(),
        };
        if self.last_report.as_ref() == Some(&request) {
            return;
        }
        match self.client.update_pod_status(request.clone()).await {
            Ok(_) => self.last_report = Some(request),
            // The pod is being rescheduled or deleted, the agent stops this worker.
            Err(status) if matches!(status.code(), Code::FailedPrecondition | Code::NotFound) => {
                debug!(%status, pod = pod.name, "status rejected");
                self.last_report = Some(request);
            }
            Err(status) => warn!(%status, pod = pod.name, "failed to report status"),
        }
    }
}
