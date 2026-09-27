use jiff::Timestamp;
use orchid_api::proto::{
    container_statuses_from_proto, duration_to_proto, node_health_from_proto, node_role_from_proto,
    parse_cidr, pod_phase_from_proto,
};
use orchid_api::{NodeInfo, NodeSpec, PodPhase, PodStatus, Resources, cidrs_overlap};
use orchid_proto::v1 as pb;
use orchid_proto::v1::node_agent_service_server::NodeAgentService;
use orchid_store::{Compare, CompareOp, Lease, Op, Store, StoreError, Txn, codec, keys};
use orchid_transport::identity::Identity;
use tonic::{Request, Response, Status};
use tracing::info;
use uuid::Uuid;

use super::{invalid, invalid_prefixed, service, validate_name};
use crate::State;
use crate::ops::{self, MAX_ATTEMPTS, Release, conflict};
use crate::records::{
    LeaseRecord, NodeSpecRecord, NodeStatusRecord, PodCidrs, RawNode, RawPod, Versioned, read,
    store_error, unchanged,
};

service!(AgentApi);

/// The node a Keiki agent acts for: taken from its certificate in mTLS mode,
/// from the request in cleartext mode.
fn node_name(identity: &Identity, requested: &str) -> Result<String, Status> {
    match identity {
        Identity::Keiki(node) => {
            if !requested.is_empty() && requested != node {
                return Err(Status::permission_denied(format!(
                    "keiki:{node} cannot act for node {requested}"
                )));
            }
            Ok(node.clone())
        }
        Identity::Anonymous => {
            validate_name("node", requested)?;
            Ok(requested.to_owned())
        }
        _ => Err(identity.denied()),
    }
}

/// The node whose pods the caller can update. `None` in cleartext mode.
fn pod_owner(identity: &Identity) -> Result<Option<&str>, Status> {
    match identity {
        Identity::Keiki(node) => Ok(Some(node)),
        Identity::Anonymous => Ok(None),
        _ => Err(identity.denied()),
    }
}

/// Checks that the pod is bound with `attempt`, to the node of the caller.
fn check_binding(raw: &RawPod, uid: &str, attempt: u32, owner: Option<&str>) -> Result<(), Status> {
    let bound = raw
        .node()
        .is_some_and(|node| owner.is_none_or(|owner| owner == node));
    if raw.spec.value.uid != uid || !bound || raw.attempt() != attempt {
        return Err(Status::failed_precondition(format!(
            "pod {} is not bound to this node with attempt {attempt}",
            raw.name
        )));
    }
    Ok(())
}

#[tonic::async_trait]
impl<S: Store> NodeAgentService for AgentApi<S> {
    async fn register(
        &self,
        request: Request<pb::RegisterRequest>,
    ) -> Result<Response<pb::RegisterResponse>, Status> {
        let identity = Identity::of(&request)?;
        let request = request.into_inner();
        let node = node_name(&identity, &request.node)?;
        let info = NodeInfo {
            role: node_role_from_proto(request.role).map_err(invalid)?,
            runtimes: request.runtimes,
            pod_cidr: parse_cidr(&request.pod_cidr).map_err(invalid)?,
            capacity: request.capacity.map(Into::into).unwrap_or_default(),
        };
        info.validate().map_err(|e| invalid_prefixed("", e))?;

        let (config, _) = self.state.cluster_config().await?;
        let lease = self
            .state
            .store
            .grant_lease(config.node_lease_ttl)
            .await
            .map_err(store_error)?;
        match register(&self.state, &node, &info, request.schedulable, lease).await {
            Ok(()) => {
                info!(node, "node registered");
                Ok(Response::new(pb::RegisterResponse {
                    node,
                    lease_ttl: Some(duration_to_proto(lease.ttl)),
                }))
            }
            Err(status) => {
                let _ = self.state.store.revoke_lease(lease.id).await;
                Err(status)
            }
        }
    }

    async fn heartbeat(
        &self,
        request: Request<pb::HeartbeatRequest>,
    ) -> Result<Response<pb::HeartbeatResponse>, Status> {
        let identity = Identity::of(&request)?;
        let request = request.into_inner();
        let node = node_name(&identity, &request.node)?;
        let health = node_health_from_proto(request.health).map_err(invalid)?;
        let store = &self.state.store;

        let unregistered = || Status::not_found(format!("node {node} is not registered"));
        let key = keys::node::lease(&node);
        let kv = store
            .get(&key)
            .await
            .map_err(store_error)?
            .kv
            .ok_or_else(unregistered)?;
        let lease = kv.lease.ok_or_else(unregistered)?;
        match store.keep_alive(lease).await {
            Ok(()) => {}
            Err(StoreError::LeaseNotFound) => return Err(unregistered()),
            Err(error) => return Err(store_error(error)),
        }

        let status = NodeStatusRecord {
            health,
            message: request.message,
            last_heartbeat: Some(Timestamp::now()),
            usage: request.usage.map(Into::into).unwrap_or_default(),
        };
        let response = store
            .txn(
                Txn::new()
                    .when([Compare::create_revision(
                        &key,
                        CompareOp::Equal,
                        kv.create_revision,
                    )])
                    .then([Op::put(keys::node::status(&node), codec::encode(&status))]),
            )
            .await
            .map_err(store_error)?;
        if !response.succeeded {
            return Err(unregistered());
        }
        Ok(Response::new(pb::HeartbeatResponse {}))
    }

    async fn update_pod_status(
        &self,
        request: Request<pb::UpdatePodStatusRequest>,
    ) -> Result<Response<pb::UpdatePodStatusResponse>, Status> {
        let identity = Identity::of(&request)?;
        let owner = pod_owner(&identity)?;
        let request = request.into_inner();
        let phase = pod_phase_from_proto(request.phase).map_err(invalid)?;
        if !matches!(
            phase,
            PodPhase::Creating | PodPhase::Running | PodPhase::Succeeded | PodPhase::Failed
        ) {
            return Err(Status::invalid_argument(format!(
                "phase cannot be set to {phase:?}"
            )));
        }
        let containers = container_statuses_from_proto(request.containers).map_err(invalid)?;
        let store = &self.state.store;

        for _ in 0..MAX_ATTEMPTS {
            let raw = RawPod::read(store, &request.pod)
                .await?
                .ok_or_else(|| Status::not_found(format!("pod {} not found", request.pod)))?;
            check_binding(&raw, &request.uid, request.attempt, owner)?;

            let current = &raw.status.value;
            // A terminating pod stays terminating, only its containers change.
            let status = if current.phase == PodPhase::Terminating {
                PodStatus {
                    containers: containers.clone(),
                    ..current.clone()
                }
            } else {
                PodStatus {
                    phase,
                    reason: request.reason.clone(),
                    message: request.message.clone(),
                    containers: containers.clone(),
                    ..current.clone()
                }
            };
            if status == *current {
                return Ok(Response::new(pb::UpdatePodStatusResponse {}));
            }
            let response = store
                .txn(Txn::new().when(raw.unchanged()).then([Op::put(
                    keys::pod::status(&raw.name),
                    codec::encode(&status),
                )]))
                .await
                .map_err(store_error)?;
            if response.succeeded {
                return Ok(Response::new(pb::UpdatePodStatusResponse {}));
            }
        }
        Err(conflict())
    }

    async fn finalize_pod(
        &self,
        request: Request<pb::FinalizePodRequest>,
    ) -> Result<Response<pb::FinalizePodResponse>, Status> {
        let identity = Identity::of(&request)?;
        let owner = pod_owner(&identity)?;
        let request = request.into_inner();
        let store = &self.state.store;

        for _ in 0..MAX_ATTEMPTS {
            let raw = RawPod::read(store, &request.pod)
                .await?
                .ok_or_else(|| Status::not_found(format!("pod {} not found", request.pod)))?;
            check_binding(&raw, &request.uid, request.attempt, owner)?;
            if raw.status.value.phase != PodPhase::Terminating {
                return Err(Status::failed_precondition(format!(
                    "pod {} is not terminating",
                    raw.name
                )));
            }
            // Deleted pods are removed, evicted pods are rescheduled.
            if ops::release(store, &raw, Release::finalize(&raw), Vec::new()).await? {
                info!(pod = raw.name, "pod finalized");
                return Ok(Response::new(pb::FinalizePodResponse {}));
            }
        }
        Err(conflict())
    }
}

/// Creates or updates the node and attaches its heartbeat key to `lease`.
async fn register<S: Store>(
    state: &State<S>,
    node: &str,
    info: &NodeInfo,
    schedulable: Option<bool>,
    lease: Lease,
) -> Result<(), Status> {
    let store = &state.store;
    for _ in 0..MAX_ATTEMPTS {
        let (config, config_kv) = state.cluster_config().await?;
        if !info.has_runtime(&config.default_runtime) {
            return Err(Status::failed_precondition(format!(
                "the node must provide the default runtime {}",
                config.default_runtime
            )));
        }

        let cidrs: Option<Versioned<PodCidrs>> = read(store, keys::POD_CIDRS).await?;
        let mut all_cidrs = cidrs.as_ref().map(|c| c.value.clone()).unwrap_or_default();
        if let Some((other, cidr)) = all_cidrs
            .iter()
            .find(|(other, cidr)| other.as_str() != node && cidrs_overlap(cidr, &info.pod_cidr))
        {
            return Err(Status::failed_precondition(format!(
                "pod CIDR {} overlaps the pod CIDR {cidr} of node {other}",
                info.pod_cidr
            )));
        }

        let raw = RawNode::read(store, node).await?;
        if let Some(raw) = &raw {
            check_bound_pods(state, raw, info).await?;
        }

        let now = Timestamp::now();
        all_cidrs.insert(node.to_owned(), info.pod_cidr);
        let status = NodeStatusRecord {
            health: orchid_api::NodeHealth::Healthy,
            message: String::new(),
            last_heartbeat: Some(now),
            usage: Resources::ZERO,
        };
        let mut ops = vec![
            Op::put(keys::POD_CIDRS, codec::encode(&all_cidrs)),
            Op::put(keys::node::info(node), codec::encode(info)),
            Op::put(keys::node::status(node), codec::encode(&status)),
            Op::put_with_lease(
                keys::node::lease(node),
                codec::encode(&LeaseRecord { registered_at: now }),
                lease.id,
            ),
        ];
        if raw.is_none() {
            let spec = NodeSpecRecord {
                uid: Uuid::now_v7().to_string(),
                spec: NodeSpec::initial(info.role, schedulable),
            };
            ops.push(Op::put(keys::node::spec(node), codec::encode(&spec)));
            ops.push(Op::put(
                keys::node::allocated(node),
                codec::encode(&Resources::ZERO),
            ));
        }

        let compares = vec![
            unchanged(keys::CLUSTER_CONFIG.to_owned(), config_kv.as_ref()),
            unchanged(keys::POD_CIDRS.to_owned(), cidrs.as_ref()),
            unchanged(keys::node::spec(node), raw.as_ref().map(|r| &r.spec)),
            unchanged(
                keys::node::allocated(node),
                raw.as_ref().and_then(|r| r.allocated.as_ref()),
            ),
        ];
        let response = store
            .txn(Txn::new().when(compares).then(ops))
            .await
            .map_err(store_error)?;
        if response.succeeded {
            // The previous heartbeat key was attached to another lease.
            if let Some(old) = raw.and_then(|r| r.lease).and_then(|kv| kv.lease)
                && old != lease.id
            {
                let _ = store.revoke_lease(old).await;
            }
            return Ok(());
        }
    }
    Err(conflict())
}

/// Rejects a registration that doesn't fit the pods already bound to the node.
async fn check_bound_pods<S: Store>(
    state: &State<S>,
    raw: &RawNode,
    info: &NodeInfo,
) -> Result<(), Status> {
    if !raw.allocated().fits_in(info.capacity) {
        return Err(Status::failed_precondition(format!(
            "the pods bound to node {} need more than the new capacity, drain the node first",
            raw.name
        )));
    }
    state.sync_pods().await?;
    let (pods, _) = state.pods.list(|pod| pod.node() == Some(raw.name.as_str()));
    if let Some(pod) = pods.iter().find(|pod| !info.has_runtime(&pod.spec.runtime)) {
        return Err(Status::failed_precondition(format!(
            "pod {} bound to node {} needs runtime {}, drain the node first",
            pod.name, raw.name, pod.spec.runtime
        )));
    }
    Ok(())
}
