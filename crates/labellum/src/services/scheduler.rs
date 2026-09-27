use orchid_api::{NodeCondition, PodBinding, PodPhase, PodStatus};
use orchid_proto::v1 as pb;
use orchid_proto::v1::scheduler_service_server::SchedulerService;
use orchid_store::{Compare, CompareOp, Op, Store, Txn, codec, keys};
use orchid_transport::errors::with_reason;
use orchid_transport::identity::Identity;
use tonic::{Code, Request, Response, Status};

use super::service;
use crate::ops::{MAX_ATTEMPTS, conflict};
use crate::records::{LeaderRecord, RawNode, RawPod, Versioned, read, store_error, unchanged};

service!(SchedulerApi);

fn failure(code: Code, reason: pb::BindFailure, message: String) -> Status {
    with_reason(code, message, reason.as_str_name())
}

fn aborted(reason: pb::BindFailure, message: String) -> Status {
    failure(Code::Aborted, reason, message)
}

#[tonic::async_trait]
impl<S: Store> SchedulerService for SchedulerApi<S> {
    async fn bind(
        &self,
        request: Request<pb::BindRequest>,
    ) -> Result<Response<pb::BindResponse>, Status> {
        let identity = Identity::of(&request)?;
        if !matches!(identity, Identity::Anonymous | Identity::Ikebana) {
            return Err(identity.denied());
        }
        let request = request.into_inner();
        let store = &self.state.store;
        let leader_key = keys::leader(keys::IKEBANA_ELECTION);

        for _ in 0..MAX_ATTEMPTS {
            let leader: Option<Versioned<LeaderRecord>> = read(store, &leader_key).await?;
            if leader
                .as_ref()
                .is_none_or(|l| l.create_revision != request.leader_token)
            {
                return Err(aborted(
                    pb::BindFailure::StaleLeaderToken,
                    "the leader token is stale".to_owned(),
                ));
            }

            let pod = RawPod::read(store, &request.pod).await?.ok_or_else(|| {
                aborted(
                    pb::BindFailure::PodChanged,
                    format!("pod {} was deleted", request.pod),
                )
            })?;
            if pod.revision() != request.pod_revision || pod.status.value.phase != PodPhase::Pending
            {
                return Err(aborted(
                    pb::BindFailure::PodChanged,
                    format!("pod {} changed", pod.name),
                ));
            }
            if pod.node().is_some() {
                return Err(aborted(
                    pb::BindFailure::PodAlreadyBound,
                    format!("pod {} is already bound", pod.name),
                ));
            }

            let unavailable = |why: &str| {
                aborted(
                    pb::BindFailure::NodeUnavailable,
                    format!("node {} {why}", request.node),
                )
            };
            let node = RawNode::read(store, &request.node)
                .await?
                .ok_or_else(|| unavailable("doesn't exist"))?;
            let spec = &node.spec.value.spec;
            if node.condition() != NodeCondition::Ready {
                return Err(unavailable("is not ready"));
            }
            if !spec.schedulable || spec.draining {
                return Err(unavailable("is not schedulable"));
            }
            let runtime = &pod.spec.value.spec.runtime;
            if !node.info.value.has_runtime(runtime) {
                return Err(unavailable(&format!("doesn't provide runtime {runtime}")));
            }

            let allocated = node
                .allocated()
                .checked_add(pod.resources())
                .filter(|total| total.fits_in(node.info.value.capacity))
                .ok_or_else(|| {
                    failure(
                        Code::ResourceExhausted,
                        pb::BindFailure::InsufficientResources,
                        format!("pod {} doesn't fit on node {}", pod.name, node.name),
                    )
                })?;

            let binding = PodBinding {
                node: node.name.clone(),
                attempt: pod.attempt() + 1,
            };
            let status = PodStatus {
                phase: PodPhase::Creating,
                reason: "Scheduled".to_owned(),
                message: String::new(),
                containers: Vec::new(),
                ..pod.status.value.clone()
            };
            let lease = node.lease.as_ref().map_or(0, |kv| kv.create_revision);
            let mut compares = pod.unchanged();
            compares.extend([
                Compare::create_revision(&leader_key, CompareOp::Equal, request.leader_token),
                unchanged(keys::node::spec(&node.name), Some(&node.spec)),
                unchanged(keys::node::info(&node.name), Some(&node.info)),
                unchanged(keys::node::allocated(&node.name), node.allocated.as_ref()),
                Compare::create_revision(keys::node::lease(&node.name), CompareOp::Equal, lease),
            ]);
            let response = store
                .txn(Txn::new().when(compares).then([
                    Op::put(keys::pod::binding(&pod.name), codec::encode(&binding)),
                    Op::put(keys::pod::status(&pod.name), codec::encode(&status)),
                    Op::put(keys::node::allocated(&node.name), codec::encode(&allocated)),
                ]))
                .await
                .map_err(store_error)?;
            if response.succeeded {
                return Ok(Response::new(pb::BindResponse {
                    binding: Some(binding.into()),
                    revision: response.revision,
                }));
            }
            // Something changed since the reads: the next attempt tells what.
        }
        Err(conflict())
    }
}
