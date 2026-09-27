use jiff::Timestamp;
use orchid_api::{Pod, PodPhase, PodSpec, PodStatus};
use orchid_proto::v1 as pb;
use orchid_proto::v1::pod_service_server::PodService;
use orchid_store::{Compare, Op, Store, Txn, codec, keys};
use orchid_transport::identity::Identity;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use super::{invalid, invalid_prefixed, service, validate_name};
use crate::ops::{MAX_ATTEMPTS, conflict};
use crate::records::{PodSpecRecord, RawPod, store_error};
use crate::watch::{self, EventStream};

service!(PodApi);

/// Which pods a filter selects.
#[derive(Clone, Debug)]
enum Filter {
    All,
    Node(String),
    Unbound,
}

impl Filter {
    fn matches(&self, pod: &Pod) -> bool {
        match self {
            Self::All => true,
            Self::Node(node) => pod.node() == Some(node.as_str()),
            Self::Unbound => pod.node().is_none(),
        }
    }
}

/// Validates the filter of a list or watch request against the caller.
fn filter(identity: &Identity, filter: Option<pb::PodFilter>) -> Result<Filter, Status> {
    let filter = filter.unwrap_or_default();
    let filter = match (filter.node, filter.unbound) {
        (Some(_), true) => {
            return Err(Status::invalid_argument(
                "node and unbound cannot both be set",
            ));
        }
        (Some(node), false) => Filter::Node(node),
        (None, true) => Filter::Unbound,
        (None, false) => Filter::All,
    };
    match identity {
        Identity::Anonymous | Identity::User(_) | Identity::Ikebana => Ok(filter),
        Identity::Keiki(node) => match &filter {
            Filter::Node(n) if n == node => Ok(filter),
            _ => Err(Status::permission_denied(format!(
                "keiki:{node} can only see the pods of node {node}"
            ))),
        },
        Identity::Labellum => Err(identity.denied()),
    }
}

#[tonic::async_trait]
impl<S: Store> PodService for PodApi<S> {
    async fn create_pod(
        &self,
        request: Request<pb::CreatePodRequest>,
    ) -> Result<Response<pb::Pod>, Status> {
        Identity::of(&request)?.require_user()?;
        let request = request.into_inner();
        validate_name("name", &request.name)?;
        let mut spec = PodSpec::try_from(request.spec.unwrap_or_default())
            .map_err(|e| invalid(e.prefixed("spec")))?;
        spec.validate().map_err(|e| invalid_prefixed("spec", e))?;

        if spec.runtime.is_empty() {
            let (config, _) = self.state.cluster_config().await?;
            spec.runtime = config.default_runtime;
        }

        let name = request.name;
        let now = Timestamp::now();
        let record = PodSpecRecord {
            uid: Uuid::now_v7().to_string(),
            created_at: now,
            spec,
        };
        let response = self
            .state
            .store
            .txn(
                Txn::new()
                    .when([Compare::absent(keys::pod::spec(&name))])
                    .then([
                        Op::put(keys::pod::spec(&name), codec::encode(&record)),
                        Op::put(
                            keys::pod::status(&name),
                            codec::encode(&PodStatus::pending(now)),
                        ),
                    ]),
            )
            .await
            .map_err(store_error)?;
        if !response.succeeded {
            return Err(Status::already_exists(format!("pod {name} already exists")));
        }
        let pod = self.state.pod_at(&name, response.revision).await?;
        Ok(Response::new(pod.into()))
    }

    async fn get_pod(
        &self,
        request: Request<pb::GetPodRequest>,
    ) -> Result<Response<pb::Pod>, Status> {
        let identity = Identity::of(&request)?;
        let name = request.into_inner().name;
        if identity == Identity::Labellum {
            return Err(identity.denied());
        }
        self.state.sync_pods().await?;
        let not_found = || Status::not_found(format!("pod {name} not found"));
        let pod = self.state.pods.get(&name).ok_or_else(not_found)?;
        // Agents only see the pods of their node.
        if let Identity::Keiki(node) = &identity
            && pod.node() != Some(node.as_str())
        {
            return Err(not_found());
        }
        Ok(Response::new(pod.into()))
    }

    async fn list_pods(
        &self,
        request: Request<pb::ListPodsRequest>,
    ) -> Result<Response<pb::ListPodsResponse>, Status> {
        let identity = Identity::of(&request)?;
        let filter = filter(&identity, request.into_inner().filter)?;
        self.state.sync_pods().await?;
        let (pods, revision) = self.state.pods.list(|pod| filter.matches(pod));
        Ok(Response::new(pb::ListPodsResponse {
            pods: pods.into_iter().map(Into::into).collect(),
            revision,
        }))
    }

    type WatchPodsStream = EventStream<pb::PodEvent>;

    async fn watch_pods(
        &self,
        request: Request<pb::WatchPodsRequest>,
    ) -> Result<Response<Self::WatchPodsStream>, Status> {
        let identity = Identity::of(&request)?;
        let request = request.into_inner();
        let filter = filter(&identity, request.filter)?;
        self.state.pods.wait_for(request.from_revision).await?;
        let subscription = self.state.pods.subscribe(request.from_revision)?;
        let stream = watch::objects(
            subscription,
            move |pod| filter.matches(pod),
            |kind, revision, pod| pb::PodEvent {
                r#type: kind.into(),
                revision,
                pod: pod.map(Into::into),
            },
        );
        Ok(Response::new(stream))
    }

    async fn delete_pod(
        &self,
        request: Request<pb::DeletePodRequest>,
    ) -> Result<Response<pb::Pod>, Status> {
        Identity::of(&request)?.require_user()?;
        let request = request.into_inner();
        let store = &self.state.store;

        for _ in 0..MAX_ATTEMPTS {
            let raw = RawPod::read(store, &request.name)
                .await?
                .ok_or_else(|| Status::not_found(format!("pod {} not found", request.name)))?;
            if request
                .uid
                .as_ref()
                .is_some_and(|uid| *uid != raw.spec.value.uid)
            {
                return Err(Status::failed_precondition("the pod has another uid"));
            }
            let status = &raw.status.value;
            let now = Timestamp::now();

            if raw.node().is_none() {
                // Nothing runs the pod: delete it right away.
                let response = store
                    .txn(
                        Txn::new()
                            .when(raw.unchanged())
                            .then([Op::delete_prefix(keys::pod::prefix(&raw.name))]),
                    )
                    .await
                    .map_err(store_error)?;
                if response.succeeded {
                    let mut pod = raw.to_pod();
                    pod.status.phase = PodPhase::Terminating;
                    pod.status.deletion_requested_at = Some(now);
                    return Ok(Response::new(pod.into()));
                }
                continue;
            }

            if status.deletion_requested_at.is_some() {
                // Already being deleted.
                let pod = self.state.pod_at(&raw.name, raw.revision()).await?;
                return Ok(Response::new(pod.into()));
            }

            // The node stops the containers, then finalizes the pod.
            let terminating = PodStatus {
                phase: PodPhase::Terminating,
                reason: "Deleted".to_owned(),
                message: String::new(),
                deletion_requested_at: Some(now),
                ..status.clone()
            };
            let response = store
                .txn(Txn::new().when(raw.unchanged()).then([Op::put(
                    keys::pod::status(&raw.name),
                    codec::encode(&terminating),
                )]))
                .await
                .map_err(store_error)?;
            if response.succeeded {
                let pod = self.state.pod_at(&raw.name, response.revision).await?;
                return Ok(Response::new(pod.into()));
            }
        }
        Err(conflict())
    }

    async fn update_pod_priority(
        &self,
        request: Request<pb::UpdatePodPriorityRequest>,
    ) -> Result<Response<pb::Pod>, Status> {
        Identity::of(&request)?.require_user()?;
        let request = request.into_inner();
        let store = &self.state.store;

        for _ in 0..MAX_ATTEMPTS {
            let raw = RawPod::read(store, &request.name)
                .await?
                .ok_or_else(|| Status::not_found(format!("pod {} not found", request.name)))?;
            if request
                .revision
                .is_some_and(|expected| expected != raw.revision())
            {
                return Err(Status::failed_precondition(format!(
                    "the pod is at revision {}",
                    raw.revision()
                )));
            }
            let mut record = raw.spec.value.clone();
            record.spec.priority = request.priority;
            let response = store
                .txn(
                    Txn::new()
                        .when(raw.unchanged())
                        .then([Op::put(keys::pod::spec(&raw.name), codec::encode(&record))]),
                )
                .await
                .map_err(store_error)?;
            if response.succeeded {
                let pod = self.state.pod_at(&raw.name, response.revision).await?;
                return Ok(Response::new(pod.into()));
            }
        }
        Err(conflict())
    }
}
