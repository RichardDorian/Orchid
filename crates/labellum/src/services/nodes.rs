use orchid_proto::v1 as pb;
use orchid_proto::v1::node_service_server::NodeService;
use orchid_store::{Op, Store, Txn, codec, keys};
use orchid_transport::identity::Identity;
use tonic::{Request, Response, Status};
use tracing::info;

use super::service;
use crate::ops::{MAX_ATTEMPTS, conflict};
use crate::records::{PodCidrs, RawNode, Versioned, read, store_error, unchanged};
use crate::watch::{self, EventStream};

service!(NodeApi);

/// Users and Ikebana can read nodes.
fn require_reader(identity: &Identity) -> Result<(), Status> {
    match identity {
        Identity::Anonymous | Identity::User(_) | Identity::Ikebana => Ok(()),
        _ => Err(identity.denied()),
    }
}

#[tonic::async_trait]
impl<S: Store> NodeService for NodeApi<S> {
    async fn get_node(
        &self,
        request: Request<pb::GetNodeRequest>,
    ) -> Result<Response<pb::Node>, Status> {
        require_reader(&Identity::of(&request)?)?;
        let name = request.into_inner().name;
        self.state.sync_nodes().await?;
        let node = self
            .state
            .nodes
            .get(&name)
            .ok_or_else(|| Status::not_found(format!("node {name} not found")))?;
        Ok(Response::new(node.into()))
    }

    async fn list_nodes(
        &self,
        request: Request<pb::ListNodesRequest>,
    ) -> Result<Response<pb::ListNodesResponse>, Status> {
        require_reader(&Identity::of(&request)?)?;
        self.state.sync_nodes().await?;
        let (nodes, revision) = self.state.nodes.list(|_| true);
        Ok(Response::new(pb::ListNodesResponse {
            nodes: nodes.into_iter().map(Into::into).collect(),
            revision,
        }))
    }

    type WatchNodesStream = EventStream<pb::NodeEvent>;

    async fn watch_nodes(
        &self,
        request: Request<pb::WatchNodesRequest>,
    ) -> Result<Response<Self::WatchNodesStream>, Status> {
        require_reader(&Identity::of(&request)?)?;
        let revision = request.into_inner().from_revision;
        self.state.nodes.wait_for(revision).await?;
        let subscription = self.state.nodes.subscribe(revision)?;
        let stream = watch::objects(
            subscription,
            |_| true,
            |kind, revision, node| pb::NodeEvent {
                r#type: kind.into(),
                revision,
                node: node.map(Into::into),
            },
        );
        Ok(Response::new(stream))
    }

    async fn update_node_spec(
        &self,
        request: Request<pb::UpdateNodeSpecRequest>,
    ) -> Result<Response<pb::Node>, Status> {
        Identity::of(&request)?.require_user()?;
        let request = request.into_inner();
        let store = &self.state.store;

        for _ in 0..MAX_ATTEMPTS {
            let raw = RawNode::read(store, &request.name)
                .await?
                .ok_or_else(|| Status::not_found(format!("node {} not found", request.name)))?;
            let compares = match request.revision {
                Some(expected) if expected != raw.revision() => {
                    return Err(Status::failed_precondition(format!(
                        "the node is at revision {}",
                        raw.revision()
                    )));
                }
                Some(_) => raw.unchanged(),
                None => vec![unchanged(keys::node::spec(&raw.name), Some(&raw.spec))],
            };

            let mut record = raw.spec.value.clone();
            if let Some(schedulable) = request.schedulable {
                record.spec.schedulable = schedulable;
            }
            if let Some(draining) = request.draining {
                record.spec.draining = draining;
            }
            let response = store
                .txn(
                    Txn::new()
                        .when(compares)
                        .then([Op::put(keys::node::spec(&raw.name), codec::encode(&record))]),
                )
                .await
                .map_err(store_error)?;
            if response.succeeded {
                let node = self.state.node_at(&raw.name, response.revision).await?;
                return Ok(Response::new(node.into()));
            }
        }
        Err(conflict())
    }

    async fn delete_node(
        &self,
        request: Request<pb::DeleteNodeRequest>,
    ) -> Result<Response<pb::DeleteNodeResponse>, Status> {
        Identity::of(&request)?.require_user()?;
        let request = request.into_inner();
        let store = &self.state.store;

        for _ in 0..MAX_ATTEMPTS {
            let raw = RawNode::read(store, &request.name)
                .await?
                .ok_or_else(|| Status::not_found(format!("node {} not found", request.name)))?;
            if request
                .uid
                .as_ref()
                .is_some_and(|uid| *uid != raw.spec.value.uid)
            {
                return Err(Status::failed_precondition("the node has another uid"));
            }
            let cidrs: Option<Versioned<PodCidrs>> = read(store, keys::POD_CIDRS).await?;
            let mut remaining = cidrs.as_ref().map(|c| c.value.clone()).unwrap_or_default();
            remaining.remove(&raw.name);

            let response = store
                .txn(
                    Txn::new()
                        .when([
                            unchanged(keys::node::spec(&raw.name), Some(&raw.spec)),
                            unchanged(keys::POD_CIDRS.to_owned(), cidrs.as_ref()),
                        ])
                        .then([
                            Op::delete_prefix(keys::node::prefix(&raw.name)),
                            Op::put(keys::POD_CIDRS, codec::encode(&remaining)),
                        ]),
                )
                .await
                .map_err(store_error)?;
            if response.succeeded {
                // The controller reschedules the pods of the node.
                if let Some(lease) = raw.lease.as_ref().and_then(|kv| kv.lease) {
                    let _ = store.revoke_lease(lease).await;
                }
                info!(node = raw.name, "node deleted");
                return Ok(Response::new(pb::DeleteNodeResponse {}));
            }
        }
        Err(conflict())
    }
}
