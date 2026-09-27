use orchid_api::proto::{cluster_config_to_proto, duration_from_proto};
use orchid_api::{ClusterConfig, NodeInfo};
use orchid_proto::prost_types;
use orchid_proto::v1 as pb;
use orchid_proto::v1::cluster_service_server::ClusterService;
use orchid_store::{Op, Store, Txn, codec, keys};
use orchid_transport::identity::Identity;
use tonic::{Request, Response, Status};

use super::{invalid, invalid_prefixed, service};
use crate::ops::{MAX_ATTEMPTS, conflict};
use crate::records::{PodCidrs, Versioned, read, store_error, unchanged};
use crate::watch::{self, EventStream};

service!(ClusterApi);

#[tonic::async_trait]
impl<S: Store> ClusterService for ClusterApi<S> {
    async fn get_cluster_config(
        &self,
        request: Request<pb::GetClusterConfigRequest>,
    ) -> Result<Response<pb::ClusterConfig>, Status> {
        Identity::of(&request)?;
        let (config, versioned) = self.state.cluster_config().await?;
        let revision = versioned.map_or(0, |v| v.mod_revision);
        Ok(Response::new(cluster_config_to_proto(config, revision)))
    }

    async fn update_cluster_config(
        &self,
        request: Request<pb::UpdateClusterConfigRequest>,
    ) -> Result<Response<pb::ClusterConfig>, Status> {
        Identity::of(&request)?.require_user()?;
        let request = request.into_inner();
        let store = &self.state.store;

        for _ in 0..MAX_ATTEMPTS {
            let (mut config, versioned) = self.state.cluster_config().await?;
            let revision = versioned.as_ref().map_or(0, |v| v.mod_revision);
            if request
                .revision
                .is_some_and(|expected| expected != revision)
            {
                return Err(Status::failed_precondition(format!(
                    "the cluster configuration is at revision {revision}"
                )));
            }
            apply(&mut config, &request)?;
            config
                .validate()
                .map_err(|errors| invalid_prefixed("", errors))?;

            // Registrations always write the pod CIDRs key: checking it has not
            // changed guarantees no node registered without the new default
            // runtime in the meantime.
            let cidrs: Option<Versioned<PodCidrs>> = read(store, keys::POD_CIDRS).await?;
            let infos = store
                .list(keys::NODES)
                .await
                .map_err(store_error)?
                .kvs
                .into_iter()
                .filter(|kv| kv.key.ends_with("/info"));
            for kv in infos {
                let info: NodeInfo = codec::decode(&kv).map_err(store_error)?;
                if !info.has_runtime(&config.default_runtime) {
                    let node =
                        crate::records::object_name(keys::NODES, &kv.key).unwrap_or_default();
                    return Err(Status::failed_precondition(format!(
                        "node {node} doesn't provide runtime {}",
                        config.default_runtime
                    )));
                }
            }

            let response = store
                .txn(
                    Txn::new()
                        .when([
                            unchanged(keys::CLUSTER_CONFIG.to_owned(), versioned.as_ref()),
                            unchanged(keys::POD_CIDRS.to_owned(), cidrs.as_ref()),
                        ])
                        .then([Op::put(keys::CLUSTER_CONFIG, codec::encode(&config))]),
                )
                .await
                .map_err(store_error)?;
            if response.succeeded {
                return Ok(Response::new(cluster_config_to_proto(
                    config,
                    response.revision,
                )));
            }
        }
        Err(conflict())
    }

    type WatchClusterConfigStream = EventStream<pb::ClusterConfigEvent>;

    async fn watch_cluster_config(
        &self,
        request: Request<pb::WatchClusterConfigRequest>,
    ) -> Result<Response<Self::WatchClusterConfigStream>, Status> {
        Identity::of(&request)?;
        let revision = request.into_inner().from_revision;
        let stream = watch::key(
            &self.state.store,
            keys::CLUSTER_CONFIG.to_owned(),
            revision,
            |kind, revision, kv| {
                let config = match (kind, kv) {
                    (pb::EventType::Added | pb::EventType::Modified, Some(kv)) => {
                        let config: ClusterConfig = codec::decode(kv).map_err(store_error)?;
                        Some(cluster_config_to_proto(config, kv.mod_revision))
                    }
                    _ => None,
                };
                Ok(pb::ClusterConfigEvent {
                    r#type: kind.into(),
                    revision,
                    config,
                })
            },
        )
        .await?;
        Ok(Response::new(stream))
    }
}

fn apply(
    config: &mut ClusterConfig,
    request: &pb::UpdateClusterConfigRequest,
) -> Result<(), Status> {
    let duration = |value: prost_types::Duration, field: &str| {
        duration_from_proto(value).map_err(|e| invalid(e.prefixed(field)))
    };
    if let Some(runtime) = &request.default_runtime {
        config.default_runtime.clone_from(runtime);
    }
    if let Some(ttl) = request.node_lease_ttl {
        config.node_lease_ttl = duration(ttl, "node_lease_ttl")?;
    }
    if let Some(delay) = request.pod_eviction_delay {
        config.pod_eviction_delay = duration(delay, "pod_eviction_delay")?;
    }
    if let Some(ttl) = request.leader_lease_ttl {
        config.leader_lease_ttl = duration(ttl, "leader_lease_ttl")?;
    }
    Ok(())
}
