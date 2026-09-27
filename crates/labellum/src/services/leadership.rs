use orchid_api::proto::duration_to_proto;
use orchid_proto::v1 as pb;
use orchid_proto::v1::leadership_service_server::LeadershipService;
use orchid_store::{Compare, Op, Store, Txn, codec, keys};
use orchid_transport::identity::Identity;
use tonic::{Request, Response, Status};

use super::{service, validate_name};
use crate::records::{LeaderRecord, Versioned, store_error};
use crate::watch::{self, EventStream};

service!(LeadershipApi);

/// Checks the election name. The controller election is internal to Labellum.
fn election_key(election: &str) -> Result<String, Status> {
    validate_name("election", election)?;
    if election == keys::CONTROLLER_ELECTION {
        return Err(Status::permission_denied(
            "the controller election is internal to Labellum",
        ));
    }
    Ok(keys::leader(election))
}

/// Ikebana can only take part in its own election.
fn require_candidate(identity: &Identity, election: &str) -> Result<(), Status> {
    match identity {
        Identity::Anonymous => Ok(()),
        Identity::Ikebana if election == keys::IKEBANA_ELECTION => Ok(()),
        _ => Err(identity.denied()),
    }
}

fn leader_proto(versioned: &Versioned<LeaderRecord>) -> pb::Leader {
    pb::Leader {
        candidate: versioned.value.candidate.clone(),
        token: versioned.create_revision,
    }
}

#[tonic::async_trait]
impl<S: Store> LeadershipService for LeadershipApi<S> {
    async fn get_leader(
        &self,
        request: Request<pb::GetLeaderRequest>,
    ) -> Result<Response<pb::GetLeaderResponse>, Status> {
        Identity::of(&request)?;
        let key = election_key(&request.into_inner().election)?;
        let response = self.state.store.get(&key).await.map_err(store_error)?;
        let leader = response
            .kv
            .as_ref()
            .map(Versioned::decode)
            .transpose()
            .map_err(store_error)?;
        Ok(Response::new(pb::GetLeaderResponse {
            leader: leader.as_ref().map(leader_proto),
            revision: response.revision,
        }))
    }

    async fn acquire_leadership(
        &self,
        request: Request<pb::AcquireLeadershipRequest>,
    ) -> Result<Response<pb::AcquireLeadershipResponse>, Status> {
        let identity = Identity::of(&request)?;
        let request = request.into_inner();
        let key = election_key(&request.election)?;
        require_candidate(&identity, &request.election)?;
        if request.candidate.is_empty() {
            return Err(Status::invalid_argument("candidate: must not be empty"));
        }
        let store = &self.state.store;

        let (config, _) = self.state.cluster_config().await?;
        let lease = store
            .grant_lease(config.leader_lease_ttl)
            .await
            .map_err(store_error)?;
        let record = LeaderRecord {
            candidate: request.candidate.clone(),
        };
        let response = store
            .txn(
                Txn::new()
                    .when([Compare::absent(&key)])
                    .then([Op::put_with_lease(&key, codec::encode(&record), lease.id)])
                    .otherwise([Op::get(&key)]),
            )
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                let _ = store.revoke_lease(lease.id).await;
                return Err(store_error(error));
            }
        };

        if response.succeeded {
            return Ok(Response::new(pb::AcquireLeadershipResponse {
                acquired: true,
                leader: Some(pb::Leader {
                    candidate: request.candidate,
                    token: response.revision,
                }),
                ttl: Some(duration_to_proto(lease.ttl)),
            }));
        }
        let _ = store.revoke_lease(lease.id).await;
        let current = response
            .get(0)
            .map(Versioned::<LeaderRecord>::decode)
            .transpose()
            .map_err(store_error)?;
        Ok(Response::new(pb::AcquireLeadershipResponse {
            acquired: false,
            leader: current.as_ref().map(leader_proto),
            ttl: None,
        }))
    }

    async fn renew_leadership(
        &self,
        request: Request<pb::RenewLeadershipRequest>,
    ) -> Result<Response<pb::RenewLeadershipResponse>, Status> {
        let identity = Identity::of(&request)?;
        let request = request.into_inner();
        let key = election_key(&request.election)?;
        require_candidate(&identity, &request.election)?;
        let store = &self.state.store;

        let lost = || Status::not_found("the leadership has been lost");
        let kv = store
            .get(&key)
            .await
            .map_err(store_error)?
            .kv
            .ok_or_else(lost)?;
        if kv.create_revision != request.token {
            return Err(lost());
        }
        let lease = kv.lease.ok_or_else(lost)?;
        store.keep_alive(lease).await.map_err(|error| match error {
            orchid_store::StoreError::LeaseNotFound => lost(),
            error => store_error(error),
        })?;
        let (config, _) = self.state.cluster_config().await?;
        Ok(Response::new(pb::RenewLeadershipResponse {
            ttl: Some(duration_to_proto(config.leader_lease_ttl)),
        }))
    }

    async fn release_leadership(
        &self,
        request: Request<pb::ReleaseLeadershipRequest>,
    ) -> Result<Response<pb::ReleaseLeadershipResponse>, Status> {
        let identity = Identity::of(&request)?;
        let request = request.into_inner();
        let key = election_key(&request.election)?;
        require_candidate(&identity, &request.election)?;
        let store = &self.state.store;

        let kv = store.get(&key).await.map_err(store_error)?.kv;
        if let Some(kv) = kv
            && kv.create_revision == request.token
            && let Some(lease) = kv.lease
        {
            match store.revoke_lease(lease).await {
                Ok(()) | Err(orchid_store::StoreError::LeaseNotFound) => {}
                Err(error) => return Err(store_error(error)),
            }
        }
        Ok(Response::new(pb::ReleaseLeadershipResponse {}))
    }

    type WatchLeaderStream = EventStream<pb::LeaderEvent>;

    async fn watch_leader(
        &self,
        request: Request<pb::WatchLeaderRequest>,
    ) -> Result<Response<Self::WatchLeaderStream>, Status> {
        Identity::of(&request)?;
        let request = request.into_inner();
        let key = election_key(&request.election)?;
        let stream = watch::key(
            &self.state.store,
            key,
            request.from_revision,
            |kind, revision, kv| {
                let leader = kv
                    .map(Versioned::<LeaderRecord>::decode)
                    .transpose()
                    .map_err(store_error)?;
                Ok(pb::LeaderEvent {
                    r#type: kind.into(),
                    revision,
                    leader: leader.as_ref().map(leader_proto),
                })
            },
        )
        .await?;
        Ok(Response::new(stream))
    }
}
