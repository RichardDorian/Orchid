//! Leader election of the controller.
//!
//! An instance becomes leader by creating the controller leader key, attached
//! to a lease it keeps alive. The `create_revision` of the key is the fencing
//! token checked by every write of the controller.

use std::sync::Arc;
use std::time::Duration;

use orchid_api::Revision;
use orchid_store::{
    Compare, EventKind, KeyRange, LeaseId, Op, Store, StoreError, Txn, WatchResponse, codec, keys,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::State;
use crate::controller;
use crate::records::LeaderRecord;

struct Leadership {
    lease: LeaseId,
    ttl: Duration,
    token: Revision,
}

/// Campaigns for the controller leadership and runs the controller while
/// leader, until `shutdown` is cancelled.
pub async fn run_controller<S: Store>(
    state: Arc<State<S>>,
    candidate: String,
    shutdown: CancellationToken,
) {
    let key = keys::leader(keys::CONTROLLER_ELECTION);
    while !shutdown.is_cancelled() {
        match campaign(&state, &key, &candidate).await {
            Ok(Ok(leadership)) => {
                info!(token = leadership.token, "leading the controller");
                let lost = shutdown.child_token();
                let keeper = tokio::spawn(keep_alive(
                    state.clone(),
                    leadership.lease,
                    leadership.ttl,
                    lost.clone(),
                ));
                controller::run(state.clone(), leadership.token, lost.clone()).await;
                lost.cancel();
                let _ = keeper.await;
                let _ = state.store.revoke_lease(leadership.lease).await;
                info!("controller leadership released");
            }
            Ok(Err(revision)) => wait_for_vacancy(&state.store, &key, revision, &shutdown).await,
            Err(error) => {
                warn!(%error, "controller election failed");
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    }
}

/// Tries to become leader. `Err(revision)` if another instance leads.
async fn campaign<S: Store>(
    state: &State<S>,
    key: &str,
    candidate: &str,
) -> Result<Result<Leadership, Revision>, StoreError> {
    let ttl = match state.cluster_config().await {
        Ok((config, _)) => config.leader_lease_ttl,
        Err(_) => orchid_api::ClusterConfig::default().leader_lease_ttl,
    };
    let lease = state.store.grant_lease(ttl).await?;
    let record = LeaderRecord {
        candidate: candidate.to_owned(),
    };
    let response = state
        .store
        .txn(
            Txn::new()
                .when([Compare::absent(key)])
                .then([Op::put_with_lease(key, codec::encode(&record), lease.id)]),
        )
        .await;
    match response {
        Ok(response) if response.succeeded => Ok(Ok(Leadership {
            lease: lease.id,
            ttl: lease.ttl,
            token: response.revision,
        })),
        Ok(response) => {
            let _ = state.store.revoke_lease(lease.id).await;
            Ok(Err(response.revision))
        }
        Err(error) => {
            let _ = state.store.revoke_lease(lease.id).await;
            Err(error)
        }
    }
}

/// Renews the lease every third of its TTL. Cancels `lost` when the lease is
/// gone or could not be renewed for a whole TTL.
async fn keep_alive<S: Store>(
    state: Arc<State<S>>,
    lease: LeaseId,
    ttl: Duration,
    lost: CancellationToken,
) {
    let mut renewed = Instant::now();
    loop {
        tokio::select! {
            () = lost.cancelled() => return,
            () = tokio::time::sleep(ttl / 3) => {}
        }
        match state.store.keep_alive(lease).await {
            Ok(()) => renewed = Instant::now(),
            Err(StoreError::LeaseNotFound) => {
                warn!("controller lease expired");
                lost.cancel();
                return;
            }
            Err(error) => {
                warn!(%error, "failed to renew the controller lease");
                if renewed.elapsed() >= ttl {
                    lost.cancel();
                    return;
                }
            }
        }
    }
}

/// Waits until the leader key is deleted.
async fn wait_for_vacancy<S: Store>(
    store: &S,
    key: &str,
    revision: Revision,
    shutdown: &CancellationToken,
) {
    let watch = store.watch(KeyRange::key(key), Some(revision + 1)).await;
    let Ok(mut watch) = watch else {
        tokio::time::sleep(Duration::from_secs(1)).await;
        return;
    };
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            response = watch.recv() => match response {
                Some(Ok(WatchResponse::Events(events))) => {
                    if events.iter().any(|e| e.kind == EventKind::Delete) {
                        return;
                    }
                }
                Some(Ok(WatchResponse::Progress(_))) => {}
                Some(Err(_)) | None => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    return;
                }
            }
        }
    }
}
