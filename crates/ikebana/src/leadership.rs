//! Leader election through Labellum.
//!
//! The current leadership is published on a watch channel: `Some(token)`
//! while leader, `None` otherwise.

use std::time::Duration;

use orchid_api::proto::duration_from_proto;
use orchid_proto::v1 as pb;
use orchid_proto::v1::leadership_service_client::LeadershipServiceClient;
use orchid_transport::client::Connection;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tracing::{info, warn};

pub const ELECTION: &str = "ikebana";

/// How long a follower waits on the leader key before trying again anyway.
const FOLLOWER_RECHECK: Duration = Duration::from_secs(10);

/// Campaigns until `shutdown` is cancelled. A message on `stale` means Labellum
/// rejected the token: the leadership is considered lost.
pub async fn run(
    channel: Connection,
    candidate: String,
    leader: watch::Sender<Option<i64>>,
    mut stale: mpsc::Receiver<()>,
    shutdown: CancellationToken,
) {
    let mut client = LeadershipServiceClient::new(channel);
    while !shutdown.is_cancelled() {
        let response = client
            .acquire_leadership(pb::AcquireLeadershipRequest {
                election: ELECTION.into(),
                candidate: candidate.clone(),
            })
            .await
            .map(tonic::Response::into_inner);
        match response {
            Ok(response) if response.acquired => {
                let token = response.leader.map_or(0, |l| l.token);
                let ttl = response
                    .ttl
                    .and_then(|ttl| duration_from_proto(ttl).ok())
                    .unwrap_or(Duration::from_secs(15));
                info!(token, "became leader");
                leader.send_replace(Some(token));
                lead(&mut client, token, ttl, &mut stale, &shutdown).await;
                leader.send_replace(None);
                if shutdown.is_cancelled() {
                    let _ = client
                        .release_leadership(pb::ReleaseLeadershipRequest {
                            election: ELECTION.into(),
                            token,
                        })
                        .await;
                    return;
                }
                info!("leadership lost");
            }
            Ok(response) => {
                let current = response.leader.map(|l| l.candidate).unwrap_or_default();
                info!(leader = current, "following");
                follow(&mut client, &shutdown).await;
            }
            Err(status) => {
                warn!(%status, "failed to campaign");
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    }
}

/// Renews the leadership every third of its TTL, until it is lost.
async fn lead(
    client: &mut LeadershipServiceClient<Connection>,
    token: i64,
    ttl: Duration,
    stale: &mut mpsc::Receiver<()>,
    shutdown: &CancellationToken,
) {
    // Leftover signals from a previous leadership.
    while stale.try_recv().is_ok() {}
    let mut renewed = Instant::now();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            Some(()) = stale.recv() => return,
            () = tokio::time::sleep(ttl / 3) => {}
        }
        let result = client
            .renew_leadership(pb::RenewLeadershipRequest {
                election: ELECTION.into(),
                token,
            })
            .await;
        match result {
            Ok(_) => renewed = Instant::now(),
            Err(status) if status.code() == Code::NotFound => return,
            Err(status) => {
                warn!(%status, "failed to renew the leadership");
                // Stop scheduling before the lease can expire.
                if renewed.elapsed() >= ttl * 2 / 3 {
                    return;
                }
            }
        }
    }
}

/// Waits until the leader key is deleted, or for a while.
async fn follow(client: &mut LeadershipServiceClient<Connection>, shutdown: &CancellationToken) {
    let wait = async {
        let current = client
            .get_leader(pb::GetLeaderRequest {
                election: ELECTION.into(),
            })
            .await
            .ok()?
            .into_inner();
        if current.leader.is_none() {
            return Some(());
        }
        let mut events = client
            .watch_leader(pb::WatchLeaderRequest {
                election: ELECTION.into(),
                from_revision: current.revision,
            })
            .await
            .ok()?
            .into_inner();
        while let Some(event) = events.message().await.transpose() {
            if event.ok()?.r#type == i32::from(pb::EventType::Deleted) {
                return Some(());
            }
        }
        None
    };
    tokio::select! {
        () = shutdown.cancelled() => {}
        _ = wait => {}
        () = tokio::time::sleep(FOLLOWER_RECHECK) => {}
    }
}
