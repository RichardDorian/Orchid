//! Ikebana, the scheduler of Orchid.
//!
//! Every instance keeps a cache of the nodes and of the pods waiting to be
//! scheduled. Only the leader schedules: it takes the pod with the highest
//! priority (then the one waiting for the longest time), picks the node with
//! the lowest `max(cpu%, mem%)`, and binds the pod through Labellum.

pub mod config;
mod leadership;
mod scheduler;
pub mod scheduling;

use orchid_client::informer::{self, NodeSource, PodSource};
use orchid_proto::v1 as pb;
use orchid_transport::client::Connection;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub use leadership::ELECTION;

/// Runs Ikebana until `shutdown` is cancelled.
pub async fn run(channel: Connection, candidate: String, shutdown: CancellationToken) {
    let (node_events, nodes) = mpsc::channel(256);
    let (pod_events, pods) = mpsc::channel(256);
    let unbound = pb::PodFilter {
        node: None,
        unbound: true,
    };
    let informers = [
        tokio::spawn(informer::run(NodeSource::new(channel.clone()), node_events)),
        tokio::spawn(informer::run(
            PodSource::new(channel.clone(), unbound),
            pod_events,
        )),
    ];

    let (leader, leader_changes) = watch::channel(None);
    let (stale, stale_signals) = mpsc::channel(1);
    let election = tokio::spawn(leadership::run(
        channel.clone(),
        candidate,
        leader,
        stale_signals,
        shutdown.clone(),
    ));

    scheduler::Scheduler::new(channel)
        .run(nodes, pods, leader_changes, stale, shutdown)
        .await;

    // Release the leadership before returning.
    let _ = election.await;
    for informer in informers {
        informer.abort();
    }
}
