//! Informers keep an up to date view of a collection.
//!
//! An informer lists the collection, then watches it from the revision of the
//! list. When the watch breaks, it resumes from the last revision it received.
//! When that revision is not available anymore (`OUT_OF_RANGE`), it lists again.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures_core::Stream;
use futures_util::StreamExt;
use orchid_api::{Node, Pod, Revision};
use orchid_proto::v1 as pb;
use orchid_proto::v1::node_service_client::NodeServiceClient;
use orchid_proto::v1::pod_service_client::PodServiceClient;
use orchid_transport::client::Connection;
use tokio::sync::mpsc;
use tonic::{Code, Status};
use tracing::{debug, warn};

const MIN_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change<T> {
    Added(T),
    Modified(T),
    /// The object was deleted or stopped matching the filter of the watch.
    Deleted(T),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InformerEvent<T> {
    /// The whole collection, after a (re)list. Replaces the previous state.
    Synced(Vec<T>),
    Changed(Change<T>),
}

/// A watch stream: changes, or `None` for bookmarks, with their revision.
pub type ChangeStream<T> =
    Pin<Box<dyn Stream<Item = Result<(Option<Change<T>>, Revision), Status>> + Send>>;

/// A watchable collection.
pub trait WatchSource: Send + 'static {
    type Object: Send + 'static;

    /// The collection and the revision it was listed at.
    fn list(
        &mut self,
    ) -> impl Future<Output = Result<(Vec<Self::Object>, Revision), Status>> + Send;

    /// Changes after `revision`.
    fn watch(
        &mut self,
        revision: Revision,
    ) -> impl Future<Output = Result<ChangeStream<Self::Object>, Status>> + Send;
}

/// Runs an informer, sending its events to `events` until the receiver is dropped.
pub async fn run<S: WatchSource>(mut source: S, events: mpsc::Sender<InformerEvent<S::Object>>) {
    let mut revision: Option<Revision> = None;
    let mut backoff = MIN_BACKOFF;
    loop {
        if events.is_closed() {
            return;
        }

        let current = match revision {
            Some(revision) => revision,
            None => match source.list().await {
                Ok((objects, listed)) => {
                    if events.send(InformerEvent::Synced(objects)).await.is_err() {
                        return;
                    }
                    revision = Some(listed);
                    backoff = MIN_BACKOFF;
                    listed
                }
                Err(status) => {
                    warn!(%status, "list failed");
                    backoff = sleep(backoff).await;
                    continue;
                }
            },
        };

        match source.watch(current).await {
            Ok(mut stream) => {
                while let Some(item) = stream.next().await {
                    match item {
                        Ok((change, event_revision)) => {
                            revision = Some(event_revision);
                            backoff = MIN_BACKOFF;
                            if let Some(change) = change
                                && events.send(InformerEvent::Changed(change)).await.is_err()
                            {
                                return;
                            }
                        }
                        Err(status) => {
                            if status.code() == Code::OutOfRange {
                                debug!("revision {current} not available anymore, listing again");
                                revision = None;
                            } else {
                                warn!(%status, "watch failed");
                            }
                            break;
                        }
                    }
                }
            }
            Err(status) if status.code() == Code::OutOfRange => revision = None,
            Err(status) => warn!(%status, "watch failed"),
        }
        backoff = sleep(backoff).await;
    }
}

/// Sleeps for `backoff`, returns the next backoff.
async fn sleep(backoff: Duration) -> Duration {
    tokio::time::sleep(backoff).await;
    (backoff * 2).min(MAX_BACKOFF)
}

fn change<T>(kind: i32, object: T) -> Option<Change<T>> {
    match pb::EventType::try_from(kind) {
        Ok(pb::EventType::Added) => Some(Change::Added(object)),
        Ok(pb::EventType::Modified) => Some(Change::Modified(object)),
        Ok(pb::EventType::Deleted) => Some(Change::Deleted(object)),
        _ => None,
    }
}

/// Pods matching a filter.
pub struct PodSource {
    client: PodServiceClient<Connection>,
    filter: pb::PodFilter,
}

impl PodSource {
    pub fn new(channel: Connection, filter: pb::PodFilter) -> Self {
        Self {
            client: PodServiceClient::new(channel),
            filter,
        }
    }
}

impl WatchSource for PodSource {
    type Object = Pod;

    async fn list(&mut self) -> Result<(Vec<Pod>, Revision), Status> {
        let response = self
            .client
            .list_pods(pb::ListPodsRequest {
                filter: Some(self.filter.clone()),
            })
            .await?
            .into_inner();
        let pods = response
            .pods
            .into_iter()
            .filter_map(|pod| {
                Pod::try_from(pod)
                    .inspect_err(|error| warn!(%error, "invalid pod"))
                    .ok()
            })
            .collect();
        Ok((pods, response.revision))
    }

    async fn watch(&mut self, revision: Revision) -> Result<ChangeStream<Pod>, Status> {
        let stream = self
            .client
            .watch_pods(pb::WatchPodsRequest {
                filter: Some(self.filter.clone()),
                from_revision: revision,
            })
            .await?
            .into_inner();
        Ok(Box::pin(stream.map(|event| {
            let event = event?;
            let change = event.pod.and_then(|pod| match Pod::try_from(pod) {
                Ok(pod) => change(event.r#type, pod),
                Err(error) => {
                    warn!(%error, "invalid pod");
                    None
                }
            });
            Ok((change, event.revision))
        })))
    }
}

/// Every node.
pub struct NodeSource {
    client: NodeServiceClient<Connection>,
}

impl NodeSource {
    pub fn new(channel: Connection) -> Self {
        Self {
            client: NodeServiceClient::new(channel),
        }
    }
}

impl WatchSource for NodeSource {
    type Object = Node;

    async fn list(&mut self) -> Result<(Vec<Node>, Revision), Status> {
        let response = self
            .client
            .list_nodes(pb::ListNodesRequest {})
            .await?
            .into_inner();
        let nodes = response
            .nodes
            .into_iter()
            .filter_map(|node| {
                Node::try_from(node)
                    .inspect_err(|error| warn!(%error, "invalid node"))
                    .ok()
            })
            .collect();
        Ok((nodes, response.revision))
    }

    async fn watch(&mut self, revision: Revision) -> Result<ChangeStream<Node>, Status> {
        let stream = self
            .client
            .watch_nodes(pb::WatchNodesRequest {
                from_revision: revision,
            })
            .await?
            .into_inner();
        Ok(Box::pin(stream.map(|event| {
            let event = event?;
            let change = event.node.and_then(|node| match Node::try_from(node) {
                Ok(node) => change(event.r#type, node),
                Err(error) => {
                    warn!(%error, "invalid node");
                    None
                }
            });
            Ok((change, event.revision))
        })))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use futures_util::stream;

    use super::*;

    type ScriptedWatch = Vec<Result<(Option<Change<u32>>, Revision), Status>>;

    /// A source replaying scripted lists and watches.
    struct Script {
        lists: VecDeque<(Vec<u32>, Revision)>,
        watches: VecDeque<ScriptedWatch>,
        report: mpsc::UnboundedSender<Revision>,
    }

    impl WatchSource for Script {
        type Object = u32;

        async fn list(&mut self) -> Result<(Vec<u32>, Revision), Status> {
            self.lists
                .pop_front()
                .ok_or_else(|| Status::unavailable("no more lists"))
        }

        async fn watch(&mut self, revision: Revision) -> Result<ChangeStream<u32>, Status> {
            let _ = self.report.send(revision);
            match self.watches.pop_front() {
                Some(items) => Ok(Box::pin(stream::iter(items))),
                None => Ok(Box::pin(stream::pending())),
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn resumes_then_lists_again_when_the_revision_is_gone() {
        let (report, mut watched_from) = mpsc::unbounded_channel();
        let source = Script {
            lists: VecDeque::from([(vec![1], 10), (vec![1, 2, 3], 30)]),
            watches: VecDeque::from([
                // An event, a bookmark, then the stream breaks.
                vec![
                    Ok((Some(Change::Added(2)), 11)),
                    Ok((None, 15)),
                    Err(Status::unavailable("connection lost")),
                ],
                // Resumed from the bookmark, but it has been compacted.
                vec![Err(Status::out_of_range("too old"))],
            ]),
            report,
        };
        let (events, mut received) = mpsc::channel(16);
        tokio::spawn(run(source, events));

        assert_eq!(received.recv().await, Some(InformerEvent::Synced(vec![1])));
        assert_eq!(
            received.recv().await,
            Some(InformerEvent::Changed(Change::Added(2)))
        );
        assert_eq!(
            received.recv().await,
            Some(InformerEvent::Synced(vec![1, 2, 3]))
        );

        let mut revisions = Vec::new();
        for _ in 0..3 {
            revisions.push(watched_from.recv().await.unwrap());
        }
        assert_eq!(revisions, [10, 15, 30]);
    }
}
