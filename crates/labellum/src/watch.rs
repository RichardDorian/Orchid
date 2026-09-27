//! gRPC watch streams.

use orchid_api::Revision;
use orchid_proto::v1 as pb;
use orchid_store::{KeyRange, KeyValue, Store, StoreError, WatchEvent, WatchResponse};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::cache::{CachedObject, Item, ObjectEvent, Subscription};
use crate::records::store_error;

const STREAM_BUFFER: usize = 64;

pub type EventStream<E> = ReceiverStream<Result<E, Status>>;

/// Streams the events of a cache subscription, filtered with `matches`.
///
/// An object that starts matching is sent as `ADDED`, one that stops matching
/// as `DELETED` (with its latest state). Progress notifications of the cache
/// are sent as bookmarks.
pub fn objects<T, E>(
    mut subscription: Subscription<T>,
    matches: impl Fn(&T) -> bool + Send + 'static,
    event: impl Fn(pb::EventType, Revision, Option<T>) -> E + Send + 'static,
) -> EventStream<E>
where
    T: CachedObject,
    E: Send + 'static,
{
    let (sender, receiver) = mpsc::channel(STREAM_BUFFER);
    tokio::spawn(async move {
        loop {
            let item = tokio::select! {
                () = sender.closed() => return,
                item = subscription.next() => item,
            };
            let message = match item {
                Ok(Item::Event(e)) => match classify(&e, &matches) {
                    Some((kind, object)) => Ok(event(kind, e.revision, Some(object))),
                    None => continue,
                },
                Ok(Item::Progress(revision)) => Ok(event(pb::EventType::Bookmark, revision, None)),
                Err(status) => Err(status),
            };
            let failed = message.is_err();
            if sender.send(message).await.is_err() || failed {
                return;
            }
        }
    });
    ReceiverStream::new(receiver)
}

fn classify<T: Clone>(
    event: &ObjectEvent<T>,
    matches: &impl Fn(&T) -> bool,
) -> Option<(pb::EventType, T)> {
    let before = event.before.as_ref().filter(|o| matches(o));
    let after = event.after.as_ref().filter(|o| matches(o));
    match (before, after) {
        (None, Some(after)) => Some((pb::EventType::Added, after.clone())),
        (Some(_), Some(after)) => Some((pb::EventType::Modified, after.clone())),
        (Some(before), None) => Some((
            pb::EventType::Deleted,
            event.after.clone().unwrap_or_else(|| before.clone()),
        )),
        (None, None) => None,
    }
}

/// Streams the changes of a single key after `revision`, straight from the store.
///
/// `event` receives the kind of change, its revision and the key after the
/// change (or before it, for deletions). Bookmarks carry no key.
pub async fn key<S, E>(
    store: &S,
    key: String,
    revision: Revision,
    event: impl Fn(pb::EventType, Revision, Option<&KeyValue>) -> Result<E, Status> + Send + 'static,
) -> Result<EventStream<E>, Status>
where
    S: Store,
    E: Send + 'static,
{
    let mut watch = store
        .watch(KeyRange::key(key), Some(revision + 1))
        .await
        .map_err(store_error)?;
    let (sender, receiver) = mpsc::channel(STREAM_BUFFER);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            let response = tokio::select! {
                () = sender.closed() => return,
                _ = ticker.tick() => {
                    watch.request_progress();
                    continue;
                }
                response = watch.recv() => response,
            };
            let messages: Vec<Result<E, Status>> = match response {
                Some(Ok(WatchResponse::Events(events))) => {
                    events.iter().map(|e| key_event(e, &event)).collect()
                }
                Some(Ok(WatchResponse::Progress(revision))) => {
                    vec![event(pb::EventType::Bookmark, revision, None)]
                }
                Some(Err(error)) => vec![Err(store_error(error))],
                None => vec![Err(store_error(StoreError::WatchClosed(
                    "watch ended".to_owned(),
                )))],
            };
            for message in messages {
                let failed = message.is_err();
                if sender.send(message).await.is_err() || failed {
                    return;
                }
            }
        }
    });
    Ok(ReceiverStream::new(receiver))
}

fn key_event<E>(
    e: &WatchEvent,
    event: &impl Fn(pb::EventType, Revision, Option<&KeyValue>) -> Result<E, Status>,
) -> Result<E, Status> {
    match e.kind {
        orchid_store::EventKind::Put if e.kv.version == 1 => {
            event(pb::EventType::Added, e.revision(), Some(&e.kv))
        }
        orchid_store::EventKind::Put => event(pb::EventType::Modified, e.revision(), Some(&e.kv)),
        orchid_store::EventKind::Delete => {
            event(pb::EventType::Deleted, e.revision(), e.prev_kv.as_ref())
        }
    }
}
