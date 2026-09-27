//! In-memory cache of the objects of a collection, fed by an etcd watch.
//!
//! Objects are split into several keys in etcd, the cache assembles them and
//! keeps a bounded history of object level events (with the object before and
//! after each event). This lets Labellum:
//! - serve lists that are consistent with the revision they return,
//! - serve watches from any revision still in the history, filtered on the
//!   state of the objects before and after each event.
//!
//! Reads that must see every write committed before them first wait for the
//! cache to reach the current etcd revision ([`ObjectCache::wait_for`]).

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use orchid_api::{Node, Pod, Revision};
use orchid_store::{EventKind, KeyRange, KeyValue, Store, WatchEvent, WatchResponse, keys};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{debug, warn};

use crate::records::{RawNode, RawPod, object_name};

/// Number of events kept in the history.
const HISTORY_SIZE: usize = 10_000;
/// How often the cache asks etcd for its progress, so that its revision (and
/// watch bookmarks) advance even without events.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const BROADCAST_CAPACITY: usize = 1024;

/// An object assembled from the keys under `<PREFIX><name>/`.
pub trait CachedObject: Clone + PartialEq + Send + Sync + 'static {
    const PREFIX: &'static str;

    /// `None` if the keys don't form a complete object yet.
    fn assemble(name: &str, kvs: &BTreeMap<String, KeyValue>) -> Option<Self>;
}

impl CachedObject for Pod {
    const PREFIX: &'static str = keys::PODS;

    fn assemble(name: &str, kvs: &BTreeMap<String, KeyValue>) -> Option<Self> {
        RawPod::from_kvs(name, kvs.values())
            .inspect_err(|error| warn!(%error, pod = name, "invalid pod"))
            .ok()
            .flatten()
            .map(|raw| raw.to_pod())
    }
}

impl CachedObject for Node {
    const PREFIX: &'static str = keys::NODES;

    fn assemble(name: &str, kvs: &BTreeMap<String, KeyValue>) -> Option<Self> {
        RawNode::from_kvs(name, kvs.values())
            .inspect_err(|error| warn!(%error, node = name, "invalid node"))
            .ok()
            .flatten()
            .map(|raw| raw.to_node())
    }
}

/// A change of an object. `before` is `None` for a creation, `after` is `None`
/// for a deletion.
#[derive(Debug, PartialEq)]
pub struct ObjectEvent<T> {
    pub revision: Revision,
    pub before: Option<T>,
    pub after: Option<T>,
}

#[derive(Clone)]
enum Message<T> {
    Events(Arc<[Arc<ObjectEvent<T>>]>),
    Progress(Revision),
}

pub struct ObjectCache<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for ObjectCache<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

struct Shared<T> {
    state: Mutex<State<T>>,
    /// Revision of the cache: every event up to it has been applied.
    revision: watch::Sender<Revision>,
    /// Asks the cache task for a progress notification.
    progress: mpsc::UnboundedSender<()>,
}

struct State<T> {
    /// Keys of every object, by object name.
    raw: HashMap<String, BTreeMap<String, KeyValue>>,
    objects: BTreeMap<String, T>,
    history: VecDeque<Arc<ObjectEvent<T>>>,
    /// Every event after this revision is in the history.
    start_revision: Revision,
    revision: Revision,
    /// Replaced when the cache is rebuilt, which closes every subscription.
    messages: broadcast::Sender<Message<T>>,
    ready: bool,
}

impl<T: CachedObject> ObjectCache<T> {
    /// Starts filling the cache from `store` until `shutdown` is cancelled.
    pub fn start<S: Store + Clone>(store: S, shutdown: CancellationToken) -> Self {
        let (revision, _) = watch::channel(0);
        let (progress, progress_requests) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                raw: HashMap::new(),
                objects: BTreeMap::new(),
                history: VecDeque::new(),
                start_revision: 0,
                revision: 0,
                messages: broadcast::channel(BROADCAST_CAPACITY).0,
                ready: false,
            }),
            revision,
            progress,
        });
        tokio::spawn(run(store, shared.clone(), progress_requests, shutdown));
        Self { shared }
    }

    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.shared.lock()
    }

    /// Waits until the cache has applied every event up to `revision`.
    pub async fn wait_for(&self, revision: Revision) -> Result<(), Status> {
        let mut receiver = self.shared.revision.subscribe();
        if *receiver.borrow() >= revision {
            return Ok(());
        }
        let _ = self.shared.progress.send(());
        match tokio::time::timeout(WAIT_TIMEOUT, receiver.wait_for(|r| *r >= revision)).await {
            Ok(Ok(_)) => Ok(()),
            _ => Err(Status::unavailable(format!(
                "cache of {} did not reach revision {revision}",
                T::PREFIX
            ))),
        }
    }

    pub fn get(&self, name: &str) -> Option<T> {
        self.lock().objects.get(name).cloned()
    }

    /// Objects matching `filter`, sorted by name, and the revision of the cache.
    pub fn list(&self, filter: impl Fn(&T) -> bool) -> (Vec<T>, Revision) {
        let state = self.lock();
        let objects = state
            .objects
            .values()
            .filter(|o| filter(o))
            .cloned()
            .collect();
        (objects, state.revision)
    }

    /// Events after `revision`. Fails with `OUT_OF_RANGE` if they are not all
    /// in the history anymore. The cache must have reached `revision`.
    pub fn subscribe(&self, revision: Revision) -> Result<Subscription<T>, Status> {
        let state = self.lock();
        if !state.ready {
            return Err(Status::unavailable("cache not ready"));
        }
        if revision < state.start_revision {
            return Err(Status::out_of_range(format!(
                "revision {revision} is too old, oldest available revision is {}",
                state.start_revision
            )));
        }
        let pending = state
            .history
            .iter()
            .filter(|e| e.revision > revision)
            .cloned()
            .collect();
        Ok(Subscription {
            pending,
            last: revision,
            messages: state.messages.subscribe(),
        })
    }
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<T: CachedObject> Shared<T> {
    /// Rebuilds the cache from a list of every key.
    fn reset(&self, kvs: Vec<KeyValue>, revision: Revision) {
        let mut state = self.lock();
        state.raw.clear();
        for kv in kvs {
            if let Some(name) = object_name(T::PREFIX, &kv.key) {
                let name = name.to_owned();
                state
                    .raw
                    .entry(name)
                    .or_default()
                    .insert(kv.key.clone(), kv);
            }
        }
        state.objects = state
            .raw
            .iter()
            .filter_map(|(name, kvs)| T::assemble(name, kvs).map(|o| (name.clone(), o)))
            .collect();
        state.history.clear();
        state.start_revision = revision;
        state.revision = revision;
        state.messages = broadcast::channel(BROADCAST_CAPACITY).0;
        state.ready = true;
        self.revision.send_replace(revision);
    }

    fn apply(&self, events: Vec<WatchEvent>) {
        let mut state = self.lock();
        let mut events = events.into_iter().peekable();
        while let Some(first) = events.next() {
            let revision = first.revision();
            let mut batch = vec![first];
            while let Some(event) = events.next_if(|e| e.revision() == revision) {
                batch.push(event);
            }
            state.apply_revision(revision, batch);
        }
        let revision = state.revision;
        self.revision.send_replace(revision);
    }

    fn progress(&self, revision: Revision) {
        let mut state = self.lock();
        if revision > state.revision {
            state.revision = revision;
        }
        let revision = state.revision;
        let _ = state.messages.send(Message::Progress(revision));
        self.revision.send_replace(revision);
    }
}

impl<T: CachedObject> State<T> {
    /// Applies the key events of one revision (one transaction).
    fn apply_revision(&mut self, revision: Revision, events: Vec<WatchEvent>) {
        let mut touched = BTreeSet::new();
        for event in events {
            let Some(name) = object_name(T::PREFIX, &event.kv.key).map(str::to_owned) else {
                continue;
            };
            match event.kind {
                EventKind::Put => {
                    self.raw
                        .entry(name.clone())
                        .or_default()
                        .insert(event.kv.key.clone(), event.kv);
                }
                EventKind::Delete => {
                    if let Some(kvs) = self.raw.get_mut(&name) {
                        kvs.remove(&event.kv.key);
                        if kvs.is_empty() {
                            self.raw.remove(&name);
                        }
                    }
                }
            }
            touched.insert(name);
        }

        let mut batch = Vec::new();
        for name in touched {
            let after = self.raw.get(&name).and_then(|kvs| T::assemble(&name, kvs));
            let before = match &after {
                Some(after) => self.objects.insert(name, after.clone()),
                None => self.objects.remove(&name),
            };
            if before != after {
                batch.push(Arc::new(ObjectEvent {
                    revision,
                    before,
                    after,
                }));
            }
        }
        self.revision = revision;
        if batch.is_empty() {
            return;
        }

        self.history.extend(batch.iter().cloned());
        while self.history.len() > HISTORY_SIZE {
            // Evict whole revisions, so that every event after
            // `start_revision` stays in the history.
            let Some(evicted) = self.history.pop_front() else {
                break;
            };
            self.start_revision = evicted.revision;
            while self
                .history
                .front()
                .is_some_and(|e| e.revision == self.start_revision)
            {
                self.history.pop_front();
            }
        }
        let _ = self.messages.send(Message::Events(batch.into()));
    }
}

async fn run<S: Store, T: CachedObject>(
    store: S,
    shared: Arc<Shared<T>>,
    mut progress_requests: mpsc::UnboundedReceiver<()>,
    shutdown: CancellationToken,
) {
    let retry = async || {
        tokio::select! {
            () = shutdown.cancelled() => false,
            () = tokio::time::sleep(Duration::from_secs(1)) => true,
        }
    };

    loop {
        let revision = match store.list(T::PREFIX).await {
            Ok(list) => {
                let revision = list.revision;
                shared.reset(list.kvs, revision);
                debug!(prefix = T::PREFIX, revision, "cache listed");
                revision
            }
            Err(error) => {
                warn!(%error, prefix = T::PREFIX, "failed to list");
                if retry().await {
                    continue;
                }
                return;
            }
        };

        let mut watch = match store
            .watch(KeyRange::prefix(T::PREFIX), Some(revision + 1))
            .await
        {
            Ok(watch) => watch,
            Err(error) => {
                warn!(%error, prefix = T::PREFIX, "failed to watch");
                if retry().await {
                    continue;
                }
                return;
            }
        };

        let mut ticker = tokio::time::interval(PROGRESS_INTERVAL);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => watch.request_progress(),
                Some(()) = progress_requests.recv() => {
                    while progress_requests.try_recv().is_ok() {}
                    watch.request_progress();
                }
                response = watch.recv() => match response {
                    Some(Ok(WatchResponse::Events(events))) => shared.apply(events),
                    Some(Ok(WatchResponse::Progress(revision))) => shared.progress(revision),
                    Some(Err(error)) => {
                        warn!(%error, prefix = T::PREFIX, "watch failed, listing again");
                        break;
                    }
                    None => {
                        warn!(prefix = T::PREFIX, "watch ended, listing again");
                        break;
                    }
                }
            }
        }
        if !retry().await {
            return;
        }
    }
}

pub enum Item<T> {
    Event(Arc<ObjectEvent<T>>),
    /// Every event up to this revision has been delivered.
    Progress(Revision),
}

pub struct Subscription<T> {
    pending: VecDeque<Arc<ObjectEvent<T>>>,
    last: Revision,
    messages: broadcast::Receiver<Message<T>>,
}

impl<T: CachedObject> Subscription<T> {
    pub async fn next(&mut self) -> Result<Item<T>, Status> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                self.last = event.revision;
                return Ok(Item::Event(event));
            }
            match self.messages.recv().await {
                Ok(Message::Events(batch)) => {
                    let last = self.last;
                    self.pending
                        .extend(batch.iter().filter(|e| e.revision > last).cloned());
                }
                Ok(Message::Progress(revision)) => {
                    if revision >= self.last {
                        self.last = revision;
                        return Ok(Item::Progress(revision));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    return Err(Status::unavailable("watch fell behind, resume it"));
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(Status::unavailable(
                        "watch cache was rebuilt, resume the watch",
                    ));
                }
            }
        }
    }
}
