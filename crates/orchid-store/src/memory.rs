use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::{
    EventKind, GetResponse, KeyRange, KeyValue, Lease, LeaseId, ListResponse, Op, OpResponse,
    Result, Revision, Store, StoreError, Txn, TxnResponse, Watch, WatchEvent, WatchResponse,
};

/// How often expired leases are collected.
const LEASE_REAPER_INTERVAL: Duration = Duration::from_millis(100);

/// In-memory [`Store`] with the semantics of etcd, for tests.
///
/// Lease expiry uses [`tokio::time`], so tests can run with a paused clock.
#[derive(Clone)]
pub struct MemoryStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStore {
    /// Creates an empty store.
    ///
    /// # Panics
    ///
    /// Must be called from within a Tokio runtime: a background task expires leases.
    pub fn new() -> Self {
        let inner = Arc::new(Mutex::new(Inner::new()));
        tokio::spawn(reap_leases(Arc::downgrade(&inner)));
        Self { inner }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        lock(&self.inner)
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

fn lock(inner: &Mutex<Inner>) -> MutexGuard<'_, Inner> {
    // The state stays consistent even if a thread panicked while holding the
    // lock: every mutation is applied after validation, without early returns.
    inner.lock().unwrap_or_else(PoisonError::into_inner)
}

async fn reap_leases(inner: Weak<Mutex<Inner>>) {
    let mut interval = tokio::time::interval(LEASE_REAPER_INTERVAL);
    loop {
        interval.tick().await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        lock(&inner).expire_leases(Instant::now());
    }
}

impl Store for MemoryStore {
    async fn get(&self, key: &str) -> Result<GetResponse> {
        let mut inner = self.lock();
        inner.expire_leases(Instant::now());
        Ok(GetResponse {
            kv: inner.data.get(key).cloned(),
            revision: inner.revision,
        })
    }

    async fn list(&self, prefix: &str) -> Result<ListResponse> {
        let mut inner = self.lock();
        inner.expire_leases(Instant::now());
        Ok(ListResponse {
            kvs: inner
                .keys_with_prefix(prefix)
                .map(|(_, kv)| kv.clone())
                .collect(),
            revision: inner.revision,
        })
    }

    async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
        let mut inner = self.lock();
        inner.expire_leases(Instant::now());
        inner.txn(txn)
    }

    async fn watch(&self, range: KeyRange, start_revision: Option<Revision>) -> Result<Watch> {
        let mut inner = self.lock();
        inner.expire_leases(Instant::now());
        Ok(inner.watch(Arc::downgrade(&self.inner), range, start_revision))
    }

    async fn grant_lease(&self, ttl: Duration) -> Result<Lease> {
        Ok(self.lock().grant_lease(ttl, Instant::now()))
    }

    async fn keep_alive(&self, lease: LeaseId) -> Result<()> {
        let now = Instant::now();
        let mut inner = self.lock();
        inner.expire_leases(now);
        let state = inner
            .leases
            .get_mut(&lease)
            .ok_or(StoreError::LeaseNotFound)?;
        state.deadline = now + state.ttl;
        Ok(())
    }

    async fn revoke_lease(&self, lease: LeaseId) -> Result<()> {
        let mut inner = self.lock();
        inner.expire_leases(Instant::now());
        if inner.revoke_lease(lease) {
            Ok(())
        } else {
            Err(StoreError::LeaseNotFound)
        }
    }

    async fn compact(&self, revision: Revision) -> Result<()> {
        self.lock().compact(revision)
    }
}

struct Inner {
    /// Current revision. Starts at 1 like etcd.
    revision: Revision,
    /// Watches cannot start before this revision.
    compact_revision: Revision,
    data: BTreeMap<String, KeyValue>,
    /// Every event since `compact_revision`, in revision order.
    history: Vec<WatchEvent>,
    watchers: Vec<Watcher>,
    next_watcher_id: u64,
    leases: HashMap<LeaseId, LeaseState>,
    next_lease_id: i64,
}

struct Watcher {
    id: u64,
    range: KeyRange,
    start_revision: Revision,
    responses: mpsc::UnboundedSender<Result<WatchResponse>>,
}

struct LeaseState {
    ttl: Duration,
    deadline: Instant,
    keys: BTreeSet<String>,
}

impl Inner {
    fn new() -> Self {
        Self {
            revision: 1,
            compact_revision: 0,
            data: BTreeMap::new(),
            history: Vec::new(),
            watchers: Vec::new(),
            next_watcher_id: 0,
            leases: HashMap::new(),
            next_lease_id: 1,
        }
    }

    fn keys_with_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = (&'a String, &'a KeyValue)> + 'a {
        self.data
            .range(prefix.to_owned()..)
            .take_while(move |(key, _)| key.starts_with(prefix))
    }

    fn txn(&mut self, txn: Txn) -> Result<TxnResponse> {
        // Like etcd, both branches are validated whatever the outcome.
        validate_ops(&txn.success)?;
        validate_ops(&txn.failure)?;

        let succeeded = txn.compares.iter().all(|c| c.holds(self.data.get(&c.key)));
        let ops = if succeeded { txn.success } else { txn.failure };

        let unknown_lease = ops.iter().any(|op| {
            matches!(op, Op::Put { lease: Some(lease), .. } if !self.leases.contains_key(lease))
        });
        if unknown_lease {
            return Err(StoreError::LeaseNotFound);
        }

        let responses = self.apply(ops);
        Ok(TxnResponse {
            succeeded,
            revision: self.revision,
            responses,
        })
    }

    /// Applies validated operations as a single write.
    fn apply(&mut self, ops: Vec<Op>) -> Vec<OpResponse> {
        let revision = self.revision + 1;
        let mut events = Vec::new();
        let mut responses = Vec::with_capacity(ops.len());

        for op in ops {
            let response = match op {
                Op::Put { key, value, lease } => {
                    let prev_kv = self.data.get(&key).cloned();
                    if let Some(old_lease) = prev_kv.as_ref().and_then(|kv| kv.lease) {
                        self.detach(old_lease, &key);
                    }
                    if let Some(lease) = lease {
                        // Existence checked before applying.
                        if let Some(state) = self.leases.get_mut(&lease) {
                            state.keys.insert(key.clone());
                        }
                    }
                    let kv = KeyValue {
                        key: key.clone(),
                        value,
                        create_revision: prev_kv.as_ref().map_or(revision, |kv| kv.create_revision),
                        mod_revision: revision,
                        version: prev_kv.as_ref().map_or(1, |kv| kv.version + 1),
                        lease,
                    };
                    self.data.insert(key, kv.clone());
                    events.push(WatchEvent {
                        kind: EventKind::Put,
                        kv,
                        prev_kv,
                    });
                    OpResponse::Put
                }
                Op::Delete { key } => {
                    let deleted = self.delete(&key, revision, &mut events);
                    OpResponse::Delete {
                        deleted: u64::from(deleted),
                    }
                }
                Op::DeletePrefix { prefix } => {
                    let keys: Vec<String> = self
                        .keys_with_prefix(&prefix)
                        .map(|(key, _)| key.clone())
                        .collect();
                    for key in &keys {
                        self.delete(key, revision, &mut events);
                    }
                    OpResponse::Delete {
                        deleted: keys.len() as u64,
                    }
                }
                Op::Get { key } => OpResponse::Get(self.data.get(&key).cloned()),
            };
            responses.push(response);
        }

        if !events.is_empty() {
            self.revision = revision;
            self.publish(events);
        }
        responses
    }

    fn delete(&mut self, key: &str, revision: Revision, events: &mut Vec<WatchEvent>) -> bool {
        let Some(prev_kv) = self.data.remove(key) else {
            return false;
        };
        if let Some(lease) = prev_kv.lease {
            self.detach(lease, key);
        }
        events.push(WatchEvent {
            kind: EventKind::Delete,
            kv: KeyValue {
                key: key.to_owned(),
                value: Vec::new(),
                create_revision: 0,
                mod_revision: revision,
                version: 0,
                lease: None,
            },
            prev_kv: Some(prev_kv),
        });
        true
    }

    fn detach(&mut self, lease: LeaseId, key: &str) {
        if let Some(state) = self.leases.get_mut(&lease) {
            state.keys.remove(key);
        }
    }

    /// Records the events of a write and sends them to the watchers.
    fn publish(&mut self, events: Vec<WatchEvent>) {
        self.watchers.retain(|watcher| {
            let matching: Vec<WatchEvent> = events
                .iter()
                .filter(|e| {
                    e.revision() >= watcher.start_revision && watcher.range.contains(&e.kv.key)
                })
                .cloned()
                .collect();
            if matching.is_empty() {
                return !watcher.responses.is_closed();
            }
            watcher
                .responses
                .send(Ok(WatchResponse::Events(matching)))
                .is_ok()
        });
        self.history.extend(events);
    }

    fn watch(
        &mut self,
        store: Weak<Mutex<Inner>>,
        range: KeyRange,
        start_revision: Option<Revision>,
    ) -> Watch {
        let (responses, receiver) = mpsc::unbounded_channel();

        let start_revision = match start_revision {
            Some(revision) if revision > 0 => revision,
            _ => self.revision + 1,
        };
        if start_revision < self.compact_revision {
            let _ = responses.send(Err(StoreError::Compacted {
                compact_revision: self.compact_revision,
            }));
            return Watch::new(receiver, || {});
        }

        // Replay the history, one response per revision.
        let mut backlog = self
            .history
            .iter()
            .filter(|e| e.revision() >= start_revision && range.contains(&e.kv.key))
            .peekable();
        while let Some(first) = backlog.next() {
            let mut events = vec![first.clone()];
            while let Some(next) = backlog.next_if(|e| e.revision() == first.revision()) {
                events.push(next.clone());
            }
            let _ = responses.send(Ok(WatchResponse::Events(events)));
        }

        let id = self.next_watcher_id;
        self.next_watcher_id += 1;
        self.watchers.push(Watcher {
            id,
            range,
            start_revision,
            responses,
        });

        Watch::new(receiver, move || {
            let Some(store) = store.upgrade() else {
                return;
            };
            let inner = lock(&store);
            if let Some(watcher) = inner.watchers.iter().find(|w| w.id == id) {
                // Every event up to the current revision has already been sent
                // to the watcher, before this response.
                let _ = watcher
                    .responses
                    .send(Ok(WatchResponse::Progress(inner.revision)));
            }
        })
    }

    fn grant_lease(&mut self, ttl: Duration, now: Instant) -> Lease {
        // Like etcd, TTLs are whole seconds.
        let seconds = ttl.as_secs() + u64::from(ttl.subsec_nanos() > 0);
        let ttl = Duration::from_secs(seconds.max(1));
        let id = LeaseId(self.next_lease_id);
        self.next_lease_id += 1;
        self.leases.insert(
            id,
            LeaseState {
                ttl,
                deadline: now + ttl,
                keys: BTreeSet::new(),
            },
        );
        Lease { id, ttl }
    }

    /// Removes the lease and deletes its keys. Returns whether the lease existed.
    fn revoke_lease(&mut self, lease: LeaseId) -> bool {
        let Some(state) = self.leases.remove(&lease) else {
            return false;
        };
        self.apply(state.keys.into_iter().map(Op::delete).collect());
        true
    }

    fn expire_leases(&mut self, now: Instant) {
        let mut expired: Vec<LeaseId> = self
            .leases
            .iter()
            .filter(|(_, state)| state.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        expired.sort();
        // Each expiry is a separate write, like in etcd.
        for lease in expired {
            self.revoke_lease(lease);
        }
    }

    fn compact(&mut self, revision: Revision) -> Result<()> {
        if revision <= self.compact_revision {
            return Err(StoreError::InvalidRequest(format!(
                "revision {revision} has already been compacted"
            )));
        }
        if revision > self.revision {
            return Err(StoreError::InvalidRequest(format!(
                "revision {revision} is a future revision"
            )));
        }
        self.compact_revision = revision;
        self.history.retain(|e| e.revision() >= revision);
        Ok(())
    }
}

/// Rejects transactions writing the same key twice, like etcd.
fn validate_ops(ops: &[Op]) -> Result<()> {
    let mut keys = HashSet::new();
    let mut prefixes = Vec::new();
    for op in ops {
        match op {
            Op::Put { key, .. } | Op::Delete { key } => {
                if !keys.insert(key.as_str()) {
                    return Err(duplicate_key());
                }
            }
            Op::DeletePrefix { prefix } => prefixes.push(prefix.as_str()),
            Op::Get { .. } => {}
        }
    }
    let overlaps = ops.iter().any(|op| {
        matches!(op, Op::Put { key, .. } if prefixes.iter().any(|prefix| key.starts_with(prefix)))
    });
    if overlaps {
        return Err(duplicate_key());
    }
    Ok(())
}

fn duplicate_key() -> StoreError {
    StoreError::InvalidRequest("duplicate key given in txn request".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn revision_starts_at_one_and_counts_writes() {
        let store = MemoryStore::new();
        assert_eq!(store.get("k").await.unwrap().revision, 1);
        assert_eq!(store.put("k", "a").await.unwrap(), 2);
        assert_eq!(store.put("k", "b").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn noop_writes_do_not_change_the_revision() {
        let store = MemoryStore::new();
        store.put("k", "a").await.unwrap();
        let response = store
            .txn(Txn::new().then([Op::delete("missing"), Op::get("k")]))
            .await
            .unwrap();
        assert_eq!(response.revision, 2);
    }

    #[tokio::test]
    async fn rejects_invalid_compactions() {
        let store = MemoryStore::new();
        let revision = store.put("k", "a").await.unwrap();
        assert!(store.compact(revision + 1).await.is_err());
        store.compact(revision).await.unwrap();
        assert!(store.compact(revision).await.is_err());
    }

    #[tokio::test]
    async fn watch_from_a_future_revision_skips_earlier_events() {
        let store = MemoryStore::new();
        let mut watch = store.watch(KeyRange::prefix(""), Some(3)).await.unwrap();
        store.put("a", "1").await.unwrap(); // revision 2
        store.put("b", "1").await.unwrap(); // revision 3
        match watch.recv().await.unwrap().unwrap() {
            WatchResponse::Events(events) => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].kv.key, "b");
            }
            response => panic!("unexpected {response:?}"),
        }
    }

    #[tokio::test]
    async fn dropped_watchers_are_removed() {
        let store = MemoryStore::new();
        let watch = store.watch(KeyRange::prefix(""), None).await.unwrap();
        drop(watch);
        store.put("k", "a").await.unwrap();
        assert!(store.lock().watchers.is_empty());
    }

    #[tokio::test]
    async fn rejects_put_overlapping_a_deleted_prefix() {
        let store = MemoryStore::new();
        let result = store
            .txn(Txn::new().then([Op::delete_prefix("p/"), Op::put("p/a", "1")]))
            .await;
        assert!(matches!(result, Err(StoreError::InvalidRequest(_))));
    }
}
