//! Tests shared by every [`Store`] implementation, to make sure the in-memory
//! store behaves like etcd.
//!
//! Every test only touches keys under `prefix`.

use std::time::Duration;

use orchid_store::{
    Compare, CompareOp, EventKind, KeyRange, Op, OpResponse, Store, StoreError, Txn, Watch,
    WatchEvent, WatchResponse,
};
use tokio::time::{sleep, timeout};

const TIMEOUT: Duration = Duration::from_secs(15);

async fn next(watch: &mut Watch) -> WatchResponse {
    timeout(TIMEOUT, watch.recv())
        .await
        .expect("timed out waiting for a watch response")
        .expect("watch ended")
        .expect("watch failed")
}

/// Collects events until `count` events have been received.
async fn next_events(watch: &mut Watch, count: usize) -> Vec<WatchEvent> {
    let mut events = Vec::new();
    while events.len() < count {
        if let WatchResponse::Events(batch) = next(watch).await {
            events.extend(batch);
        }
    }
    events
}

pub async fn get_put_delete<S: Store>(store: &S, prefix: &str) {
    let key = format!("{prefix}key");
    assert_eq!(store.get(&key).await.unwrap().kv, None);

    let created = store.put(&key, "a").await.unwrap();
    let kv = store.get(&key).await.unwrap().kv.unwrap();
    assert_eq!(kv.key, key);
    assert_eq!(kv.value, b"a");
    assert_eq!(kv.version, 1);
    assert_eq!(kv.create_revision, created);
    assert_eq!(kv.mod_revision, created);
    assert_eq!(kv.lease, None);

    let updated = store.put(&key, "b").await.unwrap();
    assert!(updated > created);
    let kv = store.get(&key).await.unwrap().kv.unwrap();
    assert_eq!(kv.value, b"b");
    assert_eq!(kv.version, 2);
    assert_eq!(kv.create_revision, created);
    assert_eq!(kv.mod_revision, updated);

    assert!(store.delete(&key).await.unwrap());
    assert_eq!(store.get(&key).await.unwrap().kv, None);
    assert!(!store.delete(&key).await.unwrap());
}

pub async fn list_prefix<S: Store>(store: &S, prefix: &str) {
    for key in ["a/2", "a/1", "ab/1", "b"] {
        store.put(format!("{prefix}{key}"), "v").await.unwrap();
    }
    let response = store.list(&format!("{prefix}a/")).await.unwrap();
    let keys: Vec<&str> = response.kvs.iter().map(|kv| kv.key.as_str()).collect();
    assert_eq!(keys, [format!("{prefix}a/1"), format!("{prefix}a/2")]);
    assert!(response.revision >= response.kvs.iter().map(|kv| kv.mod_revision).max().unwrap());
}

pub async fn txn_compare_and_swap<S: Store>(store: &S, prefix: &str) {
    let key = format!("{prefix}key");
    let create = || {
        Txn::new()
            .when([Compare::absent(&key)])
            .then([Op::put(&key, "1")])
            .otherwise([Op::get(&key)])
    };

    let response = store.txn(create()).await.unwrap();
    assert!(response.succeeded);

    let response = store.txn(create()).await.unwrap();
    assert!(!response.succeeded);
    let kv = response.get(0).unwrap().clone();
    assert_eq!(kv.value, b"1");

    let update = |mod_revision| {
        Txn::new()
            .when([Compare::unchanged(&key, mod_revision)])
            .then([Op::put(&key, "2")])
    };
    assert!(store.txn(update(kv.mod_revision)).await.unwrap().succeeded);
    assert!(!store.txn(update(kv.mod_revision)).await.unwrap().succeeded);

    let txn = Txn::new().when([
        Compare::value(&key, CompareOp::Equal, "2"),
        Compare::exists(&key),
        Compare::create_revision(&key, CompareOp::Equal, kv.create_revision),
        Compare::lease(&key, CompareOp::Equal, None),
    ]);
    assert!(store.txn(txn).await.unwrap().succeeded);

    // A value compare never holds on a missing key.
    let missing = format!("{prefix}missing");
    let txn = Txn::new().when([Compare::value(&missing, CompareOp::NotEqual, "x")]);
    assert!(!store.txn(txn).await.unwrap().succeeded);
}

pub async fn txn_writes_share_a_revision<S: Store>(store: &S, prefix: &str) {
    let (a, b) = (format!("{prefix}a"), format!("{prefix}b"));
    let response = store
        .txn(Txn::new().then([Op::put(&a, "1"), Op::put(&b, "1")]))
        .await
        .unwrap();
    for key in [&a, &b] {
        let kv = store.get(key).await.unwrap().kv.unwrap();
        assert_eq!(kv.mod_revision, response.revision);
    }
}

pub async fn txn_get_sees_previous_writes<S: Store>(store: &S, prefix: &str) {
    let key = format!("{prefix}key");
    let response = store
        .txn(Txn::new().then([Op::put(&key, "x"), Op::get(&key)]))
        .await
        .unwrap();
    assert_eq!(response.responses[0], OpResponse::Put);
    assert_eq!(response.get(1).unwrap().value, b"x");
}

pub async fn txn_rejects_duplicate_keys<S: Store>(store: &S, prefix: &str) {
    let key = format!("{prefix}key");
    let result = store
        .txn(Txn::new().then([Op::put(&key, "1"), Op::put(&key, "2")]))
        .await;
    assert!(
        matches!(result, Err(StoreError::InvalidRequest(_))),
        "{result:?}"
    );
}

pub async fn delete_prefix<S: Store>(store: &S, prefix: &str) {
    for key in ["obj/a", "obj/b", "obj-2/a"] {
        store.put(format!("{prefix}{key}"), "v").await.unwrap();
    }
    let response = store
        .txn(Txn::new().then([Op::delete_prefix(format!("{prefix}obj/"))]))
        .await
        .unwrap();
    assert_eq!(response.responses, [OpResponse::Delete { deleted: 2 }]);

    let keys: Vec<String> = store
        .list(prefix)
        .await
        .unwrap()
        .kvs
        .into_iter()
        .map(|kv| kv.key)
        .collect();
    assert_eq!(keys, [format!("{prefix}obj-2/a")]);
}

pub async fn watch_replays_history_then_follows<S: Store>(store: &S, prefix: &str) {
    let key = format!("{prefix}key");
    let r1 = store.put(&key, "a").await.unwrap();
    let r2 = store.put(&key, "b").await.unwrap();
    store.delete(&key).await.unwrap();

    let mut watch = store
        .watch(KeyRange::prefix(prefix), Some(r1))
        .await
        .unwrap();
    let events = next_events(&mut watch, 3).await;
    assert_eq!(
        events.iter().map(|e| e.kind).collect::<Vec<_>>(),
        [EventKind::Put, EventKind::Put, EventKind::Delete]
    );
    assert_eq!(events[0].revision(), r1);
    assert_eq!(events[1].revision(), r2);
    assert!(events[2].revision() > r2);
    assert_eq!(events[0].prev_kv, None);
    assert_eq!(events[1].prev_kv.as_ref().unwrap().value, b"a");
    assert_eq!(events[2].kv.key, key);
    assert_eq!(events[2].prev_kv.as_ref().unwrap().value, b"b");

    let r4 = store.put(format!("{prefix}other"), "c").await.unwrap();
    let events = next_events(&mut watch, 1).await;
    assert_eq!(events[0].revision(), r4);
    assert_eq!(events[0].kv.value, b"c");
}

pub async fn watch_single_key<S: Store>(store: &S, prefix: &str) {
    let key = format!("{prefix}key");
    let mut watch = store.watch(KeyRange::key(&key), None).await.unwrap();
    store.put(format!("{key}-other"), "x").await.unwrap();
    store.put(&key, "y").await.unwrap();
    let events = next_events(&mut watch, 1).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kv.key, key);
}

pub async fn watch_delivers_a_transaction_at_once<S: Store>(store: &S, prefix: &str) {
    let mut watch = store.watch(KeyRange::prefix(prefix), None).await.unwrap();
    store
        .txn(Txn::new().then([
            Op::put(format!("{prefix}a"), "1"),
            Op::put(format!("{prefix}b"), "1"),
        ]))
        .await
        .unwrap();
    match next(&mut watch).await {
        WatchResponse::Events(events) => assert_eq!(events.len(), 2),
        response => panic!("unexpected {response:?}"),
    }
}

pub async fn watch_progress<S: Store>(store: &S, prefix: &str) {
    let mut watch = store.watch(KeyRange::prefix(prefix), None).await.unwrap();
    let revision = store.put(format!("{prefix}key"), "v").await.unwrap();
    next_events(&mut watch, 1).await;

    watch.request_progress();
    match next(&mut watch).await {
        WatchResponse::Progress(progress) => assert!(progress >= revision),
        response => panic!("unexpected {response:?}"),
    }
}

pub async fn watch_compacted_revision<S: Store>(store: &S, prefix: &str) {
    let key = format!("{prefix}key");
    let r1 = store.put(&key, "a").await.unwrap();
    let r2 = store.put(&key, "b").await.unwrap();
    store.compact(r2).await.unwrap();

    let mut watch = store
        .watch(KeyRange::prefix(prefix), Some(r1))
        .await
        .unwrap();
    let result = timeout(TIMEOUT, watch.recv())
        .await
        .expect("timed out")
        .expect("watch ended without error");
    match result {
        Err(StoreError::Compacted { compact_revision }) => assert!(compact_revision >= r2),
        result => panic!("unexpected {result:?}"),
    }
}

pub async fn lease_expiry_deletes_keys<S: Store>(store: &S, prefix: &str) {
    let key = format!("{prefix}key");
    let lease = store.grant_lease(Duration::from_secs(1)).await.unwrap();
    assert!(lease.ttl >= Duration::from_secs(1));

    let revision = store
        .txn(Txn::new().then([Op::put_with_lease(&key, "v", lease.id)]))
        .await
        .unwrap()
        .revision;
    assert_eq!(
        store.get(&key).await.unwrap().kv.unwrap().lease,
        Some(lease.id)
    );

    let mut watch = store
        .watch(KeyRange::key(&key), Some(revision + 1))
        .await
        .unwrap();
    let events = next_events(&mut watch, 1).await;
    assert_eq!(events[0].kind, EventKind::Delete);

    assert_eq!(store.get(&key).await.unwrap().kv, None);
    assert!(matches!(
        store.keep_alive(lease.id).await,
        Err(StoreError::LeaseNotFound)
    ));
}

pub async fn keep_alive_extends_lease<S: Store>(store: &S, prefix: &str) {
    let key = format!("{prefix}key");
    let lease = store.grant_lease(Duration::from_secs(2)).await.unwrap();
    store
        .txn(Txn::new().then([Op::put_with_lease(&key, "v", lease.id)]))
        .await
        .unwrap();

    // Twice the TTL in total.
    for _ in 0..4 {
        sleep(lease.ttl / 2).await;
        store.keep_alive(lease.id).await.unwrap();
    }
    assert!(store.get(&key).await.unwrap().kv.is_some());
    store.revoke_lease(lease.id).await.unwrap();
}

pub async fn revoke_deletes_keys<S: Store>(store: &S, prefix: &str) {
    let (a, b) = (format!("{prefix}a"), format!("{prefix}b"));
    let lease = store.grant_lease(Duration::from_secs(60)).await.unwrap();
    store
        .txn(Txn::new().then([
            Op::put_with_lease(&a, "v", lease.id),
            Op::put_with_lease(&b, "v", lease.id),
        ]))
        .await
        .unwrap();

    store.revoke_lease(lease.id).await.unwrap();
    assert_eq!(store.get(&a).await.unwrap().kv, None);
    assert_eq!(store.get(&b).await.unwrap().kv, None);

    assert!(matches!(
        store.revoke_lease(lease.id).await,
        Err(StoreError::LeaseNotFound)
    ));
    let result = store
        .txn(Txn::new().then([Op::put_with_lease(&a, "v", lease.id)]))
        .await;
    assert!(
        matches!(result, Err(StoreError::LeaseNotFound)),
        "{result:?}"
    );
}

pub async fn overwriting_a_key_detaches_it_from_its_lease<S: Store>(store: &S, prefix: &str) {
    let key = format!("{prefix}key");
    let lease = store.grant_lease(Duration::from_secs(60)).await.unwrap();
    store
        .txn(Txn::new().then([Op::put_with_lease(&key, "v", lease.id)]))
        .await
        .unwrap();
    store.put(&key, "w").await.unwrap();

    store.revoke_lease(lease.id).await.unwrap();
    let kv = store.get(&key).await.unwrap().kv.unwrap();
    assert_eq!(kv.value, b"w");
    assert_eq!(kv.lease, None);
}
