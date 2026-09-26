//! Runs the store test suite against a real etcd.
//!
//! Skipped unless `ORCHID_TEST_ETCD` contains comma separated etcd endpoints:
//!
//! ```sh
//! ORCHID_TEST_ETCD=http://127.0.0.1:2379 cargo test -p orchid-store --test etcd
//! ```
//!
//! The tests compact the etcd history: don't point them at a cluster in use.

mod suite;

use std::time::{SystemTime, UNIX_EPOCH};

use orchid_store::EtcdStore;
use tokio::sync::Mutex;

/// Compactions affect the whole store: tests run one at a time.
static LOCK: Mutex<()> = Mutex::const_new(());

async fn store() -> Option<EtcdStore> {
    let endpoints = std::env::var("ORCHID_TEST_ETCD").ok()?;
    let endpoints: Vec<&str> = endpoints.split(',').collect();
    Some(
        EtcdStore::connect(&endpoints, None)
            .await
            .expect("failed to connect to etcd"),
    )
}

/// A prefix unique to the test run.
fn prefix(test: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/orchid-test/{nanos}/{test}/")
}

macro_rules! tests {
    ($($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(store) = store().await else {
                    eprintln!("skipped: ORCHID_TEST_ETCD is not set");
                    return;
                };
                let _guard = LOCK.lock().await;
                suite::$name(&store, &prefix(stringify!($name))).await;
            }
        )*
    };
}

tests!(
    get_put_delete,
    list_prefix,
    txn_compare_and_swap,
    txn_writes_share_a_revision,
    txn_get_sees_previous_writes,
    txn_rejects_duplicate_keys,
    delete_prefix,
    watch_replays_history_then_follows,
    watch_single_key,
    watch_delivers_a_transaction_at_once,
    watch_progress,
    watch_compacted_revision,
    lease_expiry_deletes_keys,
    keep_alive_extends_lease,
    revoke_deletes_keys,
    overwriting_a_key_detaches_it_from_its_lease,
);
