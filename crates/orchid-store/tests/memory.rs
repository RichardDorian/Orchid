//! Runs the store test suite against [`MemoryStore`], with a paused clock.

mod suite;

use orchid_store::MemoryStore;

macro_rules! tests {
    ($($name:ident),* $(,)?) => {
        $(
            #[tokio::test(start_paused = true)]
            async fn $name() {
                suite::$name(&MemoryStore::new(), "/test/").await;
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
