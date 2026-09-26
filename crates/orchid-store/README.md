# orchid-store

Storage layer of Labellum.

The `Store` trait is a small subset of the etcd v3 API:
- reads (`get`, `list`),
- transactions with compare-and-swap (`txn`), all writes of a transaction share one revision,
- resumable watches (`watch` from a revision, progress notifications, compaction errors),
- leases (`grant_lease`, `keep_alive`, `revoke_lease`).

It has two implementations:
- `EtcdStore`: backed by an etcd cluster, in cleartext or mTLS.
- `MemoryStore`: in memory with the same semantics as etcd, for tests. Lease expiry uses `tokio::time`, so tests can run with a paused clock.

The crate also defines the key layout of the cluster in etcd (`keys`) and the encoding of the values (`codec`, JSON).

```rust
use orchid_store::{Compare, MemoryStore, Op, Store, Txn};

let store = MemoryStore::new();
let response = store
    .txn(
        Txn::new()
            .when([Compare::absent("/orchid/pods/my-app/spec")])
            .then([Op::put("/orchid/pods/my-app/spec", "{}")]),
    )
    .await?;
assert!(response.succeeded);
```

## Tests

The tests in `tests/suite` run against both implementations to make sure they behave the same.
The etcd tests are skipped unless `ORCHID_TEST_ETCD` contains etcd endpoints:

```sh
ORCHID_TEST_ETCD=http://127.0.0.1:2379 cargo test -p orchid-store --test etcd
```

They compact the etcd history: don't point them at a cluster in use.
