# labellum

The API server of Orchid. It stores the desired state of the cluster in etcd and serves every gRPC service of [`orchid-proto`](../orchid-proto). It is stateless: any request can be served by any instance.

```sh
labellum --config /etc/labellum/labellum.toml
```

See [`configs/labellum.toml`](../../configs/labellum.toml) for the configuration file.

## How it works

- **Writes** read the keys they depend on, then commit an etcd transaction checking that none of them changed. `Bind` checks the pod, the node allocation and the Ikebana leader token in one transaction, so nodes are never overcommitted.
- **Reads and watches** are served from an in-memory cache of pods and nodes per instance (`cache`), fed by an etcd watch. Lists wait until the cache reached the current etcd revision. Watches replay the recent history from any revision still in it, send an object that stops matching a filter as `DELETED`, send bookmarks every few seconds, and fail with `OUT_OF_RANGE` for revisions too old.
- **The controller** runs on one instance at a time (leader election with a fencing token). It reschedules the pods of lost nodes (or fails them if their restart policy is `never`), evicts the pods of draining nodes, and repairs node allocations.

## Tests

The `testing` feature provides `TestServer`, an in-process Labellum on the in-memory store, used by the tests of every component. `tests/etcd.rs` runs against a real etcd when `ORCHID_TEST_ETCD` is set (it writes under `/orchid/`: use a dedicated etcd).
