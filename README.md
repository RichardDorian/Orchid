# Orchid

A container orchestration platform written in Rust. See [SPEC.md](SPEC.md) for the design.

Orchid has three components:
- **Labellum**: the API server, stores the desired state of the cluster in etcd.
- **Ikebana**: the scheduler, places pods on nodes.
- **Keiki**: the node agent, runs pods with containerd.

`orchidctl` is the command line client.

## Crates

| Crate | Description |
|-------|-------------|
| [`labellum`](crates/labellum) | The API server: stores the desired state in etcd and runs the controller. |
| [`ikebana`](crates/ikebana) | The scheduler: binds pending pods to the least loaded nodes. |
| [`keiki`](crates/keiki) | The node agent: runs the pods bound to its node with containerd. |
| [`orchidctl`](crates/orchidctl) | The command line client, modeled after `kubectl`. |
| [`orchid-proto`](crates/orchid-proto) | gRPC protocol definitions and generated clients and servers. |
| [`orchid-api`](crates/orchid-api) | Domain types shared by every component: resources, quantities, validation and protobuf conversions. |
| [`orchid-store`](crates/orchid-store) | Storage layer of Labellum: a key-value store abstraction over etcd, with an in-memory implementation for tests. |
| [`orchid-transport`](crates/orchid-transport) | gRPC transport: URLs, cleartext or mTLS connections, identities and error reasons. |
| [`orchid-client`](crates/orchid-client) | Client helpers: connections to Labellum and informers. |

## Development

```sh
cargo build
cargo test
```

Building requires `protoc`. Some tests need external services:
- the etcd tests of [`orchid-store`](crates/orchid-store/README.md) and [`labellum`](crates/labellum/README.md) run when `ORCHID_TEST_ETCD` is set,
- the containerd test of [`keiki`](crates/keiki/README.md) is ignored by default and needs root.

Example configuration files are in [`configs`](configs).
