# Orchid

A container orchestration platform written in Rust. See [SPEC.md](SPEC.md) for the design.

Orchid has three components:
- **Labellum**: the API server, stores the desired state of the cluster in etcd.
- **Ikebana**: the scheduler, places pods on nodes.
- **Keiki**: the node agent, runs pods with containerd.

## Crates

| Crate | Description |
|-------|-------------|
| [`orchid-proto`](crates/orchid-proto) | gRPC protocol definitions and generated clients and servers. |
| [`orchid-api`](crates/orchid-api) | Domain types shared by every component: resources, quantities, validation and protobuf conversions. |
| [`orchid-store`](crates/orchid-store) | Storage layer of Labellum: a key-value store abstraction over etcd, with an in-memory implementation for tests. |

## Development

```sh
cargo build
cargo test
```

Building requires `protoc`. The etcd tests of `orchid-store` only run when `ORCHID_TEST_ETCD` is set, see its [README](crates/orchid-store/README.md).
