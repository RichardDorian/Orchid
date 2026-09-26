# orchid-proto

gRPC protocol definitions of Orchid, compiled with [tonic](https://github.com/hyperium/tonic).

The `.proto` files live in [`proto/orchid/v1`](proto/orchid/v1) (package `orchid.v1`). The build script generates a client and a server for every service, exposed in the `orchid_proto::v1` module:

| Service | File | Used by |
|---------|------|---------|
| `ClusterService` | `cluster.proto` | Users (read and write), every component (read) |
| `PodService` | `pod.proto` | Users, Ikebana, Keiki |
| `NodeService` | `node.proto` | Users, Ikebana |
| `NodeAgentService` | `agent.proto` | Keiki |
| `SchedulerService`, `LeadershipService` | `scheduler.proto` | Ikebana |

Labellum implements every server, the other components use the clients.

```rust
use orchid_proto::v1::pod_service_client::PodServiceClient;

let mut client = PodServiceClient::connect("http://10.0.0.1:36116").await?;
```

Building requires `protoc` to be installed.
