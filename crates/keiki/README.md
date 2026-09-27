# keiki

The node agent of Orchid. It registers its node with Labellum, sends heartbeats, and runs the pods bound to the node with containerd.

```sh
keiki --config /etc/keiki/keiki.toml
```

See [`configs/keiki.toml`](../../configs/keiki.toml) for the configuration file. Keiki needs access to the containerd socket, usually as root.

## How it works

- A worker per pod creates it in the runtime, starts its containers, restarts them according to the restart policy (exponential backoff from 10 seconds to 5 minutes), reports the status of the pod, and removes it when it is deleted or evicted.
- Pods found in the runtime but not bound to the node anymore (e.g. rescheduled while the node was unreachable) are removed.
- Stopping Keiki leaves the containers running. When it starts again, it picks up the pods already in containerd.

## containerd

- Everything lives in the `orchid` containerd namespace (`containerd_namespace` in the configuration). Containers are labelled with the uid, attempt and name of their pod.
- Every pod gets a sandbox container running the pause image, which owns the network, IPC and UTS namespaces of the pod. The containers of the pod join them.
- Images are pulled with the transfer service and unpacked for the local platform. An image already present is not pulled again.
- Container logs are written to `<log_dir>/<pod>_<uid>/<container>.log`.

Current limitations: pods only have a loopback network (no CNI yet), image users must be numeric, and VM based runtimes (Kata) cannot share namespaces between the containers of a pod.

## Tests

`runtime::fake::FakeRuntime` is an in-memory runtime used by the agent tests. `tests/containerd.rs` runs against a real containerd and is ignored by default:

```sh
sudo -E cargo test -p keiki --test containerd -- --ignored
```
