# orchidctl

The command line client of Orchid, modeled after `kubectl`.

```sh
orchidctl get pods                      # also: get pods -o wide|toml|json|name, get pods -w
orchidctl get nodes
orchidctl run web --image nginx:alpine --cpu 500m --memory 128Mi
orchidctl create -f pod.toml            # the format of resources/pod.toml
orchidctl apply -f pod.toml             # creates, or updates the priority; --force recreates
orchidctl describe pod web
orchidctl wait pod web --for phase=running
orchidctl delete pod web                # waits until the pod is gone, --no-wait not to
orchidctl cordon|uncordon|drain phoenix
orchidctl describe node phoenix
orchidctl get cluster-config
orchidctl set cluster-config pod_eviction_delay=45s
```

Resource types accept aliases: `pod`, `pods`, `po`, `node`, `nodes`, `no`, `cluster-config`.

## Configuration

orchidctl reads `~/.config/orchid/orchidctl.toml` (or `--config`, or `ORCHID_CONFIG`) for the URLs of Labellum and the client certificate in mTLS mode. See [`configs/orchidctl.toml`](../../configs/orchidctl.toml). Without a configuration file it talks to `http://127.0.0.1:36116`. `--server` overrides the URLs.

## Pod files

`create -f` and `apply -f` read TOML files in the format of [`resources/pod.toml`](../../resources/pod.toml). Only `name` and `[spec]` are read, so the output of `get pod <name> -o toml` can be used as a file.

The spec of a pod is immutable except its priority: `apply` updates the priority, and refuses other changes unless `--force` is given, which deletes the pod and creates it again.
