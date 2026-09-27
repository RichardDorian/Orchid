# ikebana

The scheduler of Orchid. It places pending pods on nodes by binding them through Labellum.

```sh
ikebana --config /etc/ikebana/ikebana.toml
```

See [`configs/ikebana.toml`](../../configs/ikebana.toml) for the configuration file.

## How it works

- Every instance keeps a cache of the nodes and of the pending pods (informers). Only the leader schedules, the others take over when the leader stops or loses its lease.
- The next pod is the one with the highest priority, then the one pending for the longest time.
- The node is the one with the lowest `max(cpu%, mem%)` once the pod is placed, among the ready, schedulable, non draining nodes providing the pod runtime and with enough resources left. Ties are broken by node name.
- After a successful bind, the resources of the pod are counted on the node until the node cache shows them ("assumed pods").
- Pods that fit nowhere wait until a node changes in a way that could make them fit, or for 60 seconds.
