You are going to write a container orchestration platform in Rust.

There are going to be 3 components:
- Labellum: the API server, the brain of the cluster
- Ikebana: the workload scheduler
- Keiki: the node agent that runs on every node that can run workloads

There are two types of nodes:
- `control-plane`: runs etcd, Labellum and Ikebana. It may also run Keiki if it should appear as a node
  (for example to run workloads on a single node cluster). A control plane node is unschedulable by default.
- `worker`: runs Keiki and the workloads.

Bootstrapping a cluster (deploying etcd, Labellum, Ikebana, generating certificates) is out of scope for now.

## Project structure

This git repository is a Rust workspace. Each component gets its own crate. Since services communicate using gRPC we'll have a crate for tonic that reexposes all the protos.

## Conventions

- Configuration files are TOML and all keys are `snake_case`.
- Durations are strings with a unit suffix: `"500ms"`, `"15s"`, `"5m"`.
- CPU quantities are strings: whole or decimal cores (`"12"`, `"0.5"`) or millicores (`"300m"`). Internally they are stored as millicores (`u64`).
- Memory quantities are strings: bytes (`"1048576"`), binary suffixes (`"Ki"`, `"Mi"`, `"Gi"`, `"Ti"`) or decimal suffixes (`"K"`, `"M"`, `"G"`, `"T"`). Internally they are stored as bytes (`u64`).
- CPU and memory must be distinct types in the code (`MilliCpu`, `Bytes`) so they can never be mixed up.
- Resource names (pods, nodes) follow DNS label rules: lowercase alphanumerics and `-`, at most 63 characters.
- Container runtimes are identified by their full containerd runtime name: `io.containerd.runc.v2`, `io.containerd.kata.v2`, `io.containerd.runsc.v1`...

## Transport and security

All services communicate using gRPC. A cluster runs in exactly one of two modes, there is no combination:

- **cleartext**: every connection (including Labellum to etcd) is unencrypted and unauthenticated. Meant for development.
  Every caller is allowed to do everything.
- **mTLS**: every connection (including Labellum to etcd) uses mutual TLS. Nothing is accepted in cleartext.
  Every client must present a certificate signed by the cluster CA, there are no tokens.

The mode of a component is chosen by the presence of the `[tls]` section in its configuration. A component refuses to start if its configuration is inconsistent:
- URLs to other services use `http://` in cleartext mode and `https://` in mTLS mode. Any other combination is an error.
- `unix://` sockets are allowed in both modes. In mTLS mode, TLS is still negotiated over the socket.
- Labellum's etcd connection must use the same mode as its gRPC server.

### Identities

In mTLS mode the identity of a caller is the Common Name (CN) of its client certificate:

| Identity           | CN                   |
|--------------------|----------------------|
| Labellum           | `labellum`           |
| Ikebana            | `ikebana`            |
| Keiki              | `keiki:<node-name>`  |
| Users (CLI, ...)   | `user:<username>`    |

The server certificate of Labellum must have Subject Alternative Names (DNS names and/or IP addresses) covering every address clients use to reach it (node IPs, internal load balancer). The CN is not used to verify servers (rustls only checks SANs). Over a unix socket, clients check the server certificate against `localhost`: the certificate needs a `localhost` SAN.

### Authorization

Only enforced in mTLS mode. Everything not listed is denied.

| Identity   | Allowed                                                                                              |
|------------|------------------------------------------------------------------------------------------------------|
| `user:*`   | Full access to the public API (pods, nodes, cluster configuration).                                  |
| `ikebana`  | Watch pods and nodes, bind pods, use the `ikebana` leader election.                                  |
| `keiki:X`  | Register and heartbeat node `X` only. Watch pods bound to `X`, update the status and finalize pods bound to `X` only. |

Certificate revocation is out of scope for now.

## Storage

etcd is the only source of truth, and Labellum is the only component that talks to etcd.
Every object is split into several keys, each key having a single kind of writer. This avoids write conflicts
and prevents a component from writing a field it doesn't own.

```
/orchid/config/cluster              cluster configuration                 users
/orchid/leader/controller           Labellum controller leader key        Labellum (bound to a lease)
/orchid/leader/ikebana              Ikebana leader key                    Labellum on behalf of Ikebana (bound to a lease)
/orchid/cluster/pod-cidrs           map node name -> pod CIDR             Labellum (registration)

/orchid/nodes/<name>/spec           schedulable, draining                 users, controller
/orchid/nodes/<name>/info           role, runtimes, pod CIDR, capacity    Labellum on registration
/orchid/nodes/<name>/lease          heartbeat key (bound to a lease)      Labellum on registration
/orchid/nodes/<name>/status         health, usage, last heartbeat         Keiki (through Labellum)
/orchid/nodes/<name>/allocated      sum of resources of bound pods        Labellum (bind, finalize, reschedule)

/orchid/pods/<name>/spec            user desired state                    users
/orchid/pods/<name>/binding         node + attempt                        Ikebana (through Labellum), controller
/orchid/pods/<name>/status          phase, reason, containers             Keiki, controller, Labellum
```

The `revision` of an object exposed in the API is the highest etcd `mod_revision` of its keys.

## Resources

See `resources/` for annotated examples: `pod.toml`, `node.toml` and `cluster.toml`.

### Cluster configuration

Stored in etcd (`/orchid/config/cluster`), readable by everyone and writable by users through the API.
When Labellum starts and the key doesn't exist, it creates it with default values (put-if-absent transaction, so concurrent Labellum instances agree).

- `default_runtime`: runtime used by pods that don't specify one. Changing it is rejected if a registered node doesn't provide the new runtime. It only affects pods created afterwards (see pod admission).
- `node_lease_ttl`: TTL of the node heartbeat leases.
- `pod_eviction_delay`: time between a node becoming unreachable and its pods being rescheduled.
- `leader_lease_ttl`: TTL of leader election leases (controller and Ikebana).

### Pods

A pod is made of:
- **metadata**: `name`, `uid` (assigned by Labellum at creation), `revision`, `created_at`.
- **spec**: written by the user. Immutable after creation except `priority` (a pod must be deleted and re-created to change anything else).
- **binding**: `node` and `attempt`, written when the pod is scheduled. `attempt` starts at 1 and is incremented every time the pod is rescheduled.
  The binding key is created by the first bind and is never deleted while the pod exists: rescheduling only clears `node` and keeps `attempt`, so the next bind can increment it.
  A pod is **bound** when its binding has a non empty `node`, and **unbound** otherwise (no binding key yet, or `node` cleared).
- **status**: `phase`, `reason`, `message`, `pending_since`, `deletion_requested_at` and one status per container.

On creation (admission), Labellum:
- validates the pod (names, quantities, at least one container, unique container names),
- replaces an empty `runtime` with the cluster `default_runtime`, so changing the default later doesn't change existing pods,
- assigns the `uid`, sets `phase = pending` and `pending_since = now`.

#### Phases

| Phase         | Meaning                                                                                                         |
|---------------|-----------------------------------------------------------------------------------------------------------------|
| `pending`     | Waiting to be scheduled.                                                                                        |
| `creating`    | Bound to a node. The node is pulling images or creating the sandbox. Retryable errors (image pull, sandbox creation) keep the pod in this phase with a `reason` and are retried with a backoff. |
| `running`     | The sandbox exists and at least one container is running or restarting.                                        |
| `succeeded`   | All containers exited with code 0 and none will be restarted.                                                   |
| `failed`      | All containers exited, at least one with a non zero code, and none will be restarted. Also used when a pod is lost with its node (`reason = "NodeLost"`). |
| `terminating` | The pod has been deleted or evicted, containers are being stopped.                                              |

#### Restart policy

The restart policy applies to each container independently, Keiki restarts a container in place (the sandbox is kept):
- `always`: the container is always restarted.
- `failure`: the container is restarted if it exited with a non zero code.
- `never`: the container is never restarted.

Restarts use an exponential backoff: 10s, doubled on each restart, capped at 5 minutes. The backoff is reset after a container has been running for 10 minutes.

#### Deletion

1. A user deletes the pod. Labellum sets `phase = terminating` and `deletion_requested_at`.
2. If the pod is not bound, Labellum removes it immediately.
3. Otherwise Keiki stops the containers (SIGTERM, then SIGKILL after `termination_grace_period`), removes the sandbox and calls `FinalizePod`.
4. Labellum removes the pod keys and subtracts the pod resources from the node `allocated` key in a single transaction.

If the node is unreachable, the controller finalizes the pod itself once the node is considered lost.

### Nodes

A node is made of:
- **metadata**: `name`, `uid`, `revision`.
- **spec**: written by users: `schedulable` (cordon) and `draining`.
- **info**: reported by Keiki on registration: `role`, `runtimes`, `pod_cidr`, `capacity`. Read only.
- **status**: read only.
  - `condition`: `ready`, `unhealthy` or `unreachable`. It is derived when the node is read, not stored:
    `unreachable` if the lease key doesn't exist, otherwise `unhealthy` or `ready` depending on the health reported in the last heartbeat.
  - `message`, `last_heartbeat`.
  - `allocated`: sum of the resources of all pods bound to the node. Maintained by Labellum, this is the desired state, not a measure.
  - `usage`: actual resource usage as measured by Keiki. Informational only, never used for scheduling.

Draining is independent of the node condition (a node can be draining and unreachable).

Deleting a node is a manual user action. Its lease is revoked and its pods are rescheduled like for a lost node.
If the Keiki agent of a deleted node is still running, it will register the node again on its next heartbeat: stop the agent first.

## Services

### Labellum

Labellum is the API server, it controls the desired state of the cluster. Ikebana and Keiki connect to it, Labellum never initiates a connection to them.

Labellum is stateless: any request, including watches and heartbeats, can be served by any Labellum instance. A Labellum gets deployed on every control plane node alongside an etcd instance. All etcd instances make up an etcd cluster.

Port: `36116`

#### Watches

Every watchable collection (pods, nodes, cluster configuration, leader keys) exposes two calls:
- `List(filter)`: returns the matching objects and the etcd revision at which the list was taken.
- `Watch(filter, from_revision)`: a stream of events `ADDED`, `MODIFIED`, `DELETED`, each carrying the object and its revision.

Rules:
- A client always starts with a `List`, then `Watch` from the returned revision.
- `from_revision` is the last revision the client has seen: the stream contains the events after it.
- When the stream breaks, the client reconnects (to any Labellum instance) and resumes from the last revision it has seen.
- Labellum periodically sends `BOOKMARK` events carrying the current revision, so idle watchers can resume from a recent revision.
- If the events after `from_revision` are not available anymore, Labellum fails the call or the stream with `OUT_OF_RANGE`. The client must `List` again and reconcile.
- Watches can be filtered (for example pods bound to a node, or unbound pods). An object that stops matching the filter is sent as `DELETED` (for example a pod rescheduled away from a node).
  Labellum uses the previous value of the keys (etcd `prev_kv`) to detect this.

Each Labellum instance keeps an in-memory cache of pods and nodes, fed by its own etcd watch, so no state is shared between instances. The cache assembles objects from their keys and keeps a bounded history of object events (with the object before and after each event), which serves watches and evaluates filters. Lists wait until the cache has reached the current etcd revision, so they see every write committed before them. A watch from a revision older than the history of the instance fails with `OUT_OF_RANGE`.

#### Node registration and heartbeats

When Keiki starts, it calls `Register` with its node information. Labellum:

1. Determines the node name: the certificate CN in mTLS mode (`keiki:<name>`), the name sent by Keiki in cleartext mode.
2. Validates the registration and rejects it (`FAILED_PRECONDITION`) if:
   - the node runtimes don't contain the cluster `default_runtime`,
   - the pod CIDR overlaps the pod CIDR of another node (checked against `/orchid/cluster/pod-cidrs` with a compare-and-swap, so concurrent registrations are safe),
   - the node already exists and still has bound pods that don't fit the new information (capacity lower than `allocated`, a runtime used by a bound pod is missing). This resolves itself once the node is considered lost and its pods are rescheduled, or once the operator drains the node.
3. Creates the node on first registration, with `schedulable` taken from the Keiki configuration (defaults to `true` for workers and `false` for control planes). On later registrations the node `spec` is left untouched, so a cordon set by a user survives agent restarts.
4. Grants an etcd lease with the cluster `node_lease_ttl`, and writes `/orchid/nodes/<name>/lease` attached to it.
5. Returns the node name and the lease TTL.

Keiki then calls `Heartbeat` every `node_lease_ttl / 3`, sending its health and resource usage. Labellum finds the lease from the lease key (any instance can do it), renews it and updates the node status.
If the lease has expired, `Heartbeat` fails with `NOT_FOUND` and Keiki registers again.

#### Controller

The controller runs inside Labellum. Only one instance runs it at a time, chosen by leader election: a Labellum instance becomes leader by creating `/orchid/leader/controller` (put-if-absent, attached to a lease of `leader_lease_ttl` that it keeps alive).
The `create_revision` of the leader key is a fencing token: every write of the controller is a transaction that checks the leader key still has this `create_revision`, so a deposed leader cannot write anything.

When it becomes leader, the controller lists all objects before watching them, so events that happened without a leader are not missed.

Responsibilities:

- **Lost nodes**: when a node lease key disappears, the controller waits `pod_eviction_delay` (the timer restarts on leader failover, which is conservative). If the node is still unreachable, each pod bound to it is released in a single transaction that clears the binding `node` (keeping `attempt`) and subtracts its resources from the node `allocated` key:
  - pods with the restart policy `never` get `phase = failed`, `reason = "NodeLost"`,
  - other pods are rescheduled: `phase = pending`, `reason = "NodeLost"`, `pending_since = now`. The next binding increments `attempt`,
  - pods that already `succeeded` or `failed` keep their status.

  Pods in the `terminating` phase are finalized instead of being rescheduled.
- **Draining**: when a node has `draining = true`, the controller sets every pod bound to it to `terminating` with `reason = "Evicted"`. When Keiki finalizes an evicted pod, `FinalizePod` reschedules it in the same transaction (evictions are not failures, the restart policy is ignored). When no pod is bound to the node anymore, the controller sets `draining = false` and `schedulable = false`: the node stays cordoned.
- **Deleted nodes**: pods are handled like for a lost node, without waiting for `pod_eviction_delay`.
- **Allocation resync**: periodically recomputes every node `allocated` key from the bindings and fixes any drift.

Known limitation: during a network partition, a rescheduled pod may run on two nodes at the same time until the old node reconnects and reconciles.

#### Binding and overcommit protection

Ikebana binds a pod with `Bind(pod, pod_revision, node, leader_token)`. Labellum reads the node info and `allocated` key, checks that the pod fits, then commits a single etcd transaction that:

- checks the `ikebana` leader key still has `create_revision == leader_token` (fencing),
- checks the pod is unbound (no binding key, or an empty `node`) and its `revision` is still `pod_revision`,
- checks the node lease key exists (the node is not unreachable),
- checks the node `info` and `allocated` keys have not changed since they were read,
- writes the binding with the node and `attempt` = previous attempt + 1 (1 if there was no binding key), adds the pod resources to `allocated` and sets `phase = creating`.

If only the node keys changed, Labellum reads them again and retries. `Bind` fails with:
- `RESOURCE_EXHAUSTED` if the pod doesn't fit on the node anymore,
- `ABORTED` if the pod changed, is already bound, the leader token is stale, or the node is not available anymore (unreachable, unhealthy, unschedulable, draining, or without the pod runtime).

Failures carry a `google.rpc.ErrorInfo` detail (domain `orchid.io`) whose reason is a `BindFailure` value, so Ikebana can tell them apart. On success, `Bind` returns the binding and the revision of the transaction, which is the new `allocated_revision` of the node.

Every node therefore sees its `allocated` updates serialized: two concurrent binds can never overcommit a node, whatever the state of the schedulers' caches.

A pod reserves exactly the resources declared by its containers, there are no separate requests and limits and no overcommit.

### Ikebana

Ikebana is responsible for scheduling pods on nodes. It only talks to Labellum.

#### Leader election

Several Ikebana instances can run for high availability, but only one schedules at a time. Leader election goes through Labellum:
- `AcquireLeadership(election = "ikebana", candidate)`: Labellum grants a lease and creates `/orchid/leader/ikebana` with a put-if-absent transaction. Returns whether leadership was acquired, the leader token (`create_revision` of the key) and the TTL.
- `RenewLeadership(election, token)`: renews the lease. Called every `leader_lease_ttl / 3`. Fails with `NOT_FOUND` if the leadership has been lost.
- `ReleaseLeadership(election, token)`: revokes the lease on graceful shutdown.
- Followers watch the leader key and try to acquire it when it is deleted.

A leader that fails to renew its leadership stops scheduling immediately. Followers also keep their caches up to date so they can take over quickly.

#### Cache

Ikebana watches nodes and unbound pods and keeps them in memory. Each node entry holds its info, spec, condition and `allocated`.

After a successful `Bind`, the pod resources are added to the cached `allocated` of the node until the watch delivers a node whose `allocated_revision` is at least the revision returned by `Bind` ("assumed pods"). This avoids stale caches producing bind conflicts.

#### Queue

Unbound pods in the `pending` phase are added to the active queue. A scheduler only schedules a single pod at a time. It takes the pod with the highest priority. If multiple pods have the same priority it takes the one with the oldest `pending_since` (stored in the pod, so the order survives a leader failover).

When a pod cannot be placed on any node, it moves to the unschedulable queue so it doesn't block the queue. Pods move back from the unschedulable queue to the active queue when:
- a node is added, becomes ready, becomes schedulable, or its `allocated` or capacity changes,
- or after 60 seconds.

There is no preemption: a high priority pod never evicts a running pod.

#### Choosing a node

1. Filter: keep nodes that are `ready`, `schedulable`, not `draining`, provide the pod runtime, and have enough resources left (`allocated + pod <= capacity` for CPU and memory).
2. Score: for every remaining node, `score = max(cpu%, mem%)` computed after placing the pod:
   `cpu% = (allocated.cpu + pod.cpu) / capacity.cpu`, `mem% = (allocated.memory + pod.memory) / capacity.memory`.
3. Take the node with the lowest score. Ties are broken by node name.

Then call `Bind`:
- success: the pod is done,
- `RESOURCE_EXHAUSTED` or `ABORTED` because of the node: refresh the cache and put the pod back in the active queue,
- `ABORTED` because of a stale leader token: stop scheduling.

### Keiki

Keiki is the agent that runs on nodes. It does not expose a gRPC server yet: it only connects to Labellum.
Port `36117` is reserved for a future Keiki server (logs, exec).

Each pod gets a sandbox container (pause image) owning the network, IPC and UTS namespaces of the pod, which its containers join. Containers live in the `orchid` containerd namespace and are labelled with the uid, attempt and name of their pod. Images already present are not pulled again. Pods only have a loopback network until CNI support is added.

#### Lifecycle

1. Connect to one of the configured Labellum URLs, fail over to the next one on errors.
2. `Register`, then start the heartbeat loop.
3. `List` the pods bound to this node, reconcile, then `Watch` from the returned revision.
4. On `NOT_FOUND` from `Heartbeat`, go back to step 2.

#### Reconciliation

Containers and sandboxes created by Keiki are labelled in containerd with the pod `uid`, the binding `attempt` and the container name. Keiki reconciles on startup, after every registration and after every watch relist:
- containers of pods that are not bound to this node anymore, or whose `attempt` doesn't match the binding, are stopped and removed (a pod may have been rescheduled away while the node was unreachable, or rescheduled back with a new attempt),
- bound pods without a sandbox are created,
- running pods are kept and their status is reported.

#### Status reporting

Keiki reports pod status with `UpdatePodStatus(pod, uid, attempt, status)` and finalizes deleted pods with `FinalizePod(pod, uid, attempt)`. The pod name locates the pod, the uid checks it is the same pod.
Labellum rejects both (`FAILED_PRECONDITION`) if the pod is not bound to this node with this attempt, so a node coming back from a partition cannot overwrite the status of a rescheduled pod.

The health reported in heartbeats is `unhealthy` when Keiki cannot reach containerd. Unhealthy nodes are not schedulable but their pods are not rescheduled.

## Out of scope for now

- Cluster bootstrap
- Networking between pods of different nodes (CNI configuration, routes)
- Keiki gRPC server (logs, exec)
- Preemption
- Certificate revocation
- Replicated workloads (deployments)
