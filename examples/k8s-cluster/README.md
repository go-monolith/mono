# Kubernetes Cluster Example

Runs a mono application as **3 replicas on Kubernetes, one per node**, whose
embedded NATS servers form **one NATS cluster with clustered JetStream**. Every
instance serves every service, and the services behave as one application:

| Service | Kind | In the cluster |
|---|---|---|
| `greeter.hello` | request-reply | every instance subscribes in one queue group; each request is answered by exactly one instance, local or remote |
| `greeter.visits` | stream consumer | the `GREETER_VISITS` stream is replicated to all three servers (`WithJetStreamDefaultReplicas`) and consumed through one shared durable consumer, so each visit is handled by one instance (at least once) |
| `greeter.tick` | cron (`@every 15s`) | the schedule lives on the server, in a replicated stream, so each occurrence fires **once for the whole cluster**, not once per instance |

```
        node A                  node B                  node C
  ┌───────────────────┐   ┌───────────────────┐   ┌───────────────────┐
  │ pod mono-cluster-0│   │ pod mono-cluster-1│   │ pod mono-cluster-2│
  │  greeter module   │   │  greeter module   │   │  greeter module   │
  │  nats-server ─────┼───┼─ nats-server ─────┼───┼─ nats-server      │
  │  JetStream (PVC)  │   │  JetStream (PVC)  │   │  JetStream (PVC)  │
  │  HTTP :8080       │   │  HTTP :8080       │   │  HTTP :8080       │
  └───────────────────┘   └───────────────────┘   └───────────────────┘
     embedded NATS servers, routes on 6222 in a full mesh,
     server_name = pod name
        ▲ Service mono-cluster:8080 (load-balanced HTTP API)
```

## Build and deploy

Requirements: a Kubernetes cluster (1.30+) with at least **3 schedulable
nodes**, a default StorageClass for the PersistentVolumeClaims, and a registry
the nodes can pull from.

```bash
# From the repository root
IMAGE=registry.example.com/mono-k8s-cluster:0.2.0
docker build -f examples/k8s-cluster/Dockerfile -t $IMAGE .
docker push $IMAGE
```

Set `image:` in `deploy/k8s.yaml` to `$IMAGE`, then:

```bash
examples/k8s-cluster/deploy/create-route-secret.sh   # namespace mono-demo
kubectl apply -f examples/k8s-cluster/deploy/k8s.yaml
kubectl -n mono-demo rollout status sts/mono-cluster
kubectl -n mono-demo get pods -o wide                # one pod per node
```

When you change the code, use a new tag: with `imagePullPolicy: IfNotPresent`,
a node keeps the image it already has.

## Files

| File | Purpose |
|---|---|
| `main.go` | Host program: configuration from the environment, the application, the HTTP API |
| `greeter/` | The module (`module.go`) and its contract (`contract/`) |
| `Dockerfile` | Static binary on `gcr.io/distroless/static-debian12:nonroot`; build context is the repository root |
| `Dockerfile.dockerignore` | Keeps the build context small |
| `deploy/k8s.yaml` | Namespace, headless and HTTP Services, StatefulSet, PodDisruptionBudget |
| `deploy/create-route-secret.sh` | Creates the Secret with a random route password and system account password |

## Framework options

Every replica runs the same configuration; only the pod name differs.

```go
mono.NewMonoApplication(
    mono.WithNATSServerName(podName),                                   // unique and stable
    mono.WithNATSClustering("mono-demo", "0.0.0.0", 6222, routes),      // every pod, itself included
    mono.WithNATSClusterAuth("route", routePassword),                   // from a Secret
    mono.WithJetStreamStorageDir("/data/jetstream"),                    // per-pod volume
    mono.WithJetStreamDefaultReplicas(3),                               // the framework's streams
    mono.WithJetStreamReadyTimeout(10*time.Minute),                     // how long Start waits for the others
    mono.WithNATSHost("127.0.0.1"), mono.WithNATSInProcessConn(),       // client listener on loopback
)
```

- **Server name.** Clustered JetStream identifies each server by name, so the
  name must be unique within the cluster and stable across restarts. A
  StatefulSet pod name (`mono-cluster-0` .. `mono-cluster-2`) is both;
  `POD_NAME` comes from the downward API.
- **Routes.** Every pod lists every pod, its own route included (a server
  ignores the route to itself), by fully qualified name through the headless
  Service (`mono-cluster-0.mono-cluster-nats.mono-demo.svc.cluster.local`).
  While a pod's DNS record is not yet published, a short name falls through
  the search domains to the upstream resolver. An upstream that answers every
  name (a fake-IP or split-horizon DNS) then sends the route to an unrelated
  host. A name in the cluster domain is answered by CoreDNS alone. If you
  deploy to another namespace, change `NATS_ROUTES`.
- **Route authentication.** `WithNATSClusterAuth` requires the password from
  every route and adds it to the routes this server opens. The password comes
  from the Secret `mono-cluster-routes` (`deploy/create-route-secret.sh`). The
  NATS client listener is bound to `127.0.0.1`, so only the route port (6222)
  is reachable from outside the pod.
- **Start waits for the cluster.** A three-server JetStream cluster has no
  quorum until two of the servers are routed. The framework's `Start` waits for
  JetStream before it starts the module, and retries creating the streams and
  consumers while the cluster forms or a restarted server catches up (up to
  `WithJetStreamReadyTimeout`). Pods that start together wait for each other
  instead of failing and restarting, and the module needs no code for it.
- **Replicated streams.** `WithJetStreamDefaultReplicas(3)` replicates the
  streams the framework creates for the visits stream consumer and the cron
  schedule. The visits consumer is durable (`greeter-visits`), so it keeps its
  position across restarts, including a stop of every pod.

## Kubernetes manifest

`deploy/k8s.yaml` sets these StatefulSet and Service options:

- `podManagementPolicy: Parallel`: a pod is Ready only once JetStream has
  quorum, which needs two of the three pods. With the default ordered
  start, `mono-cluster-1` would never be created.
- `publishNotReadyAddresses: true` on the headless Service, so routes resolve
  before the pods are Ready.
- Required `podAntiAffinity` on `kubernetes.io/hostname`: one pod per node.
- The liveness probe (`/healthz`) only checks the process. It never depends
  on JetStream quorum, or losing pods would restart the others.
- `/readyz` reports `Health()`, including its `jetstream` section, and is
  ready while the framework and NATS are healthy, not only while JetStream
  has quorum.
- A `preStop` sleep of 5 s keeps the pod serving while it is removed from
  the Service endpoints, so clients don't get connection errors during a
  rolling update. It uses the `sleep` action, which needs no shell in the
  image (Kubernetes 1.30+). Older clusters reject the manifest: use
  `exec: {command: ["sleep", "5"]}` instead, with a base image that has a
  `sleep` binary (the distroless image used here has none).
- A PodDisruptionBudget with `minAvailable: 2`, so node drains evict one
  pod at a time and JetStream keeps its quorum.

## Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `POD_NAME` | hostname | instance and NATS server name |
| `NATS_ROUTES` | (required) | comma-separated route URLs of every replica |
| `NATS_ROUTE_PASSWORD` | unset | route password (`WithNATSClusterAuth`); unset means routes are not authenticated |
| `NATS_SYSTEM_PASSWORD` | unset | password of the system account user `admin` (`WithNATSSystemAccount`), for `nats server report` and `nats server raft peer-remove`; unset means no system account user |
| `NATS_CLUSTER_NAME` | `mono-demo` | NATS cluster name |
| `NATS_CLUSTER_PORT` | `6222` | route listener port |
| `JS_REPLICAS` | `3` | replicas of the framework's streams (at most the number of servers) |
| `JS_STORE_DIR` | `/data/jetstream` | JetStream store directory |
| `JS_READY_TIMEOUT` | `10m` | how long `Start` waits for clustered JetStream |
| `HTTP_ADDR` | `:8080` | HTTP API address |
| `NATS_DEBUG` | unset | `true` adds the embedded server's own log (routes, RAFT elections) at debug level |

## Try it

```bash
kubectl -n mono-demo port-forward svc/mono-cluster 8080:8080 &

curl 'localhost:8080/hello?name=ada'
# {"greeting": {"message": "Hello, ada!", "instance": "mono-cluster-1"},
#  "http_instance": "mono-cluster-0", "visit_stored": {"stream": "GREETER_VISITS", "sequence": 1, "duplicate": false}}
```

`http_instance` is the pod that served the HTTP request (port-forward sticks to
one pod). `greeting.instance` is the pod whose `hello` handler answered. Repeat
the call and other pods show up: the request crossed a NATS route.

A failed `/hello` may still have stored its visit (a publish that timed out
while the cluster had no quorum, for example). To retry safely, pass an `id`:
the visit is published with that message ID (`helper.WithMsgID`), and a
repeat within 2 minutes is acknowledged as a duplicate instead of being stored
again:

```bash
curl 'localhost:8080/hello?name=ada&id=req-42'   # "sequence": 2, "duplicate": false
curl 'localhost:8080/hello?name=ada&id=req-42'   # "sequence": 2, "duplicate": true
```

```bash
curl localhost:8080/cluster
```

`/cluster` shows:

- **`instances`**: every instance that answered one `$SRV.PING` (NATS services
  protocol, enabled with `WithServiceDiscovery`). All three pods answer, from
  any pod.
- **`visits_stream`**: the stream's replication: `replicas: 3`, the `leader`,
  and the two other replicas with `"current": true`.

Logs:

```bash
kubectl -n mono-demo logs -l app=mono-cluster --prefix -f | grep -E 'visit recorded|cron tick'
```

Each `cron tick` appears on exactly one pod per occurrence (every 15 s). Each
visit is recorded by whichever pod fetched it. Delivery is at least once: a
visit in flight while pods shut down can be recorded again after the restart.

## Failure behaviour

Three servers with 3 replicas keep a quorum (2 of 3) while one server is down,
for every replicated stream and consumer and for the JetStream meta group.

- **While one pod is down** (a crash, a rolling update, a node drain):
  - Every endpoint keeps working on the other two pods, including `/hello`,
    which stores a visit (a JetStream write).
  - `/readyz` of the remaining pods reports `"available": true`.
  - Cron keeps firing, once per occurrence.
  - A stream or consumer whose leader was on the lost pod elects a new one
    within seconds; the fetch loops may log a `Fetch error` (or `Cron fetch error`)
    warning meanwhile, then `… cleared` once fetching works again.
- **When the pod comes back** (with its PVC), its `Start` waits until its
  replicas have caught up, then it serves again. It does not restart.
- **While two pods are down**, there is no quorum. Request-reply keeps
  working on the remaining pod, but `/hello` answers `502` because storing the
  visit times out, `/readyz` reports `"available": false` ("no meta leader (no
  quorum)"), and cron occurrences wait. Everything resumes once a second pod is
  back; visits not yet handled are still in the stream.

Delivery is at least once: a visit fetched by a pod that shuts down before
acknowledging it is delivered again after the consumer's ack wait (30 s).

Measured on a 3-node test cluster (Kubernetes 1.37, 5 requests per second
through the Service), every visit tracked:

| | Requests failed | Visits lost / duplicated / replayed |
|---|---|---|
| rolling restart (`kubectl rollout restart`) | 0 of 268 | 0 / 0 / 0 |
| one pod down for about 40 s, then back | 0 of 469 | 0 / 0 / 0 |
| scaling an existing 2-replica deployment to 3 | 1 of 389 | 0 / 0 / 0 |

Memory per pod: about 10–20 MiB (limit 192 MiB).

For canaries, rollbacks, node drains and blue-green cutovers, see [Kubernetes Deployment Strategies](../../docs/official/extra/kubernetes-deployment.md).

To scale further, raise `replicas`, add every pod to `NATS_ROUTES`, and keep an
odd number of servers. `JS_REPLICAS` stays at 3 (at most 5, and at most the
number of servers).

## Clean up

```bash
kubectl delete namespace mono-demo     # also deletes the PVCs, their data and the route Secret
```
