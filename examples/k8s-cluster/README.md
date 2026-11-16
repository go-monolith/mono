# Kubernetes Cluster Example

Runs a mono application as **2 replicas on Kubernetes, one per node**, whose
embedded NATS servers form **one NATS cluster with clustered JetStream**. Every
instance serves every service, and the services behave as one application:

| Service | Kind | In the cluster |
|---|---|---|
| `greeter.hello` | request-reply | every instance subscribes in one queue group; each request is answered by exactly one instance, local or remote |
| `greeter.visits` | stream consumer | the `GREETER_VISITS` stream is replicated to both servers (`WithJetStreamDefaultReplicas`) and consumed through one shared durable consumer, so each visit is handled by one instance (at least once) |
| `greeter.tick` | cron (`@every 15s`) | the schedule lives on the server, in a replicated stream, so each occurrence fires **once for the whole cluster**, not once per instance |

```
            k3s-agent-1                          k3s-server
  ┌────────────────────────────┐      ┌────────────────────────────┐
  │ pod mono-cluster-0         │      │ pod mono-cluster-1         │
  │  greeter module            │      │  greeter module            │
  │  embedded nats-server ─────┼─6222─┼─ embedded nats-server      │
  │   server_name=mono-cluster-0      │   server_name=mono-cluster-1
  │   JetStream (PVC /data)    │      │   JetStream (PVC /data)    │
  │  HTTP :8080                │      │  HTTP :8080                │
  └────────────────────────────┘      └────────────────────────────┘
        ▲ Service mono-cluster:8080 (load-balanced HTTP API)
```

## Files

| File | Purpose |
|---|---|
| `main.go` | Host program: configuration from the environment, the application, the HTTP API |
| `greeter/` | The module (`module.go`) and its contract (`contract/`) |
| `Dockerfile` | Static binary on `distroless/static:nonroot` (~8 MB); build context is the repository root |
| `Dockerfile.dockerignore` | Keeps the build context small |
| `deploy/k8s.yaml` | Namespace, headless and HTTP Services, StatefulSet, PodDisruptionBudget |
| `deploy/create-route-secret.sh` | Creates the Secret with a random route password |

## How the cluster is configured

Every replica runs the same configuration; only the pod name differs.

```go
mono.NewMonoApplication(
    mono.WithNATSServerName(podName),                                   // unique and stable
    mono.WithNATSClustering("mono-demo", "0.0.0.0", 6222, routes),      // every pod, itself included
    mono.WithNATSClusterAuth("route", routePassword),                   // from a Secret
    mono.WithJetStreamStorageDir("/data/jetstream"),                    // per-pod volume
    mono.WithJetStreamDefaultReplicas(2),                               // the framework's streams
    mono.WithJetStreamReadyTimeout(10*time.Minute),                     // how long Start waits for the others
    mono.WithNATSHost("127.0.0.1"), mono.WithNATSInProcessConn(),       // client listener on loopback
)
```

- **Server name.** Clustered JetStream identifies each server by name, so the
  name must be unique within the cluster and stable across restarts. A
  StatefulSet pod name (`mono-cluster-0`, `mono-cluster-1`) is both;
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
- **Start waits for the cluster.** A two-server JetStream cluster has no
  quorum until both servers are routed. The framework's `Start` waits for
  JetStream before it starts the module, and retries creating the streams and
  consumers while the cluster forms or a restarted server catches up (up to
  `WithJetStreamReadyTimeout`). Pods that start together wait for each other
  instead of failing and restarting, and the module needs no code for it.
- **Replicated streams.** `WithJetStreamDefaultReplicas(2)` replicates the
  streams the framework creates for the visits stream consumer and the cron
  schedule. The visits consumer is durable (`greeter-visits`), so it keeps its
  position across restarts, including a stop of every pod.
- **StatefulSet settings:**
  - `podManagementPolicy: Parallel`: a pod is Ready only once JetStream has
    quorum, which needs both pods. With the default ordered start,
    `mono-cluster-1` would never be created.
  - `publishNotReadyAddresses: true` on the headless Service, so routes resolve
    before the pods are Ready.
  - Required `podAntiAffinity` on `kubernetes.io/hostname`: one pod per node.
  - The liveness probe (`/healthz`) only checks the process. It never depends
    on JetStream quorum, or losing one pod would restart the other.
  - `/readyz` reports `Health()`, including its `jetstream` section, and is
    ready while the framework and NATS are healthy, not only while JetStream
    has quorum.

## Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `POD_NAME` | hostname | instance and NATS server name |
| `NATS_ROUTES` | (required) | comma-separated route URLs of every replica |
| `NATS_ROUTE_PASSWORD` | unset | route password (`WithNATSClusterAuth`); unset means routes are not authenticated |
| `NATS_CLUSTER_NAME` | `mono-demo` | NATS cluster name |
| `NATS_CLUSTER_PORT` | `6222` | route listener port |
| `JS_REPLICAS` | `2` | replicas of the framework's streams (at most the number of servers) |
| `JS_STORE_DIR` | `/data/jetstream` | JetStream store directory |
| `JS_READY_TIMEOUT` | `10m` | how long `Start` waits for clustered JetStream |
| `HTTP_ADDR` | `:8080` | HTTP API address |
| `NATS_DEBUG` | unset | `true` adds the embedded server's own log (routes, RAFT elections) at debug level |

## Build and deploy (k3s on Kiri)

The commands assume the 2-node k3s cluster described in `/workspace/kiri/k3s-cluster.md`
(containers `k3s-server` and `k3s-agent-1`, kubeconfig `~/.kube/config-kiri-k3s`).
There is no registry, so the image is imported into each node's containerd.

```bash
# From the repository root
docker build --load -f examples/k8s-cluster/Dockerfile -t mono-k8s-cluster:0.2.0 .

for node in k3s-server k3s-agent-1; do
  docker save mono-k8s-cluster:0.2.0 | docker exec -i $node ctr -n k8s.io images import -
done

export KUBECONFIG=~/.kube/config-kiri-k3s
examples/k8s-cluster/deploy/create-route-secret.sh   # namespace mono-demo
kubectl apply -f examples/k8s-cluster/deploy/k8s.yaml
kubectl -n mono-demo rollout status sts/mono-cluster
kubectl -n mono-demo get pods -o wide                # one pod per node
```

On another cluster, push the image to a registry the nodes can pull from, and
set `image:` in `deploy/k8s.yaml` to it. When you change the code, use a new
tag: with `imagePullPolicy: IfNotPresent`, a node keeps the image it already has.

## Try it

```bash
kubectl -n mono-demo port-forward svc/mono-cluster 8080:8080 &

curl 'localhost:8080/hello?name=ada'
# {"greeting": {"message": "Hello, ada!", "instance": "mono-cluster-1"},
#  "http_instance": "mono-cluster-0", "visit_stored": {"stream": "GREETER_VISITS", "sequence": 1}}
```

`http_instance` is the pod that served the HTTP request (port-forward sticks to
one pod). `greeting.instance` is the pod whose `hello` handler answered. Repeat
the call and both pods show up: the request crossed the NATS route.

```bash
curl localhost:8080/cluster
```

`/cluster` shows:

- **`instances`**: every instance that answered one `$SRV.PING` (NATS services
  protocol, enabled with `WithServiceDiscovery`). Both pods answer, from either
  pod.
- **`visits_stream`**: the stream's replication: `replicas: 2`, the `leader`,
  and the other replica with `"current": true`.

Logs:

```bash
kubectl -n mono-demo logs -l app=mono-cluster --prefix -f | grep -E 'visit recorded|cron tick'
```

Each `cron tick` appears on exactly one pod per occurrence (every 15 s). Each
visit is recorded by whichever pod fetched it. Delivery is at least once: a
visit in flight while pods shut down can be recorded again after the restart.

## Failure behaviour with two servers

Two servers give **no fault tolerance** for JetStream: every replicated stream
and consumer needs a quorum of 2 of 2.

- **While one pod is down:**
  - Request-reply keeps working on the remaining pod, but `/hello` answers
    `502`, because storing the visit (a JetStream write) times out.
  - `/readyz` stays `200`. Its `jetstream` section reports
    `"available": false` ("no meta leader (no quorum)"), so the remaining pod
    keeps receiving traffic.
  - The fetch loops log `Fetch error ... no responders` as a warning, at most
    once every 10 s, and `Fetch error cleared` when the peer is back.
  - Cron occurrences wait.
- **When the pod comes back** (with its PVC), it rejoins in about 10 s:
  - Visits not yet handled are still in the stream.
  - The consumer resumes where it was acknowledged.
  - The returning pod's `Start` waits until its replicas have caught up, so it
    does not restart.

For a cluster that survives losing a node, run **3 replicas** (3 nodes):
set `replicas: 3`, add the third pod to `NATS_ROUTES`, set `JS_REPLICAS` to 3,
and raise the PodDisruptionBudget to `minAvailable: 2`.

Measured on the 2-node k3s cluster on Kiri:

| | |
|---|---|
| cold start (empty volumes) | 11–51 s until both pods are Ready (usually about 11 s), no restarts |
| one pod deleted | rejoined in 11–26 s, no restarts (each pod twice) |
| both pods stopped for 15 s, then started | consumer kept, no visit replayed |
| one pod down (scaled to 1) | `/hello` 502, `jetstream.available: false`, cron paused; both resume when it returns |
| memory per pod | ~10 MiB (limit 192 MiB) |

## Clean up

```bash
kubectl delete namespace mono-demo     # also deletes the PVCs, their data and the route Secret
```
