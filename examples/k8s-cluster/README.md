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
     embedded NATS servers, routes on 6222 in a full mesh (route TLS),
     server_name = pod name
        ▲ Service mono-cluster:8080 (load-balanced HTTP API)
        ▲ Service mono-cluster-client:4222 (NATS clients, mutual TLS)
        │
  ┌─────┴─────────────────────────────────────────┐
  │ Deployment mono-greeter-client (2 pods)         │
  │  nats.go client, helper.NATSClientTLS           │
  │  HTTP :8080 ◀── Service mono-greeter-client     │
  └─────────────────────────────────────────────────┘
```

The servers' own HTTP API reaches the greeter in-process. The client
Deployment is a separate program (`client/main.go`) that reaches it the way an
application outside the cluster's servers would: over NATS, through the client
listener, with a certificate from a client CA.

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
examples/k8s-cluster/deploy/create-route-secret.sh       # route and system passwords (namespace mono-demo)
examples/k8s-cluster/deploy/create-route-tls-secret.sh   # route TLS certificate (needs openssl)
examples/k8s-cluster/deploy/create-client-tls-secret.sh  # client TLS certificates (needs openssl)
# or, with cert-manager installed instead of the scripts:
#   kubectl apply -f examples/k8s-cluster/deploy/route-tls-cert-manager.yaml \
#                 -f examples/k8s-cluster/deploy/client-tls-cert-manager.yaml
kubectl apply -f examples/k8s-cluster/deploy/k8s.yaml
kubectl -n mono-demo rollout status sts/mono-cluster
kubectl -n mono-demo get pods -o wide                # one pod per node
kubectl apply -f examples/k8s-cluster/deploy/client.yaml           # the NATS client Deployment
kubectl apply -f examples/k8s-cluster/deploy/network-policy.yaml   # optional, see Client TLS
```

Set `image:` in `deploy/client.yaml` to `$IMAGE` as well: the image holds both
programs.

When you change the code, use a new tag: with `imagePullPolicy: IfNotPresent`,
a node keeps the image it already has.

## Files

| File | Purpose |
|---|---|
| `main.go` | Host program: configuration from the environment, the application, the HTTP API |
| `client/main.go` | NATS client outside the application: mutual TLS to the client listener, the greeter's services over NATS, an HTTP API |
| `greeter/` | The module (`module.go`) and its contract (`contract/`) |
| `Dockerfile` | Static binaries (`/k8s-cluster`, `/k8s-cluster-client`) on `gcr.io/distroless/static-debian12:nonroot`; build context is the repository root |
| `Dockerfile.dockerignore` | Keeps the build context small |
| `deploy/k8s.yaml` | Namespace, headless, HTTP and NATS client Services, StatefulSet, PodDisruptionBudget |
| `deploy/client.yaml` | The client's Deployment and HTTP Service |
| `deploy/network-policy.yaml` | Allows 6222 only between the servers and 4222 only from the client pods |
| `deploy/create-route-secret.sh` | Creates the Secret with a random route password and system account password |
| `deploy/create-route-tls-secret.sh` | Creates a route CA (in `./route-ca/`, git-ignored) and the Secret `mono-cluster-route-tls` with a certificate for every pod |
| `deploy/route-tls-cert-manager.yaml` | The same Secret issued by cert-manager: a 24-hour certificate renewed every 8 hours |
| `deploy/create-client-tls-secret.sh` | Creates a client CA (in `./client-ca/`, git-ignored), the listener certificate (Secret `mono-cluster-client-tls`) and the client's certificate (Secret `mono-greeter-client-tls`) |
| `deploy/client-tls-cert-manager.yaml` | The same Secrets issued by cert-manager, with their own CA: 24-hour certificates renewed every 8 hours |

## Framework options

Every replica runs the same configuration; only the pod name differs.

```go
mono.NewMonoApplication(
    mono.WithNATSServerName(podName),                                   // unique and stable
    mono.WithNATSClustering("mono-demo", "0.0.0.0", 6222, routes),      // every pod, itself included
    mono.WithNATSClusterAuth("route", routePassword),                   // from a Secret
    mono.WithNATSClusterTLS(crt, key, ca),                              // mutual TLS on the routes
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
  from the Secret `mono-cluster-routes` (`deploy/create-route-secret.sh`).
- **Route TLS.** `WithNATSClusterTLS` makes every route use mutual TLS with the
  certificate, key and CA in the Secret `mono-cluster-route-tls`, mounted at
  `NATS_ROUTE_TLS_DIR`. One certificate for
  `*.mono-cluster-nats.mono-demo.svc.cluster.local` covers every pod; a server
  without a certificate from the route CA cannot join, even with the password.
  - **Renewing:** the running servers pick up a renewed certificate for new
    routes without a restart, within 10 seconds of the kubelet updating the
    mounted files (about a minute after the Secret changes); they log
    `Reloaded the cluster TLS certificate`. Routes already established keep
    their session, also past the expiry of the certificate they started with.
    A certificate from another CA is refused (the servers keep the previous
    one), so the script refuses to create a new CA while the Secret exists
    unless `NEW_CA=1` is set.
  - **Short-lived certificates:** the cert-manager manifest issues a 24-hour
    certificate, renewed every 8 hours with a new key, so a leaked key is
    useful for at most a day. Its `renewBefore` (16 hours) is how long
    cert-manager may be down before the certificate expires; after that a
    restarted pod fails to start and cannot rejoin until cert-manager renews it.
    cert-manager accepts a `duration` down to 1 hour. The script issues a
    certificate for 365 days by default, since it is renewed by hand; use
    cert-manager for short lifetimes.
  - **Moving from the script to cert-manager:** delete the Secret
    (`kubectl -n mono-demo delete secret mono-cluster-route-tls`) before
    applying the manifest. cert-manager adopts a valid existing certificate as
    it is and would renew it only shortly before it expires. The new CA then
    needs every pod restarted at once, as below.
  - **Turning it on for a running cluster:** servers with and without route TLS
    cannot route to each other, and a rolling update would wait forever for the
    first TLS pod to find a quorum. Switch every pod at once: set the
    StatefulSet's `updateStrategy` to `OnDelete`, apply the manifest, delete all
    pods, then restore `RollingUpdate`. JetStream is unavailable until the pods
    are back (about 20 s in the test below).
- **Client TLS.** `WithNATSClientTLS` makes the client listener (4222) require
  mutual TLS with the certificate and key in the Secret
  `mono-cluster-client-tls`, mounted at `NATS_CLIENT_TLS_DIR`, and a client
  certificate from the CA in its `ca.crt`. The listener then binds to the pod
  IP behind the Service `mono-cluster-client`; without `NATS_CLIENT_TLS_DIR` it
  stays on `127.0.0.1`, never exposed without TLS. The application's own client
  connects in-process either way.
  - **Its own CA.** The client CA is never the route CA: every server accepts
    a certificate from the route CA as a cluster peer, so a client holding one
    could join the cluster. The servers refuse to start with a client CA that
    is, or is signed by, the route CA. One client CA issues both the listener
    certificate (for `mono-cluster-client.mono-demo.svc.cluster.local`, and
    `localhost`/`127.0.0.1` for `kubectl port-forward`) and the clients'
    certificates, so each Secret's `ca.crt` serves both directions.
  - **The client** (`client/main.go`, `deploy/client.yaml`) connects to
    `tls://mono-cluster-client.mono-demo.svc.cluster.local:4222` with
    `helper.NATSClientTLS`, which presents its certificate from the Secret
    `mono-greeter-client-tls` and reloads it when the Secret is renewed, so a
    renewed certificate is used on the next reconnect. It calls the greeter
    on its contract's subjects (`services.greeter.hello`, a JetStream publish
    to `services.greeter.visits`), answers `/hello` like the servers' API, and
    reconnects to another pod when its server goes away. It verifies the pods
    it discovers by IP address against the Service name it first dialed.
  - **Renewing:** as for routes, the servers pick up a renewed listener
    certificate for new connections, and the clients a renewed client
    certificate on their next reconnect, without a restart; established
    connections keep their session, also past their certificates' expiry. The
    cert-manager manifest issues 24-hour certificates renewed every 8 hours.
    The servers read the client CA once: to replace it, put the old and the new
    CA in `ca.crt` and do a rolling restart.
  - **Turning it on for a running cluster:** a rolling update is enough (no
    restart of every pod at once, unlike route TLS), since routes are not
    affected. Clients must use TLS from then on.
  - **A certificate is not a permission.** Every client with a valid
    certificate gets the global account, JetStream included.
    `deploy/network-policy.yaml` limits port 4222 to the client pods and 6222
    to the servers; it needs a network plugin that enforces NetworkPolicy.
  - **Operators** reach a pod through `kubectl port-forward`, now with TLS
    and a client certificate (any certificate from the client CA):

    ```bash
    kubectl -n mono-demo port-forward pod/mono-cluster-0 4222:4222 &
    for k in tls.crt tls.key ca.crt; do
      kubectl -n mono-demo get secret mono-greeter-client-tls -o jsonpath="{.data.${k//./\\.}}" | base64 -d > "op-$k"
    done
    nats -s tls://127.0.0.1:4222 --tlsca op-ca.crt --tlscert op-tls.crt --tlskey op-tls.key \
      --user admin --password "$SYSTEM_PASSWORD" server report jetstream
    ```
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
| `NATS_ROUTE_TLS_DIR` | unset | directory with `tls.crt`, `tls.key` and `ca.crt` for route TLS (`WithNATSClusterTLS`); unset means routes without TLS |
| `NATS_CLIENT_TLS_DIR` | unset | directory with the listener's `tls.crt` and `tls.key`, and `ca.crt`, the client CA, for client mutual TLS (`WithNATSClientTLS`); the listener then binds to `0.0.0.0`; unset means the listener stays on `127.0.0.1` without TLS |
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

Through the client Deployment, over NATS with mutual TLS:

```bash
kubectl -n mono-demo port-forward svc/mono-greeter-client 8081:8080 &
curl 'localhost:8081/hello?name=ada'
# {"greeting": {"message": "Hello, ada!", "instance": "mono-cluster-2"},
#  "http_instance": "mono-greeter-client-…", "visit_stored": {"stream": "GREETER_VISITS", "sequence": 3, "duplicate": false}}
```

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
| rolling restart with route TLS | 0 of 443 | 0 / 0 / 0 |
| route TLS with cert-manager at 1-hour certificates renewed every 15 minutes, for 69 minutes: four renewals, a pod deleted in the second of a renewal, routes outliving their certificate's expiry, then a rolling restart | 0 of 19,993 | 0 / 0 / 0 |
| one pod down for about 40 s, then back | 0 of 469 | 0 / 0 / 0 |
| scaling an existing 2-replica deployment to 3 | 1 of 389 | 0 / 0 / 0 |

Memory per pod: about 10–20 MiB (limit 192 MiB).

For canaries, rollbacks, node drains and blue-green cutovers, see [Kubernetes Deployment Strategies](../../docs/official/extra/kubernetes-deployment.md).

To scale further, raise `replicas`, add every pod to `NATS_ROUTES`, and keep an
odd number of servers. `JS_REPLICAS` stays at 3 (at most 5, and at most the
number of servers).

## Clean up

```bash
kubectl delete namespace mono-demo     # also deletes the PVCs, their data and the Secrets
```
