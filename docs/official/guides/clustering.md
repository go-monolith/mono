# Clustering and Kubernetes

This guide shows how to run several instances of a mono application whose embedded NATS servers form one cluster with clustered JetStream, and how to deploy that cluster on Kubernetes. It applies from **v0.2.0**.

**What you'll learn:**
- What clustered JetStream requires: server names, routes, a storage directory
- Authenticating routes and choosing a replica count
- How `Start` waits for JetStream, and what to put in readiness and liveness probes
- What happens when a server is lost, with 2 and with 3 servers
- The Kubernetes settings a JetStream cluster needs

**Prerequisites:**
- [Services](../core-concepts/services.md), in particular stream consumer and cron services
- [Framework API: NATS and JetStream options](../api/framework.md#configuration-options)

**Working code:** the [Kubernetes cluster example](../../../examples/k8s-cluster/README.md) has a module, a host program and the full manifests.

---

## What a Cluster Changes

Every instance runs the same modules, and its embedded NATS server connects to the other instances' servers through **routes**. Messages then flow across the whole cluster:

- Request-reply and queue group services subscribe in queue groups, so each request is handled by one instance, local or remote.
- A stream consumer service's durable consumer (`<module>-<service>`) is shared by all instances, so each message is handled by one of them.
- A cron schedule lives on the server, so each occurrence fires once for the whole cluster, not once per instance.

JetStream keeps its state (streams, consumers, schedules) in RAFT groups. A group needs a majority of its servers (a **quorum**) to elect a leader and accept changes. This drives most of the rules below.

## Requirements for Clustered JetStream

Each server needs:

| Setting | Option | Rule |
|---------|--------|------|
| Server name | `WithNATSServerName` | Unique within the cluster and **stable across restarts** |
| Cluster name, host, port, routes | `WithNATSClustering` | Same cluster name and the same route list on every server |
| JetStream storage | `WithJetStreamStorageDir` | A persistent directory per server |

**Server names.** JetStream identifies every RAFT peer by its server name, and a server's stored state belongs to that name. Two servers with the same name break placement and leader election; nats-server closes a route from a server that reports its own name ("Remote server has a duplicate name"). A restarted server must come back under the same name. The framework checks only that a name is set and has no whitespace. Uniqueness and stability come from the deployment: a Kubernetes StatefulSet pod name is both, a Deployment's pod name is unique but changes on every restart, and a random value is neither.

**Routes.** Give every server the same list: all servers of the cluster, its own included. A server ignores the route to itself, so one list fits every instance. A route has the form `nats://[user:password@]host:port`; percent-encode special characters in credentials.

Without a config file, `NewMonoApplication` returns an error when clustering and JetStream are configured without a server name or without a route. With a config file ([`WithNATSConfigFile`](../api/framework.md#withnatsconfigfile)), the checks run at `Start` on the merged configuration.

## Programmatic Configuration

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/go-monolith/mono"
)

func main() {
    // The same list on every server, this one included.
    routes := []string{
        "nats://orders-0.orders-nats.prod.svc.cluster.local:6222",
        "nats://orders-1.orders-nats.prod.svc.cluster.local:6222",
        "nats://orders-2.orders-nats.prod.svc.cluster.local:6222",
    }

    app, err := mono.NewMonoApplication(
        mono.WithNATSServerName(os.Getenv("POD_NAME")),
        mono.WithNATSClustering("orders", "0.0.0.0", 6222, routes),
        mono.WithNATSClusterAuth("route", os.Getenv("NATS_ROUTE_PASSWORD")),
        mono.WithJetStreamStorageDir("/data/jetstream"),
        mono.WithJetStreamDefaultReplicas(3),
        mono.WithJetStreamReadyTimeout(5*time.Minute),
    )
    if err != nil {
        log.Fatalf("configure: %v", err)
    }
    // app.Register(...) your modules here.

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()
    if err := app.Start(ctx); err != nil {
        log.Fatalf("start: %v", err)
    }
    <-ctx.Done()
    _ = app.Stop(context.Background())
}
```

### With a NATS config file

`WithNATSClustering` is merged into the file's `cluster{}` block: name, host, port and routes come from the option, and the file's authorization, TLS, permissions, pool size and compression apply. `WithNATSServerName` overrides the file's `server_name`, and `WithNATSClusterAuth` overrides the user and password of its `cluster.authorization`. Use a file for what the options cannot express, such as route TLS.

## Route Authentication

`WithNATSClusterAuth(username, password)` makes every server require the credentials from incoming routes, and adds them to every route URL that has none (nats-server only sends the credentials written in a route URL). It applies to config-file routes as well. Credentials are redacted from the framework's logs and errors.

- Load the password from a secret, never from code.
- The credentials are only safe on a trusted network unless the routes also use TLS (a `cluster { tls { ... } }` block in a config file).
- Limit who can reach the route port at all, for example with a NetworkPolicy.

## Replicas

The streams the framework creates for stream consumer, event stream consumer and cron services have one replica unless their configuration says otherwise. In a cluster, a one-replica stream lives on a single server and is unavailable while that server is down; a cron schedule then stops firing.

| Setting | Effect |
|---------|--------|
| `WithJetStreamDefaultReplicas(n)` | Replicas of framework-created streams whose config leaves `Replicas` at 0. Consumers follow the stream |
| `StreamConfig.Replicas` | Per stream consumer service or event stream |
| `CronServiceConfig.Replicas` | Per cron service |

A replicated stream keeps working while fewer than half of its replicas are down, so **2 replicas tolerate no loss**. Use 3 in a cluster of at least three servers (5 is the maximum). Raising the value later updates existing streams in place.

## Startup, Readiness and Health

**`Start` waits for JetStream.** Before it starts plugins and modules, `Start` waits until JetStream answers, which in a cluster means a quorum of the servers is routed and the meta group has a leader. It then retries each stream, consumer and schedule setup step while the error can clear by itself (no leader yet, no peers for placement yet, a replica catching up, a timeout). Servers that start together wait for each other instead of failing and restarting. While waiting, the framework logs `Waiting for JetStream` at Info. The wait and each step are bounded by `WithJetStreamReadyTimeout` (default 2 minutes); when the bound is reached, `Start` fails with an error that names the option.

**Health.** `app.Health(ctx).JetStream` reports the JetStream state of this server, read locally:

| Field | Meaning |
|-------|---------|
| `Available` | JetStream can serve requests from this server (in a cluster: the meta group has a leader) |
| `Clustered` | JetStream runs clustered |
| `MetaLeader` | Server name of the meta leader; empty without quorum |
| `ClusterSize` | JetStream servers the meta group knows |
| `Message` | Why JetStream is not available |

`JetStream` is not part of `Healthy`, by design. Without quorum JetStream is unavailable on **every** server, so a check that includes it fails on all survivors when one server is lost.

**Probes:**

- **Liveness**: check only that the process works (for example a plain HTTP 200 from the host program). It must never depend on JetStream quorum, or losing one server restarts the others, and the restarts prevent the quorum from coming back.
- **Readiness**: may include `JetStream.Available` when traffic sent to the instance needs JetStream.
- `Start` can take up to the JetStream ready timeout. If your probe endpoint is served only after `Start` returns, give the probes an initial delay or a startup probe that covers it.

```go
http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
    h := app.Health(r.Context())
    if !h.Healthy || (h.JetStream != nil && !h.JetStream.Available) {
        http.Error(w, "not ready", http.StatusServiceUnavailable)
        return
    }
    w.WriteHeader(http.StatusOK)
})
```

## When a Server Is Lost

Core NATS (request-reply and queue group services, events) keeps working on the remaining servers as long as they are routed to each other. JetStream depends on quorum:

| Cluster | Servers lost | Meta group | R3 streams | R2 streams | R1 streams on the lost server |
|---------|--------------|------------|------------|------------|-------------------------------|
| 2 servers | 1 | No quorum: `JetStream.Available` is false on the survivor | n/a | Unavailable | Unavailable |
| 3 servers | 1 | Quorum kept | Keep working | Unavailable if one of their 2 replicas was on it | Unavailable |
| 3 servers | 2 | No quorum | Unavailable | Unavailable | Unavailable |

A two-server cluster therefore tolerates no loss for JetStream. Run an **odd number of servers**, at least 3 when JetStream must survive the loss of one; a fourth server raises the quorum without tolerating more losses.

While a stream or consumer is unavailable, the fetch loops of stream consumer, event stream consumer and cron services log `Fetch error` (or `Cron fetch error`) at Warn, at most once per 10s with `failures` and `suppressed` counts, and wait 1s between failed fetches. They log `... cleared` at Info once the stream is back. A server restarted while quorum is missing waits in `Start` until quorum returns or the ready timeout is reached.

On shutdown, nats-server steps down the RAFT leaders it holds; the framework adds no leadership transfer of its own.

## Kubernetes

The [Kubernetes cluster example](../../../examples/k8s-cluster/README.md) has complete manifests. These are the settings that matter for a JetStream cluster.

### StatefulSet

Use a StatefulSet, not a Deployment: its pod names (`orders-0`, `orders-1`, ...) are unique and stable, so they serve as server names, and each pod keeps its own volume for the JetStream store.

- **`podManagementPolicy: Parallel`.** With the default ordered start, `orders-1` is created only after `orders-0` is Ready. `orders-0` alone has no JetStream quorum, so its `Start` waits for JetStream and fails after the ready timeout whatever its probes check, and the cluster never forms. Parallel creates all pods at once, so their servers route to each other and elect leaders while every `Start` waits.
- **An odd number of replicas**, 3 or more for JetStream that survives the loss of a server.
- **Required `podAntiAffinity` on `kubernetes.io/hostname`**, so that one node failure takes down only one server.
- **A PodDisruptionBudget** with `maxUnavailable: 1`, so that a node drain never takes down a quorum.
- **A volumeClaimTemplate** mounted at the JetStream storage directory.

### Headless Service and routes

- Expose the route port through a **headless Service** (`clusterIP: None`) with **`publishNotReadyAddresses: true`**. Routes must resolve before the pods are Ready, since a pod becomes Ready only after the cluster formed.
- Use **fully qualified route names**: `<pod>.<headless-service>.<namespace>.svc.cluster.local`. While a pod's DNS record is not yet published, a short name falls through the search domains to the upstream resolver, and an upstream that answers every name (a fake-IP or split-horizon DNS) sends the route to an unrelated host. A fully qualified name in the cluster domain is answered by the cluster DNS alone.

### Server name and secrets

Take the server name from the pod name through the downward API, and the route password from a Secret:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: NATS_ROUTE_PASSWORD
    valueFrom:
      secretKeyRef:
        name: orders-nats-routes
        key: password
```

The host program passes them to `WithNATSServerName(os.Getenv("POD_NAME"))` and `WithNATSClusterAuth("route", os.Getenv("NATS_ROUTE_PASSWORD"))`.

### Probes

```yaml
livenessProbe:          # process only, never JetStream quorum
  httpGet: { path: /healthz, port: 8080 }
readinessProbe:         # may include JetStream.Available
  httpGet: { path: /readyz, port: 8080 }
```

`Start` can wait up to the JetStream ready timeout for the other servers. Serve the liveness endpoint before calling `Start`, as the example does, so the wait is not mistaken for a hung process. If your liveness endpoint only answers once `Start` has returned, add a `startupProbe` whose `periodSeconds × failureThreshold` exceeds the ready timeout.

Set the pod's `terminationGracePeriodSeconds` above `WithShutdownTimeout`, so that module `Stop` runs before the pod is killed (see [Graceful Shutdown](../api/framework.md#withshutdowntimeout)).

## Checklist

- [ ] A stable, unique server name per server (`WithNATSServerName`, from the StatefulSet pod name)
- [ ] The same route list on every server, all servers included, with fully qualified names
- [ ] Route authentication (`WithNATSClusterAuth`) with the password from a Secret
- [ ] `WithJetStreamDefaultReplicas(3)` (and `Replicas` on streams and cron services that set their own)
- [ ] An odd number of servers, one per node, with a PodDisruptionBudget
- [ ] A liveness probe that never depends on JetStream; readiness may include `JetStream.Available`
- [ ] `podManagementPolicy: Parallel` and `publishNotReadyAddresses: true`

## Related Documentation

- [Framework API](../api/framework.md) - `WithNATSClustering`, `WithNATSServerName`, `WithNATSClusterAuth`, `WithJetStreamReadyTimeout`, `WithJetStreamDefaultReplicas`, health
- [Service Container API](../api/container.md) - stream consumer and cron services
- [Upgrading to v0.2.0](upgrading.md#upgrading-to-v020)
- [Kubernetes cluster example](../../../examples/k8s-cluster/README.md)
