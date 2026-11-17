# Clustering and Kubernetes

This guide shows how to run several instances of a mono application whose embedded NATS servers form one cluster with clustered JetStream, and how to deploy that cluster on Kubernetes. It applies from **v0.2.0**.

Read it before you run more than one instance of an application that uses JetStream (stream consumer, event stream consumer or cron services). The [Checklist](#checklist) at the end sums up the settings.

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

Without a config file, `NewMonoApplication` returns an error when clustering and JetStream are configured without a server name or without a route. With a config file ([`WithNATSConfigFile`](../api/framework.md#withnatsconfigfile)), the checks run when the embedded server starts, still inside `NewMonoApplication`, on the merged configuration.

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
        "nats://mono-cluster-0.mono-cluster-nats.mono-demo.svc.cluster.local:6222",
        "nats://mono-cluster-1.mono-cluster-nats.mono-demo.svc.cluster.local:6222",
        "nats://mono-cluster-2.mono-cluster-nats.mono-demo.svc.cluster.local:6222",
    }

    app, err := mono.NewMonoApplication(
        mono.WithNATSServerName(os.Getenv("POD_NAME")),
        mono.WithNATSClustering("mono-demo", "0.0.0.0", 6222, routes),
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

`WithNATSClustering` is merged into the file's `cluster{}` block: name, host and port come from the option, its routes are added to the file's `cluster.routes` (the server uses both lists), and the file's authorization, TLS, permissions, pool size and compression apply. `WithNATSServerName` sets `server_name` when the file has none, and must equal it when the file sets one (otherwise `NewMonoApplication` fails), and `WithNATSClusterAuth` overrides the user and password of its `cluster.authorization`. Use a file for what the options cannot express, such as route TLS.

## Route Authentication

`WithNATSClusterAuth(username, password)` makes every server require the credentials from incoming routes, and adds them to every route URL that has none (nats-server only sends the credentials written in a route URL). It applies to config-file routes as well. Credentials are redacted from the framework's logs and errors.

- Load the password from a secret, never from code.
- The credentials are only safe on a trusted network unless the routes also use TLS (a `cluster { tls { ... } }` block in a config file).
- Limit who can reach the route port at all, for example with a NetworkPolicy.

## Operator Access (System Account)

Since v0.2.2. Some cluster operations need the NATS system account: `nats server report jetstream` lists every server of the meta group, and `nats server raft peer-remove <server>` removes a server from it, which retiring servers for good requires (scaling down for good, or a [blue-green cutover](../extra/kubernetes-deployment.md#joined-cluster)). `WithNATSSystemAccount(username, password)` adds a system account user:

```go
mono.WithNATSSystemAccount("admin", os.Getenv("NATS_SYSTEM_PASSWORD"))
```

- Clients without credentials, the framework's own included, keep working in the global account, so the application's streams and consumers are unaffected. Enabling it on a running cluster one server at a time is safe. A client that sends credentials (a URL with user info, a token) is now rejected unless they are the system account user's: remove leftover credentials from client URLs first.
- The user connects to the client listener: keep it private (the example binds it to loopback and operators use a port-forward), and load the password from a secret.
- **This is not client authentication.** The system account protects only the `$SYS` requests. Clients without credentials still have full access to the global account, every stream and consumer included. Keep the client listener unexposed (loopback, as in the example), or configure real client authorization in a NATS config file and add the system account user there.
- With a NATS config file, add the user there instead when the file configures accounts or has an `authorization` block: `accounts { "$SYS": { users: [ { user: admin, password: $NATS_SYSTEM_PASSWORD } ] } }` and `system_account: "$SYS"`. With only these two settings, clients without credentials keep working too. Combining `WithNATSSystemAccount` with such a file (accounts, `system_account`, an `authorization` block, users, `no_sys_acc` or an operator) is rejected: `NewMonoApplication` returns an error.

## Replicas

The streams the framework creates for stream consumer, event stream consumer and cron services have one replica unless their configuration says otherwise. In a cluster, a one-replica stream lives on a single server and is unavailable while that server is down; a cron schedule then stops firing.

| Setting | Effect |
|---------|--------|
| `WithJetStreamDefaultReplicas(n)` | Replicas of framework-created streams whose config leaves `Replicas` at 0. Consumers follow the stream |
| `StreamConfig.Replicas` | Per stream consumer service or event stream |
| `CronServiceConfig.Replicas` | Per cron service |

A replicated stream keeps working while fewer than half of its replicas are down, so **2 replicas tolerate no loss**. Use 3 in a cluster of at least three servers (5 is the maximum). Raising the value later updates existing streams in place.

## Startup, Readiness and Health

**`Start` waits for JetStream.** Servers that start together wait for each other instead of failing and restarting:

- Before it starts plugins and modules, `Start` waits until JetStream answers, which in a cluster means a quorum of the servers is routed and the meta group has a leader.
- It then retries each stream, consumer and schedule setup step while the error can clear by itself (no leader yet, no peers for placement yet, a replica catching up, a timeout).
- While waiting, the framework logs `Waiting for JetStream` at Info.
- The wait and each step are bounded by `WithJetStreamReadyTimeout` (default 2 minutes). When the bound is reached, `Start` fails with an error that names the option.

**Health.** `app.Health(ctx).JetStream` reports the JetStream state of this server, read locally:

| Field | Meaning |
|-------|---------|
| `Available` | JetStream can serve requests from this server (in a cluster: the meta group has a leader) |
| `Clustered` | JetStream runs clustered |
| `MetaLeader` | Server name of the meta leader; empty without quorum |
| `ClusterSize` | JetStream servers the meta group knows |
| `Message` | Why JetStream is not available |

`JetStream` is not part of `Healthy`, by design. Without quorum JetStream is unavailable on **every** server, so a check that includes it fails on all survivors when one server is lost.

### Probes

- **Liveness**: check only that the process works (for example a plain HTTP 200 from the host program). It must never depend on JetStream quorum, or losing one server restarts the others, and the restarts prevent the quorum from coming back.
- **Readiness**: may include `JetStream.Available` when traffic sent to the instance needs JetStream.
- **The `Start` wait**: `Start` can take up to the JetStream ready timeout while it waits for the other servers. Serve the liveness endpoint before calling `Start`, as the example does, so the wait is not mistaken for a hung process. If your probe endpoint is served only after `Start` returns, give the probes an initial delay, or a `startupProbe` whose `periodSeconds × failureThreshold` exceeds the ready timeout.

A readiness endpoint that includes JetStream:

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

## Protecting Existing Data

These situations can delete JetStream data. The framework cannot detect most of them, so plan for them.

- **Do not turn an existing standalone store into a cluster member.** When a server starts in clustered mode, the cluster's meta layer is the source of truth: about 30s after it recovers, nats-server deletes every locally stored stream and consumer the meta layer does not know. That includes your own streams, KV buckets and object stores, and all consumer positions. Start the cluster on empty storage directories, then move the data with `nats stream backup` / `restore`, or with mirrors or sources from the old server.
- **Do not rename a JetStream server.** A server's RAFT identity is derived from its name, so a server with a new name is a new peer, and the cluster removes the local replicas assigned to the old one. A single-replica stream on that server is lost. Keep the name stable, which is why a StatefulSet pod name fits. When both `WithNATSServerName` and `server_name` in a config file are set, `NewMonoApplication` returns an error unless they are equal.
- **Single-replica streams live on one server.** Losing that server's volume loses them. Use `WithJetStreamDefaultReplicas(3)`, or `Replicas` on your own streams. The KV and object stores of the `kv-jetstream` and `fs-jetstream` plugins are not covered by this option, so set their replicas in the plugin configuration.
- **Non-durable consumers.** A consumer created with a `Name` only is deleted by the server once it has not been pulled for its inactivity threshold. That replays its stream when the consumer is recreated, and on an interest-retention stream it removes the messages only that consumer still needed. The framework's own consumers are durable. At start it gives non-durable consumers still at the default 5s threshold, yours included, one hour (see [Upgrading](upgrading.md#go-126-and-nats-server-v21410)). Use durable consumers for anything that must survive. The start pass covers every stream of the account, so it also reaches plain non-durable pull consumers of other clients connected to the same servers (another replica during a rolling update, a script, a service in another language): when such a client goes away, its consumer lives up to an hour instead of five seconds. On an interest-retention stream it keeps its messages for that hour, and on a work-queue stream a new consumer with an overlapping filter is rejected until it expires. Give such consumers an explicit `InactiveThreshold` (any value other than 5s is left alone), or make them durable.
- **Rolling back.** See [Rolling Back from v0.2.x](upgrading.md#rolling-back-from-v02x).

## Kubernetes

The [Kubernetes cluster example](../../../examples/k8s-cluster/README.md) has complete manifests. These are the settings that matter for a JetStream cluster.

### StatefulSet

Use a StatefulSet, not a Deployment: its pod names (`mono-cluster-0`, `mono-cluster-1`, ...) are unique and stable, so they serve as server names, and each pod keeps its own volume for the JetStream store.

- **`podManagementPolicy: Parallel`.** With the default ordered start, `mono-cluster-1` is created only after `mono-cluster-0` is Ready. `mono-cluster-0` alone has no JetStream quorum, so its `Start` waits for JetStream and fails after the ready timeout whatever its probes check, and the cluster never forms. Parallel creates all pods at once, so their servers route to each other and elect leaders while every `Start` waits.
- **An odd number of replicas**, 3 or more for JetStream that survives the loss of a server.
- **Required `podAntiAffinity` on `kubernetes.io/hostname`**, so that one node failure takes down only one server.
- **A PodDisruptionBudget** with `minAvailable: 2` (for 3 servers; `maxUnavailable: 1` is equivalent), so that a node drain never takes down a quorum.
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
        name: mono-cluster-routes
        key: password
```

The host program passes them to `WithNATSServerName(os.Getenv("POD_NAME"))` and `WithNATSClusterAuth("route", os.Getenv("NATS_ROUTE_PASSWORD"))`.

### Probe and shutdown settings

The probes follow the rules in [Probes](#probes):

```yaml
livenessProbe:          # process only, never JetStream quorum
  httpGet: { path: /healthz, port: 8080 }
readinessProbe:         # may include JetStream.Available
  httpGet: { path: /readyz, port: 8080 }
```

Set the pod's `terminationGracePeriodSeconds` above `WithShutdownTimeout`, so that module `Stop` runs before the pod is killed (see [Graceful Shutdown](../api/framework.md#withshutdowntimeout)).

### Rolling updates

Kubernetes removes a terminating pod from the Service endpoints asynchronously, while it sends the container SIGTERM. A host program that closes its HTTP listener on SIGTERM can therefore still receive a few requests it no longer accepts, which clients see as connection errors. Delay the signal with a `preStop` sleep. The `sleep` action needs no shell in the image (Kubernetes 1.30+):

```yaml
lifecycle:
  preStop:
    sleep:
      seconds: 5
```

The sleep counts toward `terminationGracePeriodSeconds`, so the grace period must cover the sleep plus `WithShutdownTimeout`. With 3 servers and 3 replicas, a rolling update keeps JetStream available: one server restarts at a time and the other two keep the quorum. For canaries, rollbacks, node drains and blue-green cutovers, with measured results, see [Kubernetes Deployment Strategies](../extra/kubernetes-deployment.md).

## Checklist

- [ ] A stable, unique server name per server (`WithNATSServerName`, from the StatefulSet pod name)
- [ ] The same route list on every server, all servers included, with fully qualified names
- [ ] Route authentication (`WithNATSClusterAuth`) with the password from a Secret
- [ ] `WithJetStreamDefaultReplicas(3)` (and `Replicas` on streams and cron services that set their own)
- [ ] An odd number of servers, one per node, with a PodDisruptionBudget
- [ ] A liveness probe that never depends on JetStream; readiness may include `JetStream.Available`
- [ ] A system account user (`WithNATSSystemAccount`) if operators need `nats server report` or to remove servers for good
- [ ] A `preStop` sleep, so a terminating pod keeps serving until it has left the Service endpoints
- [ ] `podManagementPolicy: Parallel` and `publishNotReadyAddresses: true`

## Related Documentation

- [Framework API](../api/framework.md) - `WithNATSClustering`, `WithNATSServerName`, `WithNATSClusterAuth`, `WithJetStreamReadyTimeout`, `WithJetStreamDefaultReplicas`, health
- [Service Container API](../api/container.md) - stream consumer and cron services
- [Upgrading from v0.1.x to v0.2.x](upgrading.md)
- [Kubernetes cluster example](../../../examples/k8s-cluster/README.md)
- [Kubernetes Deployment Strategies](../extra/kubernetes-deployment.md) - rolling, canary and blue-green deployments, measured
