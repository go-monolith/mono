# Framework API

API documentation for the `MonoApplication` (alias for `MonoFramework`), the main entry point for the Monolith Framework.

## Overview

`MonoApplication` is the central coordinator for module registration, lifecycle management, and inter-module communication. It manages NATS messaging, module dependencies, and graceful shutdown.

A typical program creates the application with options, registers its modules, calls `Start`, waits for a shutdown signal and calls `Stop`. See [Complete Application Setup](#complete-application-setup).

{% hint style="info" %}
**Functional Options Pattern:** All framework configuration uses `WithXxx()` option functions. Options are validated at application time and last-wins for conflicting settings. See [Configuration Options](#configuration-options).
{% endhint %}

## Signatures

### Creating an Application

```go
func NewMonoApplication(opts ...MonoFrameworkOption) (MonoApplication, error)
```

Creates a new framework instance with the given configuration options and starts the embedded NATS server. Returns an error if any option validation fails.

**Parameters:**
- `opts` - Zero or more functional option functions to configure the framework

**Returns:**
- `MonoApplication` - The framework instance
- `error` - Validation error or configuration error

**Example:**
```go
app, err := mono.NewMonoApplication(
    mono.WithNATSPort(4222),
    mono.WithJetStreamStorageDir("./data"),
    mono.WithLogLevel(mono.LogLevelInfo),
)
if err != nil {
    log.Fatalf("Failed to create application: %v", err)
}
```

### Lifecycle Methods

#### Start

```go
func (app MonoApplication) Start(ctx context.Context) error
```

Starts plugins, middleware and all registered modules in dependency order (the embedded NATS server is already running: `NewMonoApplication` starts it). With JetStream it first waits for JetStream (see [`WithJetStreamReadyTimeout`](#withjetstreamreadytimeout)).

**Errors:**
- A module fails to start: the previously started modules are rolled back, then the started middleware and the plugins are stopped, and the state returns to `StateCreated`. Any other failure after the plugins started stops them the same way.
- `application already running` when the application is running.
- `application already stopped, cannot restart` after `Stop`.
- `application is already starting` for a second `Start` while one is running, and `application is stopping, cannot start` during `Stop`. Only one `Start` runs at a time.
- An error wrapping `context.Canceled` when `Stop` was called while `Start` was running (see [`Stop`](#stop)).
- An error wrapping `monoerrors.ErrStopAbandoned` while the `Stop` of a module that an earlier stop or rollback gave up on is still running (see [`Stop`](#stop)).

After a failed `Start` the application is back in `StateCreated` and `Start` may be called again. Since v0.2.18 the retry finds nothing of the failed attempt: the events and event consumers it registered are forgotten, its channel routers have ended, and the built-in audit and access log middleware start again with their log still open. `Stop` on an application whose `Start` failed closes those logs.

When `Start` fails, what it had started is stopped with a context of its own, not with `ctx`: `ctx` has usually ended by then, and a module's `Stop` given an ended context would skip its cleanup. That context is bounded by the shutdown timeout ([`WithShutdownTimeout`](#withshutdowntimeout), default 30 seconds). A `Stop` that aborts the start hands over its own bound: the stopping ends when that `Stop`'s context does.

The context a module's `Start` receives is derived from `ctx` and stays live after `Start` has returned, so a module may keep it for the goroutines it starts. It ends once `Stop` has finished, or when `ctx` ends.

#### Stop

```go
func (app MonoApplication) Stop(ctx context.Context) error
```

Gracefully shuts down the application, in this order:

1. Drains subscriptions and waits for in-flight handlers (up to the shutdown drain timeout, with a live context).
2. Cancels the runtime context.
3. Stops all modules in reverse dependency order.
4. Stops NATS.

The configured shutdown timeout bounds the whole operation when `ctx` has no deadline. See [`WithShutdownTimeout`](#withshutdowntimeout) and [`WithShutdownDrainTimeout`](#withshutdowndraintimeout).

**A module `Stop` that does not return** (since v0.2.18; before, it held the shutdown for as long as it ran, with NATS never stopped):

- Each `Stop` of a module, middleware module or plugin is waited for until its context has ended and 250ms more. A `Stop` that honours its context returns in that time and is reported as it is.
- The `Stop`s that are only called after the context has ended (a module before them used up the time, or `Stop` was given a cancelled context) share one more second, so a `Stop` that ignores its context but finishes quickly still runs to its end, in order. Once that second is used up, each has 250ms.
- One that has not returned by then is abandoned: it keeps running in its goroutine, the shutdown goes on with the other modules and then NATS, and `Stop` returns an error wrapping `monoerrors.ErrStopAbandoned` that names it. Its eventual return is logged at Warn.
- The abandoned `Stop` runs on after NATS has stopped, so whatever it still does with the event bus fails. Make `Stop` return when its context ends.

**Calls that overlap** (since v0.2.17; before, each of them ran, see the [upgrade guide](../guides/upgrading.md#start-and-stop-are-guarded-against-each-other-v0217)):

- **`Stop` while `Start` is running** (a termination signal during a slow start) aborts the start: the context the modules are started with is cancelled, `Start` stops what it had started and returns an error wrapping `context.Canceled`, and only then is NATS stopped. This holds also when the modules ignored the cancellation and all of them started. `Stop` waits for `Start` within its own context and two seconds beyond it; a start that has not returned by then is left behind, and `Stop` stops NATS and returns an error saying that the start did not return.
- **`Stop` while another `Stop` is running** waits for it and returns its result. It does not stop NATS under the handlers the first one is waiting for.
- **`Stop` after `Stop` has finished** returns `application already stopped`.

Do not call `Stop` from a module's own `Start` or `Stop`: the call would wait for itself until its context ends.

### Module Management

#### Register

```go
func (app MonoApplication) Register(module Module) error
```

Registers a regular module with the framework. Must be called before `Start()`. The module can be any type that implements the `Module` interface. Returns an error for a duplicate name or an invalid module.

**Example:**
```go
app.Register(&OrderModule{})
app.Register(&PaymentModule{})
```

#### RegisterPlugin

```go
func (app MonoApplication) RegisterPlugin(plugin PluginModule, alias string) error
```

Registers a plugin module under `alias`, a unique identifier for this plugin instance (it can differ from the plugin name). Plugins start before middleware and regular modules, and stop last.

**Example:**
```go
storage, _ := fsjetstream.New(config)
app.RegisterPlugin(storage, "storage")
```

#### Plugin

```go
func (app MonoApplication) Plugin(alias string) PluginModule
```

Returns the plugin registered under `alias`, or nil if there is none.

#### Modules

```go
func (app MonoApplication) Modules() []string
```

Returns the names of the registered modules (not including plugins), in no particular order.

### Service Discovery

#### Services

```go
func (app MonoApplication) Services(moduleName string) ServiceContainer
```

Returns the `ServiceContainer` of the module named `moduleName` (from `Module.Name()`), giving access to its registered services. Returns nil before `Start`, after `Stop`, and for an unknown or empty name. It is a host-level escape hatch (main.go, API handlers, tests), not for module code.

**Example:**
```go
client, err := app.Services("payment").GetRequestReplyService("process-payment")
if err != nil {
    return err
}
reply, err := client.Call(ctx, payload) // payload is []byte
```

See also [Accessing Services](#accessing-services).

#### EventBus

```go
func (app MonoApplication) EventBus(moduleName string) EventBus
```

Returns the `EventBus` of a module: for a started module, the bus scoped to its declared boundaries; otherwise (unknown name, before `Start`) the shared, unscoped bus.

### Health and Status

#### Health

```go
func (app MonoApplication) Health(ctx context.Context) FrameworkHealth
```

Returns the aggregated health status of the framework and all modules. `FrameworkHealth` contains:

| Field | Description |
|-------|-------------|
| `Healthy` | Overall framework health (true only if framework + NATS + all modules healthy) |
| `State` | Current framework state |
| `NATSHealthy` | NATS server health |
| `JetStream` | JetStream state of this server (`*types.JetStreamHealth`, see below); nil when JetStream is not enabled or the framework is not running |
| `Modules` | Map of individual module health statuses |
| `Timestamp` | When the check was performed |
| `Message` | Error message if unhealthy |

`types.JetStreamHealth` (package `github.com/go-monolith/mono/pkg/types`) is read locally from the embedded server, without a network request:

| Field | Description |
|-------|-------------|
| `Available` | JetStream can serve requests from this server: it is enabled and, in a cluster, the meta group has a leader |
| `Clustered` | JetStream runs in clustered mode |
| `MetaLeader` | Server name of the meta group leader in a cluster; empty while there is none (no quorum) |
| `ClusterSize` | Number of JetStream servers the meta group knows |
| `Message` | Why JetStream is not available; empty otherwise |

`JetStream` is **not** part of `Healthy`. In a cluster, JetStream is unavailable on every server while the cluster has no quorum, so counting it would mark the surviving servers unhealthy (and have them restarted) when one server is lost. A readiness check that must not route JetStream-dependent traffic to this server can check `JetStream.Available` itself (see [Startup, Readiness and Health](../guides/clustering.md#startup-readiness-and-health)).

**Example:**
```go
health := app.Health(ctx)
fmt.Printf("Framework healthy: %v\n", health.Healthy)
fmt.Printf("NATS healthy: %v\n", health.NATSHealthy)
if health.JetStream != nil {
    fmt.Printf("JetStream available: %v (leader %q)\n", health.JetStream.Available, health.JetStream.MetaLeader)
}
for name, moduleHealth := range health.Modules {
    fmt.Printf("%s: %v - %s\n", name, moduleHealth.Healthy, moduleHealth.Message)
}
```

#### SubscriptionStats

```go
func (app MonoApplication) SubscriptionStats() []SubscriptionStats
```

Returns one snapshot per framework-managed NATS subscription: request-reply service, queue group and event consumer. JetStream-based stream, event stream and cron consumers are not included. It returns nil unless the application is running. Export it as metrics, or enable [`WithSubscriptionStatsLogInterval`](#withsubscriptionstatsloginterval) for a periodic Debug log.

| Group | Fields |
|-------|--------|
| Identity | `Kind`, `Module`, `Name`, `Subject`, `QueueGroup` |
| NATS client buffer | `PendingMsgs`, `PendingBytes`, `Dropped`, `Delivered` |
| Worker pool | `PoolActive`, `PoolQueued`, `MaxConcurrency`, `MaxPending` |
| Framework counters | `Rejected`, `Expired`, `DroppedAtShutdown` |

#### ModuleBoundaries

```go
func (app MonoApplication) ModuleBoundaries() []types.ModuleBoundaryReport
```

Returns one report per started module, sorted by name. It returns nil unless the application is running. The same summary is logged at `Stop`. With [`WithStrictModuleBoundaries`](#withstrictmoduleboundaries) the crossings fail instead of being recorded. `ModuleBoundaryReport` has no alias in the root package: import `github.com/go-monolith/mono/pkg/types`.

| Field | Content |
|-------|---------|
| `Declared` | The module's `Dependencies()` |
| `Consumed` | Its `ConsumeServices()` contracts |
| `Observed` | The dependency services it looked up, or for a consumed contract reached on its event bus, with counts |
| `UnusedDependencies` | Declared but never looked up |
| `UnusedContracts` | Consumed contracts not used so far |
| `Violations` | Boundary crossings (`register-on-dependency`, `channel-consumer-mismatch`, `undeclared-service-subject`, `foreign-service-subscription`, `undeclared-service-use`), each with a count and the time first seen |

`Observed`, `UnusedDependencies`, `UnusedContracts` and `Violations` are empty lists, never nil, when there is nothing to list, so their JSON keys (`observed`, `unused_dependencies`, `unused_contracts`, `violations`) are always present.

#### Logger

```go
func (app MonoApplication) Logger() Logger
```

Returns the framework's internal logger instance.

## Configuration Options

All framework configuration is done through functional options passed to `NewMonoApplication()`. Every option below has a signature, a one-line description, a table (type, default, constraints) and an example; longer details follow the example.

### Options Index

| Option | What it does | Default |
|--------|--------------|---------|
| **NATS** | | |
| [`WithNATSPort`](#withnatsport) | Client listening port | `4222` |
| [`WithNATSHost`](#withnatshost) | Client listening host | `127.0.0.1` |
| [`WithNATSDontListen`](#withnatsdontlisten) | No TCP listener | `false` |
| [`WithNATSInProcessConn`](#withnatsinprocessconn) | In-process client connection | `false` |
| [`WithNATSLogging`](#withnatslogging) | NATS server log flags | all off |
| [`WithNATSMaxPayload`](#withnatsmaxpayload) | Maximum message payload | nats-server default (1 MB) |
| [`WithStartupReadyTimeout`](#withstartupreadytimeout) | Wait for the embedded server to accept connections | `10s` |
| [`WithNATSClustering`](#withnatsclustering) | Join a NATS cluster | off |
| [`WithNATSServerName`](#withnatsservername) | Server name (required for clustered JetStream) | config file or nats-server default |
| [`WithNATSClusterAuth`](#withnatsclusterauth) | Route credentials | routes not authenticated |
| [`WithNATSClusterTLS`](#withnatsclustertls) | Mutual TLS on the routes | routes without TLS |
| [`WithNATSSystemAccount`](#withnatssystemaccount) | System account user for operator tooling | none |
| [`WithNATSClientName`](#withnatsclientname) | Name of the framework's client connection | `"mono"` |
| [`WithNATSClientOptions`](#withnatsclientoptions) | Extra nats.go client options | none |
| [`WithNATSConfigFile`](#withnatsconfigfile) | NATS server config file | none |
| [`WithNATSClientTLS`](#withnatsclienttls) | TLS (optionally mutual) on the client listener, certificate from files | disabled |
| [`WithNATSAutoTLS`](#withnatsautotls) | Let's Encrypt certificates for the client listener | disabled |
| **JetStream** | | |
| [`WithJetStreamDomain`](#withjetstreamdomain) | JetStream domain | empty |
| [`WithJetStreamStorageDir`](#withjetstreamstoragedir) | Enables JetStream with file storage | JetStream disabled |
| [`WithJetStreamReadyTimeout`](#withjetstreamreadytimeout) | How long `Start` waits for JetStream | 2 minutes |
| [`WithJetStreamDefaultReplicas`](#withjetstreamdefaultreplicas) | Replicas of framework-created streams | one replica |
| **Logger** | | |
| [`WithLogLevel`](#withloglevel) | Log level | `LogLevelInfo` |
| [`WithLogFormat`](#withlogformat) | Text or JSON | `LogFormatText` |
| [`WithLogOutput`](#withlogoutput) | Log destination | `os.Stdout` |
| [`WithLogSource`](#withlogsource) | Source file and line in log entries | `false` |
| [`WithLogger` / `WithCustomLogger`](#withlogger--withcustomlogger) | Custom logger instance | auto-created |
| **Framework** | | |
| [`WithShutdownTimeout`](#withshutdowntimeout) | Bound of `Stop` | `30s` |
| [`WithShutdownDrainTimeout`](#withshutdowndraintimeout) | Wait for draining and in-flight handlers | `min(30s, shutdown timeout)` |
| [`WithQueueGroupOptimisticWindow`](#withqueuegroupoptimisticwindow) | Fire-and-forget queue group sends after a first ACK | `0` (disabled) |
| [`WithServiceDiscovery`](#withservicediscovery) | NATS services protocol (`nats micro`) | disabled |
| [`WithStrictModuleBoundaries`](#withstrictmoduleboundaries) | Boundary violations fail instead of being recorded | off |
| [`WithSubscriptionStatsLogInterval`](#withsubscriptionstatsloginterval) | Periodic Debug log of `SubscriptionStats` | `0` (disabled) |
| [`WithDeadlineSkewAllowance`](#withdeadlineskewallowance) | Clock skew tolerated for caller deadlines | `250ms` |
| [`WithDefaultServiceConcurrency`](#withdefaultserviceconcurrency) | Handlers running at once per request-reply service | `runtime.GOMAXPROCS(0) * 4` |

### NATS Configuration

#### WithNATSPort

```go
func WithNATSPort(port int) MonoFrameworkOption
```

Sets the NATS server listening port.

| Property | Value |
|----------|-------|
| Type | `int` |
| Default | `4222` |
| Constraints | 1024-65535 |

**Example:**
```go
mono.WithNATSPort(4222)
```

#### WithNATSHost

```go
func WithNATSHost(host string) MonoFrameworkOption
```

Sets the NATS server host address. An IPv6 address is given without brackets (`"::1"`); the framework adds them where a URL needs them (since v0.2.19; before, the framework's own client failed with "too many colons in address").

| Property | Value |
|----------|-------|
| Type | `string` |
| Default | `127.0.0.1` |
| Constraints | Non-empty |

**Example:**
```go
mono.WithNATSHost("0.0.0.0")
```

#### WithNATSDontListen

```go
func WithNATSDontListen() MonoFrameworkOption
```

Prevents the NATS server from listening on TCP. Useful for embedded servers.

| Property | Value |
|----------|-------|
| Type | none (flag) |
| Default | `false` |
| Constraints | Requires `WithNATSInProcessConn()`, otherwise `NewMonoApplication` fails; cannot be combined with [`WithNATSAutoTLS`](#withnatsautotls) or [`WithNATSClientTLS`](#withnatsclienttls) |

**Example:**
```go
mono.WithNATSDontListen()
mono.WithNATSInProcessConn()
```

#### WithNATSInProcessConn

```go
func WithNATSInProcessConn() MonoFrameworkOption
```

Enables in-process client connections using `net.Pipe()` instead of TCP. Can be used alone or with `WithNATSDontListen()`.

| Property | Value |
|----------|-------|
| Type | none (flag) |
| Default | `false` |

**Example:**
```go
mono.WithNATSInProcessConn()
```

#### WithNATSLogging

```go
func WithNATSLogging(debug, trace, sysTrace bool) MonoFrameworkOption
```

Configures NATS server logging flags.

| Parameter | Description |
|-----------|-------------|
| `debug` | Enable debug-level logging |
| `trace` | Enable trace-level logging |
| `sysTrace` | Enable system trace logging |

**Example:**
```go
mono.WithNATSLogging(true, false, false)  // Debug only
```

#### WithNATSMaxPayload

```go
func WithNATSMaxPayload(bytes int32) MonoFrameworkOption
```

Sets the maximum message payload size.

| Property | Value |
|----------|-------|
| Type | `int32` |
| Default | (NATS default: 1MB) |
| Constraints | 1KB-8MB |

**Example:**
```go
mono.WithNATSMaxPayload(2 * 1024 * 1024)  // 2 MB
```

#### WithStartupReadyTimeout

```go
func WithStartupReadyTimeout(timeout time.Duration) MonoFrameworkOption
```

Sets how long `NewMonoApplication` waits for the embedded NATS server to accept connections.

| Property | Value |
|----------|-------|
| Type | `time.Duration` |
| Default | `10 * time.Second` |
| Constraints | 3s to 60s |

**Example:**
```go
mono.WithStartupReadyTimeout(15 * time.Second)
```

If the server is not ready in time (for example because another process holds the client port), `NewMonoApplication` fails with `NATS server not ready after <timeout> timeout`.

#### WithNATSClustering

```go
func WithNATSClustering(clusterName, clusterHost string, clusterPort int, routes []string) MonoFrameworkOption
```

Enables NATS clustering for distributed deployments.

| Parameter | Description |
|-----------|-------------|
| `clusterName` | Name of the cluster (required) |
| `clusterHost` | Host for cluster communication |
| `clusterPort` | Port for cluster communication (1024-65535) |
| `routes` | URLs of the cluster's servers, of the form `nats://[user:password@]host:port` |

**Example (without JetStream, seed node):**
```go
mono.WithNATSClustering("production", "127.0.0.1", 6222, nil)
```

**Example (without JetStream, non-seed node):**
```go
mono.WithNATSClustering("production", "127.0.0.1", 6223,
    []string{"nats://127.0.0.1:6222"})
```

**Example (with JetStream, three servers, same on each):**
```go
routes := []string{
    "nats://app-0.app.default.svc.cluster.local:6222",
    "nats://app-1.app.default.svc.cluster.local:6222",
    "nats://app-2.app.default.svc.cluster.local:6222",
}
mono.NewMonoApplication(
    mono.WithNATSServerName(os.Getenv("POD_NAME")),
    mono.WithNATSClustering("production", "0.0.0.0", 6222, routes),
    mono.WithJetStreamStorageDir("/data/jetstream"),
)
```

**Routes:**
- **Without JetStream**, a seed node (the first node in a cluster) may have no routes, and the other nodes point to the seed node's cluster URL.
- **With JetStream**, every server must list the same routes: all servers of the cluster, its own included (a server ignores the route to itself). Every server must also set a server name with [`WithNATSServerName`](#withnatsservername).
- `NewMonoApplication` returns an error when clustering and JetStream are configured without a server name or without a route. With a config file, these checks run on the merged configuration instead, when `NewMonoApplication` starts the embedded server, since the file may provide `server_name`, `cluster.name` and `routes` (or leafnode remotes).

**Route URLs** must have the form `nats://[user:password@]host:port`. A path, query, fragment or opaque part is rejected, which also catches an unencoded `/`, `?` or `#` in a password: percent-encode special characters in credentials. Errors name a route by its index and never quote it. [`WithNATSClusterAuth`](#withnatsclusterauth) adds credentials to every route that has none.

**With a config file** ([`WithNATSConfigFile`](#withnatsconfigfile)), the values are merged into the file's `cluster{}` block: name, host and port come from this option, its routes are added to the file's `cluster.routes` (the server uses both lists), and the file's other cluster settings (authorization, TLS, permissions, pool size, compression) apply. Before v0.2.0 the option replaced the whole block and those settings were silently dropped.

In Kubernetes, use the fully qualified names of a StatefulSet's pods behind a headless Service (`<pod>.<service>.<namespace>.svc.cluster.local`). See the [Clustering and Kubernetes guide](../guides/clustering.md).

#### WithNATSServerName

```go
func WithNATSServerName(name string) MonoFrameworkOption
```

Sets the name of the embedded NATS server (`server_name`).

| Property | Value |
|----------|-------|
| Type | `string` |
| Default | (empty: the config file's `server_name`, otherwise nats-server's default, the server ID) |
| Constraints | Non-empty, no whitespace; equal to the config file's `server_name` when the file sets one |

**Example:**
```go
mono.WithNATSServerName(os.Getenv("POD_NAME"))
```

When a NATS config file also sets `server_name`, the two must be equal: otherwise `NewMonoApplication` fails rather than silently renaming the server (a renamed JetStream server becomes a new peer and loses its local replicas).

Clustered JetStream requires a server name, and it must be:

- **unique** within the cluster (and across a supercluster or leafnodes that share JetStream). JetStream identifies every RAFT peer, of the meta group and of each stream and consumer, by server name. nats-server closes a route from a server that reports its own name ("Remote server has a duplicate name").
- **stable** across restarts. A server's stored JetStream state belongs to its name, so a restarted server must come back under the same name.

The framework only checks that the name is set and valid. Uniqueness and stability come from the deployment: a Kubernetes StatefulSet pod name (`my-app-0`, `my-app-1`, ...) is both; a Deployment's generated pod name is unique but changes on every restart; a random value is neither.

#### WithNATSClusterAuth

```go
func WithNATSClusterAuth(username, password string) MonoFrameworkOption
```

Requires the given credentials from every cluster route, and presents them on the routes this server opens.

| Property | Value |
|----------|-------|
| Type | `string`, `string` |
| Default | (empty: routes are not authenticated unless the config file says otherwise) |
| Constraints | Both non-empty; requires clustering (from `WithNATSClustering` or a config file) |

**Example:**
```go
mono.WithNATSClustering("orders", "0.0.0.0", 6222, routes)
mono.WithNATSClusterAuth("route", os.Getenv("NATS_ROUTE_PASSWORD"))
```

- nats-server only sends the credentials written in a route's URL, so the framework adds them to every route URL that has none, from `WithNATSClustering` and from a config file alike.
- They replace the user and password of a config file's `cluster.authorization` block.
- The credentials are redacted from the framework's logs and errors. Load the password from a secret, never from code.
- The credentials are only safe on a trusted network unless the routes also use TLS: see [`WithNATSClusterTLS`](#withnatsclustertls).

#### WithNATSClusterTLS

```go
func WithNATSClusterTLS(certFile, keyFile, caFile string) MonoFrameworkOption
```

Makes the routes between the servers of a cluster use mutual TLS: each server presents its certificate, requires one from every peer, and accepts only peers whose certificate is signed by the CA in `caFile`.

| Property | Value |
|----------|-------|
| Type | `string`, `string`, `string` (PEM files: certificate, key, CA) |
| Default | (empty: routes use no TLS unless the config file has a `cluster { tls {} }` block) |
| Constraints | None empty; requires clustering; not combined with a `cluster { tls {} }` block in a NATS config file |
| Error | Unreadable or invalid files, or a certificate not signed by the CA in `caFile` for both TLS server and client use, make `NewMonoApplication` fail when the embedded server starts |

**Example:**
```go
mono.WithNATSClustering("orders", "0.0.0.0", 6222, routes)
mono.WithNATSClusterTLS("/etc/nats-route-tls/tls.crt", "/etc/nats-route-tls/tls.key", "/etc/nats-route-tls/ca.crt")
```

- **Certificate names.** On a route it opens, a server also checks that the peer's certificate matches the route URL's host. The certificate needs a DNS name (or IP address) for every route host, for example `*.<headless-service>.<namespace>.svc.cluster.local` in Kubernetes. Each server is both TLS server and client on routes, so the certificate must allow both uses (extended key usages `serverAuth` and `clientAuth`, or none).
- **Renewal without a restart.** The certificate and key are read again when the files change, checked at most every 10 seconds on the next route handshake. A renewed certificate (a Kubernetes Secret updated by cert-manager, for example) is used for new routes, and routes already established keep their session. The framework logs `Reloaded the cluster TLS certificate`. A file that cannot be loaded (for example half-written), or a certificate that is not signed by the route CA, keeps the previous certificate and is retried; a certificate from another CA would otherwise break every route that reconnects. A change of CA needs a restart.
- **The reloader is public.** [`pkg/certreload`](https://pkg.go.dev/github.com/go-monolith/mono/pkg/certreload) is the same reloader, for an application's own TLS servers and clients: `certreload.New(certreload.Config{CertFile, KeyFile, Roots, Usages})`, then `GetCertificate` or `GetClientCertificate` in a `tls.Config`.
- **Expiry monitoring.** nats-server's `/varz` cannot report the expiry of a certificate served this way (`cluster.tls_cert_not_after` is absent). The framework logs it instead: `Cluster TLS enabled` at start and `Reloaded the cluster TLS certificate` on renewal, both with `not_after`. A certificate that passes its expiry without being renewed is warned about, at most once an hour (since v0.2.19).
- **With route credentials.** It combines with [`WithNATSClusterAuth`](#withnatsclusterauth): TLS encrypts the routes and authenticates peers by certificate, and the password is checked on top.
- **All servers must agree.** A server with route TLS and one without cannot route to each other, so turning it on (or off) for a running cluster takes a restart of every server at once, or a blue-green cutover.
- The client listener is not affected; see [`WithNATSClientTLS`](#withnatsclienttls) and [`WithNATSAutoTLS`](#withnatsautotls). The startup log line `NATS server started` carries `cluster_tls=true`.

#### WithNATSSystemAccount

```go
func WithNATSSystemAccount(username, password string) MonoFrameworkOption
```

Adds a user of the NATS system account (`$SYS`) to the embedded server, for operator tooling such as `nats server report jetstream` and `nats server raft peer-remove`.

| Property | Value |
|----------|-------|
| Type | `string`, `string` |
| Default | (empty: no system account user) |
| Constraints | Both non-empty; no accounts, `system_account`, `no_sys_acc`, client authorization or operator in the config file |

**Example:**
```go
mono.WithNATSSystemAccount("admin", os.Getenv("NATS_SYSTEM_PASSWORD"))
```

```bash
kubectl port-forward pod/orders-0 4222:4222 &
nats -s nats://127.0.0.1:4222 --user admin --password "$NATS_SYSTEM_PASSWORD" server report jetstream
```

- `nats server raft peer-remove` removes a server from the JetStream meta group, which retiring servers for good requires, for example at the end of a [blue-green cutover](../extra/kubernetes-deployment.md#joined-cluster).
- Clients without credentials keep working, in the global account where the application's streams and consumers live: nats-server requires a password only from the system account user as long as no other accounts and no client authorization are configured. Enabling it on a running cluster one server at a time is safe.
- A client that sends credentials (a URL with user info, a token, `nats.UserInfo` in `WithNATSClientOptions`) is rejected unless they are the system account user's: remove leftover credentials from client URLs.
- The user can connect wherever the client listener is reachable, so keep that listener private (`WithNATSHost("127.0.0.1")`, a port-forward for operators). The password is not logged.
- A NATS config file that configures `accounts`, `system_account`, `no_sys_acc`, an `authorization` block (even an empty one) or an operator is rejected when the embedded server starts, so `NewMonoApplication` returns the error: the framework's own client would need credentials. Add the system account user to that file instead.

See [Operator Access (System Account)](../guides/clustering.md#operator-access-system-account) in the clustering guide.

#### WithNATSClientName

```go
func WithNATSClientName(name string) MonoFrameworkOption
```

Sets the name the framework's client connection reports to the server (`nats.Name`), shown in `/connz` and server logs.

| Property | Value |
|----------|-------|
| Type | `string` |
| Default | `"mono"` |
| Constraints | Non-empty |

**Example:**
```go
mono.WithNATSClientName("orders-api")
```

#### WithNATSClientOptions

```go
func WithNATSClientOptions(opts ...nats.Option) MonoFrameworkOption
```

Adds nats.go options to the framework's client connection.

| Property | Value |
|----------|-------|
| Type | `...nats.Option` (`github.com/nats-io/nats.go`) |
| Default | (none) |
| Constraints | No nil option; repeated calls append |

**Example:**
```go
mono.WithNATSClientOptions(
    nats.ReconnectWait(2*time.Second),
    nats.PingInterval(20*time.Second),
)
```

The framework applies its defaults first (connection name; an error handler that logs slow consumers and other client errors with subject and queue group; disconnect, reconnect and closed handlers; a drain timeout), then these, so they can add to or override the defaults.

#### WithNATSConfigFile

```go
func WithNATSConfigFile(path string) MonoFrameworkOption
```

Sets the path to a NATS server configuration file, processed with NATS `server.ProcessConfigFile()` when `NewMonoApplication` starts the embedded server.

| Property | Value |
|----------|-------|
| Type | `string` |
| Default | (empty, disabled) |
| Constraints | Non-empty path |

**Example (Config file only):**
```go
mono.NewMonoApplication(
    mono.WithNATSConfigFile("/etc/nats/server.conf"),
)
```

**Example (Config file with programmatic overrides):**
```go
mono.NewMonoApplication(
    mono.WithNATSConfigFile("/etc/nats/server.conf"),
    mono.WithNATSPort(4333), // Overrides port from config file
)
```

- The config file provides base settings and programmatic options override them, so a standard NATS config file can be used while customizing specific settings.
- `WithNATSHost`, `WithNATSPort` and `WithNATSMaxPayload` override the file whenever they are used, also with a value that equals the framework default (`127.0.0.1`, `4222`, 1 MB). Up to v0.2.16 a value equal to the default was dropped: `WithNATSPort(4222)` did not override a `port` set in the file, and `WithNATSHost("127.0.0.1")` did not keep the listener on loopback.
- **Listen address:** without a config file the client listener binds `127.0.0.1`. With a config file that sets no `host` (or `listen`), and without `WithNATSHost`, nats-server's own default applies and the listener accepts connections on every interface, without authentication unless the file configures it. This is logged at Warn when the server starts, and the `NATS server started` line reports the address the server is bound to. Use `WithNATSHost("127.0.0.1")`, or set the host in the file, to keep the client port private.
- File existence and validity are checked when `NewMonoApplication` starts the embedded server, not when the option is applied.

For NATS configuration file format and options, see the [NATS Server Configuration documentation](https://docs.nats.io/running-a-nats-service/configuration).

#### WithNATSClientTLS

```go
func WithNATSClientTLS(clientTLS types.ClientTLSConfig) MonoFrameworkOption
```

Makes the embedded NATS server's **client listener** require TLS with a certificate from files, and optionally a certificate from every client (mutual TLS). For clients on a private network, such as other services in the same Kubernetes cluster, with a private CA. For public host names, see [`WithNATSAutoTLS`](#withnatsautotls).

| Property | Value |
|----------|-------|
| Type | `types.ClientTLSConfig` |
| Default | (disabled: the client listener uses no TLS unless the config file has a `tls {}` block) |
| Constraints | `CertFile` and `KeyFile` required; not combined with [`WithNATSAutoTLS`](#withnatsautotls), [`WithNATSDontListen`](#withnatsdontlisten), or a NATS config file with a top-level `tls {}` block, `allow_non_tls` or `ocsp`; `ClientCAFile` must not share a CA with the routes |
| Error | Unreadable or invalid files, or a certificate that has expired or does not allow TLS server use, make `NewMonoApplication` fail when the embedded server starts |

**Example:**
```go
mono.NewMonoApplication(
    mono.WithNATSHost("0.0.0.0"),
    mono.WithNATSClientTLS(types.ClientTLSConfig{
        CertFile:     "/etc/nats-client-tls/tls.crt",
        KeyFile:      "/etc/nats-client-tls/tls.key",
        ClientCAFile: "/etc/nats-client-tls/ca.crt", // omit for TLS without client certificates
    }),
)
```

`types.ClientTLSConfig` (package `github.com/go-monolith/mono/pkg/types`, no root alias):

| Field | Description |
|-------|-------------|
| `CertFile`, `KeyFile` | Required. PEM certificate (intermediates after the leaf) and key. The certificate must allow TLS server use and name the host clients dial |
| `ClientCAFile` | Optional. PEM CA bundle client certificates must chain to; set means every client must present one (mutual TLS) |

- **Renewal without a restart.** Like route TLS, the certificate is served through [`pkg/certreload`](https://pkg.go.dev/github.com/go-monolith/mono/pkg/certreload). The files are read again at most every 10 seconds, on the next connection, and reloaded when their contents changed. Connections already established keep their session. The framework logs `Client TLS enabled` at start and `Reloaded the client TLS certificate` on renewal, both with `not_after`. A file that cannot be loaded, or an expired certificate, keeps the previous one and is retried.
- **The client CA is read once.** To replace it, add the new CA to the file next to the old one, restart the servers (a rolling restart is enough: clients only need either CA), then move the clients to certificates from the new CA, and finally remove the old CA.
- **A separate CA from the routes.** Every server accepts any certificate from the route CA as a cluster peer, so a client certificate from that CA would be good for joining the cluster. `NewMonoApplication` fails when a CA in `ClientCAFile` is, or is signed by, the route CA (of `WithNATSClusterTLS` or a config file's `cluster { tls {} }` block), or the route CA is signed by a client CA.
- **Without `ClientCAFile`** the listener encrypts but does not authenticate: any client that can reach the port connects without credentials and gets the global account, as without TLS. Set `ClientCAFile`, or define users in a NATS config file, to decide who may connect.
- **Authentication, not authorization.** A client certificate proves who may connect, but every client with a valid certificate gets the permissions of the account it connects to: the global account unless a NATS config file defines users, which includes JetStream and the service discovery subjects (`$SRV.>`). Restrict who can reach the port (a Kubernetes NetworkPolicy, for example) and who gets a client certificate.
- **Clients.** Clients connect with `tls://` and the host the certificate names. With client certificates, use `helper.NATSClientTLS` (below) so a renewed client certificate is presented on the next reconnect; `nats.ClientCert` reads its files only once. When a client first connected by host name and reconnects to a server of the cluster it discovered by IP address, nats.go verifies that server against the host name, so a certificate for a Kubernetes Service name also covers the pods behind it.
- **The framework's own client** is switched to an in-process connection, which does not use TLS. Plaintext TCP clients are refused.
- Routes, gateways, leafnodes, websocket and MQTT listeners are not affected. The startup log line `NATS server started` carries `client_tls=true`.

##### helper.NATSClientTLS

```go
func NATSClientTLS(cfg helper.NATSClientTLSConfig) (nats.Option, error)
```

The client side of `WithNATSClientTLS`, for nats.go clients (package `github.com/go-monolith/mono/pkg/helper`). It returns an option that verifies the server against `CAFile` and, with `CertFile` and `KeyFile`, presents a client certificate that is reloaded when its files change.

| Field | Description |
|-------|-------------|
| `CAFile` | Required. PEM CA bundle the server certificate must chain to; read once |
| `CertFile`, `KeyFile` | Both or neither. The client certificate (TLS client use) and key |
| `Logger` | Receives `Reloaded the NATS client TLS certificate` lines; nil means no logging |
| `CheckInterval` | Least time between two checks of the files; 0 = 10 seconds |

```go
tlsOpt, err := helper.NATSClientTLS(helper.NATSClientTLSConfig{
    CAFile:   "/etc/nats-client-tls/ca.crt",
    CertFile: "/etc/nats-client-tls/tls.crt",
    KeyFile:  "/etc/nats-client-tls/tls.key",
})
if err != nil {
    return err
}
nc, err := nats.Connect("tls://mono-cluster-client.mono-demo.svc.cluster.local:4222", tlsOpt)
```

#### WithNATSAutoTLS

```go
func WithNATSAutoTLS(autoTLS types.AutoTLSConfig) MonoFrameworkOption
```

Enables automatic ACME (Let's Encrypt) TLS certificates for the embedded NATS server's **client listener**.

| Property | Value |
|----------|-------|
| Type | `types.AutoTLSConfig` |
| Default | (disabled) |
| Constraints | The fields below (checked when the option is applied); not combined with `WithNATSDontListen` or [`WithNATSClientTLS`](#withnatsclienttls); not combined with a config file that has a `tls{}` block or sets `allow_non_tls` (which would let plaintext clients in; rejected since v0.2.19) |

**Example:**
```go
mono.NewMonoApplication(
    mono.WithNATSHost("0.0.0.0"),
    mono.WithNATSAutoTLS(types.AutoTLSConfig{
        Domains:   []string{"nats.example.com"},
        Email:     "ops@example.com",
        CacheDir:  "/var/lib/mono/acme",
        AcceptTOS: true,
    }),
)
```

`types.AutoTLSConfig` (package `github.com/go-monolith/mono/pkg/types`, no root alias):

| Field | Description |
|-------|-------------|
| `Domains` | Required. Fully qualified, lowercase host names, no wildcards, no duplicates, no URL or port |
| `Email` | ACME account contact (optional, recommended) |
| `CacheDir` | Required. Persists the ACME account key and certificates; must survive restarts |
| `HTTPChallengeAddr` | Listen address of the challenge server; default `:80` |
| `DirectoryURL` | ACME directory; empty = Let's Encrypt production |
| `DirectoryCAPool` | Root pool for a private ACME CA's directory endpoint; nil = system pool |
| `RenewBefore` | How long before expiry to renew; 0 = 30 days; must not be negative |
| `StartupIssueTimeout` | Bound of the certificate fetch at startup; 0 = 60s; negative skips it |
| `AcceptTOS` | Must be `true` |

- Cluster routes, gateways, leafnodes, websocket and MQTT listeners are not affected.
- The framework starts an HTTP listener that answers ACME http-01 challenges for the whole life of the application (renewals re-run the challenge). Certificates are renewed in the background, without a restart or reload. Only the http-01 challenge is supported.
- TLS becomes mandatory for external clients: plaintext TCP connections are rejected, so enabling it on a deployment with plaintext clients breaks them.
- Clients connect with `tls://` and one of the configured domain names (the certificate is selected from the TLS SNI name, so connecting by IP fails).
- The framework's own client is switched to an in-process connection.

See the [AutoTLS example](../../../examples/auto-tls/README.md).

### JetStream Configuration

#### WithJetStreamDomain

```go
func WithJetStreamDomain(domain string) MonoFrameworkOption
```

Sets the JetStream domain for multi-tenancy. It does not enable JetStream: combine it with `WithJetStreamStorageDir` or a `jetstream{}` block in a NATS config file. It overrides the file's `domain`.

| Property | Value |
|----------|-------|
| Type | `string` |
| Default | (empty) |
| Constraints | Non-empty |

**Example:**
```go
mono.WithJetStreamDomain("production")
```

#### WithJetStreamStorageDir

```go
func WithJetStreamStorageDir(dir string) MonoFrameworkOption
```

Enables JetStream with persistent file storage at the specified directory. JetStream is enabled only by this option (or a `jetstream{}` block in a NATS config file). JetStream enabled by the file is treated the same way, with the file's `store_dir` and `domain`; this option's directory overrides `store_dir`.

| Property | Value |
|----------|-------|
| Type | `string` |
| Default | (JetStream disabled) |
| Constraints | Non-empty path |

**Example:**
```go
mono.WithJetStreamStorageDir("./data/jetstream")
```

#### WithJetStreamReadyTimeout

```go
func WithJetStreamReadyTimeout(d time.Duration) MonoFrameworkOption
```

Bounds how long `Start` waits for JetStream and retries its JetStream setup steps.

| Property | Value |
|----------|-------|
| Type | `time.Duration` |
| Default | 2 minutes |
| Constraints | > 0 |

**Example:**
```go
mono.WithJetStreamReadyTimeout(5 * time.Minute)
```

- **Wait**: before it starts plugins and modules, `Start` waits until JetStream answers. When JetStream is not enabled, `Start` does not wait. On a single server JetStream answers at once and nothing is retried.
- **Retry**: each JetStream setup step (the stream, consumer and schedule setup of stream consumer, cron and event stream consumer services, and the planning of event stream durables) is retried while it fails with an error that can clear by itself: a timeout of one attempt, no responders, no stream response, or JetStream API errors 10004 (incomplete results), 10005 (no suitable peers for placement), 10008 (JetStream system temporarily unavailable), 10118 and 10119 (stream or consumer offline while its leader recovers). Not 10194/10195 ("offline" with a reason: an unsupported configuration or API level, or a decoding error), which do not clear by themselves. Any other error fails `Start` at once.
- **Bounds**: each attempt is bounded by 15s, and the wait between attempts grows from 250ms to 2s. The wait and each retried step are bounded by `d` (and by the `Start` context); a step still failing after `d` fails `Start` with an error that names `WithJetStreamReadyTimeout`.
- **Logs**: `Waiting for JetStream` at Info (at most every 10s per step), and `JetStream step succeeded after waiting` once the step succeeds.

Why a cluster needs the wait, and how it affects probes: see [Startup, Readiness and Health](../guides/clustering.md#startup-readiness-and-health).

#### WithJetStreamDefaultReplicas

```go
func WithJetStreamDefaultReplicas(n int) MonoFrameworkOption
```

Sets the number of replicas of the streams the framework creates for stream consumer services (`StreamConsumerConfig.Stream`), event stream consumers and cron services (`CronServiceConfig.Replicas`) whose configuration leaves `Replicas` at 0. Their consumers follow the stream unless their own `Replicas` is set.

| Property | Value |
|----------|-------|
| Type | `int` |
| Default | `0` (one replica) |
| Constraints | 1 to 5; above 1 requires clustering (`WithNATSClustering` or a config file) |

**Example:**
```go
mono.WithNATSClustering("orders", "0.0.0.0", 6222, routes)
mono.WithJetStreamDefaultReplicas(3)
```

Without it these streams have one replica, so in a cluster each lives on a single server and is unavailable while that server is down; a cron schedule stops firing. A replicated stream keeps working while fewer than half of its replicas are down, so 2 replicas tolerate no loss: use 3 (or 5) in a cluster of at least as many servers. Raising the value updates existing streams in place (nats-server adds the replicas).

### Logger Configuration

#### WithLogLevel

```go
func WithLogLevel(level LogLevel) MonoFrameworkOption
```

Sets the log level for the framework logger.

| Value | Description |
|-------|-------------|
| `LogLevelDebug` | Detailed diagnostic information |
| `LogLevelInfo` | General informational messages (default) |
| `LogLevelWarn` | Warning messages about potential problems |
| `LogLevelError` | Error messages about failures |

**Example:**
```go
mono.WithLogLevel(mono.LogLevelDebug)
```

#### WithLogFormat

```go
func WithLogFormat(format LogFormat) MonoFrameworkOption
```

Sets the log output format.

| Value | Description |
|-------|-------------|
| `LogFormatText` | Human-readable text format (default) |
| `LogFormatJSON` | Structured JSON format |

**Example:**
```go
mono.WithLogFormat(mono.LogFormatJSON)
```

#### WithLogOutput

```go
func WithLogOutput(w io.Writer) MonoFrameworkOption
```

Sets the output destination for logs.

| Property | Value |
|----------|-------|
| Type | `io.Writer` |
| Default | `os.Stdout` |
| Constraints | Non-nil |

**Example:**
```go
logFile, _ := os.OpenFile("app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
mono.WithLogOutput(logFile)
```

#### WithLogSource

```go
func WithLogSource(enable bool) MonoFrameworkOption
```

Enables source file name and line number in log entries.

| Property | Value |
|----------|-------|
| Type | `bool` |
| Default | `false` |

**Example:**
```go
mono.WithLogSource(true)
```

#### WithLogger / WithCustomLogger

```go
func WithLogger(logger Logger) MonoFrameworkOption
func WithCustomLogger(logger Logger) MonoFrameworkOption
```

Injects a custom logger instance. Overrides all logger factory options.

| Property | Value |
|----------|-------|
| Type | `Logger` |
| Default | (auto-created) |
| Constraints | Non-nil |

**Example:**
```go
customLogger := myLoggerFactory.NewLogger("app")
mono.WithCustomLogger(customLogger)
```

### Framework Options

#### WithShutdownTimeout

```go
func WithShutdownTimeout(timeout time.Duration) MonoFrameworkOption
```

Sets the maximum time to wait for graceful shutdown.

| Property | Value |
|----------|-------|
| Type | `time.Duration` |
| Default | `30 * time.Second` |
| Constraints | ≥ 1 second, ≥ shutdown drain timeout |

**Example:**
```go
mono.WithShutdownTimeout(60 * time.Second)
```

- It bounds the lifecycle part of `Stop` (drain, in-flight handlers, module and plugin `Stop`) when the context passed to `Stop` has no deadline of its own; a context with a deadline is used as is.
- The final NATS connection drain (capped at 5s) and server shutdown run after that bound.
- The drain keeps a reserve of the remaining time (5s, or a quarter of it when that is smaller) for module `Stop`.

#### WithShutdownDrainTimeout

```go
func WithShutdownDrainTimeout(timeout time.Duration) MonoFrameworkOption
```

Sets how long `Stop` waits for subscriptions to drain and in-flight handlers to complete before it cancels the runtime context and stops modules.

| Property | Value |
|----------|-------|
| Type | `time.Duration` |
| Default | `30 * time.Second` (nats.go `DefaultDrainTimeout`), capped at the shutdown timeout |
| Constraints | > 0, ≤ shutdown timeout when set explicitly (checked in `NewMonoApplication`) |

**Example:**
```go
mono.NewMonoApplication(
    mono.WithShutdownDrainTimeout(10 * time.Second),
    mono.WithShutdownTimeout(20 * time.Second),
)
```

- The wait starts after `Stop` has stopped taking new messages and covers worker pools and stream/cron fetch loops. Handlers keep a live context during this wait, so a request that started just before `Stop` completes normally.
- Whatever has not finished when the budget runs out is counted in a warning (`subscriptions_not_drained`, `pools_still_busy`, `fetch_loop_handlers_still_running`) and shutdown proceeds.
- Keep it below the orchestrator's termination grace period (Kubernetes defaults to 30s), with room for module `Stop`.

#### WithQueueGroupOptimisticWindow

```go
func WithQueueGroupOptimisticWindow(window time.Duration) MonoFrameworkOption
```

Enables optimistic publish mode for queue group services: the first send uses ACK mode, subsequent sends within the window use fire-and-forget.

| Property | Value |
|----------|-------|
| Type | `time.Duration` |
| Default | `0` (disabled) |
| Constraints | 0 or ≥ 100ms |

**Example:**
```go
mono.WithQueueGroupOptimisticWindow(1 * time.Second)
```

#### WithServiceDiscovery

```go
func WithServiceDiscovery(cfg ...ServiceDiscoveryConfig) MonoFrameworkOption
```

Exposes the application's request-reply services through the NATS services protocol, so `nats micro ls`, `nats micro info <module>` and `nats micro stats <module>` work against the application.

| Property | Value |
|----------|-------|
| Type | `...ServiceDiscoveryConfig` (only the first is used) |
| Default | (disabled) |
| Constraints | `Version` empty or a semantic version (`1.2.3`, prerelease and build suffixes allowed) |

**Example:**
```go
mono.WithServiceDiscovery(mono.ServiceDiscoveryConfig{Version: "1.4.2", Description: "orders API"})
```

Then, with the NATS CLI connected to the embedded server:

```
nats micro ls
nats micro info orders
nats micro stats orders
```

```go
type ServiceDiscoveryConfig struct {
    Version     string            // semantic version reported for every service; "" = "0.0.0"
    Description string            // reported by INFO
    Metadata    map[string]string // reported with the service identity
}
```

- One service is exposed per module that owns request-reply services, with one endpoint per service.
- Stats carry the standard per-endpoint fields and, in `data`, mono's queue wait time (`queue_wait_time`, `average_queue_wait_time`, `max_queue_wait_time`), `expired`, `expired_queue_wait_max`, `rejected`, `pending_msgs`, `pool_active` and `pool_queued`.
- Any client that can reach the server can read these responses (including `last_error` text and `Metadata`); restrict `$SRV.>` with server permissions when the NATS port is exposed beyond the application.
- Off by default because it adds subscriptions.

#### WithStrictModuleBoundaries

```go
func WithStrictModuleBoundaries() MonoFrameworkOption
```

Makes a module boundary violation fail with `ErrModuleBoundaryViolation` instead of being recorded.

| Property | Value |
|----------|-------|
| Type | none (flag) |
| Default | off (record and warn) |

**Example:**
```go
mono.WithStrictModuleBoundaries()
```

A violation is:

- a mutating call on a dependency's `ServiceContainer` (only lookups are allowed);
- a channel service requested under another module's name;
- a publish, request or subscription on the `services.<module>.<service>` subject of a module that is not itself, a declared dependency or a plugin;
- for a module implementing `ServiceConsumerModule`, looking up or sending to a dependency service missing from `ConsumeServices()`. Such a use fails when the call is made (a lookup in `SetDependencyServiceContainer` fails the module start), so complete every `ConsumeServices()` list before turning strict mode on.

Without the option the call goes through, is warned once, and appears in [`ModuleBoundaries()`](#moduleboundaries). Turn it on once the report is clean, and in tests.

#### WithSubscriptionStatsLogInterval

```go
func WithSubscriptionStatsLogInterval(d time.Duration) MonoFrameworkOption
```

Logs [`SubscriptionStats`](#subscriptionstats) for every pooled subscription at Debug level every `d`.

| Property | Value |
|----------|-------|
| Type | `time.Duration` |
| Default | `0` (disabled) |
| Constraints | ≥ 0 |

**Example:**
```go
mono.WithSubscriptionStatsLogInterval(30 * time.Second)
```

#### WithDeadlineSkewAllowance

```go
func WithDeadlineSkewAllowance(d time.Duration) MonoFrameworkOption
```

Sets the clock skew tolerated when a request-reply handler decides whether the caller's deadline (sent in `Mono-Deadline`) has already passed. Set it to the largest clock difference you expect between callers and this instance.

| Property | Value |
|----------|-------|
| Type | `time.Duration` |
| Default | `250 * time.Millisecond` |
| Constraints | ≥ 0 (0 selects the default) |

**Example:**
```go
mono.WithDeadlineSkewAllowance(time.Second)
```

- A request whose deadline passed by more than the allowance while it waited in the queue is dropped without running the handler (the caller has given up); otherwise the handler runs with a context that expires at the caller's deadline.
- When `Mono-Deadline` and `Mono-Timeout` disagree by more than the allowance, the clock-independent `Mono-Timeout` is used and a warning is logged.

#### WithDefaultServiceConcurrency

```go
func WithDefaultServiceConcurrency(n int) MonoFrameworkOption
```

Sets how many handlers may run at once for each request-reply service that does not set its own `MaxConcurrency` (see [Handler Concurrency](container.md#handler-concurrency-worker-pools)).

| Property | Value |
|----------|-------|
| Type | `int` |
| Default | `0` = `runtime.GOMAXPROCS(0) * 4` |
| Constraints | ≥ 1 |

**Example:**
```go
mono.WithDefaultServiceConcurrency(16)
```

Pass `1` to run request-reply handlers serially, as versions before worker pools did; a single registration can still override it with `mono.WithMaxConcurrency`. Queue group and event consumer handlers are serial unless their own registration opts in, so this option does not affect them.

## Default Configuration

```go
func DefaultConfig() *types.MonoFrameworkConfig
```

Returns a configuration with all default values (listed in the [Options Index](#options-index)). Useful for testing. `MonoFrameworkConfig` has no root alias: it is `types.MonoFrameworkConfig` (package `github.com/go-monolith/mono/pkg/types`). The shutdown drain timeout and the deadline skew allowance are `0` in the returned value and resolve to their effective defaults. Likewise `JetStreamReadyTimeout` and `JetStreamDefaultReplicas` are `0` (2 minutes, one replica), and the max payload is `0` (the nats-server default).

## Constants and Enums

### Framework States

```go
type MonoFrameworkState int

const (
    StateCreated MonoFrameworkState = iota // Framework created, not started
    StateStarting                          // Framework starting up
    StateRunning                           // Framework running
    StateStopping                          // Framework shutting down
    StateStopped                           // Framework stopped
)
```

`MonoFrameworkState.String()` returns `Created`, `Starting`, `Running`, `Stopping` or `Stopped`.

### Log Levels

```go
const (
    LogLevelDebug LogLevel = iota
    LogLevelInfo
    LogLevelWarn
    LogLevelError
)
```

### Log Formats

```go
const (
    LogFormatJSON LogFormat = iota
    LogFormatText
)
```

## Examples

### Complete Application Setup

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
    // Create application with configuration
    app, err := mono.NewMonoApplication(
        mono.WithNATSPort(4222),
        mono.WithJetStreamStorageDir("./data"),
        mono.WithLogLevel(mono.LogLevelInfo),
        mono.WithShutdownTimeout(30*time.Second),
    )
    if err != nil {
        log.Fatalf("Failed to create application: %v", err)
    }
    defer app.Stop(context.Background())

    // Register modules
    if err := app.Register(&OrderModule{}); err != nil {
        log.Fatalf("Failed to register module: %v", err)
    }
    if err := app.Register(&PaymentModule{}); err != nil {
        log.Fatalf("Failed to register module: %v", err)
    }

    // Start framework
    if err := app.Start(context.Background()); err != nil {
        log.Fatalf("Failed to start application: %v", err)
    }

    // Application is running
    log.Println("Application started successfully")

    // Get health status
    health := app.Health(context.Background())
    log.Printf("Framework healthy: %v\n", health.Healthy)

    // The framework does not handle signals: wait for one, then the
    // deferred Stop shuts the application down.
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()
    <-ctx.Done()
}
```

### Checking Module Health

```go
health := app.Health(context.Background())
if !health.Healthy {
    log.Printf("Framework unhealthy: %s\n", health.Message)
    for name, moduleHealth := range health.Modules {
        if !moduleHealth.Healthy {
            log.Printf("Module %s: %s\n", name, moduleHealth.Message)
        }
    }
}
```

### Accessing Services

```go
// Get a module's service container (nil unless the application is running)
services := app.Services("payment")

// Call a request-reply service with typed request and response
var resp PaymentResponse
err := helper.CallRequestReplyService(context.Background(), services, "process-payment",
    json.Marshal, json.Unmarshal, PaymentRequest{Amount: 100.00}, &resp)
```

`helper` is `github.com/go-monolith/mono/pkg/helper`. For raw bytes, use `services.GetRequestReplyService("process-payment")` and the client's `Call(ctx, data)`.

## Related Documentation

- [Module API](module.md) - Module interfaces and lifecycle
- [Service Container API](container.md) - Service management
- [EventBus API](eventbus.md) - Event publishing
- [Clustering and Kubernetes](../guides/clustering.md) - Clustered JetStream, readiness, operator access
- [API Reference](README.md) - All APIs overview

---

For more information, see the [Foundation Specification](../../spec/foundation.md) and [Getting Started](../getting-started/README.md) guide.
