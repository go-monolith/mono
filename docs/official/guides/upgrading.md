# Upgrading to v0.1.0, v0.1.2 and v0.2.0

This page lists what changes when you upgrade to **v0.1.0**, including the breaking changes that shipped in v0.0.15 to v0.0.19 without a minor version bump, what changes in **v0.1.2** (see [Upgrading to v0.1.2](#upgrading-to-v012)), and what changes in **v0.2.0** (see [Upgrading to v0.2.0](#upgrading-to-v020)).

## Who Is Affected

| You have | Affected by |
|----------|-------------|
| Only modules that use the framework's own container and event bus | Behaviour changes only. No code change is required |
| A type that wraps or implements `ServiceContainer` (decorators, test fakes) | `RegisterRequestReplyService` signature |
| A type that implements `EventRegistry` (test fakes) | `RegisterEventConsumerWithOptions` |
| A type that implements `MonoApplication` without embedding it | `SubscriptionStats()` (v0.0.18) and `ModuleBoundaries()` (v0.1.0) |
| A short `WithShutdownTimeout`, or modules with a slow `Stop` | "`WithShutdownTimeout` is enforced" below |
| Polyglot clients, or middleware that inspects message headers | "Request-reply handlers receive the caller's deadline" below |
| Request-reply services that receive bursts faster than they dispatch | "Request-reply client buffer is bounded" below |
| Alerts or log parsing on framework messages | "Handler panics are recovered" and "NATS client errors go to the framework logger" below |
| `WithStrictModuleBoundaries()` together with modules that implement `ConsumeServices()` | "`ConsumeServices()` is a complete list" (v0.1.2) |
| Module code that reads handlers or channels from a dependency's `Entries()` | "`Entries()` on a dependency container returns descriptions" (v0.1.2) |
| `monotest.CheckModule` tests | "`monotest.CheckModule` has a rule 9" (v0.1.2) |
| Every application | "Go 1.26 and nats-server v2.14.10" (v0.2.0) |
| Stream consumer services | "Stream consumer services use durable consumers" (v0.2.0) |
| JetStream, in particular clustered | "`Start` waits for JetStream" (v0.2.0) |
| `WithNATSClustering` together with `WithNATSConfigFile` | "`WithNATSClustering` is merged into the config file" (v0.2.0) |
| `WithNATSClustering` with JetStream | "Clustered JetStream requires a server name and routes" and "Route URLs are validated" (v0.2.0) |
| A `MonoApplication` health endpoint | "Health reports JetStream" (v0.2.0) |
| Alerts or log parsing on fetch errors | "Fetch errors are rate-limited" (v0.2.0) |

## Compile-Time Changes

### `RegisterRequestReplyService` takes options (since v0.0.15)

```go
// Before
RegisterRequestReplyService(name string, handler RequestReplyHandler) error

// After
RegisterRequestReplyService(name string, handler RequestReplyHandler, opts ...ServiceOption) error
```

Callers are unaffected. A type that overrides or implements the method must accept the options, and a wrapper must forward them, or per-service worker pool options are silently dropped:

```go
func (c myContainer) RegisterRequestReplyService(name string, handler mono.RequestReplyHandler, opts ...mono.ServiceOption) error {
    return c.ServiceContainer.RegisterRequestReplyService(name, wrap(handler), opts...)
}
```

### `EventRegistry` gains `RegisterEventConsumerWithOptions` (since v0.0.15)

```go
RegisterEventConsumerWithOptions(def BaseEventDefinition, handler EventConsumerHandler, module Module, opts EventConsumerOptions) error
```

Add the method to hand-written registry fakes.

### `MonoApplication` gains `SubscriptionStats()` (since v0.0.18)

```go
SubscriptionStats() []SubscriptionStats
```

### `MonoApplication` gains `ModuleBoundaries()` (v0.1.0)

```go
ModuleBoundaries() []ModuleBoundaryReport
```

A type that embeds `mono.MonoApplication` still compiles. A full implementation written against v0.0.14 must add **both** `SubscriptionStats()` and `ModuleBoundaries()`.

{% hint style="info" %}
**Prefer `monotest` to hand-written fakes.** `monotest.NewContainer` is the framework's real container, so it follows interface changes for you. See the [Testing Guide](testing.md#testing-without-nats-monotest).
{% endhint %}

## Behaviour Changes

### Request-reply handlers run on a worker pool (since v0.0.15)

Request-reply handlers of one service now run concurrently, up to `runtime.GOMAXPROCS(0)*4` at once. Queue group services and event consumers stay serial unless their registration sets `MaxConcurrency`. Use `mono.WithDefaultServiceConcurrency(1)` to restore fully serial handling. See [Handler Concurrency](../api/container.md#handler-concurrency-worker-pools).

### Handler panics are recovered (since v0.0.16)

A panic in a handler no longer crashes the process. It is logged with its stack, and a request-reply caller receives `ErrHandlerPanic`.

The error text of a recovered cron handler panic changed from `cron handler panicked: <value>` to `handler panicked: <value>`, the same text as every other handler. Update alerts or log queries that match the old text.

### Request-reply handlers receive the caller's deadline (since v0.0.17)

- **New headers.** Request-reply calls, and queue group sends in ACK mode, carry two headers: `Mono-Deadline` (absolute deadline, Unix milliseconds) and `Mono-Timeout` (time remaining, milliseconds). Polyglot consumers and header-inspecting middleware see them. Removing both opts a request out of server-side expiry.
- **Expired requests are dropped.** A request whose deadline passed while it waited in the queue, by more than the clock-skew allowance (`WithDeadlineSkewAllowance`, default 250ms), is dropped: the handler does not run and no reply is sent. Queue group messages and events are never dropped.
- **Handler context.** The handler context now ends at the caller's deadline and is cancelled when the handler returns. It used to be the long-lived runtime context. A handler that starts background work must derive from `context.WithoutCancel(ctx)`.

### `WithShutdownTimeout` is enforced (since v0.0.17)

- `Stop` used to ignore `WithShutdownTimeout`. It now bounds `Stop` when the caller's context has no deadline. The default is 30s, so a `Stop(context.Background())` that used to wait indefinitely now gives up after 30s.
- The drain of subscriptions and in-flight handlers runs inside that bound and is capped to leave time for module `Stop`. With `WithShutdownTimeout(5*time.Second)`, in-flight handlers get about 3.75s.
- The new `WithShutdownDrainTimeout` must not exceed `WithShutdownTimeout`; `NewMonoApplication` returns an error otherwise.

Modules with a slow `Stop` and a short configured timeout are affected. See [WithShutdownTimeout](../api/framework.md#withshutdowntimeout) and [WithShutdownDrainTimeout](../api/framework.md#withshutdowndraintimeout).

### Request-reply client buffer is bounded (since v0.0.18)

The NATS client buffer of each request-reply subscription is now bounded at 10,000 messages and 16 MB, down from nats.go's defaults of about 500,000 messages and 64 MB. When the dispatcher falls that far behind, nats.go drops further messages as a slow consumer, and the caller sees a timeout. The framework logs the drop at Error level.

Raise or remove the bound per registration:

```go
container.RegisterRequestReplyService("check-stock", handler, mono.WithPendingLimits(100_000, 64<<20)) // -1 means unlimited
```

Queue group services and event consumers keep nats.go's defaults.

### NATS client errors go to the framework logger (since v0.0.18)

NATS client errors and connection events (disconnect, reconnect, closed) are logged through the framework logger instead of being printed to stderr. The framework's client connection now reports the name `mono` in `/connz` and server logs. Change it with [`WithNATSClientName`](../api/framework.md#withnatsclientname), or override the handlers with [`WithNATSClientOptions`](../api/framework.md#withnatsclientoptions).

### Dependency containers and event buses are wrapped (v0.1.0)

- The container passed to `SetDependencyServiceContainer` is a read-only view of the provider's container.
- The bus passed to `SetEventBus` is scoped to the module. `app.EventBus(name)` returns that scoped bus for a started module.
- Identity comparisons with `app.Services(name)` or the shared bus no longer hold.
- `EventBusWithConn[*nats.Conn]` assertions keep working.

Calls that cross a boundary still succeed by default, but now log a warning and appear in `app.ModuleBoundaries()`:

- registering on, or unregistering from, a dependency's container;
- a raw publish or request to `services.<module>.<service>` of a module that is not a declared dependency;
- subscribing to another module's service subject.

### More log lines at shutdown (v0.1.0)

`Stop` logs one Info line per declared dependency that was never looked up, and one Warn line per boundary violation kind.

### Start errors keep their cause (v0.1.0)

`errors.Is(err, ErrServiceNotFound)` and similar checks (sentinels from `github.com/go-monolith/mono/pkg/errors`) now work on the error returned by `app.Start`. The message text is unchanged.

## Upgrading to v0.1.2

v0.1.2 tightens the module boundary checks introduced in v0.1.0 and adds an import rule check. One change applies to every application: `Entries()` on a dependency container returns descriptions (below). Applications whose modules do not read a dependency's entries and that do not use `ConsumeServices()`, strict mode or `monotest.CheckModule` are not affected.

### `ConsumeServices()` is a complete list

A module that implements `ConsumeServices()` and uses a request-reply, queue group or stream consumer service of a dependency that is **not** in the list now commits an `undeclared-service-use` boundary violation. A use is a lookup on the dependency's container, or a publish or request on the module's event bus to the service subject `services.<dependency>.<service>`.

| Mode | Effect |
|------|--------|
| Default | The call goes through. One warning is logged, and the use appears in `app.ModuleBoundaries()` |
| `WithStrictModuleBoundaries()` | The lookup, publish or request fails with `ErrModuleBoundaryViolation` |

Lookups happen when a module makes the call, so in strict mode an incomplete list fails on the first call at runtime, not at startup. The exception is a service the module looks up in `SetDependencyServiceContainer`: in strict mode one that is not in the list fails the start of the module. Before enabling or keeping strict mode, run in the default mode, read the report, and add every contract the module calls to `ConsumeServices()`. Modules that do not implement the interface, channel services and plugin services are not checked.

A lookup of a service the dependency does not provide:

| Mode | Effect |
|------|--------|
| Default | `ErrServiceNotFound`, as before. It is not a violation, so a module may ask for an optional service and fall back |
| `WithStrictModuleBoundaries()` | `ErrModuleBoundaryViolation` instead of `ErrServiceNotFound`: the module asked for something it did not declare |

`ConsumeServices()` is called once per start, after the module's dependency containers are set, and that one list is both validated and enforced. A module may build it from what it was wired with.

### `Entries()` on a dependency container returns descriptions

`Entries()` on the container passed to `SetDependencyServiceContainer` returns copies that describe the dependency's services (name, type, subject, queue groups, options, stream and cron configuration, contract). The handlers and channels are left out (nil), in the default mode as well as in strict mode. The copies are deep, so changing one, including the subjects or metadata of its stream configuration, has no effect on the provider. `app.Services(name).Entries()` in the host program is unchanged.

A module that read `RequestHandler`, `QueueHandlers[i].Handler`, `InChannel` or `OutChannel` from a dependency's entry now finds nil: calling the handler panics and a send on the channel blocks. Reach the service through `GetRequestReplyService`, `GetQueueGroupService`, `GetStreamConsumerService` or `GetChannelService` instead. Reading the entries is not a boundary violation, so it is not in the boundary report. The framework logs one warning for the first `Entries()` call of a module on each dependency:

```text
Entries() of a dependency container returns descriptions without handlers and channels; reach its services through Get* (the host reads the real entries with MonoFramework.Services) module=order dependency=inventory
```

### `monotest.CheckModule` has a rule 9

A `ConsumeServices()` contract that is never used during `Start` or the exercise (looked up on the dependency's container, or reached on the event bus) fails the conformance test. Call it from `WithExercise`, remove it from the list, or exempt it with `monotest.WithUnexercisedContract(def)`.

### `ServiceLookup`

`mono.ServiceLookup` is the read side of `ServiceContainer` (`Get*` and `Has`), and `ServiceContainer` embeds it. The method set of `ServiceContainer` is unchanged, so implementations and fakes still compile. `helper.CallRequestReplyService`, `SendQueueGroupService`, `PublishStreamConsumerService` and the typed contracts' `Call` / `Send` / `Publish` accept a `ServiceLookup`; callers that pass a `ServiceContainer` are unaffected.

### More in the boundary report

`ModuleBoundaryReport` gains `UnusedContracts`: the consumed contracts not used so far. `Stop` logs one Info line for each. Like `Observed`, `UnusedDependencies` and `Violations` it is an empty list when there is nothing to list, so `unused_contracts` is always present in the JSON form.

### New: `monotest.CheckImports`

`monotest.CheckImports(t)` checks the contract package import rule on the import graph of the application. Nothing changes until you add the test.

Two things to know when you add it:

- Put the test in the root directory of the application. The check fails, instead of passing with nothing checked, when it finds no package to check, no module, or no package of a module.
- Every directory named `contract` marks its parent as a module. If you have a package with that name that is something else, such as `legal/contract` for customer contracts or a helper `order/internal/contract`, name it with `monotest.WithNonContractPackages`.

See [Checking Imports](module-boundaries.md#checking-imports).

## Upgrading to v0.2.0

v0.2.0 makes JetStream work in a cluster: durable stream consumers, a wait for JetStream at `Start`, options for server names, route authentication and replicas, and JetStream in the health report. For running a cluster, see the [Clustering and Kubernetes guide](clustering.md).

### Go 1.26 and nats-server v2.14.10

The minimum Go version is now **1.26**. The embedded server is **nats-server v2.14.10** (with nats.go v1.55.0).

nats-server v2.14.10 starts a pull consumer's inactivity timer whenever the consumer becomes leader, including when the server starts and recovers its store. A consumer created with a `Name` only (non-durable) is therefore deleted about 5s after the server starts unless something pulls it, which replays its stream when it is recreated and, on an interest-retention stream, removes the messages only it still needed. Earlier framework versions created such consumers for stream consumer services, for cron services (up to v0.0.12) and for event stream consumers (up to v0.0.11). The framework now protects them: right after the embedded server starts (inside `NewMonoApplication`, before any of your code runs) and again when `Start` begins, it raises to 1h the inactivity threshold of every pull consumer that the server could delete for inactivity: a non-durable one still at the server's default threshold, or a durable whose conversion from a non-durable was interrupted. A consumer with any threshold other than the default 5s is left alone (an explicit 5s cannot be told apart from the default). This includes non-durable consumers of your own: they are removed by the server 1h after their last pull instead of 5s after the start. On an interest-retention stream such a leftover consumer keeps its messages for that hour, and on a work-queue stream it can block a new consumer with an overlapping filter ("consumer must be unique") until it expires. If your application creates a fresh non-durable consumer at each start, delete the old one or set `InactiveThreshold` explicitly. The framework logs `Extended the inactivity threshold of non-durable consumers recovered at start` (and `Protected non-durable consumers until they are set up`) when it changed any.

In a cluster the protection can only run once JetStream has a leader. A non-durable consumer whose own leader is elected more than about 5s before the cluster reaches quorum can still be deleted; this was already the case with earlier versions.

Legacy event stream durables (up to v0.0.11) keep the 1h threshold, so the server removes one that is no longer pulled 1h after its last pull. The framework still never deletes them itself.

### Stream consumer services use durable consumers

A stream consumer service is now consumed through the durable consumer `<module>-<service>`, sent with both `Name` and `Durable`. A `Name` or `Durable` set in `StreamConsumerConfig.Consumer` is replaced.

Before, the consumer had a `Name` only. The server deleted it about 5s after the last pull: during a slow shutdown, or while every instance was down. The next start recreated it from its deliver policy, so with the default (all) the stream was delivered again.

An existing consumer from an earlier version is converted to a durable in place at the first start, keeping its position. The framework logs `Stream consumer converted to durable`. No action is needed.

A consumer configuration that changes the deliver policy or start position of an existing consumer still fails `Start`. Since the consumer is durable, it no longer disappears by itself: to apply such a change, delete it with `nats consumer rm <stream> <consumer>`.

### `Start` waits for JetStream

Before it starts plugins and modules, `Start` now waits until JetStream answers. It then retries the JetStream setup steps of stream consumer, cron and event stream consumer services while they fail with an error that can clear by itself (a timeout, no responders, no peers for placement yet, JetStream temporarily unavailable, a stream or consumer offline). Any other error still fails `Start` at once.

- A single server answers at once, so nothing changes there.
- A `Start` that used to fail fast on a JetStream timeout now retries for up to 2 minutes. Change the bound with [`WithJetStreamReadyTimeout`](../api/framework.md#withjetstreamreadytimeout).
- New Info log lines while it waits: `Waiting for JetStream` (at most every 10s per step) and `JetStream step succeeded after waiting`.
- Plugins and modules that use JetStream in their own `Start` now find it answering. Their own JetStream calls are not retried by the framework.

### `WithNATSClustering` is merged into the config file

**Breaking for applications that use `WithNATSClustering` together with `WithNATSConfigFile`.** The option used to replace the file's `cluster{}` block. It is now merged into it: name, host, port and routes come from the option, and the file's authorization, TLS, permissions, pool size and compression now apply, where before they were silently dropped.

If your file has a `cluster{}` block with `authorization` or `tls`, check that your routes still authenticate: the routes must now present those credentials (use [`WithNATSClusterAuth`](../api/framework.md#withnatsclusterauth) or credentials in the route URLs) and complete the TLS handshake.

### Clustered JetStream requires a server name and routes

Without a config file, `NewMonoApplication` now returns an error when clustering and JetStream are configured without:

- a server name ([`WithNATSServerName`](../api/framework.md#withnatsservername)), unique within the cluster and stable across restarts;
- at least one route. List all servers of the cluster on every server, its own included.

With a config file, the same checks run at `Start` on the merged configuration (`server_name`, `cluster.name`, `routes` or leafnode remotes). nats-server requires them too; the framework now reports a missing one in its own terms before the server starts, instead of a server failure that could show up only as a readiness timeout.

### Route URLs are validated

A route must have the form `nats://[user:password@]host:port`. A route with a path, query, fragment or opaque part is now rejected by `WithNATSClustering` (and at `Start`). This also catches an unencoded `/`, `?` or `#` in a password: percent-encode special characters in credentials. Errors name the route by its index and never quote it, and logged routes no longer include passwords.

### New options

| Option | Purpose |
|--------|---------|
| [`WithNATSServerName(name)`](../api/framework.md#withnatsservername) | The NATS server name; required for clustered JetStream |
| [`WithNATSClusterAuth(user, password)`](../api/framework.md#withnatsclusterauth) | Route authentication; adds the credentials to every route URL without its own |
| [`WithJetStreamReadyTimeout(d)`](../api/framework.md#withjetstreamreadytimeout) | Bounds the wait for JetStream and each retried setup step (default 2 minutes) |
| [`WithJetStreamDefaultReplicas(n)`](../api/framework.md#withjetstreamdefaultreplicas) | Replicas of framework-created streams that leave `Replicas` at 0 (1..5; above 1 requires clustering) |

`CronServiceConfig` gains `Replicas` (0 = the default above, at most 5). With one replica a cron schedule lives on one server and stops firing while that server is down. Raising the value updates existing streams in place.

### Health reports JetStream

`FrameworkHealth.JetStream` (`*JetStreamHealth`: `Available`, `Clustered`, `MetaLeader`, `ClusterSize`, `Message`) reports the JetStream state of this server. It is nil when JetStream is disabled. It is **not** part of `Healthy`: in a cluster every server loses JetStream while there is no quorum, and counting it would restart the surviving servers. Include `JetStream.Available` in a readiness probe if you need it, never in a liveness probe. The JSON form adds a `jetstream` key. See [Health and Status](../api/framework.md#health-and-status).

### Fetch errors are rate-limited

The fetch loops of stream consumer, event stream consumer and cron services used to log one Error line per failed fetch, and could spin when the server answered at once with an error. They now:

- log at most one line (failure or recovery) per 10s per loop, with `failures` and `suppressed` counts;
- log at Warn for errors that clear by themselves (for example a consumer without a leader while a cluster forms), at Error otherwise;
- log `Fetch error cleared` (`Cron fetch error cleared` for cron) at Info when fetching works again;
- wait 1s after a failed fetch that returned nothing.

The separate `Fetch batch error` and `Cron fetch batch error` messages are gone: batch errors are reported as `Fetch error` and `Cron fetch error`. Update alerts or log queries that match them.

### Rolling back from v0.2.0

Rolling back the application to an earlier framework version does not touch data on disk: v2.14.4 and v2.14.10 use the same store format. Two things change behaviour, though:

- **Stream consumer services become non-durable again.** Earlier versions update the consumer without `Durable`. It keeps its position, but from the next server restart the server deletes it once it has not been pulled for 5s. In a cluster the timer starts whenever its leader is elected, so the consumer can be lost then; on a single server with v2.14.4 the timer is not started at restart. When the consumer is recreated, the stream replays from its deliver policy, and on an interest-retention stream messages only it needed are removed. Cron consumers behave the same when rolled back to v0.0.12 or earlier.
- **Replicated streams are scaled back to one replica.** If you set `WithJetStreamDefaultReplicas` or `CronServiceConfig.Replicas` above 1, an earlier version sends 0 replicas for these streams, and the server scales each stream down to one replica the first time an old instance starts. Scaling down keeps the leader's data, but a stream whose leader is unavailable at that moment may keep a peer that lags behind. Before rolling back, scale those streams down yourself while every server is healthy (`nats stream edit <stream> --replicas 1`), or keep at least one instance on v0.2.0 until the rollback is complete.

## Recommended Next Steps

Nothing below is required to upgrade. It is the recommended path for new and existing modules:

1. Move each module's DTOs and event definitions into a `contract` sub-package and declare typed service contracts.
2. Implement `ConsumeServices()` on modules that call other modules.
3. Add a `monotest.CheckModule` test per module, and one `monotest.CheckImports` test for the application (v0.1.2+).
4. Read `app.ModuleBoundaries()`, fix what it reports, then enable `mono.WithStrictModuleBoundaries()`.

See the [Module Boundaries Guide](module-boundaries.md).
