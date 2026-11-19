# Upgrading from v0.1.x to v0.2.x

This page covers upgrading an application from any v0.1.x release to v0.2.x. Each change names the release that introduced it (up to v0.2.2; later v0.2.x releases so far changed documentation only). Applications on v0.1.2 can skip the changes marked v0.1.2.

## Who Is Affected

| You have | Read |
|----------|------|
| Any application | [Go 1.26](#go-126-and-nats-server-v21410) |
| Stream consumer services | [Durable stream consumers](#stream-consumer-services-use-durable-consumers) |
| JetStream (single server or cluster) | [`Start` waits for JetStream](#start-waits-for-jetstream), [Non-durable consumers at start](#non-durable-consumers-are-protected-at-start) |
| `WithNATSClustering` | [Clustering changes](#clustering-changes) |
| Alerts or log queries on `Fetch error` | [Fetch error logging](#fetch-error-logging) |
| Code that stores `Publish` as a function value | [`Publish` takes options](#publish-takes-options) |
| `ConsumeServices()`, `WithStrictModuleBoundaries()` or `monotest.CheckModule` (from v0.1.0 or v0.1.1) | [Module boundary changes](#module-boundary-changes-v012) |
| Module code that reads a dependency's `Entries()` (from v0.1.0 or v0.1.1) | [`Entries()` returns descriptions](#entries-on-a-dependency-container-returns-descriptions) |

Everything else upgrades without a code change.

## Changes That May Need Action

### Go 1.26 and nats-server v2.14.10

*v0.2.0.* The minimum Go version is **1.26**. The embedded server is **nats-server v2.14.10** (nats.go v1.55.0). Raise the `go` directive of your module.

### Stream consumer services use durable consumers

*v0.2.0.* A stream consumer service is consumed through the durable consumer `<module>-<service>`. A `Name` or `Durable` set in `StreamConsumerConfig.Consumer` is replaced.

- **Existing consumers** are converted in place at the first start and keep their position. The framework logs `Stream consumer converted to durable`. No action is needed.
- **Why:** the consumer used to be non-durable, so the server deleted it about 5s after the last pull (a slow shutdown, every instance down), and the next start delivered the stream again.
- **Changing the deliver policy or start position** of an existing consumer still fails `Start`. A durable no longer disappears by itself, so delete it to apply such a change: `nats consumer rm <stream> <consumer>`.

### Clustering changes

*v0.2.0.* These affect applications that use `WithNATSClustering`.

- **Merged into the config file.** With `WithNATSConfigFile`, the option is merged into the file's `cluster{}` block instead of replacing it. Name, host and port come from the option, its routes are added to the file's routes, and the file's authorization, TLS, permissions, pool size and compression now apply; they used to be dropped silently. If the block has `authorization` or `tls`, check that your routes still connect: they must present the credentials (use [`WithNATSClusterAuth`](../api/framework.md#withnatsclusterauth)) and complete the TLS handshake.
- **Clustered JetStream needs a server name and routes.** Set [`WithNATSServerName`](../api/framework.md#withnatsservername), unique within the cluster and stable across restarts, and list every server on every server, itself included. Without a config file `NewMonoApplication` rejects a missing one; with a config file the same checks run on the merged configuration when the server starts.
- **Route URLs are validated.** A route must be `nats://[user:password@]host:port`. A path, query, fragment or opaque part is rejected, which catches an unencoded `/`, `?` or `#` in a password: percent-encode credentials. Errors and logs never show the password.

### Fetch error logging

The fetch loops of stream consumer, event stream consumer and cron services log failures differently. Update alerts and log queries.

- *v0.2.0.* At most one line per 10s per loop, with `failures` and `suppressed` counts, then `Fetch error cleared` (`Cron fetch error cleared`) at Info when fetching works again. A failed fetch that returned nothing is followed by a 1s wait, so the loop no longer spins.
- *v0.2.0.* `Fetch batch error` and `Cron fetch batch error` are gone; batch errors are reported as `Fetch error` and `Cron fetch error`.
- *v0.2.0.* Errors that clear by themselves (a consumer without a leader while a cluster forms) are logged at Warn, others at Error.
- *v0.2.2.* `nats: leadership change` and `nats: server shutdown`, which every rolling restart of a peer produces, are logged at Warn instead of Error.

### `Publish` takes options

*v0.2.2.* `StreamConsumerDefinition[T].Publish` and `helper.PublishStreamConsumerService` take variadic `helper.PublishOption`s (see [Message IDs](#new-features)). Calls compile unchanged. Code that assigns either to a variable of a declared function type must add the variadic parameter to that type.

## Behaviour Changes

These need no code change, but you may notice them.

### `Start` waits for JetStream

*v0.2.0.* Before it starts plugins and modules, `Start` waits until JetStream answers, then retries the JetStream setup of stream consumer, cron and event stream consumer services while it fails with an error that clears by itself (a timeout, no responders, JetStream temporarily unavailable, a stream or consumer offline). Any other error still fails `Start` at once.

- A single server answers at once: nothing changes.
- In a cluster, `Start` waits for the other servers for up to 2 minutes instead of failing. Change the bound with [`WithJetStreamReadyTimeout`](../api/framework.md#withjetstreamreadytimeout).
- While it waits, it logs `Waiting for JetStream` at Info, at most every 10s per step.

### Non-durable consumers are protected at start

*v0.2.0.* nats-server v2.14.10 deletes a non-durable pull consumer about 5s after the server starts unless something pulls it. On an interest-retention stream that also removes the messages only that consumer needed. Right after the embedded server starts, and again in `Start`, the framework raises to 1h the inactivity threshold of every such consumer still at the server's default (5s). It logs `Extended the inactivity threshold of non-durable consumers recovered at start` when it changed any.

This includes non-durable consumers your application creates itself. The server now removes an abandoned one 1h after its last pull instead of 5s. On an interest-retention stream it keeps its messages for that hour; on a work-queue stream it can block a new consumer with an overlapping filter. If you create a fresh non-durable consumer at every start, delete the old one or set `InactiveThreshold` explicitly. A consumer with any other threshold is left alone.

### Plugin services are served, and a failed `Start` stops plugins

*v0.2.6.* Two fixes to plugins:

- **Plugin services are served over NATS.** Request-reply, queue group, stream consumer and cron services registered in a plugin's container are subscribed at `services.<plugin name>.<service>`, and its channel routers run. Before, they were never subscribed and calls got no responder. A plugin that registers such services and shares its `Name()` with a module, or with another plugin that registers such services, now fails `Start`.
- **A failed `Start` stops the plugins and middleware.** When a middleware or module fails to start, or a later step fails, the started middleware and the plugins are stopped too, after the modules. Before, their `Stop()` was never called, not by the rollback and not by `app.Stop()`.

### JetStream from a config file is recognized

*v0.2.7.* JetStream enabled only by a `jetstream{}` block in a `WithNATSConfigFile` file is treated like JetStream enabled by `WithJetStreamStorageDir`. The startup log says `jetstream=true` and `JetStream enabled`, the consumer protection pass runs right after the server starts, and `WithJetStreamDomain` applies; it was silently dropped before. JetStream services already worked.

### Middleware fixes

*v0.2.8.* Fixes to the built-in middleware and to how the framework runs middleware:

- **Middleware services are registered.** A middleware that implements `ServiceProviderModule` gets a container of its own, and `RegisterServices` is called on it before `Start`. A module that depends on it receives that container. The audit `audit-trail` service, and with it `audit.NewAuditAdapter`, now works. Each consumer's adapter receives only the confirmations of its own entries.
- **`os.Stdout` and `os.Stderr` stay open.** The access log and audit middleware no longer close them at `Stop`. A pipe or terminal no longer makes `Stop` fail with `sync … invalid argument`. Other writers are still synced and closed.
- **`audit.VerifyChainFrom(entries, startHash)`** verifies a log continued with `WithHashChaining(lastHash)`.
- **`OnConfigurationChange` is deprecated.** It was never called and stays in the interface. `audit.EventConfigurationUpdate` and `RunConfigurationChange` are deprecated with it.

### Health reports JetStream

*v0.2.0.* `FrameworkHealth.JetStream` (`Available`, `Clustered`, `MetaLeader`, `ClusterSize`, `Message`; JSON key `jetstream`) reports this server's JetStream state, and is nil when JetStream is disabled. It is not part of `Healthy`: use `JetStream.Available` in a readiness probe if you need it, never in a liveness probe. See [Health and Status](../api/framework.md#health-and-status).

### Module boundary changes (v0.1.2)

*v0.1.2.* For applications coming from v0.1.0 or v0.1.1.

- **`ConsumeServices()` is a complete list.** A module that implements it and uses a request-reply, queue group or stream consumer service of a dependency that is not in the list commits an `undeclared-service-use` violation: a warning and an entry in `app.ModuleBoundaries()` by default, `ErrModuleBoundaryViolation` under `WithStrictModuleBoundaries()`. Before keeping strict mode, run in the default mode and add every contract the report lists. In strict mode, a lookup of a service the dependency doesn't provide also returns `ErrModuleBoundaryViolation` instead of `ErrServiceNotFound`.
- **`monotest.CheckModule` has a rule 9.** A `ConsumeServices()` contract that is never used during `Start` or the exercise fails the test. Exercise it, remove it, or exempt it with `monotest.WithUnexercisedContract(def)`.
- **`ModuleBoundaryReport.UnusedContracts`** lists consumed contracts not used so far; `Stop` logs one Info line for each.

### `Entries()` on a dependency container returns descriptions

*v0.1.2.* `Entries()` on the container passed to `SetDependencyServiceContainer` returns deep copies without handlers and channels (nil). A module that read `RequestHandler`, `QueueHandlers[i].Handler`, `InChannel` or `OutChannel` from a dependency now finds nil; use `GetRequestReplyService`, `GetQueueGroupService`, `GetStreamConsumerService` or `GetChannelService`. The framework logs one warning for the first such call per dependency. `app.Services(name).Entries()` in the host program is unchanged.

## New Features

All optional.

| Feature | Release | Purpose |
|---------|---------|---------|
| [`WithNATSServerName(name)`](../api/framework.md#withnatsservername) | v0.2.0 | The NATS server name; required for clustered JetStream |
| [`WithNATSClusterAuth(user, password)`](../api/framework.md#withnatsclusterauth) | v0.2.0 | Route authentication |
| [`WithJetStreamReadyTimeout(d)`](../api/framework.md#withjetstreamreadytimeout) | v0.2.0 | Bounds the wait for JetStream at `Start` (default 2 minutes) |
| [`WithJetStreamDefaultReplicas(n)`](../api/framework.md#withjetstreamdefaultreplicas), `CronServiceConfig.Replicas` | v0.2.0 | Replicas of the streams the framework creates (1..5; above 1 requires clustering) |
| [`helper.WithMsgID(id)`](../api/container.md#typed-service-contracts) | v0.2.2 | Message ID on a stream publish, so a retry is stored once |
| [`WithNATSClusterTLS(cert, key, ca)`](../api/framework.md#withnatsclustertls) | v0.2.4 | Mutual TLS on the routes between the servers, with certificate renewal without a restart |
| [`WithNATSClientTLS(types.ClientTLSConfig)`](../api/framework.md#withnatsclienttls), `helper.NATSClientTLS`, `pkg/certreload` | v0.2.5 | TLS, optionally mutual, on the client listener with certificates from files, renewed without a restart on both sides |
| [`BucketConfig.RecreateOnStorageMismatch`](../plugins/kv-jetstream.md#changing-a-buckets-storage-type), `kvjetstream.ErrStorageMismatch` | v0.2.10 | A kv-jetstream bucket whose stream exists with another storage type fails `Start` with a clear error, or, with the opt-in, is recreated: **the bucket is emptied**, after its keys are copied to a backup bucket (kept 7 days by default; `SkipBackupOnRecreate`, `RecreateBackupTTL`). Set it only for a bucket whose contents can be lost |
| [`WithNATSSystemAccount(user, password)`](../api/framework.md#withnatssystemaccount) | v0.2.2 | A system account user for `nats server report` and `nats server raft peer-remove`. Clients that send credentials of their own are rejected once it is on, unless they are the system account user's |
| `mono.ServiceLookup` | v0.1.2 | The read side of `ServiceContainer`, accepted by the typed contracts and helpers |
| [`monotest.CheckImports`](module-boundaries.md#checking-imports) | v0.1.2 | Checks the contract package import rule; put the test in the application's root directory |

For running a cluster, see the [Clustering and Kubernetes guide](clustering.md) and [Kubernetes Deployment Strategies](../extra/kubernetes-deployment.md).

## Rolling Back from v0.2.x

Rolling back to v0.1.x leaves the data on disk readable (the store format is unchanged), but two things change behaviour:

- **Stream consumers become non-durable again.** They keep their position, but after the next server restart the server deletes one that is not pulled within 5s, and the stream is delivered again when it is recreated (on an interest-retention stream, messages only it needed are removed).
- **Replicated streams are scaled down to one replica** the first time an old instance starts, if you set `WithJetStreamDefaultReplicas` or `CronServiceConfig.Replicas` above 1. Scaling down keeps the leader's data, but a stream whose leader is unavailable at that moment may keep a peer that lags behind. Scale them down yourself first while every server is healthy (`nats stream edit <stream> --replicas 1`), or keep one instance on v0.2.x until the rollback is complete.

## Recommended Next Steps

Not required to upgrade:

1. Move each module's DTOs and events into a `contract` sub-package and declare typed service contracts.
2. Implement `ConsumeServices()` on modules that call other modules.
3. Add a `monotest.CheckModule` test per module and one `monotest.CheckImports` test for the application.
4. Read `app.ModuleBoundaries()`, fix what it reports, then enable `mono.WithStrictModuleBoundaries()`.

See the [Module Boundaries Guide](module-boundaries.md).
