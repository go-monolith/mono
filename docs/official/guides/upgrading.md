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

### Registrations that share a stream no longer overwrite each other (v0.2.13)

*v0.2.13.* Registrations that name the same stream but listen on different subjects (two stream consumer services, two events, or a service and an event) used to overwrite each other's subjects: only one registration's messages were stored, and the handlers received each other's messages. The stream is now written with the subjects of all of them and each consumer is filtered on its own.

- **Existing deployments** are repaired at the next start. Durable consumers keep their position. Messages that were never stored cannot be recovered.
- **Handlers stop receiving foreign messages.** A handler that relied on receiving another registration's messages must register for them.
- **Nothing changes** for a registration with a stream of its own, or for registrations with the same subjects on one stream.
- **Setup order:** services are now set up in name order, so which registration's other stream settings win no longer varies between starts.
- **A configuration that used to start may now fail.** Two registrations on one stream whose subjects overlap without one covering the other (`a.*.c` and `a.b.*`) started before, because each write replaced the other's subjects, and one of them lost its messages. `Start` now fails with `ErrInvalidConfiguration`, naming both registrations. Give them subjects that do not overlap, or separate streams.

See [Sharing a Stream Between Registrations](../api/container.md#sharing-a-stream-between-registrations).

### Adding several filters to an existing consumer no longer fails once (v0.2.13)

*v0.2.13.* Adding `Consumer.FilterSubjects` with two or more subjects to a consumer that already existed failed the first `Start` after a restart with `multiple consumer filter subjects not supported by nats-server`, and the next `Start` succeeded: the server had applied the filters but answered with the earlier configuration. The framework now reads the consumer back and accepts the update when the filters are in place.

### Leftover streams and consumers are reported (v0.2.13)

*v0.2.13.* The framework adds a metadata entry (`mono.registration`) to the streams and durable consumers it creates, in place at the next start, and logs a warning at every start for each marked one that no registration uses any more (after a module, service or event was renamed or removed). Nothing is deleted. Leftovers from before v0.2.13 are not recognised. The report runs during `Start` and can add up to 10 seconds when JetStream answers slowly, so allow for it in a startup probe. See [Renaming or Removing a Stream Registration](../api/container.md#renaming-or-removing-a-stream-registration).

### Stream settings that can only be switched on (v0.2.14)

*v0.2.14.* `DenyDelete`, `DenyPurge`, `Sealed`, `AllowMsgTTL` and `AllowMsgSchedules` can be enabled on an existing stream but not disabled again.

- **Switching one off** failed `Start` with the server's generic error and now fails with `ErrStreamConfigImmutable`, naming the setting.
- **A sealed stream keeps starting.** Sealing switches `DenyDelete` and `DenyPurge` on at the server, so a configuration with `Sealed` alone failed every `Start` after the one that sealed the stream. The framework now writes a sealed stream with both enabled (and `AllowRollup` disabled, which sealing also does).
- **`Sealed` with `AllowMsgSchedules` is refused.** The server sealed such a stream and then rejected every later write, for good. `Start` now fails with `ErrInvalidConfiguration` before the stream is touched.
- **Registrations of one stream that disagree** on one of these settings now fail the first `Start`, before anything is created. Before, the first start could succeed and the next one fail, depending on the order.

See [Changing a Stream's Storage Type or Retention](../api/container.md#changing-a-streams-storage-type-or-retention).

### Removing a cron service's `TTL` no longer fails `Start` (v0.2.14)

*v0.2.14.* A cron service whose `TTL` was removed failed every `Start` (`message TTL status can not be disabled`): the framework enabled per-message TTLs on the service's stream only while `TTL` was set, and the server refuses to switch them off. The stream now keeps them enabled once they are on. A cron service that never had a `TTL` is unchanged, so rolling back stays possible for it.

A `SourceSubject` under the service's own subjects (`services.<module>.<name>.<anything>`) is accepted as well; before, the server rejected the stream. A `SourceSubject` with a wildcard, which the scheduler never accepted, is now rejected when the service is registered instead of failing `Start` after the stream was written.

### Immutable consumer settings are kept instead of failing `Start` (v0.2.15)

*v0.2.15.* `DeliverPolicy` (with `OptStartSeq` and `OptStartTime`), `AckPolicy`, `ReplayPolicy`, `MaxWaiting` and `MemoryStorage` cannot be changed on an existing consumer. A configuration that changed one failed `Start` on every restart, because the consumers are durable since v0.2.0 and no longer disappear.

- **`Start` now succeeds.** The existing value is kept, the consumer keeps its position, and a warning is logged at every start while the configuration differs, with the command that applies the change (`nats consumer rm <stream> <consumer>`).
- **The configured value is not in effect** until the consumer is deleted. Deleting it makes the stream be delivered again from the consumer's deliver policy.
- **`AckPolicy`:** the handler must keep acknowledging the way the kept policy requires.
- **A start time computed at startup** no longer fails the second start; it is warned about at every start. Use a fixed time.

See [Changing a Consumer Setting That Cannot Be Changed](../api/container.md#changing-a-consumer-setting-that-cannot-be-changed).

### Clear errors for work-queue consumers and overlapping stream subjects (v0.2.16)

*v0.2.16.* Three configurations that failed `Start` with a server error naming nothing now say what is in the way. Nothing is deleted or moved automatically.

- **A second consumer on a work-queue stream** (a renamed module or service, or two modules consuming one event) fails with `ErrWorkQueueConsumerConflict`, naming the existing consumer and `nats consumer rm`. Removing the old consumer loses no message: what it had not acknowledged is delivered to the new one.
- **A stream asking for subjects another stream holds** (`Stream.Name` renamed, or a stream consumer service turned into a cron service of the same name, or the reverse) fails with `ErrStreamSubjectsOverlap`, naming the stream that holds them.
- **Lowering `ConsumerLimits.MaxAckPending`** on an existing stream no longer fails: the existing consumers of the stream's registrations are lowered to the limit and keep their position. A consumer the application does not register, or an `InactiveThreshold` limit an existing consumer exceeds, still fails `Start`, with an error that says what to change.

- **New warning:** a registration whose stream sets `ConsumerLimits.InactiveThreshold` while `Consumer.InactiveThreshold` is zero is warned about at every start. Its durable consumer inherits the threshold and is deleted, with its position, once a server has run for that long without the consumer being pulled (a rolling deploy to a version without the registration, a start-up or shutdown slower than the threshold). Nothing about the behaviour changed; it was silent before.

**Configurations that used to start and may now fail**, because the registrations are checked before anything is created:

- **Two registrations on the same subjects of one work-queue stream.** The server rejects the second consumer when it is *created*, so such a configuration normally never started. It did start when the second consumer already existed with a filter and the filter was then removed from the configuration: the server accepts that update. Two consumers then competed for the same messages. `Start` now fails with `ErrWorkQueueConsumerConflict`; give the registrations subjects or filters that do not overlap, or use interest retention on a new stream.
- **Two registrations with overlapping subjects on streams with different names.** This always failed, at the second stream's write; it now fails before the first stream is created, with `ErrStreamSubjectsOverlap`.

See [Work-Queue Streams: One Consumer per Subject](../api/container.md#work-queue-streams-one-consumer-per-subject), [Renaming a Stream, or Switching a Service Between Stream Consumer and Cron](../api/container.md#renaming-a-stream-or-switching-a-service-between-stream-consumer-and-cron) and [Lowering a Stream's Consumer Limits](../api/container.md#lowering-a-streams-consumer-limits).

### An explicit host, port or max payload overrides a config file (v0.2.17)

*v0.2.17.* With [`WithNATSConfigFile`](../api/framework.md#withnatsconfigfile), `WithNATSHost`, `WithNATSPort` and `WithNATSMaxPayload` were dropped when their value equalled the framework default. `WithNATSHost("127.0.0.1")` beside a config file without a host therefore left the client listener on every interface, without authentication unless the file configured it, while the log said `host=127.0.0.1`.

- **The options now override the file whenever they are used.** A configuration that passed a default-valued option beside a file that sets another value changes: the option wins.
- **A config file without a host, and no `WithNATSHost`, still listens on every interface** (nats-server's default). This is now logged at Warn when the server starts, and the start-up log reports the address the server is bound to. Add `WithNATSHost("127.0.0.1")` or a `host` in the file if the port is meant to be private.

### `Start` and `Stop` are guarded against each other (v0.2.17)

*v0.2.17.* Overlapping calls all ran before:

- **`Stop` during `Start`** stopped NATS under the start, which then returned nil and left the application "running" without a server and with no module stopped. `Stop` now aborts the start: `Start` stops what it started and returns an error wrapping `context.Canceled`. A program that calls `Stop` from a signal handler while `Start` is running must expect that error from `Start`.
- **A second `Stop` during a graceful one** stopped NATS at once, under the handlers the first one was waiting for. It now waits for the first and returns its result.
- **A second `Start` during `Start`** started the modules twice. It now fails with `application is already starting`.
- **A failed `Start`** stops what it started with a context of its own (bounded by the shutdown timeout) instead of the `Start` context, which has usually ended by then.
- **The context a module's `Start` receives** is now derived from the one passed to `Start`. It stays live while the application runs, as before, and ends once `Stop` has finished.

See [`Start`](../api/framework.md#start) and [`Stop`](../api/framework.md#stop).

### A message that cannot be unmarshaled no longer costs its batch (v0.2.17)

*v0.2.17.* A typed stream consumer or event stream consumer handler (`helper.RegisterTypedStreamConsumerService`, `helper.RegisterTypedEventStreamConsumer`, typed stream consumer definitions) was not called when one message of its batch failed to unmarshal. Nothing was acknowledged, and after `MaxDeliver` deliveries the valid messages of the batch were given up on together with it.

- **The handler now gets the other messages**, with `payloads[i]` belonging to `msgs[i]`. It can be called with fewer messages than the fetch batch size, and is not called when none is left.
- **The message that failed is left as it was**: not acknowledged and not terminated, so it stays in the stream and is redelivered after `AckWait`, until `MaxDeliver`. The error logged for the batch wraps `helper.ErrUndecodableMessage`.

### Other fixes in v0.2.17

- **`audit.VerifyChain`** accepts the untampered log of a normal run; it rejected every log that held a clean `module.stopped` entry. Existing logs verify without being rewritten. See [Verifying Audit Log Integrity](../middleware/audit.md#verifying-audit-log-integrity).
- **fs-jetstream:** a `Put` whose context is cancelled or times out no longer leaves its chunks in the bucket, where they counted towards `MaxBytes` and could not be removed. Chunks left behind by earlier versions are not removed automatically. For a deadline that passes during the upload `Put` now returns `context.DeadlineExceeded` instead of `nats: timeout`. See [Uploads That Are Given Up](../plugins/fs-jetstream.md#uploads-that-are-given-up).

### A `Stop` that ignores its context no longer holds the shutdown (v0.2.18)

*v0.2.18.* The `Stop` of a module, middleware module or plugin that did not return kept the application's `Stop` blocked past the shutdown timeout: the modules after it were not stopped and NATS never was.

- **Such a `Stop` is now abandoned** 250ms after its context ended. The `Stop`s called after the context had already ended share one second before that. It is left running in its goroutine, `Stop` goes on with the other modules and NATS, and returns an error wrapping `monoerrors.ErrStopAbandoned` that names the module.
- **A `Stop` that honours its context is not affected**: it returns when the context ends, and its own error is reported.
- **`WrapModuleStopFailed` errors now wrap the `Stop`'s own error**, so `errors.Is(err, context.DeadlineExceeded)` and the like find it; the message is unchanged.

See [`Stop`](../api/framework.md#stop).

### A `Start` retried after a failed one works (v0.2.18)

*v0.2.18.* After a failed `Start` the application returns to the created state and `Start` may be called again, but what the failed attempt had registered stayed:

- **Events:** with an `EventEmitterModule` every retry failed with `event ... already registered`. Without one, event consumers were registered a second time: a second subscription in the queue group and a second durable (`...-2`), so a stored event was handled once per attempt. The event registry is now put back as `Start` found it.
- **Channel services:** the routers of the failed attempt kept running and took replies meant for the consumers of the retry. They now end with the attempt.
- **Audit middleware:** every retry failed with `audit module already started`. **Access log middleware:** it started but logged nothing.
- **A `Start` while an abandoned `Stop` is still running** (see above) fails with `monoerrors.ErrStopAbandoned`.

The built-in middleware keep their log open through the rollback and close it when the application stops, whether or not it was started again. Custom middleware whose `Stop` gives up something it cannot get back can implement [`types.RollbackAwareMiddleware`](../middleware/README.md#middleware-lifecycle), as the built-in ones do.

### Audit middleware: failed writes, `Stop` and large saves (v0.2.18)

*v0.2.18.*

- **A write that fails is reported.** `SaveAuditTrail` counted an entry as saved when the write to the log had failed, and the hash chain had already advanced over it, so every later entry failed `audit.VerifyChain`. The entry is now not counted, the error is returned, and the chain continues from the last entry that is in the log.
- **`Stop` writes what was accepted.** Entries sent with `AsyncSaveAuditTrail` right before `Stop` were dropped (2 to 4 of 100 were written), and a save after `Stop` panicked on a closed channel. `Stop` now writes the accepted entries, and a save after `Stop` does not panic: `SaveAuditTrail` returns an error when its context ends, so give it a context with a deadline.
- **`SaveAuditTrail` with many entries completes.** With more than about 200 entries it lost confirmations and waited until its context ended.
- **On a context that ends**, `SaveAuditTrail` returns the number of entries confirmed so far; it returned 0 before. The confirmations that arrive afterwards are no longer counted by the next call, which could report entries as saved before they were written.
- **`Stop` with a context that has already ended** (a module before the middleware used up the time) still writes the accepted entries and closes the log; the access log is flushed and closed in that case as well. Both gave up at once before.
- **After `Stop` closed the log file**, `Start` on the same module fails with `audit.ErrOutputClosed` (access log: `accesslog.ErrOutputClosed`). Build a new module with `New` for a new application.

See [Audit Middleware](../middleware/audit.md#custom-audit-entries).

### Request IDs from message headers are sanitised (v0.2.18)

*v0.2.18.* The request ID header is written by the sender of a message, which includes any client that can reach the NATS port. The access log wrote it into every line as it came: a value with spaces forged fields of a text line (`x module=billing status=success`), control characters reached the terminal, and a 900 KB value made a 900 KB line. The request ID middleware put the same value into the handler's context and into every nested call.

- **Both middleware now keep at most 128 bytes** of ASCII letters, digits and `-_.:/=+@~`, and replace every other byte with `_`. UUIDs, ULIDs, trace IDs and base64 values are unchanged.
- **An ID with other characters changes** (a space or a non-ASCII letter becomes `_`), in the context, in nested calls and in the log alike.

See [Request ID Middleware](../middleware/requestid.md#how-it-works).

### Other fixes in v0.2.18

- **Cron:** a handler that ran longer than 30 seconds had its occurrence delivered again while it was still running: to another instance at once, so the job ran twice at the same time. The framework now tells the server every 10 seconds that the occurrence is in progress. See [`RegisterCronService`](../api/container.md#registercronservice).
- **A consumer deleted under a running application** (`nats consumer rm`, or a consumer the server removes after its inactivity threshold) left its fetch loop logging a `no responders` warning every 10 seconds, as for a failure that clears by itself. It is now logged once at Error (`Consumer no longer exists`), with a reminder every 5 minutes: the instance consumes nothing from it until it is restarted or the consumer exists again. See [Changing a Consumer Setting That Cannot Be Changed](../api/container.md#changing-a-consumer-setting-that-cannot-be-changed).
- **kv-jetstream:** with `RecreateOnStorageMismatch`, a bucket whose name starts with `KV_` (say `KV_x`) was backed up from the bucket `x` and then deleted without a copy of its own keys. The backup now copies the bucket's own stream, and a backup that copies another stream is never accepted.
- **kv-jetstream and fs-jetstream:** a backup bucket left attached by a recreate that was interrupted kept copying everything the recreated bucket received. Such backups are detached at the next recreate (they are kept, and logged at Warn).
- **kv-jetstream:** `Keys` returned a shorter list and a nil error when its context ended during the listing. It now returns the context's error.
- **AutoTLS:** one connection held open on the ACME challenge port (a request whose body never arrives) blocked `Stop` until the client went away. The challenge server now limits reading, writing and idle connections, and is closed after 2 seconds of shutdown.

### Request-reply: a caller always gets a reply (v0.2.19)

*v0.2.19.*

- **A handler that returns `(nil, nil)`** now answers with an empty reply. Before, nothing was sent and the caller waited for its timeout. A caller that relied on that timeout to mean "no result" now gets an empty response at once, and a typed call (`def.Call`, `helper.CallRequestReplyService`) fails to decode it (`unexpected end of JSON input`) instead of timing out: return a value from such a handler.
- **A response that cannot be sent** (larger than the server's maximum payload) is answered with an error of type `reply-failed`, which the caller gets as `monoerrors.ErrReplyFailed`, and is counted as an error in the service statistics. Before, the caller waited for its timeout.
- **A panic** is reported to the caller as `handler panicked`; the value the handler panicked with was sent to the caller in `Mono-Error-Message` and shown in the `last_error` of the service statistics. It is in the log only now.
- **A handler error type named like a framework type** (`OverloadedError`) is sent as `overloaded-error`. Before, it arrived as `ErrServiceOverloaded`, which means "not handled, retry", although the handler had run.
- **A message without a reply subject** on a request-reply subject is still handled, but it no longer waits for the worker pool: when the pool is full it is dropped (and counted with the rejected messages) instead of holding up the calls behind it. Its warning is rate-limited.
- **Timeout errors** report the time waited instead of the client's default of 30 seconds.

See [A Request Always Gets a Reply](error-handling.md#a-request-always-gets-a-reply).

### Stricter checks that can fail an existing configuration (v0.2.19)

*v0.2.19.*

- **AutoTLS with `allow_non_tls` in a config file** fails `NewMonoApplication`. The setting let clients connect and publish in plaintext although AutoTLS makes TLS mandatory; `WithNATSClientTLS` already rejected it.
- **Strict module boundaries** reject a subscription whose first token is a wildcard and that reaches service subjects (`>`, `*.>`, `*.billing.charge`); without `WithStrictModuleBoundaries` it is recorded as a `foreign-service-subscription`. Such a subscription received other modules' requests and replies unnoticed.
- **An event stream consumer with a negative `Fetch.BatchSize` or `Fetch.Timeout`** is rejected at registration. A negative batch size crashed the process in the fetch loop.
- **`RecreateOnStorageMismatch`:** a backup that holds fewer messages than the bucket fails `Start` instead of being accepted (see the [plugin page](../plugins/kv-jetstream.md#changing-a-buckets-storage-type)). A kv-jetstream backup now keeps as many versions of a key as the bucket's stream does, also when that is more than a KV bucket can be created with (64) or all of them; before, the older versions were left out of the backup.
- **`WithNATSClientTLS` with a route CA in a config file's `cluster { tls {} }` block:** a route CA that is signed by the client CA is now found and refused, as it is with `WithNATSClusterTLS`.
- **`monotest.CheckImports`** refuses a pattern that is empty or starts with `-`.
- **Audit log with hash chaining:** an entry whose event type, module name or service name contains `|` is not written (`audit.ErrAmbiguousEntry`; the caller of `SaveAuditTrail` gets the error), and `VerifyChain` reports a log that holds one. The hash joins its fields with `|`, so such an entry had the hash of another one (module `billing|refund` with service `x`, and module `billing` with service `refund|x`), and text could be moved between fields of a log without breaking the chain. No hash changes: other logs verify as before, with this version and earlier ones. Details and the user context may contain it. Module and service names cannot (they are kebab-case); an event definition's `Name` is not checked for it, so with hash chaining the `service.registered` entry of a consumer of an event named with `|` is not written and is reported on stderr.
- **`errors.ValidateSubject`** rejects a wildcard as the first token when wildcards are not allowed (`*.a.b` passed), a `>` that is not the last token also in first place, and an empty first token. A custom first token is still not held to kebab-case. The framework's own checks of service and event subjects are unchanged.
- **`SetRuntimeContext` on a module's event bus** is the boundary violation `runtime-context-replaced`. The runtime context is the framework's: every handler of every module receives it. Without `WithStrictModuleBoundaries` the call is reported and still goes through; with it the call does nothing, which is logged once at Warn.

### Other fixes in v0.2.19

- **A NATS server that cannot start says why.** With the port in use, `NewMonoApplication` waited the whole startup timeout and returned `NATS server not ready`. It now returns at once with the server's reason (`Error listening on port: ... address already in use`), which is also logged.
- **IPv6:** `WithNATSHost("::1")` works; the framework's client URL and `ServerInfo` put the address in brackets.
- **Config file errors** no longer quote a route URL that fails to parse, which included its password.
- **`EventBus.ChanSubscribe`** left one goroutine behind for every subscription; it now ends with the subscription. The channel is not closed (it never was). Messages delivered before an `Unsubscribe` or `Drain` are still handed over, but one that is not taken within a second is dropped with the rest; keep reading until the channel is empty.
- **kv-jetstream:** in a bucket with one replica, `Delete` of a key that does not exist no longer writes a delete marker (it costs one read per delete; a replicated bucket is deleted from as before). A `Reset` that fails no longer leaves goroutines behind, and one cut short by its context returns the context's error.
- **Access log:** a request whose handler panics is logged, with status `error`. Two modules with a channel service of the same name no longer share one proxy entry (the first one's goroutines were never stopped).
- **`pkg/certreload`:** the certificate files are read without holding the reloader's lock, so a file system that stalls no longer blocks every TLS handshake, and a certificate that expired without being renewed is warned about.
- **Channel services:** a nil message on a provider's out channel is dropped with a warning; it crashed the process.
- **Locks:** middleware hooks no longer run with the service container's lock held, and a module's `SetDependencyServiceContainer` no longer runs under the lifecycle lock; code in them that asked the framework something could deadlock.
- **Audit log:** an entry with text that is not valid UTF-8 (a user context taken from a request header) is hashed as it is written, with replacement characters. Before, it never verified, and neither did the rest of its log. `VerifyChain` also rejects an entry whose details cannot be hashed instead of treating them as empty.
- **`RecreateOnStorageMismatch` in a cluster:** whether a backup that ends short of the bucket's last sequence is complete is asked of the bucket's leader. It was read with a request any replica answers, and a replica that was behind could have said that nothing was left. While the bucket has no leader the question is asked again, until the backup's copy timeout.
- **CI and dependencies:** the issue triage workflow reads a new issue as data and may only label and comment on that issue; actions are pinned by commit; `golang.org/x/net` is v0.62.0 and `golang.org/x/crypto` v0.59.0.

### Cron: an unchanged schedule is not published again (v0.2.20)

*v0.2.20.* The schedule of a cron service was published at every `Start`. The server counts an interval from the time of that message, so every start of any instance began an `@every` interval anew.

- **What went wrong:** an interval longer than the time between two starts never fired: `@every 24h` in an application that is deployed daily, or any interval in a cluster where an instance starts more often than that (a rolling deploy, scale-out, one instance restarting in a loop). `Start` succeeded and nothing was logged. Cron expressions and aliases (`@daily`) were not affected.
- **Now:** the stored schedule is read first (two requests per cron service and start) and published only when it is missing or differs. A changed schedule is published as before, and its interval starts from the deploy.
- **During a rolling upgrade** an instance of an earlier version still publishes at every start. The interval is safe once every instance runs v0.2.20.
- **One-shot schedules** (`@at <time>`) fire once. Before, one whose time had passed fired at every start of every instance. The framework records the schedule it published on `services.<module>.<service>.one-shot`, a subject that is now reserved like `.schedule` and `.control`; changing the time schedules the job once more.

### Events with a reply subject are no longer rejected (v0.2.20)

*v0.2.20.* An event published with `EventStream().Publish` carries the inbox of its publish acknowledgement as reply subject. A core event consumer (`RegisterEventConsumer`) whose worker pool was full treated it as a request.

- **What went wrong:** the event was dropped for that consumer, and an overloaded error was sent to the publisher's inbox, where it could arrive before the server's acknowledgement: `Publish` then failed with `nats: invalid jetstream publish response` although the stream had stored the event. A publisher that retried without a message ID stored it twice.
- **Now:** an event consumer always waits for room in its pool and never answers, whatever the message carries. A burst is held back in the client buffer, as it is for events without a reply subject.
- **Who is affected:** applications that publish events through JetStream and consume the same subject with a core event consumer. Check `Rejected` of the event consumer in `SubscriptionStats()` or the warning `Messages rejected: worker pool and pending queue are full` in earlier logs.

### fs-jetstream: a rejected upload no longer replaces the object (v0.2.20)

*v0.2.20.* In a bucket with `MaxBytes`, a `Put` the bucket had no room for returned `maximum bytes exceeded` and could still replace the object stored under that name with metadata of chunks that were never stored.

- **Now:** the plugin publishes an upload itself and writes the metadata only after every chunk is stored. A failed upload removes its chunks and leaves the object as it was.
- **Objects damaged before** are not repaired: `Stat` returns them and `Get` times out. Upload them again or delete them; see [Objects damaged by a rejected upload](../plugins/fs-jetstream.md#objects-damaged-by-a-rejected-upload).
- **Code that builds a backend itself:** `fsjetstream.NewJetStreamBackend` still uploads through the object store client. Use `fsjetstream.NewJetStreamBackendWithJetStream`, as the plugin does.

### Other fixes in v0.2.21

- **Access log channel proxy shutdown & `ModuleStoppingEvent`:** the lifecycle now emits `types.ModuleStoppingEvent` (`"module.stopping"`) before each module's `Stop()` executes (both during normal shutdown and during rollback). The access log middleware intercepts this event to cancel and wait for that module's channel proxies so background goroutines return before the module closes its channels. Third-party middleware switching over all lifecycle types will observe this new event type. Channel services of a `middleware` module registered after the access log, and providers that close their `in` channel on runtime-context cancellation (before modules stop), don't get `module.stopping`, so they are not covered.

### A NATS configuration reload is refused (v0.2.22)

*v0.2.22.* With `WithNATSConfigFile`, the embedded server accepted a configuration reload (`nats server config reload`, a request from the system account user). It parsed the file again and applied it on its own, so one reload switched off what the options had added: JetStream (requests timed out), client TLS or AutoTLS (plaintext clients were let in), route authorization and route TLS, and the system account user. Nothing was logged.

- **Now:** the server is not given the file's path and answers a reload with "can only reload config when a file is provided using -c or --config". The configuration takes effect at start only; restart the application to change it.
- **nats-server's own start-up line** `Using configuration file: <path>` (shown only with server logging on) is no longer written; the framework logs the path at start (`NATS config file loaded`).

### A queue group service with several queue groups never rejects a message (v0.2.23)

*v0.2.23.* Every queue group of a service (each `QGHP` pair) receives every message, and the framework of each group answers the same `Send` with its ACK. `Send` keeps the first answer. A group whose worker pool was full answered with an overloaded error instead.

- **What went wrong:** `Send` returned `ErrServiceOverloaded` for a message another group had handled, so a retry handled it there twice. It also returned `nil` for a message the full group had dropped.
- **Now:** with more than one queue group, a full group sends its ACK at once and handles the message once it has room. The subscription waits meanwhile, so that group's backlog builds up in its NATS client buffer. `Send` to such a service no longer returns `ErrServiceOverloaded`; it still returns `ErrServiceUnavailable` when no instance subscribes. When every group is full, `Send` may time out instead, and the message is still handled later.
- **Unchanged:** a service with one queue group still rejects when its pool is full. The handler still never acknowledges a queue group message: the framework ACKs it automatically, before the handler runs (see [Who Acknowledges a Message](../api/container.md#who-acknowledges-a-message)).
- **During a rolling upgrade** an instance of an earlier version still rejects. The fix is complete once every instance runs v0.2.23.

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
| [fs-jetstream `BucketConfig.RecreateOnStorageMismatch`](../plugins/fs-jetstream.md#changing-a-buckets-storage-type), `fsjetstream.ErrStorageMismatch` | v0.2.11 | The same for object store buckets: a bucket whose stream exists with another storage type fails `Start` with a clear error, or, with the opt-in, is recreated: **the bucket is emptied**, after its objects are copied to a backup bucket (kept 7 days by default; `SkipBackupOnRecreate`, `RecreateBackupTTL`, `RecreateBackupTimeout`, default 10 minutes). Set it only for a bucket whose contents can be lost. `WithLogger(nil)` is now rejected, and without `WithLogger` the logger is resolved at `Start` |
| [`monoerrors.ErrStreamConfigImmutable`](../api/container.md#changing-a-streams-storage-type-or-retention) | v0.2.12 | A stream consumer service or event stream consumer whose `Storage`, or `Retention` to or from work-queue, differs from the existing stream's fails `Start` with a clear error instead of the server's generic one. It failed before as well; the stream is never deleted. Two event stream consumers that register one stream with different values now fail before the stream is created |
| [`monoerrors.ErrWorkQueueConsumerConflict`](../api/container.md#work-queue-streams-one-consumer-per-subject), [`monoerrors.ErrStreamSubjectsOverlap`](../api/container.md#renaming-a-stream-or-switching-a-service-between-stream-consumer-and-cron) | v0.2.16 | `Start` errors for a second consumer on the same subjects of a work-queue stream, and for a stream that asks for subjects another stream holds. Both failed before as well, with the server's error; nothing is deleted |
| `helper.DecodeBatch`, `helper.ErrUndecodableMessage` | v0.2.17 | Decode a batch leaving out the messages that fail to unmarshal; the error the typed batch handlers return for such a batch |
| `audit.CanonicalDetails`, `audit.HashEntry`, `audit.LegacyEmptyDetailsHash` | v0.2.17 | The form an audit entry's details are hashed in, and the hash earlier versions gave an entry with empty details |
| `types.ModuleStoppingEvent` (`module.stopping`) | v0.2.21 | Lifecycle event emitted by the lifecycle manager before a module's `Stop()` executes (during shutdown and rollback) |
| `fsjetstream.NewJetStreamBackendWithJetStream`, `UploadObject`, `ErrUploadAckTimeout`, `ErrUploadOutOfOrder` | v0.2.20 | A backend whose uploads write the metadata only after every chunk is stored, the upload itself, and the errors of an upload whose chunk was not acknowledged within `UploadAckTimeout` (10s) or whose chunks were not stored in the order they were read |
| `types.IsOneShotCronSchedule`, `types.CronOneShotSubject` | v0.2.20 | Whether a cron schedule fires once (`@at`), and the subject on which the framework records a one-shot schedule it has published |
| `monoerrors.ErrReplyFailed`, `types.ErrorTypeReplyFailed` (`reply-failed`) | v0.2.19 | The error a request-reply caller gets when the handler ran but its response could not be sent, and its type on the wire |
| `types.IsFrameworkErrorType`, `types.HandlerPanicMessage` | v0.2.19 | Whether a `Mono-Error-Type` is one the framework itself answers with, and the message it sends for a handler that panicked |
| `audit.ErrAmbiguousEntry`, `audit.CheckHashable`, `audit.SaveFailure` | v0.2.19 | The error for an audit entry whose event type, module name or service name contains `|`, the check for it, and the error type `SaveAuditTrail` returns for an entry that was not saved (`errors.Is` finds `ErrAmbiguousEntry` through it) |
| `types.BoundaryViolationRuntimeContextReplaced` (`runtime-context-replaced`) | v0.2.19 | The boundary violation of a module that calls `SetRuntimeContext` on its event bus |
| `certreload.ExpiryWarnInterval` | v0.2.19 | The least time between two warnings that the certificate being served has expired |
| `monoerrors.ErrStopAbandoned` | v0.2.18 | The error `Stop` returns for a module, middleware module or plugin whose `Stop` did not return after its context ended, and `Start` while such a `Stop` is still running |
| [`types.RollbackAwareMiddleware`](../middleware/README.md#middleware-lifecycle) | v0.2.18 | Optional `RollbackStart(ctx)` of a middleware module, called instead of `Stop` when a failed `Start` is rolled back, so the middleware can be started again |
| `audit.ErrOutputClosed`, `accesslog.ErrOutputClosed` | v0.2.18 | `Start` of a module whose log `Stop` has closed |
| `audit.HashChain.Next`, `Commit`, `audit.ConfirmationError`, `audit.CallHeader` | v0.2.18 | Compute the next entry of a chain and advance the chain once it is written; the error a confirmation of the audit-trail service reports; the header that ties a confirmation to its `SaveAuditTrail` call |
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
