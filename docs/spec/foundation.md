# Foundation Specification

## Project Overview

The Monolith-Framework is a Go framework for building modular monolith applications centered around an embedded NATS.io message queue system. Key features:

- Modular design within a single deployable unit (monolith with module boundaries)
- Embedded NATS server with JetStream persistence and clustering support
- Event-driven inter-module communication via publish/subscribe patterns
- Dependency injection container for managing module dependencies
- Lifecycle management with automatic dependency resolution

## Core Principles

These are non-negotiable principles that guide all decisions. Treat this as a constitution for this project:

- **Modular Monolith Architecture**: The framework must support modular design within a monolith deployment model, avoiding microservices complexity while maintaining clear module boundaries. Modules should be independently developable and testable while running in a single process.

- **Developer Experience First**: The framework must prioritize ease of use, clear APIs, and comprehensive documentation. Developers should quickly understand concepts and build applications without fighting the framework.

- **Built-in Message Queue System**: NATS.io is a native, embedded component—not optional. It's the core communication mechanism for inter-module messaging and event-driven architecture. Modules communicate through NATS, not direct calls.

- **Simplicity over Complexity**: APIs should be intuitive and follow Go conventions. Prefer composition over inheritance. Configuration and behavior must be explicit and discoverable.

- **Fail Fast**: Validate configuration and dependencies early during initialization. Surface errors immediately rather than failing silently later.

### Architecture Constraints

- **Go-Only**: The framework and all components must be written entirely in Go. No language interop.

- **Minimal Dependencies**: Keep external dependencies to the absolute minimum. Core dependencies are limited to NATS.io and Go standard library (slog, context, sync, os/signal).

- **Standard Tooling**: The framework must work seamlessly with standard Go tools: go mod, go test, go build, go vet, go doc. No special build tools required.

- **No Secrets in Code**: Sensitive information must never be hardcoded. Modules load secrets from environment variables or secret management systems.

### Testing Approaches

- **Unit Tests for All Public APIs**: Every public function, method, and interface must have comprehensive unit tests covering happy paths, error cases, and edge conditions.

- **Integration Tests for Module Interactions**: Integration tests verify module interactions through NATS and other integration points, validating end-to-end scenarios including message publishing, subscription, and error handling.

- **Example-Driven Documentation**: Runnable examples serve as both documentation and verification that examples work correctly.

## Architecture

### Package Structure

The framework follows a layered architecture with clear separation:

- **Core Layer**: Framework initialization, configuration, and lifecycle management
- **Module Layer**: Module interface, registration, and dependency resolution
- **Messaging Layer**: NATS server management, pub/sub, and queue subscriptions
- **Infrastructure Layer**: Logging, health checks, and cross-cutting concerns

### Core Interfaces

The framework exposes interfaces for extensibility while returning concrete implementations:

- **MonoFramework**: Main entry point coordinating initialization, module registration, and lifecycle
- **Module**: Base interface all modules implement (Name, Start, Stop)
- **EventBus**: Messaging abstraction for publish/subscribe patterns
- **ServiceContainer**: Dependency injection with four service types (Channel, RequestReply, QueueGroup, StreamConsumer)
- **EventRegistry**: Manages event definitions and consumer registrations
- **Logger**: Structured logging with module-specific context

### Service Communication Patterns

Four distinct patterns for inter-module communication:

- **Channel Services**: Go channels for in-process bidirectional communication. Lowest latency (~microseconds), single process only. Module creates channels and handles in a goroutine.

- **Request-Reply Services**: NATS request/reply for synchronous inter-module calls. Supports distribution, ~1ms overhead. Subject pattern: `services.<module>.<service>`.

- **Queue Group Services**: NATS queue subscriptions for async load-balanced processing. Horizontal scaling, fire-and-forget semantics. Same subject pattern with explicit queue group.

- **Stream Consumer Services**: JetStream durable pull consumers for persistent messaging. Supports batch processing, message acknowledgment (ack/nack), and replay from stream position. Used when message durability and at-least-once delivery are required.

- **Cron Services**: Server-side cron-scheduled handlers backed by the embedded NATS JetStream message scheduler (nats-server v2.14+, ADR-51). The schedule is registered server-side, so in a multi-node cluster exactly one message fires per occurrence — no client-side ticker and no leader election. Each occurrence is delivered through a durable pull consumer with explicit acknowledgement (at-least-once). Registered via `RegisterCronService(name, CronServiceConfig, CronHandler)`; requires JetStream. See "Cron Service Design" below.

### Cron Service Design

- **Subjects**: cron stays within the standard service subject tree. The schedule message is stored on the `services.<module>.<service>.schedule` sub-subject and purges go to `services.<module>.<service>.control`; the server republishes the payload to the concrete target service subject `services.<module>.<service>` on every occurrence. The durable consumer filters on the concrete target subject, so the schedule/control sub-subjects are never delivered to the handler. The `.schedule` and `.control` sub-topic suffixes are reserved for cron services.
- **Stream**: one JetStream stream per cron service, `MONO_CRON_<module>_<service>`, listening on `services.<module>.<service>` plus `services.<module>.<service>.>`. Enabling `AllowMsgSchedules` makes the server implicitly enable `AllowRollup` (and clear `DenyPurge`), so the framework does not set them explicitly. The schedule rolls up by subject (one schedule per subject), making the startup re-publish idempotent: changing `Schedule`/`Payload`/`TimeZone`/`TTL` and redeploying overwrites the live schedule in place.
- **Durable consumer**: each cron service is consumed through the durable `<module>-<service>-cron`, sent with both `Name` and `Durable` so the server keeps it, and its position, across restarts and idle periods. The stream keeps up to 256 delivered occurrences per service (limits retention, no `MaxAge`), so the consumer position is what stops them from being delivered again. The consumer is provisioned before the schedule is published.
  - **Existing consumer:** updated in place with the deliver policy and start position it already has (both are immutable server-side). Its acknowledgement state is kept, so an occurrence that fired while the application was down is delivered, and handled occurrences are not delivered again. Delivery stays at-least-once: an occurrence whose acknowledgement was lost at shutdown is delivered again.
  - **Missing consumer:** created with deliver policy all, then reset to just after the stream's last sequence, so the occurrences already stored are skipped. They are history: delivering them would run the job once per stored occurrence. An occurrence stored while no consumer existed, before the start was taken, is skipped as well. Their number is logged once as a warning. The create itself records the start sequence in the consumer metadata entry `mono.cron-start-sequence`, which is removed once the consumer is positioned. A start interrupted before that finishes the reset on the next start instead of delivering the stored occurrences, and a later reset by an operator is not undone. When several instances start at once, the create is create-only, so only one instance creates the consumer and the others update it. The position is re-read right before the reset, so a consumer that is already positioned is left where it is. Deliver policy all is kept because it is what earlier versions send, so rolling back to one of them still starts.
  - **Up to v0.0.12** the consumer was sent with a `Name` only, which the server treats as non-durable and deletes after about 5s with no pull request and nothing awaiting acknowledgement. Shutdown stops the fetch loops before modules and stops the embedded server last, so a shutdown slower than that (or a handler running longer than `AckWait` plus 5s) deleted it, and the next start recreated it with deliver-all and replayed up to 256 occurrences per service. Such a consumer is converted in place with two updates: the server still applies the non-durable inactivity threshold on the update that sets `Durable`, and the second one clears it. A rollback to v0.0.12 turns the consumer back into a non-durable one.
- **Acknowledgement is framework-owned** (unlike Stream Consumer services, where the handler owns ack): the framework Acks the occurrence when the handler returns nil and Naks it (redelivery up to `MaxDeliver`) on a non-nil error or a recovered panic. The handler must not call `Ack`/`Nak` itself.
- **Schedule formats**: cron expression in the standard five-field format (`"0 0 * * *"` = daily at midnight) or the six-field seconds-first format the NATS scheduler natively understands (`"0 0 0 * * *"`) — five-field expressions are normalized by prepending a `"0"` seconds field (`types.NormalizeCronSchedule`) — alias (`"@daily"`, `"@hourly"`, …), or interval (`"@every 5m"`, minimum 1s). `TimeZone` applies only to cron expressions (not `@every`/`@at`). `SourceSubject` (mutually exclusive with `Payload`) delivers the last message seen on a subject instead of a static payload.
- **Retiring a cron service (two-phase)**: set `Deprecated: true` and deploy — the framework purges the server-side schedule (publishing a cancel to `services.<module>.<service>.control`) and does not start the consumer, while keeping the registration code. In a later release, remove the `RegisterCronService` call. Removing the call without first deprecating leaves an orphaned durable schedule; the framework logs a warning on startup when it detects a `MONO_CRON_*` stream with no matching registration (it never auto-deletes).
- **Middleware**: cron handlers are not currently wrapped by the middleware chain (access-log, audit, request-id).

### Handler Concurrency (Worker Pools)

- **Why**: nats.go delivers the messages of one asynchronous subscription to its callback one at a time, on a single goroutine per subscription. Request-reply services, queue group services and event consumers are each one `QueueSubscribe`, so without a pool one slow handler call (a blocking HTTP call, a slow query, a lock wait) delays every later message on the same subscription, adding instances helps only per message rather than per free worker, and the backlog sits invisibly in the client's pending buffer.
- **Design**: the subscription callback only copies the message and hands it to a **bounded worker pool per subscription** (`internal/workerpool`); a worker goroutine runs the handler. Workers are started on demand up to `MaxConcurrency` and exit when idle, so an idle pool holds no goroutines. Pending jobs wait in a FIFO queue bounded by `MaxPending`; with `MaxConcurrency: 1` the pool runs handlers strictly in arrival order (the behaviour of versions before worker pools). All pool state sits behind one mutex so a job is never queued without a worker to run it.
- **Options**: `types.ServiceOptions{MaxConcurrency, MaxPending}`; zero means the default for the kind of subscription (below), negative is rejected at registration (fail fast). Middleware written before the options existed rebuilds registrations and `QGHP` pairs without them, so `applyServiceRegistration` keeps the original options when a registration or a same-named pair comes back with zero options. Set per registration with `RegisterRequestReplyService(name, handler, WithMaxConcurrency(n), WithMaxPending(n))`, `QGHP.Options` (one pool per queue group), and `EventRegistry.RegisterEventConsumerWithOptions(def, handler, module, EventConsumerOptions{QueueGroup, ServiceOptions})`. The framework-wide default is `WithDefaultServiceConcurrency(n)`; without it `MaxConcurrency` defaults to `runtime.GOMAXPROCS(0)*4` and `MaxPending` to `16 × MaxConcurrency` with a floor of 256. The pending default is deliberately generous: before worker pools a burst simply queued in the NATS client buffer and callers never saw an overload error, so the default absorbs bursts first and sheds load only once a backlog has clearly built up. A count bound cannot bound wait time: time-based shedding belongs to deadline propagation (issue #20).
- **Defaults differ by kind.** Request-reply services are **pooled by default**: each request is independent, so a zero `MaxConcurrency` means the framework default. Queue group services and event consumers are **serial by default**: a zero `MaxConcurrency` means 1, so a handler keeps seeing its messages one at a time in arrival order, exactly as before worker pools existed (downstream consumers that count, sum or apply state transitions in order are not affected). Opt in per registration by setting `MaxConcurrency` above 1. `WithDefaultServiceConcurrency` therefore applies to request-reply only; `WithDefaultServiceConcurrency(1)` restores the fully serial behaviour of earlier versions.
- **Overload**: when the pool and its queue are both full, a message that carries a reply subject (a request-reply call, or a queue group send awaiting its ACK) is answered immediately with `Mono-Error: true`, `Mono-Error-Type: overloaded`; the client maps it to a `ServiceError` wrapping `errors.ErrServiceOverloaded`, so the caller fails fast and can retry (with several instances the retry usually lands elsewhere). A message without a reply subject (an event, or a queue group send in optimistic publish mode) cannot be answered, so it is **not rejected**: the subscription callback blocks until a queue slot frees, which applies back-pressure through the NATS client's pending buffer (should that buffer's own limits ever be reached, nats.go reports a slow consumer). Rejections are logged at most once per second per subscription, with the count.
- **Queue group ACK** is sent after the pool accepted the message, so an ACK means "accepted for processing" and a rejection is distinguishable from it. The worker waits for the ACK outcome before running the handler: if the ACK publish itself fails, the handler is skipped, because the sender will time out and re-send and the message must not be handled twice (the behaviour of earlier versions, which ACKed before calling the handler and returned on failure).
- **Shutdown**: see "Graceful Shutdown" below. A request delivered after the pools closed is answered with `Mono-Error-Type: shutting-down` (mapped to `ErrServiceUnavailable`); an event still buffered at that point is dropped with a rate-limited warning.
- **Out of scope**: stream consumer, event stream consumer and cron services are pull-based loops and are not pooled.

### Request Deadlines

- **Why**: nats.go applies a request context only locally, so the caller's deadline never reached the handler. A request that waited in a queue was still processed after the caller had given up (wasted work that slowed the queue further), and handlers had no sensible timeout to pass to their own downstream calls.
- **Wire format**: request-reply and queue group clients copy the context deadline into two headers before the outgoing middleware runs: `Mono-Deadline` (absolute deadline, Unix time in milliseconds; exact across instances when clocks are in sync) and `Mono-Timeout` (time remaining when sent, in milliseconds, like gRPC's `grpc-timeout`). Request-reply clients always have a deadline (the client default of 30s applies when the caller's context has none), as do queue group sends in ACK mode (5s default); a queue group send in optimistic publish mode carries the headers only when the caller's context has a deadline. Middleware may read or overwrite them; removing both opts a request out of server-side expiry. Polyglot clients that set them get the same treatment.
- **Receive (request-reply only)**: the subscription callback records the arrival time; the pool worker, right before invoking the handler (so queue wait counts), derives the deadline. `Mono-Deadline` is used when it is plausible (within 24h of arrival, which rejects values sent in seconds or nanoseconds) and, if `Mono-Timeout` is also present, agrees with `arrival+timeout` within the **clock-skew allowance** (`WithDeadlineSkewAllowance`, default 250ms); otherwise the clock-independent `arrival+timeout` is used (clamped to 24h, so it cannot overflow), with a rate-limited warning when the two disagreed (which also happens when the send-to-arrival delay itself exceeds the allowance, for example on a slow cross-region link). `Mono-Timeout: 0` means no time left. If the deadline passed by more than the allowance, the request is **dropped**: the handler does not run and no reply is sent (the caller is gone). Dropped requests are counted and logged at Debug level, rate-limited to one line per second per subscription (`expired=<count>`). Otherwise the handler runs with `context.WithDeadline(runtimeCtx, deadline)`, the caller's deadline itself (the allowance only widens the drop decision, so a request within the allowance runs with an already expired context), and the derived context is cancelled when the handler returns. A request without the headers behaves as before (no deadline).
- **Queue groups and events are not dropped**: the ACK already promised processing, and events have no caller waiting. Their handlers get the runtime context as before.
- **Nested calls inherit the deadline**: a handler that calls another service with its context sends that context's deadline, unchanged, so nothing accumulates per hop; the client applies its default only when the context has none. The caller's deadline is deliberately **not capped** at the callee's client default (a caller that passes a 2-minute deadline for a long operation keeps it).
- **Behaviour change**: request-reply handlers used to receive the long-lived runtime context; they now receive a context that ends at the caller's deadline and is cancelled when the handler returns. A handler that starts background work with its context must derive from `context.WithoutCancel(ctx)`.
- **Not covered**: the time a request spends in the NATS client buffer before the callback records arrival is counted only through `Mono-Deadline`, not through `Mono-Timeout`.

### Handler Panic Recovery

- **Every framework-dispatched handler call is recovered**: request-reply, queue group, event consumer, stream consumer, event stream consumer and cron handlers are invoked through one set of helpers (`internal/lifecycle/handler_panic.go`) that turn a panic into an error carrying the panic value and the stack trace (`runtime/debug.Stack()`). The subscription or fetch loop keeps running, so one bad message cannot take down the process, the other modules and every in-flight request with it. The error is logged at `Error` level with the module, service or event, subject, `panic=true` and the stack. Callbacks a module subscribes itself through `EventBus.Subscribe`/`QueueSubscribe` are recovered by the event bus as well (logged with subject, queue group and stack; the message is lost, as core NATS has no redelivery).
- **Request-reply**: the caller gets an error reply at once (`Mono-Error: true`, `Mono-Error-Type: handler-panic`) instead of a timeout; clients map it to a `ServiceError` wrapping `errors.ErrHandlerPanic`. The value contains a hyphen so that no user error type name, which the framework lower-cases and strips of its `Error` suffix for the header, can collide with it.
- **Acknowledgement follows the usual ownership**: cron Naks the occurrence (framework-owned ack). Stream and event stream consumers own their acks, so after a panic the framework neither Acks nor Naks the batch: whatever the handler did not acknowledge before panicking is redelivered after `AckWait`, exactly as when the handler returns an error, and `MaxDeliver` bounds a handler that panics deterministically. An immediate Nak would instead redeliver at fetch speed and log a stack trace per attempt.
- **Worker pool last resort**: the pool runs each job under its own recover (`Pool.PanicHandler`), so a panic in the framework's own job code (not the handler) is logged and the pool's accounting stays correct.
- Not provided: an option to keep the crash-and-restart behaviour (`WithPanicOnHandlerPanic` was listed as optional in issue #22).

### Event Consumer Patterns

Two patterns for consuming broadcast events:

- **Event Consumer (NATS Core)**: Fire-and-forget event consumption via standard NATS subscriptions. Low latency, no persistence. Uses `EventConsumerHandler` or `TypedEventConsumerHandler[T]` for type-safe consumption.

- **Event Stream Consumer (JetStream)**: Durable event consumption via JetStream pull consumers. Supports batch processing with `EventStreamConsumerHandler` or `TypedEventStreamConsumerHandler[T]`. Messages require explicit acknowledgment. Used when event replay or at-least-once delivery is needed.

Event emitter modules declare events via `EventEmitterModule.EmitEvents()`. Consumer modules discover and register handlers via `EventConsumerModule.RegisterEventConsumers(registry)`.

### Event Stream Consumer Durable Naming

- **Stable name**: the durable consumer of an event stream consumer is named `<consumer-module>-<event-module>-<event>-<version>` (`types.EventStreamConsumerName`, sanitized by `types.SanitizeConsumerName`, which maps `.` to `-` because JetStream rejects `.` in consumer names). A durable is persistent server-side state, so its name must be derived only from registration identity — never from module start order, a process-local counter, or the set of other modules. A repeat registration of the same event on the same stream by the same module gets a `-2`, `-3`, … suffix in that module's registration order. `Consumer.Name`/`Durable` supplied by the caller are overridden. Both `Name` and `Durable` are sent, so the server keeps the consumer regardless of inactivity (unless the caller sets `InactiveThreshold`); a consumer with only a `Name` is non-durable to the server and is deleted after 5s without pull requests.
- **Legacy durables** (up to v0.0.11): created with a `Name` only (so non-durable to the server) and named `<event-module>-<event>-<version>-<N>` where `N` followed the then non-deterministic module start order, so a restart could rename the durable (a new durable replays the stream; the old one is orphaned and, on an interest-retention stream, retains messages). On startup the framework lists each stream's consumer names once (complete even in a cluster), fetches the info of each name that matches, and treats a pull consumer named `<event-module>-<event>-<version>-<digits>` that is not one of the application's stable names as a legacy durable of that registration.
- **Migration** runs once per stream. The first startup that sets up every event stream consumer on a stream writes the metadata entry `mono.legacy-consumer-migration: done` on it. The framework rewrites the stream configuration on every startup, which replaces its metadata, so it re-applies the marker each time. A marker write that fails is logged and the migration runs again next time, which is safe because every step is idempotent.
  - **Unmarked stream (the upgrade):**
    - Work-queue stream: the legacy durable is deleted *before* the stable one is created, because a work-queue stream accepts only one unfiltered consumer. This is lossless: the server does not purge a work-queue stream when a consumer is deleted, and acknowledged messages are already gone. A consumer configuration the server rejects on a work-queue stream (a deliver policy other than all, or acknowledgement other than explicit) fails startup before anything is deleted.
    - Other streams: the stable durable is created with the caller's configuration unchanged; deliver policy and start sequence are immutable server-side, so they are never altered. The framework then resets it to just after the legacy ack floor, but only when all of these hold:
      - it has not delivered anything and is still before that floor;
      - that floor is unambiguous: exactly one registration in the application uses the legacy name prefix on the stream, and every legacy durable of that prefix that delivered anything stopped at the same ack floor.
    - **Replay over loss:** legacy names do not record which module a durable belonged to. When several registrations share the prefix, or the legacy durables stopped at different positions, a module may have no old position of its own (it never delivered, it is new in this deploy, or the further-ahead durable belonged to a module removed in the same deploy). Resetting it to another module's position would skip messages it never processed. In those cases the stable durable keeps its configured start and the stream's retained messages are delivered again, with a warning. Duplicates are the accepted cost: a non-idempotent handler (for example one that sums payments) double-counts them. An operator who knows which legacy durable is stale can remove it before upgrading, so that a single position remains and is carried over exactly. One case cannot be detected from names: if a single deploy removes the only module consuming an event on a stream and adds a different one, the removed module's legacy position looks unambiguous and the new module starts there. Such a swap should be split across two deploys, or the legacy durable removed first.
    - Legacy durables that never delivered carry no position. Deliver policies other than all/by-start-sequence/by-start-time cannot be repositioned; a warning reports any backlog left behind. A reset rejected because the configured start is later is accepted as is. A transient reset failure (timeout, no responders, consumer not yet created on a replicated stream) is retried and then fails startup; any other failure fails startup at once.
  - **Marked stream (every later startup):** nothing is carried over, deleted or inspected. Leftover legacy consumers are recognized by name alone. This has two effects:
    - a stable durable created later for the same event (a new module, or one an operator deleted to force a replay) starts from its configured deliver policy instead of being fast-forwarded to the legacy position;
    - a leftover legacy consumer that cannot be inspected, for example with its peers offline in a cluster, cannot block startup.
  - Legacy durables on non-work-queue streams are **never deleted** (consistent with the cron "never auto-deletes" rule). Each one is reported with a warning on every startup, including a `nats consumer rm <stream> <name>` command.

### Module Lifecycle

Modules are first-class citizens with a well-defined lifecycle. The initialization sequence per module is strictly ordered:

1. ServiceContainer binding (container associated with module)
2. SetDependencyServiceContainer (provide dependency containers)
3. SetEventBus (provide event bus for pub/sub)
4. RegisterServices (module registers its own services)
5. Start (module performs initialization)
6. NATS subscriptions setup (framework creates subscriptions for registered services)

Shutdown drains subscriptions and waits for in-flight handlers (with a live runtime context) before the runtime context is cancelled and modules are stopped in reverse order. Failed module initialization triggers rollback of previously started modules.

### Subject Naming Conventions

The framework enforces standardized subject naming to prevent conflicts:

- **Service subjects**: `services.<module>.<service>` (computed by ServiceContainer, no wildcards)
- **Event subjects**: `events.<domain>.<event-type>` (supports wildcards for broadcast patterns)
- **Internal framework**: `_mono.<component>.<operation>` (reserved prefix, modules cannot publish here)
- **Rules**: Lowercase with hyphens (kebab-case), no spaces or special characters except `.`, `-`, `*`, `>`

### Module Boundaries (Ports and Adapters)

The framework supports the hexagonal (Ports and Adapters) layout described in `docs/architect/hexagonal-architecture.md`; this section records what it enforces and why.

- **Typed service contracts**: `helper.RequestReplyService[Req, Resp](module, name)`, `helper.QueueGroupService[T]` and `helper.StreamConsumerService[T]` declare a service once — provider, name, kind, payload types, codec (JSON by default) — and both sides use the same value: the provider through `def.Register(container, handler)`, the consumer through `def.Call` / `def.Send` / `def.Publish`. They are the service-side counterpart of `EventDefinition[T]`; their non-generic identity is `types.BaseServiceDefinition{ModuleName, Name, Type}` (`ToBase()`), and `types.ServiceSubject` is the single definition of the `services.<module>.<service>` convention. Constructors panic on a non-kebab-case name, like `helper.EventDefinition`: contracts are package-level variables, so this surfaces at process start. The typed definitions live in `pkg/helper` rather than `pkg/types` because they need `pkg/errors` for validation, which itself imports `pkg/types`. Channel services have no contract type (in-process, already consumer-attributed).
- **Contract package rule**: a provider publishes its port as `<module>/contract` — DTOs, service definitions and the events it emits. A contract package imports only the standard library, `pkg/types`, `pkg/helper` and other modules' contract packages, never its own module package or another module's non-contract package, so consumers import `<provider>/contract` and nothing else of the provider, events need no shared package, and no import cycle can arise. The rule is checked on the import graph by `monotest.CheckImports` (below), because no runtime check can see a module that imports another module's package and calls it directly.
- **Inside a module (convention, not enforced)**: the consumer owns its ports (`ports.go`, interfaces in its own types, not the providers' DTOs), keeps its domain logic in plain Go over them (`service.go`; with `ports.go` it imports only the standard library) and implements the ports in `adapter.go`, which translates to the providers' contracts, so a provider's DTO change stops at the adapter. Publishing events goes behind a port like any other outside call. `module.go` is the `mono.Module`: it wires the adapters behind the ports, builds the domain in `Start` (every dependency container and the bus are set by then) and binds the domain to the module's own contract (the driving adapter). A port over the provider's DTOs with a pass-through adapter, and logic kept in the handler, are the accepted shortcuts for a thin module. The framework cannot observe the inside of a module; what it enforces is the boundary between modules.
- **`types.ServiceLookup`** is the read side of `ServiceContainer` (`Get*`, `Has`), embedded in it so the method set of `ServiceContainer` is unchanged. `Call` / `Send` / `Publish` and `helper.CallRequestReplyService` / `SendQueueGroupService` / `PublishStreamConsumerService` accept it, so a driven adapter stores the container it is handed as a `ServiceLookup` and registering on a dependency becomes a compile error. `SetDependencyServiceContainer` still passes a `ServiceContainer`: narrowing its signature would break every `DependentModule`.
- **`ServiceConsumerModule`** (`ConsumeServices() []BaseServiceDefinition`) declares the contracts a module calls. The lifecycle validates the list at step 1.5 of `startModule`, after the dependency containers are set and before `RegisterServices`: each contract's module must be in `Dependencies()` or be a registered plugin (plugins are injected into every `UsePluginModule` and are outside the dependency graph), the provider must have registered the service (dependencies and plugins start first, so registrations are complete), and the registered type must match. A violation fails the module start (`ErrModuleStartFailed` wrapping `ErrMissingDependency`, `ErrServiceNotFound` or `ErrServiceContractMismatch`) and rolls back as any start failure does. A contract naming the module itself or missing a field is rejected too. The list is **complete**: implementing the interface (even with an empty list) makes a lookup of a request-reply, queue group or stream consumer service of a dependency that is not in it an `undeclared-service-use` violation (module, name and type must match). A module that does not implement the interface is not checked, so the check is opt-in per module. Channel services are not checked (they have no contract type), nor are plugin services (plugin containers are not views).
- **Contract recording**: `def.Register` calls the optional `types.ContractRegistrar.RegisterContract` after the handler is registered; the framework's container stores it in `ServiceEntry.Contract` and rejects a contract whose module or type disagrees with the entry (`ErrServiceContractMismatch`). The field lives on `ServiceEntry` only, not on `ServiceRegistration`: middleware rebuilds the registration and would drop it (the problem `ServiceOptions` had, see "Handler Concurrency"). Containers that do not implement the registrar (test doubles) record nothing.
- **Host escape hatches**: `MonoFramework.Services(name)` and `EventBus(name)` are for the host program (`main.go`, API handlers, tests), not for module code; a module that holds the application object bypasses the checks and cannot be detected.
- **Runtime boundary observation**: each started module gets a boundary recorder (`internal/boundary`) built from its `Dependencies()`, the registered plugin names and its `ConsumeServices()` by `boundary.ForModule`, the one constructor the lifecycle and the `monotest` harness share, so a module is held to the same rules in a test as in the application. The container handed to `SetDependencyServiceContainer` is a **dependency view** (`internal/container/dependency.go`) over the provider's container: every successful `Get*` is attributed to the consumer (one `sync.Map` load and an atomic increment per lookup, about 20ns on top of the raw lookup, no allocation after the first), `GetChannelService` with a `consumerModule` other than the module's own name is a `channel-consumer-mismatch` violation (the consumer channel is then registered under a name that targeted replies do not use; the name passed is reported in the violation's `Subject`), and the call is still delegated unchanged in non-strict mode, and the mutating methods (`Register*`, `Unregister`, `BindModule`, `Set*`, `StartChannelRouters`, `RegisterContract`) are `register-on-dependency` violations. A repeated violation is counted on the same path as a lookup (read lock, atomic increment), since a non-strict module that keeps crossing a boundary does so on every request. An `undeclared-service-use` (see `ServiceConsumerModule`) is checked before the lookup is delegated, with one lock-free map lookup (about 15ns more, only for modules that implement `ServiceConsumerModule`), and reports the service subject in the violation's `Subject` (a repeated one is found by module and service name, so the subject is built once and the path does not allocate); in strict mode the lookup returns `ErrModuleBoundaryViolation` whether or not the service exists. `Entries()` on the view returns **descriptions**: fresh copies with the name, type, subject, module, queue groups, options, stream and cron configuration and contract of each service, without the handlers and channels, so a consumer can inspect a dependency but cannot run or replace the provider's handlers (the host's `MonoFramework.Services(name)` still returns the real entries). The bus handed through `SetEventBus` is a **module-scoped event bus** (`internal/eventbus/scoped.go`): a publish or request to `services.<module>.<service>` of a module that is neither itself, a declared dependency nor a plugin is an `undeclared-service-subject` violation (it bypasses the port), a subscription to another module's service subject is a `foreign-service-subscription` violation, and `events.*` and other subjects pass untouched. The scoped bus forwards `Conn()` when the shared bus has one, so plugins asserting `EventBusWithConn[*nats.Conn]` keep working, and passes `EventStream()` through unchecked, so JetStream publishes are not observed; `MonoFramework.EventBus(name)` returns the scoped bus of a started module and the shared bus otherwise. Plugins are not wrapped: they start before every module and are infrastructure, not dependencies.
- **Non-strict by default, `WithStrictModuleBoundaries()` opt-in**: without the option a violation is counted, logged once per module, kind, dependency and subject at Warn, and the operation **goes through**, so an existing application that registers on a dependency's container or requests around the port keeps working and the report shows where. With the option the operation returns `ErrModuleBoundaryViolation` (void container methods become no-ops, `MustGetChannelService` panics as it does on a missing service) and nothing is recorded. A module whose start fails is dropped from the report on rollback, and `Stop` clears the reports after logging the summary. Non-strict is the default because the view and the scoped bus are always present (there is no way to opt out of attribution) and rejecting would otherwise be a breaking change; the intended path is to run non-strict, read the report, fix the modules, then turn strict on (and in tests).
- **Report**: `MonoFramework.ModuleBoundaries()` returns one `types.ModuleBoundaryReport` per started module, sorted by name: `Declared` (deduplicated `Dependencies()`), `Consumed` (the `ConsumeServices()` contracts), `Observed` (dependency services looked up, with counts), `UnusedDependencies` (declared but never looked up; a dependency declared only for start ordering is legitimate, so this is information), `UnusedContracts` (consumed contracts of declared dependencies not looked up so far, in declaration order; adapters look services up at call time, so this too is information) and `Violations` (kind, dependency, subject, count, first seen). The lists of observations (`Observed`, `UnusedDependencies`, `UnusedContracts`, `Violations`) are empty, never nil, and always present in the JSON form. It is nil unless the application is running. At `Stop` the lifecycle logs one Warn per module and violation kind and one Info per unused dependency, (and per unused contract), so a run that never calls the accessor still leaves the evidence in the log.
- **`monotest`** (root-level package, beside `middleware/` and `plugin/`, because it imports `internal/*` and `pkg/` must stay free of such imports): an in-memory `EventBus` with core NATS semantics (wildcards, queue groups, request-reply, ordered per-subscription delivery; no JetStream), a `Container` that is the real service container bound to a module and dispatches its request-reply and queue group handlers on that bus (reply payload, `Mono-Error*` headers, queue group ACK), a `Harness` that runs one module through the documented lifecycle order with the same dependency views, consumed-contract validation and scoped bus as the framework (services are dispatched only after `Start`, as the framework creates subscriptions after every module started; `Stop` drains and waits for handlers in flight before stopping the module), and `CheckModule(t, m, opts...)`, which runs the harness and reports every observable Ports and Adapters deviation: non-kebab name; self or duplicate dependency or one without a stand-in; a provider registering nothing or an invalid subject; a consumed contract without a matching stand-in service; any boundary violation; a declared dependency never looked up (`WithExercise` drives the adapters, `WithOrderingOnlyDependency` exempts a start-ordering dependency); an emitted event with an invalid subject or a published `events.*` subject not in `EmitEvents()`; `Start`/`Stop` failing or `Stop` exceeding its timeout with a cancelled context; a consumed contract never looked up (`WithUnexercisedContract` exempts one the exercise cannot reach), which together with the `undeclared-service-use` violation makes `ConsumeServices()` exact. `CheckImports(t, opts...)` checks the contract package rule on the import graph of the application: it runs `go list -deps` on the patterns and lists every package of the application (the main Go module, or every workspace module) for its path, so that a module is known even when nothing in the checked packages imports its contract (standard tooling, no extra dependency); it takes every package with a `contract` sub-package as a module (plus `WithModuleRoots`, minus the `contract` directories named with `WithNonContractPackages`, which are domain or helper packages that happen to carry the name), and reports a contract package importing anything but the standard library, `pkg/types`, `pkg/helper` and other contract packages (`contract-import`; `WithAllowedContractImports` adds, for example, a Protobuf runtime) and a package outside a module importing a non-contract package of it (`module-import`; `package main` and `WithHostPackages` are exempt, since the host constructs the modules). Imports of test files are not checked. A check that applied a rule to no package (patterns matching nothing, or only `package main` and host packages, as when the test sits beside `cmd/<app>/main.go`) is an error, never a pass, as is an application without any module, and, with the default patterns, a check none of whose packages belongs to a module (a directory beside the modules; `WithImportPatterns` says such packages are meant). The default stays `./...` in the test's directory, not the Go module root, because one Go module may hold several applications. See `monotest/monotest.spec.md`.

## Monolith-Framework

### Overview

The core framework provides modular monolith architecture with embedded NATS for inter-module communication. It coordinates module registration, lifecycle management, and service discovery.

### Key Design Decisions

- **Embedded NATS Server**: The framework automatically starts an embedded NATS server during initialization. JetStream can be enabled for message persistence. Clustering supports horizontal scaling.

- **Functional Options Pattern**: All configuration uses `WithXxx(...) Option` functions. Validation occurs at option application time (fail-fast). Options are composable and backwards-compatible. Last-wins semantics for conflicting options.

- **Dependency Resolution**: Framework resolves module dependencies topologically and initializes in correct order. The resolution is deterministic: the graph is traversed in registration order (never Go map order), so the same registrations always produce the same start order and modules without dependencies start in registration order. Circular dependencies are detected and rejected. Shutdown occurs in reverse dependency order.

- **Service Container Per Module**: Each module receives its own ServiceContainer for registering services. Modules access dependency services through SetDependencyServiceContainer. Framework pre-registers core services (Logger, EventBus, NATSManager).

- **Automatic TLS (AutoTLS)**: The client listener can obtain and renew its own Let's Encrypt certificate through the ACME protocol, backed by `golang.org/x/crypto/acme/autocert`. Enabled with `WithNATSAutoTLS(types.AutoTLSConfig)`; disabled when `NATSOptions.AutoTLS` is nil. Renewal requires no restart or reload. See "AutoTLS Design" below.

### NATS Client Connection

- **Defaults**: the framework's own client connection (TCP or in-process) is created with `nats.Name` (`WithNATSClientName`, default `mono`, visible in `/connz` and server logs), an async **error handler** that routes client errors to the framework logger, **disconnect/reconnect/closed handlers** that log connection events, and a `DrainTimeout` aligned with the 5s connection drain at shutdown. Without an error handler nats.go prints client errors unstructured to stderr.
- **Slow consumers**: when a subscription's client buffer is full nats.go drops messages and reports `ErrSlowConsumer` once per episode; the framework logs it at Error with the subject, queue group, the module and service derived from a `services.<module>.<service>` subject, and the pending and dropped counts (`Dropped()` carries the total).
- **Application options**: `WithNATSClientOptions(...nats.Option)` appends nats.go options after the defaults, so an application can add to or override them (reconnect wait, ping interval, its own error handler).
- **Pending limits**: `ServiceOptions.PendingMsgs/PendingBytes` (`WithPendingLimits`, `-1` = unlimited) bound the client buffer of one subscription, applied with `SetPendingLimits` right after subscribing. Request-reply subscriptions default to 10,000 messages / 16 MB: for a request with a reply subject the callback does not wait for the pool (it fails fast), so the buffer only fills when the dispatcher falls behind the socket, and it is the last resort behind the worker pool; a drop there surfaces as a caller timeout, not `ErrServiceOverloaded`. A zero in one dimension takes nats.go's default for that dimension (nats.go rejects a limit of 0). Queue groups and event consumers keep nats.go's defaults (500,000 messages / 64 MB) unless set per registration, because that buffer is the back-pressure reservoir when their pool is full.
- **Stats**: `MonoFramework.SubscriptionStats()` returns, per request-reply service, queue group and event consumer (stream, event stream and cron consumers pull from JetStream and are not included), the client buffer counters (pending messages and bytes, dropped, delivered), the worker pool (active, queued, bounds) and the framework's monotonic counters (rejected as overloaded, expired, dropped at shutdown). `WithSubscriptionStatsLogInterval` adds a periodic Debug line per subscription (off by default). This is the first place to look when calls are slow: a growing `PendingMsgs` means the dispatcher is behind, a growing `PoolQueued` means the handlers are.

### Service Discovery and Stats (NATS services protocol)

- **Opt-in** with `WithServiceDiscovery(ServiceDiscoveryConfig{Version, Description, Metadata})` (adds nine subscriptions per exposed module). The framework answers the control subjects of the NATS services protocol (ADR-32, the protocol behind `nats.go/micro` and the `nats micro ls / info / stats` CLI): `$SRV.PING`, `$SRV.INFO`, `$SRV.STATS`, each also with `.<service>` and `.<service>.<id>` suffixes, using the response types of the `micro` package so the wire format is identical.
- **Follow the protocol, do not adopt `micro.AddService`**: micro would create its own subscriptions on the service subjects, route requests around the worker pool, and emit its own error headers (`Nats-Service-Error`). Mono keeps its subscriptions, pools and `Mono-Error*` headers; only the discovery and stats side of the protocol is implemented. `Nats-Service-Error` headers are deliberately not emitted.
- **Mapping**: one protocol service per module that owns request-reply services (modules without any are not exposed), named after the module (names must match `^[A-Za-z0-9\-_]+$`; module names are already kebab-case by registration, so the protocol guard is defensive), with a NUID instance id and the configured semantic version (default `0.0.0`; non-semver versions are rejected at option time). Endpoints are the module's request-reply services: name = service, subject `services.<module>.<service>`, queue group. Queue groups and event consumers are not endpoints in the protocol sense.
- **Stats**: the standard fields per endpoint (`num_requests`, `num_errors`, `last_error`, `processing_time`, `average_processing_time`) count handler runs only: expired and rejected requests do not run and are reported in the `data` field instead. Measurement happens on the pool worker: **queue wait** is arrival (subscription callback) to handler start, processing time is handler start to end, so the two numbers an incident needs are kept apart. `data` carries `queue_wait_time`, `average_queue_wait_time`, `max_queue_wait_time`, `expired`, `expired_queue_wait_max` (the longest a request waited before it was found expired; those waits are not in the queue wait fields, since the handler never ran), `rejected`, `pending_msgs`, `pool_active`, `pool_queued`. Queue wait starts at the subscription callback, so time spent in the NATS client buffer is not included (`pending_msgs` is the signal for that). `last_error` is truncated to 256 bytes on a character boundary.
- **Responders** run on the event bus directly (not through a pool; the requests are tiny) and every instance answers the bare `$SRV.PING`, so discovery lists all instances. They are created after every request-reply subscription is recorded, tracked with the module's subscriptions, and drained at shutdown with them.
- **Visibility**: as in the protocol itself, any client that can reach the embedded server can read `$SRV.INFO` and `$SRV.STATS`, including `last_error` text and the configured `Metadata`. Deployments that expose the NATS port beyond the application (cluster peers, leafnodes, external clients) should restrict `$SRV.>` through server permissions, and handlers should keep secrets out of error messages.

### AutoTLS Design

- **Scope — the client listener only**: AutoTLS installs its certificate on `server.Options.TLSConfig`, which nats-server uses exclusively for **client-to-server** connections. Route (cluster), gateway, leafnode, websocket and MQTT listeners each read their own separate `TLSConfig` and are left untouched, so inter-node cluster traffic stays plaintext whether or not AutoTLS is enabled. This is a deliberate boundary, not an oversight — see "Non-goals" below for why an ACME certificate does not fit the route path.
- **Challenge type**: http-01 only. The framework serves `autocert.Manager.HTTPHandler` from a listener it owns, bound to `HTTPChallengeAddr` (default `:80`). tls-alpn-01 is unsupported because it requires the validated listener on port 443, while NATS listens on 4222 and `WithNATSPort` rejects ports below 1024; `NextProtos` deliberately omits `acme.ALPNProto` so the listener never advertises what it cannot answer. dns-01 is unsupported because autocert does not implement it, which is also why wildcard domains are rejected during validation.
- **Challenge listener lifetime**: the listener stays up for the whole life of the process, not just until first issuance, because every renewal re-runs the challenge. Calling `HTTPHandler` is also what enables http-01 at all — it sets the manager's internal `tryHTTP01` flag — so it must be bound before anything can trigger issuance. Its `Host` header is normalized to a bare hostname before the host policy runs: autocert passes `r.Host` through verbatim and `HostWhitelist` matches exactly, so a listener on any port other than 80 would otherwise answer every challenge with 403.
- **Certificate model**: autocert issues one certificate per name rather than one multi-SAN certificate, and the name in the TLS SNI extension selects which is served. `Domains` doubles as the host allowlist, so a handshake for any other name is refused, and a client that dials by IP is refused too because no SNI is presented. `CacheDir` is mandatory and has no default: it holds the ACME account key and the issued certificates, and losing it forces reissuance on every boot until the CA's rate limits are exhausted.
- **Lifecycle**: the autocert manager is built and installed on `server.Options.TLSConfig` before the NATS server is created, the challenge listener is bound next, and certificates are pre-issued after `ReadyForConnections` but before the framework's own client connects. Pre-issuance is bounded by `StartupIssueTimeout` (default 60s; negative disables it) so a misconfigured domain or an unreachable challenge port fails startup rather than surfacing as a handshake error later. `opts.TLSTimeout` is raised from the nats-server default of 2s, which a cold lazy handshake could not meet. On shutdown the challenge listener is stopped before the NATS server, so a renewal in flight cannot race a half-stopped server.
- **Cancellation**: autocert exposes no way to cancel a running `GetCertificate`, so an in-flight issuance can only be abandoned, never cancelled. Two bounds keep it finite — an explicit per-request timeout on the ACME HTTP client, and autocert's own five-minute ceiling — and the abandonment is logged so a repeating leak is diagnosable rather than silent.
- **Internal client**: enabling AutoTLS forces the framework's own NATS client onto the in-process transport, because a loopback TCP dial cannot satisfy hostname verification against a public-domain certificate. This is safe because nats-server sniffs in-process connections even when `TLSConfig` is set, so the pipe stays plaintext while every TCP client is held to TLS. `ServerInfo().ClientURL` reports `tls://<first domain>:<port>` — the name the certificate is valid for, not the bind address.
- **Compatibility**: AutoTLS makes TLS mandatory for external clients, so enabling it on a running deployment breaks plaintext clients. `AllowNonTLS` is deliberately not exposed, because it would let a client silently downgrade. AutoTLS cannot be combined with `DontListen` (there is no listener to protect) or with a config file that carries its own `tls{}` block (both would own `TLSConfig`); both combinations are rejected during initialization.
- **Non-goals**: cluster, gateway, leafnode, websocket and MQTT TLS are untouched, and `ServerInfo().ClusterURL` stays `nats://`. HTTPS monitoring is not enabled. Manual certificate files are not supported.
- **Why routes are not covered**: a route is peer-to-peer, and nats-server hands the same `opts.Cluster.TLSConfig` to both handshake roles — the soliciting node acts as the TLS *client* and the accepting node as the *server*. An autocert config supplies only `GetCertificate`, so the soliciting side would present no certificate at all; cluster TLS is conventionally mutual, and the client-auth EKU that would require is no longer part of the Let's Encrypt profile. Routes are also normally dialled by private IP or an internal hostname, neither of which a public ACME certificate covers or `HostWhitelist` accepts. Finally, `autocert.DirCache` has no distributed locking, so nodes sharing a cache would race to issue for the same name. Encrypting cluster traffic is better served by an internal CA supplied through a `cluster { tls { … } }` block in a `WithNATSConfigFile` configuration.

### Important Behaviors

**Module Start Failure Handling**: If any module fails to start, the framework immediately stops all previously started modules in reverse order, then returns an error with context about which module failed.

**Health Aggregation Logic**: Framework aggregates health from modules implementing HealthAwareModule. Overall health requires: framework running AND NATS responsive AND all health-aware modules healthy. Modules without HealthAwareModule are excluded from calculation (not marked unhealthy).

**NATS Connection Loss**: Framework automatically reconnects with exponential backoff. Modules are notified of reconnection events. Messages may be lost during disconnection unless using JetStream with durable subscriptions.

**Handler Error Semantics**:
- Request-Reply: Errors propagated immediately to caller
- Queue Group: Errors logged but don't affect sender (fire-and-forget)
- Channel Service: Handler owns error handling
- Overload (all pooled handlers): a request or ACK-mode send rejected by a full worker pool fails fast with `ErrServiceOverloaded`; events and optimistic sends are held back instead of dropped (see "Handler Concurrency")
- Panic (framework-dispatched handlers, and `EventBus.Subscribe`/`QueueSubscribe` callbacks): recovered and logged with the stack, the subscription keeps running; a request-reply caller gets `ErrHandlerPanic` (see "Handler Panic Recovery")

**Configuration Validation**: Each option validates immediately during application. First error stops processing. Cross-field validation occurs after all options applied. Error messages include context ("option N failed: specific error").

**Graceful Shutdown Sequence** (`Stop`):
1. Stop taking new work: cancel the fetch context (stream, event stream and cron loops stop fetching) and `Drain()` every subscription, module by module in reverse dependency order.
2. Wait, within the drain budget, until each subscription is no longer valid (nats.go's `Drain` returns before buffered messages are delivered, so the framework polls `IsValid`), the worker pools have run their in-flight and queued handlers, and the fetch loop handlers in flight have returned. Handlers keep a **live runtime context** throughout, so a request that started just before `Stop` completes and its reply is delivered. The drain budget is the `Stop` context bounded by `ShutdownDrainTimeout` (`WithShutdownDrainTimeout`; when not set, 30s = nats.go `DefaultDrainTimeout`, capped at `ShutdownTimeout`). What has not finished is counted and logged (`subscriptions_not_drained`, `pools_still_busy`, `fetch_loop_handlers_still_running`).
3. Cancel the runtime context: anything still running after the budget is told to stop; channel routers exit.
4. Stop all modules in reverse dependency order, then middleware, then plugins.
5. Drain and close the NATS client connection (so a reply published during step 2 still reaches an in-process requester), then stop the embedded NATS server.

`ShutdownTimeout` (`WithShutdownTimeout`, default 30s) bounds steps 1–4 of `Stop` when the caller's context has no deadline; a caller deadline is used as is. Step 5 runs outside that bound and takes at most 5s for the connection drain plus the server shutdown. The drain budget is further capped so that a reserve of the context's remaining time (5s, or a quarter of it when that is smaller) is kept for module `Stop`, so a stuck handler cannot hand modules an already expired context; with `WithShutdownTimeout(5*time.Second)` the drain gets 3.75s. Fetch loop handlers in flight are awaited before any subscription is drained, so their nested calls also still find a responder. Subscriptions are drained module by module in reverse dependency order, so a handler that calls another module's service while finishing still finds a responder. An explicit `ShutdownDrainTimeout` must not exceed `ShutdownTimeout` (checked after all options are applied); when it is not set it follows a shorter `ShutdownTimeout`, so existing configurations such as `WithShutdownTimeout(5*time.Second)` stay valid. Because the drain may use its whole budget before modules stop, set `ShutdownDrainTimeout` below `ShutdownTimeout`, and both below the orchestrator's termination grace period (Kubernetes defaults to 30s), so module `Stop` still runs before the process is killed.

**Fetch loop and handler contexts**: fetch loops (stream consumer, event stream consumer, cron) run under a fetch context that is cancelled at step 1; the handlers they call receive the runtime context, cancelled at step 3. A batch in flight at `Stop` therefore finishes with a live context and can acknowledge normally. `Stop` waits for those handlers, not for the loop goroutines: a `Fetch` blocked on the server returns on its own after at most `Fetch.Timeout` (5s by default), and a batch it brings back after shutdown began is not handled but left unacknowledged for redelivery (a cron occurrence is Nak'd), so shutdown is not delayed by the fetch timeout. (`jetstream.FetchContext` is deliberately not used: it leaves the server-side pull request open for its default 30s expiry, so messages delivered after the context ends are lost until `AckWait`.)

### Performance Targets

- Message throughput: >40,000 msgs/sec on commodity hardware
- Base memory footprint: <20MB (framework core only)
- Framework startup: <10ms to initialize embedded NATS
- Service latency: ~1ms for Request-Reply, ~microseconds for Channel services

## Directory Structure

```
mono-framework/
├── pkg/                           # PUBLIC API PACKAGES
│   ├── types/                     # Core interfaces & type definitions
│   │   ├── framework.go           # MonoFramework, MonoFrameworkState
│   │   ├── module.go              # All module interfaces
│   │   ├── container.go           # ServiceContainer & service types
│   │   ├── event.go               # Event definitions & consumers
│   │   ├── eventbus.go            # EventBus & messaging abstractions
│   │   ├── jetstream.go           # JetStream config types
│   │   ├── middleware.go          # Middleware interfaces & hooks
│   │   ├── logger.go              # Logger interfaces
│   │   └── plugin.go              # Plugin module interfaces
│   ├── errors/                    # Error handling utilities
│   └── helper/                    # Helper functions for common tasks
│
├── internal/                      # INTERNAL IMPLEMENTATIONS
│   ├── app/                       # Framework application lifecycle
│   ├── container/                 # ServiceContainer implementation
│   │   ├── channel.go             # Channel service impl
│   │   ├── requestreply.go        # RequestReply service impl
│   │   ├── queuegroup.go          # QueueGroup service impl
│   │   └── streamconsumer.go      # StreamConsumer service impl
│   ├── eventbus/                  # EventBus implementation (NATS wrapper)
│   ├── lifecycle/                 # Module lifecycle & startup order
│   ├── logger/                    # Logger implementation
│   ├── middleware/                # Middleware chain execution
│   ├── nats/                      # NATS server setup & embedded server
│   ├── registry/                  # Module registry & event registry impl
│   └── workerpool/                # Bounded worker pool for subscription handlers
│
├── middleware/                    # BUILT-IN MIDDLEWARE MODULES
│   ├── accesslog/                 # Access logging middleware
│   ├── audit/                     # Audit logging middleware
│   └── requestid/                 # Request ID injection middleware
│
├── plugin/                        # BUILT-IN PLUGINS
│   └── fs-jetstream/              # File storage plugin for JetStream
│
├── examples/                      # EXAMPLE IMPLEMENTATIONS
│   ├── basic/                     # Basic framework usage
│   ├── analytics/                 # Analytics module (channel services)
│   ├── event-emitter/             # Event pub/sub example
│   └── multi-module/              # Multi-module coordination
│
├── test/integration/              # Integration tests
├── bench/                         # Performance benchmarks
└── docs/spec/                    # Design specifications
```

### Module Interface Hierarchy

Modules can implement any combination of these optional interfaces:

- **DependentModule**: Declares dependencies on other modules
- **ServiceProviderModule**: Registers services the module provides
- **EventBusAwareModule**: Receives EventBus instance for pub/sub
- **EventEmitterModule**: Registers event definitions (extends EventBusAwareModule)
- **EventConsumerModule**: Registers event consumer handlers
- **HealthCheckableModule**: Reports health status
- **PluginModule**: Special modules that start first, stop last, excluded from dependency graph
- **UsePluginModule**: Allows modules to receive and use plugin instances
