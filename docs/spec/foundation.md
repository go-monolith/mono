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
- **Acknowledgement is framework-owned** (unlike Stream Consumer services, where the handler owns ack): the framework Acks the occurrence when the handler returns nil and Naks it (redelivery up to `MaxDeliver`) on a non-nil error or a recovered panic. The handler must not call `Ack`/`Nak` itself.
- **Schedule formats**: cron expression in the standard five-field format (`"0 0 * * *"` = daily at midnight) or the six-field seconds-first format the NATS scheduler natively understands (`"0 0 0 * * *"`) — five-field expressions are normalized by prepending a `"0"` seconds field (`types.NormalizeCronSchedule`) — alias (`"@daily"`, `"@hourly"`, …), or interval (`"@every 5m"`, minimum 1s). `TimeZone` applies only to cron expressions (not `@every`/`@at`). `SourceSubject` (mutually exclusive with `Payload`) delivers the last message seen on a subject instead of a static payload.
- **Retiring a cron service (two-phase)**: set `Deprecated: true` and deploy — the framework purges the server-side schedule (publishing a cancel to `services.<module>.<service>.control`) and does not start the consumer, while keeping the registration code. In a later release, remove the `RegisterCronService` call. Removing the call without first deprecating leaves an orphaned durable schedule; the framework logs a warning on startup when it detects a `MONO_CRON_*` stream with no matching registration (it never auto-deletes).
- **Middleware**: cron handlers are not currently wrapped by the middleware chain (access-log, audit, request-id).

### Event Consumer Patterns

Two patterns for consuming broadcast events:

- **Event Consumer (NATS Core)**: Fire-and-forget event consumption via standard NATS subscriptions. Low latency, no persistence. Uses `EventConsumerHandler` or `TypedEventConsumerHandler[T]` for type-safe consumption.

- **Event Stream Consumer (JetStream)**: Durable event consumption via JetStream pull consumers. Supports batch processing with `EventStreamConsumerHandler` or `TypedEventStreamConsumerHandler[T]`. Messages require explicit acknowledgment. Used when event replay or at-least-once delivery is needed.

Event emitter modules declare events via `EventEmitterModule.EmitEvents()`. Consumer modules discover and register handlers via `EventConsumerModule.RegisterEventConsumers(registry)`.

### Module Lifecycle

Modules are first-class citizens with a well-defined lifecycle. The initialization sequence per module is strictly ordered:

1. ServiceContainer binding (container associated with module)
2. SetDependencyServiceContainer (provide dependency containers)
3. SetEventBus (provide event bus for pub/sub)
4. RegisterServices (module registers its own services)
5. Start (module performs initialization)
6. NATS subscriptions setup (framework creates subscriptions for registered services)

Shutdown occurs in reverse order with subscription draining before module stopping. Failed module initialization triggers rollback of previously started modules.

### Subject Naming Conventions

The framework enforces standardized subject naming to prevent conflicts:

- **Service subjects**: `services.<module>.<service>` (computed by ServiceContainer, no wildcards)
- **Event subjects**: `events.<domain>.<event-type>` (supports wildcards for broadcast patterns)
- **Internal framework**: `_mono.<component>.<operation>` (reserved prefix, modules cannot publish here)
- **Rules**: Lowercase with hyphens (kebab-case), no spaces or special characters except `.`, `-`, `*`, `>`

## Monolith-Framework

### Overview

The core framework provides modular monolith architecture with embedded NATS for inter-module communication. It coordinates module registration, lifecycle management, and service discovery.

### Key Design Decisions

- **Embedded NATS Server**: The framework automatically starts an embedded NATS server during initialization. JetStream can be enabled for message persistence. Clustering supports horizontal scaling.

- **Functional Options Pattern**: All configuration uses `WithXxx(...) Option` functions. Validation occurs at option application time (fail-fast). Options are composable and backwards-compatible. Last-wins semantics for conflicting options.

- **Dependency Resolution**: Framework resolves module dependencies topologically and initializes in correct order. Circular dependencies are detected and rejected. Shutdown occurs in reverse dependency order.

- **Service Container Per Module**: Each module receives its own ServiceContainer for registering services. Modules access dependency services through SetDependencyServiceContainer. Framework pre-registers core services (Logger, EventBus, NATSManager).

- **Automatic TLS (AutoTLS)**: The client listener can obtain and renew its own Let's Encrypt certificate through the ACME protocol, backed by `golang.org/x/crypto/acme/autocert`. Enabled with `WithNATSAutoTLS(types.AutoTLSConfig)`; disabled when `NATSOptions.AutoTLS` is nil. Renewal requires no restart or reload. See "AutoTLS Design" below.

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

**Configuration Validation**: Each option validates immediately during application. First error stops processing. Cross-field validation occurs after all options applied. Error messages include context ("option N failed: specific error").

**Graceful Shutdown Sequence**:
1. Stop accepting new work
2. Drain in-flight operations (with timeout)
3. Unsubscribe from all NATS subscriptions
4. Stop all modules in reverse dependency order
5. Close NATS connection and stop embedded NATS server
6. Force shutdown if timeout exceeded

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
│   └── registry/                  # Module registry & event registry impl
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
