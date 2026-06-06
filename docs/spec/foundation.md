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

- **Subjects**: the schedule message is stored on an internal, framework-owned subject `_framework.cron.<module>.<service>.schedule`; the server republishes the payload to the target service subject `services.<module>.<service>` on every occurrence. The durable consumer filters on the concrete target subject, so the schedule/control messages are never delivered to the handler.
- **Stream**: one JetStream stream per cron service, `MONO_CRON_<module>_<service>`, with `AllowMsgSchedules` and `AllowRollup` enabled. The schedule rolls up by subject (one schedule per subject), making the startup re-publish idempotent: changing `Schedule`/`Payload`/`TimeZone`/`TTL` and redeploying overwrites the live schedule in place.
- **Acknowledgement is framework-owned** (unlike Stream Consumer services, where the handler owns ack): the framework Acks the occurrence when the handler returns nil and Naks it (redelivery up to `MaxDeliver`) on a non-nil error or a recovered panic. The handler must not call `Ack`/`Nak` itself.
- **Schedule formats**: cron expression (`"0 0 * * *"`), alias (`"@daily"`, `"@hourly"`, …), or interval (`"@every 5m"`, minimum 1s). `TimeZone` applies only to cron expressions (not `@every`/`@at`). `SourceSubject` (mutually exclusive with `Payload`) delivers the last message seen on a subject instead of a static payload.
- **Retiring a cron service (two-phase)**: set `Deprecated: true` and deploy — the framework purges the server-side schedule (publishing a cancel to `_framework.cron.<module>.<service>.control`) and does not start the consumer, while keeping the registration code. In a later release, remove the `RegisterCronService` call. Removing the call without first deprecating leaves an orphaned durable schedule; the framework logs a warning on startup when it detects a `MONO_CRON_*` stream with no matching registration (it never auto-deletes).
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
- **Internal framework**: `_framework.<component>.<operation>` (reserved, modules cannot publish here)
- **Rules**: Lowercase with hyphens (kebab-case), no spaces or special characters except `.`, `-`, `*`, `>`

## Monolith-Framework

### Overview

The core framework provides modular monolith architecture with embedded NATS for inter-module communication. It coordinates module registration, lifecycle management, and service discovery.

### Key Design Decisions

- **Embedded NATS Server**: The framework automatically starts an embedded NATS server during initialization. JetStream can be enabled for message persistence. Clustering supports horizontal scaling.

- **Functional Options Pattern**: All configuration uses `WithXxx(...) Option` functions. Validation occurs at option application time (fail-fast). Options are composable and backwards-compatible. Last-wins semantics for conflicting options.

- **Dependency Resolution**: Framework resolves module dependencies topologically and initializes in correct order. Circular dependencies are detected and rejected. Shutdown occurs in reverse dependency order.

- **Service Container Per Module**: Each module receives its own ServiceContainer for registering services. Modules access dependency services through SetDependencyServiceContainer. Framework pre-registers core services (Logger, EventBus, NATSManager).

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
