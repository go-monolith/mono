# Mono Framework Architecture

This document provides a comprehensive overview of the Mono Framework architecture, including component diagrams, module lifecycle, service patterns, and naming conventions.

## Table of Contents

- [High-Level Architecture](#high-level-architecture)
- [Layered Architecture](#layered-architecture)
- [Module Lifecycle](#module-lifecycle)
- [Service Communication Patterns](#service-communication-patterns)
- [Event System](#event-system)
- [Subject Naming Conventions](#subject-naming-conventions)
- [Sequence Diagrams](#sequence-diagrams)

## High-Level Architecture

The Mono Framework implements a modular monolith architecture where all modules run in a single process but communicate through an embedded NATS message queue, providing loose coupling and clear boundaries.

```
                    +------------------------------------------+
                    |         Mono Application                  |
                    |  +------------------------------------+  |
                    |  |         Application Modules         |  |
                    |  |  +--------+ +--------+ +--------+  |  |
                    |  |  |Order   | |Payment | |Notif.  |  |  |
                    |  |  |Module  | |Module  | |Module  |  |  |
                    |  |  +--------+ +--------+ +--------+  |  |
                    |  +------------------------------------+  |
                    |                    |                      |
                    |  +------------------------------------+  |
                    |  |         Framework Core              |  |
                    |  |  ServiceContainer | EventBus |Logger|  |
                    |  +------------------------------------+  |
                    |                    |                      |
                    |  +------------------------------------+  |
                    |  |      Embedded NATS Server           |  |
                    |  |    (Core + JetStream Optional)      |  |
                    |  +------------------------------------+  |
                    +------------------------------------------+
```

### Key Components

| Component | Purpose |
|-----------|---------|
| **MonoApplication** | Main entry point for framework initialization and lifecycle |
| **Module** | Self-contained unit of business logic with lifecycle management |
| **ServiceContainer** | Dependency injection and service registration per module |
| **EventBus** | NATS-backed messaging abstraction for pub/sub |
| **Logger** | Structured logging with module context |
| **Embedded NATS** | In-process message broker with optional JetStream |

## Layered Architecture

The framework follows a strict layered architecture with clear separation of concerns.

```
+-----------------------------------------------------------------------+
|                       Application Layer                                |
|                                                                        |
|  Your modules implementing mono.Module and optional interfaces:        |
|  - DependentModule       (declare dependencies)                        |
|  - ServiceProviderModule (register services)                           |
|  - EventBusAwareModule   (pub/sub messaging)                           |
|  - EventEmitterModule    (emit typed events)                           |
|  - EventConsumerModule   (consume events)                              |
|  - HealthCheckableModule (health reporting)                            |
|  - PluginModule          (framework extensions)                        |
+-----------------------------------------------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                        Framework Layer                                 |
|                                                                        |
|  +-------------------+  +------------------+  +---------------------+ |
|  | ServiceContainer  |  |     EventBus     |  |       Logger        | |
|  |                   |  |                  |  |                     | |
|  | - Channel         |  | - Publish        |  | - Debug/Info/Warn   | |
|  | - RequestReply    |  | - Subscribe      |  | - Error             | |
|  | - QueueGroup      |  | - Request        |  | - WithModule()      | |
|  | - StreamConsumer  |  | - QueueSubscribe |  | - WithComponent()   | |
|  +-------------------+  +------------------+  +---------------------+ |
|                                                                        |
|  +-------------------+  +------------------+  +---------------------+ |
|  |  Module Registry  |  |  Event Registry  |  | Lifecycle Manager   | |
|  +-------------------+  +------------------+  +---------------------+ |
+-----------------------------------------------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                     Infrastructure Layer                               |
|                                                                        |
|  +---------------------------------------------------------------+   |
|  |                    Embedded NATS Server                        |   |
|  |                                                                 |   |
|  |  +------------------+  +------------------------------------+ |   |
|  |  |   NATS Core      |  |           JetStream                | |   |
|  |  |                  |  |                                    | |   |
|  |  | - Pub/Sub        |  | - Streams (persistence)            | |   |
|  |  | - Request/Reply  |  | - Consumers (durable pull)         | |   |
|  |  | - Queue Groups   |  | - KV Store (key-value)             | |   |
|  |  +------------------+  | - Object Store (file storage)      | |   |
|  |                        +------------------------------------+ |   |
|  +---------------------------------------------------------------+   |
+-----------------------------------------------------------------------+
```

### Package Structure

```
mono-framework/
├── pkg/                           # PUBLIC API
│   ├── types/                     # Interfaces & types
│   ├── errors/                    # Error types
│   └── helper/                    # Convenience functions
│
├── internal/                      # PRIVATE IMPLEMENTATION
│   ├── app/                       # Framework lifecycle
│   ├── container/                 # ServiceContainer impl
│   ├── eventbus/                  # EventBus impl
│   ├── lifecycle/                 # Module ordering
│   ├── logger/                    # Logger impl
│   ├── middleware/                # Middleware chain
│   ├── nats/                      # NATS server setup
│   └── registry/                  # Module & event registry
│
├── middleware/                    # BUILT-IN MIDDLEWARE
│   ├── accesslog/                 # Access logging
│   ├── audit/                     # Security auditing
│   └── requestid/                 # Request ID propagation
│
└── plugin/                        # BUILT-IN PLUGINS
    ├── fs-jetstream/              # File storage
    └── kv-jetstream/              # Key-value storage
```

## Module Lifecycle

Modules have a well-defined lifecycle managed by the framework. The initialization sequence is strictly ordered per module.

### Initialization Sequence

```
                 Framework Start
                       |
                       v
        +-----------------------------+
        |  1. Resolve Dependencies    |
        |     (Topological Sort)      |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  2. BindModule(container)   |
        |     (ServiceContainer)      |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  3. SetDependencyService    |
        |     Container(dep, cont)    |
        |     (For each dependency)   |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  4. SetEventBus(bus)        |
        |     (If EventBusAwareModule)|
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  5. RegisterServices(cont)  |
        |     (If ServiceProvider)    |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  6. EmitEvents()            |
        |     (If EventEmitterModule) |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  7. RegisterEventConsumers  |
        |     (If EventConsumerModule)|
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  8. Start(ctx)              |
        |     (Module initialization) |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  9. Setup NATS Subscriptions|
        |     (For registered services)|
        +-----------------------------+
                       |
                       v
                 Module Running
```

### Shutdown Sequence

```
                 Shutdown Signal
                       |
                       v
        +-----------------------------+
        |  1. Stop Accepting Work     |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  2. Drain NATS Subscriptions|
        |     (With timeout)          |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  3. Stop Modules            |
        |     (Reverse dependency     |
        |      order)                 |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  4. Close NATS Connection   |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |  5. Stop NATS Server        |
        +-----------------------------+
                       |
                       v
                 Shutdown Complete
```

### Module Interface Hierarchy

```
                    +------------------+
                    |     Module       |  (Required)
                    | Name() string    |
                    | Start(ctx) error |
                    | Stop(ctx) error  |
                    +------------------+
                           |
         +-----------------+-----------------+
         |                 |                 |
         v                 v                 v
+------------------+ +------------------+ +--------------------+
| DependentModule  | |ServiceProvider   | |EventBusAwareModule |
| Dependencies()   | |Module            | |SetEventBus(bus)    |
| SetDependency... | |RegisterServices()| +--------------------+
+------------------+ +------------------+          |
                                                   v
                           +------------------------------------------+
                           |                    |                     |
                           v                    v                     v
                   +----------------+  +------------------+  +-------------------+
                   |EventEmitterMod |  |EventConsumerMod  |  |HealthCheckableMod |
                   |EmitEvents()    |  |RegisterEvent...  |  |Health(ctx)        |
                   +----------------+  +------------------+  +-------------------+
```

## Service Communication Patterns

The framework provides four distinct patterns for inter-module communication. Each pattern suits different use cases.

### Pattern Comparison

| Pattern | Latency | Durability | Load Balanced | Use Case |
|---------|---------|------------|---------------|----------|
| **Channel** | ~us | None | No | In-process, bidirectional |
| **Request-Reply** | ~1ms | None | No | Synchronous RPC calls |
| **Queue Group** | ~1ms | None | Yes | Async work distribution |
| **Stream Consumer** | ~1ms | JetStream | Yes | Durable message processing |

### Channel Services

For in-process bidirectional communication with lowest latency.

```
+-------------+                  +-------------+
|  Module A   |                  |  Module B   |
|             |  Chan<Request>   |             |
|  Client     | ---------------> |  Handler    |
|             |                  |             |
|             | <--------------- |             |
|             |  Chan<Response>  |             |
+-------------+                  +-------------+

Use when:
- Need lowest possible latency (~microseconds)
- Communication is always in-process
- Bidirectional streaming is required
```

### Request-Reply Services

For synchronous inter-module calls via NATS.

```
+-------------+        NATS        +-------------+
|  Module A   |                    |  Module B   |
|             |  Request           |             |
|  Client     | -----------------> |  Handler    |
|             |  services.b.svc    |             |
|             |                    |             |
|             | <----------------- |             |
|             |  Response          |             |
+-------------+                    +-------------+

Use when:
- Need synchronous request/response pattern
- Caller needs immediate result
- Error propagation to caller is required
```

### Queue Group Services

For load-balanced async processing.

```
+-------------+        NATS        +-------------+
|  Module A   |                    |  Module B   |
|             |  Message           |  (Worker 1) |
|  Client     | -----------------> |  Handler    |
|             |  services.b.svc    +-------------+
|             |        |
|             |        |           +-------------+
|             |        +---------> |  Module B   |
|             |                    |  (Worker 2) |
+-------------+                    |  Handler    |
                                   +-------------+

Use when:
- Fire-and-forget semantics acceptable
- Need horizontal scaling across workers
- No response needed from handler
```

### Stream Consumer Services

For durable message processing with JetStream.

```
+-------------+     JetStream      +-------------+
|  Module A   |                    |  Module B   |
|             |  Publish           |             |
|  Publisher  | -----> [Stream] -> |  Consumer   |
|             |        (persisted) |  (Pull)     |
|             |                    |             |
|             |                    |  Ack/Nak    |
+-------------+                    +-------------+

Use when:
- Message durability is required
- At-least-once delivery is needed
- Message replay capability is needed
- Batch processing is beneficial
```

## Event System

The event system enables broadcast communication between modules using typed events.

### Event Flow

```
+------------------+        +-------------------+        +------------------+
|  Emitter Module  |        |   Event Registry  |        | Consumer Module  |
|                  |        |                   |        |                  |
| EmitEvents()     | -----> | Store definitions | <----- | GetEventByName() |
| returns []Event  |        |                   |        | returns Event    |
|                  |        |                   |        |                  |
| eventBus.Publish | -----> |   NATS Pub/Sub    | -----> | Handler(msg)     |
| (subject, data)  |        |   or JetStream    |        |                  |
+------------------+        +-------------------+        +------------------+
```

### Event Patterns

**NATS Core Events** (Fire-and-Forget):
```go
// Emitter declares event
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
    }
}

// Consumer registers handler
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    eventDef, _ := registry.GetEventByName("OrderCreated", "v1", "order")
    return registry.RegisterEventConsumer(eventDef, m.handleOrderCreated, m)
}
```

**JetStream Events** (Durable):
```go
// Consumer registers durable handler with batch processing
err := helper.RegisterTypedEventStreamConsumer(
    registry,
    order.OrderCreatedV1,
    types.StreamConsumerConfig{
        Stream: types.StreamConfig{Name: "order-events"},
        Fetch:  types.FetchConfig{BatchSize: 10},
    },
    m.handleOrderBatch,
    m,
)
```

## Subject Naming Conventions

The framework enforces standardized subject naming to prevent conflicts and ensure consistency.

### Subject Patterns

| Category | Pattern | Example |
|----------|---------|---------|
| **Services** | `services.<module>.<service>` | `services.payment.process` |
| **Events** | `events.<domain>.<version>.<event-type>` | `events.order.v1.created` |
| **Internal** | `_framework.<component>.<operation>` | `_framework.health.ping` |

### Naming Rules

1. **Lowercase with hyphens** (kebab-case): `order-processing`, not `orderProcessing`
2. **No spaces or special characters** except `.`, `-`, `*`, `>`
3. **Wildcards in subscriptions only**: `events.order.>` for all order events
4. **Reserved prefix**: `_framework.*` for internal framework use

### Service Subject Construction

```
services.{module-name}.{service-name}

Examples:
  services.payment.process-payment     (RequestReply)
  services.notification.send-email     (QueueGroup)
  services.analytics.page-views        (StreamConsumer)
```

### Event Subject Construction

```
events.{module-name}.{version}.{event-name}

Examples:
  events.order.v1.order-created
  events.user.v1.user-registered
  events.payment.v2.payment-completed
```

## Sequence Diagrams

### Application Startup

```
  Main        Framework     NATS       Module A     Module B
   |              |          |            |            |
   | NewMonoApp() |          |            |            |
   |------------->|          |            |            |
   |              | Start    |            |            |
   |              | Server   |            |            |
   |              |--------->|            |            |
   |              |          |            |            |
   | Register(A)  |          |            |            |
   |------------->|          |            |            |
   | Register(B)  |          |            |            |
   |------------->|          |            |            |
   |              |          |            |            |
   | Start()      |          |            |            |
   |------------->|          |            |            |
   |              | Resolve Dependencies   |            |
   |              |--------------------->  |            |
   |              |          |            |            |
   |              | BindModule            |            |
   |              |---------------------->|            |
   |              | SetEventBus           |            |
   |              |---------------------->|            |
   |              | RegisterServices      |            |
   |              |---------------------->|            |
   |              | Start()               |            |
   |              |---------------------->|            |
   |              |          |            |            |
   |              | BindModule                         |
   |              |---------------------------------->|
   |              | SetDependency(A)                  |
   |              |---------------------------------->|
   |              | Start()                           |
   |              |---------------------------------->|
   |              |          |            |            |
   |              | Setup Subscriptions    |            |
   |              |--------->|            |            |
   |<-------------|          |            |            |
   | Ready        |          |            |            |
```

### Request-Reply Call

```
  Module A      ServiceContainer     NATS        Module B
     |                 |              |              |
     | GetRequestReply |              |              |
     | Service("svc")  |              |              |
     |---------------->|              |              |
     |<----------------|              |              |
     | client          |              |              |
     |                 |              |              |
     | client.Call()   |              |              |
     |---------------->| Request      |              |
     |                 |------------->| Handler()    |
     |                 |              |------------->|
     |                 |              |<-------------|
     |                 |<-------------| Response     |
     |<----------------|              |              |
     | response        |              |              |
```

### Graceful Shutdown

```
  Main        Framework     NATS       Module A     Module B
   |              |          |            |            |
   | Stop()       |          |            |            |
   |------------->|          |            |            |
   |              | Drain    |            |            |
   |              | Subs     |            |            |
   |              |--------->|            |            |
   |              |<---------|            |            |
   |              |          |            |            |
   |              | Stop()   |            |            | (B first if B depends on A)
   |              |---------------------------------->|
   |              |<----------------------------------|
   |              |          |            |            |
   |              | Stop()                |            |
   |              |---------------------->|            |
   |              |<----------------------|            |
   |              |          |            |            |
   |              | Shutdown |            |            |
   |              |--------->|            |            |
   |<-------------|          |            |            |
   | Done         |          |            |            |
```

## Related Documentation

- [Foundation Specification](../spec/foundation.md) - Core principles and detailed specifications
- [README](../../README.md) - Quick start and overview
- [Examples](../../examples/) - Runnable example applications
- [API Reference](https://pkg.go.dev/github.com/go-monolith/mono) - Go documentation
