# Framework Architecture

This guide explains how the Monolith Framework is organized and how all the components work together.

## Layered Architecture

The framework is organized into three logical layers:

```
┌────────────────────────────────────────────────────────┐
│                 Application Layer                       │
│       (Your Modules implementing Module interface)      │
└────────────────────────────────────────────────────────┘
                           ↑
                           │ uses
                           ↓
┌────────────────────────────────────────────────────────┐
│                  Framework Layer                        │
│  ┌──────────────────┬───────────────┬─────────────────┐ │
│  │ ServiceContainer │   EventBus    │  EventRegistry  │ │
│  │  (DI, Services)  │  (Pub/Sub)    │ (EDA,Consumers) │ │
│  └──────────────────┴───────────────┴─────────────────┘ │
└────────────────────────────────────────────────────────┘
                           ↑
                           │ uses
                           ↓
┌────────────────────────────────────────────────────────┐
│              Infrastructure Layer                       │
│  ┌──────────────────────────────────────────────────┐  │
│  │   Embedded NATS Server with JetStream Support   │  │
│  └──────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Application (MonoApplication)

The top-level entry point that coordinates everything:

```go
app, _ := mono.NewMonoApplication(
    mono.WithLogLevel(mono.LogLevelInfo),
    mono.WithJetStreamStorageDir("/tmp/jetstream"),
)

app.Register(&MyModule{})
app.Start(context.Background())
```

**Responsibilities**:
- Create and manage NATS server
- Register and initialize modules
- Manage module lifecycle
- Coordinate shutdown
- Provide health status

### 2. Module Registry

Maintains the list of registered modules and their metadata:

```
┌──────────────────┐
│  Module Registry │
├──────────────────┤
│ • order-module   │
│ • payment-module │
│ • email-module   │
└──────────────────┘
```

**Handles**:
- Module registration
- Dependency resolution
- Startup ordering
- Metadata management

### 3. Service Container (Dependency Injection)

Each module gets a ServiceContainer for registering and accessing services:

```go
func (m *MyModule) RegisterServices(container mono.ServiceContainer) error {
    // Register my service
    container.RegisterRequestReplyService("my-service", m.handleRequest)

    // Access other services
    otherService := container.RequestReplyServiceClient(...)
    return nil
}
```

**Provides**:
- Service registration
- Dependency injection
- Four service patterns (Channel, Request-Reply, Queue Group, Stream Consumer)
- Service discovery

### 4. EventBus (NATS Wrapper)

Abstracts NATS operations for services and events:

```go
func (m *MyModule) SetEventBus(bus mono.EventBus) {
    // Publish to a subject
    bus.Publish("events.order.created", eventData)

    // Subscribe to events
    sub, _ := bus.Subscribe("events.>")
}
```

**Provides**:
- Pub/sub messaging
- Request/reply patterns
- JetStream support
- Subject-based routing

### 5. NATS Server (Embedded)

The message broker running inside your application:

```
Your Application
├─ Framework
│  ├─ MonoApplication
│  ├─ Module Registry
│  └─ EventBus
│
└─ NATS Server
   ├─ Message Broker
   ├─ Core Subjects
   ├─ JetStream (optional)
   │  ├─ Streams
   │  ├─ Consumers
   │  └─ KV Stores
   └─ Plugins
```

**Features**:
- Listens on localhost:4222 (default)
- Supports clustering
- JetStream for persistence (optional)
- Built-in KV store and object store

### 6. Logger

Structured logging with module context:

```go
logger := app.Logger()
logger.Info("application started")

moduleLogger := logger.WithModule("order")
moduleLogger.Info("processing order", "orderID", "ORD-001")
```

**Features**:
- Structured (JSON or text format)
- Module context
- Sensitive data redaction
- Configurable log level

## Module Startup Sequence

When `app.Start()` is called, the framework executes this sequence for each module (in dependency order):

```
1. Create ServiceContainer
   ├─ Register framework services (Logger, EventBus, NATSManager)
   └─ Prepare for module-specific services

2. Call SetDependencyServiceContainer()
   └─ Modules access dependency services from other modules

3. Call SetEventBus()
   └─ Modules receive EventBus for publishing

4. Call RegisterServices()
   ├─ Module registers its services
   └─ Framework registers with NATS subjects (services.module.service)

5. Collect EmitEvents()
   └─ Framework discovers events this module publishes

6. Call Start()
   ├─ Module performs initialization (open files, start goroutines)
   └─ Return error to abort startup

7. Register Subscriptions
   ├─ Create NATS subscriptions for services
   └─ Create event subscriptions for consumers

8. Set Module Ready
   └─ Module is now ready to receive requests/events
```

## Subject Naming Conventions

The framework uses standardized NATS subject patterns:

### Service Subjects (Request-Reply)

Pattern: `services.<module>.<service>`

Examples:
- `services.payment.process-payment`
- `services.order.create-order`
- `services.email.send-email`

**Properties**:
- Framework manages subject names (no wildcards needed)
- Unique per module (module namespacing)
- Defined at registration time

### Event Subjects (Pub/Sub)

Pattern: `events.<domain>.<event-type>`

Examples:
- `events.order.OrderCreated`
- `events.order.OrderShipped`
- `events.payment.PaymentProcessed`

**Properties**:
- Wildcards supported: `events.order.>` (all order events)
- Human-readable naming
- Subject-based routing

### Framework Subjects

Pattern: `_framework.<component>.<operation>`

Examples:
- `_framework.health.check`
- `_framework.config.reload`

**Properties**:
- Reserved for framework use
- Modules cannot publish here
- Internal only

## Data Flow Examples

### Request-Reply Pattern

```
Module A                    NATS                      Module B
(Customer)              Message Broker               (Service)
    │                          │                           │
    │──────── request ─────────→│                           │
    │                          │────────────→ Process
    │                          │               Service
    │                          │←────────────
    │←──────── response ────────│
    │                          │
```

### Queue Group Pattern

```
Module A                    NATS                  Module B (Distributed)
(Publisher)             Message Queue         ┌─ Worker 1
    │                      │                  │
    ├─ message 1 ─→ Queue  ├─ message 1 ──→ Worker 2
    │                      │                  │
    ├─ message 2 ─→        ├─ message 2 ──→ Worker 3
    │                      │
    └─ message 3 ─→        └─ message 3 ──→ Worker 1 (round-robin)
```

### Event Pattern (Broadcast)

```
Module A                    NATS                Module B
(Emitter)              Event Bus         (Consumer 1)
    │                      │                    │
    ├─ OrderCreated ─→ Topic ─────────→ Handle Event
    │                      │
    │                      ├─────────→ Module C
    │                      │           (Consumer 2)
    │                      │
    │                      └─────────→ Module D
                                       (Consumer 3)
```

## Graceful Shutdown Sequence

When `app.Stop()` is called:

```
1. Signal Stop
   └─ Application begins shutdown

2. Stop Accepting New Work
   └─ No new subscriptions or requests accepted

3. Drain In-Flight Operations
   ├─ Wait for active message processing (with timeout)
   └─ ~10 seconds default

4. Unsubscribe from NATS
   └─ Remove all subject subscriptions

5. Stop Modules (Reverse Order)
   ├─ For each module in REVERSE dependency order:
   │  └─ Call Stop()
   │
   └─ Cleanup resources (close files, release locks)

6. Stop NATS Server
   └─ Shutdown embedded NATS server

7. Return
   └─ Application fully stopped
```

## Key Design Decisions

### 1. **Embedded NATS**

NATS is embedded, not external:

✓ **Advantages**:
- No external infrastructure needed
- Simple deployment (single binary)
- Zero-copy message passing (same process)
- Easy testing

✗ **Trade-offs**:
- Can't scale horizontally (single process)
- Not suitable for extreme performance (use HTTP/gRPC instead)

### 2. **Modules Before Services**

Modules are the primary abstraction:

✓ **Why**:
- Clear organizational unit
- Independent testability
- Lifecycle management
- Dependency tracking

✗ **Not**:
- Services or APIs directly
- Microservices architecture
- Function-based organization

### 3. **Pub/Sub Over RPC**

NATS pub/sub for inter-module communication:

✓ **Why**:
- Loose coupling
- Scalable (queue groups)
- Flexible (events, request-reply)
- Built-in persistence (JetStream)

✗ **Not**:
- Direct function calls
- HTTP/gRPC
- Shared databases

### 4. **Functional Options Pattern**

Configuration via option functions:

```go
app, _ := mono.NewMonoApplication(
    mono.WithLogLevel(mono.LogLevelInfo),
    mono.WithJetStreamStorageDir("/tmp/js"),
    mono.WithNATSPort(4222),
)
```

✓ **Why**:
- Backward compatible
- Composable
- Type-safe
- Clear intent

### 5. **Dependency Resolution at Startup**

Not runtime dependency discovery:

✓ **Why**:
- Fail fast (errors detected immediately)
- Clear startup sequence
- No runtime surprises
- Deterministic ordering

## Configuration Options

The framework provides configuration for common scenarios:

| Option | Example | Default |
|--------|---------|---------|
| `WithLogLevel()` | `LogLevelDebug` | `LogLevelInfo` |
| `WithLogFormat()` | `LogFormatJSON` | `LogFormatText` |
| `WithNATSPort()` | `4222` | `4222` |
| `WithJetStreamEnabled()` | `true` | `false` |
| `WithJetStreamStorageDir()` | `/tmp/js` | `""` |
| `WithShutdownTimeout()` | `10*time.Second` | `30*time.Second` |

## Extension Points

The framework is extensible:

### Custom Middleware

Intercept module lifecycle events:

```go
type MyMiddleware struct {}

func (m *MyMiddleware) RegisterServices(...) { ... }
func (m *MyMiddleware) Start(...) { ... }
```

### Plugins

Provide shared functionality to modules:

```go
app.RegisterPlugin(&StoragePlugin{}, "storage")
```

### Custom Loggers

Implement your own logger:

```go
app.WithLogger(myCustomLogger)
```

## Performance Characteristics

Typical performance on commodity hardware:

| Operation | Latency | Throughput |
|-----------|---------|-----------|
| Channel Service | ~1µs | >1M msg/s |
| Request-Reply | ~1ms | ~10K msg/s |
| Queue Group | ~1ms | ~10K msg/s |
| Stream Consumer | ~5ms | ~5K msg/s |

Note: Actual performance depends on message size, system load, and configuration.

## Deployment Model

The framework is designed for monolith deployment:

```
┌─────────────────────────────────┐
│    Single Deployable Unit       │
│  ┌──────────────────────────┐   │
│  │  Your Application        │   │
│  │  ┌────────────────────┐  │   │
│  │  │ Order Module       │  │   │
│  │  │ Payment Module     │  │   │
│  │  │ Email Module       │  │   │
│  │  └────────────────────┘  │   │
│  │         ↓                │   │
│  │  ┌────────────────────┐  │   │
│  │  │  Embedded NATS     │  │   │
│  │  │  with JetStream    │  │   │
│  │  └────────────────────┘  │   │
│  └──────────────────────────┘   │
└─────────────────────────────────┘
         ↓
    One Docker Image
    One Binary
    One Process
```

## Summary

- **Layered**: Application, Framework, Infrastructure
- **Component-based**: ServiceContainer, EventBus, EventRegistry, NATS
- **Module-centric**: Modules are the organizational unit
- **Message-driven**: NATS pub/sub for communication
- **Managed lifecycle**: Framework handles startup and shutdown
- **Extensible**: Middleware, plugins, custom loggers
- **Production-ready**: Health checks, structured logging, graceful shutdown

## Next Steps

- Review [modules guide](modules.md) for detailed module information
- Study [service communication](inter-module-communication.md) patterns
- Explore [examples](../../../examples/) for practical implementations

---

You now understand how the Monolith Framework is organized! Continue with the [examples](../../../examples/) to see it in action.
