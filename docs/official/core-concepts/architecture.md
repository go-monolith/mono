# Framework Architecture

This page explains how the Monolith Framework is organized: its layers and components, the order in which it starts and stops modules, and the subject names it uses.

**When to read it:** after [Modules](modules.md) and [Services](services.md), when you want to know what happens around your modules at startup and shutdown.

## Layered Architecture

The framework is organized into three logical layers:

```
┌──────────────────────────────────────────────────────┐
│                  Application Layer                   │
│     (Your Modules implementing Module interface)     │
└──────────────────────────────────────────────────────┘
                          ↑
                          │ uses
                          ↓
┌──────────────────────────────────────────────────────┐
│                   Framework Layer                    │
│  ┌────────────────┬──────────────┬────────────────┐  │
│  │ServiceContainer│   EventBus   │ EventRegistry  │  │
│  │ (DI, Services) │  (Pub/Sub)   │(EDA,Consumers) │  │
│  └────────────────┴──────────────┴────────────────┘  │
└──────────────────────────────────────────────────────┘
                          ↑
                          │ uses
                          ↓
┌──────────────────────────────────────────────────────┐
│                Infrastructure Layer                  │
│  ┌────────────────────────────────────────────────┐  │
│  │  Embedded NATS Server with JetStream Support   │  │
│  └────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
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

Maintains the list of registered modules and their metadata.

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
    return container.RegisterRequestReplyService("my-service", m.handleRequest)
}
```

Services of other modules are reached through the containers passed to `SetDependencyServiceContainer(name, container)`, e.g. `client, err := m.payment.GetRequestReplyService("process-payment")`.

**Provides**:
- Service registration
- Dependency injection
- Five service patterns (Channel, Request-Reply, Queue Group, Stream Consumer, Cron)
- Service discovery

### 4. EventBus (NATS Wrapper)

Abstracts NATS operations for services and events:

```go
func (m *MyModule) SetEventBus(bus mono.EventBus) {
    m.bus = bus
}

func (m *MyModule) Start(ctx context.Context) error {
    // Publish to a subject
    if err := m.bus.Publish("events.order.v1.order-created", eventData); err != nil {
        return err
    }

    // Subscribe to events
    _, err := m.bus.Subscribe("events.>", func(ctx context.Context, msg *mono.Msg) {
        // ...
    })
    return err
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
│  ├─ EventBus
│  └─ Plugins
│
└─ NATS Server
   ├─ Message Broker
   ├─ Core Subjects
   ├─ JetStream (optional)
   │  ├─ Streams
   │  ├─ Consumers
   │  └─ KV Stores
```

**Features**:
- Listens on 127.0.0.1:4222 (default)
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
- Redaction of sensitive configuration values in the audit middleware
- Configurable log level

## Module Startup Sequence

When `app.Start()` is called, the framework first runs these steps once, before any module:

```
a. Wait until JetStream answers (bounded by WithJetStreamReadyTimeout)
b. Start plugins
c. Start middleware
d. Resolve the dependency order
e. Collect EmitEvents() from every EventEmitterModule into the EventRegistry
```

Then, for each module in dependency order:

```
1. Create and bind its ServiceContainer
2. Inject plugins (UsePluginModule.SetPlugin)
3. Call SetDependencyServiceContainer() with read-only dependency views
4. Validate ConsumeServices() (ServiceConsumerModule)
5. Call SetEventBus() with the module-scoped bus
6. Call RegisterServices()
7. Call RegisterEventConsumers(registry)
8. Call Start()
   └─ An error rolls back the modules already started, then the started middleware, then the plugins
```

After all modules have started, the framework starts the channel routers, then creates the NATS subscriptions, worker pools and JetStream consumers for every registered service (of the modules and the plugins) and event consumer. A failure of any step after the plugins started stops the started modules, middleware and plugins, in that order.

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

Pattern: `events.<module>.<version>.<event-name-kebab>` (default computed by `helper.EventDefinition`)

Examples:
- `events.order.v1.order-created`
- `events.order.v1.order-shipped`
- `events.payment.v1.payment-processed`

**Properties**:
- Wildcards supported: `events.order.>` (all order events)
- Human-readable naming
- Subject-based routing

### Framework Subjects

Pattern: `_mono.<component>.<operation>`

Reserved prefix `_mono.`; event definitions and service subjects using it are rejected.

**Properties**:
- Reserved for framework use
- Modules must not publish here (the event bus does not currently reject such publishes)
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
   ├─ Fetch loops (stream, event stream, cron) stop fetching
   └─ Subscriptions are drained module by module in reverse dependency order (no new deliveries)

3. Wait for In-Flight Handlers
   ├─ Buffered messages still reach the worker pools
   ├─ Running handlers finish with a LIVE context
   ├─ Bounded by WithShutdownDrainTimeout (default 30s, capped at the shutdown timeout, minus a reserve for module Stop)
   └─ What has not finished is counted and logged

4. Cancel Runtime Context
   └─ Anything still running is told to stop

5. Stop Modules (Reverse Order)
   ├─ For each module in REVERSE dependency order:
   │  └─ Call Stop()
   │
   ├─ Then middleware modules (reverse registration order), then plugins last
   └─ Cleanup resources (close files, release locks)

6. Stop NATS
   ├─ Drain the client connection (pending replies are delivered)
   └─ Shutdown embedded NATS server

7. Return
   └─ Application fully stopped
```

`WithShutdownTimeout` (30s default) bounds steps 2-5 when the context given to `Stop` has no deadline; step 6 (NATS) runs outside it and takes at most about 5s for the connection drain plus the server shutdown. Keep the drain timeout below the shutdown timeout, and both below your orchestrator's termination grace period, so module `Stop` always gets to run.

## Key Design Decisions

### 1. **Embedded NATS**

NATS is embedded, not external:

✓ **Advantages**:
- No external infrastructure needed
- Simple deployment (single binary)
- No network hop: modules talk to the embedded server over loopback (or in-process with `WithNATSInProcessConn`)
- Easy testing

✗ **Trade-offs**:
- Scaling out means running several instances as a NATS cluster (`WithNATSClustering`), which adds cluster operations
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

### 3. **NATS Messaging Over Direct Calls**

Modules communicate through NATS (events and request-reply), not by calling each other:

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
| `WithNATSHost()` | `0.0.0.0` | `127.0.0.1` |
| `WithNATSPort()` | `4222` | `4222` |
| `WithJetStreamStorageDir()` | `/tmp/js` | unset (JetStream disabled); setting it enables JetStream |
| `WithShutdownTimeout()` | `10*time.Second` | `30*time.Second` |

## Extension Points

The framework is extensible:

### Custom Middleware

Intercept service and event consumer registrations, outgoing messages and lifecycle events:

```go
type MyMiddleware struct{}

func (m *MyMiddleware) OnServiceRegistration(ctx context.Context, reg mono.ServiceRegistration) mono.ServiceRegistration {
    return reg
}
// ... plus Name/Start/Stop and the other MiddlewareModule hooks
```

### Plugins

Provide shared functionality to modules:

```go
app.RegisterPlugin(&StoragePlugin{}, "storage")
```

### Custom Loggers

Implement your own logger:

```go
app, err := mono.NewMonoApplication(mono.WithLogger(myCustomLogger))
```

## Performance Characteristics

Indicative latencies:

| Operation | Latency |
|-----------|---------|
| Channel Service | ~1µs |
| Request-Reply | ~1ms |
| Queue Group | ~1ms |

Actual performance depends on message size, system load, and configuration.

## Deployment Model

The framework is designed for monolith deployment:

```
┌───────────────────────────────────┐
│      Single Deployable Unit       │
│  ┌─────────────────────────────┐  │
│  │     Your Application        │  │
│  │  ┌───────────────────────┐  │  │
│  │  │ Order Module          │  │  │
│  │  │ Payment Module        │  │  │
│  │  │ Email Module          │  │  │
│  │  └───────────────────────┘  │  │
│  │             ↓               │  │
│  │  ┌───────────────────────┐  │  │
│  │  │    Embedded NATS      │  │  │
│  │  │    with JetStream     │  │  │
│  │  └───────────────────────┘  │  │
│  └─────────────────────────────┘  │
└───────────────────────────────────┘
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
- Explore [examples](../../../examples/basic/README.md) for practical implementations

---

You now understand how the Monolith Framework is organized! Continue with the [examples](../../../examples/basic/README.md) to see it in action.
