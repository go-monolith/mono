# Event Consumers

Event consumers enable **loose coupling** between modules. Unlike services, events do NOT create dependencies between modules. Emitters declare events they publish; any interested module can consume them without knowing about or depending on the emitter.

## What is an Event Consumer?

An event consumer is a handler registered by a module to receive events published by other modules. Think of events as a broadcast mechanism:

```
┌───────────────────────────────────────────────────────────────────────────┐
│                          Event Communication                              │
│                                                                           │
│   ┌───────────────────┐                                                   │
│   │   OrderModule     │                                                   │
│   │   (Emitter)       │                                                   │
│   │                   │                                                   │
│   │  EmitEvents() →   │─── OrderCreatedV1 ──┐                             │
│   │  OrderCreatedV1   │                     │                             │
│   │  OrderShippedV1   │                     │                             │
│   └───────────────────┘                     │                             │
│                                             ▼                             │
│                                    ┌──────────────┐                       │
│                                    │ EventRegistry│ ◄── Framework manages │
│                                    └──────┬───────┘                       │
│                                           │                               │
│         ┌─────────────────────────────────┼───────────────────────────┐   │
│         ▼                                 ▼                           ▼   │
│   ┌─────────────┐                 ┌─────────────┐               ┌───────┐ │
│   │ Notification│                 │  Analytics  │               │ Audit │ │
│   │   Module    │                 │   Module    │               │Module │ │
│   │ (Consumer)  │                 │ (Consumer)  │               │(Cons.)│ │
│   └─────────────┘                 └─────────────┘               └───────┘ │
│                                                                           │
│   Note: OrderModule doesn't know about consumers.                         │
│         Consumers don't declare OrderModule as a dependency.              │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

## Events Do NOT Create Dependencies

This is the **critical difference** between events and services:

| Aspect | Services | Events |
|--------|----------|--------|
| **Dependency** | Consumer must declare dependency on provider | No dependency required |
| **Coupling** | Tight (explicit dependency) | Loose (no coupling) |
| **Direction** | Point-to-point | Broadcast to all interested |
| **Discovery** | Via `SetDependencyServiceContainer` | Via `EventRegistry` |
| **Startup Order** | Provider starts before consumer | Independent (no ordering) |
| **Knowledge** | Consumer knows the provider | Emitter doesn't know consumers |

### Why This Matters

With **services**:
```go
// OrderModule must declare PaymentModule as a dependency
func (m *OrderModule) Dependencies() []string {
    return []string{"payment"}  // Creates dependency
}
```

With **events**:
```go
// NotificationModule does NOT declare OrderModule as a dependency
// It simply registers to consume events from any emitter
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Discover event (no dependency on OrderModule)
    event, _ := registry.GetEventByName("OrderCreated", "v1", "order")
    return registry.RegisterEventConsumer(event, m.handleOrderCreated, m)
}
```

## Event Consumer Patterns

The framework supports two patterns for consuming events:

| Pattern | Interface | Durability | Use Case |
|---------|-----------|------------|----------|
| **EventConsumer** | `EventConsumerHandler` | None (fire-and-forget) | Real-time notifications, low latency |
| **EventStreamConsumer** | `EventStreamConsumerHandler` | JetStream (at-least-once) | Critical events, audit trails |

### Pattern 1: EventConsumer (NATS Core)

Fire-and-forget event consumption via standard NATS subscriptions.

#### When to Use

- Real-time notifications
- Events where occasional message loss is acceptable
- Low-latency pub/sub scenarios
- Simple broadcast patterns

#### Handler Signature

```go
// Raw handler - manual JSON unmarshaling
type EventConsumerHandler func(ctx context.Context, msg *Msg) error

// Typed handler - automatic unmarshaling
type TypedEventConsumerHandler[T any] func(ctx context.Context, event T, msg *Msg) error
```

#### Example

**Emitter Module** (declares events it will emit):

```go
import "github.com/go-monolith/mono/pkg/helper"

// Define the event
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
    "order",        // Module name
    "OrderCreated", // Event name
    "v1",           // Version
)

// Implement EventEmitterModule
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
    }
}

// Publish the event
func (m *OrderModule) createOrder(ctx context.Context, order *Order) error {
    // Business logic...

    // Fire-and-forget publish
    return OrderCreatedV1.Publish(m.eventBus, OrderCreatedEvent{
        OrderID: order.ID,
        Amount:  order.Total,
    }, nil)
}
```

**Consumer Module** (receives events with no dependency):

```go
// Implement EventConsumerModule
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Discover the event from the registry
    eventDef, ok := registry.GetEventByName("OrderCreated", "v1", "order")
    if !ok {
        return fmt.Errorf("event not found: OrderCreated.v1 from order")
    }

    // Register consumer handler
    return registry.RegisterEventConsumer(
        eventDef,
        m.handleOrderCreated,
        m,
    )
}

func (m *NotificationModule) handleOrderCreated(ctx context.Context, msg *mono.Msg) error {
    var event OrderCreatedEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        return err
    }

    // Send notification
    return m.sendEmail(ctx, event.OrderID, event.CustomerEmail)
}
```

#### Using Typed Handlers

For type-safe event consumption with automatic unmarshaling:

```go
import "github.com/go-monolith/mono/pkg/helper"

func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Type-safe registration with automatic unmarshaling
    return helper.RegisterTypedEventConsumer(
        registry,
        order.OrderCreatedV1,  // Import the event definition
        m.handleOrderCreated,
        m,
    )
}

// Handler receives pre-deserialized event
func (m *NotificationModule) handleOrderCreated(
    ctx context.Context,
    event order.OrderCreatedEvent,  // Already unmarshaled!
    msg *mono.Msg,
) error {
    // event is ready to use
    return m.sendEmail(ctx, event.OrderID, event.CustomerEmail)
}
```

#### Queue Groups for Load Balancing

Multiple consumers in the same queue group share event processing:

```go
// Register with queue group
return registry.RegisterEventConsumer(
    eventDef,
    m.handleOrderCreated,
    m,
    "notification-workers",  // Queue group name
)
```

#### Characteristics

| Aspect | Details |
|--------|---------|
| Latency | ~1ms |
| Durability | None (fire-and-forget) |
| Acknowledgment | No |
| Message Loss Risk | Yes (if no consumer available) |
| Use Case | Real-time notifications, UI updates |

---

### Pattern 2: EventStreamConsumer (JetStream)

Durable event consumption via JetStream pull consumers with explicit acknowledgment.

#### When to Use

- Event-Driven Architecture (EDA) patterns
- Message loss is unacceptable
- Need at-least-once delivery guarantee
- Require event replay capability
- Batch processing of events
- Critical business events (payments, orders, audits)

#### Handler Signature

```go
// Raw batch handler - manual JSON unmarshaling
type EventStreamConsumerHandler func(ctx context.Context, msgs []*Msg) error

// Typed batch handler - automatic unmarshaling
type TypedEventStreamConsumerHandler[T any] func(ctx context.Context, events []T, msgs []*Msg) error
```

#### Example

**Emitter Module** (publishes to JetStream for durability):

```go
// Publish to JetStream (persisted)
ack, err := OrderCreatedV1.EventStreamPublish(ctx, m.eventBus, OrderCreatedEvent{
    OrderID: "ORD-001",
    Amount:  99.99,
}, nil)
if err != nil {
    return err
}
fmt.Printf("Published with sequence: %d\n", ack.Sequence())
```

**Consumer Module** (durable consumption with ack/nack):

```go
func (m *AuditModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    eventDef, ok := registry.GetEventByName("OrderCreated", "v1", "order")
    if !ok {
        return fmt.Errorf("event not found")
    }

    config := mono.StreamConsumerConfig{
        Stream: mono.StreamConfig{
            Name:      "audit-order-events",
            Retention: mono.WorkQueuePolicy,
        },
        Fetch: mono.FetchConfig{
            BatchSize:   10,
            IdleTimeout: 5 * time.Second,
        },
    }

    return registry.RegisterEventStreamConsumer(
        eventDef,
        config,
        m.handleOrderEvents,
        m,
    )
}

func (m *AuditModule) handleOrderEvents(ctx context.Context, msgs []*mono.Msg) error {
    for _, msg := range msgs {
        var event OrderCreatedEvent
        if err := json.Unmarshal(msg.Data, &event); err != nil {
            msg.Nak()  // Retry later
            continue
        }

        if err := m.auditLog(ctx, event); err != nil {
            msg.NakWithDelay(5 * time.Second)  // Retry with delay
            continue
        }

        msg.Ack()  // Successfully processed
    }
    return nil
}
```

#### Using Typed Handlers

For type-safe batch event consumption:

```go
import "github.com/go-monolith/mono/pkg/helper"

func (m *AuditModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    config := mono.StreamConsumerConfig{
        Stream: mono.StreamConfig{
            Name:      "audit-order-events",
            Retention: mono.WorkQueuePolicy,
        },
        Fetch: mono.FetchConfig{BatchSize: 10},
    }

    return helper.RegisterTypedEventStreamConsumer(
        registry,
        order.OrderCreatedV1,
        config,
        m.handleOrderEvents,
        m,
    )
}

// Handler receives pre-deserialized events
func (m *AuditModule) handleOrderEvents(
    ctx context.Context,
    events []order.OrderCreatedEvent,  // Already unmarshaled!
    msgs []*mono.Msg,
) error {
    for i, event := range events {
        if err := m.auditLog(ctx, event); err != nil {
            msgs[i].NakWithDelay(5 * time.Second)
            continue
        }
        msgs[i].Ack()
    }
    return nil
}
```

#### Message Acknowledgment

JetStream consumers must explicitly acknowledge messages:

| Method | Effect |
|--------|--------|
| `msg.Ack()` | Message processed successfully, remove from queue |
| `msg.Nak()` | Processing failed, retry immediately |
| `msg.NakWithDelay(d)` | Processing failed, retry after delay |
| `msg.Term()` | Terminate redelivery (poison message) |
| `msg.InProgress()` | Extend processing time, prevent timeout |

#### Characteristics

| Aspect | Details |
|--------|---------|
| Latency | ~5ms (persistence overhead) |
| Durability | JetStream (messages persisted) |
| Acknowledgment | Required (Ack/Nak) |
| Delivery Guarantee | At-least-once |
| Replay Capability | Yes |
| Use Case | Audit trails, compliance, critical events |

---

## EventConsumer vs EventStreamConsumer

| Aspect | EventConsumer | EventStreamConsumer |
|--------|---------------|---------------------|
| **Underlying System** | NATS Core | JetStream |
| **Durability** | None | Messages persisted |
| **Delivery Guarantee** | At-most-once | At-least-once |
| **Acknowledgment** | No | Required |
| **Message Loss Risk** | Yes | No |
| **Latency** | ~1ms | ~5ms |
| **Replay** | No | Yes |
| **Batch Processing** | No | Yes |
| **Best For** | Real-time, non-critical | Critical, auditable |

### Decision Guide

```
Is message loss acceptable?
├── YES → Use EventConsumer
│   Examples: UI updates, real-time analytics, notifications
│
└── NO → Use EventStreamConsumer
    Examples: Payment events, audit trails, order processing
```

---

## Implementing Event Consumer Modules

### Step 1: Implement EventConsumerModule Interface

```go
type NotificationModule struct {
    // No need to store EventRegistry as a field
}

func (m *NotificationModule) Name() string { return "notification" }

func (m *NotificationModule) Start(ctx context.Context) error {
    return nil
}

func (m *NotificationModule) Stop(ctx context.Context) error {
    return nil
}

// Implement EventConsumerModule
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Register your event consumers here
    return nil
}
```

### Step 2: Discover Events via EventRegistry

Two patterns for event discovery:

**Pattern A: Direct Import (Type-Safe)**
```go
import "github.com/go-monolith/mono/pkg/helper"
import "myapp/modules/order"  // Import emitter module's events

func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Type-safe registration with compile-time checks
    return helper.RegisterTypedEventConsumer(
        registry,
        order.OrderCreatedV1,  // Direct reference
        m.handleOrderCreated,
        m,
    )
}
```

**Pattern B: Runtime Discovery (Decoupled)**
```go
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Runtime discovery - no import needed
    eventDef, ok := registry.GetEventByName("OrderCreated", "v1", "order")
    if !ok {
        return fmt.Errorf("event not found: OrderCreated.v1 from order")
    }

    return registry.RegisterEventConsumer(eventDef, m.handleOrderCreated, m)
}
```

### Step 3: Write Event Handlers

```go
// For EventConsumer
func (m *NotificationModule) handleOrderCreated(ctx context.Context, msg *mono.Msg) error {
    var event OrderCreatedEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        return err  // Error logged by framework
    }

    // Process the event
    return m.sendNotification(ctx, event)
}

// For EventStreamConsumer (batch)
func (m *AuditModule) handleOrderEvents(ctx context.Context, msgs []*mono.Msg) error {
    for _, msg := range msgs {
        // Process each message
        // Must acknowledge each message!
        msg.Ack()
    }
    return nil
}
```

---

## Implementing Event Emitter Modules

### Step 1: Define Event Definitions

```go
import "github.com/go-monolith/mono/pkg/helper"

// Define typed event definition
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
    "order",        // Module name (domain)
    "OrderCreated", // Event name
    "v1",           // Version
)

// Event payload
type OrderCreatedEvent struct {
    OrderID    string  `json:"order_id"`
    CustomerID string  `json:"customer_id"`
    Amount     float64 `json:"amount"`
}
```

### Step 2: Implement EventEmitterModule Interface

```go
type OrderModule struct {
    eventBus mono.EventBus
}

// Required: Implement EventBusAwareModule (embedded in EventEmitterModule)
func (m *OrderModule) SetEventBus(bus mono.EventBus) {
    m.eventBus = bus
}

// Required: Declare events this module emits
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
        OrderShippedV1.ToBase(),
    }
}
```

### Step 3: Publish Events

```go
func (m *OrderModule) createOrder(ctx context.Context, order *Order) error {
    // Business logic...

    // Option 1: Fire-and-forget (NATS Core)
    err := OrderCreatedV1.Publish(m.eventBus, OrderCreatedEvent{
        OrderID:    order.ID,
        CustomerID: order.CustomerID,
        Amount:     order.Total,
    }, nil)

    // Option 2: Durable publish (JetStream)
    ack, err := OrderCreatedV1.EventStreamPublish(ctx, m.eventBus, event, nil)
    if err != nil {
        return err
    }
    log.Printf("Published with sequence: %d", ack.Sequence())

    return err
}
```

---

## EventRegistry Interface

The `EventRegistry` provides event discovery and consumer registration:

```go
type EventRegistry interface {
    // Event Discovery
    GetEventByName(name, version, moduleName string) (BaseEventDefinition, bool)
    GetEventsByModule(moduleName string) []BaseEventDefinition
    GetAllEvents() []BaseEventDefinition

    // Consumer Registration
    RegisterEventConsumer(eventDef, handler, module, queueGroup...) error
    RegisterEventStreamConsumer(eventDef, config, handler, module) error
}
```

### Event Discovery Methods

```go
// Find specific event by name, version, and emitter module
event, found := registry.GetEventByName("OrderCreated", "v1", "order")

// List all events from a specific module
events := registry.GetEventsByModule("order")

// List all registered events
allEvents := registry.GetAllEvents()
```

---

## Best Practices

### Do

- **Use EventStreamConsumer for critical events**: Payments, orders, audits
- **Keep handlers idempotent**: Events may be redelivered
- **Use queue groups for scaling**: Load balance across consumer instances
- **Version your events**: `v1`, `v2` for evolution
- **Handle errors gracefully**: Log and continue, or Nak for retry

### Don't

- **Don't create circular imports**: Use runtime discovery if needed
- **Don't rely on event ordering**: Events may arrive out of order
- **Don't store EventRegistry**: Use it only in `RegisterEventConsumers`
- **Don't forget to acknowledge**: EventStreamConsumer requires explicit Ack/Nak
- **Don't assume single delivery**: Design for at-least-once semantics

---

## Complete Example

See the [event-emitter example](../../../examples/event-emitter/README.md) for a complete implementation showing:

- Event definition and registration
- EventEmitterModule implementation
- EventConsumerModule implementation
- Both type-safe and discovery patterns
- Plugin integration for event persistence

---

## Summary

- **Event consumers** receive events without creating dependencies on emitters
- **EventConsumer** is fire-and-forget via NATS Core (~1ms latency)
- **EventStreamConsumer** provides durable delivery via JetStream (~5ms latency)
- Use **EventRegistry** for event discovery and consumer registration
- Emitters declare events via `EmitEvents()`, consumers register via `RegisterEventConsumers()`
- Events enable **loose coupling** between modules

## Next Steps

- Learn about [Services](services.md) for point-to-point communication
- Explore [Inter-Module Communication](inter-module-communication.md) for all patterns
- Review [Module Interfaces](modules.md) including EventEmitterModule and EventConsumerModule

---

Now you understand event consumers! Continue to [Services](services.md) for point-to-point communication patterns.
