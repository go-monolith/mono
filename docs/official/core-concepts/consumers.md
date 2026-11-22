# Event Consumers

This page explains how modules react to events published by other modules: how events differ from services, how to emit an event, and the two ways to consume one (NATS Core or JetStream).

**When to read it:** before writing a module that implements `EventEmitterModule` or `EventConsumerModule`. For the full method reference, see the [EventRegistry API](../api/eventregistry.md).

## What is an Event Consumer?

An event consumer is a handler a module registers to receive events published by other modules. Events are a broadcast mechanism: emitters declare the events they publish, and any interested module can consume them without knowing about or depending on the emitter.

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
    event, ok := registry.GetEventByName("OrderCreated", "v1", "order")
    if !ok {
        return fmt.Errorf("event not found: OrderCreated.v1 from order")
    }
    return registry.RegisterEventConsumer(event, m.handleOrderCreated, m)
}
```

## Choosing a Pattern

The framework supports two patterns for consuming events:

| Aspect | EventConsumer | EventStreamConsumer |
|--------|---------------|---------------------|
| **Handler** | `EventConsumerHandler` | `EventStreamConsumerHandler` (batch) |
| **Underlying System** | NATS Core | JetStream |
| **Durability** | None (fire-and-forget) | Messages persisted |
| **Delivery Guarantee** | At-most-once | At-least-once |
| **Acknowledgment** | No | Required |
| **Message Loss Risk** | Yes (if no consumer is available) | No |
| **Latency (indicative)** | ~1ms | Higher (persistence) |
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

## Emitting Events

Consumers need an event to consume. An emitter module defines the event, declares it in `EmitEvents()`, and publishes it.

### Emitter Step 1: Define Event Definitions

```go
import "github.com/go-monolith/mono/pkg/helper"

// Define typed event definitions
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
    "order",        // Module name (domain)
    "OrderCreated", // Event name
    "v1",           // Version
)

var OrderShippedV1 = helper.EventDefinition[OrderShippedEvent]("order", "OrderShipped", "v1")

// Event payload
type OrderCreatedEvent struct {
    OrderID    string  `json:"order_id"`
    CustomerID string  `json:"customer_id"`
    Amount     float64 `json:"amount"`
}

type OrderShippedEvent struct {
    OrderID string `json:"order_id"`
}
```

### Emitter Step 2: Implement EventEmitterModule Interface

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

### Emitter Step 3: Publish Events

```go
func (m *OrderModule) createOrder(ctx context.Context, order *Order) error {
    // Business logic...

    event := OrderCreatedEvent{
        OrderID:    order.ID,
        CustomerID: order.CustomerID,
        Amount:     order.Total,
    }

    // Option 1: Fire-and-forget (NATS Core)
    if err := OrderCreatedV1.Publish(m.eventBus, event, nil); err != nil {
        return err
    }

    // Option 2: Durable publish (JetStream)
    ack, err := OrderCreatedV1.EventStreamPublish(ctx, m.eventBus, event, nil)
    if err != nil {
        return err
    }
    log.Printf("Published with sequence: %d", ack.Sequence())

    return nil
}
```

---

## Pattern 1: EventConsumer (NATS Core)

Fire-and-forget event consumption via standard NATS subscriptions. Use it for real-time notifications and other events where occasional loss is acceptable.

There is nothing to acknowledge: a NATS Core event is never redelivered, so `msg.Ack()` and `Nak()` do nothing here, and the handler's error is only logged. See [Who Acknowledges a Message](../api/container.md#who-acknowledges-a-message).

### Typed Handler (recommended)

Import the emitter's contract package and register with `helper.RegisterTypedEventConsumer`. The payload arrives already unmarshaled:

```go
import (
    "github.com/go-monolith/mono/pkg/helper"

    "myapp/modules/order/contract" // the emitter's contract package
)

func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Type-safe registration with automatic unmarshaling
    return helper.RegisterTypedEventConsumer(
        registry,
        contract.OrderCreatedV1,  // Import the event definition
        m.handleOrderCreated,
        m,
    )
}

// Handler receives pre-deserialized event
func (m *NotificationModule) handleOrderCreated(
    ctx context.Context,
    event contract.OrderCreatedEvent,  // Already unmarshaled!
    msg *mono.Msg,
) error {
    // event is ready to use
    return m.sendEmail(ctx, event.OrderID, event.CustomerEmail)
}
```

### Raw Handler (runtime discovery)

Find the event with `GetEventByName` and unmarshal the payload yourself:

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

### Queue Groups for Load Balancing

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

### Concurrency and Ordering

Event consumers are serial by default: each instance handles one event at a time, in arrival order. To handle events concurrently, register with options:

```go
return registry.RegisterEventConsumerWithOptions(eventDef, m.handleOrderCreated, m, mono.EventConsumerOptions{
    QueueGroup:     "notification-workers",
    ServiceOptions: mono.ServiceOptions{MaxConcurrency: 16},
})
```

`helper.RegisterTypedEventConsumerWithOptions` does the same for typed handlers. A full pool holds events back; it does not drop them. Two registrations of one event in one module share the default queue group (the module name), so each event reaches only one of them: give each its own queue group.

---

## Pattern 2: EventStreamConsumer (JetStream)

Durable event consumption via JetStream pull consumers with explicit acknowledgment. Use it when message loss is unacceptable (payments, orders, audits), or when you need replay or batch processing.

Publish these events to JetStream with `EventStreamPublish` (see [Emitting Events](#emitting-events)).

Each registration gets a durable consumer named `<consumer-module>-<event-module>-<event>-<version>`, for example `analytics-billing-PaymentProcessed-v1`. The name does not change across restarts or when other modules are added or removed, so the consumer resumes where it left off. If you are upgrading from v0.0.11 or earlier, see [Upgrading Legacy Consumers](../api/eventregistry.md#upgrading-legacy-consumers) for how existing durables are carried over.

### Typed Handler (recommended)

```go
import (
    "github.com/go-monolith/mono/pkg/helper"

    "myapp/modules/order/contract" // the emitter's contract package
)

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
        contract.OrderCreatedV1,
        config,
        m.handleOrderEvents,
        m,
    )
}

// Handler receives pre-deserialized events
func (m *AuditModule) handleOrderEvents(
    ctx context.Context,
    events []contract.OrderCreatedEvent,  // Already unmarshaled!
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

### Raw Handler (runtime discovery)

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
            Timeout:   5 * time.Second,
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
            msg.Term()  // Malformed: retrying cannot help
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

### Message Acknowledgment

Event stream consumer handlers acknowledge their messages themselves; the framework does not acknowledge them for them (unlike cron services, whose occurrences the framework acknowledges automatically, see [Who Acknowledges a Message](../api/container.md#who-acknowledges-a-message)):

| Method | Effect |
|--------|--------|
| `msg.Ack()` | Message processed successfully; not redelivered (removed from the stream only under work-queue or interest retention) |
| `msg.Nak()` | Processing failed, retry immediately |
| `msg.NakWithDelay(d)` | Processing failed, retry after delay |
| `msg.Term()` | Terminate redelivery (poison message) |
| `msg.InProgress()` | Extend processing time, prevent timeout |

---

## Implementing Event Consumer Modules

### Step 1: Implement the EventConsumerModule Interface

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

### Step 2: Choose How to Find the Event

- **Direct import (type-safe):** import the emitter's contract package and use the typed helpers, as in the typed examples above. Mistakes show up at compile time.
- **Runtime discovery (decoupled):** call `registry.GetEventByName(name, version, module)` and register a raw handler, as in the raw examples above. No import of the emitter is needed.

The registry also lists events (full interface in the [EventRegistry API](../api/eventregistry.md)):

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
- **Don't rely on ordering across instances or with `MaxConcurrency` > 1**: one instance handles a serial consumer's events in arrival order, but the instances of a queue group share them, and JetStream redelivery reorders them
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
- **EventConsumer** is fire-and-forget via NATS Core
- **EventStreamConsumer** provides durable, at-least-once delivery via JetStream
- Use **EventRegistry** for event discovery and consumer registration
- Emitters declare events via `EmitEvents()`, consumers register via `RegisterEventConsumers()`
- Events enable **loose coupling** between modules

## Next Steps

- Learn about [Services](services.md) for point-to-point communication
- Explore [Inter-Module Communication](inter-module-communication.md) for all patterns
- Review [Module Interfaces](modules.md) including EventEmitterModule and EventConsumerModule

---

Now you understand event consumers! Continue to [Services](services.md) for point-to-point communication patterns.
