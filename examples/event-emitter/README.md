# Event Emitter Example

This example demonstrates the **event-driven communication pattern** in the Monolith Framework using `EventEmitterModule` and `EventConsumerModule` interfaces.

## Overview

The example simulates an order management system with two modules:

- **Tracking Module** (Event Emitter) - Creates orders and publishes order-related events
- **Notification Module** (Event Consumer) - Receives order events and sends notifications

The modules communicate entirely through events, demonstrating loose coupling and clear module boundaries.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Mono Application                       │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────────┐         Event Channel    ┌───────┐ │
│  │ Tracking Module  │ ────── OrderCreated ───→ │       │ │
│  │ (EventEmitter)   │         OrderShipped   │ Notif. │ │
│  │                  │                        │ Module │ │
│  │ • CreateOrder()  │                        │ (Event │ │
│  │ • ShipOrder()    │                        │Consumer)
│  └──────────────────┘                        │       │ │
│                                              └───────┘ │
│                                                │       │ │
│                                                └─ Storage
│                                                  (fs-jetstream)
│
│         Embedded NATS (JetStream Enabled)         │ │
├─────────────────────────────────────────────────────────┤
```

## Key Concepts

### Event Emitter Module

The `TrackingModule` implements `EventEmitterModule`:

```go
type TrackingModule struct {
    eventBus mono.EventBus
}

// EmitEvents declares the events this module can emit
func (m *TrackingModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
        OrderShippedV1.ToBase(),
    }
}
```

**Key responsibilities:**
- Declare available events via `EmitEvents()`
- Receive `EventBus` via `SetEventBus()`
- Publish events using typed helpers: `OrderCreatedV1.Publish(eventBus, event, nil)`

### Event Consumer Module

The `NotificationModule` implements `EventConsumerModule`:

```go
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Pattern 1: Type-safe consumer with direct import
    helper.RegisterTypedEventConsumer(registry, tracking.OrderCreatedV1,
        m.handleOrderCreated, m)

    // Pattern 2: Event discovery without direct import
    event, _ := registry.GetEventByName("OrderShipped", "v1", "tracking")
    registry.RegisterEventConsumer(event, m.handleOrderShipped, m)
}
```

**Key responsibilities:**
- Register event consumers via `RegisterEventConsumers()`
- Handle events with callback functions
- Optional: Use event discovery to avoid circular imports

### Two Registration Patterns

This example demonstrates two ways to register event consumers:

1. **Type-Safe Pattern** (Direct Import)
   - Direct dependency on event definition from emitter module
   - Automatic unmarshaling of event data
   - Compile-time type safety
   - Best for modules in same codebase

2. **Discovery Pattern** (Runtime Lookup)
   - No direct dependency on emitter module
   - Manual JSON unmarshaling
   - Runtime dependency resolution
   - Best for avoiding circular imports

## Running the Example

### Prerequisites

- Go 1.25 or higher
- Framework installed: `go get github.com/go-monolith/mono`

### Run the Example

```bash
# From the mono-framework-v0 directory
make run-example-4

# Or manually
cd examples/event-emitter
go run .
```

## Expected Output

When you run the example, you should see:

```
=== Mono-Framework Event Emitter Example ===
Demonstrates: EventEmitterModule, EventConsumerModule, Event Discovery

App created successfully
Storage plugin registered (fs-jetstream)
Tracking module registered (EventEmitterModule)
Notification module registered (EventConsumerModule + UsePluginModule)
Registered modules: [tracking notification]

App started successfully

App Health: healthy=true, nats_healthy=true

Running example scenarios...

[Scenario 1] Creating an order...
Order created: ORD-000001
  [notification] Sending order confirmation notification:
    Order ID: ORD-000001
    Customer: CUST-001
    Product: laptop x 1
    Total: $999.99 USD
    [STORED] Event saved to order-created-events bucket

[Scenario 2] Shipping the order...
Order shipped successfully
  [notification] Sending shipping notification:
    Order ID: ORD-000001
    Tracking: TRK-12345 (FedEx)
    [STORED] Event saved to order-shipped-events bucket

...

=== Notification Summary ===
Total order notifications sent: 2
  1. Order ORD-000001 - Customer CUST-001 - $999.99
  2. Order ORD-000002 - Customer CUST-002 - $79.99
Total shipping notifications sent: 1
  1. Order ORD-000001 - Tracking TRK-12345 (FedEx)

=== Stored Event Files ===
Files in 'order-created-events' bucket: 2
  - ORD-000001_20250105_143022.json (123 bytes)
  - ORD-000002_20250105_143023.json (121 bytes)
Files in 'order-shipped-events' bucket: 1
  - ORD-000001_20250105_143022.json (85 bytes)

Press Ctrl+C to shutdown...
```

## Code Structure

```
event-emitter/
├── main.go                    # Application setup and scenario execution
├── tracking/
│   ├── module.go             # TrackingModule (EventEmitterModule)
│   ├── events.go             # Event definitions (OrderCreatedV1, OrderShippedV1)
│   └── types.go              # Event payload types
└── notification/
    └── module.go             # NotificationModule (EventConsumerModule + UsePluginModule)
```

### Key Files

- **`tracking/events.go`** - Defines `OrderCreatedV1` and `OrderShippedV1` event definitions with schemas
- **`tracking/module.go`** - Implements `EventEmitterModule.EmitEvents()` and provides `CreateOrder()`, `ShipOrder()` methods
- **`notification/module.go`** - Implements `EventConsumerModule.RegisterEventConsumers()` and handles events
- **`main.go`** - Sets up the application, registers modules and plugin, and runs scenarios

## Key Patterns Demonstrated

### 1. Event Declaration

Events are declared as typed definitions with schemas:

```go
// In tracking/events.go
var OrderCreatedV1 = mono.EventDefinition[OrderCreatedEvent]{
    Name:    "OrderCreated",
    Version: "v1",
    Domain:  "tracking",
    // ... schema details
}
```

### 2. Event Publishing

Events are published using type-safe methods:

```go
// Auto-marshals event to JSON using EventDefinition.Publish()
if err := OrderCreatedV1.Publish(m.eventBus, event, nil); err != nil {
    return fmt.Errorf("failed to publish event: %w", err)
}
```

### 3. Event Consumption with Type Safety

Consumers receive strongly-typed events:

```go
func (m *NotificationModule) handleOrderCreated(
    ctx context.Context,
    event tracking.OrderCreatedEvent,
    msg *mono.Msg) error {
    // event is already unmarshaled to OrderCreatedEvent
    fmt.Printf("Order: %s, Customer: %s\n", event.OrderID, event.CustomerID)
}
```

### 4. Plugin Integration

The notification module uses a storage plugin for event persistence:

```go
func (m *NotificationModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "storage" {
        m.storagePlugin = plugin.(*fsjetstream.PluginModule)
    }
}
```

## Comparison with Other Examples

| Pattern | Example | Use Case |
|---------|---------|----------|
| **Event Emitter** | This example | Event-driven architecture, loose coupling |
| **Channel Services** | [analytics](../analytics/README.md) | High-performance in-process communication |
| **Request-Reply Services** | [multi-module](../multi-module/README.md) | Synchronous service calls |
| **Queue Groups** | [multi-module](../multi-module/README.md) | Load-balanced async processing |

## Next Steps

- Review the [Core Concepts > Service Communication](../../docs/official/core-concepts/communication.md) guide for other communication patterns
- Explore the [multi-module example](../multi-module/README.md) for Request-Reply and Queue Group patterns
- Check the [API documentation](https://pkg.go.dev/github.com/go-monolith/mono) for detailed reference

## What You Learned

- ✓ How to implement `EventEmitterModule` to declare events
- ✓ How to implement `EventConsumerModule` to consume events
- ✓ Two patterns for registering event consumers (type-safe and discovery)
- ✓ Event publishing with type-safe `Publish()` methods
- ✓ Event consumption with automatic unmarshaling
- ✓ Plugin integration for event persistence
- ✓ Module communication through loose coupling via events
