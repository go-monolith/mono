# Event Emitter Example

Two modules that communicate only through events: the **Tracking** module (`EventEmitterModule`) creates and ships orders and publishes `OrderCreated` and `OrderShipped` events; the **Notification** module (`EventConsumerModule`) consumes them, prints a notification and stores each event as a JSON file through the fs-jetstream storage plugin.
It shows how to declare, publish and consume typed events, and two ways of registering a consumer: a type-safe one and one that discovers the event at runtime.

```
┌─────────────────────────────────────────────────────────────┐
│                      Mono Application                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐                      ┌─────────────┐  │
│  │ Tracking Module  │  ── OrderCreated ──▶ │ Notif.      │  │
│  │ (EventEmitter)   │  ── OrderShipped ──▶ │ Module      │  │
│  │                  │                      │ (Consumer)  │  │
│  │ • CreateOrder()  │                      │             │  │
│  │ • ShipOrder()    │                      │  ↓ Storage  │  │
│  └──────────────────┘                      └─────────────┘  │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│              Embedded NATS (JetStream Enabled)              │
└─────────────────────────────────────────────────────────────┘
```

## Running the Example

Prerequisites: Go 1.26 or higher, and a clone of this repository (the example is built from the framework's own Go module).

```bash
cd examples/event-emitter
go run .

# or, from the repository root:
go run ./examples/event-emitter
```

The program runs three scenarios (create an order, ship it, create another), prints a summary and the stored files, then waits for Ctrl+C.

## Code Structure

```
event-emitter/
├── main.go                    # Application setup, plugin registration, scenario execution
├── tracking/
│   ├── module.go             # TrackingModule (EventEmitterModule): EmitEvents(), CreateOrder(), ShipOrder()
│   ├── events.go             # Event definitions (OrderCreatedV1, OrderShippedV1)
│   └── types.go              # Event payload types
└── notification/
    └── module.go             # NotificationModule (EventConsumerModule + UsePluginModule): RegisterEventConsumers() and handlers
```

## Key Patterns

### 1. Event Declaration

Events are declared as typed definitions:

```go
// In tracking/events.go
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
    "tracking", "OrderCreated", "v1", // module, name, version -> subject events.tracking.v1.order-created
)
```

### 2. Emitting and Publishing

The `TrackingModule` declares its events via `EmitEvents()`, receives the `EventBus` via `SetEventBus()`, and publishes with the typed `Publish` method, which marshals the event to JSON:

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

// In CreateOrder:
if err := OrderCreatedV1.Publish(m.eventBus, event, nil); err != nil {
    return fmt.Errorf("failed to publish event: %w", err)
}
```

### 3. Consuming: Two Registration Patterns

The `NotificationModule` registers its consumers in `RegisterEventConsumers()`:

```go
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Pattern 1: Type-safe consumer with direct import
    if err := helper.RegisterTypedEventConsumer(registry, tracking.OrderCreatedV1,
        m.handleOrderCreated, m); err != nil {
        return err
    }

    // Pattern 2: Event discovery by name, version and module
    event, ok := registry.GetEventByName("OrderShipped", "v1", "tracking")
    if !ok {
        return fmt.Errorf("OrderShipped v1 not found")
    }
    return registry.RegisterEventConsumer(event, func(ctx context.Context, msg *mono.Msg) error {
        var e tracking.OrderShippedEvent
        if err := json.Unmarshal(msg.Data, &e); err != nil {
            return err
        }
        return m.handleOrderShipped(ctx, e, msg)
    }, m)
}
```

1. **Type-Safe Pattern** (direct import of the event definition)
   - Automatic unmarshaling of event data and compile-time type safety
   - Best for modules in the same codebase

2. **Discovery Pattern** (runtime lookup)
   - No dependency on the emitter's event definition variable (looked up at runtime by name/version/module); the payload type must still match. Here it is imported from `tracking` for brevity, but a consumer may declare its own struct
   - Manual JSON unmarshaling
   - Best for avoiding circular imports

With the type-safe pattern the handler receives the unmarshaled event:

```go
func (m *NotificationModule) handleOrderCreated(
    ctx context.Context,
    event tracking.OrderCreatedEvent,
    msg *mono.Msg) error {
    // event is already unmarshaled to OrderCreatedEvent
    fmt.Printf("Order: %s, Customer: %s\n", event.OrderID, event.CustomerID)
    return nil
}
```

### 4. Plugin Integration

The notification module receives the storage plugin (registered with alias `storage`) and persists every event:

```go
func (m *NotificationModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "storage" {
        if storagePlugin, ok := plugin.(*fsjetstream.PluginModule); ok {
            m.storagePlugin = storagePlugin
        }
    }
}
```

## Expected Output

Framework and plugin log lines are interleaved and omitted here; the order of the module list printed from `app.Modules()` varies, and file names and sizes depend on the run.

```
=== Mono-Framework Event Emitter Example ===
Demonstrates: EventEmitterModule, EventConsumerModule, Event Discovery

App created successfully
Storage plugin registered (fs-jetstream)
Tracking module registered (EventEmitterModule)
Notification module registered (EventConsumerModule + UsePluginModule)
Registered modules: [notification tracking]

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
  - ORD-000001_20261117_205806.json (143 bytes)
  - ORD-000002_20261117_205806.json (144 bytes)
Files in 'order-shipped-events' bucket: 1
  - ORD-000001_20261117_205806.json (83 bytes)

Press Ctrl+C to shutdown...
```

## Next Steps

| Pattern | Example | Use Case |
|---------|---------|----------|
| **Event Emitter** | This example | Event-driven architecture, loose coupling |
| **Channel Services** | [analytics](../analytics/README.md) | High-performance in-process communication |
| **Request-Reply Services** | [multi-module](../multi-module/README.md) | Synchronous service calls |
| **Queue Groups** | [multi-module](../multi-module/README.md) | Load-balanced async processing |

- Review the [Core Concepts > Inter-Module Communication](../../docs/official/core-concepts/inter-module-communication.md) guide for other communication patterns
- Check the [API documentation](https://pkg.go.dev/github.com/go-monolith/mono) for detailed reference
