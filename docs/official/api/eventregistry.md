# EventRegistry API

API documentation for the `EventRegistry` interface, which manages event definitions and consumer registrations for the framework's event-driven communication system.

## Signatures

```go
// Event registration (called by emitter modules)
func RegisterEvent(def BaseEventDefinition) error

// Event discovery
func GetEventByName(name string, version string, moduleName string) (BaseEventDefinition, bool)
func GetEventsByModule(moduleName string) []BaseEventDefinition
func GetAllEvents() []BaseEventDefinition

// Consumer registration
func RegisterEventConsumer(eventDef BaseEventDefinition, handler EventConsumerHandler, module Module, queueGroup ...string) error
func RegisterEventStreamConsumer(eventDef BaseEventDefinition, config StreamConsumerConfig, handler EventStreamConsumerHandler, module Module) error

// Consumer inspection
func Entries() []EventConsumerEntry
func StreamConsumerEntries() []EventStreamConsumerEntry
```

{% hint style="info" %}
**Consumer Selection:** Use `RegisterEventConsumer` for fire-and-forget events (~1ms, no persistence). Use `RegisterEventStreamConsumer` for critical events requiring at-least-once delivery with JetStream persistence.
{% endhint %}

## Overview

EventRegistry serves as the central catalog for all events in the application, enabling:
- Event producers to declare the events they will emit
- Event consumers to discover and subscribe to events without direct dependencies
- The framework to wire up NATS subscriptions automatically

Unlike `ServiceContainer` which creates explicit module dependencies, EventRegistry enables **loose coupling**: emitters don't know their consumers, and consumers don't declare dependencies on emitters. This makes events ideal for broadcast notifications and decoupled architectures.

## Initialization Sequence

The registry is used during framework initialization in this order:
1. `EventEmitterModule`s register their event definitions via `RegisterEvent()`
2. `EventConsumerModule`s receive the registry via `RegisterEventConsumers(registry)`
3. Consumer modules discover events via `GetEventByName()` during `RegisterEventConsumers()`
4. Consumers register handlers via `RegisterEventConsumer()` or `RegisterEventStreamConsumer()`
5. Framework creates NATS subscriptions from `Entries()` and `StreamConsumerEntries()`

## Event Registration Methods

### RegisterEvent

```go
func (registry EventRegistry) RegisterEvent(def BaseEventDefinition) error
```

Registers an event definition that this module will emit. Called by modules implementing `EventEmitterModule`.

**Parameters:**
- `def` - The event definition containing name, version, module name, and subject

**Returns:**
- `error` - Error if an event with the same name, version, and module already exists

**Example:**
```go
// In your event emitter module
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        mono.MustNewEventDefinition[OrderCreatedEvent]("OrderCreated", "v1", m).ToBase(),
        mono.MustNewEventDefinition[OrderCancelledEvent]("OrderCancelled", "v1", m).ToBase(),
    }
}
```

{% hint style="warning" %}
**Event Subject Format:** Events follow the subject pattern `events.<module>.<version>.<event-name-kebab>`. For example, `events.order.v1.order-created`.
{% endhint %}

## Event Discovery Methods

### GetEventByName

```go
func (registry EventRegistry) GetEventByName(name string, version string, moduleName string) (BaseEventDefinition, bool)
```

Returns the event definition by name, version, and emitting module name.

**Parameters:**
- `name` - Event name (e.g., "OrderCreated")
- `version` - Event version (e.g., "v1")
- `moduleName` - Name of the module that emits this event

**Returns:**
- `BaseEventDefinition` - The event definition if found
- `bool` - True if found, false otherwise

**Example:**
```go
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Discover the event by name, version, and emitting module
    eventDef, found := registry.GetEventByName("OrderCreated", "v1", "order")
    if !found {
        return fmt.Errorf("event not found: OrderCreated.v1 from order")
    }

    return registry.RegisterEventConsumer(eventDef, m.handleOrderCreated, m)
}
```

### GetEventsByModule

```go
func (registry EventRegistry) GetEventsByModule(moduleName string) []BaseEventDefinition
```

Returns all events registered by a specific module.

**Parameters:**
- `moduleName` - Name of the emitting module

**Returns:**
- `[]BaseEventDefinition` - Slice of all events from that module

**Example:**
```go
// List all events from the order module
events := registry.GetEventsByModule("order")
for _, event := range events {
    fmt.Printf("Event: %s.%s, Subject: %s\n", event.Name, event.Version, event.Subject)
}
```

### GetAllEvents

```go
func (registry EventRegistry) GetAllEvents() []BaseEventDefinition
```

Returns all registered event definitions across all modules.

**Returns:**
- `[]BaseEventDefinition` - Slice of all registered events

**Example:**
```go
// List all events in the system
for _, event := range registry.GetAllEvents() {
    fmt.Printf("Module: %s, Event: %s.%s\n", event.ModuleName, event.Name, event.Version)
}
```

## Consumer Registration Methods

### RegisterEventConsumer

```go
func (registry EventRegistry) RegisterEventConsumer(
    eventDef BaseEventDefinition,
    handler EventConsumerHandler,
    module Module,
    queueGroup ...string,
) error
```

Registers a fire-and-forget consumer for an event using NATS Core pub/sub.

{% hint style="warning" %}
**Fire-and-Forget Semantics:** NATS Core consumers don't detect "no responder" errors. There is a risk of message loss when no consumers are available. Use `RegisterEventStreamConsumer` for critical events.
{% endhint %}

**Parameters:**
- `eventDef` - The event definition to consume
- `handler` - Handler function for processing events
- `module` - The consuming module instance
- `queueGroup` - Optional queue group name (defaults to module name)

**Handler Signature:**
```go
type EventConsumerHandler func(ctx context.Context, msg *Msg) error
```

**Returns:**
- `error` - Nil on success

**Use Cases:**
- Real-time notifications where occasional loss is acceptable
- High-throughput event processing
- Low-latency requirements (~1ms)

**Example:**
```go
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    eventDef, found := registry.GetEventByName("OrderCreated", "v1", "order")
    if !found {
        return fmt.Errorf("event not found")
    }

    return registry.RegisterEventConsumer(eventDef, m.handleOrderCreated, m)
}

func (m *NotificationModule) handleOrderCreated(ctx context.Context, msg *mono.Msg) error {
    var event OrderCreatedEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        return err
    }

    // Send notification
    return m.sendEmail(event.CustomerEmail, "Order confirmed: "+event.OrderID)
}
```

**Queue Groups Example:**
```go
// Multiple instances with same queue group share the load
registry.RegisterEventConsumer(eventDef, m.handler, m, "notification-workers")
```

**Typed Handler (helper):**

For type-safe handlers with automatic unmarshaling, use `TypedEventConsumerHandler`:

```go
type TypedEventConsumerHandler[T any] func(ctx context.Context, event T, msg *Msg) error
```

### RegisterEventStreamConsumer

```go
func (registry EventRegistry) RegisterEventStreamConsumer(
    eventDef BaseEventDefinition,
    config StreamConsumerConfig,
    handler EventStreamConsumerHandler,
    module Module,
) error
```

Registers a JetStream durable consumer for an event with at-least-once delivery guarantees.

**Parameters:**
- `eventDef` - The event definition to consume
- `config` - JetStream stream and consumer configuration
- `handler` - Batch handler function for processing event batches
- `module` - The consuming module instance

**Handler Signature:**
```go
type EventStreamConsumerHandler func(ctx context.Context, msgs []*Msg) error
```

Messages should be acknowledged individually using `Ack()`, `Nak()`, `NakWithDelay()`, `Term()`, or `InProgress()`.

**Returns:**
- `error` - Nil on success

**Use Cases:**
- Payment processing
- Audit logs
- Compliance events
- Any event where loss is unacceptable

**Example:**
```go
func (m *AuditModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    eventDef, found := registry.GetEventByName("OrderCreated", "v1", "order")
    if !found {
        return fmt.Errorf("event not found")
    }

    config := mono.StreamConsumerConfig{
        Stream: mono.StreamConfig{
            Name:      "audit-stream",
            Retention: mono.WorkQueuePolicy,
        },
        Consumer: mono.ConsumerConfig{
            Name:          "audit-consumer",
            MaxAckPending: 1000,
        },
        Fetch: mono.FetchConfig{
            BatchSize: 10,
        },
    }

    return registry.RegisterEventStreamConsumer(eventDef, config, m.handleOrderBatch, m)
}

func (m *AuditModule) handleOrderBatch(ctx context.Context, msgs []*mono.Msg) error {
    for _, msg := range msgs {
        var event OrderCreatedEvent
        if err := json.Unmarshal(msg.Data, &event); err != nil {
            msg.Term() // Terminate malformed messages
            continue
        }

        if err := m.logAuditEntry(&event); err != nil {
            msg.NakWithDelay(5 * time.Second) // Retry later
            continue
        }

        msg.Ack() // Acknowledge successful processing
    }
    return nil
}
```

**Typed Handler (helper):**

For type-safe batch handlers, use `TypedEventStreamConsumerHandler`:

```go
type TypedEventStreamConsumerHandler[T any] func(ctx context.Context, events []T, msgs []*Msg) error
```

## Consumer Inspection Methods

### Entries

```go
func (registry EventRegistry) Entries() []EventConsumerEntry
```

Returns all registered NATS Core event consumers.

**EventConsumerEntry Structure:**
```go
type EventConsumerEntry struct {
    EventDef   BaseEventDefinition    // The event being consumed
    Handler    EventConsumerHandler   // The handler function
    Module     Module                 // The consuming module
    QueueGroup string                 // NATS queue group name
}
```

**Example:**
```go
for _, entry := range registry.Entries() {
    fmt.Printf("Consumer: module=%s, event=%s, queue=%s\n",
        entry.Module.Name(),
        entry.EventDef.Name,
        entry.QueueGroup,
    )
}
```

### StreamConsumerEntries

```go
func (registry EventRegistry) StreamConsumerEntries() []EventStreamConsumerEntry
```

Returns all registered JetStream event consumers.

**EventStreamConsumerEntry Structure:**
```go
type EventStreamConsumerEntry struct {
    EventDef   BaseEventDefinition        // The event being consumed
    Config     StreamConsumerConfig       // JetStream configuration
    Handler    EventStreamConsumerHandler // The batch handler
    Module     Module                     // The consuming module
    SequenceID int                        // Unique consumer ID
}
```

**Example:**
```go
for _, entry := range registry.StreamConsumerEntries() {
    fmt.Printf("Stream Consumer: module=%s, event=%s, stream=%s\n",
        entry.Module.Name(),
        entry.EventDef.Name,
        entry.Config.Stream.Name,
    )
}
```

## Configuration Table

| Property | Type | Description | Example |
|----------|------|-------------|---------|
| Event Name | `string` | Unique event identifier | `"OrderCreated"` |
| Event Version | `string` | Semantic version | `"v1"` |
| Module Name | `string` | Emitting module name | `"order"` |
| Queue Group | `string` | Load balancing group | `"workers"` |

## Default Configuration

| Setting | Default |
|---------|---------|
| Queue Group | Module name |
| Stream Retention | WorkQueuePolicy |
| Max Ack Pending | 1000 |
| Batch Size | 10 |

## Examples

### Complete Event Emitter Module

```go
// events.go
type OrderCreatedEvent struct {
    OrderID     string    `json:"order_id"`
    CustomerID  string    `json:"customer_id"`
    TotalAmount float64   `json:"total_amount"`
    CreatedAt   time.Time `json:"created_at"`
}

var OrderCreatedV1 = mono.MustNewEventDefinition[OrderCreatedEvent]("OrderCreated", "v1", nil)

// module.go
type OrderModule struct {
    eventBus mono.EventBus
}

func (m *OrderModule) Name() string { return "order" }

func (m *OrderModule) SetEventBus(bus mono.EventBus) {
    m.eventBus = bus
}

func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.WithModule(m).ToBase(),
    }
}

func (m *OrderModule) createOrder(ctx context.Context, order *Order) error {
    // ... create order logic ...

    // Publish event
    event := OrderCreatedEvent{
        OrderID:     order.ID,
        CustomerID:  order.CustomerID,
        TotalAmount: order.Total,
        CreatedAt:   time.Now(),
    }

    return m.eventBus.Publish(ctx, OrderCreatedV1.WithModule(m).ToBase(), event)
}
```

### Complete Event Consumer Module

```go
type NotificationModule struct{}

func (m *NotificationModule) Name() string { return "notification" }

func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Discover event from order module
    eventDef, found := registry.GetEventByName("OrderCreated", "v1", "order")
    if !found {
        return fmt.Errorf("event OrderCreated.v1 from order module not found")
    }

    // Register consumer
    return registry.RegisterEventConsumer(eventDef, m.handleOrderCreated, m)
}

func (m *NotificationModule) handleOrderCreated(ctx context.Context, msg *mono.Msg) error {
    var event OrderCreatedEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        return fmt.Errorf("invalid event data: %w", err)
    }

    fmt.Printf("Sending notification for order %s\n", event.OrderID)
    return nil
}

func (m *NotificationModule) Start(ctx context.Context) error { return nil }
func (m *NotificationModule) Stop(ctx context.Context) error  { return nil }
```

### Multiple Consumers for Same Event

```go
func (m *AnalyticsModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    eventDef, found := registry.GetEventByName("OrderCreated", "v1", "order")
    if !found {
        return fmt.Errorf("event not found")
    }

    // Real-time metrics (fire-and-forget)
    if err := registry.RegisterEventConsumer(eventDef, m.updateMetrics, m, "metrics-workers"); err != nil {
        return err
    }

    // Durable audit log (JetStream)
    config := mono.StreamConsumerConfig{
        Stream: mono.StreamConfig{Name: "analytics"},
        Fetch:  mono.FetchConfig{BatchSize: 50},
    }
    return registry.RegisterEventStreamConsumer(eventDef, config, m.persistAnalytics, m)
}
```

## Best Practices

### Do

- Define events with clear names and version strings
- Use semantic versioning for events (v1, v2, etc.)
- Keep event payloads small and focused
- Use queue groups for horizontal scaling
- Use `RegisterEventStreamConsumer` for critical events
- Handle malformed messages gracefully with `Term()`

### Don't

- Import the emitter module directly (use discovery)
- Assume events will always be delivered (NATS Core)
- Block indefinitely in event handlers
- Ignore acknowledgment in stream consumers
- Use events for request-response patterns (use ServiceContainer)

## Related Documentation

- [EventBus API](eventbus.md) - Publishing events
- [Module API](module.md) - EventEmitterModule and EventConsumerModule interfaces
- [Service Container API](container.md) - Service-based communication
- [Inter-Module Communication](../core-concepts/inter-module-communication.md) - Patterns overview

---

For more information, see [Foundation Specification](../../spec/foundation.md) and [Core Concepts](../core-concepts/README.md).
