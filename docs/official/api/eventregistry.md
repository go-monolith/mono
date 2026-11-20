# EventRegistry API

The `EventRegistry` is the catalog of the application's events. Emitting modules declare their events in it, and consuming modules use it to find those events and register handlers.

**When to read this page:** when a module consumes events from another module (it implements `EventConsumerModule`). To publish events, see [Publishing events](eventbus.md#publishing-events) on the EventBus page.

## Signatures

```go
// Event registration (called by the framework with each module's EmitEvents())
func RegisterEvent(def BaseEventDefinition) error

// Event discovery
func GetEventByName(name string, version string, moduleName string) (BaseEventDefinition, bool)
func GetEventsByModule(moduleName string) []BaseEventDefinition
func GetAllEvents() []BaseEventDefinition

// Consumer registration
func RegisterEventConsumer(eventDef BaseEventDefinition, handler EventConsumerHandler, module Module, queueGroup ...string) error
func RegisterEventConsumerWithOptions(eventDef BaseEventDefinition, handler EventConsumerHandler, module Module, opts EventConsumerOptions) error
func RegisterEventStreamConsumer(eventDef BaseEventDefinition, config StreamConsumerConfig, handler EventStreamConsumerHandler, module Module) error

// Consumer inspection
func Entries() []EventConsumerEntry
func StreamConsumerEntries() []EventStreamConsumerEntry
```

{% hint style="info" %}
**Consumer Selection:**
- `RegisterEventConsumer`: fire-and-forget events over NATS Core, no persistence (indicative latency ~1ms).
- `RegisterEventStreamConsumer`: critical events that need at-least-once delivery, persisted in JetStream.
{% endhint %}

## Overview

The registry lets modules exchange events with **loose coupling**:

- emitters declare the events they publish and don't know their consumers;
- consumers find events by name and don't declare a dependency on the emitter;
- the framework creates the NATS subscriptions and JetStream consumers.

This differs from `ServiceContainer`, where a caller declares an explicit dependency on the provider.

## Initialization Sequence

1. The framework registers each `EventEmitterModule`'s `EmitEvents()` via `RegisterEvent()`
2. `EventConsumerModule`s receive the registry via `RegisterEventConsumers(registry)`
3. Consumer modules discover events via `GetEventByName()` during `RegisterEventConsumers()`
4. Consumers register handlers via `RegisterEventConsumer()` or `RegisterEventStreamConsumer()`
5. Framework creates NATS subscriptions from `Entries()` and `StreamConsumerEntries()`

## Event Registration Methods

### RegisterEvent

```go
func (registry EventRegistry) RegisterEvent(def BaseEventDefinition) error
```

Registers an event definition. The framework calls it for each definition returned by `EventEmitterModule.EmitEvents()`, for all modules, before any module starts. Modules normally do not call it.

**Parameters:**
- `def` - The event definition containing name, version, module name, and subject

**Returns:**
- `error` - Error if an event with the same name, version, and module already exists

**Example:**
```go
import "github.com/go-monolith/mono/pkg/helper"

// In your event emitter module
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        helper.EventDefinition[OrderCreatedEvent]("order", "OrderCreated", "v1").ToBase(),
        helper.EventDefinition[OrderCancelledEvent]("order", "OrderCancelled", "v1").ToBase(),
    }
}
```

{% hint style="warning" %}
**Event Subject Format:** Events follow the subject pattern `events.<module>.<version>.<event-name-kebab>`. For example, `events.order.v1.order-created`. This is the subject `helper.EventDefinition` computes when none is given. A custom subject may be passed as the 4th argument. It must be kebab-case, contain the version token, have no wildcards and not start with `_mono.`, or `helper.EventDefinition` panics.
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

Each registration method has a typed helper in `pkg/helper` that unmarshals the payload for you. Use the typed helper when you can import the emitter's contract package; use the raw method with `GetEventByName` when you discover the event at runtime.

| Raw method | Typed helper | Typed handler |
|------------|--------------|---------------|
| `RegisterEventConsumer` | `helper.RegisterTypedEventConsumer` | `TypedEventConsumerHandler[T]` |
| `RegisterEventConsumerWithOptions` | `helper.RegisterTypedEventConsumerWithOptions` | `TypedEventConsumerHandler[T]` |
| `RegisterEventStreamConsumer` | `helper.RegisterTypedEventStreamConsumer` | `TypedEventStreamConsumerHandler[T]` |

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

**Typed (recommended):**

```go
type TypedEventConsumerHandler[T any] func(ctx context.Context, event T, msg *Msg) error
```

```go
// contract is the order module's contract package (myapp/modules/order/contract)
err := helper.RegisterTypedEventConsumer(registry, contract.OrderCreatedV1, m.handleOrderCreated, m)
```

`helper.RegisterTypedEventConsumer` takes an optional trailing queue group. An unmarshal error is returned as the handler error.

**Raw:**

```go
type EventConsumerHandler func(ctx context.Context, msg *Msg) error
```

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

A non-nil handler error is logged (`Event consumer handler error`). Core NATS events are not redelivered.

**Queue groups:**

```go
// Multiple instances with same queue group share the load
registry.RegisterEventConsumer(eventDef, m.handler, m, "notification-workers")
```

Two registrations of the same event in the same module share the default queue group (the module name), so each event reaches only one of them. Give them different queue groups to have both run.

### RegisterEventConsumerWithOptions

```go
func (registry EventRegistry) RegisterEventConsumerWithOptions(
    eventDef BaseEventDefinition,
    handler EventConsumerHandler,
    module Module,
    opts EventConsumerOptions,
) error
```

`RegisterEventConsumer` with explicit options: the queue group and the bounds of the worker pool that runs the handler on this instance.

```go
type EventConsumerOptions struct {
    QueueGroup string // "" = module name
    ServiceOptions    // MaxConcurrency (0 = 1, serial), MaxPending, PendingMsgs, PendingBytes
}
```

- Event handlers are **serial by default**: one event at a time, in publish order, as with `RegisterEventConsumer`.
- Set `MaxConcurrency` above 1 to handle events concurrently when the handler does not depend on order (see [Handler Concurrency](container.md#handler-concurrency-worker-pools)).
- Events carry no reply subject, so a full pool does not reject them: the subscription waits for a slot instead.
- Negative values fail registration.

**Typed (recommended):** `helper.RegisterTypedEventConsumerWithOptions(registry, eventDef, handler, module, opts)`.

**Raw:**
```go
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    eventDef, found := registry.GetEventByName("OrderCreated", "v1", "order")
    if !found {
        return fmt.Errorf("event not found")
    }

    // Each notification is independent and waits on an external API:
    // handle up to 16 at once instead of one at a time.
    return registry.RegisterEventConsumerWithOptions(eventDef, m.notifyOrderCreated, m, mono.EventConsumerOptions{
        ServiceOptions: mono.ServiceOptions{MaxConcurrency: 16},
    })
}
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

Registers a JetStream durable consumer for an event, with at-least-once delivery. Use it for payments, audit logs, compliance events: any event where loss is unacceptable.

**Parameters:**
- `eventDef` - The event definition to consume
- `config` - JetStream stream and consumer configuration
- `handler` - Batch handler function for processing event batches
- `module` - The consuming module instance

The handler must acknowledge each message with `Ack()`, `Nak()`, `NakWithDelay()`, `Term()`, or `InProgress()`.

**Typed (recommended):**

```go
type TypedEventStreamConsumerHandler[T any] func(ctx context.Context, events []T, msgs []*Msg) error
```

```go
// contract is the order module's contract package (myapp/modules/order/contract)
err := helper.RegisterTypedEventStreamConsumer(registry, contract.OrderCreatedV1, config, m.handleOrderBatch, m)
```

If any message in a batch fails to unmarshal, the handler is not called and the batch is returned as an error, unacknowledged. It is redelivered after `AckWait`, until `MaxDeliver`.

**Raw:**

```go
type EventStreamConsumerHandler func(ctx context.Context, msgs []*Msg) error
```

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
            // The durable name is set by the framework:
            // "audit-order-OrderCreated-v1" for the module named "audit".
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

**Durable Consumer Name:**

The framework names the durable JetStream consumer `<consumer-module>-<event-module>-<event>-<version>`, for example `analytics-billing-PaymentProcessed-v1`. Any `Consumer.Name` or `Consumer.Durable` you set is overridden. `types.EventStreamConsumerName` (package `github.com/go-monolith/mono/pkg/types`) returns the name.

- The name depends only on which module consumes which event, so it is the same on every
  restart and does not change when modules are added or removed.
- The consumer is a true JetStream durable: the server keeps it however long it goes without
  pull requests, unless you set `Consumer.InactiveThreshold`.
- If the same module registers the same event on the same stream more than once, the second
  and later registrations get a `-2`, `-3`, ... suffix, in the order that module registers them.

**Several events on one stream, renames and removed registrations:**

- Event stream consumers for different events may name the same stream. Since v0.2.13 the stream stores all of their events and each handler receives only its own; before, only the last registration's event was stored. See [Sharing a Stream Between Registrations](container.md#sharing-a-stream-between-registrations).
- Renaming the consuming module creates a new durable and leaves the old one on the server. Since v0.2.13 it is reported at every start. See [Renaming or Removing a Stream Registration](container.md#renaming-or-removing-a-stream-registration).

**Storage type and retention of an existing stream:**

NATS cannot change a stream's `Storage`, or change its `Retention` to or from `WorkQueuePolicy`, once the stream exists. Since v0.2.12 such a configuration fails `Start` with `ErrStreamConfigImmutable`, and so do two registrations of the same stream that disagree on one of them. The stream is never deleted. See [Changing a Stream's Storage Type or Retention](container.md#changing-a-streams-storage-type-or-retention) for the way out.

**Upgrading from v0.0.11 or earlier:** see [Upgrading Legacy Consumers](#upgrading-legacy-consumers) at the end of this page.

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
    Options    ServiceOptions         // Worker pool bounds (zero = defaults; serial)
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
    SequenceID int                        // Process-local registration ID (not part of the durable name)
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

## Default Configuration

| Setting | Default |
|---------|---------|
| Queue Group | Module name |
| Stream Retention | LimitsPolicy |
| Stream Storage | FileStorage |
| Ack Policy | AckExplicitPolicy |
| Max Ack Pending | server default (1000) |
| Batch Size | 10 |
| Fetch Timeout | 5s |

## Examples

### Complete Event Emitter Module

```go
// events.go
import "github.com/go-monolith/mono/pkg/helper"

type OrderCreatedEvent struct {
    OrderID     string    `json:"order_id"`
    CustomerID  string    `json:"customer_id"`
    TotalAmount float64   `json:"total_amount"`
    CreatedAt   time.Time `json:"created_at"`
}

var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent]("order", "OrderCreated", "v1")

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
        OrderCreatedV1.ToBase(),
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

    return OrderCreatedV1.Publish(m.eventBus, event, nil)
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

- Import the emitter module's own packages (import its contract package, or use discovery)
- Assume events will always be delivered (NATS Core)
- Block indefinitely in event handlers
- Ignore acknowledgment in stream consumers
- Use events for request-response patterns (use ServiceContainer)

## Upgrading Legacy Consumers

This section applies when upgrading from v0.0.11 or earlier.

Earlier versions named the consumer `<event-module>-<event>-<version>-<N>`. `N` was a counter
that followed module start order, which was not deterministic, so the name could change
between restarts. Those consumers were also created without a durable name, so the server
treated them as non-durable. The first startup on the new version handles any legacy durable
it finds on the same stream. Once every consumer on a stream is set up, it marks the stream
as migrated with the metadata entry `mono.legacy-consumer-migration: done`, and later
startups only report leftovers:

- **Work-queue streams:** the legacy durable is deleted before the new one is created. A
  work-queue stream accepts only one unfiltered consumer, so this is the only way to start.
  Messages it had not acknowledged stay in the stream and are delivered to the new durable.
  If the consumer configuration is one the server rejects on a work-queue stream (a deliver
  policy other than all, or acknowledgement other than explicit), startup fails before
  anything is deleted.
- **Other streams:** when the old position is unambiguous (one legacy durable, or several
  that stopped at the same point), the new durable is positioned just after it, so the stream
  is not replayed. This requires a deliver policy of all, by-start-sequence or by-start-time;
  for other deliver policies a warning reports what was not carried over.
- **Replay over loss:** legacy names do not record which module owned a durable. In the
  following cases the new durable keeps its configured start, and a warning is logged:
  - **Several modules consume the same event on the same stream:** a module may have no old
    position of its own.
  - **A module's legacy durables stopped at different positions:** for example, a name that
    switched between restarts, or one left by a module removed in the same deploy.

  Messages the stream still holds are then delivered again, so a handler that is not
  idempotent sees duplicates; one that sums payments double-counts them. If you know which
  legacy durable is stale, remove it before upgrading so the remaining position is carried
  over exactly.
- **Swapping the consumer of an event in one deploy:** names cannot catch this case. If a
  single deploy removes the only module consuming an event on a stream and adds a different
  one, the new module starts at the removed module's position. Split such a swap across two
  deploys, or remove the legacy durable before upgrading.
- **The framework does not delete legacy durables** on non-work-queue streams. A warning on
  every startup names each one together with a `nats consumer rm <stream> <name>` command.
  Remove them once the new durable is confirmed to be working: on an interest-retention stream
  they keep retaining messages until removed. Since v0.2.0 the framework raises their
  inactivity threshold to 1h at startup (nats-server v2.14.10 would otherwise delete them about
  5s after the server starts, before the migration), so the server itself removes a legacy
  durable that is no longer pulled 1h after its last pull.
- **Consumers created after the migration** start from their configured deliver policy. This
  covers a new module consuming the same event, or a durable an operator deleted to force a
  replay. Such a consumer is never moved to a legacy position.
- **Rolling upgrade with several replicas:**
  - **Interest and limits streams:** old replicas keep consuming through their legacy durable
    while new ones use the new durable. This can cause duplicates, never loss.
  - **Work-queue streams:** the first new replica deletes the legacy durable the old replicas
    fetch from. Their fetch loops log errors until they are replaced; the unacknowledged
    messages stay in the stream for the new durable.
- **Rolling back** to an older version: it picks up the legacy durables again from their old
  ack floors, which replays everything processed since the upgrade. On a work-queue stream it
  fails to start until the new durable is deleted.

## Related Documentation

- [EventBus API](eventbus.md) - Publishing events
- [Module API](module.md) - EventEmitterModule and EventConsumerModule interfaces
- [Service Container API](container.md) - Service-based communication
- [Inter-Module Communication](../core-concepts/inter-module-communication.md) - Patterns overview

---

For more information, see [Foundation Specification](https://github.com/go-monolith/mono/blob/main/docs/spec/foundation.md) and [Core Concepts](../core-concepts/README.md).
