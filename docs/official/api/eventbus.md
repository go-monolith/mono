# EventBus API

API documentation for the `EventBus` interface, which provides publish/subscribe messaging between modules using NATS.

## Signatures

```go
// Publishing
func Publish(subject string, data []byte) error
func PublishMsg(msg *Msg) error

// Request-Reply
func Request(subject string, data []byte, timeout time.Duration) (*Msg, error)
func RequestWithContext(ctx context.Context, subject string, data []byte) (*Msg, error)

// Subscriptions
func Subscribe(subject string, handler MsgHandler) (Subscription, error)
func QueueSubscribe(subject, queue string, handler MsgHandler) (Subscription, error)

// JetStream
func EventStream() (EventStream, error)
```

{% hint style="info" %}
**Fire-and-forget vs Persistent:** Use `Publish()` for best-effort delivery. For guaranteed at-least-once delivery, enable JetStream and use `EventStream().Publish()`.
{% endhint %}

## Overview

The `EventBus` enables loosely-coupled inter-module communication through event publishing and subscription. Events are published to subjects and consumed by registered handlers without direct module-to-module dependencies.

## Handler Types

### MsgHandler

```go
type MsgHandler func(ctx context.Context, msg *Msg)
```

Callback function for processing messages asynchronously.

## Publishing Methods

### Publish

```go
func (eb EventBus) Publish(subject string, data []byte) error
```

Publishes raw byte data to the specified subject. Fire-and-forget semantics.

**Parameters:**
- `subject` - Event subject (e.g., `events.order.created`)
- `data` - Raw byte payload (typically JSON-serialized)

**Returns:**
- `error` - Nil on success, error if NATS unreachable

**Use Cases:**
- Fire-and-forget event broadcasting
- Low-latency event distribution
- Best-effort delivery (no persistence)

**Example:**
```go
eventData, _ := json.Marshal(&OrderCreatedEvent{
    OrderID: "ORD-001",
    Total:   99.99,
})
err := eventBus.Publish("events.order.created", eventData)
```

### PublishMsg

```go
func (eb EventBus) PublishMsg(msg *Msg) error
```

Publishes a complete message with headers to the specified subject.

**Parameters:**
- `msg` - Message containing Subject, Data, and optional Header

**Returns:**
- `error` - Nil on success, error if NATS unreachable

**Use Cases:**
- Publishing with custom headers (tracing, request IDs)
- Propagating metadata with events

**Example:**
```go
eventData, _ := json.Marshal(&OrderCreatedEvent{OrderID: "ORD-001"})
err := eventBus.PublishMsg(&mono.Msg{
    Subject: "events.order.created",
    Data:    eventData,
    Header: mono.Header{
        "x-request-id": []string{"req-123"},
        "x-trace-id":   []string{"trace-456"},
    },
})
```

## Subscription Methods

### Subscribe

```go
func (eb EventBus) Subscribe(subject string, handler MsgHandler) (Subscription, error)
```

Creates an asynchronous subscription with a message handler.

**Parameters:**
- `subject` - Subject pattern (supports wildcards: `*` and `>`)
- `handler` - Callback function for messages

**Returns:**
- `Subscription` - Active subscription handle
- `error` - Nil on success

{% hint style="info" %}
**Wildcard Patterns:**
- `events.order.created` - Exact match
- `events.order.*` - Single-level wildcard (matches `events.order.created`, `events.order.updated`)
- `events.>` - Multi-level wildcard (matches all subjects starting with `events.`)
{% endhint %}

**Subject Patterns:**
- `events.order.created` - Exact match
- `events.order.*` - Single-level wildcard (all order events)
- `events.>` - Multi-level wildcard (all events)

**Example:**
```go
sub, err := eventBus.Subscribe("events.order.created", func(ctx context.Context, msg *mono.Msg) {
    var event OrderCreatedEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        return
    }
    fmt.Printf("Order created: %s\n", event.OrderID)
})
if err != nil {
    return err
}
// Later: sub.Unsubscribe() or sub.Drain()
```

### SubscribeSync

```go
func (eb EventBus) SubscribeSync(subject string) (Subscription, error)
```

Creates a synchronous subscription for pull-based message consumption.

**Returns:**
- `Subscription` - Subscription with `NextMsg()` method
- `error` - Nil on success

**Example:**
```go
sub, err := eventBus.SubscribeSync("events.order.created")
if err != nil {
    return err
}

// Pull messages manually
msg, err := sub.NextMsg(5 * time.Second)
if err != nil {
    return err
}
fmt.Printf("Received: %s\n", string(msg.Data))
```

### QueueSubscribe

```go
func (eb EventBus) QueueSubscribe(subject, queue string, handler MsgHandler) (Subscription, error)
```

Creates a queue group subscription for load-balanced message processing.

**Parameters:**
- `subject` - Subject pattern
- `queue` - Queue group name
- `handler` - Callback function

**Returns:**
- `Subscription` - Active subscription handle
- `error` - Nil on success

**Use Cases:**
- Load balancing across multiple instances
- Horizontal scaling of message processors

**Example:**
```go
sub, err := eventBus.QueueSubscribe("events.order.>", "order-processors",
    func(ctx context.Context, msg *mono.Msg) {
        // Only one instance in the queue group receives each message
        processOrder(msg)
    })
```

### QueueSubscribeSync

```go
func (eb EventBus) QueueSubscribeSync(subject, queue string) (Subscription, error)
```

Creates a synchronous queue group subscription.

### ChanSubscribe

```go
func (eb EventBus) ChanSubscribe(subject string, ch chan *Msg) (Subscription, error)
```

Creates a channel-based subscription where messages are delivered to a Go channel.

**Parameters:**
- `subject` - Subject pattern
- `ch` - Channel to receive messages

**Example:**
```go
msgChan := make(chan *mono.Msg, 100)
sub, err := eventBus.ChanSubscribe("events.order.>", msgChan)
if err != nil {
    return err
}

// Consume from channel
for msg := range msgChan {
    processMessage(msg)
}
```

## Type-Safe Handlers

These typed handler signatures are available in the `pkg/types` package for modules that need automatic JSON unmarshaling.

### TypedEventConsumerHandler

```go
type TypedEventConsumerHandler[T any] func(ctx context.Context, event T, msg *Msg) error
```

Generic event handler with automatic unmarshaling and type safety. The handler receives both the unmarshaled payload and the original message (for headers/metadata).

**Note:** When using the raw EventBus directly, you'll work with `MsgHandler` and `*Msg`. The typed handlers are used with the `EventRegistry` system.

### TypedEventStreamConsumerHandler

```go
type TypedEventStreamConsumerHandler[T any] func(ctx context.Context, payloads []T, msgs []*Msg) error
```

Generic JetStream batch handler with automatic unmarshaling. Receives both the unmarshaled payloads and original messages for acknowledgment.

### EventConsumerHandler (Raw)

```go
type EventConsumerHandler func(ctx context.Context, msg *Msg)
```

Raw event consumer handler for use with `EventRegistry.RegisterEventConsumer()`.

### EventStreamConsumerHandler (Raw)

```go
type EventStreamConsumerHandler func(ctx context.Context, msgs []*Msg) error
```

Raw stream consumer handler for use with `EventRegistry.RegisterEventStreamConsumer()`.

## Request-Reply Methods

### Request

```go
func (eb EventBus) Request(subject string, data []byte, timeout time.Duration) (*Msg, error)
```

Sends a request and waits for a single reply.

**Parameters:**
- `subject` - Target subject
- `data` - Request payload
- `timeout` - Maximum wait time

**Returns:**
- `*Msg` - Response message
- `error` - Timeout or NATS error

**Example:**
```go
requestData, _ := json.Marshal(&PingRequest{})
response, err := eventBus.Request("services.health.ping", requestData, 5*time.Second)
if err != nil {
    return fmt.Errorf("ping failed: %w", err)
}
fmt.Printf("Response: %s\n", string(response.Data))
```

### RequestWithContext

```go
func (eb EventBus) RequestWithContext(ctx context.Context, subject string, data []byte) (*Msg, error)
```

Sends a request with context-controlled timeout and cancellation.

**Parameters:**
- `ctx` - Context for cancellation/deadline
- `subject` - Target subject
- `data` - Request payload

**Example:**
```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

response, err := eventBus.RequestWithContext(ctx, "services.payment.process", requestData)
```

### RequestMsgWithContext

```go
func (eb EventBus) RequestMsgWithContext(ctx context.Context, msg *Msg) (*Msg, error)
```

Sends a complete message with headers and waits for a reply.

**Example:**
```go
response, err := eventBus.RequestMsgWithContext(ctx, &mono.Msg{
    Subject: "services.payment.process",
    Data:    requestData,
    Header: mono.Header{
        "x-request-id": []string{requestID},
    },
})
```

## Message Types

### Msg

`Msg` is a **struct** (not an interface) representing a message in the NATS system.

```go
type Msg struct {
    Subject string
    Reply   string
    Data    []byte
    Header  Header
    Sub     *Subscription
    NatsMsg any  // Internal: underlying NATS message
}
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `Subject` | `string` | Subject the message was published on |
| `Reply` | `string` | Reply-to subject (for request-reply patterns) |
| `Data` | `[]byte` | Message payload (typically JSON) |
| `Header` | `Header` | Custom headers (map[string][]string) |
| `Sub` | `*Subscription` | Subscription that received this message |

**Acknowledgment Methods (JetStream only):**

```go
// Acknowledge successful processing
func (m *Msg) Ack() error

// Negative acknowledgment - message will be redelivered
func (m *Msg) Nak() error

// Negative acknowledgment with delay before redelivery
func (m *Msg) NakWithDelay(delay time.Duration) error

// Terminate processing - message will not be redelivered
func (m *Msg) Term() error

// Signal that processing is still in progress
func (m *Msg) InProgress() error
```

**Example:**
```go
handler := func(ctx context.Context, msg *mono.Msg) {
    fmt.Printf("Received message on %s\n", msg.Subject)

    var event MyEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        msg.Nak() // Negative acknowledgment (retry)
        return
    }

    // Process event...
    msg.Ack() // Positive acknowledgment
}
```

### Header

```go
type Header map[string][]string
```

Custom key-value pairs attached to messages. Values are slices to support multiple values per key (following HTTP header conventions).

**Usage:**
```go
// Get a header value (first value)
requestID := ""
if vals := msg.Header["x-request-id"]; len(vals) > 0 {
    requestID = vals[0]
}

// Set headers when publishing
header := mono.Header{
    "x-request-id": []string{uuid.New().String()},
    "x-source":     []string{"order-module"},
}
```

### Subscription

```go
type Subscription interface {
    Unsubscribe() error
    Drain() error
    IsValid() bool
    Subject() string
    Queue() string
    NextMsg(timeout time.Duration) (*Msg, error)
    NextMsgWithContext(ctx context.Context) (*Msg, error)
}
```

Active subscription handle returned by Subscribe methods.

## JetStream Integration

### EventStream

Access JetStream operations via `EventStream()`:

```go
func (eb EventBus) EventStream() (EventStream, error)
```

**Returns:**
- `EventStream` - JetStream operations interface
- `error` - Error if JetStream is not enabled

**EventStream Interface:**

```go
type EventStream interface {
    // Publish publishes a message to JetStream synchronously
    Publish(ctx context.Context, subject string, data []byte) (MsgPubAck, error)

    // PublishMsg publishes a complete Msg to JetStream synchronously
    PublishMsg(ctx context.Context, msg *Msg) (MsgPubAck, error)

    // CreateOrUpdateStream creates or updates a stream (idempotent)
    CreateOrUpdateStream(ctx context.Context, cfg StreamConfig) (jetstream.Stream, error)

    // CreateOrUpdateConsumer creates or updates a consumer on a stream
    CreateOrUpdateConsumer(ctx context.Context, streamName string, cfg ConsumerConfig) (jetstream.Consumer, error)

    // Stream returns a stream handle for advanced operations
    Stream(ctx context.Context, name string) (jetstream.Stream, error)

    // DeleteStream deletes a stream
    DeleteStream(ctx context.Context, name string) error
}
```

**MsgPubAck Interface:**

```go
type MsgPubAck interface {
    Stream() string     // Name of the stream
    Sequence() uint64   // Sequence number assigned
    Duplicate() bool    // Was this a duplicate?
    Domain() string     // JetStream domain
}
```

**Example:**
```go
stream, err := eventBus.EventStream()
if err != nil {
    return fmt.Errorf("JetStream not available: %w", err)
}

// Publish with persistence
eventData, _ := json.Marshal(&OrderCreatedEvent{OrderID: "ORD-001"})
ack, err := stream.Publish(ctx, "events.order.created", eventData)
if err != nil {
    return fmt.Errorf("publish failed: %w", err)
}
fmt.Printf("Published to stream %s, seq %d\n", ack.Stream(), ack.Sequence())
```

## Event Registry

### EventRegistry

```go
type EventRegistry interface {
    // Register event definition
    DefineEvent(subject string) BaseEventDefinition

    // Register event consumer with type safety
    RegisterConsumer(subject string, handler TypedEventConsumerHandler[T]) error

    // Register JetStream consumer with type safety
    RegisterStreamConsumer(subject string, config StreamConsumerConfig,
        handler TypedEventStreamConsumerHandler[T]) error
}
```

Used in `EventConsumerModule.RegisterEventConsumers()`.

**Example:**
```go
func (m *AuditModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Fire-and-forget consumer
    if err := registry.RegisterConsumer("events.order.>",
        m.handleOrderEvent); err != nil {
        return err
    }

    // Persistent consumer
    if err := registry.RegisterStreamConsumer("events.payment.>",
        streamConfig, m.handlePaymentBatch); err != nil {
        return err
    }

    return nil
}
```

## Subject Naming Conventions

NATS uses hierarchical subjects with dot (`.`) separators.

**Standard Pattern:**
```
events.<domain>.<event-type>
```

**Examples:**
```
events.order.created
events.order.cancelled
events.payment.processed
events.user.registered
```

**Wildcard Patterns:**
- `*` - Single-level wildcard (e.g., `events.order.*`)
- `>` - Multi-level wildcard (e.g., `events.>`)

## Configuration Table

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| Event Subject | `string` | - | Subject pattern for publishing/subscribing |
| Handler Type | Func | - | Consumer handler function |
| Context | `context.Context` | background | Context for operations |
| Persistent | `bool` | `false` | Use JetStream for persistence |
| Batch Size | `int` | 100 | Messages per batch (stream consumers) |
| Max Ack Pending | `int` | 1000 | Unacked message limit |

## Examples

### Publishing Events

```go
type OrderModule struct {
    eventBus mono.EventBus
}

func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        helper.EventDefinition[OrderCreatedEvent]("order", "OrderCreated", "v1").ToBase(),
        helper.EventDefinition[OrderCancelledEvent]("order", "OrderCancelled", "v1").ToBase(),
    }
}

func (m *OrderModule) createOrder(ctx context.Context, order *Order) error {
    // Create order in database...

    // Publish event (serialize first)
    eventData, err := json.Marshal(&OrderCreatedEvent{
        OrderID:   order.ID,
        Total:     order.Total,
        Timestamp: time.Now(),
    })
    if err != nil {
        return fmt.Errorf("failed to serialize event: %w", err)
    }

    return m.eventBus.Publish("events.order.created.v1", eventData)
}
```

### Subscribing to Events

```go
type AuditModule struct {
    eventBus mono.EventBus
}

func (m *AuditModule) Start(ctx context.Context) error {
    // Subscribe to all order events
    _, err := m.eventBus.Subscribe("events.order.>", func(ctx context.Context, msg *mono.Msg) {
        var event OrderEvent
        if err := json.Unmarshal(msg.Data, &event); err != nil {
            return
        }
        fmt.Printf("Order event: type=%s, id=%s\n", event.EventType, event.OrderID)
    })
    return err
}
```

### Using EventRegistry (EventConsumerModule)

```go
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Find event definition
    eventDef, ok := registry.GetEventByName("OrderCreated", "v1", "order")
    if !ok {
        return fmt.Errorf("event not found: OrderCreated.v1 from order")
    }

    // Register consumer handler
    return registry.RegisterEventConsumer(eventDef, m.handleOrderCreated, m)
}

func (m *NotificationModule) handleOrderCreated(ctx context.Context, msg *mono.Msg) {
    var event OrderCreatedEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        return
    }
    // Send notification...
}
```

### JetStream (Persistent) Events

```go
func (m *AnalyticsModule) Start(ctx context.Context) error {
    stream, err := m.eventBus.EventStream()
    if err != nil {
        return fmt.Errorf("JetStream not available: %w", err)
    }

    // Publish with persistence
    eventData, _ := json.Marshal(&AnalyticsEvent{Type: "page_view"})
    ack, err := stream.Publish(ctx, "events.analytics.page_view", eventData)
    if err != nil {
        return err
    }
    fmt.Printf("Published seq %d\n", ack.Sequence())

    return nil
}
```

### Wildcard Subscriptions

```go
// All order events (single level)
eventBus.Subscribe("events.order.*", handler)

// All events (multi-level)
eventBus.Subscribe("events.>", handler)

// Specific domain
eventBus.Subscribe("events.payment.>", handler)
```

## Best Practices

✓ **Do**
- Use hierarchical subjects (e.g., `events.domain.type`)
- Return errors from handlers for failed processing
- Use `PublishWithContext` to respect deadlines
- Ack messages in queue group handlers
- Use JetStream for critical events needing persistence

✗ **Don't**
- Publish to `_*` subjects (reserved for framework)
- Ignore errors returned by handlers
- Block indefinitely in event handlers
- Forget to acknowledge queue group messages
- Mix fire-and-forget and persistent consumers for same events

## Related Documentation

- [Module API](module.md) - EventEmitterModule, EventConsumerModule
- [Service Container API](container.md) - Service patterns
- [Framework API](framework.md) - Framework lifecycle
- [API Reference](README.md) - All APIs overview
- [Core Concepts - Inter-Module Communication](../core-concepts/inter-module-communication.md) - Service and Event patterns explained

---

For more information, see [Foundation Specification](../../spec/foundation.md) and [Core Concepts](../core-concepts/README.md).
