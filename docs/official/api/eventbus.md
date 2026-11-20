# EventBus API

The `EventBus` is the framework's thin wrapper around the NATS connection: publish, subscribe, request-reply and JetStream access on any subject.

**When to read this page:** most modules never call the bus directly. They call other modules through service clients (see the [Service Container API](container.md)) and publish events with `EventDefinition[T].Publish` (see [Publishing events](#publishing-events)). Use the raw bus for custom subjects, or when you need direct control over headers and subscriptions.

{% hint style="info" %}
**Fire-and-forget vs Persistent:** Use `Publish()` for best-effort delivery. For guaranteed at-least-once delivery, enable JetStream and use `EventStream().Publish()`.
{% endhint %}

## Signatures

```go
// Publishing
func Publish(subject string, data []byte) error
func PublishMsg(msg *Msg) error

// Request-Reply
func Request(subject string, data []byte, timeout time.Duration) (*Msg, error)
func RequestWithContext(ctx context.Context, subject string, data []byte) (*Msg, error)
func RequestMsgWithContext(ctx context.Context, msg *Msg) (*Msg, error)

// Subscriptions
func Subscribe(subject string, handler MsgHandler) (Subscription, error)
func SubscribeSync(subject string) (Subscription, error)
func QueueSubscribe(subject, queue string, handler MsgHandler) (Subscription, error)
func QueueSubscribeSync(subject, queue string) (Subscription, error)
func ChanSubscribe(subject string, ch chan *Msg) (Subscription, error)

// JetStream
func EventStream() (EventStream, error)

// Called by the framework; modules do not call it
func SetRuntimeContext(ctx context.Context)
```

## Overview

The `EventBus` lets modules communicate through subjects instead of calling each other. Events are published to subjects and consumed by registered handlers, with no direct module-to-module dependency.

### The module's bus is scoped

The bus a module receives through `SetEventBus` is scoped to that module. On it, these are boundary violations:

- a publish or request to `services.<module>.<service>` when that module is not the module itself, a declared dependency or a plugin (`undeclared-service-subject`);
- a subscription to another module's service subject (`foreign-service-subscription`);
- for a module that implements `ServiceConsumerModule`, a request to a dependency's service that is not in `ConsumeServices()`.

By default a violation is logged and the call goes through. With `WithStrictModuleBoundaries()` the call returns `ErrModuleBoundaryViolation`. `events.*` and other subjects are not checked, and neither is `EventStream()`.

## Handler Types

### MsgHandler

```go
type MsgHandler func(ctx context.Context, msg *Msg)
```

The callback for `Subscribe` and `QueueSubscribe`.

- nats.go calls it for one subscription at a time, on a single goroutine. A handler that blocks delays every later message on that subscription.
- A panic is recovered and logged at Error (`Panic in subscription handler`, with subject, queue group and stack). The subscription keeps running; the message is lost.
- `ctx` is the framework's runtime context. It is cancelled only after the shutdown drain.

Event consumers registered through the `EventRegistry` use their own handler types (`EventConsumerHandler`, `EventStreamConsumerHandler` and the typed variants). See the [EventRegistry API](eventregistry.md).

## Publishing Methods

### Publishing events

For events a module declares in `EmitEvents()`, publish through the event definition rather than the raw bus. The definition knows the subject and serializes the payload:

```go
// Subject: events.order.v1.order-created
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent]("order", "OrderCreated", "v1")

// Subject: events.order.v1.order-cancelled
var OrderCancelledV1 = helper.EventDefinition[OrderCancelledEvent]("order", "OrderCancelled", "v1")

type OrderModule struct {
    eventBus mono.EventBus
}

func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
        OrderCancelledV1.ToBase(),
    }
}

func (m *OrderModule) createOrder(ctx context.Context, order *Order) error {
    // Create order in database...

    // Publish event (serialized by the definition's codec, JSON by default)
    return OrderCreatedV1.Publish(m.eventBus, OrderCreatedEvent{
        OrderID:   order.ID,
        Total:     order.Total,
        Timestamp: time.Now(),
    }, nil)
}
```

### Publish

```go
func (eb EventBus) Publish(subject string, data []byte) error
```

Publishes raw bytes to a subject. Fire-and-forget: best-effort delivery, no persistence.

**Parameters:**
- `subject` - Event subject (e.g., `events.order.created`)
- `data` - Raw byte payload (typically JSON-serialized)

**Returns:**
- `error` - Nil on success, error if NATS unreachable

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

Publishes a complete message, including headers (for example tracing or request IDs).

**Parameters:**
- `msg` - Message containing Subject, Data, and optional Header

**Returns:**
- `error` - Nil on success, error if NATS unreachable

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

Creates an asynchronous subscription that calls `handler` for each message.

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

**Example:**
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

To stop receiving, call `Unsubscribe()` or `Drain()` on the returned `Subscription`.

### SubscribeSync

```go
func (eb EventBus) SubscribeSync(subject string) (Subscription, error)
```

Creates a synchronous subscription: you pull messages with `NextMsg()`.

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

Creates a queue group subscription: each message goes to one member of the group. Use it to share processing across instances.

**Parameters:**
- `subject` - Subject pattern
- `queue` - Queue group name
- `handler` - Callback function

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

Delivers messages to a Go channel, until the subscription ends (`Unsubscribe`, a completed `Drain`, or the connection closing). Messages delivered before that are still handed over, for as long as the channel is read. The channel is yours and is never closed by the framework, so do not rely on a `range` over it ending: stop reading when you unsubscribe.

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

## Request-Reply Methods

To call another module's service, prefer the service client from the dependency container (`GetRequestReplyService`) or a typed contract (`helper.RequestReplyService[...]`):

- raw requests to service subjects are checked against the module's boundaries (see [The module's bus is scoped](#the-modules-bus-is-scoped));
- service clients set the `Mono-Deadline`/`Mono-Timeout` headers; raw `Request*` calls do not.

### Request

```go
func (eb EventBus) Request(subject string, data []byte, timeout time.Duration) (*Msg, error)
```

Sends a request and waits up to `timeout` for a single reply. Returns the reply, or a timeout or NATS error.

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

Like `Request`, with the context controlling timeout and cancellation.

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

Sends a complete message, including headers, and waits for a reply.

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

## Wire Protocol Headers

The framework uses a few `Mono-*` headers on the NATS wire. Clients in other languages that talk to a mono application should send and read them the same way. All values are strings.

| Header | Direction | Value |
|--------|-----------|-------|
| `Mono-Deadline` | request → handler | The caller's absolute deadline, Unix time in **milliseconds**. The handler runs with this deadline, and the request is dropped unanswered if it passed by more than the clock-skew allowance (250ms by default) when a worker picks it up. Values more than 24h from arrival (wrong unit) are ignored. |
| `Mono-Timeout` | request → handler | The time the caller had left when it sent the request, in **milliseconds** (`0` = no time left). Applied relative to the message's arrival, so it does not depend on either clock. Used when `Mono-Deadline` is absent, invalid, or disagrees with it by more than the allowance (clock skew; logged). Clamped to 24h. |
| `Mono-Error` | reply → caller | `"true"` when the reply is an error instead of a response payload. |
| `Mono-Error-Message` | reply → caller | Human-readable error message. For a handler that panicked it is the fixed text `handler panicked`; the value it panicked with is in the server's log only. |
| `Mono-Error-Type` | reply → caller | Error classification. Framework values: `overloaded` (pool and queue full, retry elsewhere), `handler-panic` (handler crashed on this request), `shutting-down` (instance stopping), `reply-failed` (the handler ran, but its response could not be sent, for example because it exceeds the maximum payload). Other values are the handler error's type name, lower-cased with the `Error` suffix removed; a name that would equal a framework value gets the suffix `-error` (`OverloadedError` is sent as `overloaded-error`). |

- Header names are matched case-sensitively as NATS delivers them: send them exactly as spelled here.
- A request without `Mono-Deadline`/`Mono-Timeout` is handled with no deadline, as older clients did.
- Queue group sends carry the deadline headers too, but a queue group handler is never skipped because of them: the ACK promised processing.

## Message Types

### Msg

`Msg` is a **struct** (not an interface) representing a NATS message.

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

| Field | Type | Description |
|-------|------|-------------|
| `Subject` | `string` | Subject the message was published on |
| `Reply` | `string` | Reply-to subject (for request-reply patterns) |
| `Data` | `[]byte` | Message payload (typically JSON) |
| `Header` | `Header` | Custom headers (map[string][]string) |
| `Sub` | `*Subscription` | Always nil on messages delivered by the framework's EventBus (reserved) |

**Acknowledgment methods (JetStream only):**

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

On core NATS messages (`Subscribe`, `QueueSubscribe`, event consumers) these methods do nothing.

**Example (event stream consumer handler):**
```go
handler := func(ctx context.Context, msgs []*mono.Msg) error {
    for _, msg := range msgs {
        var event MyEvent
        if err := json.Unmarshal(msg.Data, &event); err != nil {
            msg.Term() // Malformed: do not redeliver
            continue
        }

        // Process event...
        msg.Ack() // Positive acknowledgment
    }
    return nil
}
```

### Header

```go
type Header map[string][]string
```

Key-value pairs attached to a message. Each key can hold several values, as in HTTP headers.

```go
// Get a header value (first value): "" when absent, safe on a nil Header
requestID := msg.Header.Get("x-request-id")

// Set headers when publishing (Set panics on a nil Header)
header := mono.Header{}
header.Set("x-request-id", requestID)
header.Set("x-source", "order-module")
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

The handle returned by the Subscribe methods.

## JetStream Integration

### EventStream

```go
func (eb EventBus) EventStream() (EventStream, error)
```

Returns the JetStream operations interface.

`error` is returned only if the JetStream context cannot be created. When JetStream is disabled, `EventStream()` still succeeds and the first call on the stream (for example `Publish`) fails.

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

type MsgPubAck interface {
    Stream() string     // Name of the stream
    Sequence() uint64   // Sequence number assigned
    Duplicate() bool    // Was this a duplicate?
    Domain() string     // JetStream domain
}
```

**Example:**
```go
func (m *AnalyticsModule) Start(ctx context.Context) error {
    stream, err := m.eventBus.EventStream()
    if err != nil {
        return fmt.Errorf("failed to get EventStream: %w", err)
    }

    // Publish with persistence
    eventData, _ := json.Marshal(&AnalyticsEvent{Type: "page-view"})
    ack, err := stream.Publish(ctx, "events.analytics.page-view", eventData)
    if err != nil {
        return fmt.Errorf("publish failed: %w", err)
    }
    fmt.Printf("Published to stream %s, seq %d\n", ack.Stream(), ack.Sequence())

    return nil
}
```

To consume events from JetStream, register an event stream consumer through the [EventRegistry](eventregistry.md#registereventstreamconsumer).

## Subject Naming Conventions

NATS subjects are hierarchical, with dot (`.`) separators. The standard event pattern is:

```
events.<domain>.<event-type>
```

for example `events.order.created`, `events.payment.processed`. Use `*` for one level (`events.order.*`) and `>` for any number of levels (`events.>`).

## Best Practices

✓ **Do**
- Use hierarchical subjects (e.g., `events.domain.type`)
- Return errors from `EventRegistry` consumer handlers; handle errors inside raw `MsgHandler` callbacks
- Use `RequestWithContext` / `RequestMsgWithContext` so a request respects the caller's deadline
- Ack, Nak or Term every message in stream consumer and event stream consumer handlers
- Use JetStream for critical events needing persistence

✗ **Don't**
- Publish to `_mono.*` subjects (reserved for the framework)
- Ignore errors returned by `Publish`, `Request*` and `EventStream().Publish`
- Block indefinitely in event handlers
- Forget to acknowledge JetStream messages in stream handlers
- Mix fire-and-forget and persistent consumers for same events

## Related Documentation

- [EventRegistry API](eventregistry.md) - Event consumers and their handler types
- [Module API](module.md) - EventEmitterModule, EventConsumerModule
- [Service Container API](container.md) - Service patterns
- [Framework API](framework.md) - Framework lifecycle
- [API Reference](README.md) - All APIs overview
- [Core Concepts - Inter-Module Communication](../core-concepts/inter-module-communication.md) - Service and Event patterns explained

---

For more information, see [Foundation Specification](https://github.com/go-monolith/mono/blob/main/docs/spec/foundation.md) and [Core Concepts](../core-concepts/README.md).
