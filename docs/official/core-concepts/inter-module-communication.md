# Inter-Module Communication

The Monolith Framework provides **two categories of communication patterns** for modules to interact with each other. Understanding the difference between these patterns is critical for designing your module architecture.

## Communication Categories

| Category | Creates Dependency | Example |
|----------|-------------------|---------|
| **Service Communication** | ✓ Yes | Module B calls Module A's service → B depends on A |
| **Event Communication** | ✗ No | Module A emits event → Any module can consume |

### Key Difference

**Service Communication** creates explicit dependencies between modules:
- Module B must declare Module A as a dependency via `Dependencies() []string`
- Framework injects Module A's `ServiceContainer` via `SetDependencyServiceContainer()`
- Framework enforces startup order (A starts before B)

**Event Communication** has no dependencies between modules:
- Module A only declares which events it will emit via `EmitEvents()`
- Any module can subscribe to events via `RegisterEventConsumers(registry)`
- Emitter doesn't know (or care) who consumes its events
- Consumers can freely subscribe without coupling to the emitter

---

## Part 1: Service Communication Patterns

Service communication creates a **direct dependency** between modules. Use these when one module needs to call another module's functionality.

### Service Pattern Comparison

| Pattern | Latency | Durability | Use Case | Synchronous |
|---------|---------|-----------|----------|------------|
| **Channel** | ~µs | None | In-process, high throughput | Yes |
| **Request-Reply** | ~1ms | None | Synchronous service calls | Yes |
| **Queue Group** | ~1ms | None | Load-balanced async work | No |
| **Stream Consumer** | ~1ms | JetStream | Durable message processing | No |
| **Cron** | scheduled | JetStream | Periodic / scheduled work | No |

### Declaring Service Dependencies

When Module B uses a service from Module A, Module B must:

1. **Declare the dependency** in `Dependencies()`:

```go
func (m *OrderModule) Dependencies() []string {
    return []string{"payment", "inventory"}  // Depends on payment and inventory modules
}
```

2. **Receive the dependency's ServiceContainer** via `SetDependencyServiceContainer()`:

```go
func (m *OrderModule) SetDependencyServiceContainer(dep string, container mono.ServiceContainer) {
    switch dep {
    case "payment":
        m.paymentContainer = container
    case "inventory":
        m.inventoryContainer = container
    }
}
```

3. **Use the service** from the container:

```go
func (m *OrderModule) processOrder(ctx context.Context, order *Order) error {
        // Call payment service using the dependency's ServiceContainer
    var response PaymentResponse
    err := helper.CallRequestReplyService(
        ctx,
        m.paymentContainer,  // ServiceContainer from SetDependencyServiceContainer
        "process-payment",   // service name
        json.Marshal,
        json.Unmarshal,
        &PaymentRequest{Amount: order.Total},
        &response,
    )
    if err != nil {
        return err
    }
    fmt.Printf("Transaction: %s\n", response.TransactionID)
    // Continue processing...
    return nil
}
```

---

### Pattern 1: Channel Services

Direct in-process communication using Go channels.

#### When to Use

- Need highest performance (microsecond latency)
- Communication is within the same process
- No distribution needed
- High message throughput

#### Example

**Service Provider (Registration)**:

```go
func (m *AnalyticsModule) RegisterServices(container mono.ServiceContainer) error {
    // Create channel for event processing
    eventCh := make(chan *Event, 100)

    // Start handler goroutine
    go m.processEvents(eventCh)

    // Register the channel service
    return container.RegisterChannelService("events", eventCh)
}

func (m *AnalyticsModule) processEvents(ch chan *Event) {
    for event := range ch {
        // Process event
    }
}
```

**Service Consumer (Usage)**:

```go
// Get the channel
in, out, err := container.GetChannelService("events", m.Name())
if err != nil {
    return err
}

// Send event
msg := &mono.Msg{Data: []byte(`{"type":"click","data":"button-1"}`)}
in <- msg

// Receive response
response := <-out
```

#### Characteristics

- ✓ Lowest latency
- ✓ Highest throughput
- ✓ Shared memory (be careful with mutation)
- ✗ No distribution (same process only)
- ✗ No persistence
- ✗ No acknowledgment

---

### Pattern 2: Request-Reply Services

Synchronous service calls with immediate response.

#### When to Use

- Need synchronous request-response pattern
- Caller needs immediate feedback
- Response data is required for next step
- Service calls within same system

#### Example

**Service Provider (Registration)**:

```go
func (m *PaymentModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterRequestReplyService(
        "process-payment",
        m.handleProcessPayment,
    )
}

func (m *PaymentModule) handleProcessPayment(ctx context.Context,
    req *PaymentRequest) (*PaymentResponse, error) {
    // Process payment
    if req.Amount <= 0 {
        return nil, fmt.Errorf("invalid amount")
    }
    return &PaymentResponse{TransactionID: "TXN-001"}, nil
}
```

**Service Consumer (Usage)**:

```go
// Call the payment service using the dependency's ServiceContainer
var response PaymentResponse
err := helper.CallRequestReplyService(
    ctx,
    m.paymentContainer,  // ServiceContainer from SetDependencyServiceContainer
    "process-payment",   // service name
    json.Marshal,
    json.Unmarshal,
    &PaymentRequest{Amount: 100.00},
    &response,
)
if err != nil {
    return err
}
fmt.Printf("Transaction: %s\n", response.TransactionID)
```

#### Characteristics

- ✓ Synchronous (caller waits for response)
- ✓ Type-safe with generics
- ✓ Error propagation back to caller
- ✓ Good latency (~1ms over NATS)
- ✗ No persistence
- ✗ Caller blocked waiting for response

---

### Pattern 3: Queue Group Services

Load-balanced asynchronous processing.

#### When to Use

- Fire-and-forget async work
- Want to distribute load across instances
- No response needed from worker
- Horizontal scaling needed

#### Example

**Service Provider (Registration)**:

```go
func (m *EmailModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterQueueGroupService(
        "send-email",
        "email-workers",  // Queue group name
        m.handleSendEmail,
    )
}

func (m *EmailModule) handleSendEmail(ctx context.Context,
    req *EmailRequest) error {
    // Send email (fire-and-forget)
    return m.sendEmail(ctx, req)
}
```

**Service Consumer (Usage)**:

```go
// Send message to queue group (no response expected)
err := helper.SendQueueGroupService(ctx, eventBus, "email", "send-email", &EmailRequest{
    To: "user@example.com",
    Subject: "Welcome",
    Body: "Welcome to our app!",
})
if err != nil {
    // Log error (worker already started processing)
    return err
}
// Continue immediately (don't wait for email)
```

#### Characteristics

- ✓ Asynchronous (caller continues immediately)
- ✓ Load-balanced (multiple workers)
- ✓ Horizontal scalability
- ✓ Good latency (message queued quickly)
- ✗ No response (fire-and-forget)
- ✗ No persistence
- ✗ Error handling limited (worker processes independently)

---

### Pattern 4: Stream Consumer Services

Durable message processing with JetStream.

#### When to Use

- Message loss is unacceptable
- Need message persistence
- Require at-least-once delivery
- Need to replay messages
- Batch processing needed

#### Example

**Service Provider (Registration)**:

```go
func (m *ArchiveModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterStreamConsumerService(
        "archive-order",
        "archive-orders-stream",
        &mono.FetchConfig{MaxMessages: 100, IdleTimeout: 10 * time.Second},
        m.handleArchiveOrder,
    )
}

func (m *ArchiveModule) handleArchiveOrder(ctx context.Context,
    req *ArchiveRequest, msg *mono.Msg) error {
    // Process message
    err := m.archive(ctx, req)

    if err != nil {
        // Negative acknowledgment (retry later)
        return msg.Nak()
    }

    // Positive acknowledgment (message processed)
    return msg.Ack()
}
```

**Service Consumer (Publishing)**:

```go
// Publish to stream (persisted in JetStream)
err := helper.PublishStreamConsumerService(
    ctx,
    eventBus,
    "archive",
    "archive-order",
    &ArchiveRequest{OrderID: "ORD-001"},
)
```

#### Characteristics

- ✓ Durable (messages persisted in JetStream)
- ✓ At-least-once delivery
- ✓ Message replay capability
- ✓ Batch processing
- ✓ Explicit acknowledgment
- ✗ Slightly higher latency (persistence overhead)
- ✗ JetStream persistence needed
- ✗ More complex setup

---

### Pattern 5: Cron Services

Periodic work driven by a **server-side** cron schedule (NATS JetStream message scheduler, nats-server v2.14+). The schedule lives on the server, so in a multi-node cluster **exactly one** message fires per occurrence — no client-side ticker and no leader election. Each occurrence is delivered through a durable pull consumer with explicit acknowledgement (at-least-once).

#### When to Use

- Recurring/periodic work: nightly rollups, cache warmups, heartbeat emits, downsampling
- You need a single fire per occurrence across a cluster (no double-firing)
- You want the schedule to survive restarts and be managed declaratively in code

> Requires JetStream (`WithJetStreamStorageDir(...)`). Registering a cron service without JetStream fails fast at startup.

#### Example

Cron has **no consumer side** — it is driven entirely by the server-side schedule, so there is nothing to retrieve or publish. A module just registers the service:

```go
func (m *ReportModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterCronService(
        "nightly-rollup",
        mono.CronServiceConfig{
            Schedule: "@daily",                     // cron, alias, or "@every 5m"
            Payload:  []byte(`{"job":"rollup"}`),   // static payload delivered each tick
        },
        m.handleRollup,
    )
}

// Acknowledgement is owned by the framework: return nil to Ack the occurrence,
// or a non-nil error to Nak it (redelivered up to MaxDeliver). Do NOT call
// msg.Ack()/Nak() yourself.
func (m *ReportModule) handleRollup(ctx context.Context, msg *mono.Msg) error {
    if err := m.runRollup(ctx, msg.Data); err != nil {
        return err // framework Naks -> redelivery
    }
    return nil // framework Acks
}
```

#### Schedule Formats

| Form | Example | Notes |
|------|---------|-------|
| Cron expression | `"0 0 * * *"` (= daily at midnight) | Standard five-field or six-field **seconds-first** form; five-field expressions are normalized by prepending a `"0"` seconds field. Supports an optional `TimeZone` (IANA) |
| Named alias | `"@daily"`, `"@hourly"`, `"@weekly"` | Convenience aliases |
| Interval | `"@every 5m"` | Minimum 1s; `TimeZone` not applicable |

Additional `CronServiceConfig` options: `TimeZone` (cron expressions only), `TTL` (per-occurrence message TTL, ≥1s), `SourceSubject` (deliver the last message seen on a subject instead of a static `Payload` — mutually exclusive with `Payload`), and `Deprecated` (see below).

#### Updating and Retiring a Schedule

- **Update**: change `Schedule`/`Payload`/`TimeZone`/`TTL` and redeploy — the schedule is re-published idempotently and overwritten in place (rollup by subject).
- **Retire (two-phase, revertible)**: set `Deprecated: true` and deploy — the framework purges the server-side schedule and does not start the consumer, while keeping the registration code. In a later release, delete the `RegisterCronService` call. Removing the call without first deprecating leaves an orphaned durable schedule; the framework logs a warning on startup when it detects one.

```go
container.RegisterCronService("nightly-rollup", mono.CronServiceConfig{
    Schedule:   "@daily",
    Payload:    []byte(`{"job":"rollup"}`),
    Deprecated: true, // stop the schedule on deploy, keep the code (set back to false to re-arm)
}, m.handleRollup)
```

#### Characteristics

- ✓ Server-side schedule: single fire per occurrence across a cluster
- ✓ Durable, at-least-once delivery with framework-owned ack (Ack on nil, Nak on error/panic)
- ✓ Idempotent updates; survives restarts
- ✓ Safe two-phase retirement via `Deprecated`
- ✗ Requires JetStream and nats-server v2.14+
- ✗ Not wrapped by the middleware chain (access-log, audit, request-id)

---

## Part 2: Event Communication Patterns

Event communication enables **loose coupling** between modules. Emitters declare events they publish; any interested module can consume them **without creating a dependency**.

### Event Pattern Comparison

| Pattern | Durability | Use Case | Acknowledgment | Message Loss Risk |
|---------|-----------|----------|----------------|-------------------|
| **EventConsumer** | None (NATS Core) | Pub/Sub, real-time notifications | No | ⚠️ Yes (fire-and-forget) |
| **EventStreamConsumer** | JetStream | Event-Driven Architecture (EDA) | Yes (Ack/Nak) | No (persisted) |

### How Event Communication Works

```
┌───────────────────────────────────────────────────────────────────────────┐
│                        Event Communication Flow                           │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌────────────────┐                                                       │
│  │  OrderModule   │                                                       │
│  │  (Emitter)     │                                                       │
│  └───────┬────────┘                                                       │
│          │                                                                │
│          │ 1. EmitEvents() → declares OrderCreatedV1                      │
│          │                                                                │
│          ▼                                                                │
│  ┌────────────────┐                                                       │
│  │  EventRegistry │  ← Framework manages event catalog                    │
│  └───────┬────────┘                                                       │
│          │                                                                │
│          │ 2. Consumer modules discover via GetEventByName()              │
│          │                                                                │
│          ├─────────────────────────┬─────────────────────────┐            │
│          ▼                         ▼                         ▼            │
│  ┌──────────────┐          ┌──────────────┐          ┌──────────────┐     │
│  │Notification  │          │  Analytics   │          │    Audit     │     │
│  │Module (Cons.)│          │Module (Cons.)│          │Module (Cons.)│     │
│  └──────────────┘          └──────────────┘          └──────────────┘     │
│                                                                           │
│  3. Each consumer registers via RegisterEventConsumers(registry)          │
│                                                                           │
│  Note: OrderModule doesn't know about consumers.                          │
│        Consumers don't declare OrderModule as a dependency.               │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

### Declaring Events (Emitter Side)

Module A declares which events it will emit:

```go
// Define the event (typically in a shared package or the module itself)
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
    "order",        // Module name
    "OrderCreated", // Event name
    "v1",           // Version
)

// Module A implements EventEmitterModule
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
        OrderShippedV1.ToBase(),
    }
}

// Later, publish the event
func (m *OrderModule) createOrder(ctx context.Context, order *Order) error {
    // ... create order logic ...

    // Publish event (fire-and-forget for EventConsumer)
    return OrderCreatedV1.Publish(m.eventBus, OrderCreatedEvent{
        OrderID: order.ID,
        Amount:  order.Total,
    }, nil)
}
```

### Consuming Events (Consumer Side)

Any module can consume events **without declaring a dependency**:

```go
// Module B implements EventConsumerModule
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Discover the event from the registry
    eventDef, ok := registry.GetEventByName("OrderCreated", "v1", "order")
    if !ok {
        return fmt.Errorf("event not found: OrderCreated.v1 from order")
    }

    // Register consumer handler
    return registry.RegisterEventConsumer(eventDef, m.handleOrderCreated, m)
}

func (m *NotificationModule) handleOrderCreated(ctx context.Context, msg *mono.Msg) error {
    var event OrderCreatedEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        return err
    }
    return m.sendNotification(ctx, event)
}
```

---

### Pattern 6: EventConsumer (NATS Core)

Fire-and-forget event consumption via standard NATS subscriptions.

#### When to Use

- Real-time notifications
- Events where occasional message loss is acceptable
- Low-latency pub/sub scenarios
- Simple broadcast patterns

#### Example

**Emitter Module**:

```go
// Declare events
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
    }
}

// Publish event
err := OrderCreatedV1.Publish(m.eventBus, OrderCreatedEvent{
    OrderID: "ORD-001",
    Amount:  99.99,
}, nil)
```

**Consumer Module**:

```go
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    eventDef, ok := registry.GetEventByName("OrderCreated", "v1", "order")
    if !ok {
        return fmt.Errorf("event not found")
    }

    // Register with optional queue group for load balancing
    return registry.RegisterEventConsumer(
        eventDef,
        m.handleOrderCreated,
        m,
        "notification-workers",  // Optional: queue group for load balancing
    )
}

func (m *NotificationModule) handleOrderCreated(ctx context.Context, msg *mono.Msg) error {
    var event OrderCreatedEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        return err
    }
    return m.sendEmail(ctx, event.OrderID)
}
```

#### Characteristics

- ✓ Lowest latency (~1ms)
- ✓ Simple setup
- ✓ Broadcast to multiple consumers
- ✓ Queue groups for load balancing
- ✓ No dependency on emitter module
- ✗ **⚠️ Message loss risk** if no consumer is available
- ✗ No persistence (fire-and-forget)
- ✗ No acknowledgment mechanism
- ✗ No replay capability

---

### Pattern 7: EventStreamConsumer (JetStream)

Durable event consumption via JetStream pull consumers.

#### When to Use

- Event-Driven Architecture (EDA) patterns
- Message loss is unacceptable
- Need at-least-once delivery guarantee
- Require event replay capability
- Batch processing of events
- Critical business events (payments, orders, audits)

#### Example

**Emitter Module**:

```go
// Declare events
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
    }
}

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

**Consumer Module**:

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

#### Characteristics

- ✓ Durable (messages persisted in JetStream)
- ✓ At-least-once delivery guarantee
- ✓ Explicit acknowledgment (Ack/Nak)
- ✓ Event replay capability
- ✓ Batch processing support
- ✓ No dependency on emitter module
- ✗ Slightly higher latency (~5ms with persistence)
- ✗ More complex setup
- ✗ Requires JetStream configuration

---

## Choosing the Right Pattern

### Decision Tree

```
┌─ Is this a direct service call (caller needs result)?
│  ├─ YES → Use Service Communication (Part 1)
│  │  ├─ Periodic / scheduled work? → Cron
│  │  ├─ Need durability? → Stream Consumer
│  │  ├─ Need response? → Request-Reply
│  │  ├─ Need highest performance? → Channel
│  │  └─ Fire-and-forget work? → Queue Group
│  │
│  └─ NO → Use Event Communication (Part 2)
│     ├─ Message loss acceptable? → EventConsumer
│     └─ Message loss unacceptable? → EventStreamConsumer

Additional considerations:
├─ One consumer, specific work → Service Communication
├─ Many consumers, broadcast → Event Communication
├─ Module coupling needed → Service Communication
└─ Loose coupling preferred → Event Communication
```

### Quick Reference

| Scenario | Pattern | Dependency |
|----------|---------|------------|
| Payment processing (need response) | Request-Reply | Yes |
| Email sending (fire-and-forget) | Queue Group | Yes |
| Order created notification | EventConsumer | No |
| Audit trail for compliance | EventStreamConsumer | No |
| High-throughput analytics | Channel | Yes |
| Inventory updates (durable) | Stream Consumer | Yes |
| Nightly rollup (scheduled) | Cron | Yes |

---

## Performance Characteristics

Typical latencies on modern hardware:

| Pattern | Latency | Throughput | Durability |
|---------|---------|-----------|------------|
| Channel | ~1µs | >1M msgs/sec | None |
| Request-Reply | ~1ms | ~10K msgs/sec | None |
| Queue Group | ~1ms | ~10K msgs/sec | None |
| Stream Consumer | ~5ms | ~5K msgs/sec | JetStream |
| Cron | schedule-driven | per-schedule | JetStream |
| EventConsumer | ~1ms | ~10K msgs/sec | None |
| EventStreamConsumer | ~5ms | ~5K msgs/sec | JetStream |

---

## Subject Naming Conventions

The framework enforces consistent naming:

**Service Subjects**: `services.<module>.<service>`
- Example: `services.payment.process-payment`
- Framework computes automatically (no wildcards)

**Event Subjects**: `events.<domain>.<version>.<event-type>`
- Example: `events.order.v1.OrderCreated`
- Supports wildcards for routing

**Reserved**: `_mono.*` (internal use only)

---

## Complete Example

See the [multi-module example](../../../examples/multi-module/README.md) for a complete implementation using all communication patterns.

## Next Steps

- Explore [Module Examples](../../../examples/)
- Learn about [Framework Architecture](architecture.md)
- Review [Core Concepts Overview](README.md)

---

Now you understand all communication patterns! Next: [Framework Architecture](architecture.md).
