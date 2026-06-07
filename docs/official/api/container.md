# Service Container API

API documentation for the `ServiceContainer` interface, which manages service registration and discovery within modules.

## Signatures

```go
// Registration methods
func RegisterChannelService(name string, in chan *Msg, out chan *Msg) error
func RegisterRequestReplyService(name string, handler RequestReplyHandler) error
func RegisterQueueGroupService(name string, pairs ...QGHP) error
func RegisterStreamConsumerService(name string, config StreamConsumerConfig, handler StreamConsumerHandler) error
func RegisterCronService(name string, config CronServiceConfig, handler CronHandler) error

// Discovery methods
func GetChannelService(name string, consumer string) (chan *Msg, chan *Msg, error)
func GetRequestReplyService(name string) (RequestReplyServiceClient, error)
func GetQueueGroupService(name string) (QueueGroupServiceClient, error)
func GetStreamConsumerService(name string) (StreamConsumerServiceClient, error)
```

{% hint style="info" %}
**Pattern Selection:** Use Channel for lowest latency (~µs), Request-Reply for synchronous calls (~ms), Queue Group for async fire-and-forget, and Stream Consumer for persistent message processing with acknowledgment.
{% endhint %}

## Overview

Each module receives a `ServiceContainer` for registering services that other modules can call. Services support five communication patterns: in-process channels, request-reply, queue groups, stream consumers, and cron (server-scheduled) services.

## Service Registration Methods

### RegisterChannelService

```go
func (container ServiceContainer) RegisterChannelService(name string, in chan *Msg, out chan *Msg) error
```

{% hint style="warning" %}
**Single process only.** Channel services only work within the same process. For distributed communication, use Request-Reply or Queue Group services.
{% endhint %}

Registers a bidirectional Go channel service for in-process communication.

**Parameters:**
- `name` - Service name (e.g., "submit-order")
- `in` - Input channel for receiving messages from consumers
- `out` - Output channel for sending responses to consumers

**Returns:**
- `error` - Nil on success

**Use Cases:**
- Real-time high-performance communication between modules
- Lowest latency inter-module calls (~microseconds)
- Only works within single process
- Bidirectional communication pattern

**Example:**
```go
func (m *OrderModule) RegisterServices(container mono.ServiceContainer) error {
    // Bind container to this module first
    if err := container.BindModule(m); err != nil {
        return err
    }

    // Create bidirectional channels
    in := make(chan *mono.Msg, 100)
    out := make(chan *mono.Msg, 100)

    // Start handler goroutine
    go func() {
        for msg := range in {
            response := m.handleOrder(context.Background(), msg)
            out <- response
        }
    }()

    return container.RegisterChannelService("place-order", in, out)
}
```

### RegisterRequestReplyService

```go
func (container ServiceContainer) RegisterRequestReplyService(
    name string,
    handler RequestReplyHandler,
) error
```

Registers a request-reply service for synchronous inter-module calls via NATS.

**Parameters:**
- `name` - Service name (subject: `services.<module>.<name>`)
- `handler` - Function handling requests

**Handler Signature:**
```go
type RequestReplyHandler func(ctx context.Context, req *Msg) (response []byte, err error)
```

**Returns:**
- `error` - Nil on success

**Use Cases:**
- Synchronous request-response patterns
- Works across process/machine boundaries (with NATS)
- ~1ms overhead for NATS communication

**Example:**
```go
func (m *PaymentModule) RegisterServices(container mono.ServiceContainer) error {
    if err := container.BindModule(m); err != nil {
        return err
    }
    return container.RegisterRequestReplyService("process", m.handleProcess)
}

func (m *PaymentModule) handleProcess(ctx context.Context, req *mono.Msg) ([]byte, error) {
    var payment PaymentRequest
    if err := json.Unmarshal(req.Data, &payment); err != nil {
        return nil, fmt.Errorf("invalid request: %w", err)
    }

    result, err := m.processor.Process(ctx, &payment)
    if err != nil {
        return nil, err
    }

    return json.Marshal(&PaymentResponse{Success: true, TransactionID: result.ID})
}
```

**Typed Handler (helper):**

For type-safe handlers, use the `TypedRequestReplyHandler`:

```go
type TypedRequestReplyHandler[Req any, Resp any] func(ctx context.Context, req Req, msg *Msg) (Resp, error)
```

### RegisterQueueGroupService

```go
func (container ServiceContainer) RegisterQueueGroupService(
    name string,
    pairs ...QGHP,
) error
```

Registers a queue group service with one or more handler pairs for load-balanced async message processing via NATS.

**Parameters:**
- `name` - Service name (subject: `services.<module>.<name>`)
- `pairs` - One or more queue group handler pairs (QGHP)

**QGHP Structure:**
```go
type QGHP struct {
    QueueGroup string            // Queue group name (e.g., "high-priority-workers")
    Handler    QueueGroupHandler // Handler function for this queue group
}
```

**Handler Signature:**
```go
type QueueGroupHandler func(ctx context.Context, msg *Msg) error
```

**Returns:**
- `error` - Nil on success

**Use Cases:**
- Async load-balanced processing
- Fire-and-forget messaging pattern
- Horizontal scaling with multiple instances
- Multiple queue groups can process the same service subject

**Example:**
```go
func (m *AuditModule) RegisterServices(container mono.ServiceContainer) error {
    if err := container.BindModule(m); err != nil {
        return err
    }

    return container.RegisterQueueGroupService("audit-log",
        mono.QGHP{
            QueueGroup: "audit-writers",
            Handler:    m.handleAuditLog,
        },
    )
}

func (m *AuditModule) handleAuditLog(ctx context.Context, msg *mono.Msg) error {
    var event AuditEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        return fmt.Errorf("invalid event: %w", err)
    }

    return m.persistEvent(&event)
}
```

**Multiple Queue Groups Example:**
```go
// Multiple handlers for different processing priorities
container.RegisterQueueGroupService("process-orders",
    mono.QGHP{QueueGroup: "high-priority", Handler: m.handleHighPriority},
    mono.QGHP{QueueGroup: "low-priority", Handler: m.handleLowPriority},
)
```

**Typed Handler (helper):**

For type-safe handlers, use the `TypedQueueGroupHandler`:

```go
type TypedQueueGroupHandler[T any] func(ctx context.Context, payload T, msg *Msg) error
```

### RegisterStreamConsumerService

```go
func (container ServiceContainer) RegisterStreamConsumerService(
    name string,
    config StreamConsumerConfig,
    handler StreamConsumerHandler,
) error
```

Registers a JetStream durable pull consumer service for persistent, ordered message processing.

**Parameters:**
- `name` - Service name (subject pattern)
- `config` - JetStream consumer configuration
- `handler` - Function handling message batches

**Handler Signature:**
```go
type StreamConsumerHandler func(ctx context.Context, msgs []*Msg) error
```

Messages should be acknowledged individually using `Ack()`, `Nak()`, `NakWithDelay()`, `Term()`, or `InProgress()`.

**Returns:**
- `error` - Nil on success

**Use Cases:**
- Persistent message processing (survives restarts)
- Batch processing from JetStream
- At-least-once delivery guarantees
- Message replay support

**Configuration:**

The `StreamConsumerConfig` is a composite type combining stream, consumer, and fetch configuration. See [JetStream types](../core-concepts/inter-module-communication.md) for full details.

**Example:**
```go
func (m *AnalyticsModule) RegisterServices(container mono.ServiceContainer) error {
    if err := container.BindModule(m); err != nil {
        return err
    }

    config := mono.StreamConsumerConfig{
        Stream: mono.StreamConfig{
            Name:     "events",
            Subjects: []string{"events.>"},
        },
        Consumer: mono.ConsumerConfig{
            Name:          "analytics-durable",
            MaxAckPending: 1000,
        },
        Fetch: mono.FetchConfig{
            MaxMessages: 100,
        },
    }

    return container.RegisterStreamConsumerService("analytics-consumer", config, m.handleEventBatch)
}

func (m *AnalyticsModule) handleEventBatch(ctx context.Context, msgs []*mono.Msg) error {
    for _, msg := range msgs {
        var event Event
        if err := json.Unmarshal(msg.Data, &event); err != nil {
            msg.Term() // Terminate processing of malformed message
            continue
        }
        m.processEvent(&event)
        msg.Ack() // Acknowledge successful processing
    }
    return nil
}
```

**Typed Handler (helper):**

For type-safe handlers, use the `TypedStreamConsumerHandler`:

```go
type TypedStreamConsumerHandler[T any] func(ctx context.Context, payloads []T, msgs []*Msg) error
```

### RegisterCronService

```go
func (container ServiceContainer) RegisterCronService(
    name string,
    config CronServiceConfig,
    handler CronHandler,
) error
```

Registers a cron-scheduled service backed by the embedded NATS JetStream message scheduler (nats-server v2.14+). The schedule is registered **server-side**, so in a multi-node cluster exactly one message fires per occurrence (no client-side ticker, no leader election). Each occurrence is delivered through a durable pull consumer with explicit acknowledgement.

Requires JetStream (`WithJetStreamStorageDir(...)`); registering a cron service without JetStream fails fast at startup. Cron services are server-driven and have **no** consumer-side client (there is no `GetCronService`).

**Parameters:**
- `name` - Service name. The handler receives ticks on `services.<module>.<name>`.
- `config` - `CronServiceConfig` (schedule, payload/source, time zone, TTL, deprecation)
- `handler` - Function invoked on each scheduled occurrence

**Handler Signature:**
```go
type CronHandler func(ctx context.Context, msg *Msg) error
```

Acknowledgement is owned by the framework: return `nil` to Ack the occurrence, or a non-nil error to Nak it (redelivered up to `MaxDeliver`). The handler must **not** call `msg.Ack()`/`Nak()` itself. A recovered panic is treated as an error (Nak).

**CronServiceConfig:**

| Field | Type | Description |
|-------|------|-------------|
| `Schedule` | `string` | Required. Cron expression (`"0 0 * * *"`), alias (`"@daily"`), or interval (`"@every 5m"`, min 1s) |
| `Payload` | `[]byte` | Static payload delivered each occurrence (mutually exclusive with `SourceSubject`) |
| `SourceSubject` | `string` | Deliver the last message seen on this subject instead of a static payload |
| `TimeZone` | `string` | Optional IANA time zone (cron expressions only; not `@every`) |
| `TTL` | `time.Duration` | Optional per-occurrence message TTL (≥ 1s) |
| `Deprecated` | `bool` | When true, purge the schedule on deploy and don't start the consumer (keep the code) |

**Returns:**
- `error` - Nil on success; non-nil if validation fails (e.g. empty `Schedule`, both `Payload` and `SourceSubject` set, invalid `TimeZone`)

**Use Cases:**
- Periodic work: nightly rollups, cache warmups, heartbeat emits, downsampling
- Single fire per occurrence across a cluster
- Schedules that survive restarts and are managed declaratively in code

**Example:**
```go
func (m *ReportModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterCronService(
        "nightly-rollup",
        mono.CronServiceConfig{
            Schedule: "@daily",
            Payload:  []byte(`{"job":"rollup"}`),
        },
        m.handleRollup,
    )
}

func (m *ReportModule) handleRollup(ctx context.Context, msg *mono.Msg) error {
    if err := m.runRollup(ctx, msg.Data); err != nil {
        return err // framework Naks -> redelivery
    }
    return nil // framework Acks
}
```

Re-publishing the schedule on every startup is idempotent (rollup by subject), so changing `Schedule`/`Payload`/`TimeZone`/`TTL` and redeploying overwrites the live schedule in place. To retire a service, set `Deprecated: true` and deploy (purges the schedule, keeps the code), then remove the `RegisterCronService` call in a later release.

## Service Discovery Methods

### GetChannelService

```go
func (container ServiceContainer) GetChannelService(serviceName string, consumerModule string) (in chan *Msg, out chan *Msg, err error)
```

Gets bidirectional channels for a registered channel service. Each consumer module receives a dedicated output channel to prevent race conditions.

**Parameters:**
- `serviceName` - Name of the registered service
- `consumerModule` - Name of the calling module (for dedicated output channel)

**Returns:**
- `in` - Channel for sending messages to the service
- `out` - Dedicated channel for receiving responses
- `err` - Error if service not found

**Example:**
```go
func (m *OrderModule) SetDependencyServiceContainer(dep string, container mono.ServiceContainer) {
    if dep == "analytics" {
        in, out, err := container.GetChannelService("metrics", m.Name())
        if err != nil {
            // Handle error
        }
        m.metricsIn = in
        m.metricsOut = out
    }
}
```

### MustGetChannelService

```go
func (container ServiceContainer) MustGetChannelService(serviceName string, consumerModule string) (in chan *Msg, out chan *Msg)
```

Same as `GetChannelService` but panics if the service is not found. Use when the service is required and absence is a programming error.

### GetRequestReplyService

```go
func (container ServiceContainer) GetRequestReplyService(name string) (RequestReplyServiceClient, error)
```

Gets a client for calling a registered request-reply service.

**Returns:**
- `RequestReplyServiceClient` - Client for making calls
- `error` - Error if service not found

**Client Interface:**
```go
type RequestReplyServiceClient interface {
    // Call sends a request payload and waits for a response
    Call(ctx context.Context, data []byte) (*Msg, error)

    // CallMsg sends a raw request message and waits for a response
    CallMsg(ctx context.Context, msg *Msg) (*Msg, error)
}
```

**Example:**
```go
client, err := container.GetRequestReplyService("process-payment")
if err != nil {
    return fmt.Errorf("payment service unavailable: %w", err)
}

requestData, _ := json.Marshal(&PaymentRequest{Amount: 100})
response, err := client.Call(context.Background(), requestData)
if err != nil {
    return err
}

var payment PaymentResponse
json.Unmarshal(response.Data, &payment)
```

### GetQueueGroupService

```go
func (container ServiceContainer) GetQueueGroupService(name string) (QueueGroupServiceClient, error)
```

Gets a client for sending messages to a registered queue group service.

**Returns:**
- `QueueGroupServiceClient` - Client for sending messages
- `error` - Error if service not found

**Client Interface:**
```go
type QueueGroupServiceClient interface {
    // Send sends a message payload to the queue group and waits for ACK.
    // Returns ErrServiceUnavailable if no handlers are online.
    Send(ctx context.Context, data []byte) error

    // SendMsg sends a raw message to the queue group and waits for ACK.
    // Returns ErrServiceUnavailable if no handlers are online.
    SendMsg(ctx context.Context, msg *Msg) error
}
```

**Example:**
```go
client, err := container.GetQueueGroupService("audit-log")
if err != nil {
    return err
}

eventData, _ := json.Marshal(&AuditEvent{
    EventType: "user.login",
    UserID:    "123",
})
err = client.Send(context.Background(), eventData)
```

### GetStreamConsumerService

```go
func (container ServiceContainer) GetStreamConsumerService(name string) (StreamConsumerServiceClient, error)
```

Gets a client for publishing messages to a stream consumer service.

**Returns:**
- `StreamConsumerServiceClient` - Client for publishing
- `error` - Error if service not found

**Client Interface:**
```go
type StreamConsumerServiceClient interface {
    // Publish publishes a message to the stream (with JetStream persistence)
    Publish(ctx context.Context, data []byte) (MsgPubAck, error)

    // PublishMsg publishes a complete message with headers to the stream
    PublishMsg(ctx context.Context, msg *Msg) (MsgPubAck, error)
}
```

**Example:**
```go
client, err := container.GetStreamConsumerService("analytics-consumer")
if err != nil {
    return err
}

eventData, _ := json.Marshal(&AnalyticsEvent{Type: "page_view"})
ack, err := client.Publish(context.Background(), eventData)
if err != nil {
    return err
}
fmt.Printf("Published to stream %s, seq %d\n", ack.Stream(), ack.Sequence())
```

## Service Inspection

### Has

```go
func (container ServiceContainer) Has(name string) bool
```

Checks if a service with the given name is registered.

**Example:**
```go
if container.Has("payment-processor") {
    // Service is available
}
```

### Entries

```go
func (container ServiceContainer) Entries() []*ServiceEntry
```

Returns pointers to all registered service entries.

**Returns:**
- `[]*ServiceEntry` - Slice of service entry pointers

**ServiceEntry Structure:**
```go
type ServiceEntry struct {
    Name                  string
    Type                  ServiceType
    InChannel             chan *Msg
    OutChannel            chan *Msg
    ConsumerChannels      map[string]chan *Msg  // Per-consumer output channels
    ConsumerMu            sync.RWMutex
    RequestHandler        RequestReplyHandler
    QueueGroup            string
    QueueHandlers         []QGHP
    StreamConsumerConfig  *StreamConsumerConfig
    StreamConsumerHandler StreamConsumerHandler
    ModuleName            string
    Subject               string
    Created               time.Time
}
```

**Example:**
```go
for _, svc := range container.Entries() {
    fmt.Printf("Service: %s (%s) from module %s\n",
        svc.Name,
        mono.FormatServiceType(svc.Type),
        svc.ModuleName,
    )
}
```

### Unregister

```go
func (container ServiceContainer) Unregister(name string) error
```

Removes a service from the container.

**Returns:**
- `error` - Error if service not found

### ServiceType Constants

```go
const (
    ServiceTypeChannel ServiceType = iota
    ServiceTypeRequestReply
    ServiceTypeQueueGroup
    ServiceTypeStreamConsumer
    ServiceTypeCron
)
```

Use `FormatServiceType(serviceType)` to convert to string representation.

## Configuration Table

| Property | Type | Description | Example |
|----------|------|-------------|---------|
| Service Name | `string` | Unique service identifier | `"place-order"` |
| Request Timeout | `time.Duration` | Max wait for response | `5 * time.Second` |
| Message Size | `int` | Max message payload | `1048576` (1MB) |
| Batch Size | `int` | Messages per batch | `100` |

## Default Configuration

| Setting | Default |
|---------|---------|
| Request Timeout | 30 seconds |
| Max Message Size | 1 MB |
| Queue Group Batch Size | 10 |
| Stream Consumer Batch | 100 |

## Examples

### Complete Service Provider Module

```go
type PaymentModule struct {
    eventBus  mono.EventBus
    processor *PaymentProcessor
}

func (m *PaymentModule) Name() string { return "payment" }

func (m *PaymentModule) RegisterServices(container mono.ServiceContainer) error {
    // Bind container to this module first
    if err := container.BindModule(m); err != nil {
        return err
    }

    // Request-reply service for synchronous calls
    if err := container.RegisterRequestReplyService(
        "process",
        m.handleProcess,
    ); err != nil {
        return err
    }

    // Queue group for async processing
    if err := container.RegisterQueueGroupService(
        "audit",
        mono.QGHP{QueueGroup: "audit-workers", Handler: m.handleAudit},
    ); err != nil {
        return err
    }

    return nil
}

func (m *PaymentModule) handleProcess(ctx context.Context, req *mono.Msg) ([]byte, error) {
    var payment PaymentRequest
    if err := json.Unmarshal(req.Data, &payment); err != nil {
        return nil, fmt.Errorf("invalid request: %w", err)
    }

    result, err := m.processor.Process(ctx, &payment)
    if err != nil {
        return nil, err
    }

    return json.Marshal(&PaymentResponse{Success: true, TransactionID: result.ID})
}

func (m *PaymentModule) handleAudit(ctx context.Context, msg *mono.Msg) error {
    var event AuditEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        return fmt.Errorf("invalid audit event: %w", err)
    }
    // Log the audit event
    return nil
}
```

### Service Discovery and Calling

```go
type OrderModule struct {
    paymentContainer mono.ServiceContainer
    paymentClient    mono.RequestReplyServiceClient
}

func (m *OrderModule) Name() string { return "order" }

func (m *OrderModule) Dependencies() []string {
    return []string{"payment"}
}

func (m *OrderModule) SetDependencyServiceContainer(dep string, container mono.ServiceContainer) {
    if dep == "payment" {
        m.paymentContainer = container
    }
}

func (m *OrderModule) Start(ctx context.Context) error {
    // Get payment service client from dependency container
    client, err := m.paymentContainer.GetRequestReplyService("process")
    if err != nil {
        return fmt.Errorf("payment service not available: %w", err)
    }
    m.paymentClient = client

    return nil
}

func (m *OrderModule) processOrder(ctx context.Context, order *Order) error {
    // Serialize request
    requestData, err := json.Marshal(&PaymentRequest{Amount: order.Total})
    if err != nil {
        return fmt.Errorf("failed to serialize request: %w", err)
    }

    // Call payment service
    resp, err := m.paymentClient.Call(ctx, requestData)
    if err != nil {
        return fmt.Errorf("payment failed: %w", err)
    }

    // Deserialize response
    var payment PaymentResponse
    if err := json.Unmarshal(resp.Data, &payment); err != nil {
        return fmt.Errorf("invalid response: %w", err)
    }

    if !payment.Success {
        return fmt.Errorf("payment rejected")
    }

    return nil
}
```

## Best Practices

✓ **Do**
- Register services in `RegisterServices()` method
- Check for service availability before using
- Use request-reply for critical operations
- Use queue group for non-critical async work
- Use stream consumer for persistent message processing

✗ **Don't**
- Call service methods directly (use clients)
- Assume services are always available
- Register the same service name twice
- Block indefinitely in service handlers
- Ignore error returns from Send/Call

## Related Documentation

- [Framework API](framework.md) - Framework lifecycle
- [Module API](module.md) - Module interfaces
- [EventBus API](eventbus.md) - Event publishing
- [API Reference](README.md) - All APIs overview
- [Core Concepts - Inter-Module Communication](../core-concepts/inter-module-communication.md) - Service and Event patterns explained

---

For more information, see [Foundation Specification](../../spec/foundation.md) and [Core Concepts](../core-concepts/README.md).
