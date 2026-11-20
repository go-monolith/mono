# Service Container API

Every module gets a `ServiceContainer`. A provider registers its services on it, and a consumer uses it to look up and call the services of the modules it depends on. Read this page when you register or call a service. For when to pick which kind of service, see [Inter-Module Communication](../core-concepts/inter-module-communication.md).

The recommended path is [typed service contracts](#typed-service-contracts). The raw methods further down are what the contracts are built on.

## Overview

A container supports five kinds of service: in-process channels, request-reply, queue groups, stream consumers and cron (server-scheduled) services.

```go
// Registration (provider side)
func RegisterChannelService(name string, in chan *Msg, out chan *Msg) error
func RegisterRequestReplyService(name string, handler RequestReplyHandler, opts ...ServiceOption) error
func RegisterQueueGroupService(name string, pairs ...QGHP) error
func RegisterStreamConsumerService(name string, config StreamConsumerConfig, handler StreamConsumerHandler) error
func RegisterCronService(name string, config CronServiceConfig, handler CronHandler) error

// Lookup (consumer side)
func GetChannelService(serviceName string, consumerModule string) (in chan *Msg, out chan *Msg, err error)
func MustGetChannelService(serviceName string, consumerModule string) (in chan *Msg, out chan *Msg)
func GetRequestReplyService(name string) (RequestReplyServiceClient, error)
func GetQueueGroupService(name string) (QueueGroupServiceClient, error)
func GetStreamConsumerService(name string) (StreamConsumerServiceClient, error)

// Inspection
func Has(name string) bool
func Entries() []*ServiceEntry
func Unregister(name string) error
```

{% hint style="info" %}
**Pattern Selection:** Use Channel for lowest latency (~µs), Request-Reply for synchronous calls (~ms), Queue Group for async fire-and-forget, and Stream Consumer for persistent message processing with acknowledgment.
{% endhint %}

The framework binds the container to the module before calling `RegisterServices`; do not call `BindModule`, `SetEventBus`, `SetMiddlewareChain`, `SetQueueGroupOptimisticWindow` or `StartChannelRouters` yourself.

## Typed Service Contracts

Since v0.1.0, `pkg/helper` provides typed contracts that wrap the registration and lookup methods below. Declare one per service in the provider's `contract` package:

```go
func RequestReplyService[Req, Resp any](moduleName, serviceName string) RequestReplyDefinition[Req, Resp]
func QueueGroupService[T any](moduleName, serviceName string) QueueGroupDefinition[T]
func StreamConsumerService[T any](moduleName, serviceName string) StreamConsumerDefinition[T]
```

| Definition | Provider | Consumer |
|------------|----------|----------|
| `RequestReplyDefinition[Req, Resp]` | `Register(container, handler, opts...)` | `Call(ctx, container, req) (Resp, error)` |
| `QueueGroupDefinition[T]` | `Register(container, pairs...)` | `Send(ctx, container, payload) error` |
| `StreamConsumerDefinition[T]` | `Register(container, config, handler)` | `Publish(ctx, container, payload, opts...) (MsgPubAck, error)` |

- All three have `WithMarshaler`, `WithUnmarshaler` (JSON by default), `ToBase()` and `Subject()`.
- The constructors panic on a module or service name that is not kebab-case.
- `Register` records the contract in `ServiceEntry.Contract`. It returns `ErrServiceContractMismatch` when the container belongs to a different module than the contract names.

**Message IDs.** `Publish` (and `helper.PublishStreamConsumerService`) accept `helper.WithMsgID(id)`, which sends the JetStream message ID header (`Nats-Msg-Id`, available as `helper.MsgIDHeader`, for example to set it yourself on a `PublishMsg`).

- The stream stores an ID once within its duplicate window (`StreamConfig.Duplicates`, 2 minutes when zero) and acknowledges a repeat with `Duplicate() == true` and the sequence of the stored message.
- Use it to retry a failed publish: a publish error, for example a timeout while a cluster has no quorum, does not prove that the message was not stored.
- Derive the ID from the message's identity (an order ID, an idempotency key), not from the attempt.
- An empty ID fails with `helper.ErrEmptyMsgID`.

```go
ack, err := contract.OrderPlaced.Publish(ctx, services, event, helper.WithMsgID("order-"+event.OrderID))
```

See the [Module Boundaries Guide](../guides/module-boundaries.md) for where contracts live and how consumers use them.

### String-based typed helpers

The contracts are built on lower-level helpers that take the service name as a string. They register typed handlers without declaring a contract (nothing is recorded in `ServiceEntry.Contract`):

```go
func RegisterTypedRequestReplyService[Req, Resp any](container ServiceContainer, name string, unmarshalReq Unmarshaler, marshalResp Marshaler, handler TypedRequestReplyHandler[Req, Resp], opts ...ServiceOption) error
func RegisterTypedQueueGroupService[T any](container ServiceContainer, name string, unmarshalF Unmarshaler, pairs ...TypedQGHP[T]) error
func RegisterTypedStreamConsumerService[T any](container ServiceContainer, name string, config StreamConsumerConfig, unmarshalF Unmarshaler, handler TypedStreamConsumerHandler[T]) error
```

The typed handler types live in `github.com/go-monolith/mono/pkg/types`. `types.TypedQGHP[T]` has the same fields as `QGHP`:

```go
type TypedRequestReplyHandler[Req any, Resp any] func(ctx context.Context, req Req, msg *Msg) (Resp, error)
type TypedQueueGroupHandler[T any] func(ctx context.Context, payload T, msg *Msg) error
type TypedStreamConsumerHandler[T any] func(ctx context.Context, payloads []T, msgs []*Msg) error
```

The consumer-side counterparts are `helper.CallRequestReplyService`, `helper.SendQueueGroupService` and `helper.PublishStreamConsumerService`.

## Service Registration Methods

Each service is reachable on the subject `services.<module>.<name>`. For a typed handler, use the contract's `Register` or the matching [string-based typed helper](#string-based-typed-helpers).

### RegisterChannelService

```go
func (container ServiceContainer) RegisterChannelService(name string, in chan *Msg, out chan *Msg) error
```

{% hint style="warning" %}
**Single process only.** Channel services only work within the same process. For distributed communication, use Request-Reply or Queue Group services.
{% endhint %}

Registers a bidirectional Go channel service for in-process communication: the lowest-latency inter-module call (~microseconds).

**Parameters:**
- `name` - Service name (e.g., "submit-order")
- `in` - Input channel for receiving messages from consumers
- `out` - Output channel for sending responses to consumers

**Returns:** `error`, nil on success.

**Example:**
```go
func (m *OrderModule) RegisterServices(container mono.ServiceContainer) error {
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
    opts ...ServiceOption,
) error
```

Registers a request-reply service for synchronous inter-module calls via NATS. It works across process and machine boundaries, with ~1ms overhead.

**Parameters:**
- `name` - Service name (subject: `services.<module>.<name>`)
- `handler` - Function handling requests
- `opts` - Optional worker pool bounds: `mono.WithMaxConcurrency(n)`, `mono.WithMaxPending(n)` (see [Handler Concurrency](#handler-concurrency-worker-pools)). Negative values fail registration.

**Handler Signature:**
```go
type RequestReplyHandler func(ctx context.Context, req *Msg) (response []byte, err error)
```

**Returns:** `error`, nil on success.

**Example:**
```go
func (m *PaymentModule) RegisterServices(container mono.ServiceContainer) error {
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

**Deadline propagation:**
- The handler's `ctx.Deadline()` is the caller's deadline (sent in the `Mono-Deadline` / `Mono-Timeout` headers), and `ctx` is cancelled when the handler returns.
- Pass `ctx` to downstream calls so they stop when the caller gives up; use `context.WithoutCancel(ctx)` for work that must outlive the request.
- A request whose deadline passed by more than the clock-skew allowance (`mono.WithDeadlineSkewAllowance`, 250ms default) while it waited for a worker is dropped without running the handler or replying.

See [Wire Protocol Headers](eventbus.md#wire-protocol-headers).

### RegisterQueueGroupService

```go
func (container ServiceContainer) RegisterQueueGroupService(
    name string,
    pairs ...QGHP,
) error
```

Registers a queue group service with one or more handler pairs for load-balanced, fire-and-forget processing via NATS. It scales horizontally with multiple instances, and several queue groups can process the same service subject.

**Parameters:**
- `name` - Service name (subject: `services.<module>.<name>`)
- `pairs` - One or more queue group handler pairs (QGHP)

**QGHP Structure:**
```go
type QGHP struct {
    QueueGroup string            // Queue group name (e.g., "high-priority-workers")
    Handler    QueueGroupHandler // Handler function for this queue group
    Options    ServiceOptions    // Worker pool bounds for this queue group (zero = defaults)
}
```

Each pair is its own NATS subscription with its own worker pool, so `Options` is set per pair. Queue groups are serial by default (see [Handler Concurrency](#handler-concurrency-worker-pools)).

**Handler Signature:**
```go
type QueueGroupHandler func(ctx context.Context, msg *Msg) error
```

**Returns:** `error`, nil on success.

**Example:**
```go
func (m *AuditModule) RegisterServices(container mono.ServiceContainer) error {
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

**Multiple queue groups on one service:**
```go
// Multiple handlers for different processing priorities
container.RegisterQueueGroupService("process-orders",
    mono.QGHP{QueueGroup: "high-priority", Handler: m.handleHighPriority},
    mono.QGHP{QueueGroup: "low-priority", Handler: m.handleLowPriority},
)
```

### RegisterStreamConsumerService

```go
func (container ServiceContainer) RegisterStreamConsumerService(
    name string,
    config StreamConsumerConfig,
    handler StreamConsumerHandler,
) error
```

Registers a JetStream durable pull consumer service for persistent, ordered message processing: messages survive restarts, are delivered at least once, in batches, and can be replayed.

**Parameters:**
- `name` - Service name (subject pattern)
- `config` - JetStream consumer configuration
- `handler` - Function handling message batches

**Handler Signature:**
```go
type StreamConsumerHandler func(ctx context.Context, msgs []*Msg) error
```

The handler acknowledges each message itself, using `Ack()`, `Nak()`, `NakWithDelay()`, `Term()`, or `InProgress()`.

**Returns:** `error`, nil on success.

**Configuration:** `StreamConsumerConfig` combines stream, consumer and fetch configuration:

- `Stream` (`mono.StreamConfig`): `Name` is required. `Subjects` default to `services.<module>.<service>` and `services.<module>.<service>.>`; `Retention` defaults to limits and `Storage` to file. The storage type, and whether the retention is work-queue, cannot be changed once the stream exists: see [Changing a Stream's Storage Type or Retention](#changing-a-streams-storage-type-or-retention).
- `Consumer` (`mono.ConsumerConfig`): `AckPolicy` defaults to explicit, `AckWait` to 30s and `MaxDeliver` to 3.
- `Fetch` (`mono.FetchConfig{BatchSize, Timeout}`): defaults 10 messages and 5s.

**Replicas and the durable consumer:**

- **Stream replicas:** a `Stream.Replicas` of 0 takes the framework default ([`WithJetStreamDefaultReplicas`](framework.md#withjetstreamdefaultreplicas), otherwise one replica). The consumer follows the stream unless `Consumer.Replicas` is set.
- **Durable consumer:** the service is consumed through the durable consumer `<module>-<service>` (sanitized: spaces and `.` become `-`, other characters outside `[a-zA-Z0-9_-]` are removed). The framework sends it as both `Name` and `Durable`, so the server keeps the consumer, and its delivery position, across restarts and idle periods. A `Name` or `Durable` set in `Consumer` is replaced.
- **Settings that cannot be changed on an existing consumer:** `DeliverPolicy` with `OptStartSeq` and `OptStartTime`, `AckPolicy`, `ReplayPolicy`, `MaxWaiting` and `MemoryStorage`. See [Changing a Consumer Setting That Cannot Be Changed](#changing-a-consumer-setting-that-cannot-be-changed).
- **Upgrading from v0.1.x or earlier:** those versions sent only a `Name`, which the server treats as non-durable and deletes after about 5s without a pull; the next start then recreated it from its deliver policy and, with the default (all), replayed the stream. An existing consumer of that kind is converted to a durable in place at startup, keeping its position, and `Stream consumer converted to durable` is logged.

**Example:**
```go
func (m *AnalyticsModule) RegisterServices(container mono.ServiceContainer) error {
    config := mono.StreamConsumerConfig{
        Stream: mono.StreamConfig{
            Name:     "events",
            Subjects: []string{"events.>"},
        },
        Consumer: mono.ConsumerConfig{
            // Name and Durable are set by the framework: "<module>-analytics-consumer"
            MaxAckPending: 1000,
        },
        Fetch: mono.FetchConfig{
            BatchSize: 100,
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

### Changing a Consumer Setting That Cannot Be Changed

NATS cannot change these settings of a consumer that already exists:

| `ConsumerConfig` setting | Notes |
|---|---|
| `DeliverPolicy`, `OptStartSeq`, `OptStartTime` | They only decide where a *new* consumer starts. |
| `AckPolicy` | |
| `ReplayPolicy` | |
| `MaxWaiting` | Leaving it at zero asks for the server default (512). |
| `MemoryStorage` | |

This applies to the durable consumer of a stream consumer service and of an [event stream consumer](eventregistry.md#registereventstreamconsumer). The consumer is durable, so it does not go away by itself.

Since v0.2.15 a configuration that changes one of them **starts, keeps the value the consumer has, and logs a warning** at every start while the configuration differs:

```text
WARN Consumer setting cannot be changed on an existing consumer; the existing value is kept and the configured one is not applied. ...
     registration="stream consumer service orders.ingest" stream=ORDERS consumer=orders-ingest
     setting=DeliverPolicy configured=new kept=all apply_with="nats consumer rm ORDERS orders-ingest"
```

Before v0.2.15 such a configuration failed `Start` on every restart with the server's error (`deliver policy can not be updated`).

- **The consumer keeps its position.** Nothing it acknowledged is delivered again.
- **The configured value is not in effect.** Watch for the warning after changing one of these settings; the application starting is not a sign that the change was applied.
- **To apply the change, delete the consumer:** `nats consumer rm <stream> <consumer>`. The next start creates it with the configured values. It then starts from its deliver policy, so with the default (all) the stream is delivered again.
- **`AckPolicy` decides what the handler must do.** While the existing policy is kept, the handler has to acknowledge the way *that* policy requires. A handler that stops acknowledging because the configuration now says `AckNonePolicy` gets each message redelivered until `MaxDeliver` is reached.
- **A start time computed at startup** (`time.Now().Add(-time.Hour)`) differs on every start, so it is warned about at every start and the first value is kept. Use a fixed time.
- **Other settings are still applied** in the same update: `FilterSubject`, `MaxDeliver`, `AckWait`, `MaxAckPending`, `InactiveThreshold` and the rest can be changed in place.
- **If another setting does not fit a kept policy**, the server rejects the update and `Start` fails with that error.

A healthy start is not affected: the consumer is only read back after the server rejected the update.

### Changing a Stream's Storage Type or Retention

NATS cannot change two settings of a stream that already exists:

- **`Storage`:** file to memory, or memory to file.
- **`Retention`:** to or from `WorkQueuePolicy`. Limits and interest can be changed into each other in place.

This applies to the stream of a stream consumer service and to the stream of an [event stream consumer](eventregistry.md#registereventstreamconsumer). Cron service streams are not affected: their storage and retention are fixed.

Since v0.2.12 a configuration that changes one of them fails `Start` with `ErrStreamConfigImmutable` (package `github.com/go-monolith/mono/pkg/errors`). The error names what registered the stream, the stream and both values, and wraps the server's error:

```text
stream configuration cannot be changed in place: stream ORDERS_INGEST of stream consumer service
orders.ingest exists with file storage and is configured with memory storage; NATS cannot change
the storage type of an existing stream, or its retention to or from work-queue; keep the existing
value, or register a new service name or event version on a new stream and retire the old one once
it is drained (deleting the stream loses its messages and consumer positions): ... err_code=10052 ...
```

```go
if err := app.Start(ctx); err != nil {
    if errors.Is(err, monoerrors.ErrStreamConfigImmutable) {
        // the configured storage type or retention differs from the existing stream's
    }
    log.Fatalf("start: %v", err)
}
```

- **Every restart fails the same way.** JetStream state survives restarts, so restarting or redeploying does not help. Earlier versions failed too, with the server's generic error only.
- **Nothing is deleted.** The framework never deletes or recreates the stream, so its messages and the position of every consumer are as they were.
- **Registrations that disagree.** Two event stream consumers that register the same stream with different values fail `Start` with the same error before anything is created. The error names both registrations.

**Option 1: keep the existing value.** Set `Storage` or `Retention` back to what the stream has. Nothing is lost.

**Option 2: move to a new stream, side by side.** This loses no message, because the old and the new stream never share a subject:

1. **Add the new registration and keep the old one.** Register a second service under a new name (for example `ingest-v2`), or a new version of the event (`v2`), on a new stream name with the new settings.
2. **Deploy.** Both streams exist and both handlers run.
3. **Switch the publishers** to the new service or event version.
4. **Wait until the old stream is drained.** `nats consumer info <old stream> <consumer>` reports no unprocessed messages and no outstanding acknowledgements.
5. **Remove the old registration and deploy.** Then delete the old stream with `nats stream rm <old stream>`, or keep it if its history is still needed.

The new stream must listen on other subjects than the old one, otherwise the server rejects it (`subjects overlap with an existing stream`):

- **Default subjects** follow the service name or the event version, so a new name or version is enough.
- **Custom subjects** (`Stream.Subjects` of a stream consumer service, or an event defined with its own subject) must be changed as well.

**Settings that can only be switched on.** Five more `StreamConfig` settings can be enabled on an existing stream but never disabled again:

| Setting | Notes |
|---|---|
| `DenyDelete`, `DenyPurge` | |
| `AllowMsgTTL`, `AllowMsgSchedules` | |
| `Sealed` | Can only be set on a stream that already exists. Sealing also switches `DenyDelete` and `DenyPurge` on and `AllowRollup` off at the server. Cannot be combined with `AllowMsgSchedules`. |

Since v0.2.14:

- **Switching one off fails `Start` with `ErrStreamConfigImmutable`.** The error names the setting (`has DenyPurge enabled and is configured with it disabled`). Before, it was the server's generic error. The way out is the same: keep the setting enabled, or move to a new stream side by side.
- **A sealed stream keeps starting.** The framework writes a sealed stream with `DenyDelete` and `DenyPurge` enabled and `AllowRollup` disabled, as the server holds it. Before, every `Start` after the one that sealed the stream failed (`can not cancel deny message deletes`), unless both were set by hand.
- **`Sealed` together with `AllowMsgSchedules`** fails `Start` with `ErrInvalidConfiguration` before the stream is touched. The server would seal such a stream and then reject every later write, and a sealed stream cannot be unsealed.
- **`Sealed` on a stream that does not exist yet** fails `Start` with `ErrInvalidConfiguration` and says so: start once without `Sealed`, then set it.
- **Registrations of one stream must agree** on these settings as well, or `Start` fails before anything is created.

{% hint style="warning" %}
**Why the framework does not migrate the stream itself.** Changing either setting means deleting the stream and creating it again, and no other stream can hold its subjects in the meantime without a moment in which no stream listens. A message published in that moment without an acknowledgement, such as an event sent with `EventDefinition.Publish` or `EventBus.Publish`, is dropped silently. Other instances and other clients keep publishing while one instance starts, so the framework cannot rule that out.
{% endhint %}

{% hint style="danger" %}
**Deleting the stream** (`nats stream rm <stream>`) and restarting also applies the new settings, but it permanently deletes every message in the stream and the position of every consumer on it. Use it only when the stream holds nothing you need, for example in a development environment.
{% endhint %}

### Sharing a Stream Between Registrations

Several registrations may name the same stream: stream consumer services, [event stream consumers](eventregistry.md#registereventstreamconsumer), or both.

- **Same subjects.** Two modules that consume the same event on one stream each get every event (on a limits or interest stream). Nothing changed here.
- **Different subjects.** Since v0.2.13 the stream is written with the subjects of all its registrations, and each consumer is filtered on the subjects of its own registration. Each handler receives only its own messages.

```go
// Two events of one module, kept in one stream.
config := mono.StreamConsumerConfig{Stream: mono.StreamConfig{Name: "billing-events"}}
registry.RegisterEventStreamConsumer(invoiceIssued.ToBase(), config, m.handleInvoice, m)
registry.RegisterEventStreamConsumer(paymentReceived.ToBase(), config, m.handlePayment, m)
```

{% hint style="warning" %}
**Before v0.2.13 this configuration lost messages.** Each registration overwrote the stream's subjects with its own, so only one registration's messages were stored, and the handlers received each other's messages. `Start` succeeded and nothing was logged. For stream consumer services, which registration won could change from one start to the next.
{% endhint %}

Details:

- **Existing deployments are repaired in place** at the next start: the stream gets the missing subjects and the durable consumers get their filter. They keep their position, so nothing they acknowledged is delivered again. Messages that were never stored cannot be recovered.
- **Your own filter is kept.** A registration that sets `Consumer.FilterSubject` or `Consumer.FilterSubjects` is not given another one.
- **Subjects that cover each other** (`tasks.>` and `tasks.urgent`) are reduced to the covering one for the stream. Both consumers keep their own filter, so a message on `tasks.urgent` goes to both handlers.
- **Subjects that overlap without one covering the other** (`a.*.c` and `a.b.*`) cannot be in one stream, unless a third subject on the stream covers both. `Start` fails with `ErrInvalidConfiguration`, naming both registrations, before anything is written.
- **Other stream settings** (`MaxAge`, limits, `Description`) are not merged. Every registration writes them, and the one set up last wins. Services are set up in name order, modules first by name, then event stream consumers in registration order, so the result is the same on every start. Give the registrations of one stream the same settings.
- **`Storage`, work-queue retention and the settings that can only be switched on** must agree across the registrations of a stream, or `Start` fails with `ErrStreamConfigImmutable` (see above).
- **Work-queue streams** accept one consumer per subject. Registrations with different subjects can share one; registrations whose subjects are the same or cover each other cannot.
- **When a stream stops being shared** (the other registrations are removed), the remaining consumers keep the filter on their own subjects, also when those subjects change. The stream may still hold the removed registrations' messages, and they are not delivered to the remaining handlers. To free the space, purge them (`nats stream purge <stream> --subject <subject>`).
- **Rolling deploy from an earlier version:** an instance of the earlier version that starts later overwrites the subjects again, until every instance runs v0.2.13 or later. The next start of a newer instance repairs them.

### Renaming or Removing a Stream Registration

The names of durable consumers and service subjects are derived from module, service and event names:

| Registration | Durable consumer |
|---|---|
| Stream consumer service | `<module>-<service>` |
| Event stream consumer | `<consumer-module>-<event-module>-<event>-<version>` |

A rename therefore creates a **new** durable, and the old one stays on the server. Removing a registration leaves its durable, and for a service its stream. The framework never deletes them, because another version of the application may still be running and using them, and deleting them loses their position or messages.

| What changed | Limits stream | Interest stream | Work-queue stream |
|---|---|---|---|
| Module or service renamed | The new durable starts from its deliver policy: with the default, the stream is delivered again | Handled messages are kept, because the old durable never acknowledges them | `Start` fails: the old durable holds the subjects |
| Registration removed | Nothing is consumed; the stream keeps its messages | Messages the remaining consumers handled are kept | Messages are kept |
| Service removed or changed to another kind | The stream keeps storing what is published to the service subject | The same | The same |

Messages are kept until the stream's own limits (`MaxAge`, `MaxMsgs`, `MaxBytes`) remove them. Without limits, which is the default, they are kept without bound.

**Leftover report.** Since v0.2.13 the framework marks the streams and durable consumers it creates (metadata entry `mono.registration`) and, at every start, logs a warning for each marked one that no registration of the application uses:

```text
WARN Consumer left behind by a stream registration this application no longer has (renamed or removed). ...
     stream=EVENTS consumer=billing-orders-OrderPlaced-v1
     created_for="event stream consumer of module billing for event orders.OrderPlaced"
     retains_messages=true remove_with="nats consumer rm EVENTS billing-orders-OrderPlaced-v1"
WARN Stream left behind by a stream registration this application no longer has (renamed or removed). ...
     stream=ORDERS created_for="stream consumer service orders.ingest" remove_with="nats stream rm ORDERS"
```

- **Check before you remove.** During a rolling deploy, an instance of another version may still use what is reported, and so may another application built on the framework that uses the same JetStream: its streams and consumers carry the same marker. Removing a consumer loses its position; removing a stream loses its messages.
- **A stream that existed before** and that a registration names is marked too, and reported once no registration names it.
- **What is not reported:** streams and consumers the application created itself, and anything left behind before v0.2.13 (it never received the marker). List those with `nats stream ls` and `nats consumer ls <stream>`.
- **Marking happens at the first start on v0.2.13 or later**, in place, without changing a consumer's position.
- **The report is best effort.** It inspects at most 64 unregistered consumers and 64 unregistered streams per start, within 10 seconds, logs when it could not inspect everything, and never fails `Start`.
- **It runs during `Start`.** With a JetStream that answers slowly it can add up to those 10 seconds to startup; allow for that in a startup probe.

A new durable cannot take over the old one's position, so on a limits stream a rename means the stream is delivered again. Handlers should be idempotent in any case, because delivery is at least once.

### RegisterCronService

```go
func (container ServiceContainer) RegisterCronService(
    name string,
    config CronServiceConfig,
    handler CronHandler,
) error
```

Registers a cron-scheduled service backed by the embedded NATS JetStream message scheduler (nats-server v2.14+). Use it for periodic work such as nightly rollups, cache warmups, heartbeat emits or downsampling.

**How it behaves:**
- The schedule is registered **server-side**, so in a multi-node cluster exactly one message fires per occurrence (no client-side ticker, no leader election).
- Each occurrence is delivered through a durable pull consumer with explicit acknowledgement.
- The consumer keeps its position across restarts: an occurrence that fired while the application was down is delivered, and occurrences already handled are not delivered again (delivery is at-least-once, so an occurrence whose acknowledgement was lost at shutdown is).
- If the consumer is missing at startup (for example, deleted by an operator), it is recreated after the occurrences already stored, so it delivers only new ones; the number it skipped is logged as a warning.
- Requires JetStream (`WithJetStreamStorageDir(...)`); registering a cron service without JetStream fails fast at startup.
- Cron services are server-driven and have **no** consumer-side client (there is no `GetCronService`).

**Parameters:**
- `name` - Service name. The handler receives ticks on `services.<module>.<name>`.
- `config` - `CronServiceConfig` (schedule, payload/source, time zone, TTL, replicas, deprecation)
- `handler` - Function invoked on each scheduled occurrence

**Handler Signature:**
```go
type CronHandler func(ctx context.Context, msg *Msg) error
```

Acknowledgement is owned by the framework: return `nil` to Ack the occurrence, or a non-nil error to Nak it (redelivered up to `MaxDeliver`). The handler must **not** call `msg.Ack()`/`Nak()` itself. A recovered panic is treated as an error (Nak).

**CronServiceConfig:**

| Field | Type | Description |
|-------|------|-------------|
| `Schedule` | `string` | Required. Cron expression — standard five-field (`"0 0 * * *"` = daily at midnight) or six-field seconds-first (`"0 0 0 * * *"`; five-field forms get a `"0"` seconds field prepended) — alias (`"@daily"`), or interval (`"@every 5m"`, min 1s) |
| `Payload` | `[]byte` | Static payload delivered each occurrence (mutually exclusive with `SourceSubject`) |
| `SourceSubject` | `string` | Deliver the last message seen on this subject instead of a static payload |
| `TimeZone` | `string` | Optional IANA time zone (cron expressions only; not `@every`) |
| `TTL` | `time.Duration` | Optional per-occurrence message TTL (≥ 1s) |
| `Replicas` | `int` | Replicas of the service's stream (schedule and occurrences) and consumer in a cluster. `0` = the framework default (`WithJetStreamDefaultReplicas`, otherwise 1); at most 5. With one replica the schedule lives on one server and stops firing while that server is down; use 3 in a cluster of at least three servers |
| `Deprecated` | `bool` | When true, purge the schedule on deploy and don't start the consumer (keep the code) |

**Returns:** `error`, nil on success; non-nil if validation fails (e.g. empty `Schedule`, both `Payload` and `SourceSubject` set, invalid `TimeZone`, `Replicas` outside 0..5, nil handler, `TTL` set below 1s, `SourceSubject` with a wildcard, or equal to `services.<module>.<name>` or to its reserved `.schedule` or `.control` sub-subject).

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

**Changing and retiring a schedule:**
- Re-publishing the schedule on every startup is idempotent (rollup by subject), so changing `Schedule`/`Payload`/`TimeZone`/`TTL` and redeploying overwrites the live schedule in place.
- Removing `TTL` again works since v0.2.14. The service's stream keeps per-message TTLs enabled, because the server cannot switch them off; a schedule without `TTL` simply sends none. Before, removing `TTL` failed every `Start` (`message TTL status can not be disabled`).
- `SourceSubject` may be a subject under the service's own subjects (`services.<module>.<name>.<anything>`) since v0.2.14; before, the server rejected the stream. The `.schedule` and `.control` sub-subjects are reserved.
- `SourceSubject` must be a single subject. One with a wildcard (`*` or `>`) is rejected at registration since v0.2.14; before, `Start` failed after the stream was written (`message schedules source is invalid`).
- To retire a service, set `Deprecated: true` and deploy (purges the schedule, keeps the code), then remove the `RegisterCronService` call in a later release.

## Service Discovery Methods

A consumer calls these on the container it received for a dependency in `SetDependencyServiceContainer`. With a typed contract, `Call`, `Send` and `Publish` do the lookup for you.

{% hint style="info" %}
**Dependency containers are read-only views.** The container a module receives in `SetDependencyServiceContainer` allows lookups only.

- Mutating calls are recorded as boundary violations, and fail with `ErrModuleBoundaryViolation` under `WithStrictModuleBoundaries`.
- Since v0.1.2, so is a lookup of a service missing from the module's `ConsumeServices()`, when the module implements it (by default only when the service exists: a lookup of a service the dependency does not provide fails with `ErrServiceNotFound`).
- `Entries()` on the view returns descriptions of the services: copies without the provider's handlers and channels, in both modes, with one warning logged for the first call per dependency. Changing one, including its stream or consumer configuration, has no effect on the provider.

See the [Module Boundaries Guide](../guides/module-boundaries.md).
{% endhint %}

### ServiceLookup

```go
type ServiceLookup interface {
    GetChannelService(serviceName string, consumerModule string) (in chan *Msg, out chan *Msg, err error)
    MustGetChannelService(serviceName string, consumerModule string) (in chan *Msg, out chan *Msg)
    GetRequestReplyService(name string) (RequestReplyServiceClient, error)
    GetQueueGroupService(name string) (QueueGroupServiceClient, error)
    GetStreamConsumerService(name string) (StreamConsumerServiceClient, error)
    Has(name string) bool
}
```

`mono.ServiceLookup` (v0.1.2+) is the read side of `ServiceContainer`, which embeds it. Hold a dependency's container as a `ServiceLookup` in code that only calls services, such as a driven adapter: registering on it is then a compile error. `Call`, `Send`, `Publish` and the `helper.Call*` / `Send*` / `Publish*` functions accept it.

```go
type inventoryAdapter struct{ services mono.ServiceLookup }

func (m *OrderModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
    if dependency == "inventory" {
        m.inventory = inventoryAdapter{services: container}
    }
}
```

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

Gets a client for calling a registered request-reply service, or an error if the service is not found.

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

Gets a client for sending messages to a registered queue group service, or an error if the service is not found.

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

Gets a client for publishing messages to a stream consumer service, or an error if the service is not found.

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

```go
if container.Has("payment-processor") {
    // Service is available
}
```

### Entries

```go
func (container ServiceContainer) Entries() []*ServiceEntry
```

Returns pointers to all registered service entries (on a dependency's container: descriptions, see the hint under [Service Discovery Methods](#service-discovery-methods)).

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
    Options               ServiceOptions // worker pool bounds (request-reply)
    QueueGroup            string
    QueueHandlers         []QGHP
    StreamConsumerConfig  *StreamConsumerConfig
    StreamConsumerHandler StreamConsumerHandler
    CronConfig            *CronServiceConfig
    CronHandler           CronHandler
    ScheduleSubject       string // cron: internal schedule subject
    Contract              *BaseServiceDefinition // set when registered through a typed contract
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

Removes a service from the container. Returns an error if the service is not found.

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

## Handler Concurrency (Worker Pools)

**Why:** NATS delivers the messages of one subscription to its callback **one at a time**. Request-reply services, queue group services and event consumers are each one subscription, so without a pool a single slow handler call (a blocking HTTP call, a slow query, a lock wait) delays every later message to that service on that instance.

**What the framework does:** it runs these handlers on a **bounded worker pool per subscription**. The subscription callback only hands the message over; a worker goroutine runs the handler. Workers are started on demand and exit when idle, so an idle service holds no goroutines. Stream consumers, event stream consumers and cron services pull messages in their own loops and are not affected.

```go
type ServiceOptions struct {
    MaxConcurrency int // handlers running at once on this instance; 0 = default, 1 = serial, in order
    MaxPending     int // messages waiting for a free worker; 0 = default
    PendingMsgs    int // NATS client buffer behind the pool; 0 = default, -1 = unlimited
    PendingBytes   int // same, in bytes
}
```

| Setting | Default | How to change |
|---------|---------|---------------|
| `MaxConcurrency` (request-reply) | `runtime.GOMAXPROCS(0) * 4` | per registration: `mono.WithMaxConcurrency(n)`; app-wide: `mono.WithDefaultServiceConcurrency(n)` |
| `MaxConcurrency` (queue group, event consumer) | `1` (serial, in arrival order) | per registration: `QGHP.Options` / `EventConsumerOptions` |
| `MaxPending` | `16 × MaxConcurrency`, at least 256 | per registration: `mono.WithMaxPending(n)` / `QGHP.Options` / `EventConsumerOptions` |
| `PendingMsgs` / `PendingBytes` (NATS client buffer behind the pool) | request-reply: `10000` / `16 MB`; queue group, event consumer: nats.go defaults (`500000` / `64 MB`) | per registration: `mono.WithPendingLimits(msgs, bytes)` / `QGHP.Options` / `EventConsumerOptions`; `-1` = unlimited |

**How to set it:**
```go
// Request-reply: up to 32 handlers at once on this instance, at most 256 requests waiting
container.RegisterRequestReplyService("check-stock", m.handleCheckStock,
    mono.WithMaxConcurrency(32), mono.WithMaxPending(256))

// Request-reply: strictly one request at a time, in arrival order
container.RegisterRequestReplyService("apply-ledger-entry", m.handleLedger,
    mono.WithMaxConcurrency(1))

// Queue group: up to 16 notifications at once on this instance (each is independent)
container.RegisterQueueGroupService("send-notification",
    mono.QGHP{
        QueueGroup: "notification-workers",
        Handler:    m.handleNotification,
        Options:    mono.ServiceOptions{MaxConcurrency: 16},
    },
)
```

{% hint style="warning" %}
**Behaviour change for request-reply:** request-reply handlers now run concurrently by default (each request is independent). A request-reply handler that uses unsynchronized state must set `mono.WithMaxConcurrency(1)`, or the application can keep the previous serial behaviour with `mono.WithDefaultServiceConcurrency(1)`. Queue group and event consumer handlers stay serial and in order unless their registration opts in with `MaxConcurrency` above 1.
{% endhint %}

**When the pool and its pending queue are both full:**

| Message | Behaviour |
|---------|-----------|
| Request-reply call | Answered at once with an error the caller sees as `errors.ErrServiceOverloaded`. The request was not handled; retry, ideally on another instance. |
| Queue group `Send` (ACK mode) | `Send` returns `ErrServiceOverloaded` instead of an ACK. The message was not handled. |
| Queue group send in optimistic publish mode, event | Not rejected: the subscription waits for a free slot (back-pressure through the NATS client buffer). |

**The NATS client buffer behind the pool:** messages wait there before the subscription callback hands them to the pool.
- Request-reply: the callback does not wait for the pool (a request with a reply subject is answered "overloaded" at once), so the buffer only fills when the dispatcher falls behind the socket. A drop there is a slow consumer (logged, counted in `SubscriptionStats().Dropped`) and surfaces to the caller as a timeout, not `ErrServiceOverloaded`.
- Queue groups and event consumers: the buffer is the back-pressure reservoir when the pool is full, so its default is left at nats.go's.

**Sizing:**
- The pending default is generous on purpose: before worker pools a burst simply queued in the NATS client buffer and callers never saw an overload error. Lower `MaxPending` where failing fast matters more than absorbing bursts.
- A bound on the count cannot bound the wait: with a slow handler, a queued request may still wait longer than the caller's timeout.
- Size `MaxPending` for the burst you want to absorb and `MaxConcurrency` to drain it.

## Configuration and Defaults

| Property | Where it is set | Example |
|----------|-----------------|---------|
| Service name | `name` argument of `Register*` (kebab-case) | `"place-order"` |
| Call timeout | the caller's `context` deadline | `context.WithTimeout(ctx, 5*time.Second)` |
| Max message size | `mono.WithNATSMaxPayload(bytes)` | `2 * 1024 * 1024` |
| Stream consumer batch size | `FetchConfig.BatchSize` | `100` |
| Max concurrency | `mono.WithMaxConcurrency(n)` / `ServiceOptions.MaxConcurrency` | `32` |
| Max pending | `mono.WithMaxPending(n)` / `ServiceOptions.MaxPending` | `256` |

| Setting | Default |
|---------|---------|
| Request-reply call timeout (no context deadline) | 30 seconds |
| Queue group `Send` ACK timeout (no context deadline) | 5 seconds |
| Max message size | 1 MB |
| Stream consumer batch (`FetchConfig.BatchSize`) | 10 |
| Stream consumer fetch timeout (`FetchConfig.Timeout`) | 5 seconds |
| Stream consumer `AckWait` / `MaxDeliver` | 30 seconds / 3 |

Worker pool defaults are in [Handler Concurrency](#handler-concurrency-worker-pools).

## Best Practices

✓ **Do**
- Register services in `RegisterServices()` method
- Register and call through typed contracts
- Check for service availability before using
- Use request-reply for critical operations
- Use queue group for non-critical async work
- Use stream consumer for persistent message processing

✗ **Don't**
- Call service methods directly (use clients)
- Assume services are always available
- Register the same service name twice
- Register on a dependency's container
- Block indefinitely in service handlers
- Ignore error returns from Send/Call

## Related Documentation

- [Framework API](framework.md) - Framework lifecycle
- [Module API](module.md) - Module interfaces
- [Module Boundaries Guide](../guides/module-boundaries.md) - Contracts, ports and adapters
- [EventBus API](eventbus.md) - Event publishing
- [API Reference](README.md) - All APIs overview
- [Core Concepts - Inter-Module Communication](../core-concepts/inter-module-communication.md) - Service and Event patterns explained

---

For more information, see [Foundation Specification](../../spec/foundation.md) and [Core Concepts](../core-concepts/README.md).
