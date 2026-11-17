# 🤔 FAQ

Common questions developers ask when working with the Monolith Framework.

{% hint style="info" %}
**Can't find your answer?** Check the [How-To Guides](../guides/README.md) for step-by-step instructions, or the [Core Concepts](../core-concepts/README.md) for deeper explanations.
{% endhint %}

## Module Development

### Q: How do I add a new module?

**A:** Creating a module is straightforward. Implement the `Module` interface and register it with the application:

```go
// Define your module
type OrderModule struct {
    logger mono.Logger
}

// Implement required Module interface
func (m *OrderModule) Name() string {
    return "orders"
}

func (m *OrderModule) Start(ctx context.Context) error {
    m.logger.Info("Order module starting")
    return nil
}

func (m *OrderModule) Stop(ctx context.Context) error {
    m.logger.Info("Order module stopping")
    return nil
}

// Register and start
func main() {
    app, err := mono.NewMonoApplication()
    if err != nil {
        log.Fatal(err)
    }
    // Nothing injects a logger into a plain module: pass the framework's.
    if err := app.Register(&OrderModule{logger: app.Logger()}); err != nil {
        log.Fatal(err)
    }
    if err := app.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
    defer app.Stop(context.Background())
}
```

**Key points:**
- Implement `Name()`, `Start()`, and `Stop()` methods
- Optionally implement additional interfaces (EventBusAwareModule, DependentModule, etc.)
- Register your module with `app.Register()`
- The framework calls lifecycle methods in order

**Learn more:** [Modules - Core Concepts](../core-concepts/modules.md)

---

### Q: How do modules communicate with each other?

**A:** The Monolith Framework provides in-process Channel services plus four patterns over NATS. In the snippets, `container` is the module's own container (passed to `RegisterServices`), `m.orders` is the "order" dependency's container (passed to `SetDependencyServiceContainer`), and `helper` is `github.com/go-monolith/mono/pkg/helper`.

1. **Channel Services** - In-process Go channels for bidirectional communication (lowest latency, single process only, not through NATS)

```go
// Module A registers a channel pair in RegisterServices
in, out := make(chan *mono.Msg, 100), make(chan *mono.Msg, 100)
err := container.RegisterChannelService("updates", in, out)
```

```go
// Module B gets the channels from its dependency container
in, out, err := m.orders.GetChannelService("updates", m.Name())
```

2. **Request-Reply Services** - Synchronous inter-module calls with automatic service discovery (supports distribution)

```go
// Module A registers a service in RegisterServices
err := helper.RegisterTypedRequestReplyService(container, "get-order",
    json.Unmarshal, json.Marshal, m.handleGetOrder)
```

```go
// Module B calls the service
var resp GetOrderResponse
err := helper.CallRequestReplyService(ctx, m.orders, "get-order",
    json.Marshal, json.Unmarshal, GetOrderRequest{ID: "42"}, &resp)
```

3. **Queue Group Services** - Asynchronous load-balanced processing (fire-and-forget)

```go
// Module A registers a queue group in RegisterServices
err := container.RegisterQueueGroupService("process-order",
    mono.QGHP{QueueGroup: "order-workers", Handler: m.handleProcessOrder})
```

```go
// Module B sends to the queue
err := helper.SendQueueGroupService(ctx, m.orders, "process-order",
    json.Marshal, ProcessOrderRequest{ID: "42"})
```

4. **Event Stream Consumers** - Persistent event consumption with acknowledgment (JetStream)

```go
// Shared definition, in the emitter's contract package
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent]("order", "OrderCreated", "v1")
```

```go
// Module A emits events
err := OrderCreatedV1.Publish(m.eventBus, OrderCreatedEvent{OrderID: "42"}, nil)
```

```go
// Module B registers a consumer in RegisterEventConsumers(registry mono.EventRegistry) error
return helper.RegisterTypedEventStreamConsumer(registry, OrderCreatedV1,
    mono.StreamConsumerConfig{Stream: mono.StreamConfig{Name: "order-events"}},
    m.handleOrderCreated, m)
```

5. **Cron Services** - Server-scheduled periodic work via the JetStream message scheduler (single fire per occurrence across a cluster, requires JetStream)

```go
// Register a server-side schedule in RegisterServices; the handler runs on each occurrence.
err := container.RegisterCronService("nightly-rollup", mono.CronServiceConfig{
    Schedule: "@daily",
    Payload:  []byte(`{"job":"rollup"}`),
}, m.handleRollup)
```

**Best practice:** Choose based on requirements:
- Use Channel for high-frequency in-process communication
- Use Request-Reply for synchronous operations
- Use Queue Groups for asynchronous fire-and-forget work
- Use Stream Consumers for durable, acknowledged work (JetStream)
- Use Cron services for scheduled work (JetStream)
- Use Event Consumers for event-driven patterns

**Learn more:** [Inter-Module Communication - Core Concepts](../core-concepts/inter-module-communication.md)

---

## Inter-Module Communication Patterns

### Q: What's the difference between Channel and Request-Reply services?

**A:** They serve different purposes:

| Aspect | Channel | Request-Reply |
|--------|---------|---------------|
| **Type** | Go channel (in-process) | NATS message (distributed) |
| **Latency** | ~1 microsecond | ~1 millisecond |
| **Distribution** | Single process only | Across modules (even remote) |
| **Protocol** | Native Go | NATS protocol |
| **Use Case** | High-frequency updates | Cross-module API calls |

**Example - Channel Service (high frequency):**

```go
// Provider registers the channel pair in RegisterServices
in, out := make(chan *mono.Msg, 100), make(chan *mono.Msg, 100)
if err := container.RegisterChannelService("updates", in, out); err != nil {
    return err
}
```

```go
// A consumer module gets the channels from its dependency container
in, _, err := m.orders.GetChannelService("updates", m.Name())
if err != nil {
    return err
}
for _, update := range updates {
    in <- &mono.Msg{Data: update} // Very fast, no serialization through NATS
}
```

**Example - Request-Reply Service:**

```go
// Module registers the service in RegisterServices
err := helper.RegisterTypedRequestReplyService(container, "get-status", json.Unmarshal, json.Marshal,
    func(ctx context.Context, req StatusRequest, msg *mono.Msg) (StatusResponse, error) {
        return StatusResponse{Status: "ready"}, nil
    })
```

```go
// Caller invokes the service through the provider's dependency container
var resp StatusResponse
err := helper.CallRequestReplyService(ctx, m.status, "get-status",
    json.Marshal, json.Unmarshal, StatusRequest{}, &resp)
```

**Learn more:** [Inter-Module Communication - Core Concepts](../core-concepts/inter-module-communication.md)

---

## Error Handling

### Q: How do I handle errors in handlers?

**A:** Follow Go's error handling conventions and wrap errors with context:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    // Validate request
    if req.CustomerID == "" {
        m.logger.Warn("Invalid request",
            "error", "missing customer ID",
        )
        return nil, errors.New("customer ID required")
    }

    // Call dependency
    if err := m.validateCustomer(ctx, req.CustomerID); err != nil {
        // Wrap with context
        return nil, fmt.Errorf("failed to validate customer: %w", err)
    }

    // Process
    order := &Order{ID: generateID()}

    if err := m.persistOrder(ctx, order); err != nil {
        // Log full context
        m.logger.Error("Failed to persist order",
            "order_id", order.ID,
            "error", err,
        )
        return nil, fmt.Errorf("failed to persist order: %w", err)
    }

    return order, nil
}
```

**Key principles:**
- Check errors immediately after function calls
- Wrap errors with context using `fmt.Errorf(...%w...)`
- Log errors with debugging information
- Return user-friendly error messages to clients

**Learn more:** [Error Handling - How-To Guide](../guides/error-handling.md)

---

## Testing

### Q: How do I test modules in isolation?

**A:** Create mocks for dependencies and test the module directly:

```go
// Create mock dependency
type MockPaymentService struct {
    chargeFunc func(ctx context.Context, amount int64) error
}

func (m *MockPaymentService) Charge(ctx context.Context, amount int64) error {
    if m.chargeFunc != nil {
        return m.chargeFunc(ctx, amount)
    }
    return nil
}

// Test module with mock
func TestCreateOrder(t *testing.T) {
    mockPayment := &MockPaymentService{}

    module := &OrderModule{
        paymentService: mockPayment,
        logger:        newTestLogger(),
    }

    ctx := context.Background()
    order, err := module.handleCreateOrder(ctx, &CreateOrderRequest{
        CustomerID: "cust-1",
        Amount:     100,
    })

    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if order.ID == "" {
        t.Error("order ID should not be empty")
    }
}
```

**Best practices:**
- Create lightweight mocks for dependencies
- Use table-driven tests for multiple cases
- Test both success and error paths
- Mock external dependencies (databases, services)

Since v0.1.0 you can also start a module without NATS using `monotest`, and check its boundaries with one call:

```go
bus := monotest.NewEventBus()
inventory := monotest.NewContainer("inventory", bus) // register stand-in services on it
monotest.CheckModule(t, order.NewModule(),
    monotest.WithEventBus(bus),
    monotest.WithDependency("inventory", inventory))
```

**Learn more:** [Testing - How-To Guide](../guides/testing.md) and the [Module Boundaries Guide](../guides/module-boundaries.md)

---

## Framework Features

### Q: Can I use this framework without JetStream?

**A:** Yes! JetStream is optional. By default, the framework uses NATS Core (in-memory messaging) without persistence:

```go
// Create application without JetStream (default)
app, err := mono.NewMonoApplication()

// All communication patterns work with Core NATS:
// - Request-Reply: Yes
// - Channel Services: Yes
// - Queue Groups: Yes
// - Event Bus (Pub/Sub): Yes

// Only JetStream-specific features are unavailable:
// - Event Stream Consumers (use regular Event Consumers instead)
// - Stream Consumer services
// - Cron services
// - The fs-jetstream and kv-jetstream storage plugins
// - Message persistence across restarts
// - Stream replay
```

**Enable JetStream only if you need persistence** (setting a storage directory enables it):

```go
app, err := mono.NewMonoApplication(
    mono.WithJetStreamStorageDir("/var/lib/my-app/jetstream"),
)
```

**When to enable JetStream:**
- You need message persistence
- You want to replay events after restart
- You need guaranteed message delivery (at-least-once)
- You're implementing event sourcing

**Learn more:** [Architecture - Core Concepts](../core-concepts/architecture.md)

---

### Q: How do I implement health checks?

**A:** Implement the `HealthCheckableModule` interface:

```go
type OrderModule struct {
    logger mono.Logger
    db     *sql.DB // Dependency to check
}

// Implement health check
func (m *OrderModule) Health(ctx context.Context) mono.HealthStatus {
    // Check if database is responsive
    if err := m.db.PingContext(ctx); err != nil {
        m.logger.Warn("Health check failed", "error", err)
        return mono.HealthStatus{Healthy: false, Message: err.Error()}
    }

    return mono.HealthStatus{Healthy: true, Message: "operational"}
}

// Framework aggregates health from all modules
app, err := mono.NewMonoApplication()
// ... register and start the modules ...

// Get overall health
health := app.Health(ctx) // Healthy only if the framework, NATS and all health-aware modules are healthy
```

**Key points:**
- Return `mono.HealthStatus{Healthy: ..., Message: ...}` (optionally with `Details`)
- Keep health checks fast (no expensive operations)
- Check critical dependencies only
- Framework aggregates health from all modules

---

## Project Structure

### Q: What's the recommended project structure?

**A:** Give each module its own package under `modules/`, with a `contract` sub-package that is the only package other modules import:

```
my-app/
├── main.go                          # Entry point: creates the app, registers modules
├── go.mod
├── go.sum
│
├── modules/
│   ├── order/
│   │   ├── contract/                # DTOs, typed service contracts, emitted events
│   │   │   └── contract.go
│   │   ├── module.go                # mono.Module implementation
│   │   ├── ports.go                 # Ports this module needs, in its own types
│   │   ├── adapter.go               # Adapters to other modules' contracts
│   │   ├── service.go               # Business logic over the ports
│   │   └── module_test.go           # monotest.CheckModule conformance test
│   │
│   └── payment/
│       ├── contract/
│       │   └── contract.go
│       └── module.go
│
└── imports_test.go                  # monotest.CheckImports(t): checks the contract import rule
```

**Key principles:**
- Group related functionality in modules
- Keep modules independent and focused
- Import another module's `contract` package and nothing else of it
- Document architecture and design decisions

**Learn more:** [Getting Started - Project Structure](../getting-started/project-structure.md)

---

## Troubleshooting

### Q: My service handler isn't being called. What's wrong?

**A:** Check these common issues:

1. **Module not registered:**
```go
app.Register(&MyModule{})  // Don't forget this!
```

2. **Service not registered in `RegisterServices`:** implement `ServiceProviderModule` and register there (the framework calls it before `Start`):
```go
func (m *MyModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterRequestReplyService("my-service", m.handleRequest)
}
```

3. **Service name mismatch:**
```go
// Registration in module "order" uses the subject "services.order.get-order"
container.RegisterRequestReplyService("get-order", m.handleGetOrder)
```

```go
// The caller must use the same service name, on the "order" dependency container
client, err := m.orders.GetRequestReplyService("get-order")
```

4. **Module not started:**
```go
app.Start(ctx)  // Must call Start before using services!
```

---

### Q: Events aren't being received. What's wrong?

**A:** Check these common issues:

1. **Consumer not registered before Start():**
```go
// Register BEFORE start
app.Register(&EventConsumerModule{})  // Must implement RegisterEventConsumers
app.Start(ctx)  // Then start
```

2. **Event handler not registered:**
```go
func (m *MyModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    return helper.RegisterTypedEventConsumer(registry, OrderCreatedV1, m.handleOrderCreated, m)
}
```

3. **Event subject doesn't match:**
```go
// Publisher and consumer must use the same event definition
// (helper.EventDefinition, normally from the emitter's contract package)
err := OrderCreatedV1.Publish(m.eventBus, OrderCreatedEvent{OrderID: "42"}, nil)
```

---

## Additional Resources

- [How-To Guides](../guides/README.md) - Step-by-step instructions for common tasks
- [Core Concepts](../core-concepts/README.md) - Foundational concepts and architecture
- [API Reference](../api/README.md) - Complete API documentation
- [Examples](../../../examples/) - Runnable example applications

---

Still have questions? Check the [How-To Guides](../guides/README.md) or [Core Concepts](../core-concepts/README.md) for more detailed information.
