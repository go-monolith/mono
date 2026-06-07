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
    logger types.Logger
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
    app, _ := mono.NewMonoApplication()
    app.Register(&OrderModule{})
    app.Start(context.Background())
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

**A:** The Monolith Framework provides five communication patterns through NATS:

1. **Channel Services** - In-process Go channels for bidirectional communication (lowest latency, single process only)

```go
// Module A creates a channel and handles it
dataChannel := make(chan *Data)
go m.handleDataChannel(dataChannel)

// Module B sends data
dataChannel <- &Data{...}
```

2. **Request-Reply Services** - Synchronous inter-module calls with automatic service discovery (supports distribution)

```go
// Module A registers a service
m.container.RegisterRequestReplyService("get-order", m.handleGetOrder)

// Module B calls the service
response, err := m.container.RequestReplyService(ctx, "order", "get-order", &GetOrderRequest{})
```

3. **Queue Group Services** - Asynchronous load-balanced processing (fire-and-forget)

```go
// Module A registers queue group
m.container.RegisterQueueGroupService("process-order", "order-workers", m.handleProcessOrder)

// Module B publishes to queue
m.container.QueueGroupService(ctx, "order", "process-order", &ProcessOrderRequest{})
```

4. **Event Stream Consumers** - Persistent event consumption with acknowledgment (JetStream)

```go
// Module A emits events
m.eventBus.Publish(ctx, &OrderCreatedEvent{...})

// Module B registers consumer
m.eventBus.RegisterStreamConsumer(&OrderCreatedEvent{}, m.handleOrderCreated)
```

5. **Cron Services** - Server-scheduled periodic work via the JetStream message scheduler (single fire per occurrence across a cluster, requires JetStream)

```go
// Register a server-side schedule; the handler runs on each occurrence.
m.container.RegisterCronService("nightly-rollup", mono.CronServiceConfig{
    Schedule: "@daily",
    Payload:  []byte(`{"job":"rollup"}`),
}, m.handleRollup)
```

**Best practice:** Choose based on requirements:
- Use Channel for high-frequency in-process communication
- Use Request-Reply for synchronous operations
- Use Queue Groups for asynchronous fire-and-forget work
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
// Module creates channel for real-time updates
updateChan := make(chan *OrderUpdate, 100)
go m.listenForUpdates(updateChan)

// Other modules send frequent updates
for _, update := range updates {
    updateChan <- update  // Very fast
}
```

**Example - Request-Reply Service:**

```go
// Module registers service
m.container.RegisterRequestReplyService("get-status", func(ctx context.Context, req *StatusRequest) (*StatusResponse, error) {
    return &StatusResponse{Status: "ready"}, nil
})

// Caller invokes service
response, err := m.container.RequestReplyService(ctx, "module-name", "get-status", &StatusRequest{})
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

**Learn more:** [Testing - How-To Guide](../guides/testing.md)

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
// - Message persistence across restarts
// - Stream replay
```

**Enable JetStream only if you need persistence:**

```go
app, err := mono.NewMonoApplication(
    mono.WithJetStreamEnabled(true),
    mono.WithJetStreamStorageDir("/tmp/jetstream"),
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
    logger types.Logger
    db     *Database  // Dependency to check
}

// Implement health check
func (m *OrderModule) Health(ctx context.Context) types.HealthStatus {
    // Check if database is responsive
    if err := m.db.Ping(ctx); err != nil {
        m.logger.Warn("Health check failed", "error", err)
        return types.HealthStatusUnhealthy
    }

    return types.HealthStatusHealthy
}

// Framework aggregates health from all modules
app, _ := mono.NewMonoApplication()
app.Register(&OrderModule{})

// Get overall health
health := app.Health()  // Returns Healthy only if all modules are healthy
```

**Key points:**
- Return `HealthStatusHealthy` or `HealthStatusUnhealthy`
- Keep health checks fast (no expensive operations)
- Check critical dependencies only
- Framework aggregates health from all modules

---

## Project Structure

### Q: What's the recommended project structure?

**A:** Organize your project like this:

```
my-app/
├── main.go                          # Entry point
├── go.mod
├── go.sum
├── README.md
│
├── internal/                        # Private application code
│   ├── ordermodule/
│   │   ├── order.go                 # Module implementation
│   │   ├── handler.go               # Service handlers
│   │   └── handler_test.go          # Unit tests
│   │
│   └── paymentmodule/
│       ├── payment.go
│       ├── handler.go
│       └── handler_test.go
│
├── pkg/                             # Public packages (if any)
│   └── orderapi/                    # APIs exposed to external packages
│       └── types.go
│
├── test/                            # Integration tests
│   ├── integration/
│   │   └── order_service_test.go    # Integration tests
│   └── testdata/                    # Test fixtures
│
├── config/                          # Configuration
│   └── config.go
│
└── docs/                            # Documentation
    ├── README.md
    └── architecture.md
```

**Key principles:**
- Group related functionality in modules
- Keep modules independent and focused
- Put integration tests in `test/integration/`
- Use internal packages for private code
- Document architecture and design decisions

**Example module structure:**

```go
// internal/ordermodule/order.go
package ordermodule

type OrderModule struct {
    logger types.Logger
    // ... other fields
}

func (m *OrderModule) Name() string { return "order" }
func (m *OrderModule) Start(ctx context.Context) error { ... }
func (m *OrderModule) Stop(ctx context.Context) error { ... }
```

**Learn more:** [Getting Started - Project Structure](../getting-started/project-structure.md)

---

## Troubleshooting

### Q: My service handler isn't being called. What's wrong?

**A:** Check these common issues:

1. **Module not registered:**
```go
app.Register(&MyModule{})  // Don't forget this!
```

2. **Service not registered in Start():**
```go
func (m *MyModule) Start(ctx context.Context) error {
    m.container.RegisterRequestReplyService("my-service", m.handleRequest)
    return nil
}
```

3. **Service name mismatch:**
```go
// Registration uses: "services.module-name.service-name"
m.container.RegisterRequestReplyService("get-order", ...)

// Call must use same module and service name
m.container.RequestReplyService(ctx, "module-name", "get-order", ...)
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
func (m *MyModule) RegisterEventConsumers(registry types.EventRegistry) {
    registry.RegisterEventConsumer(&OrderCreatedEvent{}, m.handleOrderCreated)
}
```

3. **Event subject doesn't match:**
```go
// Events use subject: "events.domain.event-type"
// Make sure publisher and consumer use same event types
m.eventBus.Publish(ctx, &OrderCreatedEvent{})
```

---

## Additional Resources

- [How-To Guides](../guides/README.md) - Step-by-step instructions for common tasks
- [Core Concepts](../core-concepts/README.md) - Foundational concepts and architecture
- [API Reference](../api/README.md) - Complete API documentation
- [Examples](../../../examples/) - Runnable example applications

---

Still have questions? Check the [How-To Guides](../guides/README.md) or [Core Concepts](../core-concepts/README.md) for more detailed information.
