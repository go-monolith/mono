# Modules

Modules are the fundamental unit of organization in the Monolith Framework. A module is an independent component with a well-defined lifecycle and clear responsibilities.

## Module Interface

Every module must implement the `Module` interface:

```go
type Module interface {
    Name() string                              // Unique identifier
    Start(context.Context) error               // Called on startup
    Stop(context.Context) error                // Called on shutdown
}
```

A minimal module:

```go
type MyModule struct{}

func (m *MyModule) Name() string               { return "my-module" }
func (m *MyModule) Start(_ context.Context) error { return nil }
func (m *MyModule) Stop(_ context.Context) error  { return nil }
```

## Module Lifecycle

The framework manages a strict lifecycle for each module:

```
1. Application Start
   │
   ├─→ Resolve dependencies (topological sort)
   │
   ├─→ For each module in dependency order:
   │   ├─ Create ServiceContainer
   │   ├─ Call SetDependencyServiceContainer()
   │   ├─ Call SetEventBus()
   │   ├─ Call RegisterServices()
   │   ├─ Call Start()
   │   └─ Set up NATS subscriptions
   │
   └─ Application Ready


2. Running
   │
   └─ Modules process events and requests


3. Application Stop
   │
   ├─→ For each module in REVERSE dependency order:
   │   ├─ Drain subscriptions
   │   └─ Call Stop()
   │
   └─ Shutdown Complete
```

## Optional Module Interfaces

Modules can implement optional interfaces to receive framework services and participate in various lifecycle phases:

### EventBusAwareModule

Receive the EventBus for publishing messages:

```go
func (m *MyModule) SetEventBus(bus mono.EventBus) {
    m.eventBus = bus
}
```

**When to use**: When you need to publish events or make service calls.

### ServiceProviderModule

Register services your module provides:

```go
func (m *MyModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterRequestReplyService("process-order",
        m.handleProcessOrder)
}
```

**When to use**: When other modules need to call your services.

### DependentModule

Declare dependencies on other modules:

```go
func (m *MyModule) Dependencies() []string {
    return []string{"database", "cache"}
}
```

**When to use**: When your module needs other modules to start first.

### SetDependencyServiceContainer

Access services from other modules:

```go
func (m *MyModule) SetDependencyServiceContainer(
    module string,
    container mono.ServiceContainer) {
    if module == "payment" {
        m.paymentService = container.RequestReplyServiceClient(...)
    }
}
```

**When to use**: When you need to call services provided by other modules.

### EventEmitterModule

Declare events your module emits:

```go
func (m *MyModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
    }
}
```

**When to use**: When your module publishes events for other modules to consume.

### EventConsumerModule

Register event consumers:

```go
func (m *MyModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    return registry.RegisterEventConsumer(
        orderCreatedDef,
        m.handleOrderCreated,
        m)
}
```

**When to use**: When you need to react to events from other modules.

### HealthCheckableModule

Report custom health status:

```go
func (m *MyModule) Health(ctx context.Context) mono.ModuleHealth {
    return mono.ModuleHealth{
        Healthy: m.isHealthy(),
        Status: "Running",
    }
}
```

**When to use**: When your module needs to report custom health conditions (e.g., database connectivity).

### PluginModule

A special module that starts first and stops last:

```go
type MyPlugin struct {}

func (p *MyPlugin) Name() string { return "my-plugin" }
func (p *MyPlugin) Start(context.Context) error { return nil }
func (p *MyPlugin) Stop(context.Context) error  { return nil }
```

Register with `app.RegisterPlugin(plugin, "alias")`.

**When to use**: For cross-cutting concerns like storage, caching, or authentication that other modules depend on.

### UsePluginModule

Access plugin instances:

```go
func (m *MyModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "storage" {
        m.storage = plugin.(*StoragePlugin)
    }
}
```

**When to use**: When you need to use functionality from a plugin.

## Complete Module Example

Here's a complete module implementing multiple optional interfaces:

```go
package orderservice

import (
    "context"
    "log/slog"

    "github.com/go-monolith/mono"
    "my-app/modules/payment"
)

// Order module with dependencies, services, and events
type OrderModule struct {
    eventBus       mono.EventBus
    paymentService payment.ServiceClient
    storage        StoragePlugin
}

// Required: Module interface
func (m *OrderModule) Name() string { return "order" }

func (m *OrderModule) Start(ctx context.Context) error {
    slog.Info("Starting order module")
    return nil
}

func (m *OrderModule) Stop(ctx context.Context) error {
    slog.Info("Stopping order module")
    return nil
}

// Optional: EventBusAwareModule
func (m *OrderModule) SetEventBus(bus mono.EventBus) {
    m.eventBus = bus
}

// Optional: DependentModule (depends on payment module)
func (m *OrderModule) Dependencies() []string {
    return []string{"payment"}
}

// Optional: SetDependencyServiceContainer (receive payment service)
func (m *OrderModule) SetDependencyServiceContainer(
    module string,
    container mono.ServiceContainer) {
    if module == "payment" {
        m.paymentService = payment.NewServiceClient(container)
    }
}

// Optional: ServiceProviderModule (provide order service)
func (m *OrderModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterRequestReplyService("create-order",
        m.handleCreateOrder)
}

func (m *OrderModule) handleCreateOrder(ctx context.Context,
    req *CreateOrderRequest) (*Order, error) {
    order := &Order{ID: "ORD-001"}
    slog.Info("Creating order", "id", order.ID)
    return order, nil
}

// Optional: EventEmitterModule (emit OrderCreated events)
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
    }
}

// Optional: HealthCheckableModule
func (m *OrderModule) Health(ctx context.Context) mono.ModuleHealth {
    return mono.ModuleHealth{
        Healthy: true,
        Status: "Ready",
    }
}

// Optional: UsePluginModule (receive storage plugin)
func (m *OrderModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "storage" {
        m.storage = plugin.(StoragePlugin)
    }
}
```

## Module Registration

Register modules in your main application:

```go
app, _ := mono.NewMonoApplication()

// Register modules
app.Register(&OrderModule{})
app.Register(&PaymentModule{})
app.Register(&NotificationModule{})

// Start application (framework handles lifecycle)
app.Start(context.Background())
```

The framework automatically:
1. Resolves dependencies
2. Initializes modules in dependency order
3. Sets up logging, event bus, services
4. Handles graceful shutdown

## Best Practices

### ✓ Do

- Keep modules focused on a single domain
- Use dependency injection for cross-module communication
- Implement optional interfaces only if you need them
- Log important events for debugging
- Handle errors explicitly in Start/Stop

### ✗ Don't

- Store global state in module variables (use the framework's DI)
- Call other modules directly (use services or events)
- Block in Start() on I/O (use context with timeout)
- Panic in modules (return errors instead)
- Create tight coupling between modules

## Common Patterns

### Module with Database

```go
func (m *MyModule) Start(ctx context.Context) error {
    db, err := m.connectDatabase(ctx)
    if err != nil {
        return fmt.Errorf("database connect failed: %w", err)
    }
    m.db = db
    return nil
}

func (m *MyModule) Stop(ctx context.Context) error {
    if m.db != nil {
        return m.db.Close()
    }
    return nil
}
```

### Module with Service Provider

```go
func (m *MyModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterRequestReplyService("get-user",
        m.handleGetUser)
}

func (m *MyModule) handleGetUser(ctx context.Context,
    req *GetUserRequest) (*User, error) {
    // Fetch from database
    return m.db.GetUser(ctx, req.UserID)
}
```

### Module with Event Handling

```go
func (m *MyModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    return registry.RegisterEventConsumer(
        orderCreatedDef,
        m.handleOrderCreated,
        m)
}

func (m *MyModule) handleOrderCreated(ctx context.Context,
    event *OrderCreatedEvent,
    msg *mono.Msg) error {
    // Process order
    return nil
}
```

## Testing Modules

Test modules in isolation:

```go
func TestOrderModule(t *testing.T) {
    // Create module
    module := &OrderModule{}

    // Test Start
    if err := module.Start(context.Background()); err != nil {
        t.Fatalf("Start failed: %v", err)
    }

    // Test Stop
    if err := module.Stop(context.Background()); err != nil {
        t.Fatalf("Stop failed: %v", err)
    }
}
```

## Summary

- **Modules** are independent components with clear responsibilities
- **Required interface**: Module (Name, Start, Stop)
- **Optional interfaces**: EventBusAwareModule, ServiceProviderModule, EventConsumerModule, etc.
- **Lifecycle**: Framework manages startup order, dependency injection, and shutdown
- **Communication**: Use services and events, not direct calls

## Next Steps

- Learn about [Inter-Module Communication](inter-module-communication.md) patterns
- Explore [Module Examples](../../../examples/)
- Review the [Event Emitter Example](../../../examples/event-emitter/README.md)

---

Ready to learn about inter-module communication? Continue to [Inter-Module Communication](inter-module-communication.md).
