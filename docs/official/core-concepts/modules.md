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
   ├─→ Start plugins, then middleware modules
   │
   ├─→ Resolve dependencies (topological sort)
   │
   ├─→ Collect EmitEvents() from every emitter module
   │
   ├─→ For each module in dependency order:
   │   ├─ Create and bind ServiceContainer
   │   ├─ Call SetPlugin()
   │   ├─ Call SetDependencyServiceContainer()
   │   ├─ Validate ConsumeServices()
   │   ├─ Call SetEventBus()
   │   ├─ Call RegisterServices()
   │   ├─ Call RegisterEventConsumers()
   │   └─ Call Start()
   │
   ├─→ Set up NATS subscriptions for all modules
   │
   └─ Application Ready


2. Running
   │
   └─ Modules process events and requests


3. Application Stop
   │
   ├─→ Drain all subscriptions and wait for in-flight handlers
   │   (module by module, in REVERSE dependency order)
   │
   ├─→ Cancel the runtime context
   │
   ├─→ Call Stop() on each module in REVERSE dependency order,
   │   then on middleware modules, then on plugins
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
        // Look services up later, e.g. container.GetRequestReplyService("process-payment")
        m.paymentContainer = container
    }
}
```

**When to use**: When you need to call services provided by other modules.

{% hint style="info" %}
**Since v0.1.0** this container is a read-only view of the dependency's container. Look services up on it; do not register on it. Code that only calls services can hold it as a `mono.ServiceLookup` (v0.1.2+). The bus from `SetEventBus` is scoped to the module in the same way. See the [Module Boundaries Guide](../guides/module-boundaries.md).
{% endhint %}

### ServiceConsumerModule

Declare the service contracts your module calls (v0.1.0+):

```go
import "github.com/go-monolith/mono/pkg/types"

func (m *OrderModule) ConsumeServices() []types.BaseServiceDefinition {
    return []types.BaseServiceDefinition{
        inventorycontract.CheckStock.ToBase(),
        paymentcontract.Process.ToBase(),
    }
}
```

The framework validates the list when the module starts. A contract that no declared dependency provides fails startup instead of the first call. List every contract the module calls: since v0.1.2 a call to a dependency service that is not in the list is reported as a boundary violation, whether it goes through the dependency's container or over the event bus.

**When to use**: When your module calls other modules through typed service contracts.

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
func (m *MyModule) Health(ctx context.Context) mono.HealthStatus {
    return mono.HealthStatus{
        Healthy: m.isHealthy(),
        Message: "running",
    }
}
```

**When to use**: When your module needs to report custom health conditions (e.g., database connectivity).

### PluginModule

A special module that starts first and stops last:

```go
type MyPlugin struct {
    container mono.ServiceContainer
}

func (p *MyPlugin) Name() string { return "my-plugin" }
func (p *MyPlugin) Start(context.Context) error { return nil }
func (p *MyPlugin) Stop(context.Context) error  { return nil }

// PluginModule: the framework hands the plugin its own container
func (p *MyPlugin) SetContainer(c mono.ServiceContainer) { p.container = c }
func (p *MyPlugin) Container() mono.ServiceContainer     { return p.container }
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
    "encoding/json"
    "log/slog"

    "github.com/go-monolith/mono"
    paymentcontract "my-app/modules/payment/contract" // only the provider's contract package
)

// Order module with dependencies, services, and events
type OrderModule struct {
    eventBus mono.EventBus
    payment  mono.ServiceLookup // payment's container, used through its contract
    storage  StoragePlugin
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
        m.payment = container
    }
}

// Calls payment through its typed contract
func (m *OrderModule) charge(ctx context.Context, amount float64) (paymentcontract.PaymentResponse, error) {
    return paymentcontract.Process.Call(ctx, m.payment, paymentcontract.PaymentRequest{Amount: amount})
}

// Optional: ServiceProviderModule (provide order service)
func (m *OrderModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterRequestReplyService("create-order",
        m.handleCreateOrder)
}

func (m *OrderModule) handleCreateOrder(ctx context.Context, req *mono.Msg) ([]byte, error) {
    var createReq CreateOrderRequest
    if err := json.Unmarshal(req.Data, &createReq); err != nil {
        return nil, err
    }
    order := &Order{ID: "ORD-001"}
    slog.Info("Creating order", "id", order.ID)
    return json.Marshal(order)
}

// Optional: EventEmitterModule (emit OrderCreated events)
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
    }
}

// Optional: HealthCheckableModule
func (m *OrderModule) Health(ctx context.Context) mono.HealthStatus {
    return mono.HealthStatus{
        Healthy: true,
        Message: "ready",
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
- Declare consumed services with `ConsumeServices()` and add a `monotest.CheckModule` test
- Log important events for debugging
- Handle errors explicitly in Start/Stop

### ✗ Don't

- Store global state in module variables (use the framework's DI)
- Call other modules directly (use services or events)
- Register on a dependency's container, or request another module's `services.*` subject on the event bus
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

func (m *MyModule) handleGetUser(ctx context.Context, req *mono.Msg) ([]byte, error) {
    var getReq GetUserRequest
    if err := json.Unmarshal(req.Data, &getReq); err != nil {
        return nil, err
    }
    // Fetch from database
    user, err := m.db.GetUser(ctx, getReq.UserID)
    if err != nil {
        return nil, err
    }
    return json.Marshal(user)
}
```

### Module with Event Handling

```go
func (m *MyModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Typed registration: the event is unmarshaled for the handler
    return helper.RegisterTypedEventConsumer(registry, OrderCreatedV1, m.handleOrderCreated, m)
}

func (m *MyModule) handleOrderCreated(ctx context.Context,
    event OrderCreatedEvent,
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
- Explore [Module Examples](../../../examples/multi-module/README.md)
- Review the [Event Emitter Example](../../../examples/event-emitter/README.md)

---

Ready to learn about inter-module communication? Continue to [Inter-Module Communication](inter-module-communication.md).
