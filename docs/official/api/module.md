# Module API

Reference for the module interfaces. Every module implements `Module`; the optional interfaces below let it depend on other modules, provide or call services, publish or consume events, report health and use plugins. Read this page when you write a module and need the exact method signatures and when the framework calls them. For the concepts, see [Core Concepts - Modules](../core-concepts/modules.md).

## Signatures

```go
// Required interface - all modules must implement
type Module interface {
    Name() string
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
}

// Optional interfaces for extended capabilities
type EventBusAwareModule interface { SetEventBus(EventBus) }
type DependentModule interface { Dependencies() []string; SetDependencyServiceContainer(string, ServiceContainer) }
type ServiceProviderModule interface { RegisterServices(ServiceContainer) error }
type ServiceConsumerModule interface { ConsumeServices() []BaseServiceDefinition }  // pkg/types
type HealthCheckableModule interface { Health(ctx context.Context) HealthStatus }
type EventEmitterModule interface { EmitEvents() []BaseEventDefinition }
type EventConsumerModule interface { RegisterEventConsumers(EventRegistry) error }
type UsePluginModule interface { SetPlugin(alias string, plugin PluginModule) }

// Special kinds of module
type PluginModule interface { SetContainer(ServiceContainer); Container() ServiceContainer }
type MiddlewareModule interface { /* six On... hooks, see below */ }
```

{% hint style="info" %}
**Interface Composition:** Implement only the interfaces your module needs. The framework detects which interfaces each module implements and calls the appropriate lifecycle methods automatically. The order in which it calls them is in [Module Lifecycle](#module-lifecycle).
{% endhint %}

## Module Interface (Required)

Every module must implement these three methods:

```go
type MyModule struct {
    logger mono.Logger
}

func (m *MyModule) Name() string {
    return "mymodule"  // Unique identifier
}

func (m *MyModule) Start(ctx context.Context) error {
    // Initialize resources, connect to databases, etc.
    return nil
}

func (m *MyModule) Stop(ctx context.Context) error {
    // Clean up resources, close connections, etc.
    return nil
}
```

## Optional Module Interfaces

Modules can implement any combination of these interfaces to declare capabilities.

### EventBusAwareModule

```go
type EventBusAwareModule interface {
    Module

    // SetEventBus receives the framework's event bus instance.
    SetEventBus(eventBus EventBus)
}
```

**When to implement:** When your module needs to publish events or subscribe to events.

**Example:**
```go
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent]("order", "OrderCreated", "v1")

type OrderModule struct {
    eventBus mono.EventBus
}

func (m *OrderModule) SetEventBus(eventBus mono.EventBus) {
    m.eventBus = eventBus
}

func (m *OrderModule) publishOrderCreated(ctx context.Context, orderID string) error {
    return OrderCreatedV1.Publish(m.eventBus, OrderCreatedEvent{OrderID: orderID}, nil)
}
```

### DependentModule

```go
type DependentModule interface {
    Module

    // Dependencies returns the list of module names this module depends on.
    // The framework will ensure these modules start before this one.
    Dependencies() []string

    // SetDependencyServiceContainer provides access to each dependency's ServiceContainer.
    // Called once per dependency during initialization, before RegisterServices.
    SetDependencyServiceContainer(dependency string, container ServiceContainer)
}
```

**When to implement:** When your module requires other modules to be initialized first and needs access to their services.

**Framework guarantees:**
- Dependencies are started before this module
- `SetDependencyServiceContainer` is called for each dependency before `RegisterServices` and `Start`
- Circular dependencies are detected and rejected when the application starts

{% hint style="warning" %}
**Circular dependencies are rejected.** The framework validates the dependency graph during `Start` and returns an error if cycles are detected.
{% endhint %}

**Example:**
```go
type OrderModule struct {
    paymentContainer   mono.ServiceContainer
    inventoryContainer mono.ServiceContainer
    paymentClient      mono.RequestReplyServiceClient
}

func (m *OrderModule) Dependencies() []string {
    return []string{"payment", "inventory"}
}

func (m *OrderModule) SetDependencyServiceContainer(dep string, container mono.ServiceContainer) {
    switch dep {
    case "payment":
        m.paymentContainer = container
    case "inventory":
        m.inventoryContainer = container
    }
}

func (m *OrderModule) Start(ctx context.Context) error {
    // Get service clients from dependency containers
    client, err := m.paymentContainer.GetRequestReplyService("process")
    if err != nil {
        return fmt.Errorf("payment service unavailable: %w", err)
    }
    m.paymentClient = client

    return nil
}
```

### ServiceProviderModule

```go
type ServiceProviderModule interface {
    Module

    // RegisterServices registers this module's services in the container.
    // Called before Start(), allowing other modules to discover these services.
    RegisterServices(container ServiceContainer) error
}
```

**When to implement:** When your module provides services for other modules to call. The registration methods are in the [Service Container API](container.md).

**Example:**
```go
type PaymentModule struct {
    processor *PaymentProcessor
}

func (m *PaymentModule) RegisterServices(container mono.ServiceContainer) error {
    if err := container.RegisterRequestReplyService("process", m.handleProcess); err != nil {
        return err
    }
    if err := container.RegisterQueueGroupService("audit",
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
    resp, err := m.processor.Process(ctx, &payment)
    if err != nil {
        return nil, err
    }
    return json.Marshal(resp)
}

func (m *PaymentModule) handleAudit(ctx context.Context, msg *mono.Msg) error {
    // Audit logging
    return nil
}
```

### ServiceConsumerModule

```go
// pkg/types (no root alias yet)
type ServiceConsumerModule interface {
    // ConsumeServices returns the contracts of the services this module calls.
    ConsumeServices() []types.BaseServiceDefinition
}
```

**When to implement:** When your module calls services of its dependencies through typed contracts (`helper.RequestReplyService`, `helper.QueueGroupService`, `helper.StreamConsumerService`). Return the `ToBase()` of each contract you call:

```go
func (m *OrderModule) ConsumeServices() []types.BaseServiceDefinition {
    return []types.BaseServiceDefinition{
        inventorycontract.CheckStock.ToBase(),
        paymentcontract.Process.ToBase(),
    }
}
```

**When it is called:** once when the module starts, after its dependency containers are set, so the list may be built from what the module was wired with.

**What is validated:**
- each contract must name a module in `Dependencies()` (or a plugin);
- that module must have registered the service;
- the registered service type must match.

A mismatch fails the start (`ErrModuleStartFailed` wrapping `ErrMissingDependency`, `ErrServiceNotFound` or `ErrServiceContractMismatch`) instead of surfacing on the first call.

**What counts as a violation (since v0.1.2 the list is complete):**
- A lookup of a request-reply, queue group or stream consumer service of a dependency that is not in the list, or a publish or request to that service's subject on the module's event bus, is an `undeclared-service-use` boundary violation (recorded by default, rejected under `WithStrictModuleBoundaries`).
- By default a lookup of a service the dependency does not provide fails with `ErrServiceNotFound` and is not a violation.

The list is also part of the module's `ModuleBoundaryReport`, with the contracts not used so far. See the [Module Boundaries Guide](../guides/module-boundaries.md).

### HealthCheckableModule

```go
type HealthCheckableModule interface {
    Module

    // Health returns the health status of this module.
    // Return Healthy=true if everything is working normally.
    Health(ctx context.Context) HealthStatus
}
```

**When to implement:** When your module has internal state that could become unhealthy (database connections, external APIs, etc.).

**HealthStatus Structure:**
```go
type HealthStatus struct {
    Healthy bool           `json:"healthy"`
    Message string         `json:"message,omitempty"`
    Details map[string]any `json:"details,omitempty"`
}
```

**Example:**
```go
type DatabaseModule struct {
    db *sql.DB
}

func (m *DatabaseModule) Health(ctx context.Context) mono.HealthStatus {
    // Test database connectivity
    if err := m.db.PingContext(ctx); err != nil {
        return mono.HealthStatus{
            Healthy: false,
            Message: fmt.Sprintf("Database ping failed: %v", err),
            Details: map[string]any{"error": err.Error()},
        }
    }

    // Query pool stats
    stats := m.db.Stats()
    return mono.HealthStatus{
        Healthy: true,
        Message: fmt.Sprintf("Database healthy (open: %d, in use: %d)",
            stats.OpenConnections, stats.InUse),
        Details: map[string]any{
            "open_connections": stats.OpenConnections,
            "in_use":           stats.InUse,
        },
    }
}
```

### EventEmitterModule

```go
type EventEmitterModule interface {
    EventBusAwareModule

    // EmitEvents returns the list of event definitions this module emits.
    EmitEvents() []BaseEventDefinition
}
```

**When to implement:** When your module publishes domain events that other modules may subscribe to.

**Example:**
```go
type OrderCreatedEvent struct {
    OrderID string    `json:"order_id"`
    Total   float64   `json:"total"`
    Time    time.Time `json:"time"`
}

// Subject: events.order.v1.order-created
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent]("order", "OrderCreated", "v1")

type OrderModule struct {
    eventBus mono.EventBus
}

func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
    }
}

func (m *OrderModule) publishOrderCreated(ctx context.Context, orderID string, total float64) error {
    return OrderCreatedV1.Publish(m.eventBus, OrderCreatedEvent{
        OrderID: orderID,
        Total:   total,
        Time:    time.Now(),
    }, nil)
}
```

### EventConsumerModule

```go
type EventConsumerModule interface {
    Module

    // RegisterEventConsumers registers event consumers for this module.
    // Called after RegisterServices but before Start.
    RegisterEventConsumers(registry EventRegistry) error
}
```

**When to implement:** When your module subscribes to events published by other modules.

**Note:** This interface extends only `Module`, not `EventBusAwareModule`. The `EventRegistry` provides event discovery and registration without requiring direct EventBus access.

**Example:**
```go
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    // Discover the event definition
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
    // Send notification...
    return nil
}
```

**EventRegistry Interface:**
```go
type EventRegistry interface {
    // Event discovery
    GetEventByName(name, version, moduleName string) (BaseEventDefinition, bool)
    GetEventsByModule(moduleName string) []BaseEventDefinition
    GetAllEvents() []BaseEventDefinition

    // Consumer registration
    RegisterEventConsumer(eventDef BaseEventDefinition, handler EventConsumerHandler, module Module, queueGroup ...string) error
    RegisterEventConsumerWithOptions(eventDef BaseEventDefinition, handler EventConsumerHandler, module Module, opts EventConsumerOptions) error
    RegisterEventStreamConsumer(eventDef BaseEventDefinition, config StreamConsumerConfig, handler EventStreamConsumerHandler, module Module) error

    // ... plus RegisterEvent, Entries, StreamConsumerEntries and SetMiddlewareChain
}
```

See the [EventRegistry API](eventregistry.md) for the full interface.

### UsePluginModule

```go
type UsePluginModule interface {
    Module

    // SetPlugin receives each registered plugin instance.
    // The alias parameter identifies which plugin is being injected.
    SetPlugin(alias string, plugin PluginModule)
}
```

**When to implement:** When your module needs to use services from plugins.

**Example:**
```go
type DocumentModule struct {
    storage *fsjetstream.PluginModule
}

func (m *DocumentModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "storage" {
        m.storage = plugin.(*fsjetstream.PluginModule)
    }
}

func (m *DocumentModule) Start(ctx context.Context) error {
    if m.storage == nil {
        return fmt.Errorf("required plugin 'storage' not registered")
    }

    bucket := m.storage.Bucket("documents")
    if bucket == nil {
        return fmt.Errorf("bucket 'documents' not found")
    }

    return nil
}
```

## Special Module Kinds

### PluginModule

```go
type PluginModule interface {
    Module

    // SetContainer receives the plugin's dedicated ServiceContainer.
    SetContainer(container ServiceContainer)

    // Container returns the plugin's ServiceContainer.
    Container() ServiceContainer
}
```

**When to implement:** When creating infrastructure plugins (databases, caches, etc.).

**Related:** See [Plugins Documentation](../plugins/README.md) and [Creating Plugins](../plugins/creating-plugins.md).

### MiddlewareModule

```go
type MiddlewareModule interface {
    Module

    // Each hook returns the (possibly modified) value for the next middleware.
    OnModuleLifecycle(ctx context.Context, event ModuleLifecycleEvent) ModuleLifecycleEvent
    OnServiceRegistration(ctx context.Context, reg ServiceRegistration) ServiceRegistration
    OnConfigurationChange(ctx context.Context, event ConfigurationEvent) ConfigurationEvent
    OnOutgoingMessage(octx OutgoingMessageContext) OutgoingMessageContext
    OnEventConsumerRegistration(ctx context.Context, entry EventConsumerEntry) EventConsumerEntry
    OnEventStreamConsumerRegistration(ctx context.Context, entry EventStreamConsumerEntry) EventStreamConsumerEntry
}
```

**When to implement:** When you're creating a middleware component (advanced users).

**Related:** See [Middleware Documentation](../middleware/README.md) for pre-built middleware modules.

## Module Lifecycle

### Overall Startup Order

1. Plugins (in registration order)
2. Middleware (in registration order)
3. Regular modules (in dependency order)

### Startup Order (per module)

Before any regular module starts, the framework collects `EmitEvents()` from every module implementing EventEmitterModule. Then, for each module in dependency order:

1. ServiceContainer created and bound to the module
2. `SetPlugin()` - For modules implementing UsePluginModule
3. `SetDependencyServiceContainer()` - Provide dependency containers
4. `ConsumeServices()` - Validated, for modules implementing ServiceConsumerModule
5. `SetEventBus()` - Provide event bus
6. `RegisterServices()` - Register this module's services
7. `RegisterEventConsumers()` - Register event handlers
8. `Start()` - Initialize module

After all modules have started, channel service routers are started and the NATS subscriptions are created. `SetContainer()` is called on plugin modules when the plugins start. Middleware modules only get `Start()`.

### Shutdown Order

1. Regular modules (in reverse dependency order)
2. Middleware (in reverse registration order)
3. Plugins (in reverse registration order)

## Best Practices

### Module Design

✓ **Do**
- Keep modules focused on a single responsibility
- Implement only the interfaces you actually need
- Declare all dependencies in `Dependencies()`
- Get logger via `app.Logger()` from the framework
- Handle shutdown gracefully in `Stop()`

✗ **Don't**
- Create circular dependencies (framework detects and rejects)
- Call other modules' methods directly (use services/events)
- Assume SetXxx methods are always called (check for nil)
- Block indefinitely in `Start()` without context awareness
- Ignore shutdown context deadline

### Error Handling

```go
func (m *MyModule) Start(ctx context.Context) error {
    resource, err := m.initializeResource(ctx)
    if err != nil {
        return fmt.Errorf("failed to initialize resource: %w", err)
    }
    m.resource = resource
    return nil
}
```

### Graceful Shutdown

```go
func (m *MyModule) Stop(ctx context.Context) error {
    if m.resource == nil {
        return nil
    }

    // Use context deadline for timeout
    return m.resource.Close(ctx)
}
```

## Examples

### Complete Module Example

```go
// Subject: events.order.v1.order-created
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent]("order", "OrderCreated", "v1")

type OrderModule struct {
    eventBus          mono.EventBus
    paymentContainer  mono.ServiceContainer
    paymentClient     mono.RequestReplyServiceClient
    db                *sql.DB
}

// Module interface
func (m *OrderModule) Name() string { return "order" }

func (m *OrderModule) Start(ctx context.Context) error {
    // Get payment service client
    client, err := m.paymentContainer.GetRequestReplyService("process")
    if err != nil {
        return fmt.Errorf("payment service unavailable: %w", err)
    }
    m.paymentClient = client

    return m.db.PingContext(ctx)
}

func (m *OrderModule) Stop(ctx context.Context) error {
    return m.db.Close()
}

// EventBusAwareModule interface
func (m *OrderModule) SetEventBus(eventBus mono.EventBus) {
    m.eventBus = eventBus
}

// DependentModule interface
func (m *OrderModule) Dependencies() []string {
    return []string{"payment"}
}

func (m *OrderModule) SetDependencyServiceContainer(dep string, container mono.ServiceContainer) {
    if dep == "payment" {
        m.paymentContainer = container
    }
}

// ServiceProviderModule interface
func (m *OrderModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterRequestReplyService("create-order", m.handleCreateOrder)
}

// EventEmitterModule interface
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        OrderCreatedV1.ToBase(),
    }
}

// HealthCheckableModule interface
func (m *OrderModule) Health(ctx context.Context) mono.HealthStatus {
    if err := m.db.PingContext(ctx); err != nil {
        return mono.HealthStatus{Healthy: false, Message: "DB unavailable"}
    }
    return mono.HealthStatus{Healthy: true, Message: "operational"}
}

func (m *OrderModule) handleCreateOrder(ctx context.Context, req *mono.Msg) ([]byte, error) {
    var createReq CreateOrderRequest
    if err := json.Unmarshal(req.Data, &createReq); err != nil {
        return nil, fmt.Errorf("invalid request: %w", err)
    }

    // Create order...
    order := &Order{ID: "ORD-001"}

    // Publish event
    if err := OrderCreatedV1.Publish(m.eventBus, OrderCreatedEvent{OrderID: order.ID}, nil); err != nil {
        return nil, err
    }

    return json.Marshal(&CreateOrderResponse{Order: order})
}
```

## Related Documentation

- [Framework API](framework.md) - Framework creation and lifecycle
- [Service Container API](container.md) - Service management
- [EventBus API](eventbus.md) - Event publishing
- [API Reference](README.md) - All APIs overview
- [Middleware](../middleware/README.md) - Built-in middleware
- [Plugins](../plugins/README.md) - Plugin system

---

For more information, see [Core Concepts - Modules](../core-concepts/modules.md) and [Foundation Specification](../../spec/foundation.md).
