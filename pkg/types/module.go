package types

import (
	"context"
)

// Module is the interface that all modules must implement.
//
// Modules are the fundamental building blocks of the framework, providing
// isolated functionality with lifecycle management. Each module has a unique
// name and implements Start/Stop lifecycle methods.
//
// The framework manages module lifecycle in dependency order:
//   - Start: called in dependency order (dependencies start first)
//   - Stop: called in reverse dependency order (dependencies stop last)
//
// Module implementations must be safe for concurrent access to their exported methods.
//
// See docs/spec/foundation.md for detailed design documentation.
type Module interface {
	// Name returns a unique identifier for the module.
	// The name must be unique across all registered modules and should use kebab-case
	// (e.g., "user-auth", "order-processing").
	// This name is used for dependency resolution and service registration.
	Name() string

	// Start initializes the module and prepares it to receive requests.
	// The context can be used to respect cancellation during initialization.
	// Modules should:
	//   - Initialize resources (connections, caches, workers)
	//   - Set up internal state
	//   - Start background goroutines if needed
	//
	// Start is called after all dependencies have been started and after
	// RegisterServices (if ServiceProviderModule) has been called.
	//
	// If Start returns an error, the framework will:
	//   - Stop all previously started modules in reverse order
	//   - Not call this module's Stop method
	//   - Return the error to the caller
	//
	// Modules start receiving messages only after Start() completes successfully.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the module.
	// The context can be used to enforce shutdown timeouts.
	// Modules should:
	//   - Stop accepting new requests
	//   - Complete in-flight requests (respecting context deadline)
	//   - Release resources (connections, files, workers)
	//   - Stop background goroutines
	//
	// Stop is called in reverse dependency order, meaning this module's
	// dependencies are still running when Stop is called.
	//
	// Modules stop receiving messages only after Stop() finishes.
	// Stop should be idempotent and safe to call multiple times.
	Stop(ctx context.Context) error
}

// DependentModule declares dependencies on other modules and receives their service containers.
//
// Modules implementing this interface will be started after their dependencies
// and stopped before their dependencies. The framework uses this information
// for topological sorting to determine the correct startup order.
//
// Dependencies are specified by module name and must match the Name() of other
// registered modules. Circular dependencies are detected and will cause registration to fail.
//
// The framework calls SetDependencyServiceContainer for each dependency before calling
// RegisterServices or Start, allowing modules to access services provided by their dependencies.
//
// Example:
//
//	type OrderModule struct {
//	    inventory ServiceContainer  // Set via SetDependencyServiceContainer
//	    payment   ServiceContainer  // Set via SetDependencyServiceContainer
//	}
//
//	func (m *OrderModule) Name() string { return "order" }
//
//	func (m *OrderModule) Dependencies() []string {
//	    return []string{"inventory", "payment"}
//	}
//
//	func (m *OrderModule) SetDependencyServiceContainer(dep string, container ServiceContainer) {
//	    switch dep {
//	    case "inventory":
//	        m.inventory = container
//	    case "payment":
//	        m.payment = container
//	    }
//	}
type DependentModule interface {
	Module
	// Dependencies returns names of modules this module depends on.
	// The returned slice must contain valid module names that will be registered.
	// Dependencies are resolved during framework startup, and missing or circular
	// dependencies will cause startup to fail.
	//
	// Dependencies are started before this module and stopped after this module.
	Dependencies() []string

	// SetDependencyServiceContainer provides access to the service container of a dependency.
	// Called once for each dependency during module initialization, before RegisterServices.
	//
	// Parameters:
	//   - dependency: The name of the dependency module (matches a value from Dependencies())
	//   - container: The ServiceContainer of the dependency module
	//
	// Modules should save the container to an appropriately named field for later use
	// during Start() or when handling requests.
	SetDependencyServiceContainer(dependency string, container ServiceContainer)
}

// ServiceProviderModule allows modules to register services they provide.
//
// Modules implementing this interface can register services that other modules
// can access. Services are registered by name and type (Channel, RequestReply, QueueGroup).
//
// RegisterServices is called during framework initialization:
//  1. After SetDependencyServiceContainer (if DependentModule)
//  2. Before Start()
//  3. For all modules before any module's Start() is called
//
// This ensures all services are registered before modules start handling requests.
//
// Example:
//
//	func (m *InventoryModule) RegisterServices(container ServiceContainer) error {
//	    // Bind container to this module
//	    if err := container.BindModule(m); err != nil {
//	        return err
//	    }
//
//	    // Register a request-reply service
//	    handler := func(ctx context.Context, msg *Msg) (*Msg, error) {
//	        // Handle inventory check request
//	        return &Msg{Data: []byte("in-stock")}, nil
//	    }
//	    return container.RegisterRequestReplyService("check-stock", handler)
//	}
type ServiceProviderModule interface {
	Module
	// RegisterServices is called after SetDependencyServiceContainer but before Start().
	// Modules should register all services they provide using the given container.
	//
	// The container must be bound to this module using container.BindModule(m) before
	// registering services. Services registered here will be available to other modules
	// that have a dependency on this module.
	//
	// If RegisterServices returns an error, framework initialization will fail and
	// no modules will be started.
	RegisterServices(container ServiceContainer) error
}

// EventBusAwareModule receives an EventBus instance during framework initialization
//
// Modules implementing this interface gain access to the EventBus for low-level
// pub/sub messaging, queue subscriptions, and JetStream operations. This is useful
// for modules that need to:
//   - Publish domain events to other modules
//   - Subscribe to events from other modules
//   - Use queue groups for load balancing
//   - Use JetStream for durable messaging
//
// SetEventBus is called during framework initialization, after NATS server is
// started but before RegisterServices.
//
// Example:
//
//	type NotificationModule struct {
//	    eventBus EventBus
//	}
//
//	func (m *NotificationModule) SetEventBus(bus EventBus) {
//	    m.eventBus = bus
//	}
//
//	func (m *NotificationModule) Start(ctx context.Context) error {
//	    // Subscribe to order events
//	    _, err := m.eventBus.Subscribe("events.order.created", func(msg *Msg) {
//	        // Send notification
//	    })
//	    return err
//	}
type EventBusAwareModule interface {
	Module
	// SetEventBus provides the event bus for low-level pub/sub, queue, jetstream operations.
	// Called during framework initialization before RegisterServices and Start.
	//
	// Modules should store the EventBus in a field for later use. The EventBus can be
	// used to:
	//   - Publish events: bus.Publish(subject, data)
	//   - Subscribe to events: bus.Subscribe(subject, handler)
	//   - Use queue groups: bus.QueueSubscribe(subject, queue, handler)
	//   - Make request-reply calls: bus.Request(subject, data, timeout)
	SetEventBus(bus EventBus)
}

// HealthCheckableModule provides health status.
//
// Modules implementing this interface can report their health status to the framework.
// The framework aggregates module health and exposes it through the framework's Health() method.
//
// Health checks should be fast (< 100ms) and should check critical dependencies:
//   - Database connection status
//   - External service connectivity
//   - Resource utilization (memory, disk)
//   - Internal state validity
//
// Example:
//
//	func (m *DatabaseModule) Health(ctx context.Context) HealthStatus {
//	    if err := m.db.PingContext(ctx); err != nil {
//	        return HealthStatus{
//	            Healthy: false,
//	            Message: "database connection failed",
//	            Details: map[string]any{"error": err.Error()},
//	        }
//	    }
//	    return HealthStatus{Healthy: true, Message: "operational"}
//	}
type HealthCheckableModule interface {
	Module
	// Health returns the current health status.
	// Implementations should:
	//   - Complete quickly (< 100ms) or respect the context deadline
	//   - Check critical dependencies and resources
	//   - Return detailed information in the Details map for debugging
	//   - Handle panics internally and return unhealthy status
	//
	// The framework may call Health periodically or on-demand (e.g., health check endpoints).
	Health(ctx context.Context) HealthStatus
}

// HealthStatus represents module health.
// Used by HealthAwareModule to report operational status.
type HealthStatus struct {
	// Healthy indicates if the module is operating normally
	Healthy bool `json:"healthy"`

	// Message provides a human-readable status description
	// Examples: "operational", "database connection lost", "degraded performance"
	Message string `json:"message,omitempty"`

	// Details provides additional debugging information
	// Can include metrics, error details, dependency status, etc.
	Details map[string]any `json:"details,omitempty"`
}

// EventEmitterModule registers event definitions with the framework.
//
// Modules implementing this interface can declare events they will emit during operation.
// The framework collects these event definitions during initialization and makes them
// available for discovery by consumer modules via the EventRegistry.
// This module require EventBusAwareModule interface in order to publish events to EventBus
//
// EmitEvents is called during framework initialization:
//   - After SetEventBus (require EventBusAwareModule)
//   - Before module Start()
//   - Before consumer modules can register event handlers
//
// This ensures all event definitions are available when consumer modules start.
//
// Example:
//
//	type OrderModule struct {
//	    eventBus EventBus
//	}
//
//	// OrderCreatedV1 is emitted when a new order is created.
//	var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
//	    "order", "OrderCreated", "v1",
//	)
//
//	func (m *OrderModule) EmitEvents() []BaseEventDefinition {
//	    return []BaseEventDefinition{
//	        OrderCreatedV1.ToBase(),
//	    }
//	}
type EventEmitterModule interface {
	EventBusAwareModule

	// EmitEvents returns all base event definitions this module can emit.
	// Called during framework initialization (after SetEventBus, before Start).
	// The method name clearly indicates the module's intent: declaring what events it emits.
	//
	// Use eventDef.ToBase() to convert generic EventDefinition[T] to BaseEventDefinition.
	EmitEvents() []BaseEventDefinition
}

// EventConsumerModule registers event consumers during framework initialization.
//
// Modules implementing this interface can register handlers for events emitted by
// other modules. This follows the same pattern as ServiceProviderModule - the framework
// calls the method directly for registration rather than providing a setter.
//
// RegisterEventConsumers is called during framework initialization:
//   - After SetDependencyServiceContainer (if DependentModule)
//   - After SetEventBus (if EventBusAwareModule)
//   - After RegisterServices (if ServiceProviderModule)
//   - After all EventEmitterModules have registered their events
//   - Before Start()
//
// This ensures:
//   - All services are registered before event consumers are set up
//   - All event definitions are available for discovery
//   - Consumer registration happens in a dedicated phase
//
// Example:
//
//	type NotificationModule struct {
//	    // No need to store eventRegistry as a field
//	}
//
//	func (m *NotificationModule) RegisterEventConsumers(registry EventRegistry) error {
//	    // Discover the event
//	    eventDef, ok := registry.GetEventByName("OrderCreated", "v1", "order")
//	    if !ok {
//	        return fmt.Errorf("event not found: OrderCreated.v1 from order")
//	    }
//
//	    // Register consumer handler
//	    return registry.RegisterEventConsumer(eventDef, m.handleOrderCreated, m)
//	}
type EventConsumerModule interface {
	Module

	// RegisterEventConsumers registers event consumers for this module.
	// Called after RegisterServices but before Start.
	//
	// The registry provides event discovery and consumer registration:
	//   - GetEventByName(name, version, moduleName) - find specific event
	//   - GetEventsByModule(moduleName) - list all events from a module
	//   - GetAllEvents() - list all registered events
	//   - RegisterEventConsumer(eventDef, handler, module) - register handler
	//
	// If RegisterEventConsumers returns an error, framework initialization will fail
	// and no modules will be started.
	RegisterEventConsumers(registry EventRegistry) error
}
