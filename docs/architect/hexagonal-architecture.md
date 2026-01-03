# Hexagonal Architecture Guide

This guide explains how to apply hexagonal architecture (also known as Ports and Adapters) when developing modules with the Monolith-Framework.

> **Working Example**: See [`/examples/multi-module/`](../../examples/multi-module/) for a complete
> working implementation with inventory, payment, order, and notification modules demonstrating
> these patterns.

## When to Apply This Pattern

Hexagonal architecture is most valuable when:

- Your module has **external dependencies** (other modules, databases, external APIs)
- The module contains **complex business logic** that benefits from isolation
- You want **comprehensive unit tests** without infrastructure setup
- Multiple modules need to **consume your module's services**

For simple utility modules or single-responsibility modules with minimal logic, the full adapter pattern may be overkill. Use your judgment based on complexity.

## Overview

Hexagonal architecture organizes code around the core business logic, isolating it from external concerns like databases, messaging, and external services. In this framework, each module naturally fits this pattern through:

- **Ports**: Interfaces defining how the module interacts with the outside world
- **Adapters**: Concrete implementations that connect ports to infrastructure
- **Domain**: Core business logic independent of external systems

The diagram below shows the flow: incoming NATS requests are handled by driving adapters
(services), which invoke domain logic, which uses driven adapters to communicate with
external modules via NATS.

```
                    ┌─────────────────────────────────────────────────┐
                    │                    Module                        │
                    │                                                  │
   ┌──────────┐     │  ┌──────────────┐      ┌──────────────────┐     │
   │ Incoming │     │  │   Driving    │      │                  │     │
   │ Request  │────▶│  │   Adapter    │─────▶│   Domain Logic   │     │
   │ (NATS)   │     │  │ (Services)   │      │                  │     │
   └──────────┘     │  └──────────────┘      └────────┬─────────┘     │
                    │                                  │               │
                    │                                  ▼               │
                    │                        ┌──────────────────┐     │     ┌──────────┐
                    │                        │   Driven Adapter │     │     │ External │
                    │                        │    (AdapterPort) │─────│────▶│ Module   │
                    │                        └──────────────────┘     │     │ (NATS)   │
                    │                                                  │     └──────────┘
                    └─────────────────────────────────────────────────┘
```

## Module File Structure

Follow this standard file organization for each module:

```
mymodule/
├── types.go      # Port interface + DTOs (request/response types)
├── adapter.go    # Driven adapter implementation (outbound)
└── module.go     # Domain logic + driving adapter (inbound services)
```

### File Responsibilities

| File | Purpose |
|------|---------|
| `types.go` | Defines the **port interface** (`XxxAdapterPort`) and **DTOs** used for communication |
| `adapter.go` | Implements the **driven adapter** that wraps `ServiceContainer` for outbound calls |
| `module.go` | Contains **domain logic** and registers **driving adapters** (services that handle incoming requests) |

## Ports: Defining Module Boundaries

A port is an interface that defines how external systems interact with your module. There are two types:

### Driving Ports (Primary/Inbound)

Services that handle incoming requests to your module. These are registered via `RegisterServices()`.

```go
// In module.go - Driving adapter via RegisterServices
func (m *InventoryModule) RegisterServices(container mono.ServiceContainer) error {
    return helper.RegisterTypedRequestReplyService(
        container,
        "check-stock",
        json.Unmarshal,
        json.Marshal,
        m.checkStock,  // Domain logic handler
    )
}
```

### Driven Ports (Secondary/Outbound)

Interfaces that your domain logic uses to interact with external modules. Define these in `types.go`.

```go
// types.go - Port interface for external consumers
type InventoryAdapterPort interface {
    CheckStock(ctx context.Context, req *CheckStockRequest) (*CheckStockResponse, error)
}
```

## Adapters: Connecting to Infrastructure

### Driven Adapter Implementation

The driven adapter implements the port interface and handles the infrastructure details (NATS messaging, serialization).

```go
// adapter.go
package inventory

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/go-monolith/mono"
    "github.com/go-monolith/mono/pkg/helper"
)

// inventoryAdapter implements InventoryAdapterPort
type inventoryAdapter struct {
    container mono.ServiceContainer
}

// NewInventoryAdapter creates a new adapter for inventory operations.
// This is called by dependent modules in SetDependencyServiceContainer.
func NewInventoryAdapter(container mono.ServiceContainer) InventoryAdapterPort {
    if container == nil {
        panic("inventory adapter requires non-nil ServiceContainer")
    }
    return &inventoryAdapter{container: container}
}

func (a *inventoryAdapter) CheckStock(ctx context.Context, req *CheckStockRequest) (*CheckStockResponse, error) {
    var resp CheckStockResponse
    if err := helper.CallRequestReplyService(
        ctx, a.container, "check-stock",
        json.Marshal, json.Unmarshal, req, &resp,
    ); err != nil {
        return nil, fmt.Errorf("inventory check failed: %w", err)
    }
    return &resp, nil
}
```

### Driving Adapter (Service Registration)

The driving adapter is implemented through service registration in `module.go`. The framework handles the infrastructure (NATS subscriptions) automatically.

```go
// module.go
func (m *InventoryModule) RegisterServices(container mono.ServiceContainer) error {
    // Register a typed request-reply service
    return helper.RegisterTypedRequestReplyService(
        container,
        "check-stock",
        json.Unmarshal,
        json.Marshal,
        m.checkStock,
    )
}

// checkStock is the domain logic handler
func (m *InventoryModule) checkStock(ctx context.Context, req CheckStockRequest, msg *mono.Msg) (CheckStockResponse, error) {
    // Pure domain logic - no infrastructure concerns
    m.mu.RLock()
    stock, exists := m.inventory[req.ProductID]
    m.mu.RUnlock()

    return CheckStockResponse{
        Available: exists && stock >= req.Quantity,
        Stock:     stock,
    }, nil
}
```

## Data Transfer Objects (DTOs)

Define request and response types in `types.go`. These are the data structures exchanged between modules.

```go
// types.go
package inventory

import "context"

// InventoryAdapterPort - the driven port interface
type InventoryAdapterPort interface {
    CheckStock(ctx context.Context, req *CheckStockRequest) (*CheckStockResponse, error)
}

// CheckStockRequest - inbound DTO
type CheckStockRequest struct {
    ProductID string `json:"product_id"`
    Quantity  int    `json:"quantity"`
}

// CheckStockResponse - outbound DTO
type CheckStockResponse struct {
    Available bool `json:"available"`
    Stock     int  `json:"stock"`
}
```

## Consuming Dependencies

When your module depends on other modules, use their adapter ports to maintain loose coupling.

### Declaring Dependencies

```go
// module.go
func (m *OrderModule) Dependencies() []string {
    return []string{"inventory", "payment", "notification"}
}
```

### Receiving and Using Adapters

```go
// module.go
type OrderModule struct {
    inventory    inventory.InventoryAdapterPort    // Driven port
    payment      payment.PaymentAdapterPort        // Driven port
    notification notification.NotificationAdapterPort // Driven port
    eventBus     mono.EventBus
}

// SetDependencyServiceContainer - create adapters from dependency containers
func (m *OrderModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
    switch dependency {
    case "inventory":
        m.inventory = inventory.NewInventoryAdapter(container)
    case "payment":
        m.payment = payment.NewPaymentAdapter(container)
    case "notification":
        m.notification = notification.NewNotificationAdapter(container)
    }
}

// Domain logic uses adapters, not raw containers
func (m *OrderModule) placeOrder(ctx context.Context, req CreateOrderRequest, msg *mono.Msg) (CreateOrderResponse, error) {
    // Use the typed adapter interface
    stockResult, err := m.inventory.CheckStock(ctx, &inventory.CheckStockRequest{
        ProductID: req.ProductID,
        Quantity:  req.Quantity,
    })
    if err != nil {
        return CreateOrderResponse{}, fmt.Errorf("inventory check failed: %w", err)
    }
    // ... continue with domain logic
}
```

## Service Communication Patterns

The framework supports multiple communication patterns. Choose based on your use case:

| Pattern | Use Case | Example |
|---------|----------|---------|
| **Request-Reply** | Synchronous operations requiring a response | Check inventory, process payment |
| **Queue Group** | Fire-and-forget with load balancing | Send notifications |
| **Channel** | High-performance in-process communication | Analytics event tracking |
| **Event Bus** | Broadcast events to multiple consumers | Order created notification |

### Request-Reply Adapter

For synchronous request-response communication:

```go
func (a *paymentAdapter) Process(ctx context.Context, req *ProcessPaymentRequest) (*ProcessPaymentResponse, error) {
    var resp ProcessPaymentResponse
    if err := helper.CallRequestReplyService(
        ctx, a.container, "process",
        json.Marshal, json.Unmarshal, req, &resp,
    ); err != nil {
        return nil, fmt.Errorf("payment failed: %w", err)
    }
    return &resp, nil
}
```

### Queue Group Adapter

For asynchronous fire-and-forget operations:

```go
func (a *notificationAdapter) SendOrderCreated(ctx context.Context, notif *OrderCreatedNotification) error {
    if err := helper.SendQueueGroupService(
        ctx, a.container, "on-order-created",
        json.Marshal, notif,
    ); err != nil {
        return fmt.Errorf("notification send failed: %w", err)
    }
    return nil
}
```

## Event-Driven Communication

For cross-cutting events that multiple modules may consume:

### Publishing Events

```go
// module.go - Implement EventEmitterModule
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{
        events.OrderCreatedV1.ToBase(),
    }
}

// In domain logic
func (m *OrderModule) placeOrder(ctx context.Context, ...) (..., error) {
    // After order is created...
    event := events.OrderCreatedEvent{
        OrderID:   orderID,
        ProductID: req.ProductID,
        Amount:    req.Amount,
        Timestamp: time.Now(),
    }
    events.OrderCreatedV1.Publish(m.eventBus, event, nil)
    // ...
}
```

### Consuming Events

```go
// module.go - Implement EventConsumerModule
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    return registry.RegisterTypedEventConsumer(
        events.OrderCreatedV1,
        m.handleOrderCreated,
    )
}

func (m *NotificationModule) handleOrderCreated(ctx context.Context, event events.OrderCreatedEvent, msg *mono.Msg) error {
    // Handle the event
    return nil
}
```

## Shared Event Definitions

To avoid circular dependencies, place shared event definitions in a separate package:

```
myapp/
├── events/
│   └── events.go       # Shared event definitions
├── order/
│   └── module.go       # Emits events
└── notification/
    └── module.go       # Consumes events
```

```go
// events/events.go
package events

import (
    "time"
    "github.com/go-monolith/mono/pkg/helper"
)

type OrderCreatedEvent struct {
    OrderID   string    `json:"order_id"`
    ProductID string    `json:"product_id"`
    Amount    float64   `json:"amount"`
    Timestamp time.Time `json:"timestamp"`
}

var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
    "order", "OrderCreated", "v1",
)
```

## Best Practices

### 1. Keep Domain Logic Pure

The domain logic in your handlers should not depend on infrastructure:

```go
// Good: Pure domain logic
func (m *InventoryModule) checkStock(ctx context.Context, req CheckStockRequest, msg *mono.Msg) (CheckStockResponse, error) {
    m.mu.RLock()
    stock := m.inventory[req.ProductID]
    m.mu.RUnlock()
    return CheckStockResponse{Available: stock >= req.Quantity, Stock: stock}, nil
}

// Bad: Infrastructure leaking into domain
func (m *InventoryModule) checkStock(ctx context.Context, req *mono.Msg) ([]byte, error) {
    // Parsing JSON and marshaling are infrastructure concerns
    var request CheckStockRequest
    json.Unmarshal(req.Data, &request)
    // ...
}
```

### 2. Use Typed Helpers

Prefer typed service registration and calls:

```go
// Good: Typed helper
helper.RegisterTypedRequestReplyService(container, "check-stock",
    json.Unmarshal, json.Marshal, m.checkStock)

// Less ideal: Raw handler
container.RegisterRequestReplyService("check-stock", func(ctx context.Context, req *mono.Msg) ([]byte, error) {
    // Manual marshaling...
})
```

### 3. Interface Validation

Use compile-time interface checks to catch errors early:

```go
var (
    _ mono.DependentModule       = (*OrderModule)(nil)
    _ mono.ServiceProviderModule = (*OrderModule)(nil)
    _ mono.EventEmitterModule    = (*OrderModule)(nil)
)
```

### 4. Graceful Degradation

Handle failures appropriately based on criticality:

```go
// Critical: Return error to stop the operation
stockResult, err := m.inventory.CheckStock(ctx, req)
if err != nil {
    return CreateOrderResponse{}, fmt.Errorf("inventory check failed: %w", err)
}

// Non-critical: Log and continue
if err := m.notification.SendOrderCreated(ctx, notif); err != nil {
    log.Printf("notification failed (continuing): %v", err)
}
```

### 5. Validate at Boundaries

Validate input at the adapter boundary, not deep in domain logic:

```go
func (m *InventoryModule) checkStock(ctx context.Context, req CheckStockRequest, msg *mono.Msg) (CheckStockResponse, error) {
    // Validate at the boundary
    if req.ProductID == "" {
        return CheckStockResponse{}, fmt.Errorf("product_id is required")
    }
    if req.Quantity <= 0 {
        return CheckStockResponse{}, fmt.Errorf("quantity must be positive")
    }

    // Domain logic proceeds with valid data
    // ...
}
```

## Testing Hexagonal Architecture

One of the primary benefits of hexagonal architecture is improved testability. Here's how to test each layer:

### Unit Testing Domain Logic

Test domain handlers by passing typed requests directly:

```go
func TestCheckStock_Available(t *testing.T) {
    m := &InventoryModule{
        inventory: map[string]int{"laptop": 10},
    }

    resp, err := m.checkStock(context.Background(), CheckStockRequest{
        ProductID: "laptop",
        Quantity:  5,
    }, nil)

    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if !resp.Available {
        t.Error("expected stock to be available")
    }
    if resp.Stock != 10 {
        t.Errorf("expected stock=10, got %d", resp.Stock)
    }
}
```

### Testing with Mock Adapters

Mock the adapter port interface to test modules in isolation:

```go
type mockInventoryAdapter struct {
    checkStockFn func(ctx context.Context, req *CheckStockRequest) (*CheckStockResponse, error)
}

func (m *mockInventoryAdapter) CheckStock(ctx context.Context, req *CheckStockRequest) (*CheckStockResponse, error) {
    return m.checkStockFn(ctx, req)
}

func TestOrderModule_PlaceOrder_OutOfStock(t *testing.T) {
    orderModule := &OrderModule{
        inventory: &mockInventoryAdapter{
            checkStockFn: func(_ context.Context, _ *CheckStockRequest) (*CheckStockResponse, error) {
                return &CheckStockResponse{Available: false, Stock: 0}, nil
            },
        },
    }

    resp, err := orderModule.placeOrder(context.Background(), CreateOrderRequest{
        ProductID: "laptop",
        Quantity:  1,
        Amount:    999.99,
    }, nil)

    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if resp.Status != "failed_out_of_stock" {
        t.Errorf("expected status=failed_out_of_stock, got %s", resp.Status)
    }
}
```

## Complete Module Example

> **Note**: This example extends beyond the `/examples/multi-module/inventory/` implementation
> to demonstrate additional patterns. For the working code, see the examples directory.

Here's a complete example following hexagonal architecture:

### types.go

```go
package inventory

import "context"

// InventoryAdapterPort is the driven port for inventory operations
type InventoryAdapterPort interface {
    CheckStock(ctx context.Context, req *CheckStockRequest) (*CheckStockResponse, error)
}

type CheckStockRequest struct {
    ProductID string `json:"product_id"`
    Quantity  int    `json:"quantity"`
}

type CheckStockResponse struct {
    Available bool `json:"available"`
    Stock     int  `json:"stock"`
}
```

### adapter.go

```go
package inventory

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/go-monolith/mono"
    "github.com/go-monolith/mono/pkg/helper"
)

type inventoryAdapter struct {
    container mono.ServiceContainer
}

func NewInventoryAdapter(container mono.ServiceContainer) InventoryAdapterPort {
    if container == nil {
        panic("inventory adapter requires non-nil ServiceContainer")
    }
    return &inventoryAdapter{container: container}
}

func (a *inventoryAdapter) CheckStock(ctx context.Context, req *CheckStockRequest) (*CheckStockResponse, error) {
    var resp CheckStockResponse
    if err := helper.CallRequestReplyService(ctx, a.container, "check-stock",
        json.Marshal, json.Unmarshal, req, &resp); err != nil {
        return nil, fmt.Errorf("inventory: check stock failed: %w", err)
    }
    return &resp, nil
}
```

### module.go

```go
package inventory

import (
    "context"
    "encoding/json"
    "fmt"
    "sync"

    "github.com/go-monolith/mono"
    "github.com/go-monolith/mono/pkg/helper"
)

const NAME = "inventory"

type InventoryModule struct {
    inventory map[string]int
    mu        sync.RWMutex
}

var _ mono.ServiceProviderModule = (*InventoryModule)(nil)

func NewModule() *InventoryModule {
    return &InventoryModule{
        inventory: make(map[string]int),
    }
}

func (m *InventoryModule) Name() string { return NAME }

func (m *InventoryModule) Start(ctx context.Context) error {
    // Initialize inventory
    m.inventory["laptop"] = 10
    m.inventory["mouse"] = 50
    return nil
}

func (m *InventoryModule) Stop(ctx context.Context) error { return nil }

func (m *InventoryModule) RegisterServices(container mono.ServiceContainer) error {
    return helper.RegisterTypedRequestReplyService(container, "check-stock",
        json.Unmarshal, json.Marshal, m.checkStock)
}

func (m *InventoryModule) checkStock(ctx context.Context, req CheckStockRequest, msg *mono.Msg) (CheckStockResponse, error) {
    if req.ProductID == "" {
        return CheckStockResponse{}, fmt.Errorf("product_id is required")
    }

    m.mu.RLock()
    stock := m.inventory[req.ProductID]
    m.mu.RUnlock()

    return CheckStockResponse{
        Available: stock >= req.Quantity,
        Stock:     stock,
    }, nil
}
```

## Summary

Hexagonal architecture in this framework:

1. **types.go**: Define port interfaces and DTOs
2. **adapter.go**: Implement driven adapters wrapping `ServiceContainer`
3. **module.go**: Implement domain logic and register driving adapters via `RegisterServices()`

Key benefits:
- **Testability**: Domain logic can be unit tested without NATS
- **Loose coupling**: Modules communicate through interfaces
- **Flexibility**: Swap adapters without changing domain logic
- **Clear boundaries**: Each module has well-defined responsibilities

## See Also

- [Foundation Specification](../spec/foundation.md) - Core framework design
- [Multi-Module Example](../../examples/multi-module/) - Complete working example
- [Analytics Example](../../examples/analytics/) - Channel-based service example
