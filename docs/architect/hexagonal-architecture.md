# Hexagonal Architecture Guide

This guide explains how to apply hexagonal architecture (also known as Ports and Adapters) when developing modules with the Monolith-Framework, and what the framework checks for you.

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

Hexagonal architecture organizes code around the core business logic, isolating it from external concerns like databases, messaging, and external services. In this framework, each module fits this pattern through:

- **Contract**: the published port of a module — the DTOs its services exchange and the typed service and event definitions other modules use to reach it (`<module>/contract`)
- **Ports**: interfaces owned by a module that describe what its domain logic needs from the outside world (`ports.go`)
- **Adapters**: concrete implementations that connect ports to infrastructure — here, to other modules' contracts over NATS (`adapter.go`)
- **Domain**: core business logic independent of external systems (`module.go`)

The diagram below shows the flow: incoming NATS requests are handled by driving adapters
(services registered through the module's contract), which invoke domain logic, which uses
driven adapters to call other modules through *their* contracts.

```
                 ┌───────────────────────────────────────────────────┐
                 │                      Module                       │
                 │                                                   │
┌──────────┐     │  ┌──────────────┐       ┌──────────────────┐      │
│ Incoming │     │  │   Driving    │       │                  │      │
│ Request  │────▶│  │   Adapter    │──────▶│   Domain Logic   │      │
│ (NATS)   │     │  │  (contract   │       │                  │      │
└──────────┘     │  │   .Register) │       └────────┬─────────┘      │
                 │  └──────────────┘                │ port           │
                 │                                  ▼                │
                 │                         ┌──────────────────┐      │    ┌──────────┐
                 │                         │  Driven Adapter  │      │    │ External │
                 │                         │  (their contract │──────│───▶│  Module  │
                 │                         │   .Call)         │      │    │  (NATS)  │
                 │                         └──────────────────┘      │    └──────────┘
                 └───────────────────────────────────────────────────┘
```

## Module File Structure

Follow this standard file organization for each module:

```
mymodule/
├── contract/
│   └── contract.go   # Published port: DTOs + typed service/event definitions
├── ports.go          # Driven ports this module needs (interfaces it owns)
├── adapter.go        # Driven adapters: implement ports.go over other modules' contracts
└── module.go         # Domain logic + driving adapter (service registration)
```

### File Responsibilities

| File | Purpose | Who imports it |
|------|---------|----------------|
| `contract/` | **Published port**: DTOs, `helper.RequestReplyService[...]` / `QueueGroupService[...]` / `StreamConsumerService[...]` definitions, `helper.EventDefinition[...]` definitions | Any module, the host program, tests |
| `ports.go` | **Driven ports** the domain logic depends on, written in terms of other modules' contract DTOs | The module itself (and its tests, to substitute fakes) |
| `adapter.go` | **Driven adapters** that implement `ports.go` by calling other modules' contracts with the `ServiceContainer` received in `SetDependencyServiceContainer` | The module itself |
| `module.go` | **Domain logic** and the **driving adapter** (`RegisterServices` binding handlers to the module's own contracts) | `main.go` only |

### The contract package import rule

A `contract` package imports **only** the standard library, `pkg/types` and `pkg/helper`. It never imports its own module package and never another module's non-contract package. Consumers import `<provider>/contract` and nothing else of the provider.

This rule is what keeps consumers decoupled from provider internals, lets two modules share events without a separate "events" package or an import cycle, and is the anchor for boundary tooling.

A module that only exists to be started before others and exposes nothing does not need a `contract` package.

## Contracts: Defining the Published Port

A contract declares a service once — name, providing module, kind, request/response types and serialization — and both sides use it. Service names are never string literals in module code.

```go
// inventory/contract/contract.go
package contract

import "github.com/go-monolith/mono/pkg/helper"

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

// CheckStock is the request-reply service that checks stock availability.
var CheckStock = helper.RequestReplyService[CheckStockRequest, CheckStockResponse]("inventory", "check-stock")
```

Three kinds of service contract exist, mirroring the three client-facing service types:

| Constructor | Kind | Consumer side | Provider side |
|-------------|------|---------------|---------------|
| `helper.RequestReplyService[Req, Resp](module, name)` | Request-reply | `def.Call(ctx, container, req) (Resp, error)` | `def.Register(container, handler, opts...)` |
| `helper.QueueGroupService[T](module, name)` | Queue group | `def.Send(ctx, container, payload) error` | `def.Register(container, pairs...)` |
| `helper.StreamConsumerService[T](module, name)` | Stream consumer | `def.Publish(ctx, container, payload) (MsgPubAck, error)` | `def.Register(container, config, handler)` |

Serialization is JSON by default; `WithMarshaler` / `WithUnmarshaler` return a copy with a different codec (for example Protobuf). `ToBase()` returns the non-generic `types.BaseServiceDefinition` used in interfaces. The constructors panic on a name that is not kebab-case, like `helper.EventDefinition`: contracts are package-level variables and an invalid one is a programming error caught at process start.

Channel services have no contract type: they are in-process and already attributed to the consumer through `GetChannelService(name, consumerModule)`.

### Driving Adapter (Service Registration)

The provider binds its domain handler to its own contract. The framework handles the infrastructure (NATS subscriptions) and the contract handles serialization:

```go
// inventory/module.go
func (m *InventoryModule) RegisterServices(container mono.ServiceContainer) error {
    return contract.CheckStock.Register(container, m.checkStock)
}

// checkStock is the domain logic handler: typed, pure, testable without NATS.
func (m *InventoryModule) checkStock(ctx context.Context, req contract.CheckStockRequest, msg *mono.Msg) (contract.CheckStockResponse, error) {
    m.mu.RLock()
    stock, exists := m.inventory[req.ProductID]
    m.mu.RUnlock()

    return contract.CheckStockResponse{
        Available: exists && stock >= req.Quantity,
        Stock:     stock,
    }, nil
}
```

`Register` also records the contract on the service entry (`ServiceEntry.Contract`), so the framework and tooling can tell contract-backed services from ad hoc ones. A contract registered on a container bound to a different module fails with `ErrServiceContractMismatch`.

## Ports: What the Domain Needs

The consumer owns its ports. They are small interfaces, written in terms of the providers' contract DTOs, that say what the domain logic needs — nothing about NATS:

```go
// order/ports.go
package order

import (
    "context"

    inventorycontract "myapp/inventory/contract"
    paymentcontract   "myapp/payment/contract"
)

// InventoryPort checks stock for an order.
type InventoryPort interface {
    CheckStock(ctx context.Context, req inventorycontract.CheckStockRequest) (inventorycontract.CheckStockResponse, error)
}

// PaymentPort charges for an order.
type PaymentPort interface {
    Process(ctx context.Context, req paymentcontract.ProcessPaymentRequest) (paymentcontract.ProcessPaymentResponse, error)
}
```

Tests substitute fakes for these ports; production wires the adapters below.

## Adapters: Connecting Ports to Contracts

A driven adapter implements one port by calling the provider's contract with the provider's `ServiceContainer`. It is the only place in the module that knows the call crosses a module boundary:

```go
// order/adapter.go
package order

type inventoryAdapter struct{ container mono.ServiceContainer }

func (a inventoryAdapter) CheckStock(ctx context.Context, req inventorycontract.CheckStockRequest) (inventorycontract.CheckStockResponse, error) {
    resp, err := inventorycontract.CheckStock.Call(ctx, a.container, req)
    if err != nil {
        return inventorycontract.CheckStockResponse{}, fmt.Errorf("inventory check failed: %w", err)
    }
    return resp, nil
}
```

`Call` propagates the caller's context deadline to the handler (see "Request Deadlines" in the foundation spec) and wraps errors with the contract identity (`contract inventory/check-stock (request_reply): ...`).

## Consuming Dependencies

### Declaring Dependencies and Consumed Services

```go
// order/module.go
func (m *OrderModule) Dependencies() []string {
    return []string{"inventory", "payment", "notification"}
}

// ConsumeServices declares the contracts this module calls. The framework
// validates them at startup (see "What the Framework Checks").
func (m *OrderModule) ConsumeServices() []types.BaseServiceDefinition {
    return []types.BaseServiceDefinition{
        inventorycontract.CheckStock.ToBase(),
        paymentcontract.Process.ToBase(),
        notificationcontract.OnOrderCreated.ToBase(),
    }
}
```

### Wiring Adapters Behind Ports

```go
// order/module.go
type OrderModule struct {
    inventory    InventoryPort    // Driven port (ports.go)
    payment      PaymentPort      // Driven port
    notification NotificationPort // Driven port
    eventBus     mono.EventBus
}

// SetDependencyServiceContainer - wire the adapters behind the ports
func (m *OrderModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
    switch dependency {
    case "inventory":
        m.inventory = inventoryAdapter{container: container}
    case "payment":
        m.payment = paymentAdapter{container: container}
    case "notification":
        m.notification = notificationAdapter{container: container}
    }
}

// Domain logic uses ports, never containers or service names
func (m *OrderModule) placeOrder(ctx context.Context, req contract.CreateOrderRequest, msg *mono.Msg) (contract.CreateOrderResponse, error) {
    stockResult, err := m.inventory.CheckStock(ctx, inventorycontract.CheckStockRequest{
        ProductID: req.ProductID,
        Quantity:  req.Quantity,
    })
    if err != nil {
        return contract.CreateOrderResponse{}, fmt.Errorf("inventory check failed: %w", err)
    }
    // ... continue with domain logic
}
```

## Service Communication Patterns

The framework supports multiple communication patterns. Choose based on your use case:

| Pattern | Use Case | Contract | Example |
|---------|----------|----------|---------|
| **Request-Reply** | Synchronous operations requiring a response | `helper.RequestReplyService` | Check inventory, process payment |
| **Queue Group** | Fire-and-forget with load balancing | `helper.QueueGroupService` | Send notifications |
| **Stream Consumer** | Durable, at-least-once processing | `helper.StreamConsumerService` | Audit trail |
| **Channel** | High-performance in-process communication | none (in-process) | Analytics event tracking |
| **Event Bus** | Broadcast events to multiple consumers | `helper.EventDefinition` | Order created notification |

### Queue Group Adapter

```go
// notification/contract/contract.go
var OnOrderCreated = helper.QueueGroupService[OrderCreatedNotification]("notification", "on-order-created")

// order/adapter.go
func (a notificationAdapter) OrderCreated(ctx context.Context, notif notificationcontract.OrderCreatedNotification) error {
    if err := notificationcontract.OnOrderCreated.Send(ctx, a.container, notif); err != nil {
        return fmt.Errorf("notification send failed: %w", err)
    }
    return nil
}
```

## Event-Driven Communication

Events belong in the **emitter's** contract package: the emitter declares them, consumers import them from there. No shared "events" package is needed, and no import cycle arises because contract packages import no module packages.

```
myapp/
├── order/
│   ├── contract/
│   │   ├── contract.go   # PlaceOrder service + DTOs
│   │   └── events.go     # OrderCreatedEvent + OrderCreatedV1
│   └── module.go         # Emits OrderCreatedV1
└── notification/
    └── module.go         # Imports order/contract, consumes OrderCreatedV1
```

### Publishing Events

```go
// order/contract/events.go
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent]("order", "OrderCreated", "v1")

// order/module.go - Implement EventEmitterModule
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
    return []mono.BaseEventDefinition{contract.OrderCreatedV1.ToBase()}
}

// In domain logic (best-effort)
if err := contract.OrderCreatedV1.Publish(m.eventBus, event, nil); err != nil {
    log.Printf("event publish failed (continuing): %v", err)
}
```

### Consuming Events

```go
// notification/module.go - Implement EventConsumerModule
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    return helper.RegisterTypedEventConsumer(registry, ordercontract.OrderCreatedV1, m.handleOrderCreated, m)
}

func (m *NotificationModule) handleOrderCreated(ctx context.Context, event ordercontract.OrderCreatedEvent, msg *mono.Msg) error {
    // Handle the event
    return nil
}
```

## What the Framework Checks

The layout above is a convention; these are the parts the framework enforces or reports.

### At startup: consumed contracts (`ServiceConsumerModule`)

When a module implements `ConsumeServices()`, the framework validates the list while the module starts, after its dependency containers are set and before `RegisterServices`. For each contract:

- the providing module must be in `Dependencies()` — or be a registered plugin, since plugins are injected into every `UsePluginModule` and are not part of the dependency graph;
- that provider must have registered the service;
- the registered service must be of the contract's type.

A violation fails the module start (`ErrModuleStartFailed` wrapping `ErrMissingDependency`, `ErrServiceNotFound` or `ErrServiceContractMismatch`) and rolls back the modules already started. A renamed service or a forgotten dependency is therefore caught the moment the process starts, not on the first call in production.

### At registration: contract recording

`def.Register(...)` records the contract on the provider's `ServiceEntry.Contract`. The container refuses a contract whose module or type does not match (`ErrServiceContractMismatch`). The field is set after registration, outside the `ServiceRegistration` that middleware rebuilds, so access-log, audit and request-id middleware cannot drop it.

### Host-level escape hatches

`MonoApplication.Services(name)` and `MonoApplication.EventBus(name)` hand the host program (`main.go`) a module's container and the event bus, for API handlers, CLI commands or tests. They are not for module code: a module that holds the application object bypasses every check above, and neither the framework nor tooling can detect it.

### Planned

- **Runtime boundary enforcement**: dependency containers handed to a consumer become read-only views that attribute each lookup to the consumer, a module-scoped `EventBus` flags `services.<undeclared-module>.*` subjects, `WithStrictModuleBoundaries()` turns violations into errors, and `MonoApplication.ModuleBoundaries()` reports declared versus observed dependencies.
- **`monotest` harness**: an in-memory event bus and container, a `Harness` that runs the documented lifecycle order without NATS, and `CheckModule(t, m)` conformance assertions.

## Best Practices

### 1. Keep Domain Logic Pure

The domain logic in your handlers should not depend on infrastructure:

```go
// Good: typed handler, no serialization, talks to ports
func (m *InventoryModule) checkStock(ctx context.Context, req contract.CheckStockRequest, msg *mono.Msg) (contract.CheckStockResponse, error) {
    m.mu.RLock()
    stock := m.inventory[req.ProductID]
    m.mu.RUnlock()
    return contract.CheckStockResponse{Available: stock >= req.Quantity, Stock: stock}, nil
}

// Bad: infrastructure leaking into domain
func (m *InventoryModule) checkStock(ctx context.Context, req *mono.Msg) ([]byte, error) {
    var request CheckStockRequest
    json.Unmarshal(req.Data, &request) // parsing is an infrastructure concern
    // ...
}
```

### 2. Register and Call Through Contracts

```go
// Good: one definition, both sides use it
return contract.CheckStock.Register(container, m.checkStock)
resp, err := inventorycontract.CheckStock.Call(ctx, a.container, req)

// Less ideal: service name as a string literal in two places, hand-rolled codecs
helper.RegisterTypedRequestReplyService(container, "check-stock", json.Unmarshal, json.Marshal, m.checkStock)
helper.CallRequestReplyService(ctx, c, "check-stock", json.Marshal, json.Unmarshal, req, &resp)

// Avoid: bypassing the port entirely
m.eventBus.Request("services.inventory.check-stock", data, time.Second)
```

### 3. Declare What You Consume

Implement `ConsumeServices()` on every module that calls another module's services. It costs one method and turns a missing or renamed service into a startup failure.

### 4. Interface Validation

Use compile-time interface checks to catch errors early:

```go
var (
    _ mono.DependentModule        = (*OrderModule)(nil)
    _ mono.ServiceProviderModule  = (*OrderModule)(nil)
    _ mono.EventEmitterModule     = (*OrderModule)(nil)
    _ types.ServiceConsumerModule = (*OrderModule)(nil)
    _ InventoryPort               = inventoryAdapter{}
)
```

### 5. Graceful Degradation

Handle failures appropriately based on criticality:

```go
// Critical: Return error to stop the operation
stockResult, err := m.inventory.CheckStock(ctx, req)
if err != nil {
    return contract.CreateOrderResponse{}, fmt.Errorf("inventory check failed: %w", err)
}

// Non-critical: Log and continue
if err := m.notification.OrderCreated(ctx, notif); err != nil {
    log.Printf("notification failed (continuing): %v", err)
}
```

### 6. Validate at Boundaries

Validate input at the adapter boundary, not deep in domain logic:

```go
func (m *InventoryModule) checkStock(ctx context.Context, req contract.CheckStockRequest, msg *mono.Msg) (contract.CheckStockResponse, error) {
    if req.ProductID == "" {
        return contract.CheckStockResponse{}, fmt.Errorf("product_id is required")
    }
    if req.Quantity <= 0 {
        return contract.CheckStockResponse{}, fmt.Errorf("quantity must be positive")
    }
    // Domain logic proceeds with valid data
}
```

## Testing Hexagonal Architecture

One of the primary benefits of hexagonal architecture is improved testability.

### Unit Testing Domain Logic

Test domain handlers by passing typed requests directly:

```go
func TestCheckStock_Available(t *testing.T) {
    m := &InventoryModule{inventory: map[string]int{"laptop": 10}}

    resp, err := m.checkStock(context.Background(), contract.CheckStockRequest{ProductID: "laptop", Quantity: 5}, nil)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if !resp.Available || resp.Stock != 10 {
        t.Errorf("unexpected response: %+v", resp)
    }
}
```

### Testing with Fake Ports

Because the module owns its ports, a test substitutes a fake without any NATS:

```go
type fakeInventory struct {
    checkStock func(ctx context.Context, req inventorycontract.CheckStockRequest) (inventorycontract.CheckStockResponse, error)
}

func (f fakeInventory) CheckStock(ctx context.Context, req inventorycontract.CheckStockRequest) (inventorycontract.CheckStockResponse, error) {
    return f.checkStock(ctx, req)
}

func TestOrderModule_PlaceOrder_OutOfStock(t *testing.T) {
    orderModule := &OrderModule{
        inventory: fakeInventory{checkStock: func(context.Context, inventorycontract.CheckStockRequest) (inventorycontract.CheckStockResponse, error) {
            return inventorycontract.CheckStockResponse{Available: false}, nil
        }},
    }

    resp, err := orderModule.placeOrder(context.Background(), contract.CreateOrderRequest{ProductID: "laptop", Quantity: 1, Amount: 999.99}, nil)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if resp.Status != "failed_out_of_stock" {
        t.Errorf("expected status=failed_out_of_stock, got %s", resp.Status)
    }
}
```

### Integration Testing Through Contracts

With a real embedded NATS (`mono.WithNATSDontListen()`, `mono.WithNATSInProcessConn()`), the host calls the module exactly as another module would:

```go
resp, err := ordercontract.PlaceOrder.Call(ctx, app.Services("order"), ordercontract.CreateOrderRequest{ProductID: "laptop", Quantity: 1, Amount: 10})
```

## Summary

Hexagonal architecture in this framework:

1. **contract/**: publish DTOs and typed service/event definitions — the only thing other modules import
2. **ports.go**: declare the driven ports the domain needs, in the providers' contract DTOs
3. **adapter.go**: implement the ports over the providers' contracts
4. **module.go**: domain logic, `RegisterServices` through the module's own contracts, `ConsumeServices` for what it calls

Key benefits:
- **Testability**: domain logic and ports can be unit tested without NATS
- **Loose coupling**: modules know each other only through contracts
- **Fail fast**: a missing or mistyped consumed service fails startup
- **Clear boundaries**: the contract import rule gives every boundary a line that tooling can check

## See Also

- [Foundation Specification](../spec/foundation.md) - Core framework design ("Module Boundaries (Ports and Adapters)")
- [Multi-Module Example](../../examples/multi-module/) - Complete working example
