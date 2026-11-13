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

- **Contract**: what a module publishes to the others — the DTOs its services exchange and the typed service and event definitions other modules use to reach it (`<module>/contract`). It is the wire-level form of the module's driving port: it also fixes the service kind, the subject and the serialization
- **Ports**: interfaces owned by a module that describe what its domain logic needs from the outside world, in the module's own terms (`ports.go`)
- **Domain**: core business logic, plain Go that depends only on the ports (`service.go`)
- **Adapters**: concrete implementations that connect ports to infrastructure — here, to other modules' contracts over NATS — and translate between the two (`adapter.go`)
- **Module**: the `mono.Module` that wires the adapters behind the ports and binds the domain to the module's own contract (`module.go`)

The diagram below shows the flow: incoming NATS requests are handled by driving adapters
(services registered through the module's contract), which invoke domain logic, which uses
driven adapters to call other modules through *their* contracts.

The rule that makes it hexagonal is the direction of the dependencies: adapters and the module
know the domain, the domain knows only its ports. `service.go` and `ports.go` import neither the
framework nor another module.

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
│   └── contract.go   # Published contract: DTOs + typed service/event definitions
├── ports.go          # Driven ports this module needs (interfaces it owns, in its own terms)
├── service.go        # Domain logic: plain Go over ports.go
├── adapter.go        # Driven adapters: implement ports.go over other modules' contracts
└── module.go         # mono.Module: wiring + driving adapter (service registration)
```

### File Responsibilities

| File | Purpose | May import | Who imports it |
|------|---------|------------|----------------|
| `contract/` | **Published contract**: DTOs, `helper.RequestReplyService[...]` / `QueueGroupService[...]` / `StreamConsumerService[...]` definitions, `helper.EventDefinition[...]` definitions | Standard library, `pkg/types`, `pkg/helper`, other contract packages | Any module, the host program, tests |
| `ports.go` | **Driven ports** the domain logic depends on, written in the module's own types | Standard library | The module itself (and its tests, to substitute fakes) |
| `service.go` | **Domain logic**: the rules of the module, calling the outside world only through `ports.go` | Standard library | The module itself |
| `adapter.go` | **Driven adapters** that implement `ports.go` by calling other modules' contracts, and translate between the module's types and the providers' DTOs | The framework, the providers' `contract` packages | The module itself |
| `module.go` | **Wiring** (adapters behind ports, domain built in `Start`) and the **driving adapter** (`RegisterServices` binding the domain to the module's own contracts) | The framework, its own and the providers' `contract` packages | `main.go` only |

For a module with little logic, `service.go` can be left out and the handler in `module.go` can hold the logic, as the inventory, payment and notification modules of the example do. Split it out once the module has rules worth testing on their own, or dependencies to call.

### The contract package import rule

A `contract` package imports **only** the standard library, `pkg/types`, `pkg/helper` and other modules' contract packages. It never imports its own module package and never another module's non-contract package. Consumers import `<provider>/contract` and nothing else of the provider.

This rule is what keeps consumers decoupled from provider internals and lets two modules share events without a separate "events" package or an import cycle. The framework cannot see imports at runtime, so the rule has its own check: `monotest.CheckImports` (see "In tests: the import rule" below).

A module that only exists to be started before others and exposes nothing does not need a `contract` package.

## Contracts: Defining What a Module Publishes

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

// checkStock is the handler: typed, no serialization. In a module this small it
// also holds the logic; a larger module delegates to its domain service.
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

The consumer owns its ports. They are small interfaces that say what the domain logic needs, in the consumer's own terms — nothing about NATS, the framework, or another module's DTOs:

```go
// order/ports.go
package order

import "context"

// InventoryPort tells whether the stock covers an order.
type InventoryPort interface {
    Available(ctx context.Context, productID string, quantity int) (bool, error)
}

// PaymentPort charges for an order.
type PaymentPort interface {
    Charge(ctx context.Context, orderID string, amount float64, currency string) (approved bool, err error)
}

// EventsPort announces what happened in the order module.
type EventsPort interface {
    OrderPlaced(ctx context.Context, order PlacedOrder) error
}
```

The port asks for what the domain needs (`Available`), not for what the provider happens to offer (`CheckStock` with a stock count). When the inventory module changes its DTO, the port and the domain do not change; only the adapter does.

Tests substitute fakes for these ports; production wires the adapters below.

> **Shortcut for thin modules.** A port may use a provider's contract DTOs directly when the two models are the same and likely to stay so. The adapter is then a pass-through. The price is that a change to the provider's DTO reaches the consumer's domain code, so prefer the module's own types as soon as the domain does more than forward the call.

## Domain: Plain Go Over the Ports

```go
// order/service.go
package order

// Service places orders. It imports neither the framework nor another module.
type Service struct {
    inventory InventoryPort
    payment   PaymentPort
    events    EventsPort
}

func NewService(inventory InventoryPort, payment PaymentPort, events EventsPort) (*Service, error) {
    // returns an error when a port is nil
}

func (s *Service) PlaceOrder(ctx context.Context, order NewOrder) (Result, error) {
    available, err := s.inventory.Available(ctx, order.ProductID, order.Quantity)
    if err != nil {
        return Result{}, fmt.Errorf("place order: %w", err)
    }
    if !available {
        return Result{Status: StatusOutOfStock}, nil
    }
    // ... charge, then announce
}
```

## Adapters: Connecting Ports to Contracts

A driven adapter implements one port by calling the provider's contract, and translates between the module's own types and the provider's DTOs. It is the only place in the module that knows which module answers the port, what its DTOs look like, and that the call crosses a module boundary:

```go
// order/adapter.go
package order

type inventoryAdapter struct{ services types.ServiceLookup }

func (a inventoryAdapter) Available(ctx context.Context, productID string, quantity int) (bool, error) {
    resp, err := inventorycontract.CheckStock.Call(ctx, a.services, inventorycontract.CheckStockRequest{
        ProductID: productID,
        Quantity:  quantity,
    })
    if err != nil {
        return false, fmt.Errorf("inventory check failed: %w", err)
    }
    return resp.Available, nil
}
```

`Call` propagates the caller's context deadline to the handler (see "Request Deadlines" in the foundation spec) and wraps errors with the contract identity (`contract inventory/check-stock (request_reply): ...`).

### Hold a dependency as a `ServiceLookup`

`types.ServiceLookup` is the read side of a `ServiceContainer`: the `Get*` methods and `Has`. An adapter only looks services up, so it stores the container it is handed as a `ServiceLookup`. Registering on a dependency's container is then a compile error instead of a `register-on-dependency` violation at runtime. `Call`, `Send` and `Publish` accept a `ServiceLookup`, and any `ServiceContainer` is one.

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
    inventory InventoryPort // Driven ports (ports.go)
    payment   PaymentPort
    events    EventsPort
    service   *Service      // Domain (service.go)
}

// SetDependencyServiceContainer - wire the adapters behind the ports
func (m *OrderModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
    switch dependency {
    case "inventory":
        m.inventory = inventoryAdapter{services: container}
    case "payment":
        m.payment = paymentAdapter{services: container}
    }
}

// SetEventBus - the event bus goes behind a port like any other dependency
func (m *OrderModule) SetEventBus(bus mono.EventBus) {
    m.events = eventsAdapter{bus: bus}
}

// Start - the containers and the bus are set by now; build the domain
func (m *OrderModule) Start(_ context.Context) error {
    service, err := NewService(m.inventory, m.payment, m.events)
    if err != nil {
        return fmt.Errorf("order module: %w", err)
    }
    m.service = service
    return nil
}

// RegisterServices - bind the driving adapter to the module's own contract
func (m *OrderModule) RegisterServices(container mono.ServiceContainer) error {
    return contract.PlaceOrder.Register(container, m.placeOrder)
}

// placeOrder is the driving adapter: contract DTO in, domain call, contract DTO out
func (m *OrderModule) placeOrder(ctx context.Context, req contract.CreateOrderRequest, _ *mono.Msg) (contract.CreateOrderResponse, error) {
    result, err := m.service.PlaceOrder(ctx, NewOrder{ProductID: req.ProductID, Quantity: req.Quantity, Amount: req.Amount})
    if err != nil {
        return contract.CreateOrderResponse{}, err
    }
    return contract.CreateOrderResponse{OrderID: result.OrderID, Status: string(result.Status)}, nil
}
```

The framework calls `SetDependencyServiceContainer`, then `SetEventBus`, then `RegisterServices`, then `Start`, and delivers requests only after every module started. Building the domain in `Start` therefore sees every port, and a missing one fails the start.

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
func (a notificationAdapter) OrderPlaced(ctx context.Context, order PlacedOrder) error {
    err := notificationcontract.OnOrderCreated.Send(ctx, a.services, notificationcontract.OrderCreatedNotification{
        OrderID:   order.OrderID,
        ProductID: order.ProductID,
        Amount:    order.Amount,
        Timestamp: order.PlacedAt,
    })
    if err != nil {
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
│   ├── adapter.go        # eventsAdapter publishes OrderCreatedV1
│   └── module.go         # Declares OrderCreatedV1 in EmitEvents()
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

// order/adapter.go - the events port implemented over the event bus
type eventsAdapter struct{ bus mono.EventBus }

func (a eventsAdapter) OrderPlaced(_ context.Context, order PlacedOrder) error {
    return contract.OrderCreatedV1.Publish(a.bus, contract.OrderCreatedEvent{
        OrderID:   order.OrderID,
        ProductID: order.ProductID,
        Amount:    order.Amount,
        Timestamp: order.PlacedAt,
    }, nil)
}

// order/service.go - the domain announces through the port (best-effort)
if err := s.events.OrderPlaced(ctx, placed); err != nil {
    log.Printf("event publish failed (continuing): %v", err)
}
```

Publishing is a call to the outside world like any other, so it sits behind a port: the domain says what happened, the adapter knows the event definition and the bus.

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

The layout above is a convention; these are the parts the framework enforces or reports. Together they cover the boundaries *between* modules. What happens *inside* a module (whether `service.go` stays free of infrastructure) is not observable by the framework: keep it by review, helped by the import list of `service.go` and `ports.go`.

### At startup: consumed contracts (`ServiceConsumerModule`)

When a module implements `ConsumeServices()`, the framework validates the list while the module starts, after its dependency containers are set and before `RegisterServices`. For each contract:

- the providing module must be in `Dependencies()` — or be a registered plugin, since plugins are injected into every `UsePluginModule` and are not part of the dependency graph;
- that provider must have registered the service;
- the registered service must be of the contract's type.

A violation fails the module start (`ErrModuleStartFailed` wrapping `ErrMissingDependency`, `ErrServiceNotFound` or `ErrServiceContractMismatch`) and rolls back the modules already started. A renamed service or a forgotten dependency is therefore caught the moment the process starts, not on the first call in production.

The list is also **complete**: a module that implements `ConsumeServices()` and looks up a dependency service that is not in it commits an `undeclared-service-use` violation (see the table below). A module that does not implement the interface is not checked. Channel services have no contract type and are not checked either.

### At registration: contract recording

`def.Register(...)` records the contract on the provider's `ServiceEntry.Contract`. The container refuses a contract whose module or type does not match (`ErrServiceContractMismatch`). The field is set after registration, outside the `ServiceRegistration` that middleware rebuilds, so access-log, audit and request-id middleware cannot drop it.

### Host-level escape hatches

`MonoApplication.Services(name)` and `MonoApplication.EventBus(name)` hand the host program (`main.go`) a module's container and the event bus, for API handlers, CLI commands or tests. They are not for module code: a module that holds the application object bypasses every check above, and neither the framework nor tooling can detect it.

### At runtime: boundary report and strict mode

The `ServiceContainer` a module receives for a dependency is a read-only view that attributes every lookup to the module, and the `EventBus` it receives is scoped to its declared boundaries. Five kinds of crossing are detected:

| Kind | What the module did |
|------|---------------------|
| `register-on-dependency` | Called `Register*`, `Unregister`, `BindModule`, `Set*`, `StartChannelRouters` or `RegisterContract` on a dependency's container |
| `channel-consumer-mismatch` | Asked for a channel service under another module's name |
| `undeclared-service-subject` | Published to or requested `services.<module>.<service>` of a module that is not itself, a declared dependency or a plugin — bypassing the port |
| `foreign-service-subscription` | Subscribed to another module's service subject |
| `undeclared-service-use` | Implements `ConsumeServices()` and looked up a request-reply, queue group or stream consumer service of a dependency that is not in the list |

By default a crossing is counted, warned once, and let through, so existing applications keep working. `MonoApplication.ModuleBoundaries()` returns, per module, the declared dependencies, the consumed contracts, the services actually looked up (with counts), the declared dependencies never used, the consumed contracts not used so far, and the crossings. The same summary is logged at `Stop`.

```go
for _, r := range app.ModuleBoundaries() {
    fmt.Println(r.Module, "uses", r.Observed, "never used", r.UnusedDependencies, r.UnusedContracts, "violations", r.Violations)
}
```

`Entries()` on a dependency's view returns descriptions of the provider's services (name, type, subject, queue groups, options, contract), not the provider's entries: the handlers and channels are left out, so a consumer cannot run or replace them.

With `mono.WithStrictModuleBoundaries()` a crossing fails instead with `ErrModuleBoundaryViolation`. The intended path: run non-strict, read the report, fix the modules, then turn strict on, and keep it on in tests.

### In tests: `monotest.CheckModule`

The `monotest` package runs a module without NATS through the same lifecycle order, dependency views and scoped bus as the framework, and `CheckModule` turns the rules above into one test per module. See "Testing Hexagonal Architecture" below.

### In tests: the import rule (`monotest.CheckImports`)

Everything above watches what a module does through the container and the bus it is handed. A module that imports another module's package and calls it directly never touches either, so no runtime check can see it. `monotest.CheckImports` reads the import graph instead (it runs `go list`) and reports:

| Rule | What it reports |
|------|-----------------|
| `contract-import` | A `contract` package that imports anything but the standard library, `pkg/types`, `pkg/helper` and other contract packages |
| `module-import` | A package outside a module that imports a package of that module other than its `contract` |

A module is a package with a `contract` sub-package, together with every package below it. `package main` is exempt, because the host program imports module packages to construct and register them. One test for the whole application is enough:

```go
// In the root directory of the application, so that "./..." covers every module
func TestImportRules(t *testing.T) {
    monotest.CheckImports(t)
}
```

A test placed elsewhere, such as beside `cmd/app/main.go`, points the check at the root with `monotest.WithImportDir("../..")`. The check fails when the patterns match no package, or only `package main` and host packages, when the application has no module at all, and when `./...` matches no package of a module (a `cmd/app` directory with helper packages of its own), so a wrong directory cannot pass by accident.

| Option | Use |
|--------|-----|
| `WithHostPackages(paths...)` | A composition root that is not `package main` |
| `WithModuleRoots(paths...)` | A module without a `contract` sub-package |
| `WithNonContractPackages(paths...)` | A package named `contract` that is not a module's contract (`legal/contract`, `order/internal/contract`) |
| `WithAllowedContractImports(paths...)` | Extra imports for contract packages, such as a Protobuf runtime or a shared value type |
| `WithImportDir(dir)`, `WithImportPatterns(patterns...)` | Where to run and what to check (default `./...` in the test's directory). Naming the patterns accepts a set without a package of a module |

Imports of test files are not checked, so an integration test may wire real modules together. Packages are listed for the default build context, so files behind build tags are not checked. `monotest.ImportViolations(ctx, opts...)` returns the same findings without a test.

## Best Practices

### 1. Keep Domain Logic Pure

The domain should not depend on infrastructure. In a module with a `service.go`, that file and `ports.go` import only the standard library: no framework, no other module, no `*mono.Msg`, no event bus.

```go
// Good: the domain talks to its ports, in its own types
func (s *Service) PlaceOrder(ctx context.Context, order NewOrder) (Result, error) {
    available, err := s.inventory.Available(ctx, order.ProductID, order.Quantity)
    // ...
}

// Bad: the domain builds another module's DTO and publishes on the bus itself
func (m *OrderModule) placeOrder(ctx context.Context, req contract.CreateOrderRequest, msg *mono.Msg) (contract.CreateOrderResponse, error) {
    stock, err := inventorycontract.CheckStock.Call(ctx, m.inventoryContainer, inventorycontract.CheckStockRequest{ProductID: req.ProductID})
    // ...
    contract.OrderCreatedV1.Publish(m.eventBus, event, nil)
}
```

In a module small enough to keep its logic in the handler, at least keep serialization out of it:

```go
// Good: typed handler, no serialization
func (m *InventoryModule) checkStock(ctx context.Context, req contract.CheckStockRequest, msg *mono.Msg) (contract.CheckStockResponse, error) {
    m.mu.RLock()
    stock := m.inventory[req.ProductID]
    m.mu.RUnlock()
    return contract.CheckStockResponse{Available: stock >= req.Quantity, Stock: stock}, nil
}

// Bad: infrastructure leaking into the handler
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
resp, err := inventorycontract.CheckStock.Call(ctx, a.services, req)

// Less ideal: service name as a string literal in two places, hand-rolled codecs
helper.RegisterTypedRequestReplyService(container, "check-stock", json.Unmarshal, json.Marshal, m.checkStock)
helper.CallRequestReplyService(ctx, c, "check-stock", json.Marshal, json.Unmarshal, req, &resp)

// Avoid: bypassing the port entirely
m.eventBus.Request("services.inventory.check-stock", data, time.Second)
```

### 3. Declare What You Consume

Implement `ConsumeServices()` on every module that calls another module's services, and list every contract the module calls. It costs one method, turns a missing or renamed service into a startup failure, and makes a call outside the list show up as an `undeclared-service-use` violation.

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
available, err := s.inventory.Available(ctx, order.ProductID, order.Quantity)
if err != nil {
    return Result{}, fmt.Errorf("place order: %w", err)
}

// Non-critical: Log and continue
if err := s.notification.OrderPlaced(ctx, placed); err != nil {
    log.Printf("notification failed (continuing): %v", err)
}
```

### 6. Validate at Boundaries

Validate the shape of the input at the adapter boundary, not deep in domain logic. Rules that belong to the business (an order needs a positive quantity) stay in the domain, so that every way into the module applies them:

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

One of the primary benefits of hexagonal architecture is improved testability. The `monotest` package (`github.com/go-monolith/mono/monotest`) runs a module without NATS: an in-memory event bus, the real service container bound to the module with its handlers dispatched, stand-in containers for dependencies, and the framework's own dependency views and scoped bus, so a test sees exactly what production would.

### Conformance Test (one per module)

```go
func TestOrderModuleConformance(t *testing.T) {
    bus := monotest.NewEventBus()
    inventory := monotest.NewContainer("inventory", bus)
    // Register the dependency's services through its own contract...
    err := inventorycontract.CheckStock.Register(inventory, func(_ context.Context, req inventorycontract.CheckStockRequest, _ *mono.Msg) (inventorycontract.CheckStockResponse, error) {
        return inventorycontract.CheckStockResponse{Available: true, Stock: 5}, nil
    })
    if err != nil {
        t.Fatal(err)
    }
    // ...or with a JSON stub when the contract is not at hand.
    payment := monotest.NewContainer("payment", bus)
    err = monotest.StubRequestReply(payment, "process", func(_ context.Context, req paymentcontract.ProcessPaymentRequest) (paymentcontract.ProcessPaymentResponse, error) {
        return paymentcontract.ProcessPaymentResponse{Success: true}, nil
    })
    if err != nil {
        t.Fatal(err)
    }

    monotest.CheckModule(t, order.NewModule(),
        monotest.WithDependency("inventory", inventory),
        monotest.WithDependency("payment", payment),
        monotest.WithExercise(func(ctx context.Context, h *monotest.Harness) error {
            _, err := contract.PlaceOrder.Call(ctx, h.Container(), contract.CreateOrderRequest{ProductID: "laptop", Quantity: 1, Amount: 10})
            return err
        }))
}
```

`CheckModule` reports, each as its own failure: a non-kebab-case name; a self or duplicate dependency, or one without a stand-in container; a provider that registers nothing or an invalid subject; a consumed contract the stand-in does not provide with the same type; every boundary violation, including a dependency service the module calls without listing it in `ConsumeServices()` (with a hint on how to fix it); a declared dependency that was never looked up (use `WithOrderingOnlyDependency` for start-ordering dependencies); a `ConsumeServices()` contract that was never looked up (use `WithUnexercisedContract` for one the exercise cannot reach); an emitted event with an invalid subject or an `events.*` subject the module published that is missing from `EmitEvents()`; and `Start`/`Stop` failures or a `Stop` that exceeds its timeout (`Stop` receives a context with that deadline, as in the framework). The harness dispatches services only after `Start`, as the framework does, so a module that calls a dependency from `Start` fails here as it would in production; a consumer that looks events up through the registry gets them with `WithEmittedEvents`.

### Driving a Module in a Test

```go
h := monotest.New(t, order.NewModule(), monotest.WithDependency("inventory", inventory), monotest.WithDependency("payment", payment))
if err := h.Start(ctx); err != nil { t.Fatal(err) }
defer h.Stop(ctx)

resp := monotest.Call[contract.CreateOrderRequest, contract.CreateOrderResponse](t, h, "place-order", contract.CreateOrderRequest{ProductID: "laptop", Quantity: 1, Amount: 10})
if inventory.Calls("check-stock") != 1 { t.Error("inventory not consulted") }
report := h.Report() // the module's types.ModuleBoundaryReport
```

Events published by the module appear on `h.EventBus()`; events the module consumes can be published there with their definition's `Publish`. JetStream (stream consumer, event stream consumer, cron) is not emulated: use an integration test with the embedded server for those.

### Unit Testing a Handler

In a module that keeps its logic in the handler, test the handler by passing typed requests directly:

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

### Testing the Domain with Fake Ports

Because the module owns its ports and the domain depends on nothing else, a test substitutes fakes and needs no framework, no NATS and no other module. The test file imports only the module's package and the standard library:

```go
// order/service_test.go
package order_test

type fakeInventory struct{ available bool }

func (f fakeInventory) Available(context.Context, string, int) (bool, error) {
    return f.available, nil
}

func TestService_PlaceOrder_OutOfStock(t *testing.T) {
    service, err := order.NewService(fakeInventory{available: false}, fakePayment{}, fakeEvents{})
    if err != nil {
        t.Fatal(err)
    }

    result, err := service.PlaceOrder(context.Background(), order.NewOrder{ProductID: "laptop", Quantity: 1, Amount: 999.99})
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if result.Status != order.StatusOutOfStock {
        t.Errorf("status = %s, want %s", result.Status, order.StatusOutOfStock)
    }
}
```

This is the fast test for the rules of the module. The conformance test above covers what it cannot: that the adapters translate correctly and that the module stays within its boundaries.

### Checking the Import Rule

```go
func TestImportRules(t *testing.T) {
    monotest.CheckImports(t) // see "In tests: the import rule"
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
2. **ports.go**: declare the driven ports the domain needs, in the module's own types
3. **service.go**: the domain, plain Go over the ports
4. **adapter.go**: implement the ports over the providers' contracts and translate between the two
5. **module.go**: wire the adapters behind the ports, `RegisterServices` through the module's own contracts, `ConsumeServices` for everything it calls

Key benefits:
- **Testability**: the domain is unit tested with fake ports, without the framework or NATS
- **Loose coupling**: modules know each other only through contracts, and a provider's DTO change stops at the consumer's adapter
- **Fail fast**: a missing or mistyped consumed service fails startup
- **Clear boundaries**: between modules they are checked — at startup, at runtime (report, strict mode), in `CheckModule`, and on the import graph by `CheckImports`

## See Also

- [Foundation Specification](../spec/foundation.md) - Core framework design ("Module Boundaries (Ports and Adapters)")
- [Multi-Module Example](../../examples/multi-module/) - Complete working example
