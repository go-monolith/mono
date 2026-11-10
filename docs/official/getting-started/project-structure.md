# Project Structure

This guide shows you how to organize your Monolith Framework application for larger projects.

## Recommended Project Layout

```
my-mono-app/
├── main.go                          # Application entry point
├── go.mod
├── go.sum
├── README.md
├── .gitignore
│
├── modules/                         # Application modules
│   ├── order/
│   │   ├── contract/               # Published port: the only package other modules import
│   │   │   ├── contract.go         # DTOs + typed service contracts
│   │   │   └── events.go           # Events this module emits
│   │   ├── module.go               # Module implementation, ConsumeServices()
│   │   ├── module_test.go          # monotest.CheckModule conformance test
│   │   ├── ports.go                # Driven ports this module needs
│   │   ├── adapter.go              # Adapters over other modules' contracts
│   │   ├── service.go              # Business logic
│   │   └── types.go                # Internal domain types
│   │
│   ├── payment/
│   │   ├── contract/
│   │   │   └── contract.go
│   │   ├── module.go
│   │   ├── service.go
│   │   └── types.go
│   │
│   └── notification/
│       ├── module.go
│       ├── service.go
│       └── handlers.go
│
├── config/                          # Configuration
│   ├── config.go                   # Configuration loading
│   └── defaults.go                 # Default values
│
├── tests/                           # Integration tests
│   ├── integration_test.go
│   └── fixtures.go
│
└── docs/                            # Documentation
    ├── README.md
    ├── architecture.md
    └── modules.md
```

{% hint style="info" %}
**Since v0.1.0** the recommended layout gives each module a `contract` sub-package, and consumers a `ports.go` and `adapter.go`. The framework can then check module boundaries. See the [Module Boundaries Guide](../guides/module-boundaries.md).
{% endhint %}

## Module File Structure

Organize each module in its own package:

### `modules/order/`

**`module.go`** - Core module implementation:

```go
package order

import "context"

type Module struct {
    service *Service
}

func (m *Module) Name() string { return "order" }

func (m *Module) Start(ctx context.Context) error {
    m.service = NewService()
    return nil
}

func (m *Module) Stop(ctx context.Context) error {
    return nil
}
```

**`service.go`** - Business logic:

```go
package order

import "log/slog"

type Service struct {}

func NewService() *Service {
    return &Service{}
}

func (s *Service) CreateOrder(customerID, productID string, quantity int) (*Order, error) {
    // Implementation
}
```

**`types.go`** - Domain models:

```go
package order

type Order struct {
    ID        string
    CustomerID string
    Items     []OrderItem
    Total     float64
}

type OrderItem struct {
    ProductID string
    Quantity  int
    Price     float64
}
```

**`events.go`** - Event definitions (if using EventEmitterModule):

```go
package order

import "github.com/go-monolith/mono"

type OrderCreatedEvent struct {
    OrderID    string
    CustomerID string
    Total      float64
}

var OrderCreatedV1 = mono.EventDefinition[OrderCreatedEvent]{
    Name:   "OrderCreated",
    Version: "v1",
    Domain: "order",
    // ... schema details
}
```

## Application Setup

Your `main.go` should be clean and focused:

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/go-monolith/mono"
    "my-mono-app/config"
    "my-mono-app/modules/order"
    "my-mono-app/modules/payment"
    "my-mono-app/modules/notification"
)

func main() {
    // Load configuration
    cfg := config.Load()

    // Create application
    app, err := mono.NewMonoApplication(
        mono.WithLogLevel(cfg.LogLevel),
        mono.WithShutdownTimeout(cfg.ShutdownTimeout),
    )
    if err != nil {
        log.Fatalf("Failed to create app: %v", err)
    }

    // Register modules
    app.Register(order.NewModule())
    app.Register(payment.NewModule())
    app.Register(notification.NewModule())

    // Start application
    ctx := context.Background()
    if err := app.Start(ctx); err != nil {
        log.Fatalf("Failed to start app: %v", err)
    }

    defer app.Stop(ctx)

    // Run application
    select {} // Keep running
}
```

## Configuration Pattern

Create a `config/config.go`:

```go
package config

import (
    "os"
    "strconv"
    "time"

    "github.com/go-monolith/mono"
)

type Config struct {
    LogLevel        mono.LogLevel
    LogFormat       mono.LogFormat
    ShutdownTimeout time.Duration
    NATSPort        int
}

func Load() *Config {
    return &Config{
        LogLevel:        parseLogLevel(os.Getenv("LOG_LEVEL")),
        LogFormat:       parseLogFormat(os.Getenv("LOG_FORMAT")),
        ShutdownTimeout: parseDuration(os.Getenv("SHUTDOWN_TIMEOUT"), 10*time.Second),
        NATSPort:        parseInt(os.Getenv("NATS_PORT"), 4222),
    }
}

// Helper functions...
```

## Service Dependencies

Use dependency injection for module dependencies:

**`modules/order/module.go`**:

```go
type Module struct {
    logger           mono.Logger
    paymentService   *payment.Service
}

// Declare dependency
func (m *Module) Dependencies() []string {
    return []string{"payment"}
}

// Receive dependency
func (m *Module) SetDependencyServiceContainer(
    module string,
    container mono.ServiceContainer) {
    if module == "payment" {
        m.paymentService = container.RequestReplyServiceClient(...)
    }
}
```

## Service Registration

If your module provides services:

```go
func (m *Module) RegisterServices(container mono.ServiceContainer) error {
    // Register a request-reply service
    return container.RegisterRequestReplyService(
        "create-order",
        m.handleCreateOrder,
    )
}

func (m *Module) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    return m.service.CreateOrder(req.CustomerID, req.ProductID, req.Quantity)
}
```

## Testing Organization

Create integration tests in `tests/`:

```go
// tests/integration_test.go
package tests

import (
    "context"
    "testing"
    "time"

    "github.com/go-monolith/mono"
    "my-mono-app/modules/order"
)

func TestOrderCreation(t *testing.T) {
    app, _ := mono.NewMonoApplication()
    app.Register(order.NewModule())
    app.Start(context.Background())
    defer app.Stop(context.Background())

    // Test order creation...
}
```

## Directory Considerations

### Keep Modules Self-Contained

Each module should have:
- Its own types (no shared domain models across modules)
- Its own error types (or use sentinel errors from errors package)
- Its own configuration (if any)

### Separate Public and Internal APIs

```go
// ✓ Public API (used by other modules)
func (s *Service) CreateOrder(req *CreateOrderRequest) (*Order, error)

// ✓ Internal helper (not used by other modules)
func (s *Service) validateOrder(order *Order) error
```

### Import Only Contract Packages

A module imports another module's `contract` package and nothing else of it. A `contract` package imports only the standard library, `mono/pkg/types` and `mono/pkg/helper`.

```go
// ✓ order imports the published port
import inventorycontract "my-mono-app/modules/inventory/contract"

// ❌ order imports the module package
import "my-mono-app/modules/inventory"
```

### Avoid Circular Dependencies

Use event-driven communication instead:

```
// ❌ Don't do this (circular dependency)
Order module → Payment module → Order module

// ✓ Do this (decoupled via events)
Order module --publishes--> OrderCreated event
Payment module --consumes--> OrderCreated event
```

## Scaling Patterns

### Many Modules

For projects with 10+ modules, organize by domain:

```
modules/
├── order/           # Order domain
│   ├── order/
│   ├── cart/
│   └── fulfillment/
├── payment/         # Payment domain
│   ├── billing/
│   └── invoicing/
└── customer/        # Customer domain
    ├── profile/
    └── account/
```

### Shared Types

For types used by multiple modules:

```
shared/
├── types.go         # Common domain types
├── events.go        # Shared event definitions
└── errors.go        # Common error types
```

But prefer event-driven communication over shared dependencies. Types that cross a module boundary belong in the owning module's `contract` package: request and response types with the provider, event types with the emitter. That removes the need for a shared events package.

## Complete Example

See the [multi-module example](../../../examples/multi-module/README.md) for a complete project structure with:
- Multiple modules with dependencies
- Service communication patterns
- Event publishing and consumption
- Complete runnable code

## Best Practices

✓ **Do:**
- Keep modules focused and single-purpose
- Use dependency injection for cross-module communication
- Organize by domain/feature, not by layer
- Keep module packages independent where possible
- Publish a `contract` package per module and import only that from other modules
- Use configuration for environment-specific settings

✗ **Don't:**
- Create a package per file
- Mix multiple modules in one package
- Share internal implementation details across modules
- Import another module's package other than its `contract`
- Store global state in module packages
- Create deep package hierarchies (2-3 levels is typical)

## Next Steps

- Follow the [Module Boundaries Guide](../guides/module-boundaries.md) for contracts, ports and adapters
- Learn about [Inter-Module Communication](../core-concepts/inter-module-communication.md) patterns
- Explore the [multi-module example](../../../examples/multi-module/README.md)
- Review [Core Concepts](../core-concepts/modules.md) for module lifecycle

---

You now understand how to structure a larger Monolith Framework application! Next, dive into [Core Concepts](../core-concepts/README.md) to learn about module communication patterns.
