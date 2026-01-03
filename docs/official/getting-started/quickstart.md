# ⚡ Quick Start

Build your first Monolith Framework application in 5 minutes.

## What You'll Build

A simple order tracking system with:
- An `OrderModule` module that creates orders
- Structured logging for debugging
- Application startup and graceful shutdown

## Step 1: Create the Module

Create a file `order_service.go`:

```go
package main

import (
    "context"
    "fmt"
    "log/slog"
    "sync/atomic"
)

type OrderModule struct {
    orderCount  atomic.Int64
}

// Implement the Module interface

func (m *OrderModule) Name() string {
    return "order"
}

func (m *OrderModule) Start(ctx context.Context) error {
    slog.Info("Starting order module")
    return nil
}

func (m *OrderModule) Stop(ctx context.Context) error {
    slog.Info("Stopping order module")
    return nil
}

// Business logic

func (m *OrderModule) CreateOrder(customerID, productID string, amount float64) (string, error) {
    orderNum := m.orderCount.Add(1)
    orderID := fmt.Sprintf("ORD-%06d", orderNum)

    slog.Info(
        "Order created",
        "orderID", orderID,
        "customer", customerID,
        "product", productID,
        "amount", amount,
    )

    return orderID, nil
}
```

## Step 2: Create the Main Application

Create a file `main.go`:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/go-monolith/mono"
)

func main() {
    // Step 1: Create the application
    app, err := mono.NewMonoApplication(
        mono.WithLogLevel(mono.LogLevelInfo),
        mono.WithLogFormat(mono.LogFormatText),
        mono.WithShutdownTimeout(5*time.Second),
    )
    if err != nil {
        log.Fatalf("Failed to create app: %v", err)
    }

    // Step 2: Create and register the module
    orderModule := &OrderModule{}
    if err := app.Register(orderModule); err != nil {
        log.Fatalf("Failed to register module: %v", err)
    }

    // Step 3: Start the application
    ctx := context.Background()
    if err := app.Start(ctx); err != nil {
        log.Fatalf("Failed to start app: %v", err)
    }

    fmt.Println("\n=== Application Started ===")
    fmt.Println("Modules:", app.Modules())
    fmt.Println()

    // Step 4: Use the module
    fmt.Println("=== Creating Orders ===")
    orderModule.CreateOrder("CUST-001", "laptop", 1299.99)
    orderModule.CreateOrder("CUST-002", "mouse", 29.99)
    orderModule.CreateOrder("CUST-003", "keyboard", 79.99)

    fmt.Println("\n=== Health Check ===")
    health := app.Health(ctx)
    fmt.Printf("Healthy: %v\n", health.Healthy)
    fmt.Printf("NATS Healthy: %v\n", health.NATSHealthy)

    // Step 5: Graceful shutdown
    fmt.Println("\n=== Shutting Down ===")
    if err := app.Stop(ctx); err != nil {
        log.Fatalf("Failed to stop app: %v", err)
    }

    fmt.Println("Application stopped successfully!")
}
```

## Step 3: Run the Application

Build and run:

```bash
go run .
```

You should see:

```
INFO [main] app/framework.go:XXX framework starting
INFO [main] app/nats.go:XXX NATS server started
INFO [main] app/framework.go:XXX registering module: order
INFO [main] app/framework.go:XXX module order started
INFO [main] app/framework.go:XXX framework started

=== Application Started ===
Modules: [order]

=== Creating Orders ===
INFO [order] ... Order created orderID=ORD-000001 customer=CUST-001 product=laptop amount=1299.99
INFO [order] ... Order created orderID=ORD-000002 customer=CUST-002 product=mouse amount=29.99
INFO [order] ... Order created orderID=ORD-000003 customer=CUST-003 product=keyboard amount=79.99

=== Health Check ===
Healthy: true
NATS Healthy: true

=== Shutting Down ===
INFO [main] app/framework.go:XXX stopping framework
INFO [order] ... Stopping order module
INFO [main] app/framework.go:XXX framework stopped
Application stopped successfully!
```

{% hint style="success" %}
**Congratulations!** You've built your first Monolith Framework application.
{% endhint %}

## Key Concepts

### Module Interface

Every module must implement three methods:

- **`Name()`** - Unique identifier for the module (used in logs and discovery)
- **`Start(ctx)`** - Called when the application starts (initialize resources here)
- **`Stop(ctx)`** - Called when the application shuts down (clean up resources)

### Logging

Use Go's standard `log/slog` package for structured logging:

```go
import "log/slog"

slog.Info("Order created", "orderID", id, "amount", amount)
```

### Application Configuration

Configure the application with functional options:

```go
mono.NewMonoApplication(
    mono.WithLogLevel(mono.LogLevelInfo),     // Log level
    mono.WithLogFormat(mono.LogFormatText),   // Text or JSON
    mono.WithShutdownTimeout(5*time.Second),  // Graceful shutdown timeout
)
```

{% hint style="info" %}
**Advanced Configuration**: For complex NATS settings, use `WithNATSConfigFile()` to load configuration from a file. Programmatic options can override specific settings. See the [NATS Config File example](../../../examples/nats-config/README.md) for details.
{% endhint %}

### Graceful Shutdown

Always call `app.Stop()` to gracefully shut down:
- Stops all modules in reverse order
- Drains in-flight operations
- Stops the embedded NATS server

## Common Patterns

### RegisterServices Pattern

If you implement `ServiceProviderModule`:

```go
func (m *OrderModule) RegisterServices(container mono.ServiceContainer) error {
    // Register services here
    return nil
}
```

### Health Checks

Implement `HealthCheckableModule` for custom health logic:

```go
func (m *OrderModule) Health(ctx context.Context) mono.ModuleHealth {
    return mono.ModuleHealth{
        Healthy: true,
        Status: "Running",
    }
}
```

### Dependencies Between Modules

Use `DependentModule`:

```go
func (m *OrderModule) Dependencies() []string {
    return []string{"payment"}
}
```

This ensures `payment` module starts before `order` module.

## Next Steps

Now that you understand the basics:

1. **[Project Structure](project-structure.md)** - Learn how to organize larger applications
2. **[Core Concepts](../core-concepts/modules.md)** - Deep dive into modules
3. **[Inter-Module Communication](../core-concepts/inter-module-communication.md)** - Learn about Service and Event communication patterns
4. **[Examples](../../../examples/)** - Study the multi-module and event emitter examples

## Troubleshooting

### "address already in use"

Another application is using port 4222. Either:
- Stop the other application
- Use a different port: `mono.WithNATSPort(4223)`
- Or use a config file with port override: `mono.WithNATSConfigFile("server.conf")` with `mono.WithNATSPort(4223)`

### Module won't start

Check that your module:
1. Implements `Name()`, `Start()`, and `Stop()` methods
2. Is registered with `app.Register(module)`
3. Has correct error handling in `Start()`

---

Great job! You've built your first Monolith Framework application. Continue with [Project Structure](project-structure.md) to learn how to organize larger applications.
