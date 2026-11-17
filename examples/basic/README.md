# Basic Example

This example demonstrates the basic usage of the Monolith-Framework with a simple "Hello World" module.

## Overview

This example shows:
- Framework initialization with default configuration
- Single module registration
- Module lifecycle (Start/Stop)
- Basic logging
- Graceful shutdown handling

## What You'll Learn

1. **Framework Creation**: Using `mono.NewMonoApplication()` with functional options
2. **Module Interface**: Implementing the core `Module` interface (Name, Start, Stop)
3. **Configuration**: Setting log level, format, and shutdown timeout
4. **Lifecycle Management**: Framework automatically manages module startup and shutdown
5. **Health Checks**: Querying framework and module health status
6. **Signal Handling**: Graceful shutdown on SIGINT/SIGTERM

## Code Structure

```
examples/basic/
├── main.go           # Application entry point, framework initialization
├── hello_module.go   # Simple "Hello World" module implementation
└── README.md         # This file
```

## Module Implementation

The `HelloModule` demonstrates the minimal module interface:

```go
type HelloModule struct {
    name string
}

// Name returns the module identifier
func (m *HelloModule) Name() string {
    return m.name
}

// Start initializes the module
func (m *HelloModule) Start(ctx context.Context) error {
    fmt.Println("  → Hello from HelloModule! Module is now running...")
    return nil
}

// Stop gracefully shuts down the module
func (m *HelloModule) Stop(ctx context.Context) error {
    fmt.Println("  → Goodbye from HelloModule!")
    return nil
}
```

The real module also implements `mono.HealthCheckableModule` (a `Health` method that returns `mono.HealthStatus{Healthy: true}`), which is why `health.Modules` lists it with `supports_health: true`.

Note: The framework automatically logs module lifecycle events (registration, start, stop), so modules don't need to implement their own logging for basic operations.

## Running the Example

```bash
# Navigate to the example directory
cd examples/basic

# Run the example
go run .

# Expected output (framework log lines are interleaved and omitted here):
# === Mono-Framework Basic Example ===
# Demonstrates: Application initialization, module registration, and lifecycle management
#
# ✓ App created successfully
# ✓ Module registered successfully
#   → Hello from HelloModule! Module is now running...
#   → Module name: hello-world
# ✓ App started - module is running
#
# App Health:
# {
#   "healthy": true,
#   "state": 2,
#   "nats_healthy": true,
#   "modules": {
#     "hello-world": {
#       "name": "hello-world",
#       "healthy": true,
#       "supports_health": true
#     }
#   },
#   "timestamp": "..."
# }
# Registered modules: [hello-world]
#
# Press Ctrl+C to shutdown...
#
# Shutdown signal received...
#   → Goodbye from HelloModule!
# ✓ App stopped successfully
# Example completed!

# Or, from the repository root (stops after 3 s):
# make run-example-1
```

## Key Concepts Demonstrated

### 1. App Configuration

```go
app, err := mono.NewMonoApplication(
    mono.WithLogLevel(mono.LogLevelInfo),
    mono.WithLogFormat(mono.LogFormatText),
    mono.WithNATSLogging(false, false, false), // NATS server logs off (the default)
    mono.WithShutdownTimeout(10*time.Second),
)
```

The app uses functional options for clean, composable configuration.

### 2. Module Registration

```go
helloModule := NewHelloModule()
err := app.Register(helloModule)
```

Modules must be registered before starting the app.

### 3. Lifecycle Management

```go
// Start all registered modules
app.Start(ctx)

// Stop all modules gracefully (in reverse order)
app.Stop(ctx)
```

The app manages the complete module lifecycle automatically.

### 4. Health Checks

```go
health := app.Health(ctx)
fmt.Printf("Healthy: %v, NATS: %v\n", health.Healthy, health.NATSHealthy)
```

Check the operational status of the app and all modules.

## Next Steps

After understanding this basic example, explore:

1. **[Multi-Module Example](../multi-module/README.md)** - Learn about:
   - Module dependencies
   - Inter-module communication
   - Request-reply patterns
   - Event-driven architecture

2. **[Analytics Example (Channel Services)](../analytics/README.md)** - Learn about:
   - Channel-based services
   - Advanced service patterns
   - Custom message handlers

## See Also

- [Design Document](https://github.com/go-monolith/mono/blob/main/docs/spec/foundation.md)
- [Module Interface Documentation](https://github.com/go-monolith/mono/blob/main/pkg/types/module.go)
- [Framework API Documentation](https://github.com/go-monolith/mono/blob/main/pkg/types/framework.go)
