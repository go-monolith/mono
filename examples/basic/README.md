# Basic Example

The smallest mono application: one "Hello World" module, registered, started and stopped by the framework.
It shows how to create the application with functional options, implement the `Module` interface (Name, Start, Stop), query health, and shut down gracefully on SIGINT/SIGTERM.

## Running the Example

```bash
cd examples/basic
go run .

# Or, from the repository root (stops after 3 s):
make run-example-1
```

Press Ctrl+C to stop the application.

## Code Structure

```
examples/basic/
├── main.go           # Application entry point, framework initialization
├── hello_module.go   # Simple "Hello World" module implementation
└── README.md         # This file
```

## Key Patterns

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

### 2. The Module

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

The framework logs module lifecycle events (registration, start, stop) itself, so modules don't need their own logging for these.

### 3. Registration and Lifecycle

```go
helloModule := NewHelloModule()
err := app.Register(helloModule) // register before Start

app.Start(ctx) // start all registered modules
app.Stop(ctx)  // stop all modules gracefully (in reverse order)
```

### 4. Health Checks

```go
health := app.Health(ctx)
fmt.Printf("Healthy: %v, NATS: %v\n", health.Healthy, health.NATSHealthy)
```

`Health` reports the operational status of the app and all modules.

## Expected Output

Framework log lines are interleaved and omitted here:

```
=== Mono-Framework Basic Example ===
Demonstrates: Application initialization, module registration, and lifecycle management

✓ App created successfully
✓ Module registered successfully
  → Hello from HelloModule! Module is now running...
  → Module name: hello-world
✓ App started - module is running

App Health:
{
  "healthy": true,
  "state": 2,
  "nats_healthy": true,
  "modules": {
    "hello-world": {
      "name": "hello-world",
      "healthy": true,
      "supports_health": true
    }
  },
  "timestamp": "..."
}
Registered modules: [hello-world]

Press Ctrl+C to shutdown...

Shutdown signal received...
  → Goodbye from HelloModule!
✓ App stopped successfully
Example completed!
```

## Next Steps

- **[Multi-Module Example](../multi-module/README.md)**: module dependencies, request-reply and queue group services, events.
- **[Analytics Example (Channel Services)](../analytics/README.md)**: channel-based services for in-process communication.

See also:

- [Design Document](https://github.com/go-monolith/mono/blob/main/docs/spec/foundation.md)
- [Module Interface Documentation](https://github.com/go-monolith/mono/blob/main/pkg/types/module.go)
- [Framework API Documentation](https://github.com/go-monolith/mono/blob/main/pkg/types/framework.go)
