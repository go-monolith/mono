# Framework API

API documentation for the `MonoApplication` (alias for `MonoFramework`), the main entry point for the Monolith Framework.

## Overview

`MonoApplication` is the central coordinator for module registration, lifecycle management, and inter-module communication. It manages NATS messaging, module dependencies, and graceful shutdown.

{% hint style="info" %}
**Functional Options Pattern:** All framework configuration uses `WithXxx()` option functions. Options are validated at application time and last-wins for conflicting settings.
{% endhint %}

## Signatures

### Creating an Application

```go
func NewMonoApplication(opts ...MonoFrameworkOption) (MonoApplication, error)
```

Creates a new framework instance with the given configuration options. Returns an error if any option validation fails.

**Parameters:**
- `opts` - Zero or more functional option functions to configure the framework

**Returns:**
- `MonoApplication` - The framework instance
- `error` - Validation error or configuration error

**Example:**
```go
app, err := mono.NewMonoApplication(
    mono.WithNATSPort(4222),
    mono.WithJetStreamStorageDir("./data"),
    mono.WithLogLevel(mono.LogLevelInfo),
)
if err != nil {
    log.Fatalf("Failed to create application: %v", err)
}
```

### Lifecycle Methods

```go
func (app MonoApplication) Start(ctx context.Context) error
```

Initializes and starts the framework, NATS server, and all registered modules in dependency order. Returns error if any module fails to start (triggers automatic rollback).

**Parameters:**
- `ctx` - Context for the startup operation

**Returns:**
- `error` - Startup error or nil on success

```go
func (app MonoApplication) Stop(ctx context.Context) error
```

Gracefully shuts down all modules in reverse dependency order, then stops NATS. Respects the configured shutdown timeout.

**Parameters:**
- `ctx` - Context for the shutdown operation

**Returns:**
- `error` - Shutdown error or nil on success

### Module Management

```go
func (app MonoApplication) Register(module Module) error
```

Registers a regular module with the framework. Must be called before `Start()`. The module can be any type that implements the `Module` interface.

**Parameters:**
- `module` - Module implementing the `Module` interface

**Returns:**
- `error` - Registration error (duplicate name, invalid module)

**Example:**
```go
app.Register(&OrderModule{})
app.Register(&PaymentModule{})
```

```go
func (app MonoApplication) RegisterPlugin(plugin PluginModule, alias string) error
```

Registers a plugin module with the given alias. Plugins start before middleware and regular modules, and stop last.

**Parameters:**
- `plugin` - Plugin implementing the `PluginModule` interface
- `alias` - Unique identifier for this plugin instance (can differ from plugin name)

**Returns:**
- `error` - Registration error

**Example:**
```go
storage, _ := fsjetstream.New(config)
app.RegisterPlugin(storage, "storage")
```

```go
func (app MonoApplication) Plugin(alias string) PluginModule
```

Retrieves a registered plugin by alias. Returns nil if not found.

**Parameters:**
- `alias` - Plugin alias from registration

**Returns:**
- `PluginModule` - Plugin instance or nil

```go
func (app MonoApplication) Modules() []string
```

Returns a list of registered module names (not including plugins).

**Returns:**
- `[]string` - Slice of module names

### Service Discovery

```go
func (app MonoApplication) Services(moduleName string) ServiceContainer
```

Returns the `ServiceContainer` for a specific module, allowing access to registered services.

**Parameters:**
- `moduleName` - Name of the module (from `Module.Name()`)

**Returns:**
- `ServiceContainer` - The module's service container

**Example:**
```go
services := app.Services("payment")
client := services.RequestReplyServiceClient("process-payment")
```

```go
func (app MonoApplication) EventBus(moduleName string) EventBus
```

Returns the `EventBus` for a specific module.

**Parameters:**
- `moduleName` - Name of the module

**Returns:**
- `EventBus` - The module's event bus

### Health and Status

```go
func (app MonoApplication) Health(ctx context.Context) FrameworkHealth
```

Returns the aggregated health status of the framework and all modules.

**Parameters:**
- `ctx` - Context for the health check operation

**Returns:**
- `FrameworkHealth` - Health status structure containing:
  - `Healthy` - Overall framework health (true only if framework + NATS + all modules healthy)
  - `State` - Current framework state
  - `NATSHealthy` - NATS server health
  - `Modules` - Map of individual module health statuses
  - `Timestamp` - When the check was performed
  - `Message` - Error message if unhealthy

**Example:**
```go
health := app.Health(ctx)
fmt.Printf("Framework healthy: %v\n", health.Healthy)
fmt.Printf("NATS healthy: %v\n", health.NATSHealthy)
for name, moduleHealth := range health.Modules {
    fmt.Printf("%s: %v - %s\n", name, moduleHealth.Healthy, moduleHealth.Message)
}
```

```go
func (app MonoApplication) Logger() Logger
```

Returns the framework's internal logger instance.

**Returns:**
- `Logger` - The framework logger

## Configuration Options

All framework configuration is done through functional options passed to `NewMonoApplication()`.

### NATS Configuration

#### WithNATSPort

```go
func WithNATSPort(port int) MonoFrameworkOption
```

Sets the NATS server listening port. Default is `4222`. Port must be between 1024 and 65535.

| Property | Value |
|----------|-------|
| Type | `int` |
| Default | `4222` |
| Constraints | 1024-65535 |

**Example:**
```go
mono.WithNATSPort(4222)
```

#### WithNATSHost

```go
func WithNATSHost(host string) MonoFrameworkOption
```

Sets the NATS server host address. Default is `127.0.0.1`.

| Property | Value |
|----------|-------|
| Type | `string` |
| Default | `127.0.0.1` |
| Constraints | Non-empty |

**Example:**
```go
mono.WithNATSHost("0.0.0.0")
```

#### WithNATSDontListen

```go
func WithNATSDontListen() MonoFrameworkOption
```

Prevents the NATS server from listening on TCP. Useful for embedded servers. Must be combined with `WithNATSInProcessConn()`.

**Example:**
```go
mono.WithNATSDontListen()
mono.WithNATSInProcessConn()
```

#### WithNATSInProcessConn

```go
func WithNATSInProcessConn() MonoFrameworkOption
```

Enables in-process client connections using `net.Pipe()` instead of TCP. Can be used alone or with `WithNATSDontListen()`.

**Example:**
```go
mono.WithNATSInProcessConn()
```

#### WithNATSLogging

```go
func WithNATSLogging(debug, trace, sysTrace bool) MonoFrameworkOption
```

Configures NATS server logging flags.

| Parameter | Description |
|-----------|-------------|
| `debug` | Enable debug-level logging |
| `trace` | Enable trace-level logging |
| `sysTrace` | Enable system trace logging |

**Example:**
```go
mono.WithNATSLogging(true, false, false)  // Debug only
```

#### WithNATSMaxPayload

```go
func WithNATSMaxPayload(bytes int32) MonoFrameworkOption
```

Sets the maximum message payload size. Must be between 1KB and 8MB.

| Property | Value |
|----------|-------|
| Type | `int32` |
| Default | (NATS default: 1MB) |
| Constraints | 1KB-8MB |

**Example:**
```go
mono.WithNATSMaxPayload(2 * 1024 * 1024)  // 2 MB
```

#### WithNATSClustering

```go
func WithNATSClustering(clusterName, clusterHost string, clusterPort int, routes []string) MonoFrameworkOption
```

Enables NATS clustering for distributed deployments.

| Parameter | Description |
|-----------|-------------|
| `clusterName` | Name of the cluster (required) |
| `clusterHost` | Host for cluster communication |
| `clusterPort` | Port for cluster communication (1024-65535) |
| `routes` | URLs to other cluster nodes (optional for seed node) |

**Example (Seed Node):**
```go
mono.WithNATSClustering("production", "127.0.0.1", 6222, nil)
```

**Example (Non-Seed Node):**
```go
mono.WithNATSClustering("production", "127.0.0.1", 6223,
    []string{"nats://127.0.0.1:6222"})
```

#### WithNATSConfigFile

```go
func WithNATSConfigFile(path string) MonoFrameworkOption
```

Sets the path to a NATS server configuration file. The file is processed using NATS `server.ProcessConfigFile()` during startup.

When both a config file and programmatic options (like `WithNATSPort`) are specified, the config file provides base settings and programmatic options override them. This allows using a standard NATS config file while customizing specific settings.

| Property | Value |
|----------|-------|
| Type | `string` |
| Default | (empty, disabled) |
| Constraints | Non-empty path |

{% hint style="info" %}
File existence and validity are checked during `Start()`, not during option application.
{% endhint %}

**Example (Config file only):**
```go
mono.NewMonoApplication(
    mono.WithNATSConfigFile("/etc/nats/server.conf"),
)
```

**Example (Config file with programmatic overrides):**
```go
mono.NewMonoApplication(
    mono.WithNATSConfigFile("/etc/nats/server.conf"),
    mono.WithNATSPort(4333), // Overrides port from config file
)
```

For NATS configuration file format and options, see the [NATS Server Configuration documentation](https://docs.nats.io/running-a-nats-service/configuration).

### JetStream Configuration

#### WithJetStreamDomain

```go
func WithJetStreamDomain(domain string) MonoFrameworkOption
```

Enables JetStream with the specified domain for multi-tenancy. Enables JetStream implicitly.

| Property | Value |
|----------|-------|
| Type | `string` |
| Default | (disabled) |
| Constraints | Non-empty |

**Example:**
```go
mono.WithJetStreamDomain("production")
```

#### WithJetStreamStorageDir

```go
func WithJetStreamStorageDir(dir string) MonoFrameworkOption
```

Enables JetStream with persistent file storage at the specified directory.

| Property | Value |
|----------|-------|
| Type | `string` |
| Default | (in-memory if not set) |
| Constraints | Non-empty path |

**Example:**
```go
mono.WithJetStreamStorageDir("./data/jetstream")
```

### Logger Configuration

#### WithLogLevel

```go
func WithLogLevel(level LogLevel) MonoFrameworkOption
```

Sets the log level for the framework logger.

| Value | Description |
|-------|-------------|
| `LogLevelDebug` | Detailed diagnostic information |
| `LogLevelInfo` | General informational messages (default) |
| `LogLevelWarn` | Warning messages about potential problems |
| `LogLevelError` | Error messages about failures |

**Example:**
```go
mono.WithLogLevel(mono.LogLevelDebug)
```

#### WithLogFormat

```go
func WithLogFormat(format LogFormat) MonoFrameworkOption
```

Sets the log output format.

| Value | Description |
|-------|-------------|
| `LogFormatText` | Human-readable text format (default) |
| `LogFormatJSON` | Structured JSON format |

**Example:**
```go
mono.WithLogFormat(mono.LogFormatJSON)
```

#### WithLogOutput

```go
func WithLogOutput(w io.Writer) MonoFrameworkOption
```

Sets the output destination for logs. Default is `os.Stdout`.

| Property | Value |
|----------|-------|
| Type | `io.Writer` |
| Default | `os.Stdout` |
| Constraints | Non-nil |

**Example:**
```go
logFile, _ := os.OpenFile("app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
mono.WithLogOutput(logFile)
```

#### WithLogSource

```go
func WithLogSource(enable bool) MonoFrameworkOption
```

Enables source file name and line number in log entries.

| Property | Value |
|----------|-------|
| Type | `bool` |
| Default | `false` |

**Example:**
```go
mono.WithLogSource(true)
```

#### WithLogger / WithCustomLogger

```go
func WithLogger(logger Logger) MonoFrameworkOption
func WithCustomLogger(logger Logger) MonoFrameworkOption
```

Injects a custom logger instance. Overrides all logger factory options.

| Property | Value |
|----------|-------|
| Type | `Logger` |
| Default | (auto-created) |
| Constraints | Non-nil |

**Example:**
```go
customLogger := myLoggerFactory.NewLogger("app")
mono.WithCustomLogger(customLogger)
```

### Framework Options

#### WithShutdownTimeout

```go
func WithShutdownTimeout(timeout time.Duration) MonoFrameworkOption
```

Sets the maximum time to wait for graceful shutdown. Default is 30 seconds. Must be at least 1 second.

| Property | Value |
|----------|-------|
| Type | `time.Duration` |
| Default | `30 * time.Second` |
| Constraints | ≥ 1 second |

**Example:**
```go
mono.WithShutdownTimeout(60 * time.Second)
```

#### WithQueueGroupOptimisticWindow

```go
func WithQueueGroupOptimisticWindow(window time.Duration) MonoFrameworkOption
```

Enables optimistic publish mode for queue group services. When set, the first send uses ACK mode, subsequent sends within the window use fire-and-forget.

| Property | Value |
|----------|-------|
| Type | `time.Duration` |
| Default | `0` (disabled) |
| Constraints | 0 or ≥ 100ms |

**Example:**
```go
mono.WithQueueGroupOptimisticWindow(1 * time.Second)
```

## Default Configuration

```go
func DefaultConfig() *MonoFrameworkConfig
```

Returns a configuration with all default values. Useful for testing.

**Default Values:**

| Setting | Default |
|---------|---------|
| NATS Host | `127.0.0.1` |
| NATS Port | `4222` |
| NATS DontListen | `false` |
| NATS InProcessConn | `false` |
| NATS ConfigFile | (empty, disabled) |
| JetStream Domain | (empty, disabled) |
| JetStream Dir | (empty, in-memory) |
| Log Level | `LogLevelInfo` |
| Log Format | `LogFormatText` |
| Log Output | `os.Stdout` |
| Log Source | `false` |
| Shutdown Timeout | `30 * time.Second` |
| Queue Group Optimistic Window | `0` (disabled) |

## Constants and Enums

### Framework States

```go
const (
    StateCreated  = iota   // Framework created, not started
    StateStarting          // Framework starting up
    StateRunning           // Framework running
    StateStopping          // Framework shutting down
    StateStopped           // Framework stopped
)
```

### Log Levels

```go
const (
    LogLevelDebug LogLevel = iota
    LogLevelInfo
    LogLevelWarn
    LogLevelError
)
```

### Log Formats

```go
const (
    LogFormatText LogFormat = iota
    LogFormatJSON
)
```

## Examples

### Complete Application Setup

```go
package main

import (
    "context"
    "log"
    mono "github.com/go-monolith/mono/v1"
)

func main() {
    // Create application with configuration
    app, err := mono.NewMonoApplication(
        mono.WithNATSPort(4222),
        mono.WithJetStreamStorageDir("./data"),
        mono.WithLogLevel(mono.LogLevelInfo),
        mono.WithShutdownTimeout(30 * time.Second),
    )
    if err != nil {
        log.Fatalf("Failed to create application: %v", err)
    }
    defer app.Stop(context.Background())

    // Register modules
    app.Register(&OrderModule{})
    app.Register(&PaymentModule{})

    // Start framework
    if err := app.Start(context.Background()); err != nil {
        log.Fatalf("Failed to start application: %v", err)
    }

    // Application is running
    log.Println("Application started successfully")

    // Get health status
    health := app.Health(context.Background())
    log.Printf("Framework healthy: %v\n", health.Healthy)

    // Application runs until os.Interrupt or app.Stop()
}
```

### Checking Module Health

```go
health := app.Health(context.Background())
if !health.Healthy {
    log.Printf("Framework unhealthy: %s\n", health.Message)
    for name, moduleHealth := range health.Modules {
        if !moduleHealth.Healthy {
            log.Printf("Module %s: %s\n", name, moduleHealth.Message)
        }
    }
}
```

### Accessing Services

```go
// Get a module's service container
services := app.Services("payment")

// Create a client for a request-reply service
paymentClient := services.RequestReplyServiceClient("process-payment")

// Use the client
response, err := paymentClient.Call(context.Background(), &PaymentRequest{
    Amount: 100.00,
})
```

## Related Documentation

- [Module API](module.md) - Module interfaces and lifecycle
- [Service Container API](container.md) - Service management
- [EventBus API](eventbus.md) - Event publishing
- [API Reference](README.md) - All APIs overview

---

For more information, see the [Foundation Specification](../../spec/foundation.md) and [Getting Started](../getting-started/README.md) guide.
