# 📚 API Reference

Complete API documentation for the Monolith Framework, including function signatures, interfaces, configuration options, and usage examples.

## Overview

The Monolith Framework provides a clean, Go-idiomatic API for building modular monolithic applications with embedded NATS messaging. This API reference documents all public types, functions, and options.

## Core Components

### Framework

The main entry point for creating and managing the framework instance.

- **Type**: `MonoApplication` (alias for `MonoFramework`)
- **Creation**: `NewMonoApplication(opts ...MonoFrameworkOption)`
- **Lifecycle**: `Start(ctx context.Context)`, `Stop(ctx context.Context)`
- **Inspection**: `Modules()`, `Health(ctx)`, `Services(moduleName)`, `EventBus(moduleName)`

See [Framework API](framework.md) for detailed documentation.

### Modules

The basic building blocks of any application. Modules are registered with the framework and managed through a well-defined lifecycle.

- **Base Interface**: `Module` (required for all modules)
- **Optional Interfaces**: `EventBusAwareModule`, `DependentModule`, `ServiceProviderModule`, `HealthCheckableModule`, `EventEmitterModule`, `EventConsumerModule`, `PluginModule`, `UsePluginModule`
- **Registration**: `app.Register(module)` or `app.RegisterPlugin(plugin, alias)`

See [Module API](module.md) for detailed documentation.

### Service Container

Manages service registration and discovery within a module.

- **Interface**: `ServiceContainer`
- **Service Types**: Channel, Request-Reply, Queue Group, Stream Consumer, Cron
- **Registration**: `RegisterChannelService()`, `RegisterRequestReplyService()`, `RegisterQueueGroupService()`, `RegisterStreamConsumerService()`, `RegisterCronService()`
- **Consumption**: `GetChannelService()`, `GetRequestReplyService()`, `GetQueueGroupService()`, `GetStreamConsumerService()`

See [Service Container API](container.md) for detailed documentation.

### EventBus

Message bus for publish/subscribe communication between modules.

- **Interface**: `EventBus`
- **Publishing**: `Publish(subject, data)`, `PublishMsg(msg)`
- **Subscription**: `Subscribe()`, `QueueSubscribe()` for event consumers
- **JetStream**: `Stream()` for persistent event streams

See [EventBus API](eventbus.md) for detailed documentation.

### Storage

Unified storage interface for plugin storage backends.

- **Base Interface**: `Storage` (Get, Set, Delete, Reset, Close)
- **Extended Interfaces**: `StorageWithWatch`, `StorageWithRevision`, `StorageWithReader`, etc.
- **Data Types**: `ObjectInfo`, `Entry`, `BucketStatus`, `KeyWatcher`
- **Sentinel Errors**: `ErrKeyNotFound`, `ErrKeyExists`, `ErrRevisionMismatch`

See [Storage API](storage.md) for detailed documentation.

## Framework Configuration

All framework configuration is done through functional options passed to `NewMonoApplication()`:

```go
app, err := mono.NewMonoApplication(
    mono.WithNATSPort(4222),
    mono.WithLogLevel(mono.LogLevelInfo),
    mono.WithJetStreamStorageDir("./data"),
)
```

Categories of options:

- **NATS Configuration**: Host, port, in-process, clustering
- **JetStream Configuration**: Domain, storage directory
- **Logger Configuration**: Level, format, output
- **Framework Options**: Shutdown timeout, queue group optimization

See [Framework API](framework.md) for complete option list with descriptions and defaults.

## Type Safety

The framework provides type-safe event handlers for common operations:

- `TypedEventConsumerHandler[T]` - Generic event handler with automatic marshaling
- `TypedEventStreamConsumerHandler[T]` - Generic JetStream event handler
- `RequestReplyHandler` - Type-safe request-reply handler
- `QueueGroupHandler` - Type-safe queue group handler

## Error Handling

The framework follows Go's standard error handling patterns:

- All functions return `error` as the last return value
- Errors are wrapped with context using `fmt.Errorf` with `%w` verb
- Sentinel errors provide specific error types for programmatic handling

## Interfaces vs Implementations

The framework API emphasizes interfaces over concrete types:

- Consumer code depends on interfaces (`Module`, `ServiceContainer`, `EventBus`)
- Framework returns concrete implementations that satisfy these interfaces
- This enables testing with mock implementations and future backend swaps

## Navigation

- [Framework API](framework.md) - Application creation, configuration, and lifecycle
- [Module API](module.md) - Module interface definitions and lifecycle
- [Service Container API](container.md) - Service registration and discovery
- [EventBus API](eventbus.md) - Event publishing and subscription

## Common Patterns

### Creating an Application

```go
app, err := mono.NewMonoApplication(
    mono.WithNATSPort(4222),
    mono.WithJetStreamStorageDir("./data"),
)
if err != nil {
    panic(err)
}
defer app.Stop(context.Background())
```

### Implementing a Module

```go
type MyModule struct {
    eventBus mono.EventBus
}

func (m *MyModule) Name() string { return "mymodule" }

func (m *MyModule) SetEventBus(eventBus mono.EventBus) {
    m.eventBus = eventBus
}

func (m *MyModule) Start(ctx context.Context) error {
    slog.Info("Starting module")
    return nil
}

func (m *MyModule) Stop(ctx context.Context) error {
    slog.Info("Stopping module")
    return nil
}
```

### Registering Services

```go
func (m *MyModule) RegisterServices(container mono.ServiceContainer) error {
    return container.RegisterRequestReplyService(
        "get-user",
        m.handleGetUser,
    )
}

func (m *MyModule) handleGetUser(ctx context.Context, req *GetUserRequest, msg *mono.Msg) (*GetUserResponse, error) {
    return &GetUserResponse{...}, nil
}
```

### Publishing Events

```go
func (m *MyModule) emitUserCreated(ctx context.Context, userID string) error {
    return m.eventBus.PublishWithContext(ctx, "events.user.created", &UserCreatedEvent{
        UserID: userID,
    })
}
```

## Constants

### Log Levels

```go
const (
    LogLevelDebug = iota  // 0 - Detailed diagnostic information
    LogLevelInfo          // 1 - General informational messages
    LogLevelWarn          // 2 - Warning messages about potential problems
    LogLevelError         // 3 - Error messages about failures
)
```

### Log Formats

```go
const (
    LogFormatText = iota  // 0 - Human-readable text format
    LogFormatJSON         // 1 - JSON format for structured logging
)
```

### Framework States

```go
const (
    StateCreated  = iota  // 0 - Framework created, not started
    StateStarting         // 1 - Framework starting up
    StateRunning          // 2 - Framework running normally
    StateStopping         // 3 - Framework shutting down
    StateStopped          // 4 - Framework stopped
)
```

### Service Types

```go
const (
    ServiceTypeChannel = iota      // In-process Go channels
    ServiceTypeRequestReply        // NATS request-reply pattern
    ServiceTypeQueueGroup          // NATS queue group pattern
    ServiceTypeStreamConsumer      // JetStream durable consumer pattern
    ServiceTypeCron                // Server-scheduled cron pattern (JetStream)
)
```

## Related Documentation

- [Getting Started](../getting-started/README.md) - Tutorial for new users
- [Core Concepts](../core-concepts/README.md) - Architectural concepts and patterns
- [Middleware](../middleware/README.md) - Built-in middleware modules
- [Plugins](../plugins/README.md) - Plugin system and built-in plugins

---

For specific API details, see the individual API documentation pages.
