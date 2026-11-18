# 🧬 Plugins

This page explains what plugins are, when to use one, and how a module uses a plugin. Read it before you add storage or another shared resource to an application. For the exact interfaces and the start and stop order, see the [Plugin Module API](../api/plugins.md). To write your own plugin, see [Creating Plugins](creating-plugins.md).

Plugins are specialized modules that extend the framework with capabilities such as persistent storage, caching or custom integrations. Unlike regular modules, plugins start first and stop last, so they are ready before any module starts and stay up until all modules have stopped.

## Overview

Plugins are:

- **Stateful Infrastructure**: Manage external resources (databases, storage, caches)
- **Lifecycle-Aware**: Start first, stop last; the framework manages the initialization order
- **Multi-Instance Capable**: Register multiple instances with different aliases and configurations
- **Framework Integrated**: Receive their own ServiceContainer for managing internal services

A plugin implements `PluginModule`; a module that wants plugins implements `UsePluginModule` and receives every registered plugin through `SetPlugin(alias, plugin)`. Both interfaces, the full startup sequence and the shutdown order are documented in the [Plugin Module API](../api/plugins.md).

## When to Use Plugins

Use plugins for:

- **External Storage**: Databases, file systems, object stores
- **Caching Layers**: In-memory caches, distributed caches
- **Cross-Cutting Concerns**: Monitoring, tracing, security
- **Infrastructure Services**: Connection pools, resource managers, external service clients
- **System Integration**: Hardware, cloud services, APIs
- **Multiple instances** of the same resource type with different configurations

Use regular modules instead for:

- Business logic (domain-specific functionality)
- Service providers: services called by other modules
- Event emitters and consumers
- Simple inter-module communication or data passing (use services and events)
- Startup order that depends on other modules (declare `Dependencies()`)

Don't use plugins for one-time setup either: use framework options, environment variables or module initialization.

## Built-in Plugins

The framework includes two production-ready plugins:

| Plugin | Backed by | Use case | Features |
|--------|-----------|----------|----------|
| [File Storage (fs-jetstream)](fs-jetstream.md) | JetStream ObjectStore | Documents, media, large binary files | Bucket-based organization, TTL support, streaming |
| [Key-Value Storage (kv-jetstream)](kv-jetstream.md) | JetStream KV Store | Caching, sessions, configuration | Revision-based locking, watch notifications, TTL support |

Both need JetStream, which is enabled with `mono.WithJetStreamStorageDir(...)`.

## Usage Patterns

### Single Plugin Instance

Register a plugin with one alias:

```go
app, err := mono.NewMonoApplication(mono.WithJetStreamStorageDir("./data/jetstream"))
if err != nil {
    log.Fatal(err)
}

storage, err := fsjetstream.New(config)
if err != nil {
    log.Fatal(err)
}
if err := app.RegisterPlugin(storage, "storage"); err != nil {
    log.Fatal(err)
}

if err := app.Register(&DocumentModule{}); err != nil { // Can access "storage" plugin
    log.Fatal(err)
}
```

### Multiple Plugin Instances

Register the same plugin type with different aliases:

```go
app, err := mono.NewMonoApplication(mono.WithJetStreamStorageDir("./data/jetstream"))
if err != nil {
    log.Fatal(err)
}

primary, err := fsjetstream.New(primaryConfig)
if err != nil {
    log.Fatal(err)
}
if err := app.RegisterPlugin(primary, "primary"); err != nil {
    log.Fatal(err)
}

backup, err := fsjetstream.New(backupConfig)
if err != nil {
    log.Fatal(err)
}
if err := app.RegisterPlugin(backup, "backup"); err != nil {
    log.Fatal(err)
}

if err := app.Register(&BackupModule{}); err != nil { // Can access both "primary" and "backup"
    log.Fatal(err)
}
```

### Plugin Configuration

Plugins accept configuration during creation:

```go
storage, err := fsjetstream.New(fsjetstream.Config{
    Buckets: []fsjetstream.BucketConfig{
        {
            Name:     "documents",
            MaxBytes: 1_000_000_000,  // 1GB
            TTL:      30 * 24 * time.Hour,
        },
    },
})
```

## Example: Using the Storage Plugin

A module receives the plugin in `SetPlugin`, type-asserts it to the plugin's type, and checks in `Start` that it was injected:

```go
func main() {
    ctx := context.Background()

    // Create application with JetStream enabled
    app, err := mono.NewMonoApplication(
        mono.WithJetStreamStorageDir("./data/jetstream"),
    )
    if err != nil {
        panic(err)
    }

    // Register storage plugin
    storage, err := fsjetstream.New(fsjetstream.Config{
        Buckets: []fsjetstream.BucketConfig{
            {Name: "documents", MaxBytes: 1_000_000_000},
        },
    })
    if err != nil {
        panic(err)
    }
    if err := app.RegisterPlugin(storage, "storage"); err != nil {
        panic(err)
    }

    // Register modules that use storage
    if err := app.Register(&DocumentModule{}); err != nil {
        panic(err)
    }
    if err := app.Register(&MediaModule{}); err != nil {
        panic(err)
    }

    // Start application
    if err := app.Start(ctx); err != nil {
        panic(err)
    }
}

type DocumentModule struct {
    storage *fsjetstream.PluginModule
    docs    fsjetstream.FileStoragePort
}

func (m *DocumentModule) Name() string { return "documents" }

// Receive the storage plugin
func (m *DocumentModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "storage" {
        m.storage = plugin.(*fsjetstream.PluginModule)
    }
}

func (m *DocumentModule) Start(ctx context.Context) error {
    // Verify the required plugin was injected
    if m.storage == nil {
        return fmt.Errorf("required plugin 'storage' not registered")
    }

    // Get bucket from plugin
    m.docs = m.storage.Bucket("documents")
    if m.docs == nil {
        return fmt.Errorf("bucket 'documents' not found")
    }

    // Use the storage
    info, err := m.docs.Put(ctx, "hello.txt", []byte("Hello!"))
    if err != nil {
        return err
    }

    fmt.Printf("Stored: %s\n", info.Name)
    return nil
}

func (m *DocumentModule) Stop(ctx context.Context) error {
    return nil
}
```

## Plugins and Other Components

### With Middleware

Plugins start before middleware, whatever the registration order, but middleware does not receive plugins through `SetPlugin()`; only regular modules implementing `UsePluginModule` do:

```go
rid, err := requestid.New()
if err != nil {
    log.Fatal(err)
}
al, err := accesslog.New()
if err != nil {
    log.Fatal(err)
}

_ = app.Register(al)                       // Middleware
_ = app.RegisterPlugin(storage, "storage") // Plugin: starts first
_ = app.Register(rid)                      // Middleware: after accesslog (see middleware ordering)
_ = app.Register(&AppModule{})             // Module: receives "storage" via SetPlugin
```

### With Services

A plugin receives its own ServiceContainer, and `RegisterServices()` is called on plugins that implement `ServiceProviderModule`. The services registered there are served over NATS at `services.<plugin name>.<service>` once every module has started, so modules call them through the plugin's `Container()` (not from their own `Start()`); see [Container](../api/plugins.md#container) for the naming rule. For in-process functionality, Go methods on the plugin type are simpler, as the built-in plugins do with `Bucket()`:

```go
func (p *StoragePlugin) Bucket(name string) Bucket {
    return p.buckets[name]
}
```

### With Events

Plugins that implement `EventBusAwareModule` receive the shared (unscoped) EventBus before `Start()`, and can publish and subscribe:

```go
func (p *StoragePlugin) SetEventBus(bus mono.EventBus) {
    p.bus = bus
}

func (p *StoragePlugin) Start(ctx context.Context) error {
    // Subscribe to events
    sub, err := p.bus.Subscribe("events.documents.uploaded", p.handleDocumentUpload)
    if err != nil {
        return fmt.Errorf("subscribe: %w", err)
    }
    p.sub = sub
    return nil
}

func (p *StoragePlugin) handleDocumentUpload(ctx context.Context, msg *mono.Msg) {
    // ...
}
```

## Best Practices

✓ **Do**
- Register every plugin before `app.Start()`; registering after Start fails
- Implement `UsePluginModule` in modules that need plugins, and type-assert to the plugin's type to reach its methods
- Verify in `Start()` that required plugins were injected and return an error if not: a plugin that is not registered leaves the reference nil
- Give plugins meaningful aliases ("primary-storage", not "s1")
- Document which plugins a module needs

✗ **Don't**
- Implement business logic in plugins
- Make plugins depend on regular modules
- Register many small plugins (combine them into one plugin)

## Related Documentation

- [Plugin Module API](../api/plugins.md) - Interfaces, registration, lifecycle order
- [File Storage Plugin (fs-jetstream)](fs-jetstream.md)
- [Key-Value Storage Plugin (kv-jetstream)](kv-jetstream.md)
- [Creating Custom Plugins](creating-plugins.md)
- [Core Concepts - Modules](../core-concepts/modules.md)
- [Middleware System](../middleware/README.md)
