# 🧬 Plugins

Plugins are specialized modules that extend the framework with additional capabilities like persistent storage, caching, or custom integrations. Unlike regular modules, plugins start first and stop last, ensuring their services are always available to other modules.

## Overview

The plugin system enables the framework to be extended with cross-cutting concerns and infrastructure services. Plugins are:

- **Stateful Infrastructure**: Manage external resources (databases, storage, caches)
- **Lifecycle-Aware**: Start first, stop last; framework manages initialization order
- **Multi-Instance Capable**: Register multiple instances with different aliases and configurations
- **Framework Integrated**: Receive their own ServiceContainer for managing internal services

## Key Concepts

### PluginModule Interface

All plugins implement the `PluginModule` interface:

```go
type PluginModule interface {
    Module  // Implements Name(), Start(), Stop()

    // SetContainer receives the plugin's dedicated ServiceContainer
    SetContainer(container ServiceContainer)

    // Container returns the plugin's ServiceContainer
    Container() ServiceContainer
}
```

### UsePluginModule Interface

Modules that want to use plugins implement `UsePluginModule`:

```go
type UsePluginModule interface {
    Module

    // SetPlugin receives each registered plugin
    SetPlugin(alias string, plugin PluginModule)
}
```

## Plugin Lifecycle

### Startup Sequence

```
1. NATS server started (in NewMonoApplication); Start waits until JetStream answers
2. Plugins start (in registration order), each:
   - SetContainer() with its dedicated ServiceContainer
   - SetEventBus() if it implements EventBusAwareModule (shared, unscoped bus)
   - RegisterServices() if it implements ServiceProviderModule
   - Start()
3. Middleware starts (in registration order)
4. Regular modules start (in dependency order), each:
   - ServiceContainer bound
   - SetPlugin() for every registered plugin, if it implements UsePluginModule
   - SetDependencyServiceContainer(), SetEventBus(), RegisterServices()
   - Start()
```

### Shutdown Sequence

```
1. Regular modules stop (in reverse dependency order)
2. Middleware stops (in reverse registration order)
3. Plugins stop (in reverse registration order)
   - Plugin cleanup happens here
4. NATS connection closes
```

If a regular module fails to start, the modules started before it are rolled back, but plugins (and middleware) that already started are not stopped: their `Stop()` is not called.

## When to Use Plugins

Use plugins for:

- **External Storage**: Databases, file systems, object stores
- **Caching Layers**: In-memory caches, distributed caches
- **Cross-Cutting Concerns**: Monitoring, tracing, security
- **Infrastructure Services**: Connection pools, resource managers
- **System Integration**: Hardware, cloud services, APIs

Don't use plugins for:

- Business logic modules (use regular modules instead)
- One-time setup (use framework options or module initialization)
- Simple inter-module communication (use services and events)

## Built-in Plugins

The framework includes two production-ready plugins:

### File Storage Plugin (fs-jetstream)

Provides persistent file and object storage using JetStream ObjectStore.

- **Use Case**: Documents, media, large binary files
- **Features**: Bucket-based organization, TTL support, streaming
- **Location**: `docs/official/plugins/fs-jetstream.md`

### Key-Value Storage Plugin (kv-jetstream)

Provides fast key-value storage using JetStream KV Store.

- **Use Case**: Caching, sessions, configuration
- **Features**: Revision-based locking, watch notifications, TTL support
- **Location**: `docs/official/plugins/kv-jetstream.md`

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

Register same plugin type with different aliases:

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

## Creating Custom Plugins

See [Creating Plugins](creating-plugins.md) for a complete guide to building your own plugins.

## Best Practices

✓ **Do**
- Register plugins before regular modules
- Implement UsePluginModule in modules that need plugins
- Check for nil plugin references in Start()
- Use plugins for infrastructure, not business logic
- Alias plugins with meaningful names
- Handle plugin startup failures gracefully

✗ **Don't**
- Implement business logic in plugins
- Register plugins after modules start
- Assume plugins are always present (check nil)
- Use plugins for simple data passing (use services instead)
- Register many small plugins (combine into one plugin)
- Ignore plugin lifecycle management

## Integration with Other Components

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

_ = app.Register(rid)                      // Middleware
_ = app.RegisterPlugin(storage, "storage") // Plugin: starts first
_ = app.Register(al)                       // Middleware
_ = app.Register(&AppModule{})             // Module: receives "storage" via SetPlugin
```

### With Services

A plugin receives its own ServiceContainer, and `RegisterServices()` is called on plugins that implement `ServiceProviderModule`. However, services registered in a plugin's container are not currently served over NATS (no subscription is created for them), so a call to them gets no responder. Expose plugin functionality through Go methods on the plugin type instead, as the built-in plugins do with `Bucket()`:

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

## Example: Using the Storage Plugin

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

## Related Documentation

- [File Storage Plugin (fs-jetstream)](fs-jetstream.md)
- [Key-Value Storage Plugin (kv-jetstream)](kv-jetstream.md)
- [Creating Custom Plugins](creating-plugins.md)
- [Core Concepts - Modules](../core-concepts/modules.md)
- [Middleware System](../middleware/README.md)

---

For building your own plugins, see [Creating Plugins](creating-plugins.md).
