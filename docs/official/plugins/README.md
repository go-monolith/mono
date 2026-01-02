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
1. Framework initializes NATS
2. Plugins start (in registration order)
   - SetContainer() called with dedicated ServiceContainer
   - Start() called for plugin initialization
3. SetPlugin() called on modules implementing UsePluginModule
4. Middleware starts
5. Regular modules start (respecting dependency order)
```

### Shutdown Sequence

```
1. Regular modules stop (in reverse dependency order)
2. Middleware stops (in reverse registration order)
3. Plugins stop (in reverse registration order)
   - Plugin cleanup happens here
4. NATS connection closes
```

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
app, _ := mono.NewMonoApplication()

storage, _ := fsjetstream.New(config)
app.RegisterPlugin(storage, "storage")

app.Register(&DocumentModule{})  // Can access "storage" plugin
```

### Multiple Plugin Instances

Register same plugin type with different aliases:

```go
app, _ := mono.NewMonoApplication()

primary, _ := fsjetstream.New(primaryConfig)
app.RegisterPlugin(primary, "primary")

backup, _ := fsjetstream.New(backupConfig)
app.RegisterPlugin(backup, "backup")

app.Register(&BackupModule{})  // Can access both "primary" and "backup"
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

Plugins start before middleware, so middleware can use plugin services:

```go
app.Register(requestid.New())           // Middleware
app.RegisterPlugin(storage, "storage")  // Plugin

app.Register(accesslog.New())           // Middleware uses storage
app.Register(&AppModule{})              // Module uses storage
```

### With Services

Plugins can register services in their ServiceContainer for other modules to access:

```go
func (p *StoragePlugin) Start(ctx context.Context) error {
    // ... plugin initialization ...

    // Register services in plugin's container
    p.container.RegisterRequestReplyService("storage", p.handleStorageRequest)

    return nil
}
```

### With Events

Plugins are EventBus-aware and can publish/subscribe to events:

```go
func (p *StoragePlugin) Start(ctx context.Context) error {
    // Access EventBus through ServiceContainer
    eventBus := p.container.GetService("event-bus")

    // Subscribe to events
    eventBus.On("document.uploaded", p.handleDocumentUpload)

    return nil
}
```

## Example: Using the Storage Plugin

```go
func main() {
    ctx := context.Background()

    // Create application
    app, _ := mono.NewMonoApplication(
        mono.WithJetStreamDomain("default"),
    )

    // Register storage plugin
    storage, _ := fsjetstream.New(fsjetstream.Config{
        Buckets: []fsjetstream.BucketConfig{
            {Name: "documents", MaxBytes: 1_000_000_000},
        },
    })
    app.RegisterPlugin(storage, "storage")

    // Register modules that use storage
    app.Register(&DocumentModule{})
    app.Register(&MediaModule{})

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
