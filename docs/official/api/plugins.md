# Plugin Module API

API documentation for the `PluginModule` and `UsePluginModule` interfaces, which enable shared infrastructure components that start before all other modules.

## Signatures

```go
// PluginModule interface
type PluginModule interface {
    Module
    SetContainer(container ServiceContainer)
    Container() ServiceContainer
}

// UsePluginModule interface
type UsePluginModule interface {
    Module
    SetPlugin(alias string, plugin PluginModule)
}

// Framework registration
func (app MonoApplication) RegisterPlugin(plugin PluginModule, alias string) error
```

{% hint style="info" %}
**Lifecycle Order:** Plugins start first and stop last. This guarantees that shared infrastructure (storage, caches) is ready before any module needs it.
{% endhint %}

## Overview

Plugins are special modules designed for shared infrastructure that other modules depend on. Unlike regular modules, plugins:

- **Start first, stop last** - Guaranteed initialization order
- **Excluded from dependency graph** - Don't participate in circular dependency checks
- **Have their own ServiceContainer** - Can register services other modules access
- **Excluded from middleware hooks** - Raw access without interception
- **Registered with aliases** - Multiple instances of the same plugin type supported

## PluginModule Interface

### SetContainer

```go
func (plugin PluginModule) SetContainer(container ServiceContainer)
```

Called by the framework to inject the plugin's dedicated ServiceContainer during initialization.

**Parameters:**
- `container` - The plugin's ServiceContainer (already bound to the plugin)

**Timing:**
- Called before `Start()`
- Called before middleware modules start
- EventBus is already set on the container

**Example:**
```go
type StoragePlugin struct {
    container mono.ServiceContainer
}

func (p *StoragePlugin) SetContainer(container mono.ServiceContainer) {
    p.container = container
}
```

### Container

```go
func (plugin PluginModule) Container() ServiceContainer
```

Returns the plugin's ServiceContainer, allowing other modules to access the plugin's registered services.

**Returns:**
- `ServiceContainer` - The plugin's service container

**Example:**
```go
func (p *StoragePlugin) Container() mono.ServiceContainer {
    return p.container
}
```

## UsePluginModule Interface

### SetPlugin

```go
func (module UsePluginModule) SetPlugin(alias string, plugin PluginModule)
```

Called by the framework for each registered plugin. Modules should filter by alias and store the plugins they need.

**Parameters:**
- `alias` - The alias used when registering the plugin
- `plugin` - The plugin instance

**Timing:**
- Called after `BindModule()`
- Called before `SetDependencyServiceContainer()`
- Called before `SetEventBus()`
- Called before `RegisterServices()`
- Called before `Start()`

{% hint style="warning" %}
**Required Plugin Verification:** Always verify that required plugins were injected before using them in `Start()`. If a plugin is not registered, the reference will be nil.
{% endhint %}

**Example:**
```go
type DocumentModule struct {
    storagePlugin *fsjetstream.PluginModule
}

func (m *DocumentModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "storage" {
        // Type-assert to the specific plugin type
        m.storagePlugin = plugin.(*fsjetstream.PluginModule)
    }
}

func (m *DocumentModule) Start(ctx context.Context) error {
    // Verify required plugin was injected
    if m.storagePlugin == nil {
        return fmt.Errorf("required plugin 'storage' not registered")
    }

    // Access plugin functionality
    m.documents = m.storagePlugin.Bucket("documents")
    return nil
}
```

## Framework Registration

### RegisterPlugin

```go
func (app MonoApplication) RegisterPlugin(plugin PluginModule, alias string) error
```

Registers a plugin with the framework under a specific alias.

**Parameters:**
- `plugin` - The plugin instance implementing `PluginModule`
- `alias` - A unique identifier for this plugin instance

**Returns:**
- `error` - Returns error if alias is empty or already registered

{% hint style="info" %}
**Multiple Instances:** The alias mechanism allows registering multiple instances of the same plugin type with different configurations. For example, "primary-storage" and "backup-storage" can both use the same `fsjetstream.PluginModule` implementation.
{% endhint %}

**Example:**
```go
// Create plugin instances
primaryStorage, _ := fsjetstream.New(fsjetstream.Config{
    Buckets: []fsjetstream.BucketConfig{{Name: "primary-docs"}},
})

backupStorage, _ := fsjetstream.New(fsjetstream.Config{
    Buckets: []fsjetstream.BucketConfig{{Name: "backup-docs"}},
})

// Register with different aliases
app.RegisterPlugin(primaryStorage, "primary-storage")
app.RegisterPlugin(backupStorage, "backup-storage")
```

## Lifecycle Order

Plugins follow a strict lifecycle order relative to other module types:

### Startup Order

```
1. Plugins          (in registration order)
2. Middleware       (in registration order)
3. Regular Modules  (in dependency order)
```

### Shutdown Order

```
1. Regular Modules  (reverse dependency order)
2. Middleware       (reverse registration order)
3. Plugins          (reverse registration order)
```

This guarantees:
- Plugins are fully initialized before any module needs them
- Plugins remain available until all modules have stopped

## Implementing a Plugin

### Step 1: Define the Plugin Struct

```go
package myplugin

import (
    "context"
    mono "github.com/go-monolith/mono"
)

type MyPlugin struct {
    container mono.ServiceContainer
    // Plugin-specific fields
}

// Compile-time interface check
var _ mono.PluginModule = (*MyPlugin)(nil)
```

### Step 2: Implement Required Methods

```go
func (p *MyPlugin) Name() string {
    return "my-plugin"
}

func (p *MyPlugin) SetContainer(container mono.ServiceContainer) {
    p.container = container
}

func (p *MyPlugin) Container() mono.ServiceContainer {
    return p.container
}

func (p *MyPlugin) Start(ctx context.Context) error {
    // Initialize plugin resources
    return nil
}

func (p *MyPlugin) Stop(ctx context.Context) error {
    // Cleanup plugin resources
    return nil
}
```

### Step 3: Add Plugin-Specific Methods

```go
// Expose plugin functionality to consuming modules
func (p *MyPlugin) GetResource(name string) Resource {
    // Return plugin-managed resource
}
```

### Step 4: Register and Use

```go
// In main.go
plugin := myplugin.New(config)
app.RegisterPlugin(plugin, "my-plugin")

// In consuming module
func (m *MyModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "my-plugin" {
        m.myPlugin = plugin.(*myplugin.MyPlugin)
    }
}
```

## Built-in Plugins

### fs-jetstream

File storage plugin using JetStream ObjectStore.

```go
import "github.com/go-monolith/mono/plugin/fs-jetstream"

storage, err := fsjetstream.New(fsjetstream.Config{
    Buckets: []fsjetstream.BucketConfig{
        {Name: "documents", MaxBytes: 100 * 1024 * 1024},
        {Name: "images", TTL: 24 * time.Hour},
    },
})

app.RegisterPlugin(storage, "storage")
```

See [fs-jetstream Plugin](../plugins/fs-jetstream.md) for detailed documentation.

### kv-jetstream

Key-value storage plugin using JetStream KeyValue store.

```go
import "github.com/go-monolith/mono/plugin/kv-jetstream"

kvStore, err := kvjetstream.New(kvjetstream.Config{
    Buckets: []kvjetstream.BucketConfig{
        {Name: "sessions", TTL: 30 * time.Minute},
        {Name: "cache", MaxBytes: 50 * 1024 * 1024},
    },
})

app.RegisterPlugin(kvStore, "kv")
```

See [kv-jetstream Plugin](../plugins/kv-jetstream.md) for detailed documentation.

## When to Use Plugins

### Use Plugins For

- **Shared infrastructure** - Storage, caching, external service clients
- **Resources requiring early initialization** - Must be ready before modules start
- **Multiple instances** - Same resource type with different configurations
- **Bypass middleware** - Low-level operations without interception

### Use Regular Modules For

- **Business logic** - Domain-specific functionality
- **Service providers** - Services called by other modules
- **Event emitters/consumers** - Event-driven communication
- **Dependency relationships** - When startup order matters based on dependencies

## Best Practices

### Do

- **Verify plugins in Start()** - Check if required plugins were injected
- **Use descriptive aliases** - "primary-storage" vs "s1"
- **Type-assert to specific type** - Access plugin-specific methods
- **Document plugin requirements** - State which plugins a module needs

### Don't

- **Don't use plugins for business logic** - Use regular modules instead
- **Don't assume plugin exists** - Always check for nil before using
- **Don't register after Start()** - Plugins must be registered before `app.Start()`
- **Don't create circular dependencies** - Plugins shouldn't depend on regular modules

## Complete Example

```go
package main

import (
    "context"
    "fmt"
    "log"

    mono "github.com/go-monolith/mono"
    "github.com/go-monolith/mono/plugin/fs-jetstream"
)

// DocumentModule uses the storage plugin
type DocumentModule struct {
    storage *fsjetstream.PluginModule
}

func (m *DocumentModule) Name() string { return "documents" }

func (m *DocumentModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "storage" {
        m.storage = plugin.(*fsjetstream.PluginModule)
    }
}

func (m *DocumentModule) Start(ctx context.Context) error {
    if m.storage == nil {
        return fmt.Errorf("required plugin 'storage' not registered")
    }

    // Access the documents bucket
    docs := m.storage.Bucket("documents")
    if docs == nil {
        return fmt.Errorf("bucket 'documents' not configured")
    }

    return nil
}

func (m *DocumentModule) Stop(ctx context.Context) error {
    return nil
}

func main() {
    // Create application
    app, _ := mono.New()

    // Create and register storage plugin
    storage, _ := fsjetstream.New(fsjetstream.Config{
        Buckets: []fsjetstream.BucketConfig{
            {Name: "documents"},
        },
    })
    app.RegisterPlugin(storage, "storage")

    // Register module that uses the plugin
    app.Register(&DocumentModule{})

    // Start application
    if err := app.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

## Related Documentation

- [Core Concepts: Modules](../core-concepts/modules.md) - Module interface hierarchy
- [Creating Plugins](../plugins/creating-plugins.md) - Detailed plugin creation guide
- [fs-jetstream Plugin](../plugins/fs-jetstream.md) - File storage plugin
- [kv-jetstream Plugin](../plugins/kv-jetstream.md) - Key-value storage plugin
