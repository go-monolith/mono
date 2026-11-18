# Plugin Module API

This page is the reference for the `PluginModule` and `UsePluginModule` interfaces, plugin registration on `MonoApplication`, and the order in which the framework starts and stops plugins, middleware and modules. Read it when you implement a plugin or a module that uses one. For what plugins are and when to use them, see [Plugins](../plugins/README.md); for a step-by-step guide, see [Creating Plugins](../plugins/creating-plugins.md).

## Overview

Plugins are special modules designed for shared infrastructure that other modules depend on. Unlike regular modules, plugins:

- **Start first, stop last** - Guaranteed initialization order
- **Excluded from dependency graph** - Don't participate in circular dependency checks
- **Have their own ServiceContainer** - Services registered in it are served over NATS at `services.<plugin name>.<service>`, after every module has started
- **Excluded from middleware hooks** - Raw access without interception
- **Registered with aliases** - Multiple instances of the same plugin type supported

## Signatures

```go
// PluginModule is implemented by every plugin.
type PluginModule interface {
    Module // Name(), Start(), Stop()
    SetContainer(container ServiceContainer)
    Container() ServiceContainer
}

// UsePluginModule is implemented by modules that receive plugins.
type UsePluginModule interface {
    Module
    SetPlugin(alias string, plugin PluginModule)
}

// Methods of MonoApplication
type MonoApplication interface {
    // ...
    RegisterPlugin(plugin PluginModule, alias string) error
    Plugin(alias string) PluginModule
}
```

## PluginModule Interface

### SetContainer

```go
SetContainer(container ServiceContainer)
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
Container() ServiceContainer
```

Returns the plugin's ServiceContainer. Request-reply, queue group, stream consumer and cron services registered in it are served over NATS at `services.<plugin name>.<service>` (the plugin's `Name()`, not its alias), and the routers of its channel services run, like a module's. They are subscribed after every module has started, so a module cannot call them from its own `Start()`. A plugin that registers services must not share its `Name()` with a module or with another plugin that registers services (two instances of one plugin type, for example): both would answer on the same subjects, so `app.Start()` fails. Modules reach these services through the plugin's container (`Container()`), and may list their contracts in `ConsumeServices()`.

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
SetPlugin(alias string, plugin PluginModule)
```

Called by the framework for each registered plugin, in no guaranteed order. Modules should filter by alias and store the plugins they need. Only regular modules receive plugins: `SetPlugin()` is not called on middleware or on other plugins.

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
    documents     fsjetstream.FileStoragePort
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
RegisterPlugin(plugin PluginModule, alias string) error
```

Registers a plugin with the framework under a specific alias. Must be called before `app.Start()`.

**Parameters:**
- `plugin` - The plugin instance implementing `PluginModule`
- `alias` - A unique identifier for this plugin instance

**Returns:**
- `error` - Returns an error if plugin is nil, alias is empty, alias is already registered (wraps `ErrPluginAlreadyRegistered`), or the application has already been started

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
if err := app.RegisterPlugin(primaryStorage, "primary-storage"); err != nil {
    log.Fatal(err)
}
if err := app.RegisterPlugin(backupStorage, "backup-storage"); err != nil {
    log.Fatal(err)
}
```

### Plugin

```go
Plugin(alias string) PluginModule
```

Returns the plugin registered under `alias`, or nil if there is none. Meant for host code (`main.go`, tests); modules receive plugins through `SetPlugin()`.

## Lifecycle Order

Plugins start first and stop last, so shared infrastructure (storage, caches) is fully initialized before any module needs it and remains available until all modules have stopped.

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

If a plugin's `Start()` fails, the plugins already started are stopped in reverse order and `app.Start()` returns the error.

### Shutdown Sequence

```
1. Regular modules stop (in reverse dependency order)
2. Middleware stops (in reverse registration order)
3. Plugins stop (in reverse registration order)
   - Plugin cleanup happens here
4. NATS connection closes
```

If `app.Start()` fails after the plugins started (a middleware or module `Start()` fails, the dependencies do not resolve, an event definition is rejected, or a subscription cannot be set up), nothing is left running: the modules started so far stop in reverse order, then the middleware that started in reverse order, then every plugin. A later `app.Start()` starts the plugins again.

## Built-in Plugins

| Plugin | Package | Backed by |
|--------|---------|-----------|
| [fs-jetstream](../plugins/fs-jetstream.md) | `github.com/go-monolith/mono/plugin/fs-jetstream` (`fsjetstream`) | JetStream ObjectStore (file storage) |
| [kv-jetstream](../plugins/kv-jetstream.md) | `github.com/go-monolith/mono/plugin/kv-jetstream` (`kvjetstream`) | JetStream KeyValue store |

For a complete application using a plugin, see [Example: Using the Storage Plugin](../plugins/README.md#example-using-the-storage-plugin); to implement one, see [Creating Plugins](../plugins/creating-plugins.md).

## Related Documentation

- [Plugins](../plugins/README.md) - When to use plugins, usage patterns, best practices
- [Creating Plugins](../plugins/creating-plugins.md) - Detailed plugin creation guide
- [Core Concepts: Modules](../core-concepts/modules.md) - Module interface hierarchy
- [fs-jetstream Plugin](../plugins/fs-jetstream.md) - File storage plugin
- [kv-jetstream Plugin](../plugins/kv-jetstream.md) - Key-value storage plugin
