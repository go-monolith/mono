# Creating Custom Plugins

This guide builds one plugin from start to finish: a `customplugin` package that wraps a connection to an external service, exposes it to modules through a small interface (a port), and is registered, used and tested in an application. Read it when no built-in plugin fits and you need your own shared infrastructure component. For whether a plugin is the right tool, see [When to Use Plugins](README.md#when-to-use-plugins); for the interfaces and the lifecycle order the framework applies, see the [Plugin Module API](../api/plugins.md).

## Plugin Architecture

Plugins follow a standard structure with three layers:

```
PluginModule (implements PluginModule interface)
    └── Public API (consumer-facing interfaces)
            └── Internal Implementation
                    └── External Resources (NATS, databases, etc.)
```

In the example, `MyPlugin` is the plugin module, `DataPort` is the public API, `adapter` is the internal implementation, and `Connection` stands for the client of your external resource.

## Step 1: Define Configuration and Constructor

Validate configuration early, in `New()`, so a bad configuration fails before the application starts. The framework does not inject a logger into plugins: accept one through an option, as the built-in plugins do with `WithLogger(*slog.Logger)`, and default to `slog.Default()`.

```go
package customplugin

import (
    "context"
    "fmt"
    "log/slog"
    "time"

    mono "github.com/go-monolith/mono"
)

// Config holds plugin configuration
type Config struct {
    ConnectionString string
    MaxConnections   int
    Timeout          time.Duration
}

// MyPlugin is the plugin module
type MyPlugin struct {
    name      string
    config    Config
    container mono.ServiceContainer
    conn      *Connection
    logger    *slog.Logger
}

// Option configures the plugin
type Option func(*MyPlugin)

// WithLogger sets the plugin's logger
func WithLogger(logger *slog.Logger) Option {
    return func(p *MyPlugin) { p.logger = logger }
}

// New creates the plugin
func New(config Config, opts ...Option) (*MyPlugin, error) {
    // Validate required fields
    if config.ConnectionString == "" {
        return nil, fmt.Errorf("connection string required")
    }

    // Validate ranges
    if config.MaxConnections < 1 || config.MaxConnections > 1000 {
        return nil, fmt.Errorf("max connections must be 1-1000, got %d", config.MaxConnections)
    }

    p := &MyPlugin{
        name:   "my-plugin",
        config: config,
        logger: slog.Default(),
    }
    for _, opt := range opts {
        opt(p)
    }
    return p, nil
}
```

## Step 2: Implement the Module Interface

`Start()` acquires the resource and `Stop()` releases it. Wrap errors with context, and bound the cleanup in `Stop()` with a timeout:

```go
func (p *MyPlugin) Name() string {
    return p.name
}

func (p *MyPlugin) Start(ctx context.Context) error {
    p.logger.Info("Initializing plugin", "name", p.Name())

    // Initialize connection
    conn, err := NewConnection(ctx, &ConnectionConfig{
        URL:      p.config.ConnectionString,
        MaxConns: p.config.MaxConnections,
        Timeout:  p.config.Timeout,
    })
    if err != nil {
        return fmt.Errorf("failed to connect: %w", err)
    }

    p.conn = conn

    // Verify connection works
    if err := p.conn.Ping(ctx); err != nil {
        return fmt.Errorf("failed to ping: %w", err)
    }

    p.logger.Info("Plugin initialized successfully")
    return nil
}

func (p *MyPlugin) Stop(ctx context.Context) error {
    if p.conn != nil {
        // Use timeout for cleanup
        shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
        defer cancel()

        if err := p.conn.Close(shutdownCtx); err != nil {
            return fmt.Errorf("failed to close connection: %w", err)
        }
    }
    return nil
}
```

## Step 3: Implement the PluginModule Interface

Store the container the framework injects and return it from `Container()`. A compile-time check catches a missing method:

```go
// Compile-time interface check
var _ mono.PluginModule = (*MyPlugin)(nil)

func (p *MyPlugin) SetContainer(container mono.ServiceContainer) {
    p.container = container
}

func (p *MyPlugin) Container() mono.ServiceContainer {
    return p.container
}
```

If the plugin cannot work without JetStream, also implement `RequiresJetStream`. Without JetStream, `app.Start` then fails before the plugin starts, with `ErrJetStreamNotEnabled`, instead of at its first JetStream call:

```go
func (p *MyPlugin) RequiresJetStream() bool { return true }
```

## Step 4: Define Your Public API

Give consumers an interface (a port) that hides the implementation, and return it from a method on the plugin:

```go
// DataPort defines the interface consumers will use
type DataPort interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Set(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
}

// Port returns the public API
func (p *MyPlugin) Port() DataPort {
    return &adapter{conn: p.conn}
}

type adapter struct {
    conn *Connection
}

func (a *adapter) Get(ctx context.Context, key string) ([]byte, error) {
    return a.conn.Retrieve(ctx, key)
}

func (a *adapter) Set(ctx context.Context, key string, value []byte) error {
    return a.conn.Store(ctx, key, value)
}

func (a *adapter) Delete(ctx context.Context, key string) error {
    return a.conn.Remove(ctx, key)
}
```

## Step 5: Use Your Plugin in a Module

The module implements `UsePluginModule`, keeps the plugin registered under its alias, and checks in `Start()` that it was injected:

```go
type MyModule struct {
    plugin *MyPlugin
    data   DataPort
}

func (m *MyModule) Name() string {
    return "mymodule"
}

// Receive the plugin
func (m *MyModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "my-plugin" {
        m.plugin = plugin.(*MyPlugin)
    }
}

func (m *MyModule) Start(ctx context.Context) error {
    // Verify plugin was injected
    if m.plugin == nil {
        return fmt.Errorf("required plugin 'my-plugin' not registered")
    }

    // Get the public API
    m.data = m.plugin.Port()

    // Use the plugin
    err := m.data.Set(ctx, "greeting", []byte("Hello!"))
    if err != nil {
        return fmt.Errorf("failed to set value: %w", err)
    }

    value, err := m.data.Get(ctx, "greeting")
    if err != nil {
        return fmt.Errorf("failed to get value: %w", err)
    }

    fmt.Printf("Retrieved: %s\n", string(value))
    return nil
}

func (m *MyModule) Stop(ctx context.Context) error {
    return nil
}
```

## Step 6: Register and Run

```go
func main() {
    ctx := context.Background()

    // Create application
    app, err := mono.NewMonoApplication()
    if err != nil {
        panic(err)
    }

    // Create and register plugin
    plugin, err := customplugin.New(customplugin.Config{
        ConnectionString: "myservice://localhost:9000",
        MaxConnections:   10,
        Timeout:          5 * time.Second,
    })
    if err != nil {
        panic(err)
    }

    if err := app.RegisterPlugin(plugin, "my-plugin"); err != nil {
        panic(err)
    }

    // Register modules that use the plugin
    if err := app.Register(&customplugin.MyModule{}); err != nil {
        panic(err)
    }

    // Start application
    if err := app.Start(ctx); err != nil {
        panic(err)
    }

    // Application runs here...

    // Shutdown
    if err := app.Stop(ctx); err != nil {
        panic(err)
    }
}
```

## Step 7: Test Your Plugin

Give the plugin an in-memory container from package `monotest` and exercise its port:

```go
func TestPluginStart(t *testing.T) {
    ctx := context.Background()

    // Create plugin with test config
    plugin, err := New(Config{
        ConnectionString: "test://localhost:9000",
        MaxConnections:   1,
    })
    if err != nil {
        t.Fatalf("Failed to create plugin: %v", err)
    }

    // Give the plugin an in-memory container (package monotest)
    bus := monotest.NewEventBus()
    plugin.SetContainer(monotest.NewContainer(plugin.Name(), bus))

    // Start plugin
    if err := plugin.Start(ctx); err != nil {
        t.Fatalf("Failed to start plugin: %v", err)
    }

    // Test plugin operations
    port := plugin.Port()
    data, err := port.Get(ctx, "test-key")
    if err != nil {
        t.Fatalf("Failed to get value: %v", err)
    }

    if string(data) != "expected" {
        t.Errorf("Expected 'expected', got '%s'", string(data))
    }

    // Stop plugin
    if err := plugin.Stop(ctx); err != nil {
        t.Fatalf("Failed to stop plugin: %v", err)
    }
}
```

To test a module that uses the plugin, inject it with `monotest.WithPlugin(alias, plugin)`; the harness calls `SetPlugin` as the framework does (it does not start the plugin):

```go
func TestMyModule(t *testing.T) {
    plugin, _ := New(Config{ConnectionString: "test://localhost:9000", MaxConnections: 1})
    plugin.SetContainer(monotest.NewContainer(plugin.Name(), monotest.NewEventBus()))
    if err := plugin.Start(context.Background()); err != nil {
        t.Fatal(err)
    }

    monotest.CheckModule(t, &MyModule{}, monotest.WithPlugin("my-plugin", plugin))
}
```

## Advanced Patterns

### Multiple Ports

Expose multiple interfaces from one plugin:

```go
func (p *MyPlugin) DataPort() DataPort { /* ... */ }
func (p *MyPlugin) AdminPort() AdminPort { /* ... */ }
```

### Service Registration

A plugin that implements `ServiceProviderModule` has `RegisterServices()` called on its own container before `Start()`. They are served over NATS at `services.<plugin name>.<service>` once every module has started (see [Container](../api/plugins.md#container)). For in-process functionality, Go methods (ports), as shown above, are simpler.

### Events

A plugin that needs the event bus implements `EventBusAwareModule`; see [Plugins and Other Components](README.md#with-events).

### Plugin Dependencies

Plugins do not receive other plugins: `SetPlugin()` is not called on plugins. If your plugin depends on another plugin, pass it to the constructor and register the dependency first (plugins start in registration order):

```go
type MyPlugin struct {
    otherPlugin *OtherPlugin
}

func New(config Config, other *OtherPlugin) (*MyPlugin, error) {
    if other == nil {
        return nil, fmt.Errorf("other plugin required")
    }
    return &MyPlugin{otherPlugin: other}, nil
}

// main.go
_ = app.RegisterPlugin(other, "other-plugin") // starts first
_ = app.RegisterPlugin(mine, "my-plugin")
```

## Troubleshooting

### Plugin Not Initialized

**Problem**: Plugin reference is nil in module

**Solution**: Ensure you implement SetPlugin and check the alias matches registration:

```go
// Registration
app.RegisterPlugin(myPlugin, "my-alias")

// In module
func (m *Module) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "my-alias" {  // Must match!
        m.plugin = plugin.(*MyPlugin)
    }
}
```

### Start Fails with `ErrJetStreamNotEnabled`

**Problem**: `app.Start` returns an error wrapping `ErrJetStreamNotEnabled`.

**Solution**: The plugin (or a module) needs JetStream and the application runs without it. Enable it with `mono.WithJetStreamStorageDir(dir)`, or a `jetstream{}` block in the NATS config file.

## Related Documentation

- [Plugin System Overview](README.md)
- [Plugin Module API](../api/plugins.md)
- [File Storage Plugin (fs-jetstream)](fs-jetstream.md)
- [Key-Value Storage Plugin (kv-jetstream)](kv-jetstream.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

For examples of production plugins, examine `plugin/fs-jetstream` and `plugin/kv-jetstream` in the framework source code.
