# Creating Custom Plugins

This guide walks you through creating your own plugins for the Monolith Framework. Plugins are the way to extend the framework with custom infrastructure services, external integrations, or specialized capabilities.

## When to Create a Plugin

Create a plugin for:

- **External Services**: Database connections, message queues, cache servers
- **Resource Managers**: Connection pools, thread pools, memory managers
- **Integration Points**: Cloud APIs, payment processors, third-party services
- **Infrastructure Concerns**: Monitoring, logging, health checking
- **Stateful Services**: Anything that needs special lifecycle management

Don't create a plugin for:

- **Business Logic**: Use regular modules instead
- **Simple Configuration**: Use framework options or environment variables
- **Service Handlers**: Register them in modules using ServiceContainer

## Plugin Architecture

Plugins follow a standard structure with three layers:

```
PluginModule (implements PluginModule interface)
    └── Public API (consumer-facing interfaces)
            └── Internal Implementation
                    └── External Resources (NATS, databases, etc.)
```

## Core Plugin Interfaces

### PluginModule Interface

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

```go
type UsePluginModule interface {
    Module

    // SetPlugin receives each registered plugin instance
    SetPlugin(alias string, plugin PluginModule)
}
```

## Step 1: Define Your Plugin Structure

Start with a concrete struct that implements `PluginModule`:

```go
package customplugin

import (
    "context"
    "fmt"

    mono "github.com/go-monolith/mono"
)

type PluginModule struct {
    name      string
    container mono.ServiceContainer
    // Add your resource fields here
    resource  *YourResource
}

// Config holds plugin configuration
type Config struct {
    // Add configuration fields
    Hostname string
    Port     int
}

// New creates and initializes the plugin
func New(config Config) (*PluginModule, error) {
    return &PluginModule{
        name: "customplugin",
        // Initialize your resource
    }, nil
}
```

## Step 2: Implement the Module Interface

Implement the required methods from `Module`:

```go
func (p *PluginModule) Name() string {
    return p.name
}

func (p *PluginModule) Start(ctx context.Context) error {
    // Initialize your resource
    // Example:
    resource, err := ConnectToExternalService(ctx)
    if err != nil {
        return fmt.Errorf("failed to initialize: %w", err)
    }
    p.resource = resource
    return nil
}

func (p *PluginModule) Stop(ctx context.Context) error {
    // Clean up your resource
    if p.resource != nil {
        return p.resource.Close(ctx)
    }
    return nil
}
```

## Step 3: Implement the PluginModule Interface

Implement the plugin-specific methods:

```go
func (p *PluginModule) SetContainer(container mono.ServiceContainer) {
    p.container = container
}

func (p *PluginModule) Container() mono.ServiceContainer {
    return p.container
}
```

## Step 4: Define Your Public API

Create consumer-facing interfaces that abstract your implementation:

```go
// PublicPort defines the interface consumers will use
type PublicPort interface {
    // Operation 1
    Operation1(ctx context.Context, arg string) (string, error)

    // Operation 2
    Operation2(ctx context.Context, arg string) error
}

// Implement the port
func (p *PluginModule) Port() PublicPort {
    return &adapter{resource: p.resource}
}

type adapter struct {
    resource *YourResource
}

func (a *adapter) Operation1(ctx context.Context, arg string) (string, error) {
    return a.resource.Do(ctx, arg)
}

func (a *adapter) Operation2(ctx context.Context, arg string) error {
    return a.resource.Perform(ctx, arg)
}
```

## Step 5: Create a Complete Example

```go
package customplugin

import (
    "context"
    "fmt"
    "time"

    mono "github.com/go-monolith/mono"
)

// Plugin configuration
type Config struct {
    ConnectionString string
    MaxConnections   int
    Timeout          time.Duration
}

// Plugin implementation
type MyPlugin struct {
    name      string
    config    Config
    container mono.ServiceContainer
    conn      *Connection
}

// New creates the plugin
func New(config Config) (*MyPlugin, error) {
    if config.ConnectionString == "" {
        return nil, fmt.Errorf("connection string required")
    }

    return &MyPlugin{
        name:   "my-plugin",
        config: config,
    }, nil
}

// Module interface
func (p *MyPlugin) Name() string {
    return p.name
}

func (p *MyPlugin) Start(ctx context.Context) error {
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

    return nil
}

func (p *MyPlugin) Stop(ctx context.Context) error {
    if p.conn != nil {
        return p.conn.Close(ctx)
    }
    return nil
}

// PluginModule interface
func (p *MyPlugin) SetContainer(container mono.ServiceContainer) {
    p.container = container
}

func (p *MyPlugin) Container() mono.ServiceContainer {
    return p.container
}

// Public API
type DataPort interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Set(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
}

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

## Step 6: Use Your Plugin in a Module

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

## Step 7: Register and Use

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
    if err := app.Register(&MyModule{}); err != nil {
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

## Best Practices

### Error Handling

Always wrap errors with context:

```go
func (p *MyPlugin) Start(ctx context.Context) error {
    conn, err := NewConnection(ctx)
    if err != nil {
        return fmt.Errorf("failed to create connection: %w", err)
    }
    p.conn = conn
    return nil
}
```

### Graceful Shutdown

Implement proper cleanup in Stop():

```go
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

### Configuration Validation

Validate configuration early in New():

```go
func New(config Config) (*MyPlugin, error) {
    // Validate required fields
    if config.ConnectionString == "" {
        return nil, fmt.Errorf("connection string required")
    }

    // Validate ranges
    if config.MaxConnections < 1 || config.MaxConnections > 1000 {
        return nil, fmt.Errorf("max connections must be 1-1000, got %d", config.MaxConnections)
    }

    return &MyPlugin{...}, nil
}
```

### Nil Checks in Consumers

Always check if plugin was injected:

```go
func (m *MyModule) Start(ctx context.Context) error {
    if m.plugin == nil {
        return fmt.Errorf("required plugin not registered")
    }

    // Use plugin...
    return nil
}
```

### Logging

The framework does not inject a logger into plugins. Accept one through an option, as the built-in plugins do with `WithLogger(*slog.Logger)`, and default to `slog.Default()`:

```go
// MyPlugin holds a field `logger *slog.Logger`, set to slog.Default() in New
// and replaced by WithLogger.
type Option func(*MyPlugin)

func WithLogger(logger *slog.Logger) Option {
    return func(p *MyPlugin) { p.logger = logger }
}

func (p *MyPlugin) Start(ctx context.Context) error {
    p.logger.Info("Initializing plugin", "name", p.Name())

    if err := p.initialize(ctx); err != nil {
        p.logger.Error("Initialization failed", "error", err)
        return err
    }

    p.logger.Info("Plugin initialized successfully")
    return nil
}
```

## Testing Your Plugin

```go
func TestPluginStart(t *testing.T) {
    ctx := context.Background()

    // Create plugin with test config
    plugin, err := New(Config{
        ConnectionString: "test://localhost:9000",
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
    plugin, _ := New(Config{ConnectionString: "test://localhost:9000"})
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
type MyPlugin struct {
    // ...
}

func (p *MyPlugin) DataPort() DataPort { /* ... */ }
func (p *MyPlugin) AdminPort() AdminPort { /* ... */ }
```

### Service Registration

A plugin that implements `ServiceProviderModule` has `RegisterServices()` called on its own container before `Start()`. Services registered in a plugin's container are not currently served over NATS (no subscription is created for them), so calls to them get no responder. Expose plugin functionality through Go methods (ports) instead, as shown above.

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

## Comparison with Built-in Plugins

| Aspect | fs-jetstream | kv-jetstream | Your Plugin |
|--------|------------|------------|------------|
| Scope | File/object storage | Key-value storage | Any infrastructure |
| Complexity | Medium | Medium | Your choice |
| External Deps | JetStream | JetStream | Your choice |
| Testing | Unit + integration | Unit + integration | Your choice |
| Lifecycle | Plugin standard | Plugin standard | Plugin standard |

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

### Start Fails

**Problem**: Plugin fails to start

**Solution**: Ensure all required resources are available:

```go
func (p *MyPlugin) Start(ctx context.Context) error {
    // Validate configuration
    if p.config.ConnectionString == "" {
        return fmt.Errorf("connection string required")
    }

    // Initialize with proper error handling
    resource, err := NewResource(ctx, p.config)
    if err != nil {
        return fmt.Errorf("failed to create resource: %w", err)
    }

    p.resource = resource
    return nil
}
```

## Related Documentation

- [Plugin System Overview](README.md)
- [File Storage Plugin (fs-jetstream)](fs-jetstream.md)
- [Key-Value Storage Plugin (kv-jetstream)](kv-jetstream.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

For existing plugins, see the [Plugin System Overview](README.md). For examples of production plugins, examine `fs-jetstream` and `kv-jetstream` in the framework source code.
