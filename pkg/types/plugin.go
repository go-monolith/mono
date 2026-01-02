// Package types provides core interfaces and types for the mono framework.
package types

// PluginModule is a special module type that starts before middleware modules
// and stops after all other modules. Plugins receive their own dedicated
// ServiceContainer and are excluded from the dependency graph and middleware hooks.
//
// Plugins are designed for shared infrastructure that other modules need to access,
// such as storage backends, caching layers, or external service clients. Unlike
// regular modules, plugins:
//   - Start first and stop last (guaranteed initialization order)
//   - Are excluded from the dependency graph (don't participate in circular dependency checks)
//   - Receive their own ServiceContainer (can register services other modules access)
//   - Are excluded from middleware hooks (raw access without interception)
//
// Plugin modules are registered with an alias via RegisterPlugin(), allowing
// multiple instances of the same plugin type to be registered under different aliases.
// This enables scenarios like having "primary-storage" and "backup-storage" plugins
// using the same underlying implementation with different configurations.
//
// # When to Use Plugins
//
// Use PluginModule when you need:
//   - Shared infrastructure accessed by multiple modules (e.g., file storage, cache)
//   - Resources that must be ready before any module starts
//   - Multiple instances of the same resource type with different configs
//   - Bypass middleware for low-level operations
//
// # Lifecycle Order
//
// Startup: Plugins -> Middleware -> Regular modules (dependency order)
// Shutdown: Regular modules (reverse) -> Middleware (reverse) -> Plugins (reverse)
//
// # Built-in Plugin
//
// See the fs-jetstream plugin in plugin/fs-jetstream/ for a production-ready example
// that provides file storage capabilities using JetStream ObjectStore.
//
// # Example
//
//	type FileStoragePlugin struct {
//	    container ServiceContainer
//	}
//
//	func (p *FileStoragePlugin) Name() string { return "fs-jetstream" }
//
//	func (p *FileStoragePlugin) SetContainer(container ServiceContainer) {
//	    p.container = container
//	}
//
//	func (p *FileStoragePlugin) Container() ServiceContainer {
//	    return p.container
//	}
//
//	func (p *FileStoragePlugin) Start(ctx context.Context) error {
//	    // Plugin initialization
//	    return nil
//	}
//
//	func (p *FileStoragePlugin) Stop(ctx context.Context) error {
//	    // Plugin cleanup
//	    return nil
//	}
type PluginModule interface {
	Module

	// SetContainer is called by the framework to inject the plugin's
	// dedicated ServiceContainer during initialization.
	// This is called before Start() and before middleware modules start.
	//
	// The container is already bound to the plugin module when this method
	// is called, and the EventBus is set on the container.
	SetContainer(container ServiceContainer)

	// Container returns the plugin's ServiceContainer.
	// This allows other modules to access the plugin's registered services
	// via the plugin instance they receive through SetPlugin().
	Container() ServiceContainer
}

// UsePluginModule allows modules (including middleware) to receive plugin
// instances from the framework. The framework injects ALL registered plugins
// via SetPlugin() during module initialization, and the module can filter
// which plugins it wants to use.
//
// This interface enables plugin discovery without creating explicit dependencies.
// Modules don't need to declare plugins in Dependencies() - the framework
// automatically injects all registered plugins, and the module filters by alias.
//
// # Plugin Discovery Pattern
//
// Unlike DependentModule which requires declaring dependencies upfront,
// UsePluginModule provides a discovery-based approach:
//  1. Framework registers plugins with aliases (e.g., "storage", "cache")
//  2. Framework calls SetPlugin() for each registered plugin
//  3. Module filters and stores plugins it needs based on alias
//  4. Module verifies required plugins in Start() before using them
//
// # Timing
//
// SetPlugin is called:
//   - After ServiceContainer binding (BindModule)
//   - Before SetDependencyServiceContainer
//   - Before SetEventBus
//   - Before RegisterServices
//   - Before Start
//
// # Important
//
// Modules implementing this interface should verify that required plugins
// were injected before using them in Start(). If a required plugin is not
// registered, the plugin reference will be nil.
//
// # Example
//
//	type DocumentModule struct {
//	    storagePlugin mono.PluginModule
//	}
//
//	func (m *DocumentModule) Name() string { return "documents" }
//
//	func (m *DocumentModule) SetPlugin(alias string, plugin mono.PluginModule) {
//	    // Filter and store only the plugins you need
//	    if alias == "primary-storage" {
//	        m.storagePlugin = plugin
//	    }
//	}
//
//	func (m *DocumentModule) Start(ctx context.Context) error {
//	    // Verify required plugin was injected
//	    if m.storagePlugin == nil {
//	        return fmt.Errorf("required plugin 'primary-storage' not registered")
//	    }
//	    // Access plugin's services via its container
//	    container := m.storagePlugin.Container()
//	    // Use services...
//	    return nil
//	}
type UsePluginModule interface {
	Module

	// SetPlugin is called by the framework for each registered plugin.
	// The module should filter by alias and store the plugins it needs.
	// This method is called for all registered plugins in registration order.
	SetPlugin(alias string, plugin PluginModule)
}
