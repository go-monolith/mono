// Package registry provides thread-safe module registration and retrieval
// with dependency resolution capabilities.
package registry

import (
	"sync"

	monoerrors "github.com/go-monolith/mono/v1/pkg/errors"
	"github.com/go-monolith/mono/v1/pkg/types"
)

// PluginRegistry manages plugin registration and retrieval by alias.
//
// Unlike ModuleRegistry which uses module name for lookup, PluginRegistry
// uses an alias that can differ from the module name. This enables multiple
// instances of the same plugin type to be registered under different aliases.
//
// For example, two instances of "fs-jetstream" plugin can be registered
// as "primary-storage" and "backup-storage" with different configurations.
//
// The registry provides thread-safe storage for plugins and maintains
// registration order. It detects duplicate alias registrations and provides
// fast O(1) lookups by alias.
type PluginRegistry interface {
	// Register adds a plugin with the given alias.
	// Returns an error if the plugin is nil, alias is empty, or alias already exists.
	Register(plugin types.PluginModule, alias string) error

	// Get retrieves a plugin by its alias.
	// Returns an error if the alias is not found.
	Get(alias string) (types.PluginModule, error)

	// List returns all registered plugin aliases in registration order.
	List() []string

	// All returns all registered plugins as a map of alias to plugin.
	All() map[string]types.PluginModule
}

// pluginRegistry is the concrete implementation of PluginRegistry.
type pluginRegistry struct {
	sync.RWMutex                               // Protects concurrent access
	plugins      map[string]types.PluginModule // Fast O(1) lookup by alias
	aliases      []string                      // Maintains registration order
	logger       types.Logger                  // Structured logger for registry operations
}

// NewPluginRegistry creates a new plugin registry instance with the provided logger.
// The logger must not be nil and is used for logging registry operations.
//
// Example:
//
//	registry := NewPluginRegistry(logger)
//	err := registry.Register(myPlugin, "primary-storage")
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewPluginRegistry(logger types.Logger) PluginRegistry {
	if logger == nil {
		panic("logger cannot be nil")
	}
	return &pluginRegistry{
		plugins: make(map[string]types.PluginModule),
		aliases: make([]string, 0),
		logger:  logger,
	}
}

// Register adds a plugin to the registry with the given alias.
//
// The alias must be unique and non-empty. The plugin must not be nil.
// Duplicate alias registrations will return an error.
//
// Example:
//
//	err := registry.Register(storagePlugin, "primary-storage")
//	if err != nil {
//	    log.Printf("Plugin registration failed: %v", err)
//	}
func (r *pluginRegistry) Register(plugin types.PluginModule, alias string) error {
	if plugin == nil {
		return monoerrors.WrapInvalidModule(nil, "plugin cannot be nil")
	}

	if alias == "" {
		return monoerrors.WrapInvalidModule(plugin, "plugin alias cannot be empty")
	}

	r.Lock()
	defer r.Unlock()

	// Check for duplicate registration
	if _, exists := r.plugins[alias]; exists {
		return monoerrors.WrapPluginAlreadyRegistered(alias)
	}

	// Add to both map and slice
	r.plugins[alias] = plugin
	r.aliases = append(r.aliases, alias)

	r.logger.Info("Plugin registered", "alias", alias, "module", plugin.Name())

	return nil
}

// Get retrieves a plugin by its alias.
//
// Returns an error if the alias is not found in the registry.
//
// Example:
//
//	plugin, err := registry.Get("primary-storage")
//	if err != nil {
//	    log.Printf("Plugin not found: %v", err)
//	}
func (r *pluginRegistry) Get(alias string) (types.PluginModule, error) {
	if alias == "" {
		return nil, monoerrors.WrapInvalidModule(nil, "plugin alias cannot be empty")
	}

	r.RLock()
	defer r.RUnlock()

	plugin, exists := r.plugins[alias]
	if !exists {
		return nil, monoerrors.WrapPluginNotFound(alias)
	}

	r.logger.Debug("Plugin retrieved", "alias", alias, "module", plugin.Name())

	return plugin, nil
}

// List returns all registered plugin aliases in registration order.
//
// Returns a copy of the aliases slice to prevent external modification.
//
// Example:
//
//	aliases := registry.List()
//	for _, alias := range aliases {
//	    fmt.Println("Registered plugin:", alias)
//	}
func (r *pluginRegistry) List() []string {
	r.RLock()
	defer r.RUnlock()

	// Return a copy to prevent external modification
	aliases := make([]string, len(r.aliases))
	copy(aliases, r.aliases)

	r.logger.Debug("Listed plugins", "count", len(aliases))

	return aliases
}

// All returns all registered plugins as a map of alias to plugin.
//
// Returns a copy of the plugins map to prevent external modification.
//
// Example:
//
//	plugins := registry.All()
//	for alias, plugin := range plugins {
//	    fmt.Printf("Plugin %s: %s\n", alias, plugin.Name())
//	}
func (r *pluginRegistry) All() map[string]types.PluginModule {
	r.RLock()
	defer r.RUnlock()

	// Return a copy to prevent external modification
	plugins := make(map[string]types.PluginModule, len(r.plugins))
	for alias, plugin := range r.plugins {
		plugins[alias] = plugin
	}

	r.logger.Debug("Retrieved all plugins", "count", len(plugins))

	return plugins
}
