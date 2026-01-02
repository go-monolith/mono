// Package registry provides thread-safe module registration and retrieval
// with dependency resolution capabilities.
package registry

import (
	"sync"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// ModuleRegistry manages module registration and retrieval.
//
// The registry provides thread-safe storage for modules and maintains
// registration order. It detects duplicate registrations and provides
// fast O(1) lookups by module name.
//
// See docs/spec/monolith-framework/design.md Module Registry Module section.
type ModuleRegistry interface {
	// Register adds a module to the registry.
	// Returns an error if the module is nil, has an empty name, or is already registered.
	Register(module types.Module) error

	// Get retrieves a module by name.
	// Returns an error if the module is not found.
	Get(name string) (types.Module, error)

	// List returns all registered module names in registration order.
	List() []string

	// All returns all registered modules as a map of name to module.
	All() map[string]types.Module
}

// moduleRegistry is the concrete implementation of ModuleRegistry.
type moduleRegistry struct {
	sync.RWMutex                         // Protects concurrent access
	modules      map[string]types.Module // Fast O(1) lookup by name
	names        []string                // Maintains registration order
	logger       types.Logger            // Structured logger for registry operations
}

// NewModuleRegistry creates a new module registry instance with the provided logger.
// The logger must not be nil and is used for logging registry operations.
//
// Example:
//
//	registry := NewModuleRegistry(logger)
//	err := registry.Register(myModule)
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewModuleRegistry(logger types.Logger) ModuleRegistry {
	if logger == nil {
		panic("logger cannot be nil")
	}
	return &moduleRegistry{
		modules: make(map[string]types.Module),
		names:   make([]string, 0),
		logger:  logger,
	}
}

// Register adds a module to the registry.
//
// The module must have a unique, non-empty name. Duplicate registrations
// will return an error.
//
// Example:
//
//	err := registry.Register(myModule)
//	if err != nil {
//	    if types.IsModuleError(err) {
//	        log.Printf("Module registration failed: %v", err)
//	    }
//	}
func (r *moduleRegistry) Register(module types.Module) error {
	if module == nil {
		return monoerrors.WrapInvalidModule(nil, "module cannot be nil")
	}

	name := module.Name()
	if name == "" {
		return monoerrors.WrapInvalidModule(module, "module name cannot be empty")
	}

	r.Lock()
	defer r.Unlock()

	// Check for duplicate registration
	if _, exists := r.modules[name]; exists {
		return monoerrors.WrapModuleAlreadyRegistered(name)
	}

	// Add to both map and slice
	r.modules[name] = module
	r.names = append(r.names, name)

	r.logger.Info("Module registered", "module", name)

	return nil
}

// Get retrieves a module by name.
//
// Returns an error if the module is not found in the registry.
//
// Example:
//
//	module, err := registry.Get("my-module")
//	if err != nil {
//	    if types.IsModuleError(err) {
//	        log.Printf("Module not found: %v", err)
//	    }
//	}
func (r *moduleRegistry) Get(name string) (types.Module, error) {
	if name == "" {
		return nil, monoerrors.WrapInvalidModule(nil, "module name cannot be empty")
	}

	r.RLock()
	defer r.RUnlock()

	module, exists := r.modules[name]
	if !exists {
		return nil, monoerrors.WrapModuleNotFound(name)
	}

	r.logger.Debug("Module retrieved", "module", name)

	return module, nil
}

// List returns all registered module names in registration order.
//
// Returns a copy of the names slice to prevent external modification.
//
// Example:
//
//	names := registry.List()
//	for _, name := range names {
//	    fmt.Println("Registered module:", name)
//	}
func (r *moduleRegistry) List() []string {
	r.RLock()
	defer r.RUnlock()

	// Return a copy to prevent external modification
	names := make([]string, len(r.names))
	copy(names, r.names)

	r.logger.Debug("Listed modules", "count", len(names))

	return names
}

// All returns all registered modules as a map of name to module.
//
// Returns a copy of the modules map to prevent external modification.
//
// Example:
//
//	modules := registry.All()
//	for name, module := range modules {
//	    fmt.Printf("Module %s: %T\n", name, module)
//	}
func (r *moduleRegistry) All() map[string]types.Module {
	r.RLock()
	defer r.RUnlock()

	// Return a copy to prevent external modification
	modules := make(map[string]types.Module, len(r.modules))
	for name, module := range r.modules {
		modules[name] = module
	}

	r.logger.Debug("Retrieved all modules", "count", len(modules))

	return modules
}
