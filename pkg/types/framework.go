package types

import (
	"context"
	"fmt"
	"time"
)

// MonoFramework is the main entry point for the monolith framework.
// It manages module lifecycle, configuration, and inter-module communication.
//
// See docs/spec/monolith-framework/design.md for detailed design documentation.
type MonoFramework interface {
	// Register adds a module to the framework
	Register(module Module) error

	// RegisterPlugin registers a plugin module with an alias.
	// The alias uniquely identifies the plugin and can differ from the module name.
	// This allows multiple instances of the same plugin type under different aliases.
	// Must be called before Start().
	//
	// Plugins start before middleware modules and stop after all other modules.
	// They are excluded from the dependency graph and middleware hooks.
	//
	// Example:
	//
	//	err := app.RegisterPlugin(storagePlugin, "primary-storage")
	//	err = app.RegisterPlugin(backupPlugin, "backup-storage")
	RegisterPlugin(plugin PluginModule, alias string) error

	// Plugin returns the plugin registered under the given alias.
	// Returns nil if no plugin with that alias exists.
	Plugin(alias string) PluginModule

	// Start initializes and starts the framework and all registered modules
	Start(ctx context.Context) error

	// Stop gracefully shuts down all modules and the framework
	Stop(ctx context.Context) error

	// Services returns the service container of a module
	Services(moduleName string) ServiceContainer

	// EventBus returns the event bus used by a module
	EventBus(moduleName string) EventBus

	// Modules returns list of registered module names
	Modules() []string

	// Health returns the aggregated health status of the framework and all modules
	Health(ctx context.Context) FrameworkHealth

	// Logger returns the framework's internal logger.
	// Modules can use this to obtain a logger instance for their own logging needs.
	Logger() Logger
}

// MonoFrameworkState represents the current state of the framework
type MonoFrameworkState int

const (
	StateCreated MonoFrameworkState = iota
	StateStarting
	StateRunning
	StateStopping
	StateStopped
)

// String returns a human-readable string representation of the framework state.
func (s MonoFrameworkState) String() string {
	switch s {
	case StateCreated:
		return "Created"
	case StateStarting:
		return "Starting"
	case StateRunning:
		return "Running"
	case StateStopping:
		return "Stopping"
	case StateStopped:
		return "Stopped"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// FrameworkHealth represents the aggregated health status of the framework.
// It includes the overall framework state, NATS health, and health of all modules.
type FrameworkHealth struct {
	// Healthy indicates if the framework AND all modules are healthy.
	// This is true only if the framework is running, NATS is healthy,
	// and all modules that implement HealthAwareModule report healthy.
	Healthy bool `json:"healthy"`

	// State represents the current framework lifecycle state
	State MonoFrameworkState `json:"state"`

	// NATSHealthy indicates if the NATS server is operational
	NATSHealthy bool `json:"nats_healthy"`

	// Modules contains health status of each registered module, keyed by module name.
	// Modules that don't implement HealthAwareModule will have SupportsHealth=false.
	Modules map[string]ModuleHealth `json:"modules"`

	// Timestamp is when this health check was performed
	Timestamp time.Time `json:"timestamp"`

	// Message provides additional context if the framework is unhealthy.
	// Empty if Healthy is true.
	Message string `json:"message,omitempty"`
}

// ModuleHealth represents health status of a single module.
type ModuleHealth struct {
	// Name is the module identifier (matches Module.Name())
	Name string `json:"name"`

	// Healthy indicates if the module is operating normally.
	// Always false if SupportsHealth is false (module doesn't implement HealthAwareModule).
	Healthy bool `json:"healthy"`

	// Message provides human-readable status description
	Message string `json:"message,omitempty"`

	// Details contains additional debugging information from the module's Health() method.
	// Nil if SupportsHealth is false.
	Details map[string]any `json:"details,omitempty"`

	// SupportsHealth indicates if the module implements HealthAwareModule.
	// If false, the Healthy field is meaningless (always True) and Details will be nil.
	SupportsHealth bool `json:"supports_health"`
}
