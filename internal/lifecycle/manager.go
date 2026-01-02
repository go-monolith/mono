// Package lifecycle provides lifecycle management for modules, coordinating
// initialization and shutdown sequences in dependency order.
package lifecycle

import (
	"context"

	"github.com/go-monolith/mono/v1/pkg/types"
)

// LifecycleManager coordinates module lifecycle.
//
// Manages the complete module initialization and shutdown sequence including:
// - Module initialization in dependency order
// - Event definition collection from EventEmitterModules
// - NATS subscription setup (including event consumers)
// - Graceful shutdown in reverse dependency order
// - NATS subscription teardown
//
// See docs/spec/monolith-framework/design.md Lifecycle Manager Module section.
type LifecycleManager interface {
	// Start starts all modules in dependency order
	Start(ctx context.Context) error

	// Stop stops all modules in reverse dependency order
	Stop(ctx context.Context) error

	// WaitForShutdown blocks until shutdown signal or context cancellation
	WaitForShutdown(ctx context.Context) error

	// GetRuntimeContext returns the context that is passed to message handlers.
	// This context is cancelled when the framework shuts down.
	GetRuntimeContext() context.Context

	// GetMiddlewareHook returns a function that can be called to run middleware lifecycle events.
	// This is used to wire the registry to the middleware chain.
	GetMiddlewareHook() func(ctx context.Context, event types.ModuleLifecycleEvent)

	// GetServiceContainer returns the service container for a registered module.
	// Returns nil if the module is not found or hasn't been started yet.
	GetServiceContainer(moduleName string) types.ServiceContainer

	// GetPlugin returns the plugin registered under the given alias.
	// Returns nil if no plugin with that alias exists.
	GetPlugin(alias string) types.PluginModule

	// GetPluginContainer returns the service container for a registered plugin.
	// Returns nil if the plugin is not found or hasn't been started yet.
	GetPluginContainer(alias string) types.ServiceContainer
}

// LifecycleEvent represents a module lifecycle event.
type LifecycleEvent struct {
	Module string
	Event  string // "starting", "started", "stopping", "stopped", "failed"
	// Timestamp time.Time
	Error error
}

// Implementation complete - see manager_impl.go
// - LifecycleManager interface implemented
// - Start() method with dependency ordering and rollback
// - Stop() method with reverse dependency ordering
// - Panic recovery for Start() and Stop()
// - Module initialization sequence (steps 0-5)
// - NATS subscription lifecycle management
// - Graceful shutdown with signals (signals.go)
// - Comprehensive unit tests (manager_test.go)
