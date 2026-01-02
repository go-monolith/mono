// Package app provides the framework implementation that ties together all internal packages.
// This package exists to avoid circular dependencies between pkg/mono and internal packages.
package app

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-monolith/mono/v1/internal/eventbus"
	"github.com/go-monolith/mono/v1/internal/lifecycle"
	"github.com/go-monolith/mono/v1/internal/nats"
	"github.com/go-monolith/mono/v1/internal/registry"
	"github.com/go-monolith/mono/v1/pkg/types"
)

// frameworkApplication implements the types.MonoFramework interface.
type frameworkApplication struct {
	mu                         sync.RWMutex
	natsManager                nats.NATSManager
	eventBus                   types.EventBus
	eventRegistry              types.EventRegistry
	logger                     types.Logger
	registry                   registry.ModuleRegistry
	pluginRegistry             registry.PluginRegistry
	lifecycleManager           lifecycle.LifecycleManager
	state                      types.MonoFrameworkState
	queueGroupOptimisticWindow time.Duration
}

// NewFrameworkAppInstance creates a new framework instance with the given logger.
//
// The framework initialization sequence:
//  1. Initialize NATS manager and connect
//  2. Initialize EventBus
//  3. Initialize module registry
//  4. Initialize event registry
//  5. Initialize lifecycle manager
//  6. Set runtime context on event bus for graceful shutdown
//
// Modules can be registered before or after framework initialization,
// but must be registered before calling Start().
func NewFrameworkAppInstance(logger types.Logger, queueGroupOptimisticWindow time.Duration, natsOptions ...nats.NATSOption) (types.MonoFramework, error) {
	fwApp := &frameworkApplication{
		logger:                     logger,
		state:                      types.StateCreated,
		queueGroupOptimisticWindow: queueGroupOptimisticWindow,
	}

	// Step 1: Initialize NATS manager
	natsManager, err := nats.NewNATSManager(logger, natsOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS manager: %w", err)
	}
	fwApp.natsManager = natsManager

	// Step 2: Start NATS server
	if err := fwApp.natsManager.Start(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to start NATS: %w", err)
	}

	// Step 3: Get NATS connection and create EventBus
	conn, err := fwApp.natsManager.Connection()
	if err != nil {
		return nil, fmt.Errorf("failed to get NATS connection: %w", err)
	}
	fwApp.eventBus, err = eventbus.NewEventBus(conn, fwApp.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create EventBus: %w", err)
	}

	// Step 4: Initialize module registry
	fwApp.registry = registry.NewModuleRegistry(fwApp.logger)

	// Step 4.5: Initialize event registry
	fwApp.eventRegistry = registry.NewEventRegistry(fwApp.logger)

	// Step 4.7: Initialize plugin registry
	fwApp.pluginRegistry = registry.NewPluginRegistry(fwApp.logger)

	// Step 5: Initialize lifecycle manager
	fwApp.lifecycleManager = lifecycle.NewLifecycleManager(
		fwApp.registry,
		fwApp.pluginRegistry,
		fwApp.eventBus,
		fwApp.eventRegistry,
		fwApp.logger,
		fwApp.queueGroupOptimisticWindow,
	)

	// Step 6: Set runtime context on event bus for graceful shutdown
	runtimeCtx := fwApp.lifecycleManager.GetRuntimeContext()
	fwApp.eventBus.SetRuntimeContext(runtimeCtx)

	fwApp.logger.Info("Application initialized")
	return fwApp, nil
}

// Register registers a module with the framework.
func (fw *frameworkApplication) Register(module types.Module) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.state != types.StateCreated {
		return fmt.Errorf("cannot register module %s: framework is %s", module.Name(), fw.state)
	}

	if module.Name() == "" {
		return fmt.Errorf("module name cannot be empty")
	}

	if err := fw.registry.Register(module); err != nil {
		return err
	}

	return nil
}

// RegisterPlugin registers a plugin module with an alias.
func (fw *frameworkApplication) RegisterPlugin(plugin types.PluginModule, alias string) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.state != types.StateCreated {
		return fmt.Errorf("cannot register plugin %s: framework is %s", alias, fw.state)
	}

	return fw.pluginRegistry.Register(plugin, alias)
}

// Plugin returns the plugin registered under the given alias.
func (fw *frameworkApplication) Plugin(alias string) types.PluginModule {
	fw.mu.RLock()
	defer fw.mu.RUnlock()

	plugin, err := fw.pluginRegistry.Get(alias)
	if err != nil {
		return nil
	}
	return plugin
}

// Start initializes and starts all registered modules in dependency order.
func (fw *frameworkApplication) Start(ctx context.Context) error {
	fw.mu.Lock()
	if fw.state == types.StateRunning {
		fw.mu.Unlock()
		return fmt.Errorf("application already running")
	}
	if fw.state == types.StateStopped {
		fw.mu.Unlock()
		return fmt.Errorf("application already stopped, cannot restart")
	}
	fw.state = types.StateStarting
	fw.mu.Unlock()

	fw.logger.Info("Starting application")

	if err := fw.lifecycleManager.Start(ctx); err != nil {
		fw.mu.Lock()
		fw.state = types.StateCreated
		fw.mu.Unlock()
		return err
	}

	fw.mu.Lock()
	fw.state = types.StateRunning
	fw.mu.Unlock()

	fw.logger.Info("Application started successfully")
	return nil
}

// Stop gracefully shuts down all modules in reverse dependency order.
func (fw *frameworkApplication) Stop(ctx context.Context) error {
	defer finalizeShutdown(fw.logger)

	fw.mu.Lock()
	if fw.state == types.StateStopped {
		fw.mu.Unlock()
		return fmt.Errorf("application already stopped")
	}
	wasRunning := fw.state == types.StateRunning
	fw.state = types.StateStopping
	fw.mu.Unlock()

	fw.logger.Info("Stopping application")

	var stopErr error
	// Only stop modules if the framework was actually running
	if wasRunning {
		if err := fw.lifecycleManager.Stop(ctx); err != nil {
			fw.logger.Error("Application stopped with errors", "error", err)
			stopErr = err
		}
	}

	// Always stop NATS server if it was started (NATS starts in NewFramework)
	// Use Background() context to ensure NATS cleanup completes even if caller context is cancelled
	if fw.natsManager != nil {
		if err := fw.natsManager.Stop(context.Background()); err != nil {
			fw.logger.Error("Failed to stop NATS manager", "error", err)
			if stopErr == nil {
				stopErr = err
			}
		}
	}

	fw.mu.Lock()
	fw.state = types.StateStopped
	fw.mu.Unlock()

	if stopErr != nil {
		return stopErr
	}

	fw.logger.Info("Application stopped successfully")
	return nil
}

func finalizeShutdown(logger types.Logger) {
	if logger == nil {
		return
	}
	logger.Info("Shutdown completed")
	flushLogger(logger)
}

func flushLogger(logger types.Logger) {
	if logger == nil {
		return
	}
	flusher, ok := logger.(interface{ Flush() error })
	if !ok {
		return
	}
	if err := flusher.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to flush logger: %v\n", err)
	}
}

// Services returns the service container for the specified module.
func (fw *frameworkApplication) Services(moduleName string) types.ServiceContainer {
	fw.mu.RLock()
	defer fw.mu.RUnlock()

	if fw.state != types.StateRunning {
		return nil
	}

	if moduleName == "" {
		return nil
	}

	return fw.lifecycleManager.GetServiceContainer(moduleName)
}

// EventBus returns the framework's EventBus for the specified module.
func (fw *frameworkApplication) EventBus(moduleName string) types.EventBus {
	// For now, all modules share the same EventBus
	return fw.eventBus
}

// Modules returns a list of all registered module names.
func (fw *frameworkApplication) Modules() []string {
	fw.mu.RLock()
	defer fw.mu.RUnlock()

	modules := fw.registry.All()
	names := make([]string, 0, len(modules))
	for name := range modules {
		names = append(names, name)
	}
	return names
}

// Health aggregates health status from all framework components and modules.
func (fw *frameworkApplication) Health(ctx context.Context) types.FrameworkHealth {
	fw.mu.RLock()
	state := fw.state
	fw.mu.RUnlock()

	health := types.FrameworkHealth{
		Healthy:   false,
		State:     state,
		Modules:   make(map[string]types.ModuleHealth),
		Timestamp: time.Now(),
	}

	// Framework must be running to be healthy
	if state != types.StateRunning {
		health.Message = fmt.Sprintf("Application not running (state: %s)", state)
		return health
	}

	// Check NATS connectivity
	conn, err := fw.natsManager.Connection()
	if err != nil || conn == nil || !conn.IsConnected() {
		health.NATSHealthy = false
		health.Message = "NATS server unhealthy"
		return health
	}
	health.NATSHealthy = true

	// Check module health
	allModulesHealthy := true
	var unhealthyModules []string

	modules := fw.registry.All()
	for _, name := range fw.registry.List() {
		module := modules[name]
		moduleHealth := types.ModuleHealth{
			Name:           name,
			SupportsHealth: false,
			Healthy:        true, // Default to true for non-health modules
		}

		// Check if module implements HealthCheckableModule
		if healthMod, ok := module.(types.HealthCheckableModule); ok {
			moduleHealth.SupportsHealth = true

			// Call Health() with panic recovery
			func() {
				defer func() {
					if r := recover(); r != nil {
						moduleHealth.Healthy = false
						moduleHealth.Message = fmt.Sprintf("health check panicked: %v", r)
						allModulesHealthy = false
						unhealthyModules = append(unhealthyModules, name)
					}
				}()

				status := healthMod.Health(ctx)
				moduleHealth.Healthy = status.Healthy
				moduleHealth.Message = status.Message
				moduleHealth.Details = status.Details

				if !status.Healthy {
					allModulesHealthy = false
					unhealthyModules = append(unhealthyModules, name)
				}
			}()
		}

		health.Modules[name] = moduleHealth
	}

	// Framework is healthy only if all modules are healthy
	health.Healthy = allModulesHealthy
	if !allModulesHealthy {
		health.Message = fmt.Sprintf("unhealthy modules: %v", unhealthyModules)
	}

	return health
}

// Logger returns the framework's internal logger.
func (fw *frameworkApplication) Logger() types.Logger {
	return fw.logger
}
