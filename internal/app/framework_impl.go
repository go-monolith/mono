// Package app provides the framework implementation that ties together all internal packages.
// This package exists to avoid circular dependencies between pkg/mono and internal packages.
package app

import (
	"context"
	stderrors "errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-monolith/mono/internal/eventbus"
	"github.com/go-monolith/mono/internal/lifecycle"
	"github.com/go-monolith/mono/internal/nats"
	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/pkg/types"
)

// frameworkApplication implements the types.MonoFramework interface.
type frameworkApplication struct {
	mu               sync.RWMutex
	natsManager      nats.NATSManager
	eventBus         types.EventBus
	eventRegistry    types.EventRegistry
	logger           types.Logger
	registry         registry.ModuleRegistry
	pluginRegistry   registry.PluginRegistry
	lifecycleManager lifecycle.LifecycleManager
	state            types.MonoFrameworkState
	settings         Settings

	// startCancel cancels the context of the Start in progress and startDone
	// is closed when that Start has returned, after it stopped what it had
	// started if a Stop was called meanwhile. Both are nil when no Start is
	// in progress. startBudget is the rollback budget of that Start: a Stop
	// that aborts it hands over its deadline. abortErr is what stopping the
	// lifecycle returned to a Start that a Stop aborted. Guarded by mu.
	startCancel context.CancelFunc
	startDone   chan struct{}
	startBudget *lifecycle.RollbackBudget
	abortErr    error
	// releaseStart cancels the context of the Start that succeeded, once
	// Stop has finished. Modules may keep that context for the goroutines
	// they start (as they could keep the caller's before), so it must
	// outlive Start and everything Stop waits for. Guarded by mu.
	releaseStart context.CancelFunc
	// stopDone is closed when the Stop in progress has finished, and stopErr
	// is its result; a Stop called meanwhile waits for it instead of stopping
	// a second time. Guarded by mu.
	stopDone chan struct{}
	stopErr  error
}

// Settings carries the framework-level tunables from MonoFrameworkConfig that
// are not NATS server or logger options.
type Settings struct {
	// QueueGroupOptimisticWindow is the optimistic publish window for queue
	// group clients (0 = always ACK).
	QueueGroupOptimisticWindow time.Duration
	// DefaultServiceConcurrency is the MaxConcurrency of request-reply services
	// whose options leave it at 0 (0 = types.DefaultMaxConcurrency).
	DefaultServiceConcurrency int
	// ShutdownTimeout bounds Stop when the caller's context has no deadline
	// (0 = unbounded).
	ShutdownTimeout time.Duration
	// ShutdownDrainTimeout bounds the drain and in-flight handler wait during
	// Stop (0 = types.DefaultShutdownDrainTimeout).
	ShutdownDrainTimeout time.Duration
	// DeadlineSkewAllowance is the clock skew tolerated when deciding that a
	// request's deadline has passed (0 = types.DefaultDeadlineSkewAllowance).
	DeadlineSkewAllowance time.Duration
	// JetStreamReadyTimeout bounds the JetStream wait and the retried setup
	// steps during Start (0 = types.DefaultJetStreamReadyTimeout).
	JetStreamReadyTimeout time.Duration
	// JetStreamDefaultReplicas is the replica count of framework-created
	// streams whose configuration leaves it at 0 (0 = 1).
	JetStreamDefaultReplicas int
	// StatsLogInterval is the period of the Debug subscription stats log
	// (0 = off).
	StatsLogInterval time.Duration
	// ServiceDiscovery enables the NATS services protocol responders (nil = off).
	ServiceDiscovery *types.ServiceDiscoveryConfig
	// StrictModuleBoundaries makes module boundary violations fail instead of
	// being recorded (see types.ModuleBoundaryReport).
	StrictModuleBoundaries bool
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
func NewFrameworkAppInstance(logger types.Logger, settings Settings, natsOptions ...nats.NATSOption) (types.MonoFramework, error) {
	fwApp := &frameworkApplication{
		logger:   logger,
		state:    types.StateCreated,
		settings: settings,
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

	// Step 3: Get NATS connection and create EventBus. The server is
	// running from here on: stop it if creation fails.
	conn, err := fwApp.natsManager.Connection()
	if err != nil {
		fwApp.stopNATSAfterFailedCreate()
		return nil, fmt.Errorf("failed to get NATS connection: %w", err)
	}
	fwApp.eventBus, err = eventbus.NewEventBus(conn, fwApp.logger)
	if err != nil {
		fwApp.stopNATSAfterFailedCreate()
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
		fwApp.settings.QueueGroupOptimisticWindow,
	)
	fwApp.lifecycleManager.SetDefaultServiceConcurrency(fwApp.settings.DefaultServiceConcurrency)
	fwApp.lifecycleManager.SetDrainTimeout(fwApp.settings.ShutdownDrainTimeout)
	fwApp.lifecycleManager.SetDeadlineSkewAllowance(fwApp.settings.DeadlineSkewAllowance)
	fwApp.lifecycleManager.SetJetStreamReadyTimeout(fwApp.settings.JetStreamReadyTimeout)
	fwApp.lifecycleManager.SetJetStreamDefaultReplicas(fwApp.settings.JetStreamDefaultReplicas)
	fwApp.lifecycleManager.SetStatsLogInterval(fwApp.settings.StatsLogInterval)
	fwApp.lifecycleManager.SetServiceDiscovery(fwApp.settings.ServiceDiscovery)
	fwApp.lifecycleManager.SetStrictModuleBoundaries(fwApp.settings.StrictModuleBoundaries)

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
//
// Only one Start runs at a time: a call while the application is starting,
// running, stopping or stopped fails without doing anything. A Start that
// failed returns the application to the created state, so it can be called
// again.
//
// A Stop called while Start is running aborts it: the context the modules are
// started with is cancelled, and Start returns an error wrapping
// context.Canceled after what it had started was stopped. That holds also
// when the modules did not react to the cancellation and all of them started.
//
// What a failed or aborted Start had started is stopped with a context of its
// own, bounded by ShutdownTimeout (30 seconds without one). A Stop that
// aborts the start hands over its own bound: the stopping ends when that
// Stop's context does. A start that returns only after that Stop has given
// up waiting has the whole timeout again.
//
// The context the modules are started with is derived from ctx. It stays live
// after a Start that succeeded, until Stop has finished or ctx ends.
func (fw *frameworkApplication) Start(ctx context.Context) error {
	fw.mu.Lock()
	switch fw.state {
	case types.StateRunning:
		fw.mu.Unlock()
		return fmt.Errorf("application already running")
	case types.StateStopped:
		fw.mu.Unlock()
		return fmt.Errorf("application already stopped, cannot restart")
	case types.StateStarting:
		fw.mu.Unlock()
		return fmt.Errorf("application is already starting")
	case types.StateStopping:
		fw.mu.Unlock()
		return fmt.Errorf("application is stopping, cannot start")
	}
	fw.state = types.StateStarting
	budget := &lifecycle.RollbackBudget{Timeout: fw.settings.ShutdownTimeout}
	startCtx, cancel := context.WithCancel(lifecycle.WithRollbackBudget(ctx, budget))
	done := make(chan struct{})
	fw.startCancel, fw.startDone, fw.startBudget = cancel, done, budget
	fw.mu.Unlock()
	// Closed last: a Stop that aborted this Start waits for it, and must not
	// stop the NATS server under modules that are still being stopped.
	defer close(done)

	fw.logger.Info("Starting application")

	// From here the lifecycle protects and converts consumers itself; a
	// pass of the manager's early protection must not run concurrently with
	// the conversion and write back a consumer's old configuration.
	fw.natsManager.StopConsumerProtection()
	err := fw.lifecycleManager.Start(startCtx)

	fw.mu.Lock()
	// A Stop that was called meanwhile has taken the state over.
	aborted := fw.state != types.StateStarting
	if !aborted {
		if err != nil {
			fw.state = types.StateCreated
		} else {
			fw.state = types.StateRunning
			// The modules run on: their context stays live until Stop
			// has finished.
			fw.releaseStart = cancel
		}
	}
	fw.startCancel, fw.startDone, fw.startBudget = nil, nil, nil
	fw.mu.Unlock()

	if aborted {
		defer cancel()
		return fw.abortedStart(startCtx, err)
	}
	if err != nil {
		cancel()
		return err
	}

	fw.logger.Info("Application started successfully")
	return nil
}

// abortedStart finishes a Start during which Stop was called. startErr is
// what the lifecycle returned: an error when it rolled the start back, as it
// does when a module reacts to the cancelled context, or nil when every
// module started regardless. In that case nothing was rolled back, and the
// Stop that is waiting only stops the NATS server, so the lifecycle is
// stopped here, with the budget a rollback has (startCtx carries it): it ends
// when that Stop's context does, or has the whole timeout when that Stop has
// given up already. An error of that stop is kept for the Stop (abortErr),
// which would otherwise report success.
//
// The returned error always wraps context.Canceled, whatever the modules
// returned when their context was cancelled.
func (fw *frameworkApplication) abortedStart(startCtx context.Context, startErr error) error {
	if startErr != nil {
		return fmt.Errorf("application start aborted by Stop: %w", stderrors.Join(context.Canceled, startErr))
	}
	fw.logger.Warn("Start completed although Stop was called while starting; stopping what was started")
	stopCtx, cancel := lifecycle.RollbackContext(startCtx)
	defer cancel()
	if err := fw.lifecycleManager.Stop(stopCtx); err != nil {
		fw.logger.Error("Application stopped with errors", "error", err)
		fw.mu.Lock()
		fw.abortErr = err
		fw.mu.Unlock()
		return fmt.Errorf("application start aborted by Stop: %w", stderrors.Join(context.Canceled, err))
	}
	return fmt.Errorf("application start aborted by Stop: %w", context.Canceled)
}

// AbortGrace is how long a Stop that aborted a Start keeps waiting for it
// after its own context has ended. The rollback of the start ends with that
// context, so it is about to return; stopping the NATS server in the same
// instant would pull it from under the last module being stopped.
const AbortGrace = 2 * time.Second

// shutdownContext bounds ctx with the configured ShutdownTimeout when it has
// no deadline of its own.
func (fw *frameworkApplication) shutdownContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && fw.settings.ShutdownTimeout > 0 {
		return context.WithTimeout(ctx, fw.settings.ShutdownTimeout)
	}
	return ctx, func() {}
}

// Stop gracefully shuts down all modules in reverse dependency order.
//
// Only one Stop runs at a time. A Stop called while another one is in
// progress waits for it (within ctx) and returns its result, instead of
// stopping the NATS server under the handlers the first one is still waiting
// for. A Stop after one has finished fails ("application already stopped").
//
// A Stop called while Start is running aborts the start (see Start) and waits
// until Start has stopped what it had started, before it stops the NATS
// server. It waits within ctx and AbortGrace beyond it; a start that has not
// returned by then is left behind, the server is stopped and an error says
// so. Errors of the modules the aborted start stopped are logged, as for any
// failed Start; Stop returns one only when the start had completed and was
// stopped as a whole. Stop must not be called from a module's own Start or
// Stop: it would wait for itself until ctx ends.
func (fw *frameworkApplication) Stop(ctx context.Context) error {
	defer finalizeShutdown(fw.logger)

	// Bound the shutdown with the configured ShutdownTimeout when the caller
	// did not set a deadline of its own.
	ctx, cancel := fw.shutdownContext(ctx)
	defer cancel()

	fw.mu.Lock()
	switch fw.state {
	case types.StateStopped:
		fw.mu.Unlock()
		return fmt.Errorf("application already stopped")
	case types.StateStopping:
		inProgress := fw.stopDone
		fw.mu.Unlock()
		return fw.waitForStop(ctx, inProgress)
	}
	previous := fw.state
	startCancel, startDone, startBudget := fw.startCancel, fw.startDone, fw.startBudget
	if previous == types.StateStarting {
		// What the start stops, it stops until this Stop stops waiting for
		// it. Handed over before the state changes hands, so that a Start
		// that returns in this moment already finds it.
		if deadline, ok := ctx.Deadline(); ok {
			startBudget.SetDeadline(deadline)
		}
	}
	fw.state = types.StateStopping
	done := make(chan struct{})
	fw.stopDone = done
	fw.mu.Unlock()

	fw.logger.Info("Stopping application")

	var stopErr error
	switch previous {
	case types.StateRunning:
		if err := fw.lifecycleManager.Stop(ctx); err != nil {
			fw.logger.Error("Application stopped with errors", "error", err)
			stopErr = err
		}
	case types.StateStarting:
		// Abort the start and wait until it has stopped what it started:
		// stopping the NATS server first would pull it from under modules
		// that are still starting or being stopped.
		fw.logger.Info("Stop called while starting; aborting the start")
		// A context that ends before its deadline (cancelled by the caller)
		// ends the waiting just the same.
		stopHandOver := context.AfterFunc(ctx, func() { startBudget.SetDeadline(time.Now()) })
		defer stopHandOver()
		startCancel()
		if fw.waitForAbortedStart(ctx, startDone) {
			fw.mu.RLock()
			stopErr = fw.abortErr
			fw.mu.RUnlock()
		} else {
			fw.logger.Error("Start in progress did not return while stopping; stopping the NATS server anyway", "error", ctx.Err())
			stopErr = fmt.Errorf("start in progress did not return while stopping: %w", ctx.Err())
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
	fw.stopErr = stopErr
	releaseStart := fw.releaseStart
	fw.releaseStart = nil
	fw.mu.Unlock()
	if releaseStart != nil {
		releaseStart()
	}
	close(done)

	if stopErr != nil {
		return stopErr
	}

	fw.logger.Info("Application stopped successfully")
	return nil
}

// waitForAbortedStart waits until the Start that was aborted has returned
// (startDone), and reports whether it did before ctx ended and AbortGrace
// passed.
func (fw *frameworkApplication) waitForAbortedStart(ctx context.Context, startDone <-chan struct{}) bool {
	select {
	case <-startDone:
		return true
	case <-ctx.Done():
	}
	grace := time.NewTimer(AbortGrace)
	defer grace.Stop()
	select {
	case <-startDone:
		return true
	case <-grace.C:
		return false
	}
}

// waitForStop waits for the Stop in progress, whose completion inProgress
// signals, and returns its result, or the error of ctx when that ends first.
func (fw *frameworkApplication) waitForStop(ctx context.Context, inProgress <-chan struct{}) error {
	select {
	case <-inProgress:
		fw.mu.RLock()
		defer fw.mu.RUnlock()
		return fw.stopErr
	case <-ctx.Done():
		return fmt.Errorf("a stop is already in progress and did not finish: %w", ctx.Err())
	}
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

// EventBus returns the EventBus of the specified module: the bus scoped to
// that module's declared boundaries when the module has been started, and the
// shared bus otherwise (unknown names, or before Start). This is a host-level
// escape hatch for main.go, API handlers and tests, not for module code.
func (fw *frameworkApplication) EventBus(moduleName string) types.EventBus {
	if moduleName != "" {
		if bus := fw.lifecycleManager.ModuleEventBus(moduleName); bus != nil {
			return bus
		}
	}
	return fw.eventBus
}

// ModuleBoundaries returns the boundary report of every started module. It
// returns nil unless the application is running.
func (fw *frameworkApplication) ModuleBoundaries() []types.ModuleBoundaryReport {
	fw.mu.RLock()
	running := fw.state == types.StateRunning
	fw.mu.RUnlock()
	if !running {
		return nil
	}
	return fw.lifecycleManager.ModuleBoundaries()
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

// SubscriptionStats returns a snapshot of every framework-managed NATS
// subscription with its worker pool. It returns nil unless the application
// is running.
func (fw *frameworkApplication) SubscriptionStats() []types.SubscriptionStats {
	fw.mu.RLock()
	running := fw.state == types.StateRunning
	fw.mu.RUnlock()
	if !running {
		return nil
	}
	return fw.lifecycleManager.SubscriptionStats()
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
	health.JetStream = fw.natsManager.JetStreamHealth()

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

// stopNATSAfterFailedCreate stops the embedded server when creating the
// application fails after it started, so neither the server nor its
// background consumer protection outlives the failed call.
func (fw *frameworkApplication) stopNATSAfterFailedCreate() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := fw.natsManager.Stop(ctx); err != nil {
		fw.logger.Warn("Failed to stop NATS after a failed application create", "error", err)
	}
}
