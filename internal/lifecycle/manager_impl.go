package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-monolith/mono/internal/container"
	"github.com/go-monolith/mono/internal/eventbus"
	"github.com/go-monolith/mono/internal/middleware"
	"github.com/go-monolith/mono/internal/registry"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// lifecycleManager implements LifecycleManager interface
type lifecycleManager struct {
	mu                         sync.RWMutex
	registry                   registry.ModuleRegistry
	pluginRegistry             registry.PluginRegistry
	eventBus                   types.EventBus
	eventRegistry              types.EventRegistry // Event registry for event definitions and consumers
	logger                     types.Logger
	middlewareChain            *middleware.Chain
	containers                 map[string]types.ServiceContainer
	pluginContainers           map[string]types.ServiceContainer // containers by plugin alias
	subscriptions              map[string][]types.Subscription
	shutdownCh                 chan struct{}
	shutdownOnce               sync.Once
	runtimeCtx                 context.Context
	cancelRuntime              context.CancelFunc
	streamConsumers            map[string]context.CancelFunc // cancel funcs for stream consumer fetch loops
	queueGroupOptimisticWindow time.Duration                 // Optimistic publish window for queue groups
}

// NewLifecycleManager creates a new lifecycle manager
func NewLifecycleManager(
	reg registry.ModuleRegistry,
	pluginReg registry.PluginRegistry,
	eventBus types.EventBus,
	eventRegistry types.EventRegistry,
	logger types.Logger,
	queueGroupOptimisticWindow time.Duration,
) LifecycleManager {
	// Create runtime context for graceful shutdown
	runtimeCtx, cancelRuntime := context.WithCancel(context.Background()) //nolint:gosec // G118: cancelRuntime is stored in lm.cancelRuntime and called in shutdown

	return &lifecycleManager{
		registry:                   reg,
		pluginRegistry:             pluginReg,
		eventBus:                   eventBus,
		eventRegistry:              eventRegistry,
		logger:                     logger,
		containers:                 make(map[string]types.ServiceContainer),
		pluginContainers:           make(map[string]types.ServiceContainer),
		subscriptions:              make(map[string][]types.Subscription),
		shutdownCh:                 make(chan struct{}),
		runtimeCtx:                 runtimeCtx,
		cancelRuntime:              cancelRuntime,
		streamConsumers:            make(map[string]context.CancelFunc),
		queueGroupOptimisticWindow: queueGroupOptimisticWindow,
	}
}

// detectMiddlewareModules scans all registered modules and builds the middleware chain.
// Middleware modules are detected via type assertion for types.MiddlewareModule.
// The chain is built in registration order.
func (lm *lifecycleManager) detectMiddlewareModules() {
	modules := lm.registry.All()
	middlewares := make([]types.MiddlewareModule, 0)

	// Iterate modules in registration order
	for _, name := range lm.registry.List() {
		module := modules[name]
		if mw, ok := module.(types.MiddlewareModule); ok {
			middlewares = append(middlewares, mw)
			lm.logger.Debug("Detected middleware module", "module", name)
		}
	}

	if len(middlewares) > 0 {
		chain := middleware.NewChain(middlewares)
		lm.mu.Lock()
		lm.middlewareChain = chain
		lm.mu.Unlock()
		lm.logger.Debug("Middleware chain built", "count", len(middlewares))
	}
}

// Start starts all modules in dependency order
func (lm *lifecycleManager) Start(ctx context.Context) error {
	// Step 0: Start plugin modules FIRST (before middleware)
	if err := lm.startPlugins(ctx); err != nil {
		return fmt.Errorf("failed to start plugins: %w", err)
	}

	// Step 1: Detect and build middleware chain
	lm.detectMiddlewareModules()

	// Step 1.5: Inject middleware chain into event registry for consumer registration interception
	if lm.middlewareChain != nil && lm.eventRegistry != nil {
		lm.eventRegistry.SetMiddlewareChain(lm.middlewareChain)
	}

	// Step 2: Start middleware modules FIRST (in registration order)
	if lm.middlewareChain != nil {
		for _, mw := range lm.middlewareChain.Middlewares() {
			lm.logger.Info("Starting middleware module", "module", mw.Name())
			if err := mw.Start(ctx); err != nil {
				return fmt.Errorf("failed to start middleware module %s: %w", mw.Name(), err)
			}
		}
	}

	// Step 3: Get modules in dependency order
	ordered, err := registry.ResolveDependencies(lm.registry)
	if err != nil {
		return fmt.Errorf("failed to resolve dependencies: %w", err)
	}

	lm.logger.Info("Starting modules", "count", len(ordered))

	// Step 3.5: Collect event definitions from EventEmitterModules
	// This happens BEFORE module.Start() so consumers can discover events during their Start()
	if lm.eventRegistry != nil {
		for _, module := range ordered {
			if emitter, ok := module.(types.EventEmitterModule); ok {
				lm.logger.Debug("Collecting event definitions", "module", module.Name())
				for _, eventDef := range emitter.EmitEvents() {
					if err := lm.eventRegistry.RegisterEvent(eventDef); err != nil {
						return fmt.Errorf("failed to register event from %s: %w", module.Name(), err)
					}
				}
			}
		}
	}

	// Track started modules for rollback
	started := []string{}

	// Step 4: Start each module in dependency order
	for _, module := range ordered {
		// Skip middleware modules (already started)
		if _, ok := module.(types.MiddlewareModule); ok {
			continue
		}

		if err := lm.startModule(ctx, module); err != nil {
			return lm.rollback(ctx, started, err)
		}

		started = append(started, module.Name())
	}

	// Step 4.5: Start channel service routers
	for moduleName, container := range lm.containers {
		if container != nil {
			container.StartChannelRouters(lm.runtimeCtx)
			lm.logger.Debug("Started channel routers", "module", moduleName)
		}
	}

	// Step 5: Setup NATS subscriptions for all modules
	if err := lm.setupNATSSubscriptions(ctx); err != nil {
		return lm.rollback(ctx, started, err)
	}

	lm.logger.Info("All modules started successfully")
	return nil
}

// startModule executes the initialization sequence for a single module
func (lm *lifecycleManager) startModule(ctx context.Context, module types.Module) (err error) {
	moduleName := module.Name()
	start := time.Now()

	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			err = monoerrors.WrapModulePanic(moduleName, "start", r)
			lm.logger.Error("Module panic during start", "module", moduleName, "panic", r)
		}
	}()

	lm.logger.Info("Starting module", "module", moduleName)

	// Step 0: Create and bind ServiceContainer
	moduleContainer := container.New(lm.logger.WithModule(moduleName))
	if err := moduleContainer.BindModule(module); err != nil {
		return monoerrors.WrapModuleStartFailed(moduleName, err)
	}
	// Always set EventBus on container for RequestReply/QueueGroup/StreamConsumer services
	// regardless of whether the module uses them or not
	moduleContainer.SetEventBus(lm.eventBus)

	// Set queue group optimistic window if configured
	if lm.queueGroupOptimisticWindow > 0 {
		moduleContainer.SetQueueGroupOptimisticWindow(lm.queueGroupOptimisticWindow)
	}

	// Set middleware chain if available (for service registration interception)
	if lm.middlewareChain != nil {
		moduleContainer.SetMiddlewareChain(lm.middlewareChain)
	}

	lm.mu.Lock()
	lm.containers[moduleName] = moduleContainer
	lm.mu.Unlock()

	// Step 0.5: Inject plugins if UsePluginModule
	if pluginUser, ok := module.(types.UsePluginModule); ok {
		for alias, plugin := range lm.pluginRegistry.All() {
			pluginUser.SetPlugin(alias, plugin)
			lm.logger.Debug("Plugin injected", "module", moduleName, "plugin", alias)
		}
	}

	// Step 1: Set dependency service containers
	if depMod, ok := module.(types.DependentModule); ok {
		lm.mu.RLock()
		for _, depName := range depMod.Dependencies() {
			depContainer, exists := lm.containers[depName]
			if !exists {
				lm.mu.RUnlock()
				return monoerrors.WrapModuleStartFailed(moduleName, fmt.Errorf("dependency %s container not found", depName))
			}
			depMod.SetDependencyServiceContainer(depName, depContainer)
		}
		lm.mu.RUnlock()
	}

	// Step 2: Set EventBus if NATS-aware
	if natsAware, ok := module.(types.EventBusAwareModule); ok {
		natsAware.SetEventBus(lm.eventBus)
	}

	// Step 3: Register services if ServiceProvider
	if serviceProvider, ok := module.(types.ServiceProviderModule); ok {
		if err := serviceProvider.RegisterServices(moduleContainer); err != nil {
			return monoerrors.WrapModuleStartFailed(moduleName, err)
		}
	}

	// Step 4: Register event consumers if EventConsumerModule
	if lm.eventRegistry != nil {
		if eventConsumer, ok := module.(types.EventConsumerModule); ok {
			if err := eventConsumer.RegisterEventConsumers(lm.eventRegistry); err != nil {
				return monoerrors.WrapModuleStartFailed(moduleName, err)
			}
		}
	}

	// Step 5: Start the module
	if err := module.Start(ctx); err != nil {
		return monoerrors.WrapModuleStartFailed(moduleName, err)
	}

	duration := time.Since(start)
	lm.logger.Info("Module started", "module", moduleName, "duration", duration)

	// Notify middleware of module started
	if lm.middlewareChain != nil {
		event := types.ModuleLifecycleEvent{
			Type:       types.ModuleStartedEvent,
			ModuleName: moduleName,
			Duration:   duration,
			Metadata:   make(map[string]any),
		}
		lm.middlewareChain.RunModuleLifecycle(ctx, event)
	}

	return nil
}

// setupNATSSubscriptions creates NATS subscriptions for all registered services
func (lm *lifecycleManager) setupNATSSubscriptions(ctx context.Context) error {
	lm.logger.Debug("Setting up NATS subscriptions")

	lm.mu.RLock()
	containers := make(map[string]types.ServiceContainer, len(lm.containers))
	for k, v := range lm.containers {
		containers[k] = v
	}
	lm.mu.RUnlock()

	// Track cron stream names registered this boot for best-effort orphan
	// detection after all services are set up.
	registeredCronStreams := make(map[string]struct{})

	for moduleName, moduleContainer := range containers {
		entries := moduleContainer.Entries()

		for _, entry := range entries {
			switch entry.Type {
			case types.ServiceTypeRequestReply:
				// Capture fields to avoid race conditions in handler closure
				eventBus := lm.eventBus
				logger := lm.logger
				serviceName := entry.Name
				modName := moduleName // Capture module name for error responses
				queueGroup := entry.QueueGroup

				// Create NATS request-reply subscription with queue group for "at most one" delivery
				sub, err := eventBus.QueueSubscribe(entry.Subject, queueGroup, func(reqCtx context.Context, msg *types.Msg) {
					response, err := entry.RequestHandler(reqCtx, msg)
					if err != nil {
						logger.Error("RequestReply handler error", "service", serviceName, "module", modName, "error", err)

						// Send error response instead of leaving the client hanging
						if msg.Reply != "" {
							errorMsg := &types.Msg{
								Subject: msg.Reply,
								Header: types.Header{
									types.HeaderError:        []string{"true"},
									types.HeaderErrorMessage: []string{err.Error()},
								},
								Data: nil,
							}

							// Add error type classification if available (optional header).
							// Uses reflection to extract the error type name.
							if errorType := getErrorTypeName(err); errorType != "" {
								errorMsg.Header[types.HeaderErrorType] = []string{errorType}
							}

							if pubErr := eventBus.PublishMsg(errorMsg); pubErr != nil {
								logger.Error("Failed to publish error response", "service", serviceName, "module", modName, "error", pubErr)
							}
						}
						return
					}
					// Manually publish response to the reply subject when sender specifies one
					// automatic reply handling to maintain explicit control over error handling
					if msg.Reply != "" && response != nil {
						if pubErr := eventBus.Publish(msg.Reply, response); pubErr != nil {
							logger.Error("Failed to publish response", "service", serviceName, "error", pubErr)
						}
					} else {
						logger.Warn("No reply subject specified, response not sent", "service", serviceName)
					}
				})
				if err != nil {
					return fmt.Errorf("failed to subscribe to %s: %w", entry.Subject, err)
				}

				lm.mu.Lock()
				lm.subscriptions[moduleName] = append(lm.subscriptions[moduleName], sub)
				lm.mu.Unlock()

			case types.ServiceTypeQueueGroup:
				// Create NATS queue group subscription
				// Iterate over all queue group handler pairs
				for _, pair := range entry.QueueHandlers {
					queueGroup := pair.QueueGroup
					handler := pair.Handler
					serviceName := entry.Name
					eventBus := lm.eventBus
					logger := lm.logger

					sub, err := eventBus.QueueSubscribe(entry.Subject, queueGroup, func(reqCtx context.Context, msg *types.Msg) {
						// Immediately send ACK before processing
						if msg.Reply != "" {
							if pubErr := eventBus.Publish(msg.Reply, []byte{}); pubErr != nil {
								logger.Error("Failed to send ACK for QueueGroup message",
									"service", serviceName,
									"queueGroup", queueGroup,
									"error", pubErr)
								return
							}
						}

						// Call handler synchronously (handler can spawn goroutine if async needed)
						if err := handler(reqCtx, msg); err != nil {
							logger.Error("QueueGroup handler error",
								"service", serviceName,
								"queueGroup", queueGroup,
								"error", err)
						}
					})
					if err != nil {
						return fmt.Errorf("failed to queue subscribe to %s (queue: %s): %w", entry.Subject, queueGroup, err)
					}

					lm.mu.Lock()
					lm.subscriptions[moduleName] = append(lm.subscriptions[moduleName], sub)
					lm.mu.Unlock()
				}

			case types.ServiceTypeStreamConsumer:
				// Setup JetStream stream consumer
				if err := lm.setupStreamConsumer(ctx, entry); err != nil {
					return fmt.Errorf("failed to setup stream consumer %s: %w", entry.Name, err)
				}

			case types.ServiceTypeCron:
				// Setup server-side cron schedule + durable consumer
				if err := lm.setupCronService(ctx, entry); err != nil {
					return fmt.Errorf("failed to setup cron service %s: %w", entry.Name, err)
				}
				registeredCronStreams[types.CronStreamName(entry.ModuleName, entry.Name)] = struct{}{}
			}
		}
	}

	// Best-effort: warn about cron schedules left orphaned by a removed
	// registration. Only meaningful when cron services are in use.
	if len(registeredCronStreams) > 0 {
		lm.warnOrphanedCronStreams(ctx, registeredCronStreams)
	}

	// Setup event consumer subscriptions from EventRegistry.
	// Uses QueueSubscribe to enable load balancing across multiple consumer instances
	// in the same queue group (defaults to module name if not specified).
	// This ensures only one consumer in the queue group processes each event.
	if lm.eventRegistry != nil {
		consumers := lm.eventRegistry.Entries()
		for _, consumer := range consumers {
			lm.logger.Debug("Setting up event subscription",
				"consumer_module", consumer.Module.Name(),
				"event_module", consumer.EventDef.ModuleName,
				"event", consumer.EventDef.Name,
				"subject", consumer.EventDef.Subject,
				"queue_group", consumer.QueueGroup)

			// Capture handler to avoid closure issues
			handler := consumer.Handler
			eventBus := lm.eventBus
			logger := lm.logger
			eventName := consumer.EventDef.Name
			consumerModule := consumer.Module.Name()
			queueGroup := consumer.QueueGroup

			sub, err := eventBus.QueueSubscribe(consumer.EventDef.Subject, queueGroup, func(ctx context.Context, msg *types.Msg) {
				if err := handler(ctx, msg); err != nil {
					logger.Error("Event consumer handler error",
						"consumer_module", consumerModule,
						"event", eventName,
						"error", err)
				}
			})
			if err != nil {
				return fmt.Errorf("failed to subscribe to event %s.%s: %w",
					consumer.EventDef.ModuleName, consumer.EventDef.Name, err)
			}

			lm.mu.Lock()
			lm.subscriptions[consumer.Module.Name()] = append(lm.subscriptions[consumer.Module.Name()], sub)
			lm.mu.Unlock()
		}

		// Setup event stream consumers from EventRegistry.
		// These use JetStream durable pull consumers for at-least-once delivery.
		streamConsumers := lm.eventRegistry.StreamConsumerEntries()
		for _, entry := range streamConsumers {
			if err := lm.setupEventStreamConsumer(ctx, entry); err != nil {
				return fmt.Errorf("failed to setup event stream consumer for %s.%s: %w",
					entry.EventDef.ModuleName, entry.EventDef.Name, err)
			}
		}
	}

	lm.logger.Debug("NATS subscriptions setup complete")
	return nil
}

// Stop stops all modules in reverse dependency order
func (lm *lifecycleManager) Stop(ctx context.Context) error {
	lm.logger.Info("Stopping all modules")

	// Cancel runtime context to signal all handlers to stop
	lm.cancelRuntime()

	// Teardown NATS subscriptions first
	if err := lm.teardownNATSSubscriptions(ctx); err != nil {
		lm.logger.Error("Error tearing down NATS subscriptions", "error", err)
	}

	// Get modules in reverse dependency order
	ordered, err := registry.ResolveDependencies(lm.registry)
	if err != nil {
		return fmt.Errorf("failed to resolve dependencies: %w", err)
	}

	// Reverse the order for shutdown
	for i, j := 0, len(ordered)-1; i < j; i, j = i+1, j-1 {
		ordered[i], ordered[j] = ordered[j], ordered[i]
	}

	// Stop each regular module (skip middleware modules)
	var errors []error
	for _, module := range ordered {
		// Skip middleware modules (stopped last)
		if _, ok := module.(types.MiddlewareModule); ok {
			continue
		}

		if err := lm.stopModule(ctx, module); err != nil {
			errors = append(errors, err)
		}
	}

	// Stop middleware modules (in reverse registration order)
	if lm.middlewareChain != nil {
		middlewares := lm.middlewareChain.Middlewares()
		for i := len(middlewares) - 1; i >= 0; i-- {
			mw := middlewares[i]
			if err := lm.stopMiddlewareModule(ctx, mw); err != nil {
				errors = append(errors, err)
			}
		}
	}

	// Stop plugin modules LAST (in reverse registration order)
	if err := lm.stopPlugins(ctx); err != nil {
		errors = append(errors, err)
	}

	// Signal shutdown complete
	lm.shutdownOnce.Do(func() {
		close(lm.shutdownCh)
	})

	if len(errors) > 0 {
		return monoerrors.AggregateErrors(errors)
	}

	lm.logger.Info("All modules stopped successfully")
	return nil
}

// stopModule stops a single module with panic recovery
func (lm *lifecycleManager) stopModule(ctx context.Context, module types.Module) (err error) {
	moduleName := module.Name()

	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			err = monoerrors.WrapModulePanic(moduleName, "stop", r)
			lm.logger.Error("Module panic during stop", "module", moduleName, "panic", r)
		}
	}()

	lm.logger.Info("Stopping module", "module", moduleName)

	if err := module.Stop(ctx); err != nil {
		lm.logger.Error("Module stop failed", "module", moduleName, "error", err)

		// Notify middleware of module stopped with error
		if lm.middlewareChain != nil {
			event := types.ModuleLifecycleEvent{
				Type:       types.ModuleStoppedEvent,
				ModuleName: moduleName,
				Error:      err,
				Metadata:   make(map[string]any),
			}
			lm.middlewareChain.RunModuleLifecycle(ctx, event)
		}

		return monoerrors.WrapModuleStopFailed(moduleName, err)
	}

	// Notify middleware of module stopped successfully
	if lm.middlewareChain != nil {
		event := types.ModuleLifecycleEvent{
			Type:       types.ModuleStoppedEvent,
			ModuleName: moduleName,
			Error:      nil,
			Metadata:   make(map[string]any),
		}
		lm.middlewareChain.RunModuleLifecycle(ctx, event)
	}

	lm.logger.Info("Module stopped", "module", moduleName)
	return nil
}

// stopMiddlewareModule stops a single middleware module with panic recovery
func (lm *lifecycleManager) stopMiddlewareModule(ctx context.Context, mw types.MiddlewareModule) (err error) {
	moduleName := mw.Name()

	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			err = monoerrors.WrapModulePanic(moduleName, "stop", r)
			lm.logger.Error("Middleware module panic during stop", "module", moduleName, "panic", r)
		}
	}()

	lm.logger.Info("Stopping middleware module", "module", moduleName)

	if err := mw.Stop(ctx); err != nil {
		lm.logger.Error("Middleware module stop failed", "module", moduleName, "error", err)
		return fmt.Errorf("failed to stop middleware module %s: %w", moduleName, err)
	}

	lm.logger.Info("Middleware module stopped", "module", moduleName)
	return nil
}

// teardownNATSSubscriptions drains and unsubscribes all NATS subscriptions
func (lm *lifecycleManager) teardownNATSSubscriptions(ctx context.Context) error {
	lm.logger.Debug("Tearing down NATS subscriptions")

	// Cancel all stream consumer loops first (no deletion - durable!)
	lm.mu.RLock()
	streamConsumers := make(map[string]context.CancelFunc, len(lm.streamConsumers))
	for k, v := range lm.streamConsumers {
		streamConsumers[k] = v
	}
	lm.mu.RUnlock()

	for name, cancel := range streamConsumers {
		lm.logger.Debug("Stopping stream consumer", "service", name)
		cancel()
	}

	lm.mu.RLock()
	subscriptions := make(map[string][]types.Subscription, len(lm.subscriptions))
	for k, v := range lm.subscriptions {
		subscriptions[k] = v
	}
	lm.mu.RUnlock()

	// Drain all subscriptions concurrently
	var wg sync.WaitGroup
	for moduleName, subs := range subscriptions {
		for _, sub := range subs {
			wg.Add(1)
			go func(s types.Subscription, mod string) {
				defer wg.Done()
				if err := s.Drain(); err != nil {
					lm.logger.Error("Failed to drain subscription", "module", mod, "error", err)
				}
			}(sub, moduleName)
		}
	}

	// Wait for drains to complete (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		lm.logger.Info("All subscriptions drained successfully")
	case <-time.After(5 * time.Second):
		lm.logger.Warn("Drain timeout exceeded, some subscriptions may not have drained")
	case <-ctx.Done():
		lm.logger.Warn("Drain interrupted by context cancellation")
	}

	lm.logger.Info("NATS subscriptions teardown complete")
	return nil
}

// rollback stops all started modules in reverse order
func (lm *lifecycleManager) rollback(ctx context.Context, started []string, originalErr error) error {
	lm.logger.Error("Rolling back module startup", "error", originalErr, "started", len(started))

	// Stop modules in reverse order
	for i := len(started) - 1; i >= 0; i-- {
		moduleName := started[i] //nolint:gosec // G602: bounds are checked by loop condition (i >= 0)
		module, err := lm.registry.Get(moduleName)
		if err != nil {
			lm.logger.Error("Failed to get module during rollback", "module", moduleName, "error", err)
			continue
		}

		if err := lm.stopModule(ctx, module); err != nil {
			lm.logger.Error("Failed to stop module during rollback", "module", moduleName, "error", err)
		}
	}

	return fmt.Errorf("module startup failed, rolled back %d modules: %w", len(started), originalErr)
}

// WaitForShutdown blocks until shutdown signal or context cancellation
func (lm *lifecycleManager) WaitForShutdown(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-lm.shutdownCh:
		return nil
	}
}

// GetRuntimeContext returns the context that is passed to message handlers.
// This context is cancelled when the framework shuts down.
func (lm *lifecycleManager) GetRuntimeContext() context.Context {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.runtimeCtx
}

// GetServiceContainer returns the service container for a registered module.
// Returns nil if the module is not found or hasn't been started yet.
func (lm *lifecycleManager) GetServiceContainer(moduleName string) types.ServiceContainer {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.containers[moduleName]
}

// GetPlugin returns the plugin registered under the given alias.
// Returns nil if no plugin with that alias exists.
func (lm *lifecycleManager) GetPlugin(alias string) types.PluginModule {
	plugin, err := lm.pluginRegistry.Get(alias)
	if err != nil {
		return nil
	}
	return plugin
}

// GetPluginContainer returns the service container for a registered plugin.
// Returns nil if the plugin is not found or hasn't been started yet.
func (lm *lifecycleManager) GetPluginContainer(alias string) types.ServiceContainer {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.pluginContainers[alias]
}

// startPlugins initializes and starts all registered plugins.
// Plugins start before middleware modules and regular modules.
func (lm *lifecycleManager) startPlugins(ctx context.Context) error {
	aliases := lm.pluginRegistry.List()
	if len(aliases) == 0 {
		return nil
	}

	plugins := lm.pluginRegistry.All()
	lm.logger.Info("Starting plugins", "count", len(aliases))

	startedAliases := []string{}

	for _, alias := range aliases {
		plugin := plugins[alias]
		if err := lm.startPlugin(ctx, plugin, alias); err != nil {
			// Rollback already started plugins
			lm.rollbackPlugins(ctx, startedAliases)
			return err
		}
		startedAliases = append(startedAliases, alias)
	}

	return nil
}

// startPlugin executes the initialization sequence for a single plugin.
func (lm *lifecycleManager) startPlugin(ctx context.Context, plugin types.PluginModule, alias string) (err error) {
	start := time.Now()

	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			err = monoerrors.WrapPluginStartFailed(alias, fmt.Errorf("panic: %v", r))
			lm.logger.Error("Plugin panic during start", "alias", alias, "panic", r)
		}
	}()

	lm.logger.Info("Starting plugin", "alias", alias, "module", plugin.Name())

	// Step 0: Create dedicated ServiceContainer for plugin.
	// Note: Middleware chain is NOT set on plugin containers - plugins are
	// explicitly excluded from middleware hooks by design (see pkg/types/plugin.go).
	// This means service registrations in plugins won't be intercepted by middleware.
	pluginContainer := container.New(lm.logger.WithModule(plugin.Name()))
	if err := pluginContainer.BindModule(plugin); err != nil {
		return monoerrors.WrapPluginStartFailed(alias, err)
	}
	pluginContainer.SetEventBus(lm.eventBus)

	// Step 1: Inject container into plugin
	plugin.SetContainer(pluginContainer)

	// Step 1b: Set EventBus on plugin (if EventBusAwareModule)
	if eventBusAware, ok := plugin.(types.EventBusAwareModule); ok {
		eventBusAware.SetEventBus(lm.eventBus)
	}

	lm.mu.Lock()
	lm.pluginContainers[alias] = pluginContainer
	lm.mu.Unlock()

	// Step 2: Register services (if ServiceProviderModule)
	if serviceProvider, ok := plugin.(types.ServiceProviderModule); ok {
		if err := serviceProvider.RegisterServices(pluginContainer); err != nil {
			return monoerrors.WrapPluginStartFailed(alias, err)
		}
	}

	// Step 3: Start the plugin
	if err := plugin.Start(ctx); err != nil {
		return monoerrors.WrapPluginStartFailed(alias, err)
	}

	lm.logger.Info("Plugin started", "alias", alias, "module", plugin.Name(), "duration", time.Since(start))
	return nil
}

// stopPlugins stops all plugins in reverse registration order.
func (lm *lifecycleManager) stopPlugins(ctx context.Context) error {
	aliases := lm.pluginRegistry.List()
	if len(aliases) == 0 {
		return nil
	}

	plugins := lm.pluginRegistry.All()
	var errors []error

	lm.logger.Info("Stopping plugins", "count", len(aliases))

	// Reverse order
	for i := len(aliases) - 1; i >= 0; i-- {
		alias := aliases[i]
		plugin := plugins[alias]

		if err := lm.stopPlugin(ctx, plugin, alias); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return monoerrors.AggregateErrors(errors)
	}
	return nil
}

// stopPlugin stops a single plugin with panic recovery.
func (lm *lifecycleManager) stopPlugin(ctx context.Context, plugin types.PluginModule, alias string) (err error) {
	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			err = monoerrors.WrapPluginStopFailed(alias, fmt.Errorf("panic: %v", r))
			lm.logger.Error("Plugin panic during stop", "alias", alias, "panic", r)
		}
	}()

	lm.logger.Info("Stopping plugin", "alias", alias, "module", plugin.Name())

	if err := plugin.Stop(ctx); err != nil {
		lm.logger.Error("Plugin stop failed", "alias", alias, "error", err)
		return monoerrors.WrapPluginStopFailed(alias, err)
	}

	lm.logger.Info("Plugin stopped", "alias", alias, "module", plugin.Name())
	return nil
}

// rollbackPlugins stops all started plugins in reverse order during startup failure.
// It also cleans up the pluginContainers map to remove stale references.
func (lm *lifecycleManager) rollbackPlugins(ctx context.Context, startedAliases []string) {
	if len(startedAliases) == 0 {
		return
	}

	lm.logger.Error("Rolling back plugin startup", "started", len(startedAliases))
	plugins := lm.pluginRegistry.All()

	for i := len(startedAliases) - 1; i >= 0; i-- {
		alias := startedAliases[i]
		plugin := plugins[alias]
		if err := lm.stopPlugin(ctx, plugin, alias); err != nil {
			lm.logger.Error("Failed to stop plugin during rollback", "alias", alias, "error", err)
		}
		// Clean up container reference
		lm.mu.Lock()
		delete(lm.pluginContainers, alias)
		lm.mu.Unlock()
	}
}

// setupStreamConsumer sets up a JetStream stream consumer with stream/consumer creation
// and starts the fetch loop goroutine.
func (lm *lifecycleManager) setupStreamConsumer(ctx context.Context, entry *types.ServiceEntry) error {
	cfg := entry.StreamConsumerConfig
	if cfg == nil {
		return fmt.Errorf("stream consumer config is nil for service %s", entry.Name)
	}

	// Get JetStream from EventBus
	es, err := lm.eventBus.EventStream()
	if err != nil {
		return fmt.Errorf("failed to get JetStream from EventBus: %w", err)
	}

	// Create/Update Stream (idempotent)
	// Use the embedded Stream config - subjects are already populated during registration
	streamCfg := cfg.Stream
	// Apply defaults for zero values if user didn't specify
	if streamCfg.Retention == 0 {
		streamCfg.Retention = types.LimitsPolicy
	}
	if streamCfg.Storage == 0 {
		streamCfg.Storage = types.FileStorage
	}
	_, err = es.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		return fmt.Errorf("failed to create stream %s: %w", cfg.Stream.Name, err)
	}

	// Create/Update Consumer (idempotent)
	consumerName := sanitizeConsumerName(entry.ModuleName + "-" + entry.Name)
	consumerCfg := cfg.Consumer
	// Set required fields that must be managed by the framework
	consumerCfg.Name = consumerName
	if consumerCfg.AckPolicy == 0 {
		consumerCfg.AckPolicy = types.AckExplicitPolicy
	}
	consumer, err := es.CreateOrUpdateConsumer(ctx, cfg.Stream.Name, consumerCfg)
	if err != nil {
		return fmt.Errorf("failed to create consumer %s: %w", consumerName, err)
	}

	// Start fetch loop goroutine
	loopCtx, cancel := context.WithCancel(lm.runtimeCtx) //nolint:gosec // G118: cancel is stored in lm.streamConsumers and called in shutdown
	lm.mu.Lock()
	lm.streamConsumers[entry.Name] = cancel
	lm.mu.Unlock()

	go lm.runStreamConsumerLoop(loopCtx, consumer, entry)

	lm.logger.Info("Stream consumer started",
		"service", entry.Name,
		"stream", cfg.Stream.Name,
		"consumer", consumerName,
		"batch_size", cfg.Fetch.BatchSize)

	return nil
}

// Default acknowledgement parameters for cron consumers, mirroring the
// stream-consumer defaults so retry behaviour is consistent across services.
const (
	defaultCronAckWait        = 30 * time.Second
	defaultCronMaxDeliver     = 3
	defaultCronFetchWait      = 5 * time.Second
	defaultCronMaxMsgsPerSubj = 256
	defaultCronStreamMaxAge   = 0 // unbounded: deleting the schedule message would stop the schedule
)

// setupCronService provisions the server-side cron schedule and, unless the
// service is deprecated, the durable consumer that delivers each occurrence to
// the handler. It reuses the JetStream machinery behind RegisterStreamConsumer:
// a per-service stream with message scheduling enabled, an idempotently
// (re)published schedule message, and a pull-consumer fetch loop.
func (lm *lifecycleManager) setupCronService(ctx context.Context, entry *types.ServiceEntry) error {
	cfg := entry.CronConfig
	if cfg == nil {
		return fmt.Errorf("cron config is nil for service %s", entry.Name)
	}
	if entry.ScheduleSubject == "" {
		return fmt.Errorf("cron schedule subject is empty for service %s", entry.Name)
	}

	es, err := lm.eventBus.EventStream()
	if err != nil {
		return fmt.Errorf("cron service %q requires JetStream (enable it via WithJetStreamStorageDir): %w", entry.Name, err)
	}

	module := entry.ModuleName
	streamName := types.CronStreamName(module, entry.Name)
	targetSubject := entry.Subject
	scheduleSubject := entry.ScheduleSubject
	controlSubject := types.CronControlSubject(targetSubject)

	// The stream covers the schedule/control sub-subjects plus the concrete
	// target subject the server republishes to. Enabling AllowMsgSchedules makes
	// the server implicitly enable AllowRollup (one schedule per subject) and
	// clear DenyPurge, so neither is set explicitly here. MaxMsgsPerSubject bounds
	// the accumulation of delivered ticks without a stream-wide MaxAge, which
	// could otherwise delete the durable schedule message for infrequent
	// schedules.
	streamCfg := types.StreamConfig{
		Name:              streamName,
		Subjects:          []string{targetSubject, types.CronSubjectsWildcard(targetSubject)},
		Retention:         types.LimitsPolicy,
		Storage:           types.FileStorage,
		AllowMsgSchedules: true,
		MaxMsgsPerSubject: defaultCronMaxMsgsPerSubj,
		MaxAge:            defaultCronStreamMaxAge,
	}
	if cfg.SourceSubject != "" {
		streamCfg.Subjects = append(streamCfg.Subjects, cfg.SourceSubject)
	}
	if cfg.TTL > 0 {
		streamCfg.AllowMsgTTL = true
	}

	if _, err := es.CreateOrUpdateStream(ctx, streamCfg); err != nil {
		return fmt.Errorf("failed to create cron schedule stream %q (server may not support message schedules; requires nats-server v2.14+): %w", streamName, err)
	}

	// Deprecated: cancel the schedule and do not start a consumer. The purge is
	// idempotent — the server accepts it even when no schedule exists.
	if cfg.Deprecated {
		if err := lm.purgeCronSchedule(ctx, es, controlSubject, scheduleSubject); err != nil {
			return fmt.Errorf("failed to purge deprecated cron schedule %q: %w", entry.Name, err)
		}
		lm.logger.Info("Cron service deprecated; schedule purged",
			"service", entry.Name,
			"schedule_subject", scheduleSubject)
		return nil
	}

	// Publish the schedule message. Rollup-by-subject makes re-publishing on
	// every boot idempotent: a changed Schedule/Payload/TimeZone/TTL overwrites
	// the prior schedule in place.
	if err := lm.publishCronSchedule(ctx, es, entry); err != nil {
		return fmt.Errorf("failed to publish cron schedule %q: %w", entry.Name, err)
	}

	// Durable pull consumer filtered on the concrete target subject so it never
	// receives the internal schedule/control messages.
	consumerName := sanitizeConsumerName(module + "-" + entry.Name + "-cron")
	consumerCfg := types.ConsumerConfig{
		Name:          consumerName,
		FilterSubject: targetSubject,
		AckPolicy:     types.AckExplicitPolicy,
		AckWait:       defaultCronAckWait,
		MaxDeliver:    defaultCronMaxDeliver,
	}
	consumer, err := es.CreateOrUpdateConsumer(ctx, streamName, consumerCfg)
	if err != nil {
		return fmt.Errorf("failed to create cron consumer %q: %w", consumerName, err)
	}

	// Key the cancel func by a module-qualified, type-prefixed key so a cron
	// service never collides with a same-named service in another module (or a
	// stream consumer), which would overwrite the cancel func and leak this
	// fetch loop past shutdown.
	loopCtx, cancel := context.WithCancel(lm.runtimeCtx) //nolint:gosec // G118: cancel is stored in lm.streamConsumers and called in shutdown
	consumerKey := fmt.Sprintf("cron-%s-%s", entry.ModuleName, entry.Name)
	lm.mu.Lock()
	lm.streamConsumers[consumerKey] = cancel
	lm.mu.Unlock()

	go lm.runCronConsumerLoop(loopCtx, consumer, entry)

	lm.logger.Info("Cron service started",
		"service", entry.Name,
		"stream", streamName,
		"consumer", consumerName,
		"schedule", cfg.Schedule,
		"target_subject", targetSubject)

	return nil
}

// publishCronSchedule publishes the schedule message that drives the server-side
// scheduler. The schedule headers flow to the wire unchanged through the
// existing EventStream.PublishMsg path.
func (lm *lifecycleManager) publishCronSchedule(ctx context.Context, es types.EventStream, entry *types.ServiceEntry) error {
	cfg := entry.CronConfig
	hdr := types.Header{
		types.HeaderNatsSchedule:       []string{cfg.Schedule},
		types.HeaderNatsScheduleTarget: []string{entry.Subject},
	}
	if cfg.TimeZone != "" {
		hdr[types.HeaderNatsScheduleTimeZone] = []string{cfg.TimeZone}
	}
	if cfg.TTL > 0 {
		hdr[types.HeaderNatsScheduleTTL] = []string{cfg.TTL.String()}
	}
	if cfg.SourceSubject != "" {
		hdr[types.HeaderNatsScheduleSource] = []string{cfg.SourceSubject}
	}
	_, err := es.PublishMsg(ctx, &types.Msg{
		Subject: entry.ScheduleSubject,
		Data:    cfg.Payload,
		Header:  hdr,
	})
	return err
}

// purgeCronSchedule cancels a server-side schedule. The purge must be published
// to a subject different from the schedule subject (here the control subject),
// naming the schedule to purge via the Nats-Scheduler header.
func (lm *lifecycleManager) purgeCronSchedule(ctx context.Context, es types.EventStream, controlSubject, scheduleSubject string) error {
	_, err := es.PublishMsg(ctx, &types.Msg{
		Subject: controlSubject,
		Header: types.Header{
			types.HeaderNatsScheduleNext: []string{types.HeaderNatsScheduleNextPurge},
			types.HeaderNatsScheduler:    []string{scheduleSubject},
		},
	})
	return err
}

// runCronConsumerLoop fetches scheduled occurrences one at a time and dispatches
// each to the handler. Unlike the stream-consumer loop, the framework owns
// acknowledgement here (see dispatchCronTick).
func (lm *lifecycleManager) runCronConsumerLoop(ctx context.Context, consumer jetstream.Consumer, entry *types.ServiceEntry) {
	for {
		select {
		case <-ctx.Done():
			lm.logger.Info("Cron consumer loop stopping", "service", entry.Name)
			return
		default:
			if ctx.Err() != nil {
				lm.logger.Info("Cron consumer loop cancelled", "service", entry.Name)
				return
			}

			msgs, err := consumer.Fetch(1, jetstream.FetchMaxWait(defaultCronFetchWait))
			if err != nil {
				if ctx.Err() != nil {
					lm.logger.Info("Cron consumer loop cancelled", "service", entry.Name)
					return
				}
				if !errors.Is(err, nats.ErrTimeout) {
					lm.logger.Error("Cron fetch error", "service", entry.Name, "error", err)
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Second):
					}
				}
				continue
			}

			for msg := range msgs.Messages() {
				if ctx.Err() != nil {
					// Nak the in-flight occurrence so redelivery starts
					// immediately on the next run instead of waiting for AckWait.
					if nakErr := msg.Nak(); nakErr != nil {
						lm.logger.Error("Cron Nak on cancel failed", "service", entry.Name, "error", nakErr)
					}
					return
				}
				lm.dispatchCronTick(ctx, entry, eventbus.WrapJetStreamMsg(msg))
			}

			if msgs.Error() != nil && !errors.Is(msgs.Error(), nats.ErrTimeout) {
				lm.logger.Error("Cron fetch batch error", "service", entry.Name, "error", msgs.Error())
			}
		}
	}
}

// dispatchCronTick invokes the cron handler and acknowledges the occurrence on
// the handler's behalf: Ack on nil, Nak on error (including a recovered panic).
func (lm *lifecycleManager) dispatchCronTick(ctx context.Context, entry *types.ServiceEntry, msg *types.Msg) {
	if err := lm.invokeCronHandler(ctx, entry, msg); err != nil {
		lm.logger.Error("Cron handler error", "service", entry.Name, "error", err)
		if nakErr := msg.Nak(); nakErr != nil {
			lm.logger.Error("Cron Nak failed", "service", entry.Name, "error", nakErr)
		}
		return
	}
	if ackErr := msg.Ack(); ackErr != nil {
		lm.logger.Error("Cron Ack failed", "service", entry.Name, "error", ackErr)
	}
}

// invokeCronHandler calls the handler, converting a panic into an error so the
// loop survives and the occurrence is redelivered.
func (lm *lifecycleManager) invokeCronHandler(ctx context.Context, entry *types.ServiceEntry, msg *types.Msg) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cron handler panicked: %v", r)
		}
	}()
	return entry.CronHandler(ctx, msg)
}

// cronStreamNamesLister is the optional capability used to enumerate streams for
// best-effort orphan detection. The concrete NatsJetStream implements it.
type cronStreamNamesLister interface {
	StreamNames(ctx context.Context) ([]string, error)
}

// warnOrphanedCronStreams logs a warning for any framework-managed cron stream
// that has no matching registration this boot (a schedule that may still be
// firing server-side because its RegisterCronService call was removed without
// first setting Deprecated). It never deletes anything.
func (lm *lifecycleManager) warnOrphanedCronStreams(ctx context.Context, registered map[string]struct{}) {
	es, err := lm.eventBus.EventStream()
	if err != nil {
		return
	}
	lister, ok := es.(cronStreamNamesLister)
	if !ok {
		return
	}
	// Note: this enumerates every stream in the JetStream account once at
	// startup. It is best-effort and only runs when cron services are in use.
	names, err := lister.StreamNames(ctx)
	if err != nil {
		lm.logger.Debug("Could not list streams for cron orphan check", "error", err)
		return
	}
	for _, name := range names {
		if !strings.HasPrefix(name, types.CronStreamNamePrefix) {
			continue
		}
		if _, ok := registered[name]; !ok {
			lm.logger.Warn("Orphaned cron schedule stream detected (no matching registration); it may still be firing. To retire it cleanly, re-add the service with Deprecated:true to purge the schedule, or delete the stream manually",
				"stream", name)
		}
	}
}

// setupEventStreamConsumer sets up a JetStream stream consumer for an event
// and starts the fetch loop goroutine.
func (lm *lifecycleManager) setupEventStreamConsumer(ctx context.Context, entry types.EventStreamConsumerEntry) error {
	cfg := entry.Config

	// Get JetStream from EventBus
	es, err := lm.eventBus.EventStream()
	if err != nil {
		return fmt.Errorf("failed to get JetStream from EventBus: %w", err)
	}

	// Create/Update Stream (idempotent)
	// Stream.Subjects is already set to eventDef.Subject during registration
	streamCfg := cfg.Stream
	// Apply defaults for zero values if user didn't specify
	if streamCfg.Retention == 0 {
		streamCfg.Retention = types.LimitsPolicy
	}
	if streamCfg.Storage == 0 {
		streamCfg.Storage = types.FileStorage
	}
	_, err = es.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		return fmt.Errorf("failed to create stream %s: %w", cfg.Stream.Name, err)
	}

	// Create/Update Consumer (idempotent)
	// Use event name, version, and sequence ID for unique consumer naming
	// The sequence ID ensures uniqueness when multiple consumers subscribe to the same event
	consumerName := sanitizeConsumerName(fmt.Sprintf("%s-%s-%s-%d",
		entry.EventDef.ModuleName, entry.EventDef.Name, entry.EventDef.Version, entry.SequenceID))
	consumerCfg := cfg.Consumer
	// Set required fields that must be managed by the framework
	consumerCfg.Name = consumerName
	if consumerCfg.AckPolicy == 0 {
		consumerCfg.AckPolicy = types.AckExplicitPolicy
	}
	consumer, err := es.CreateOrUpdateConsumer(ctx, cfg.Stream.Name, consumerCfg)
	if err != nil {
		return fmt.Errorf("failed to create consumer %s: %w", consumerName, err)
	}

	// Start fetch loop goroutine
	loopCtx, cancel := context.WithCancel(lm.runtimeCtx) //nolint:gosec // G118: cancel is stored in lm.streamConsumers and called in shutdown
	// Use sequence ID for unique key to avoid collision with other consumers
	consumerKey := fmt.Sprintf("event-stream-%d", entry.SequenceID)
	lm.mu.Lock()
	lm.streamConsumers[consumerKey] = cancel
	lm.mu.Unlock()

	go lm.runEventStreamConsumerLoop(loopCtx, consumer, entry)

	lm.logger.Info("Event stream consumer started",
		"event_module", entry.EventDef.ModuleName,
		"event", entry.EventDef.Name,
		"version", entry.EventDef.Version,
		"stream", cfg.Stream.Name,
		"consumer", consumerName,
		"sequence_id", entry.SequenceID,
		"batch_size", cfg.Fetch.BatchSize)

	return nil
}

// runEventStreamConsumerLoop runs the fetch loop for an event stream consumer.
// It continuously fetches batches of messages and calls the handler.
func (lm *lifecycleManager) runEventStreamConsumerLoop(
	ctx context.Context,
	consumer jetstream.Consumer,
	entry types.EventStreamConsumerEntry,
) {
	cfg := entry.Config
	eventName := entry.EventDef.ModuleName + "." + entry.EventDef.Name

	for {
		select {
		case <-ctx.Done():
			lm.logger.Info("Event stream consumer loop stopping", "event", eventName)
			return
		default:
			// Check context cancellation first (shutdown signal takes priority)
			if ctx.Err() != nil {
				lm.logger.Info("Event stream consumer loop cancelled", "event", eventName)
				return
			}

			// Fetch batch with timeout
			msgs, err := consumer.Fetch(cfg.Fetch.BatchSize, jetstream.FetchMaxWait(cfg.Fetch.Timeout))
			if err != nil {
				// Context cancellation takes priority (shutdown signal)
				if ctx.Err() != nil {
					lm.logger.Info("Event stream consumer loop cancelled", "event", eventName)
					return
				}

				// Fetch timeout is expected when no messages are available - don't log as error
				if !errors.Is(err, nats.ErrTimeout) {
					lm.logger.Error("Fetch error", "event", eventName, "error", err)
					// Context-aware backoff to allow quick shutdown
					select {
					case <-ctx.Done():
						lm.logger.Info("Event stream consumer loop cancelled during backoff", "event", eventName)
						return
					case <-time.After(time.Second):
						// Continue to next iteration
					}
				}
				continue
			}

			// Collect messages into slice with pre-allocation for better memory efficiency
			batch := make([]*types.Msg, 0, cfg.Fetch.BatchSize)
			for msg := range msgs.Messages() {
				batch = append(batch, eventbus.WrapJetStreamMsg(msg))
			}

			// Check for fetch errors
			if msgs.Error() != nil && !errors.Is(msgs.Error(), nats.ErrTimeout) {
				lm.logger.Error("Fetch batch error", "event", eventName, "error", msgs.Error())
			}

			if len(batch) == 0 {
				continue
			}

			// Call handler with runtime context
			if err := entry.Handler(ctx, batch); err != nil {
				lm.logger.Error("Event stream consumer handler error",
					"event", eventName,
					"error", err,
					"batch_size", len(batch))
			}
		}
	}
}

// runStreamConsumerLoop runs the fetch loop for a stream consumer.
// It continuously fetches batches of messages and calls the handler.
func (lm *lifecycleManager) runStreamConsumerLoop(
	ctx context.Context,
	consumer jetstream.Consumer,
	entry *types.ServiceEntry,
) {
	cfg := entry.StreamConsumerConfig

	for {
		select {
		case <-ctx.Done():
			lm.logger.Info("Stream consumer loop stopping", "service", entry.Name)
			return
		default:
			// Check context cancellation first (shutdown signal takes priority)
			if ctx.Err() != nil {
				lm.logger.Info("Stream consumer loop cancelled", "service", entry.Name)
				return
			}

			// Fetch batch with timeout
			msgs, err := consumer.Fetch(cfg.Fetch.BatchSize, jetstream.FetchMaxWait(cfg.Fetch.Timeout))
			if err != nil {
				// Context cancellation takes priority (shutdown signal)
				if ctx.Err() != nil {
					lm.logger.Info("Stream consumer loop cancelled", "service", entry.Name)
					return
				}

				// Fetch timeout is expected when no messages are available - don't log as error
				if !errors.Is(err, nats.ErrTimeout) {
					lm.logger.Error("Fetch error", "service", entry.Name, "error", err)
					// Context-aware backoff to allow quick shutdown
					select {
					case <-ctx.Done():
						lm.logger.Info("Stream consumer loop cancelled during backoff", "service", entry.Name)
						return
					case <-time.After(time.Second):
						// Continue to next iteration
					}
				}
				continue
			}

			// Collect messages into slice with pre-allocation for better memory efficiency
			batch := make([]*types.Msg, 0, cfg.Fetch.BatchSize)
			for msg := range msgs.Messages() {
				batch = append(batch, eventbus.WrapJetStreamMsg(msg))
			}

			// Check for fetch errors
			if msgs.Error() != nil && !errors.Is(msgs.Error(), nats.ErrTimeout) {
				lm.logger.Error("Fetch batch error", "service", entry.Name, "error", msgs.Error())
			}

			if len(batch) == 0 {
				continue
			}

			// Call handler with runtime context
			if err := entry.StreamConsumerHandler(ctx, batch); err != nil {
				lm.logger.Error("Handler error",
					"service", entry.Name,
					"error", err,
					"batch_size", len(batch))
			}
		}
	}
}

// GetMiddlewareHook returns a function that can be called to run middleware lifecycle events.
// This is used to wire the registry to the middleware chain.
func (lm *lifecycleManager) GetMiddlewareHook() func(ctx context.Context, event types.ModuleLifecycleEvent) {
	return func(ctx context.Context, event types.ModuleLifecycleEvent) {
		// Use read lock to safely access middlewareChain (prevents data race)
		lm.mu.RLock()
		chain := lm.middlewareChain
		lm.mu.RUnlock()

		if chain != nil {
			chain.RunModuleLifecycle(ctx, event)
		}
	}
}

// sanitizeConsumerName converts a string to a valid JetStream consumer name.
// Consumer names must be alphanumeric with allowed separators: - _ .
var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9\-_.]`)

func sanitizeConsumerName(name string) string {
	// Replace spaces and common separators with hyphens
	name = strings.ReplaceAll(name, " ", "-")
	// Remove any remaining invalid characters
	name = nonAlphanumericRegex.ReplaceAllString(name, "")
	// Ensure not empty
	if name == "" {
		name = "consumer"
	}
	return name
}

// getErrorTypeName extracts a clean error type name using reflection.
// It returns the type name in lowercase with "Error" suffix removed.
// For example: *errors.ServiceError -> "service", *errors.TimeoutError -> "timeout"
// Returns empty string if the error type cannot be determined.
func getErrorTypeName(err error) string {
	if err == nil {
		return ""
	}

	// Get the underlying type, handling pointers
	t := reflect.TypeOf(err)
	if t == nil {
		return ""
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Get the type name and format it
	typeName := t.Name()
	if typeName == "" {
		return ""
	}

	// Remove "Error" suffix and convert to lowercase
	typeName = strings.TrimSuffix(typeName, "Error")
	return strings.ToLower(typeName)
}
