// Package container provides service container implementation for managing
// and invoking services (channel-based, request-reply, queue groups, stream consumers).
package container

import (
	"context"
	"fmt"
	"sync"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// serviceContainer implements types.ServiceContainer with thread-safe service storage.
type serviceContainer struct {
	mu                         sync.RWMutex
	boundModule                types.Module
	services                   map[string]*types.ServiceEntry
	logger                     types.Logger
	eventBus                   types.EventBus              // For RequestReply, QueueGroup, and StreamConsumer services
	queueGroupOptimisticWindow time.Duration               // Optimistic publish window for queue groups (0 = disabled)
	middlewareChain            types.MiddlewareChainRunner // Middleware chain for service registration interception
}

// NewServiceContainer creates a new ServiceContainer instance.
func NewServiceContainer(logger types.Logger) types.ServiceContainer {
	return &serviceContainer{
		services: make(map[string]*types.ServiceEntry),
		logger:   logger,
	}
}

// SetEventBus sets the EventBus for NATS-based services.
// This must be called before registering RequestReply, QueueGroup, or StreamConsumer services.
func (c *serviceContainer) SetEventBus(bus types.EventBus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventBus = bus
}

// SetQueueGroupOptimisticWindow configures the optimistic publish window for queue group services.
// When window > 0, queue group clients will use fire-and-forget publish after a successful ACK.
// When window = 0 (default), all sends use ACK mode.
func (c *serviceContainer) SetQueueGroupOptimisticWindow(window time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueGroupOptimisticWindow = window
}

// SetMiddlewareChain sets the middleware chain for service registration interception.
// This must be called before registering any services if middleware is needed.
func (c *serviceContainer) SetMiddlewareChain(chain types.MiddlewareChainRunner) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.middlewareChain = chain
}

// BindModule binds this container to a module.
// Must be called before registering any services.
func (c *serviceContainer) BindModule(module types.Module) error {
	if module == nil {
		return monoerrors.WrapInvalidModule(nil, "module cannot be nil")
	}

	moduleName := module.Name()
	if moduleName == "" {
		return monoerrors.WrapInvalidModule(module, "module name cannot be empty")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.boundModule != nil {
		return fmt.Errorf("container already bound to module '%s'", c.boundModule.Name())
	}

	c.boundModule = module
	c.logger.Debug("ServiceContainer bound to module")

	return nil
}

// Has checks if a service with the given name is registered.
func (c *serviceContainer) Has(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.services[name]
	return exists
}

// Unregister removes a service from the container.
func (c *serviceContainer) Unregister(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.services[name]
	if !exists {
		if c.boundModule != nil {
			return monoerrors.WrapServiceNotFound(name, c.boundModule.Name())
		}
		return monoerrors.WrapServiceNotFound(name, "<unbound>")
	}

	// Clean up channel services
	// Note: We don't close channels here because:
	// 1. Closing an already-closed channel panics
	// 2. The registering code owns the channel lifecycle
	// 3. Multiple unregister calls or external closes would cause panics
	// The caller is responsible for closing channels they own
	//
	// Router goroutines are managed by the lifecycle manager and will exit when
	// lm.runtimeCtx is cancelled during framework shutdown.

	delete(c.services, name)

	c.logger.Info("Service unregistered",
		"service", name,
		"type", types.FormatServiceType(entry.Type))

	return nil
}

// Entries returns all registered ServiceEntry pointers.
func (c *serviceContainer) Entries() []*types.ServiceEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entries := make([]*types.ServiceEntry, 0, len(c.services))
	for _, entry := range c.services {
		entries = append(entries, entry)
	}

	return entries
}

// validateServiceName validates that a service name is kebab-case.
func validateServiceName(name string) error {
	if name == "" {
		return fmt.Errorf("service name cannot be empty")
	}

	// Use the same kebab-case validation as subject validation
	// Service names must be kebab-case (lowercase, numbers, hyphens)
	// Check for leading/trailing hyphens or consecutive hyphens
	if name[0] == '-' || name[len(name)-1] == '-' {
		return fmt.Errorf("service name '%s' must be kebab-case (lowercase, numbers, hyphens)", name)
	}

	prevHyphen := false
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			prevHyphen = false
			continue
		}
		if r == '-' {
			if prevHyphen {
				return fmt.Errorf("service name '%s' must be kebab-case (no consecutive hyphens)", name)
			}
			prevHyphen = true
			continue
		}
		return fmt.Errorf("service name '%s' must be kebab-case (lowercase, numbers, hyphens)", name)
	}

	return nil
}

// computeServiceSubject computes the NATS subject for a service.
// Format: services.<module>.<service>
func (c *serviceContainer) computeServiceSubject(serviceName string) (string, error) {
	if c.boundModule == nil {
		return "", monoerrors.ErrContainerNotBound
	}

	subject := fmt.Sprintf("services.%s.%s", c.boundModule.Name(), serviceName)

	// Validate the computed subject
	if err := monoerrors.ValidateServiceSubject(subject); err != nil {
		return "", err
	}

	return subject, nil
}

// checkDuplicate checks if a service is already registered.
func (c *serviceContainer) checkDuplicate(name string) error {
	if entry, exists := c.services[name]; exists {
		return monoerrors.WrapServiceAlreadyRegistered(name, entry.ModuleName, entry.Type)
	}
	return nil
}

// registerService is a common helper for registering services.
func (c *serviceContainer) registerService(name string, entry *types.ServiceEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate container is bound
	if c.boundModule == nil {
		return monoerrors.ErrContainerNotBound
	}

	// Validate EventBus is available for NATS-based services
	if (entry.Type == types.ServiceTypeRequestReply || entry.Type == types.ServiceTypeQueueGroup) && c.eventBus == nil {
		return fmt.Errorf("EventBus must be set before registering %s services", types.FormatServiceType(entry.Type))
	}

	// Validate service name
	if err := validateServiceName(name); err != nil {
		return err
	}

	// Check for duplicates
	if err := c.checkDuplicate(name); err != nil {
		return err
	}

	// Set common fields
	entry.Name = name
	entry.ModuleName = c.boundModule.Name()
	entry.Created = time.Now()

	// Run through middleware chain if available
	if c.middlewareChain != nil {
		// Build ServiceRegistration from entry
		reg := buildServiceRegistration(entry)

		// Run through middleware chain (may wrap handlers, modify config)
		reg = c.middlewareChain.RunServiceRegistration(context.Background(), reg)

		// Apply modifications back to entry
		applyServiceRegistration(entry, reg)
	}

	// Store service
	c.services[name] = entry

	c.logger.Info("Service registered",
		"service", name,
		"type", types.FormatServiceType(entry.Type))

	return nil
}

// buildServiceRegistration creates a ServiceRegistration from a ServiceEntry.
func buildServiceRegistration(entry *types.ServiceEntry) types.ServiceRegistration {
	return types.ServiceRegistration{
		Type:                 entry.Type,
		Name:                 entry.Name,
		ModuleName:           entry.ModuleName,
		Subject:              entry.Subject,
		RequestHandler:       entry.RequestHandler,
		QueueHandlers:        entry.QueueHandlers,
		StreamHandler:        entry.StreamConsumerHandler,
		StreamConsumerConfig: entry.StreamConsumerConfig,
		InChannel:            entry.InChannel,
		OutChannel:           entry.OutChannel,
		Metadata:             make(map[string]any),
	}
}

// applyServiceRegistration applies modifications from ServiceRegistration back to ServiceEntry.
func applyServiceRegistration(entry *types.ServiceEntry, reg types.ServiceRegistration) {
	// Apply handler modifications (middleware may have wrapped them)
	entry.RequestHandler = reg.RequestHandler
	entry.QueueHandlers = reg.QueueHandlers
	entry.StreamConsumerHandler = reg.StreamHandler

	// Apply config modifications
	entry.StreamConsumerConfig = reg.StreamConsumerConfig

	// Apply channel modifications (in case middleware changed them)
	entry.InChannel = reg.InChannel
	entry.OutChannel = reg.OutChannel
}
