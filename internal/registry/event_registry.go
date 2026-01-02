// Package registry provides thread-safe module registration and retrieval
// with dependency resolution capabilities.
package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-monolith/mono/v1/pkg/errors"
	"github.com/go-monolith/mono/v1/pkg/types"
)

// eventRegistry implements the types.EventRegistry interface.
// It provides thread-safe storage for event definitions and consumer registrations.
type eventRegistry struct {
	mu                     sync.RWMutex
	events                 []types.BaseEventDefinition          // All registered events in registration order
	consumers              []types.EventConsumerEntry           // All registered consumers in registration order
	streamConsumers        []types.EventStreamConsumerEntry     // All registered stream consumers in registration order
	eventMap               map[string]types.BaseEventDefinition // Lookup map: "moduleName:name:version" -> BaseEventDefinition
	logger                 types.Logger
	chain                  types.MiddlewareChainRunner // Middleware chain for event consumer registration interception
	streamConsumerSequence int                         // Sequence counter for unique stream consumer IDs
}

// NewEventRegistry creates a new event registry instance with the provided logger.
// The logger must not be nil and is used for logging registry operations.
//
// Example:
//
//	registry := NewEventRegistry(logger)
//	err := registry.RegisterEvent(eventDef.ToBase())
func NewEventRegistry(logger types.Logger) types.EventRegistry {
	if logger == nil {
		panic("logger cannot be nil")
	}
	return &eventRegistry{
		events:          make([]types.BaseEventDefinition, 0),
		consumers:       make([]types.EventConsumerEntry, 0),
		streamConsumers: make([]types.EventStreamConsumerEntry, 0),
		eventMap:        make(map[string]types.BaseEventDefinition),
		logger:          logger,
	}
}

// makeEventKey creates a unique key for event lookup.
// Key format: "moduleName:name:version"
func makeEventKey(moduleName, name, version string) string {
	return fmt.Sprintf("%s:%s:%s", moduleName, name, version)
}

// RegisterEvent registers an event definition.
// Returns an error if an event with the same name, version, and module already exists.
//
// The subject is validated against the framework naming conventions:
//   - Must follow pattern: events.[<module>.]<domain>.[<sub-domain>].<event-type>
//   - All tokens must be kebab-case (lowercase, numbers, hyphens)
//   - Wildcards (*,>) are NOT allowed in event definitions
//   - Reserved prefix "_mono." is not allowed
//
// Example:
//
//	// Using generic EventDefinition[T]
//	err := registry.RegisterEvent(OrderCreatedV1.ToBase())
//
//	// Using BaseEventDefinition directly
//	err := registry.RegisterEvent(types.BaseEventDefinition{
//	    ModuleName: "order",
//	    Name:       "OrderCreated",
//	    Version:    "v1",
//	    Subject:    "events.orders.v1.created",
//	})
func (r *eventRegistry) RegisterEvent(def types.BaseEventDefinition) error {
	if def.ModuleName == "" {
		return fmt.Errorf("event module name cannot be empty")
	}
	if def.Name == "" {
		return fmt.Errorf("event name cannot be empty")
	}
	if def.Version == "" {
		return fmt.Errorf("event version cannot be empty")
	}
	if def.Subject == "" {
		return fmt.Errorf("event subject cannot be empty")
	}

	// Validate subject against framework naming conventions
	if err := errors.ValidateEventDefinitionSubject(def.Subject); err != nil {
		return fmt.Errorf("invalid event subject: %w", err)
	}

	key := makeEventKey(def.ModuleName, def.Name, def.Version)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check for duplicate
	if _, exists := r.eventMap[key]; exists {
		return fmt.Errorf("event %s.%s.%s already registered", def.ModuleName, def.Name, def.Version)
	}

	// Register the event
	r.events = append(r.events, def)
	r.eventMap[key] = def

	r.logger.Debug("Event registered",
		"module", def.ModuleName,
		"event", def.Name,
		"version", def.Version,
		"subject", def.Subject)

	return nil
}

// GetEventsByModule returns all events registered by a specific module.
// Returns an empty slice if no events are found for the module.
func (r *eventRegistry) GetEventsByModule(moduleName string) []types.BaseEventDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]types.BaseEventDefinition, 0)
	for _, event := range r.events {
		if event.ModuleName == moduleName {
			result = append(result, event)
		}
	}

	r.logger.Debug("Retrieved events by module",
		"module", moduleName,
		"count", len(result))

	return result
}

// GetEventByName returns the event definition by name, version, and module name.
// Parameter order: name first (what), version second (which version), moduleName third (from where).
// Returns the event definition and true if found, or an empty definition and false if not found.
//
// Example:
//
//	eventDef, ok := registry.GetEventByName("OrderCreated", "v1", "order")
//	if !ok {
//	    return fmt.Errorf("event not found")
//	}
func (r *eventRegistry) GetEventByName(name string, version string, moduleName string) (types.BaseEventDefinition, bool) {
	key := makeEventKey(moduleName, name, version)

	r.mu.RLock()
	defer r.mu.RUnlock()

	event, exists := r.eventMap[key]
	if !exists {
		r.logger.Debug("Event not found",
			"name", name,
			"version", version,
			"module", moduleName)
		return types.BaseEventDefinition{}, false
	}

	r.logger.Debug("Event found",
		"name", name,
		"version", version,
		"module", moduleName,
		"subject", event.Subject)

	return event, true
}

// GetAllEvents returns all registered event definitions in registration order.
// Returns a copy to prevent external modification.
func (r *eventRegistry) GetAllEvents() []types.BaseEventDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy to prevent external modification
	result := make([]types.BaseEventDefinition, len(r.events))
	copy(result, r.events)

	r.logger.Debug("Retrieved all events", "count", len(result))

	return result
}

// RegisterEventConsumer registers a consumer for an event.
// This DOES NOT create the NATS subscription - it only registers the intent.
// Framework will later call Entries() and set up NATS subscriptions.
// The queueGroup parameter is optional. If not provided or empty, defaults to module.Name().
//
// Example:
//
//	// Default queue group (uses module name)
//	err := registry.RegisterEventConsumer(eventDef.ToBase(), handler, module)
//
//	// Custom queue group (for multiple handlers of same event in same module)
//	err := registry.RegisterEventConsumer(eventDef.ToBase(), handler, module, "custom-queue")
func (r *eventRegistry) RegisterEventConsumer(eventDef types.BaseEventDefinition, handler types.EventConsumerHandler, module types.Module, queueGroup ...string) error {
	if handler == nil {
		return fmt.Errorf("event consumer handler cannot be nil")
	}
	if module == nil {
		return fmt.Errorf("event consumer module cannot be nil")
	}
	if eventDef.Subject == "" {
		return fmt.Errorf("event definition subject cannot be empty")
	}

	// Compute queue group: default to module name if not provided or empty
	computedQueueGroup := module.Name()
	if len(queueGroup) > 0 {
		if queueGroup[0] != "" {
			computedQueueGroup = queueGroup[0]
		}
		// Warn if multiple queue groups provided (only first is used)
		if len(queueGroup) > 1 {
			r.logger.Warn("Multiple queue groups provided, using first non-empty value",
				"provided_count", len(queueGroup),
				"used", computedQueueGroup,
				"consumer_module", module.Name())
		}
	}

	entry := types.EventConsumerEntry{
		EventDef:   eventDef,
		Handler:    handler,
		Module:     module,
		QueueGroup: computedQueueGroup,
	}

	// Run entry through middleware chain if available
	// This allows middleware to wrap handlers or modify the entry
	//
	// Note: The chain is set once during framework startup (before any modules start)
	// via SetMiddlewareChain. It is never modified afterward, so this read is safe
	// even though we release the lock before using the chain. The lifecycle manager
	// guarantees this ordering by setting the chain before starting any modules.
	r.mu.RLock()
	chain := r.chain
	r.mu.RUnlock()

	if chain != nil {
		entry = chain.RunEventConsumerRegistration(context.Background(), entry)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.consumers = append(r.consumers, entry)

	r.logger.Debug("Event consumer registered",
		"consumer_module", module.Name(),
		"event_module", eventDef.ModuleName,
		"event", eventDef.Name,
		"version", eventDef.Version,
		"subject", eventDef.Subject,
		"queue_group", computedQueueGroup)

	return nil
}

// Entries returns all registered event consumers in registration order.
// Framework uses this during setupNATSSubscriptions phase.
// Returns a copy to prevent external modification.
func (r *eventRegistry) Entries() []types.EventConsumerEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy to prevent external modification
	result := make([]types.EventConsumerEntry, len(r.consumers))
	copy(result, r.consumers)

	r.logger.Debug("Retrieved event consumers", "count", len(result))

	return result
}

// SetMiddlewareChain sets the middleware chain for event consumer registration interception.
// This allows middleware to wrap event consumer handlers or modify entries before they are stored.
func (r *eventRegistry) SetMiddlewareChain(chain types.MiddlewareChainRunner) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chain = chain
}

// RegisterEventStreamConsumer registers a JetStream durable consumer for an event.
// The config.Stream.Subjects will be overridden with []string{eventDef.Subject}.
// This provides durable, at-least-once delivery for event consumers using JetStream.
//
// Example:
//
//	config := types.StreamConsumerConfig{
//	    Stream: types.StreamConfig{
//	        Name:      "order-events",
//	        Retention: types.WorkQueuePolicy,
//	    },
//	    Fetch: types.FetchConfig{BatchSize: 10},
//	}
//	err := registry.RegisterEventStreamConsumer(orderCreatedV1.ToBase(), config, handler, m)
func (r *eventRegistry) RegisterEventStreamConsumer(eventDef types.BaseEventDefinition, config types.StreamConsumerConfig, handler types.EventStreamConsumerHandler, module types.Module) error {
	if handler == nil {
		return fmt.Errorf("event stream consumer handler cannot be nil")
	}
	if module == nil {
		return fmt.Errorf("event stream consumer module cannot be nil")
	}
	if eventDef.Subject == "" {
		return fmt.Errorf("event definition subject cannot be empty")
	}
	if config.Stream.Name == "" {
		return fmt.Errorf("stream name cannot be empty")
	}

	// Override the stream subjects with the event definition subject
	// This ensures the stream only captures messages for this specific event
	config.Stream.Subjects = []string{eventDef.Subject}

	// Apply default fetch config if not set
	if config.Fetch.BatchSize == 0 {
		config.Fetch.BatchSize = 10
	}
	if config.Fetch.Timeout == 0 {
		config.Fetch.Timeout = 5 * time.Second
	}

	// Assign unique sequence ID for this consumer
	// Must be done under lock to ensure unique IDs
	r.mu.Lock()
	r.streamConsumerSequence++
	sequenceID := r.streamConsumerSequence
	r.mu.Unlock()

	entry := types.EventStreamConsumerEntry{
		EventDef:   eventDef,
		Config:     config,
		Handler:    handler,
		Module:     module,
		SequenceID: sequenceID,
	}

	// Run entry through middleware chain if available
	// This allows middleware to wrap handlers or modify the entry
	//
	// Note: The chain is set once during framework startup (before any modules start)
	// via SetMiddlewareChain. It is never modified afterward, so this read is safe
	// even though we release the lock before using the chain. The lifecycle manager
	// guarantees this ordering by setting the chain before starting any modules.
	r.mu.RLock()
	chain := r.chain
	r.mu.RUnlock()

	if chain != nil {
		entry = chain.RunEventStreamConsumerRegistration(context.Background(), entry)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.streamConsumers = append(r.streamConsumers, entry)

	r.logger.Debug("Event stream consumer registered",
		"consumer_module", module.Name(),
		"event_module", eventDef.ModuleName,
		"event", eventDef.Name,
		"version", eventDef.Version,
		"subject", eventDef.Subject,
		"stream", config.Stream.Name,
		"sequence_id", sequenceID)

	return nil
}

// StreamConsumerEntries returns all registered event stream consumers in registration order.
// Framework uses this during setupNATSSubscriptions phase to create JetStream streams and consumers.
// Returns a copy to prevent external modification.
func (r *eventRegistry) StreamConsumerEntries() []types.EventStreamConsumerEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy to prevent external modification
	result := make([]types.EventStreamConsumerEntry, len(r.streamConsumers))
	copy(result, r.streamConsumers)

	r.logger.Debug("Retrieved event stream consumers", "count", len(result))

	return result
}
