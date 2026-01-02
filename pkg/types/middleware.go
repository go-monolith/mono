package types

import (
	"context"
	"time"
)

// MiddlewareModule provides interceptor hooks for framework events.
//
// Middleware modules are auto-discovered by the lifecycle manager via type assertion
// and chained in registration order. Each hook receives event data and can modify it
// before passing to the next middleware in the chain.
//
// Middleware modules have special lifecycle ordering:
//   - Start: Called BEFORE regular modules start
//   - Stop: Called AFTER regular modules stop
//
// This ensures middleware can observe and modify the entire framework lifecycle.
//
// Example implementations:
//   - Audit logging: Observe events without modification
//   - Handler wrapping: Wrap service handlers for auto-encoding/decoding
//   - Config modification: Override service configuration
//
// See docs/spec/monolith-framework/design.md Middleware Module section.
type MiddlewareModule interface {
	Module

	// OnModuleLifecycle intercepts module lifecycle events.
	// Middleware can observe or modify event metadata before it continues through the chain.
	// Returns the (possibly modified) event for the next middleware.
	//
	// Example uses:
	//   - Audit logging of module start/stop
	//   - Adding custom metadata
	//   - Tracking module lifecycle timing
	OnModuleLifecycle(ctx context.Context, event ModuleLifecycleEvent) ModuleLifecycleEvent

	// OnServiceRegistration intercepts service registration.
	// Middleware can wrap handlers (decorator pattern) or modify service configuration.
	// Returns the (possibly modified) registration for the next middleware.
	//
	// Example uses:
	//   - Wrapping handlers for auto-encoding/decoding
	//   - Adding authentication/authorization layers
	//   - Injecting logging or metrics
	//   - Modifying stream consumer configuration
	OnServiceRegistration(ctx context.Context, reg ServiceRegistration) ServiceRegistration

	// OnConfigurationChange intercepts configuration updates.
	// Middleware can observe or modify configuration values before they're applied.
	// Returns the (possibly modified) event for the next middleware.
	//
	// Example uses:
	//   - Audit logging of config changes
	//   - Validating configuration values
	//   - Redacting sensitive values from logs
	OnConfigurationChange(ctx context.Context, event ConfigurationEvent) ConfigurationEvent

	// OnOutgoingMessage intercepts outgoing messages from service clients.
	// Middleware can modify the message (e.g., inject headers) before it's sent.
	// Returns the (possibly modified) context for the next middleware.
	// The Msg field in the context can be modified in-place.
	//
	// Example uses:
	//   - Injecting request IDs or trace IDs into message headers
	//   - Adding authentication tokens
	//   - Logging outgoing message metadata
	//   - Encrypting message payloads
	OnOutgoingMessage(octx OutgoingMessageContext) OutgoingMessageContext

	// OnEventConsumerRegistration intercepts event consumer registration.
	// Middleware can wrap handlers (decorator pattern) or modify the entry.
	// Returns the (possibly modified) entry for the next middleware.
	//
	// Example uses:
	//   - Wrapping handlers for access logging with timing
	//   - Adding request ID extraction/injection
	//   - Injecting authentication/authorization layers
	//   - Adding metrics collection
	OnEventConsumerRegistration(ctx context.Context, entry EventConsumerEntry) EventConsumerEntry

	// OnEventStreamConsumerRegistration intercepts event stream consumer registration.
	// Middleware can wrap handlers (decorator pattern) or modify the entry.
	// Returns the (possibly modified) entry for the next middleware.
	//
	// This is similar to OnEventConsumerRegistration but for JetStream-based
	// durable consumers that process message batches.
	//
	// Example uses:
	//   - Wrapping handlers for access logging with timing
	//   - Adding request ID extraction/injection for batch processing
	//   - Injecting authentication/authorization layers
	//   - Adding metrics collection for batch operations
	OnEventStreamConsumerRegistration(ctx context.Context, entry EventStreamConsumerEntry) EventStreamConsumerEntry
}

// ModuleLifecycleEventType represents the type of module lifecycle event.
type ModuleLifecycleEventType string

const (
	// ModuleStartedEvent indicates a module was started successfully.
	ModuleStartedEvent ModuleLifecycleEventType = "module.started"

	// ModuleStoppedEvent indicates a module was stopped.
	ModuleStoppedEvent ModuleLifecycleEventType = "module.stopped"

	// Note: ModuleRegisteredEvent does not exist because module registration
	// occurs before the middleware chain is built (during framework.Register()),
	// so it cannot be captured by middleware. The chain is only built during
	// framework.Start() after all modules are registered.
)

// ModuleLifecycleEvent contains data for module lifecycle events.
//
// Different event types populate different fields:
//   - ModuleStartedEvent: ModuleName, Duration
//   - ModuleStoppedEvent: ModuleName, Error (nil if successful)
//
// The Metadata field is available for all event types and allows middleware
// to attach custom data that flows through the chain.
type ModuleLifecycleEvent struct {
	// Type identifies the lifecycle event type
	Type ModuleLifecycleEventType

	// ModuleName is the name of the module (present for all events)
	ModuleName string

	// Duration is the module startup time (only for ModuleStartedEvent)
	Duration time.Duration

	// Error is the module stop error (only for ModuleStoppedEvent, nil if successful)
	Error error

	// Metadata holds extensible custom data that middleware can attach.
	// This allows middleware to communicate through the chain.
	Metadata map[string]any
}

// ServiceRegistration contains service registration data that can be modified by middleware.
//
// Middleware can:
//   - Wrap handlers (decorator pattern) by replacing handler functions
//   - Modify configuration (e.g., StreamConsumerConfig)
//   - Add metadata for other middleware
//
// Only the fields relevant to the ServiceType are populated:
//   - RequestReply: Type, Name, ModuleName, Subject, RequestHandler
//   - QueueGroup: Type, Name, ModuleName, Subject, QueueHandlers
//   - StreamConsumer: Type, Name, ModuleName, Subject, StreamHandler, StreamConsumerConfig
//   - Channel: Type, Name, ModuleName, InChannel, OutChannel
type ServiceRegistration struct {
	// Type identifies the service type
	Type ServiceType

	// Name is the service name
	Name string

	// ModuleName is the name of the module registering this service
	ModuleName string

	// Subject is the NATS subject for NATS-based services
	Subject string

	// RequestHandler processes RequestReply requests (can be wrapped by middleware)
	RequestHandler RequestReplyHandler

	// QueueHandlers contains queue group handler pairs (handlers can be wrapped)
	QueueHandlers []QGHP

	// StreamHandler processes StreamConsumer message batches (can be wrapped by middleware)
	StreamHandler StreamConsumerHandler

	// StreamConsumerConfig configures the JetStream consumer (can be modified by middleware)
	StreamConsumerConfig *StreamConsumerConfig

	// InChannel is the input channel for Channel services
	InChannel chan *Msg

	// OutChannel is the output channel for Channel services
	OutChannel chan *Msg

	// Metadata holds extensible custom data that middleware can attach
	Metadata map[string]any
}

// ConfigurationEventType represents the type of configuration event.
type ConfigurationEventType string

const (
	// ConfigurationUpdatedEvent indicates a configuration option was changed.
	ConfigurationUpdatedEvent ConfigurationEventType = "configuration.updated"
)

// ConfigurationEvent contains data for configuration change events.
//
// Middleware can observe or modify old/new values before they're applied.
// This is useful for:
//   - Audit logging of configuration changes
//   - Validating configuration values
//   - Redacting sensitive values
type ConfigurationEvent struct {
	// Type identifies the configuration event type
	Type ConfigurationEventType

	// OptionName is the name of the configuration option being changed
	OptionName string

	// OldValue is the previous value of the configuration option
	OldValue any

	// NewValue is the new value of the configuration option
	NewValue any

	// Metadata holds extensible custom data that middleware can attach
	Metadata map[string]any
}

// OutgoingMessageContext contains data for outgoing message interception.
//
// This context is passed to middleware when a service client is about to send a message.
// Middleware can modify the message headers, data, or other fields before the message is sent.
//
// The Msg field can be modified in-place. Middleware should ensure that Msg.Header is not nil
// before adding headers.
type OutgoingMessageContext struct {
	// ServiceType identifies the type of service sending the message
	ServiceType ServiceType

	// ServiceName is the name of the service
	ServiceName string

	// ModuleName is the name of the module that registered this service
	ModuleName string

	// Subject is the NATS subject the message will be sent to
	Subject string

	// Msg is the message being sent (middleware can modify headers/data in-place)
	Msg *Msg

	// Ctx is the context from the client call (may contain request ID, trace ID, etc.)
	Ctx context.Context

	// Metadata holds extensible custom data that middleware can attach
	Metadata map[string]any
}

// MiddlewareChainRunner executes middleware chains for different event types.
//
// This interface is implemented by internal/middleware.Chain and is used by
// the service container to run service registration through the middleware chain.
//
// This is an internal interface used for dependency injection between
// internal packages. Users should not implement this interface directly.
type MiddlewareChainRunner interface {
	// RunServiceRegistration runs a service registration through the middleware chain.
	// Each middleware can modify the registration before it's stored.
	RunServiceRegistration(ctx context.Context, reg ServiceRegistration) ServiceRegistration

	// RunOutgoingMessage runs an outgoing message through the middleware chain.
	// Each middleware can modify the message before it's sent.
	// The final context is returned after all middleware have processed it.
	RunOutgoingMessage(octx OutgoingMessageContext) OutgoingMessageContext

	// RunEventConsumerRegistration runs an event consumer registration through the middleware chain.
	// Each middleware can wrap the handler or modify the entry before it's stored.
	RunEventConsumerRegistration(ctx context.Context, entry EventConsumerEntry) EventConsumerEntry

	// RunEventStreamConsumerRegistration runs an event stream consumer registration through the middleware chain.
	// Each middleware can wrap the handler or modify the entry before it's stored.
	RunEventStreamConsumerRegistration(ctx context.Context, entry EventStreamConsumerEntry) EventStreamConsumerEntry
}
