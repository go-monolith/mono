package mono

import (
	"github.com/go-monolith/mono/pkg/types"
)

// This file provides type aliases and constant re-exports for core types of the mono framework.
// It allows users to import from the root package instead of pkg/types for better ergonomics.
//
// Import pattern:
//
//	import "github.com/go-monolith/mono"
//
//	app, err := mono.NewMonoApplication(
//	    mono.WithLogLevel(mono.LogLevelInfo),
//	)

// =============================================================================
// Core Framework Types
// =============================================================================

// MonoApplication is the main framework interface.
// We use MonoApplication here to provide a clearer name that represents the application instance.
type MonoApplication = types.MonoFramework

// MonoFrameworkState represents the state of the framework.
type MonoFrameworkState = types.MonoFrameworkState

// Framework state constants
const (
	StateCreated  = types.StateCreated
	StateStarting = types.StateStarting
	StateRunning  = types.StateRunning
	StateStopping = types.StateStopping
	StateStopped  = types.StateStopped
)

// FrameworkHealth contains aggregated health status of the framework.
type FrameworkHealth = types.FrameworkHealth

// ModuleHealth contains health status for a single module.
type ModuleHealth = types.ModuleHealth

// =============================================================================
// Module Interfaces
// =============================================================================

// Module is the basic interface that all modules must implement.
type Module = types.Module

// DependentModule declares dependencies on other modules.
type DependentModule = types.DependentModule

// ServiceProviderModule registers services in the container.
type ServiceProviderModule = types.ServiceProviderModule

// EventBusAwareModule receives the EventBus instance.
type EventBusAwareModule = types.EventBusAwareModule

// HealthCheckableModule provides health status.
type HealthCheckableModule = types.HealthCheckableModule

// MiddlewareModule provides interceptor hooks for framework events.
type MiddlewareModule = types.MiddlewareModule

// EventEmitterModule declares events that a module emits.
type EventEmitterModule = types.EventEmitterModule

// EventConsumerModule registers event consumers.
type EventConsumerModule = types.EventConsumerModule

// HealthStatus represents the health of a module.
type HealthStatus = types.HealthStatus

// =============================================================================
// Plugin Module Interfaces
// =============================================================================

// PluginModule is a special module type that starts before middleware.
// Plugins receive their own ServiceContainer and are excluded from the
// dependency graph and middleware hooks.
type PluginModule = types.PluginModule

// UsePluginModule allows modules to declare plugin dependencies.
// The framework will inject plugin instances via SetPlugin() during
// module initialization.
type UsePluginModule = types.UsePluginModule

// =============================================================================
// EventBus Types
// =============================================================================

// EventBus is the NATS-backed message bus for inter-module communication.
type EventBus = types.EventBus

// EventStream provides JetStream operations.
type EventStream = types.EventStream

// Subscription represents an active subscription.
type Subscription = types.Subscription

// Msg represents a NATS message.
type Msg = types.Msg

// Header is a map of header key-value pairs.
type Header = types.Header

// MsgHandler is a callback for message handling.
type MsgHandler = types.MsgHandler

// MsgPubAck is the acknowledgment from JetStream publish.
type MsgPubAck = types.MsgPubAck

// =============================================================================
// ServiceContainer Types
// =============================================================================

// ServiceContainer manages service registration and discovery.
type ServiceContainer = types.ServiceContainer

// ServiceEntry represents a registered service.
type ServiceEntry = types.ServiceEntry

// ServiceType identifies the type of service.
type ServiceType = types.ServiceType

// Service type constants
const (
	ServiceTypeChannel        = types.ServiceTypeChannel
	ServiceTypeRequestReply   = types.ServiceTypeRequestReply
	ServiceTypeQueueGroup     = types.ServiceTypeQueueGroup
	ServiceTypeStreamConsumer = types.ServiceTypeStreamConsumer
)

// FormatServiceType returns a human-readable string for the service type.
var FormatServiceType = types.FormatServiceType

// RequestReplyHandler handles synchronous request-reply calls.
type RequestReplyHandler = types.RequestReplyHandler

// RequestReplyServiceClient is a client for request-reply services.
type RequestReplyServiceClient = types.RequestReplyServiceClient

// QueueGroupHandler handles queue group messages.
type QueueGroupHandler = types.QueueGroupHandler

// QGHP is a queue group handler pair.
type QGHP = types.QGHP

// QueueGroupServiceClient is a client for queue group services.
type QueueGroupServiceClient = types.QueueGroupServiceClient

// StreamConsumerHandler handles JetStream consumer messages.
type StreamConsumerHandler = types.StreamConsumerHandler

// StreamConsumerServiceClient is a client for stream consumer services.
type StreamConsumerServiceClient = types.StreamConsumerServiceClient

// =============================================================================
// Logger Types
// =============================================================================

// Logger is the framework's logging interface.
type Logger = types.Logger

// LoggerFactory creates logger instances.
type LoggerFactory = types.LoggerFactory

// LogLevel represents the log level.
type LogLevel = types.LogLevel

// Log level constants
const (
	LogLevelDebug = types.LogLevelDebug
	LogLevelInfo  = types.LogLevelInfo
	LogLevelWarn  = types.LogLevelWarn
	LogLevelError = types.LogLevelError
)

// LogFormat represents the log output format.
type LogFormat = types.LogFormat

// Log format constants
const (
	LogFormatJSON = types.LogFormatJSON
	LogFormatText = types.LogFormatText
)

// =============================================================================
// Event Types
// =============================================================================

// BaseEventDefinition is the non-generic representation of an event definition.
type BaseEventDefinition = types.BaseEventDefinition

// EventRegistry manages event registration and discovery.
type EventRegistry = types.EventRegistry

// EventConsumerHandler handles event messages.
type EventConsumerHandler = types.EventConsumerHandler

// TypedEventConsumerHandler is a type-safe event handler.
type TypedEventConsumerHandler[T any] = types.TypedEventConsumerHandler[T]

// EventConsumerEntry tracks an event consumer registration.
type EventConsumerEntry = types.EventConsumerEntry

// EventStreamConsumerHandler handles batches of JetStream messages.
type EventStreamConsumerHandler = types.EventStreamConsumerHandler

// TypedEventStreamConsumerHandler is a type-safe JetStream event handler.
type TypedEventStreamConsumerHandler[T any] = types.TypedEventStreamConsumerHandler[T]

// EventStreamConsumerEntry tracks a JetStream event consumer registration.
type EventStreamConsumerEntry = types.EventStreamConsumerEntry

// Marshaler serializes data to bytes.
type Marshaler = types.Marshaler

// Unmarshaler deserializes bytes to data.
type Unmarshaler = types.Unmarshaler

// =============================================================================
// Middleware Types
// =============================================================================

// MiddlewareChainRunner executes the middleware chain.
type MiddlewareChainRunner = types.MiddlewareChainRunner

// ModuleLifecycleEventType identifies lifecycle event types.
type ModuleLifecycleEventType = types.ModuleLifecycleEventType

// Module lifecycle event type constants
const (
	ModuleStartedEvent = types.ModuleStartedEvent
	ModuleStoppedEvent = types.ModuleStoppedEvent
)

// ModuleLifecycleEvent contains data for module lifecycle events.
type ModuleLifecycleEvent = types.ModuleLifecycleEvent

// ServiceRegistration contains service registration data.
type ServiceRegistration = types.ServiceRegistration

// ConfigurationEventType identifies configuration event types.
type ConfigurationEventType = types.ConfigurationEventType

// Configuration event type constants
const (
	ConfigurationUpdatedEvent = types.ConfigurationUpdatedEvent
)

// ConfigurationEvent contains data for configuration events.
type ConfigurationEvent = types.ConfigurationEvent

// OutgoingMessageContext contains context for outgoing messages.
type OutgoingMessageContext = types.OutgoingMessageContext

// =============================================================================
// JetStream Types
// =============================================================================

// StreamConfig configures a JetStream stream.
type StreamConfig = types.StreamConfig

// ConsumerConfig configures a JetStream consumer.
type ConsumerConfig = types.ConsumerConfig

// StreamConsumerConfig configures a JetStream durable pull consumer service.
type StreamConsumerConfig = types.StreamConsumerConfig

// FetchConfig configures the fetch behavior for JetStream pull consumers.
type FetchConfig = types.FetchConfig

// RetentionPolicy defines the retention policy for a stream.
type RetentionPolicy = types.RetentionPolicy

// Retention policy constants
const (
	LimitsPolicy    = types.LimitsPolicy
	InterestPolicy  = types.InterestPolicy
	WorkQueuePolicy = types.WorkQueuePolicy
)

// StorageType defines the storage backend for a stream.
type StorageType = types.StorageType

// Storage type constants
const (
	FileStorage   = types.FileStorage
	MemoryStorage = types.MemoryStorage
)

// AckPolicy defines the acknowledgement policy for a consumer.
type AckPolicy = types.AckPolicy

// Ack policy constants
const (
	AckExplicitPolicy = types.AckExplicitPolicy
	AckAllPolicy      = types.AckAllPolicy
	AckNonePolicy     = types.AckNonePolicy
)

// DeliverPolicy defines when a consumer should start delivering messages.
type DeliverPolicy = types.DeliverPolicy

// Deliver policy constants
const (
	DeliverAllPolicy             = types.DeliverAllPolicy
	DeliverLastPolicy            = types.DeliverLastPolicy
	DeliverNewPolicy             = types.DeliverNewPolicy
	DeliverByStartSequencePolicy = types.DeliverByStartSequencePolicy
	DeliverByStartTimePolicy     = types.DeliverByStartTimePolicy
	DeliverLastPerSubjectPolicy  = types.DeliverLastPerSubjectPolicy
)

// ReplayPolicy determines how the consumer should replay messages.
type ReplayPolicy = types.ReplayPolicy

// Replay policy constants
const (
	ReplayInstantPolicy  = types.ReplayInstantPolicy
	ReplayOriginalPolicy = types.ReplayOriginalPolicy
)

// DiscardPolicy determines how to proceed when limits are reached.
type DiscardPolicy = types.DiscardPolicy

// Discard policy constants
const (
	DiscardOld = types.DiscardOld
	DiscardNew = types.DiscardNew
)

// StoreCompression determines how messages are compressed.
type StoreCompression = types.StoreCompression

// Store compression constants
const (
	NoCompression = types.NoCompression
	S2Compression = types.S2Compression
)

// Placement guides stream placement in clustered JetStream.
type Placement = types.Placement

// StreamSource describes an external stream source.
type StreamSource = types.StreamSource

// ExternalStream describes an external stream.
type ExternalStream = types.ExternalStream

// SubjectTransformConfig describes subject transformation.
type SubjectTransformConfig = types.SubjectTransformConfig

// RePublish configures re-publishing of messages.
type RePublish = types.RePublish

// StreamConsumerLimits sets limits for stream consumers.
type StreamConsumerLimits = types.StreamConsumerLimits
