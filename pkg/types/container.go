package types

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ServiceContainer provides dependency injection with name-based registration.
//
// The Service Container supports five types of services:
// - Channel services: Bidirectional Go channel communication (in-process)
// - RequestReply services: Synchronous NATS request/response pattern
// - Queue Group services: Asynchronous NATS queue subscription pattern
// - Stream Consumer services: JetStream durable pull consumer (at-least-once)
// - Cron services: Server-side cron-scheduled handler (JetStream message scheduler)
//
// The lookup methods (Get*, Has) are those of the embedded ServiceLookup, the
// read side of the container. Code that only calls services should accept a
// ServiceLookup rather than a ServiceContainer.
//
// See docs/spec/foundation.md for detailed design documentation.
type ServiceContainer interface {
	// ServiceLookup is the read side: the service clients and channels a
	// consumer needs to call the services registered on this container.
	ServiceLookup

	// BindModule binds this container to a module
	BindModule(module Module) error

	// SetEventBus sets the EventBus for NATS-based services.
	// This must be called before registering RequestReply, QueueGroup, or StreamConsumer services.
	SetEventBus(bus EventBus)

	// SetQueueGroupOptimisticWindow configures the optimistic publish window for queue group services.
	// When window > 0, queue group clients will use fire-and-forget publish after a successful ACK.
	// When window = 0 (default), all sends use ACK mode.
	SetQueueGroupOptimisticWindow(window time.Duration)

	// SetMiddlewareChain sets the middleware chain for service registration interception.
	// This must be called before registering any services if middleware is needed.
	SetMiddlewareChain(chain MiddlewareChainRunner)

	// RegisterChannelService registers a bidirectional Go channel service
	RegisterChannelService(name string, in chan *Msg, out chan *Msg) error

	// RegisterRequestReplyService registers a request-reply service over NATS.
	// Options bound the worker pool that runs the handler on this instance; see
	// ServiceOptions. Without options the framework defaults apply.
	RegisterRequestReplyService(name string, handler RequestReplyHandler, opts ...ServiceOption) error

	// RegisterQueueGroupService registers a queue group service with multiple handlers and acknowledgment.
	// All handlers share the same subject (services.<module>.<service>) but use different queue groups.
	// Handlers send an ACK before processing to enable "no responder" error detection.
	// Each pair's Options bound the worker pool of its queue group's subscription.
	RegisterQueueGroupService(name string, pairs ...QGHP) error

	// RegisterStreamConsumerService registers a JetStream durable pull consumer service.
	// The service will automatically create/update the stream and consumer on startup.
	// The handler receives batches of messages that should be individually acknowledged.
	RegisterStreamConsumerService(name string, config StreamConsumerConfig, handler StreamConsumerHandler) error

	// RegisterCronService registers a cron-scheduled service backed by the
	// embedded NATS JetStream message scheduler (nats-server v2.14+). The
	// schedule is registered server-side, so exactly one message fires per
	// occurrence across a cluster. Each occurrence is delivered to the handler
	// through a durable pull consumer with explicit acknowledgement. Requires
	// JetStream to be enabled. There is no client to retrieve: the service is
	// driven entirely by the server-side schedule.
	//
	// Note: cron handlers are not currently wrapped by the middleware chain
	// (access-log, audit, request-id); the handler is invoked directly.
	RegisterCronService(name string, config CronServiceConfig, handler CronHandler) error

	// Unregister removes a service from the container
	Unregister(name string) error

	// Entries returns all registered ServiceEntry pointers
	Entries() []*ServiceEntry

	// StartChannelRouters starts router goroutines for all registered channel services.
	// This is called by the lifecycle manager after all modules have started.
	// Router goroutines handle message routing from provider out channels to per-consumer channels.
	StartChannelRouters(ctx context.Context)
}

// ServiceLookup is the read side of a ServiceContainer: it hands out the
// clients and channels of the services registered on a container and nothing
// else. It cannot register, replace or remove a service.
//
// It is the interface a consumer should hold for a dependency. The container a
// module receives in DependentModule.SetDependencyServiceContainer is a
// ServiceContainer for compatibility, but a driven adapter only needs to look
// services up, so it should store it as a ServiceLookup: mutating a
// dependency's container is then a compile error instead of a
// register-on-dependency boundary violation at runtime. The typed contracts in
// pkg/helper (Call, Send, Publish) and the helper.Call*/Send*/Publish*
// functions accept a ServiceLookup.
//
// Example:
//
//	type inventoryAdapter struct{ services types.ServiceLookup }
//
//	func (m *OrderModule) SetDependencyServiceContainer(dependency string, container types.ServiceContainer) {
//	    if dependency == "inventory" {
//	        m.inventory = inventoryAdapter{services: container}
//	    }
//	}
type ServiceLookup interface {
	// GetChannelService retrieves a channel service by name with a per-consumer out channel.
	// The consumerModule parameter identifies the calling module and ensures it receives
	// a dedicated out channel, preventing race conditions when multiple modules consume
	// from the same service.
	GetChannelService(serviceName string, consumerModule string) (in chan *Msg, out chan *Msg, err error)

	// MustGetChannelService retrieves a channel service and panics if not found.
	// The consumerModule parameter identifies the calling module and ensures it receives
	// a dedicated out channel.
	MustGetChannelService(serviceName string, consumerModule string) (in chan *Msg, out chan *Msg)

	// GetRequestReplyService retrieves a request-reply service client
	GetRequestReplyService(name string) (RequestReplyServiceClient, error)

	// GetQueueGroupService retrieves a queue group service client
	GetQueueGroupService(name string) (QueueGroupServiceClient, error)

	// GetStreamConsumerService retrieves a stream consumer service client
	// that can publish messages to the stream for consumption.
	GetStreamConsumerService(name string) (StreamConsumerServiceClient, error)

	// Has checks if a service with the given name is registered
	Has(name string) bool
}

// RequestReplyHandler processes request messages and returns response data.
type RequestReplyHandler func(ctx context.Context, req *Msg) (response []byte, err error)

// TypedRequestReplyHandler processes typed request messages and returns typed responses.
// The handler receives an already-unmarshaled request and returns a typed response
// that will be marshaled before sending.
type TypedRequestReplyHandler[Req any, Resp any] func(ctx context.Context, req Req, msg *Msg) (Resp, error)

// RequestReplyServiceClient sends requests and receives responses.
type RequestReplyServiceClient interface {
	// Call sends a request payload and waits for a response
	Call(ctx context.Context, data []byte) (*Msg, error)

	// CallMsg sends a raw request message and waits for a response
	CallMsg(ctx context.Context, msg *Msg) (*Msg, error)
}

// QueueGroupHandler processes fire-and-forget messages from queue group.
type QueueGroupHandler func(ctx context.Context, msg *Msg) error

// TypedQueueGroupHandler processes typed fire-and-forget messages from queue group.
// The handler receives an already-unmarshaled message payload.
type TypedQueueGroupHandler[T any] func(ctx context.Context, payload T, msg *Msg) error

// QGHP (QueueGroupHandler pair) associates a queue group name with its handler.
// Multiple pairs can be registered for a single service, allowing different
// queue groups to process messages from the same subject.
type QGHP struct {
	QueueGroup string            // Queue group name (e.g., "high-priority-workers")
	Handler    QueueGroupHandler // Handler function for this queue group
	// Options bounds the worker pool of this queue group's subscription on
	// this instance. Queue groups are serial by default (MaxConcurrency 0
	// means 1), so messages are handled one at a time in arrival order; set
	// MaxConcurrency above 1 to handle them concurrently.
	Options ServiceOptions
}

// TypedQGHP associates a queue group name with a typed handler.
// This is the generic version of QGHP for use with RegisterTypedQueueGroupService.
type TypedQGHP[T any] struct {
	QueueGroup string                    // Queue group name (e.g., "high-priority-workers")
	Handler    TypedQueueGroupHandler[T] // Typed handler function for this queue group
	// Options bounds the worker pool of this queue group's subscription on
	// this instance; see QGHP.Options.
	Options ServiceOptions
}

// StreamConsumerHandler processes batches of messages from a JetStream pull consumer.
// The handler receives a slice of messages that should be individually acknowledged.
//
// Messages should be acknowledged using methods like Ack(), Nak(), NakWithDelay(), Term(), or InProgress().
type StreamConsumerHandler func(ctx context.Context, msgs []*Msg) error

// TypedStreamConsumerHandler processes typed batches of messages from a JetStream pull consumer.
// The handler receives slices of already-unmarshaled payloads and the original messages
// for acknowledgment.
type TypedStreamConsumerHandler[T any] func(ctx context.Context, payloads []T, msgs []*Msg) error

// QueueGroupServiceClient sends fire-and-forget messages to queue group.
type QueueGroupServiceClient interface {
	// Send sends a message payload to the queue group and waits for ACK.
	// Returns ErrServiceUnavailable if no handlers are online.
	Send(ctx context.Context, data []byte) error

	// SendMsg sends a raw message to the queue group and waits for ACK.
	// Returns ErrServiceUnavailable if no handlers are online.
	SendMsg(ctx context.Context, msg *Msg) error
}

// StreamConsumerServiceClient publishes messages to a JetStream stream for consumption.
// Messages are persisted in JetStream and will be delivered to the stream consumer.
type StreamConsumerServiceClient interface {
	// Publish publishes a message to the stream (with JetStream persistence)
	Publish(ctx context.Context, data []byte) (MsgPubAck, error)

	// PublishMsg publishes a complete message with headers to the stream
	PublishMsg(ctx context.Context, msg *Msg) (MsgPubAck, error)
}

// ServiceEntry represents a registered service.
type ServiceEntry struct {
	Name       string
	Type       ServiceType
	InChannel  chan *Msg
	OutChannel chan *Msg
	// ConsumerChannels maintains per-consumer output channels to prevent race conditions
	// when multiple modules consume from the same channel service. Map key is the consumer
	// module name. Access must be synchronized using ConsumerMu.
	ConsumerChannels map[string]chan *Msg
	// ConsumerMu synchronizes access to ConsumerChannels during read/write operations.
	// The router goroutine uses RLock for routing, while GetChannelService uses Lock
	// when creating new consumer channels.
	ConsumerMu            sync.RWMutex
	RequestHandler        RequestReplyHandler
	Options               ServiceOptions        // Worker pool bounds for RequestReply services (zero = framework defaults)
	QueueGroup            string                // Queue group for RequestReply services
	QueueHandlers         []QGHP                // Multiple handler pairs for QueueGroup services
	StreamConsumerConfig  *StreamConsumerConfig // For StreamConsumer type
	StreamConsumerHandler StreamConsumerHandler // For StreamConsumer type
	CronConfig            *CronServiceConfig    // For Cron type
	CronHandler           CronHandler           // For Cron type
	ScheduleSubject       string                // For Cron type: internal schedule subject
	// Contract is the service contract the provider registered this service
	// with (see BaseServiceDefinition and helper.RequestReplyDefinition);
	// nil for a service registered without one. It is set through
	// ContractRegistrar after registration, so middleware that rebuilds the
	// ServiceRegistration cannot drop it.
	Contract   *BaseServiceDefinition
	ModuleName string
	Subject    string
	Created    time.Time
}

// ServiceType identifies the type of service.
type ServiceType int

const (
	ServiceTypeChannel ServiceType = iota
	ServiceTypeRequestReply
	ServiceTypeQueueGroup
	ServiceTypeStreamConsumer // JetStream durable pull consumer
	ServiceTypeCron           // Server-side cron-scheduled service (JetStream message scheduler)
)

// FormatServiceType converts a ServiceType to its lowercase snake_case string representation
// for display in logs, error messages, and audit trails.
// Returns "unknown(N)" for unrecognized service types where N is the numeric value.
func FormatServiceType(serviceType ServiceType) string {
	switch serviceType {
	case ServiceTypeChannel:
		return "channel"
	case ServiceTypeRequestReply:
		return "request_reply"
	case ServiceTypeQueueGroup:
		return "queue_group"
	case ServiceTypeStreamConsumer:
		return "stream_consumer"
	case ServiceTypeCron:
		return "cron"
	default:
		return fmt.Sprintf("unknown(%d)", serviceType)
	}
}
