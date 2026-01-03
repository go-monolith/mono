package types

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// EventBus provides event-driven communication wrapping NATS client.
//
// The EventBus abstraction provides high-level messaging patterns including
// publish/subscribe, request/reply, and queue group subscriptions.
//
// See docs/spec/foundation.md for detailed design documentation.
type EventBus interface {
	// Publish publishes data to a subject (fire-and-forget)
	Publish(subject string, data []byte) error

	// PublishMsg publishes a complete message with headers
	PublishMsg(msg *Msg) error

	// Request sends a request and waits for a single reply
	Request(subject string, data []byte, timeout time.Duration) (*Msg, error)

	// RequestWithContext sends a request and waits for a single reply with context support.
	// The context controls cancellation and deadline. If the context is cancelled or
	// its deadline is exceeded, the request is aborted and an appropriate error is returned.
	// This method is preferred over Request when context cancellation is needed.
	RequestWithContext(ctx context.Context, subject string, data []byte) (*Msg, error)

	// RequestMsgWithContext sends a complete message with headers and waits for a single reply.
	// The context controls cancellation and deadline. If the context is cancelled or
	// its deadline is exceeded, the request is aborted and an appropriate error is returned.
	// This method allows transmitting headers along with the request payload.
	// Note: The Reply field is ignored; NATS generates the reply subject automatically.
	RequestMsgWithContext(ctx context.Context, msg *Msg) (*Msg, error)

	// Subscribe creates an asynchronous subscription with a message handler
	Subscribe(subject string, handler MsgHandler) (Subscription, error)

	// SubscribeSync creates a synchronous subscription
	SubscribeSync(subject string) (Subscription, error)

	// QueueSubscribe creates a queue group subscription for load balancing
	QueueSubscribe(subject, queue string, handler MsgHandler) (Subscription, error)

	// QueueSubscribeSync creates a synchronous queue group subscription
	QueueSubscribeSync(subject, queue string) (Subscription, error)

	// ChanSubscribe creates a channel-based subscription
	ChanSubscribe(subject string, ch chan *Msg) (Subscription, error)

	// EventStream returns an EventStream interface instance for durable/persistent subscriptions
	EventStream() (EventStream, error)

	// SetRuntimeContext sets the context that will be passed to all message handlers.
	// When this context is cancelled, handlers can detect shutdown and terminate gracefully.
	SetRuntimeContext(ctx context.Context)
}

// EventBusWithConn is a generic interface for accessing the underlying connection
// from an EventBus implementation.
//
// This interface provides an escape hatch for plugins and advanced modules that need
// direct access to the underlying event bus driver (e.g., for JetStream operations,
// custom NATS features not exposed through EventBus).
//
// IMPORTANT: Most modules should NOT use this interface. Standard module communication
// should use EventBus methods (Publish, Request, Subscribe) or services registered
// through ServiceContainer. Only use this for:
//   - Plugin modules needing JetStream/ObjectStore/KV access
//   - Advanced features requiring driver-specific APIs
//   - Integration with third-party NATS libraries
//
// Example usage:
//
//	if provider, ok := eventBus.(types.EventBusWithConn[*nats.Conn]); ok {
//	    conn := provider.Conn()
//	    // Use NATS connection for JetStream operations
//	    js, err := jetstream.New(conn)
//	}
type EventBusWithConn[T any] interface {
	Conn() T
}

// MsgHandler is a callback function for processing messages asynchronously in a subscription.
//
// The handler is invoked whenever a message arrives on the subscribed subject.
// The context parameter contains the subscription's runtime context and can be used to detect
// graceful shutdown. When the context is cancelled, the subscription should terminate processing.
//
// Error handling: The handler should not return an error; it should handle errors internally
// (logging, retrying, etc.). Errors in handlers do not affect the subscription - the handler
// is responsible for error recovery.
//
// Performance note: Handlers are invoked asynchronously in a dedicated goroutine. Blocking
// operations in the handler will not block other message processing or the event loop.
//
// Context cancellation: When the EventBus's runtime context is cancelled (typically during
// graceful shutdown), handlers may receive calls with a cancelled context. Handlers should
// check context.Err() and terminate cleanly.
//
// Example:
//
//	handler := func(ctx context.Context, msg *mono.Msg) {
//	    // Check for shutdown
//	    if ctx.Err() != nil {
//	        return  // Context cancelled, terminate gracefully
//	    }
//
//	    // Process the message
//	    var event MyEvent
//	    if err := json.Unmarshal(msg.Data, &event); err != nil {
//	        // Handle decode error internally
//	        log.Printf("decode error: %v", err)
//	        return
//	    }
//
//	    // Process event
//	    if err := processEvent(ctx, &event); err != nil {
//	        // Handle processing error internally
//	        log.Printf("processing error: %v", err)
//	    }
//	}
type MsgHandler func(ctx context.Context, msg *Msg)

// Subscription represents an active subscription.
type Subscription interface {
	// Unsubscribe removes interest in the subscription
	Unsubscribe() error

	// Drain removes interest but processes pending messages before completion
	Drain() error

	// IsValid returns false if subscription has been unsubscribed
	IsValid() bool

	// Subject returns the subject pattern
	Subject() string

	// Queue returns the queue group name (empty string for non-queue subscriptions)
	Queue() string

	// NextMsg fetches the next message (for sync subscriptions)
	NextMsg(timeout time.Duration) (*Msg, error)

	// NextMsgWithContext fetches next message with context cancellation
	NextMsgWithContext(ctx context.Context) (*Msg, error)
}

// EventStream provides Stream operations for durable, persistent messaging (via JetStream).
//
// JetStream adds persistence, replay, and at-least-once delivery semantics
// on top of core NATS functionality. This interface uses internal StreamConfig and ConsumerConfig
// types to abstract the underlying JetStream implementation.
type EventStream interface {
	// Publish publishes a message to JetStream synchronously
	Publish(ctx context.Context, subject string, data []byte) (MsgPubAck, error)

	// PublishMsg publishes a complete mono.Msg to JetStream synchronously
	PublishMsg(ctx context.Context, msg *Msg) (MsgPubAck, error)

	// CreateOrUpdateStream creates or updates a stream (idempotent operation)
	CreateOrUpdateStream(ctx context.Context, cfg StreamConfig) (jetstream.Stream, error)

	// CreateOrUpdateConsumer creates or updates a consumer on a stream (idempotent operation)
	CreateOrUpdateConsumer(ctx context.Context, streamName string, cfg ConsumerConfig) (jetstream.Consumer, error)

	// Stream returns a stream handle for advanced operations
	Stream(ctx context.Context, name string) (jetstream.Stream, error)

	// DeleteStream deletes a stream
	DeleteStream(ctx context.Context, name string) error
}

// Msg represents a message in the NATS system.
//
// This unified message type supports both regular NATS messaging and JetStream messaging:
//   - For regular NATS: Subject, Reply, Data, and Header are populated
//   - For JetStream: Subject, Data, and Header are populated; Reply is typically empty
//   - JetStream methods (Ack, Nak, NakWithDelay, Term, InProgress) work for JetStream messages
//   - JetStream methods are no-ops for regular NATS messages
type Msg struct {
	Subject string
	Reply   string
	Data    []byte
	Header  Header
	Sub     *Subscription

	// NatsMsg holds the underlying NATS message for acknowledgment.
	// WARNING: This field is exported for internal eventbus implementations only.
	// Application code should NOT access or modify this field directly.
	NatsMsg any
}

// Ack acknowledges the message when using JetStream.
// For non-JetStream messages, this is a no-op.
func (m *Msg) Ack() error {
	if m.NatsMsg == nil {
		return nil
	}
	// Type assert to *nats.Msg and call Ack()
	// We can't import nats here, so we use reflection/interface
	type acker interface {
		Ack() error
	}
	if acker, ok := m.NatsMsg.(acker); ok {
		return acker.Ack()
	}
	return nil
}

// Nak negatively acknowledges the message when using JetStream.
// For non-JetStream messages, this is a no-op.
func (m *Msg) Nak() error {
	if m.NatsMsg == nil {
		return nil
	}
	type naker interface {
		Nak() error
	}
	if naker, ok := m.NatsMsg.(naker); ok {
		return naker.Nak()
	}
	return nil
}

// NakWithDelay negatively acknowledges the message with a delay when using JetStream.
// For non-JetStream messages, this is a no-op.
func (m *Msg) NakWithDelay(delay time.Duration) error {
	if m.NatsMsg == nil {
		return nil
	}
	type nakDelayer interface {
		NakWithDelay(delay time.Duration) error
	}
	if nakDelayer, ok := m.NatsMsg.(nakDelayer); ok {
		return nakDelayer.NakWithDelay(delay)
	}
	return nil
}

// Term terminates the message processing when using JetStream.
// For non-JetStream messages, this is a no-op.
func (m *Msg) Term() error {
	if m.NatsMsg == nil {
		return nil
	}
	type termer interface {
		Term() error
	}
	if termer, ok := m.NatsMsg.(termer); ok {
		return termer.Term()
	}
	return nil
}

// InProgress indicates that work is still in progress when using JetStream.
// For non-JetStream messages, this is a no-op.
func (m *Msg) InProgress() error {
	if m.NatsMsg == nil {
		return nil
	}
	type inProgresser interface {
		InProgress() error
	}
	if inProgresser, ok := m.NatsMsg.(inProgresser); ok {
		return inProgresser.InProgress()
	}
	return nil
}

// MsgPubAck represents a publish acknowledgment from JetStream.
// This interface abstracts the underlying JetStream PubAck for loose coupling.
//
// SAFETY NOTE: This is an interface type. When implementing methods that return MsgPubAck:
//   - Always return nil (not a wrapped nil pointer) on error paths
//   - Always wrap a valid non-nil value on success paths
//   - Example:
//     if err != nil {
//     return nil, err  // ✓ Correct: returns nil interface
//     }
//     return &msgPubAck{ack: validAck}, nil  // ✓ Correct: wraps non-nil value
//
// The nil interface gotcha (returning a typed nil) can cause subtle bugs where
// the returned value is non-nil but contains a nil pointer, which fails nil checks.
type MsgPubAck interface {
	// Stream returns the name of the stream the message was published to
	Stream() string
	// Sequence returns the sequence number assigned to the message
	Sequence() uint64
	// Duplicate returns whether this message was detected as a duplicate
	Duplicate() bool
	// Domain returns the JetStream domain (for super-cluster deployments)
	Domain() string
}

// Header represents NATS message headers.
//
// Headers are key-value pairs attached to NATS messages for metadata propagation.
// Each header key maps to a slice of values, supporting multiple values per key
// (following HTTP header semantics).
//
// Common header use cases:
// - Request tracing: "x-request-id", "x-trace-id" for distributed tracing
// - Correlation: "x-correlation-id" to link related messages
// - Routing: Custom headers for message routing logic
// - Metadata: "x-timestamp", "x-source" for context information
//
// Headers are optional and survive NATS request-reply round trips.
// When publishing messages with headers, they are preserved through the NATS network.
//
// Example:
//
//	msg := &mono.Msg{
//	    Subject: "events.order.created",
//	    Data:    orderData,
//	    Header: mono.Header{
//	        "x-request-id": []string{"req-12345"},
//	        "x-trace-id":   []string{"trace-98765"},
//	    },
//	}
//	eventBus.PublishMsg(msg)
type Header map[string][]string
