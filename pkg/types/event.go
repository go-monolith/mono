// Package types contains the core types and interfaces for the mono framework.
package types

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Marshaler defines the function signature for event serialization.
// The default is json.Marshal, but can be overridden for custom formats (e.g., Protobuf).
type Marshaler func(v any) ([]byte, error)

// Unmarshaler defines the function signature for event deserialization.
// The default is json.Unmarshal, but can be overridden for custom formats (e.g., Protobuf).
type Unmarshaler func(data []byte, v any) error

// BaseEventDefinition is the non-generic representation of an event definition.
// This type is used by EventRegistry and other interfaces because Go interface methods
// cannot have type parameters.
//
// Subject naming convention: events.<domain>.<version>.<event-type>
// Example: events.orders.v1.created
type BaseEventDefinition struct {
	// ModuleName is the name of the module that emits this event (e.g., "order")
	ModuleName string

	// Name is the event name (e.g., "OrderCreated")
	Name string

	// Subject is the NATS subject for this event (e.g., "events.orders.v1.created")
	Subject string

	// Version is the semantic version of this event (e.g., "v1")
	Version string
}

// PublishRaw publishes raw bytes to the event bus.
// For type-safe publishing with automatic marshaling, use EventDefinition[T].Publish() instead.
//
// Example:
//
//	data, _ := json.Marshal(orderCreatedEvent)
//	err := baseDef.PublishRaw(eventBus, data, nil)
func (e BaseEventDefinition) PublishRaw(eventBus EventBus, data []byte, header Header) error {
	if eventBus == nil {
		return errors.New("eventBus cannot be nil")
	}
	msg := &Msg{
		Subject: e.Subject,
		Data:    data,
		Header:  header,
	}
	return eventBus.PublishMsg(msg)
}

// EventStreamPublishRaw publishes raw bytes to JetStream for guaranteed persistence.
// Unlike PublishRaw which uses NATS core fire-and-forget publishing, this method
// persists the message in JetStream and returns a publish acknowledgment.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - eventBus: The EventBus to publish through
//   - data: The raw bytes to publish
//   - header: Optional message headers (can be nil)
//
// Returns:
//   - MsgPubAck: Contains stream name, sequence number, and duplicate detection info
//   - error: Returns error if EventBus is nil, EventStream unavailable, or publish fails
//
// Example:
//
//	data, _ := json.Marshal(orderCreatedEvent)
//	ack, err := baseDef.EventStreamPublishRaw(ctx, eventBus, data, nil)
//	if err != nil {
//	    return err
//	}
//	fmt.Printf("Published with sequence: %d\n", ack.Sequence())
func (e BaseEventDefinition) EventStreamPublishRaw(ctx context.Context, eventBus EventBus, data []byte, header Header) (MsgPubAck, error) {
	if eventBus == nil {
		return nil, errors.New("eventBus cannot be nil")
	}

	es, err := eventBus.EventStream()
	if err != nil {
		return nil, fmt.Errorf("failed to get EventStream: %w", err)
	}

	msg := &Msg{
		Subject: e.Subject,
		Data:    data,
		Header:  header,
	}

	ack, err := es.PublishMsg(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish to JetStream: %w", err)
	}

	return ack, nil
}

// EventDefinition is a generic event definition with type-safe publish and consume support.
// The type parameter T represents the event payload type.
//
// EventDefinition embeds BaseEventDefinition for field reuse. Fields like ModuleName, Name,
// Subject, and Version are accessed directly via Go's field promotion.
//
// Subject naming convention: events.<domain>.<version>.<event-type>
// Example: events.orders.v1.created
//
// Example usage:
//
//	// OrderCreatedV1 defines the event for order creation.
//	var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
//	    "order", "OrderCreated", "v1",
//	)
//
//	// Type-safe publish:
//	event := OrderCreatedEvent{OrderID: "123", Amount: 99.99}
//	err := OrderCreatedV1.Publish(eventBus, event, nil)
type EventDefinition[T any] struct {
	BaseEventDefinition // embedded - fields are promoted

	// marshal is the function used to serialize events (defaults to json.Marshal)
	marshal Marshaler

	// unmarshal is the function used to deserialize events (defaults to json.Unmarshal)
	unmarshal Unmarshaler
}

// NewEventDefinition creates a new generic EventDefinition with JSON as the default
// serialization format.
//
// NOTE: This is the internal constructor that does not validate the subject.
// Use helper.EventDefinition from pkg/mono for the validated version.
//
// Parameters:
//   - moduleName: The name of the module that emits this event (e.g., "order")
//   - name: The event name (e.g., "OrderCreated")
//   - version: The semantic version of this event (e.g., "v1")
//   - subject: The NATS subject for this event (e.g., "events.orders.v1.created") - optional, variadic
//
// Example:
//
//	var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
//	    "order",                       // moduleName
//	    "OrderCreated",                // name
//	    "v1",                          // version
//	    "events.orders.v1.created",    // subject (optional)
//	)
func NewEventDefinition[T any](moduleName, name, version string, subject ...string) EventDefinition[T] {
	finalSubject := ""
	if len(subject) > 0 {
		finalSubject = subject[0]
	}

	return EventDefinition[T]{
		BaseEventDefinition: BaseEventDefinition{
			ModuleName: moduleName,
			Name:       name,
			Subject:    finalSubject,
			Version:    version,
		},
		marshal:   json.Marshal,
		unmarshal: json.Unmarshal,
	}
}

// WithMarshaler sets a custom marshaler for the event definition.
// Returns a copy with the marshaler set.
//
// Example:
//
//	var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](...).
//	    WithMarshaler(protobufMarshal)
func (e EventDefinition[T]) WithMarshaler(m Marshaler) EventDefinition[T] {
	e.marshal = m
	return e
}

// WithUnmarshaler sets a custom unmarshaler for the event definition.
// Returns a copy with the unmarshaler set.
//
// Example:
//
//	var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](...).
//	    WithUnmarshaler(protobufUnmarshal)
func (e EventDefinition[T]) WithUnmarshaler(u Unmarshaler) EventDefinition[T] {
	e.unmarshal = u
	return e
}

// Publish serializes the event and publishes it to the event bus.
// This is the type-safe method that automatically marshals the event.
//
// Example:
//
//	event := OrderCreatedEvent{OrderID: "123", Amount: 99.99}
//	err := OrderCreatedV1.Publish(eventBus, event, nil)
func (e EventDefinition[T]) Publish(eventBus EventBus, event T, header Header) error {
	if eventBus == nil {
		return errors.New("eventBus cannot be nil")
	}

	data, err := e.marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := &Msg{
		Subject: e.Subject,
		Data:    data,
		Header:  header,
	}
	return eventBus.PublishMsg(msg)
}

// EventStreamPublish serializes the event and publishes it to JetStream for guaranteed persistence.
// Unlike Publish which uses NATS core fire-and-forget publishing, this method
// persists the message in JetStream and returns a publish acknowledgment.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - eventBus: The EventBus to publish through
//   - event: The typed event payload to publish
//   - header: Optional message headers (can be nil)
//
// Returns:
//   - MsgPubAck: Contains stream name, sequence number, and duplicate detection info
//   - error: Returns error if marshaling fails, EventBus is nil, or publish fails
//
// Example:
//
//	event := OrderCreatedEvent{OrderID: "123", Amount: 99.99}
//	ack, err := OrderCreatedV1.EventStreamPublish(ctx, eventBus, event, nil)
//	if err != nil {
//	    return err
//	}
//	fmt.Printf("Published with sequence: %d\n", ack.Sequence())
func (e EventDefinition[T]) EventStreamPublish(ctx context.Context, eventBus EventBus, event T, header Header) (MsgPubAck, error) {
	if eventBus == nil {
		return nil, errors.New("eventBus cannot be nil")
	}

	data, err := e.marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	es, err := eventBus.EventStream()
	if err != nil {
		return nil, fmt.Errorf("failed to get EventStream: %w", err)
	}

	msg := &Msg{
		Subject: e.Subject,
		Data:    data,
		Header:  header,
	}

	ack, err := es.PublishMsg(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish to JetStream: %w", err)
	}

	return ack, nil
}

// Unmarshal deserializes the message data into the event type.
// This is useful in consumer handlers when you have access to the event definition.
//
// Example:
//
//	func (m *Module) handleOrderCreated(ctx context.Context, msg *mono.Msg) error {
//	    event, err := order.OrderCreatedV1.Unmarshal(msg)
//	    if err != nil {
//	        return err
//	    }
//	    // Process event...
//	}
func (e EventDefinition[T]) Unmarshal(msg *Msg) (T, error) {
	var event T
	if err := e.unmarshal(msg.Data, &event); err != nil {
		return event, fmt.Errorf("failed to unmarshal event: %w", err)
	}
	return event, nil
}

// ToBase returns the embedded BaseEventDefinition for use with the EventRegistry interface.
//
// Example:
//
//	func (m *Module) EmitEvents() []mono.BaseEventDefinition {
//	    return []mono.BaseEventDefinition{
//	        OrderCreatedV1.ToBase(),
//	        OrderShippedV1.ToBase(),
//	    }
//	}
func (e EventDefinition[T]) ToBase() BaseEventDefinition {
	return e.BaseEventDefinition
}

// EventConsumerHandler is the signature for event consumer handlers.
// It follows the same pattern as QueueGroupHandler for consistency.
//
// The handler receives the runtime context and the message containing the event data.
// For type-safe handlers with automatic unmarshaling, use TypedEventConsumerHandler[T]
// with RegisterTypedConsumer instead.
//
// Example:
//
//	handler := func(ctx context.Context, msg *Msg) error {
//	    var event OrderCreatedEvent
//	    if err := json.Unmarshal(msg.Data, &event); err != nil {
//	        return err
//	    }
//	    // Process event...
//	    return nil
//	}
type EventConsumerHandler func(ctx context.Context, msg *Msg) error

// TypedEventConsumerHandler is a type-safe event consumer handler signature.
// The handler receives the deserialized event directly, along with the raw message
// for accessing headers and other metadata.
//
// Use RegisterTypedConsumer to register handlers with this signature.
//
// Example:
//
//	func (m *Module) handleOrderCreated(ctx context.Context, event OrderCreatedEvent, msg *mono.Msg) error {
//	    // event is already deserialized!
//	    fmt.Println("Order ID:", event.OrderID)
//	    return nil
//	}
type TypedEventConsumerHandler[T any] func(ctx context.Context, event T, msg *Msg) error

// EventConsumerEntry tracks an event consumer registration.
// The framework collects these entries from the EventRegistry during setupNATSSubscriptions
// and creates the actual NATS subscriptions.
type EventConsumerEntry struct {
	// EventDef is the event definition being consumed
	EventDef BaseEventDefinition

	// Handler is the consumer handler function
	Handler EventConsumerHandler

	// Module is the module that registered this consumer
	Module Module

	// QueueGroup is the NATS queue group name for load balancing.
	// Defaults to the consumer module's name if not explicitly provided.
	// Multiple consumers in the same queue group will share event processing (load balancing).
	QueueGroup string
}

// EventStreamConsumerHandler processes batches of messages from a JetStream pull consumer.
// The handler receives a slice of messages that should be individually acknowledged.
//
// Messages should be acknowledged using methods like Ack(), Nak(), NakWithDelay(), Term(), or InProgress().
// This is similar to StreamConsumerHandler but used for event-based JetStream consumers.
//
// Example:
//
//	handler := func(ctx context.Context, msgs []*Msg) error {
//	    for _, msg := range msgs {
//	        var event OrderCreatedEvent
//	        if err := json.Unmarshal(msg.Data, &event); err != nil {
//	            msg.Nak()
//	            continue
//	        }
//	        // Process event...
//	        msg.Ack()
//	    }
//	    return nil
//	}
type EventStreamConsumerHandler func(ctx context.Context, msgs []*Msg) error

// TypedEventStreamConsumerHandler is a type-safe batch event handler for JetStream consumers.
// The handler receives pre-deserialized events along with the raw messages for acknowledgment.
//
// Example:
//
//	func (m *Module) handleOrders(ctx context.Context, events []OrderCreatedEvent, msgs []*mono.Msg) error {
//	    for i, event := range events {
//	        fmt.Println("Order ID:", event.OrderID)
//	        msgs[i].Ack()
//	    }
//	    return nil
//	}
type TypedEventStreamConsumerHandler[T any] func(ctx context.Context, events []T, msgs []*Msg) error

// EventStreamConsumerEntry tracks a JetStream event stream consumer registration.
// The framework collects these entries from the EventRegistry during setupNATSSubscriptions
// and creates the JetStream streams and consumers.
type EventStreamConsumerEntry struct {
	// EventDef is the event definition being consumed
	EventDef BaseEventDefinition

	// Config is the StreamConsumerConfig with Stream.Subjects overridden to eventDef.Subject
	Config StreamConsumerConfig

	// Handler is the batch consumer handler function
	Handler EventStreamConsumerHandler

	// Module is the module that registered this consumer (the consuming module).
	// This is NOT the module that emits the event - for that, use EventDef.ModuleName.
	Module Module

	// SequenceID is a unique identifier for this consumer registration.
	// Used to ensure unique JetStream consumer names when multiple consumers
	// subscribe to the same event. Set automatically during registration.
	SequenceID int
}

// EventRegistry manages event definitions and consumer registrations for the framework's
// event-driven communication system.
//
// EventRegistry serves as the central catalog for all events in the application, enabling:
//   - Event producers to declare the events they will emit
//   - Event consumers to discover and subscribe to events without direct dependencies
//   - The framework to wire up NATS subscriptions automatically
//
// Unlike ServiceContainer which creates explicit module dependencies, EventRegistry enables
// loose coupling: emitters don't know their consumers, and consumers don't declare dependencies
// on emitters. This makes events ideal for broadcast notifications and decoupled architectures.
//
// # Initialization Sequence
//
// The registry is used during framework initialization in this order:
//  1. EventEmitterModules register their event definitions via RegisterEvent()
//  2. EventConsumerModules receive the registry via RegisterEventConsumers(registry)
//  3. Consumer modules discover events via GetEventByName() during RegisterEventConsumers()
//  4. Consumers register handlers via RegisterEventConsumer() or RegisterEventStreamConsumer()
//  5. Framework creates NATS subscriptions from Entries() and StreamConsumerEntries()
//
// # Event Discovery Pattern
//
// Consumer modules implement EventConsumerModule to receive the registry and register handlers.
// Events can be discovered by name, version, and emitter module without importing the emitter:
//
//	func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
//	    // Discover the event by name, version, and emitting module
//	    eventDef, found := registry.GetEventByName("OrderCreated", "v1", "order")
//	    if !found {
//	        return fmt.Errorf("event not found: OrderCreated.v1 from order")
//	    }
//
//	    // Register a fire-and-forget consumer (NATS Core)
//	    err := registry.RegisterEventConsumer(eventDef, m.handleOrderCreated, m)
//	    if err != nil {
//	        return err
//	    }
//
//	    // Or register a durable consumer (JetStream) for critical events
//	    config := mono.StreamConsumerConfig{
//	        Stream: mono.StreamConfig{Name: "notifications"},
//	        Fetch:  mono.FetchConfig{BatchSize: 10},
//	    }
//	    return registry.RegisterEventStreamConsumer(eventDef, config, m.handleOrderBatch, m)
//	}
//
// # Consumer Types
//
//   - EventConsumer: Fire-and-forget via NATS Core. Low latency (~1ms), no persistence.
//     Use for real-time notifications where occasional message loss is acceptable.
//
//   - EventStreamConsumer: Durable via JetStream. At-least-once delivery with ack/nack.
//     Use for critical events like payments, audits, or compliance where loss is unacceptable.
//
// See EventConsumerModule for the interface that consumer modules implement.
// See EventEmitterModule for the interface that producer modules implement.
//
// The registry is thread-safe for concurrent access.
type EventRegistry interface {
	// RegisterEvent registers an event definition (called by EventEmitterModule).
	// Returns an error if an event with the same name, version, and module already exists.
	RegisterEvent(def BaseEventDefinition) error

	// GetEventsByModule returns all events registered by a specific module.
	GetEventsByModule(moduleName string) []BaseEventDefinition

	// GetEventByName returns the event definition by name, version, and module name.
	// Parameter order: name first (what), version second (which version), moduleName third (from where).
	// Returns the event definition and true if found, or an empty definition and false if not found.
	GetEventByName(name string, version string, moduleName string) (BaseEventDefinition, bool)

	// GetAllEvents returns all registered event definitions.
	GetAllEvents() []BaseEventDefinition

	// RegisterEventConsumer registers a consumer for an event (called by consumer modules during Start).
	// This DOES NOT create the NATS subscription - it only registers the intent.
	// Framework will later call Entries() and set up NATS subscriptions.
	// The queueGroup parameter is optional (variadic). If not provided or empty, defaults to module.Name().
	// Multiple consumers with the same queue group will load balance event processing.
	// Importance: EventConsumer doesn't detect "no responder" error. There is a risk
	// of "loss event" when there is no consumer is available
	RegisterEventConsumer(eventDef BaseEventDefinition, handler EventConsumerHandler, module Module, queueGroup ...string) error

	// Entries returns all registered event consumers.
	// Framework uses this during setupNATSSubscriptions phase.
	// Named to be consistent with ServiceContainer.Entries().
	Entries() []EventConsumerEntry

	// RegisterEventStreamConsumer registers a JetStream durable consumer for an event.
	// The config.Stream.Subjects will be overridden with []string{eventDef.Subject}.
	// This provides durable, at-least-once delivery for event consumers using JetStream.
	//
	// Unlike RegisterEventConsumer which uses NATS core pub/sub (fire-and-forget),
	// EventStreamConsumer persists messages in JetStream and supports message acknowledgment,
	// redelivery on failure, and durable subscriptions.
	//
	// Example:
	//
	//	config := mono.StreamConsumerConfig{
	//	    Stream: mono.StreamConfig{
	//	        Name:      "order-events",
	//	        Retention: mono.WorkQueuePolicy,
	//	    },
	//	    Fetch: mono.FetchConfig{BatchSize: 10},
	//	}
	//	err := registry.RegisterEventStreamConsumer(orderCreatedV1.ToBase(), config, handler, m)
	RegisterEventStreamConsumer(eventDef BaseEventDefinition, config StreamConsumerConfig, handler EventStreamConsumerHandler, module Module) error

	// StreamConsumerEntries returns all registered event stream consumers.
	// Framework uses this during setupNATSSubscriptions phase to create JetStream streams and consumers.
	StreamConsumerEntries() []EventStreamConsumerEntry

	// SetMiddlewareChain sets the middleware chain for event consumer registration interception.
	// This allows middleware to wrap event consumer handlers or modify entries before they are stored.
	// Called by the lifecycle manager after building the middleware chain.
	SetMiddlewareChain(chain MiddlewareChainRunner)
}
