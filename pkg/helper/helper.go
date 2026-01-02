// Package helper provides convenience functions for working with the Mono framework.
// It includes utilities for creating typed event definitions, registering type-safe
// event consumers, and making typed service calls for request-reply and queue group services.
package helper

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// ToKebabCase converts a string to kebab-case.
// Handles CamelCase, PascalCase, acronyms (API, HTTP, ID), and replaces spaces/special chars with hyphens.
//
// Examples:
//   - "OrderCreated" -> "order-created"
//   - "APIHandler" -> "api-handler"
//   - "HTTPServer" -> "http-server"
//   - "UserID" -> "user-id"
//   - "Order Created" -> "order-created"
func ToKebabCase(s string) string {
	var result strings.Builder
	runes := []rune(s)

	for i := 0; i < len(runes); i++ {
		r := runes[i]

		if unicode.IsUpper(r) {
			// Add hyphen before uppercase if:
			// 1. Not at start AND previous was lowercase/digit
			// 2. Not at start AND next char is lowercase (end of acronym: "IDToken" -> "id-token")
			if i > 0 {
				prevIsLowerOrDigit := unicode.IsLower(runes[i-1]) || unicode.IsDigit(runes[i-1])
				nextIsLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])

				if prevIsLowerOrDigit || (unicode.IsUpper(runes[i-1]) && nextIsLower) {
					result.WriteRune('-')
				}
			}
			result.WriteRune(unicode.ToLower(r))
		} else if unicode.IsLetter(r) || unicode.IsDigit(r) {
			result.WriteRune(unicode.ToLower(r))
		} else {
			// Replace special chars and spaces with hyphens
			if result.Len() > 0 && !strings.HasSuffix(result.String(), "-") {
				result.WriteRune('-')
			}
		}
	}

	return strings.TrimRight(result.String(), "-")
}

// EventDefinition helper creates a new generic EventDefinition with JSON as the default
// serialization format.
//
// If subject is not provided, it will be auto-computed using the pattern:
//
//	events.{moduleName}.{version}.{kebab-case(name)}
//
// The subject is validated against the framework naming conventions:
//   - Must follow pattern: events.[<module>.]<domain>.[<sub-domain>].<event-type>
//   - All tokens must be kebab-case (lowercase, numbers, hyphens)
//   - Wildcards (*,>) are NOT allowed in event definitions
//   - Reserved prefix "_mono." is not allowed
//
// Panics if the subject is invalid. This is intentional because event definitions
// are typically created at package initialization time.
//
// Examples:
//
//	// Auto-computed subject: events.order.v1.order-created
//	var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
//	    "order", "OrderCreated", "v1",
//	)
//
//	// Explicit custom subject
//	var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
//	    "order", "OrderCreated", "v1", "events.orders.v1.created",
//	)
func EventDefinition[T any](moduleName, name, version string, subject ...string) types.EventDefinition[T] {
	// Compute subject if not provided
	finalSubject := ""
	if len(subject) > 0 && subject[0] != "" {
		finalSubject = subject[0]
	} else {
		finalSubject = fmt.Sprintf("events.%s.%s.%s", moduleName, version, ToKebabCase(name))
	}

	if err := errors.ValidateEventDefinitionSubject(finalSubject); err != nil {
		panic(fmt.Sprintf("invalid event definition subject for %s.%s.%s: %v", moduleName, name, version, err))
	}
	subjectTokens := strings.Split(finalSubject, ".")
	if !slices.Contains(subjectTokens, version) {
		panic(fmt.Sprintf("invalid event definition subject for %s.%s.%s: subject must include version token '%s'", moduleName, name, version, version))
	}
	return types.NewEventDefinition[T](moduleName, name, version, finalSubject)
}

// RegisterTypedEventConsumer registers a type-safe consumer for an event.
// This is a convenience function that wraps the typed handler in a standard
// EventConsumerHandler with automatic unmarshaling.
// Important: EventConsumer doesn't detect "no responder" error. There is a risk
// of event loss when no consumer is available.
//
// Example:
//
//	err := helper.RegisterTypedEventConsumer(
//	    registry,
//	    order.OrderCreatedV1,
//	    m.handleOrderCreated,
//	    m,
//	)
//
//	func (m *Module) handleOrderCreated(ctx context.Context, event order.OrderCreatedEvent, msg *types.Msg) error {
//	    // event is already deserialized
//	    fmt.Println("Order ID:", event.OrderID)
//	    return nil
//	}
func RegisterTypedEventConsumer[T any](
	registry types.EventRegistry,
	eventDef types.EventDefinition[T],
	handler types.TypedEventConsumerHandler[T],
	module types.Module,
	queueGroup ...string,
) error {
	// Wrap the typed handler in a standard EventConsumerHandler
	wrappedHandler := func(ctx context.Context, msg *types.Msg) error {
		event, err := eventDef.Unmarshal(msg)
		if err != nil {
			return err
		}
		return handler(ctx, event, msg)
	}

	return registry.RegisterEventConsumer(eventDef.ToBase(), wrappedHandler, module, queueGroup...)
}

// RegisterTypedEventStreamConsumer registers a type-safe JetStream consumer for an event.
// This is a convenience function that wraps the typed handler in a standard
// EventStreamConsumerHandler with automatic unmarshaling of batched messages.
//
// Unlike RegisterTypedEventConsumer which uses NATS core pub/sub (fire-and-forget),
// EventStreamConsumer provides durable, at-least-once delivery using JetStream.
// Messages are persisted and will be redelivered on failure until acknowledged.
//
// The config.Stream.Subjects will be overridden with the event definition's subject.
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
//
//	err := helper.RegisterTypedEventStreamConsumer(
//	    registry,
//	    order.OrderCreatedV1,
//	    config,
//	    m.handleOrderBatch,
//	    m,  // consumer module
//	)
//
//	func (m *Module) handleOrderBatch(ctx context.Context, events []order.OrderCreatedEvent, msgs []*types.Msg) error {
//	    for i, event := range events {
//	        fmt.Println("Order ID:", event.OrderID)
//	        msgs[i].Ack()
//	    }
//	    return nil
//	}
func RegisterTypedEventStreamConsumer[T any](
	registry types.EventRegistry,
	eventDef types.EventDefinition[T],
	config types.StreamConsumerConfig,
	handler types.TypedEventStreamConsumerHandler[T],
	module types.Module,
) error {
	// Wrap the typed handler in a standard EventStreamConsumerHandler
	wrappedHandler := func(ctx context.Context, msgs []*types.Msg) error {
		events := make([]T, 0, len(msgs))
		for _, msg := range msgs {
			event, err := eventDef.Unmarshal(msg)
			if err != nil {
				return fmt.Errorf("failed to unmarshal event: %w", err)
			}
			events = append(events, event)
		}
		return handler(ctx, events, msgs)
	}

	return registry.RegisterEventStreamConsumer(eventDef.ToBase(), config, wrappedHandler, module)
}

// CallRequestReplyService performs a typed request-reply call with automatic marshaling
// and unmarshaling of request and response payloads.
//
// This helper function eliminates boilerplate code for service calls by:
//   - Marshaling the typed request into a byte slice
//   - Retrieving the request-reply service client from the container
//   - Executing the synchronous Call operation
//   - Unmarshaling the response into the provided response object
//
// Type Parameters:
//   - T1: The request payload type (must be marshalable)
//   - T2: The response payload type (must be unmarshalable)
//
// Parameters:
//   - ctx: Context for cancellation and deadline control
//   - container: ServiceContainer to retrieve the service client from
//   - serviceName: Name of the request-reply service to call
//   - marshalF: Function to marshal the request (e.g., json.Marshal, proto.Marshal)
//   - unmarshalF: Function to unmarshal the response (e.g., json.Unmarshal, proto.Unmarshal)
//   - req: Pointer to the request payload to send
//   - resp: Pointer to the response object to populate (modified in-place)
//
// Returns:
//   - error: Returns error if marshaling, service call, or unmarshaling fails
//
// Example:
//
//	type PlaceOrderRequest struct {
//	    ProductID string
//	    Quantity  int
//	}
//
//	type PlaceOrderResponse struct {
//	    OrderID   string
//	    Status    string
//	}
//
//	req := &PlaceOrderRequest{ProductID: "ABC123", Quantity: 2}
//	var resp PlaceOrderResponse
//
//	err := helper.CallRequestReplyService(
//	    ctx,
//	    container,
//	    "place-order",
//	    json.Marshal,
//	    json.Unmarshal,
//	    req,
//	    &resp,
//	)
//	if err != nil {
//	    return fmt.Errorf("failed to place order: %w", err)
//	}
//	fmt.Printf("Order placed: %s (status: %s)\n", resp.OrderID, resp.Status)
func CallRequestReplyService[T1 any, T2 any](ctx context.Context, container types.ServiceContainer, serviceName string, marshalF types.Marshaler, unmarshalF types.Unmarshaler, req T1, resp *T2) error {
	// Validate response pointer is not nil
	if resp == nil {
		return fmt.Errorf("response pointer cannot be nil")
	}

	// Serialize request
	data, err := marshalF(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// Get the request-reply service client
	serviceClient, err := container.GetRequestReplyService(serviceName)
	if err != nil {
		return fmt.Errorf("failed to get service '%s': %w", serviceName, err)
	}

	// Make the synchronous call
	serviceResp, err := serviceClient.Call(ctx, data)
	if err != nil {
		return fmt.Errorf("failed to call service '%s': %w", serviceName, err)
	}

	// Deserialize response
	if err := unmarshalF(serviceResp.Data, resp); err != nil {
		return fmt.Errorf("failed to parse service '%s' response: %w", serviceName, err)
	}

	return nil
}

// SendQueueGroupService performs a typed fire-and-forget send to a queue group service
// with automatic marshaling of the request payload.
//
// This helper function eliminates boilerplate code for queue group sends by:
//   - Marshaling the typed request into a byte slice
//   - Retrieving the queue group service client from the container
//   - Executing the Send operation (with ACK for reliability)
//   - Detecting if no handlers are available (ErrServiceUnavailable)
//
// Type Parameters:
//   - T: The request payload type (must be marshalable)
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - container: ServiceContainer to retrieve the service client from
//   - serviceName: Name of the queue group service to send to
//   - marshalF: Function to marshal the request (e.g., json.Marshal, proto.Marshal)
//   - req: Pointer to the request payload to send
//
// Returns:
//   - error: Returns error if marshaling fails, service is not found, or no handlers are online
//
// Important:
//   - This is a fire-and-forget operation - no response is expected
//   - The send will wait for an ACK from a handler to confirm delivery
//   - Returns ErrServiceUnavailable if no handlers are available
//   - Messages are load-balanced across all online handlers in the queue group
//
// Example:
//
//	type SendNotificationRequest struct {
//	    UserID  string
//	    Message string
//	    Type    string
//	}
//
//	req := &SendNotificationRequest{
//	    UserID:  "user-123",
//	    Message: "Your order has shipped!",
//	    Type:    "email",
//	}
//
//	err := helper.SendQueueGroupService(
//	    ctx,
//	    container,
//	    "send-notification",
//	    json.Marshal,
//	    req,
//	)
//	if err != nil {
//	    return fmt.Errorf("failed to send notification: %w", err)
//	}
//	fmt.Println("Notification queued successfully")
func SendQueueGroupService[T any](ctx context.Context, container types.ServiceContainer, serviceName string, marshalF types.Marshaler, req T) error {
	// Serialize request
	data, err := marshalF(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// Get the queue group service client
	serviceClient, err := container.GetQueueGroupService(serviceName)
	if err != nil {
		return fmt.Errorf("failed to get service '%s': %w", serviceName, err)
	}

	// Send the message (fire-and-forget with ACK)
	if err := serviceClient.Send(ctx, data); err != nil {
		return fmt.Errorf("failed to send to service '%s': %w", serviceName, err)
	}

	return nil
}

// PublishStreamConsumerService performs a typed publish to a JetStream stream
// with automatic marshaling of the message payload.
//
// This helper function eliminates boilerplate code for stream publishing by:
//   - Marshaling the typed message into a byte slice
//   - Retrieving the stream consumer service client from the container
//   - Publishing the message to the JetStream stream
//   - Returning the publish acknowledgment for tracking
//
// Type Parameters:
//   - T: The message payload type (must be marshalable)
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - container: ServiceContainer to retrieve the service client from
//   - serviceName: Name of the stream consumer service to publish to
//   - marshalF: Function to marshal the message (e.g., json.Marshal, proto.Marshal)
//   - msg: Pointer to the message payload to publish
//
// Returns:
//   - types.MsgPubAck: JetStream publish acknowledgment with sequence number and stream info
//   - error: Returns error if marshaling, service lookup, or publishing fails
//
// Important:
//   - Messages are persisted in JetStream and delivered to durable consumers
//   - The publish acknowledgment confirms the message was stored in the stream
//   - Messages will be redelivered to consumers on failure until acknowledged
//   - Stream retention policies control how long messages are stored
//
// Example:
//
//	type OrderCreatedEvent struct {
//	    OrderID    string
//	    CustomerID string
//	    Amount     float64
//	    Timestamp  time.Time
//	}
//
//	event := &OrderCreatedEvent{
//	    OrderID:    "order-456",
//	    CustomerID: "cust-789",
//	    Amount:     99.99,
//	    Timestamp:  time.Now(),
//	}
//
//	ack, err := helper.PublishStreamConsumerService(
//	    ctx,
//	    container,
//	    "order-processor",
//	    json.Marshal,
//	    event,
//	)
//	if err != nil {
//	    return fmt.Errorf("failed to publish event: %w", err)
//	}
//	fmt.Printf("Event published with sequence: %d\n", ack.Sequence)
func PublishStreamConsumerService[T any](ctx context.Context, container types.ServiceContainer, serviceName string, marshalF types.Marshaler, event T) (types.MsgPubAck, error) {
	// Serialize message
	data, err := marshalF(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	// Get the stream consumer service client
	serviceClient, err := container.GetStreamConsumerService(serviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get service '%s': %w", serviceName, err)
	}

	// Publish the message to the stream
	ack, err := serviceClient.Publish(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("failed to publish to service '%s': %w", serviceName, err)
	}

	return ack, nil
}

// RegisterTypedRequestReplyService registers a type-safe request-reply service
// with automatic marshaling and unmarshaling of request and response payloads.
//
// This helper function eliminates boilerplate code for service registration by:
//   - Unmarshaling the raw request bytes into the typed request object
//   - Passing the typed request to the handler along with the original message
//   - Marshaling the typed response back to bytes for transmission
//
// Type Parameters:
//   - Req: The request payload type (must be unmarshalable)
//   - Resp: The response payload type (must be marshalable)
//
// Parameters:
//   - container: ServiceContainer to register the service in
//   - name: Name of the request-reply service
//   - unmarshalReq: Function to unmarshal the request (e.g., json.Unmarshal)
//   - marshalResp: Function to marshal the response (e.g., json.Marshal)
//   - handler: Type-safe handler function that receives typed request and returns typed response
//
// Returns:
//   - error: Returns error if registration fails
//
// Example:
//
//	type PlaceOrderRequest struct {
//	    ProductID string
//	    Quantity  int
//	}
//
//	type PlaceOrderResponse struct {
//	    OrderID string
//	    Status  string
//	}
//
//	err := helper.RegisterTypedRequestReplyService(
//	    container,
//	    "place-order",
//	    json.Unmarshal,
//	    json.Marshal,
//	    func(ctx context.Context, req PlaceOrderRequest, msg *types.Msg) (PlaceOrderResponse, error) {
//	        // req is already deserialized
//	        orderID := processOrder(req.ProductID, req.Quantity)
//	        return PlaceOrderResponse{OrderID: orderID, Status: "created"}, nil
//	    },
//	)
func RegisterTypedRequestReplyService[Req any, Resp any](
	container types.ServiceContainer,
	name string,
	unmarshalReq types.Unmarshaler,
	marshalResp types.Marshaler,
	handler types.TypedRequestReplyHandler[Req, Resp],
) error {
	// Wrap the typed handler in a standard RequestReplyHandler
	wrappedHandler := func(ctx context.Context, msg *types.Msg) ([]byte, error) {
		var req Req
		if err := unmarshalReq(msg.Data, &req); err != nil {
			return nil, fmt.Errorf("service '%s': failed to unmarshal request: %w", name, err)
		}

		resp, err := handler(ctx, req, msg)
		if err != nil {
			return nil, err
		}

		data, err := marshalResp(resp)
		if err != nil {
			return nil, fmt.Errorf("service '%s': failed to marshal response: %w", name, err)
		}

		return data, nil
	}

	return container.RegisterRequestReplyService(name, wrappedHandler)
}

// RegisterTypedQueueGroupService registers a type-safe queue group service
// with automatic unmarshaling of message payloads.
//
// This helper function eliminates boilerplate code for queue group registration by:
//   - Converting typed handler pairs (TypedQGHP) to standard QGHP pairs
//   - Wrapping each typed handler to automatically unmarshal message data
//   - Passing the typed payload to the handler along with the original message
//
// Type Parameters:
//   - T: The message payload type (must be unmarshalable)
//
// Parameters:
//   - container: ServiceContainer to register the service in
//   - name: Name of the queue group service
//   - unmarshalF: Function to unmarshal messages (e.g., json.Unmarshal)
//   - pairs: One or more TypedQGHP pairs associating queue groups with typed handlers
//
// Returns:
//   - error: Returns error if registration fails
//
// Example:
//
//	type NotificationRequest struct {
//	    UserID  string
//	    Message string
//	    Type    string
//	}
//
//	err := helper.RegisterTypedQueueGroupService(
//	    container,
//	    "send-notification",
//	    json.Unmarshal,
//	    types.TypedQGHP[NotificationRequest]{
//	        QueueGroup: "email-workers",
//	        Handler: func(ctx context.Context, req NotificationRequest, msg *types.Msg) error {
//	            // req is already deserialized
//	            sendEmail(req.UserID, req.Message)
//	            return nil
//	        },
//	    },
//	    types.TypedQGHP[NotificationRequest]{
//	        QueueGroup: "sms-workers",
//	        Handler: func(ctx context.Context, req NotificationRequest, msg *types.Msg) error {
//	            sendSMS(req.UserID, req.Message)
//	            return nil
//	        },
//	    },
//	)
func RegisterTypedQueueGroupService[T any](
	container types.ServiceContainer,
	name string,
	unmarshalF types.Unmarshaler,
	pairs ...types.TypedQGHP[T],
) error {
	// Convert typed pairs to standard QGHP pairs
	qghpPairs := make([]types.QGHP, 0, len(pairs))
	for _, typedPair := range pairs {
		// Capture the handler and queue group for the closure
		typedHandler := typedPair.Handler
		queueGroup := typedPair.QueueGroup

		// Wrap the typed handler in a standard QueueGroupHandler
		wrappedHandler := func(ctx context.Context, msg *types.Msg) error {
			var payload T
			if err := unmarshalF(msg.Data, &payload); err != nil {
				return fmt.Errorf("service '%s' queue group '%s': failed to unmarshal message: %w", name, queueGroup, err)
			}
			return typedHandler(ctx, payload, msg)
		}

		qghpPairs = append(qghpPairs, types.QGHP{
			QueueGroup: queueGroup,
			Handler:    wrappedHandler,
		})
	}

	return container.RegisterQueueGroupService(name, qghpPairs...)
}

// RegisterTypedStreamConsumerService registers a type-safe JetStream stream consumer service
// with automatic unmarshaling of batched message payloads.
//
// This helper function eliminates boilerplate code for stream consumer registration by:
//   - Wrapping the typed batch handler in a standard StreamConsumerHandler
//   - Automatically unmarshaling each message in the batch into typed objects
//   - Passing both the typed payloads and original messages to the handler for acknowledgment
//
// Type Parameters:
//   - T: The message payload type (must be unmarshalable)
//
// Parameters:
//   - container: ServiceContainer to register the service in
//   - name: Name of the stream consumer service
//   - config: JetStream stream and consumer configuration
//   - unmarshalF: Function to unmarshal messages (e.g., json.Unmarshal)
//   - handler: Type-safe batch handler that receives typed payloads and original messages
//
// Returns:
//   - error: Returns error if registration fails
//
// Important:
//   - Messages should be individually acknowledged using Ack(), Nak(), etc.
//   - The payloads slice and msgs slice are parallel - payloads[i] corresponds to msgs[i]
//   - If unmarshaling fails for any message, the error is returned and no messages are processed
//
// Example:
//
//	type OrderEvent struct {
//	    OrderID    string
//	    CustomerID string
//	    Amount     float64
//	}
//
//	config := types.StreamConsumerConfig{
//	    Stream: types.StreamConfig{
//	        Name:      "order-events",
//	        Retention: types.WorkQueuePolicy,
//	    },
//	    Fetch: types.FetchConfig{BatchSize: 10},
//	}
//
//	err := helper.RegisterTypedStreamConsumerService(
//	    container,
//	    "order-processor",
//	    config,
//	    json.Unmarshal,
//	    func(ctx context.Context, orders []OrderEvent, msgs []*types.Msg) error {
//	        for i, order := range orders {
//	            // orders[i] is already deserialized
//	            if err := processOrder(order); err != nil {
//	                msgs[i].Nak() // Requeue for retry
//	                continue
//	            }
//	            msgs[i].Ack()
//	        }
//	        return nil
//	    },
//	)
func RegisterTypedStreamConsumerService[T any](
	container types.ServiceContainer,
	name string,
	config types.StreamConsumerConfig,
	unmarshalF types.Unmarshaler,
	handler types.TypedStreamConsumerHandler[T],
) error {
	// Wrap the typed handler in a standard StreamConsumerHandler
	wrappedHandler := func(ctx context.Context, msgs []*types.Msg) error {
		payloads := make([]T, 0, len(msgs))
		for _, msg := range msgs {
			var payload T
			if err := unmarshalF(msg.Data, &payload); err != nil {
				return fmt.Errorf("service '%s': failed to unmarshal message: %w", name, err)
			}
			payloads = append(payloads, payload)
		}
		return handler(ctx, payloads, msgs)
	}

	return container.RegisterStreamConsumerService(name, config, wrappedHandler)
}
