package container

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// RegisterQueueGroupService registers a queue group service with multiple handlers and acknowledgment.
//
// QueueGroup services provide asynchronous message processing using NATS queue groups.
// Multiple handler pairs can be registered for a single service, each with its own queue group.
// All handlers share the same subject, and messages are load-balanced within each queue group
// (one handler per queue group processes each message).
//
// Parameters:
//   - name: Service name (must be kebab-case)
//   - pairs: One or more QueueGroupHandlerPair, each defining a queue group and handler.
//     A pair's Options bound the worker pool of its subscription on this instance
//     (see types.ServiceOptions); zero values select the framework defaults.
//
// The service subject is computed as: services.<module>.<service>
//
// Returns an error if a pair's Options are negative.
//
// Example:
//
//	highHandler := func(ctx context.Context, msg *types.Msg) error {
//	    // Process high-priority messages
//	    return nil
//	}
//	lowHandler := func(ctx context.Context, msg *types.Msg) error {
//	    // Process low-priority messages
//	    return nil
//	}
//	err := container.RegisterQueueGroupService("process-order",
//	    types.QGHP{QueueGroup: "high-priority-workers", Handler: highHandler},
//	    types.QGHP{QueueGroup: "low-priority-workers", Handler: lowHandler},
//	)
func (c *serviceContainer) RegisterQueueGroupService(name string, pairs ...types.QGHP) error {
	// Validate at least one pair
	if len(pairs) == 0 {
		return fmt.Errorf("at least one QueueGroupHandlerPair is required for service '%s'", name)
	}

	// Validate each pair + check for duplicate queue groups
	seen := make(map[string]bool)
	for i, pair := range pairs {
		if pair.QueueGroup == "" {
			return fmt.Errorf("queue group name cannot be empty for pair %d in service '%s'", i, name)
		}
		if pair.Handler == nil {
			return fmt.Errorf("handler cannot be nil for pair %d (queue group '%s') in service '%s'", i, pair.QueueGroup, name)
		}
		if seen[pair.QueueGroup] {
			return fmt.Errorf("duplicate queue group '%s' in service '%s'", pair.QueueGroup, name)
		}
		if err := pair.Options.Validate(); err != nil {
			return fmt.Errorf("invalid options for pair %d (queue group '%s') in service '%s': %w", i, pair.QueueGroup, name, err)
		}
		seen[pair.QueueGroup] = true
	}

	// Compute subject
	subject, err := c.computeServiceSubject(name)
	if err != nil {
		return err
	}

	entry := &types.ServiceEntry{
		Type:          types.ServiceTypeQueueGroup,
		QueueHandlers: pairs,
		Subject:       subject,
	}

	return c.registerService(name, entry)
}

// GetQueueGroupService retrieves a queue group service client.
//
// Returns a client that can be used to send messages to the queue group.
// Returns ErrServiceNotFound if the service is not registered.
// Returns an error if the service type is not QueueGroup.
//
// Example:
//
//	client, err := container.GetQueueGroupService("process-order")
//	if err != nil {
//	    return err
//	}
//	err = client.Send(ctx, []byte("order-data"))
func (c *serviceContainer) GetQueueGroupService(name string) (types.QueueGroupServiceClient, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.services[name]
	if !exists {
		if c.boundModule != nil {
			return nil, monoerrors.WrapServiceNotFound(name, c.boundModule.Name())
		}
		return nil, monoerrors.WrapServiceNotFound(name, "<unbound>")
	}

	if entry.Type != types.ServiceTypeQueueGroup {
		return nil, monoerrors.WrapServiceError(name, entry.ModuleName, entry.Type,
			fmt.Errorf("service is not a QueueGroup service (type: %s)", types.FormatServiceType(entry.Type)))
	}

	// Validate EventBus is available
	if c.eventBus == nil {
		return nil, monoerrors.WrapServiceError(name, entry.ModuleName, entry.Type,
			fmt.Errorf("EventBus not available (required for QueueGroup services)"))
	}

	return &queueGroupClient{
		subject:          entry.Subject,
		eventBus:         c.eventBus,
		timeout:          5 * time.Second,
		optimisticWindow: c.queueGroupOptimisticWindow,
		middlewareChain:  c.middlewareChain,
		serviceName:      name,
		moduleName:       entry.ModuleName,
	}, nil
}

// queueGroupClient implements types.QueueGroupServiceClient
type queueGroupClient struct {
	subject           string
	eventBus          types.EventBus
	timeout           time.Duration
	optimisticWindow  time.Duration // Optimistic publish window (0 = disabled, always use ACK)
	lastSuccessfulACK atomic.Int64  // Unix nano timestamp of last successful ACK (0 = never)
	middlewareChain   types.MiddlewareChainRunner
	serviceName       string
	moduleName        string
}

// ensureContextDeadline ensures the context has a deadline, applying the default timeout if needed.
// Returns an error if the deadline is already expired.
func (c *queueGroupClient) ensureContextDeadline(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if deadline, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel := context.WithTimeout(ctx, c.timeout)
		return ctx, cancel, nil
	} else {
		if time.Until(deadline) <= 0 {
			return ctx, nil, monoerrors.WrapTimeout("queue-group", 0, context.DeadlineExceeded)
		}
		return ctx, nil, nil
	}
}

// runMiddleware runs the message through the middleware chain, allowing header injection.
// Returns the potentially modified message.
func (c *queueGroupClient) runMiddleware(ctx context.Context, msg *types.Msg) *types.Msg {
	if c.middlewareChain == nil {
		return msg
	}

	octx := types.OutgoingMessageContext{
		ServiceType: types.ServiceTypeQueueGroup,
		ServiceName: c.serviceName,
		ModuleName:  c.moduleName,
		Subject:     c.subject,
		Msg:         msg,
		Ctx:         ctx,
		Metadata:    make(map[string]any),
	}
	octx = c.middlewareChain.RunOutgoingMessage(octx)
	return octx.Msg
}

// sendInternal implements the smart switching logic for queue group sends.
// It switches between ACK mode (request-reply) and publish mode (fire-and-forget)
// based on the optimistic window and last successful ACK time.
func (c *queueGroupClient) sendInternal(ctx context.Context, data []byte) error {
	// Lock-free read for performance - stale reads are acceptable
	// (worst case: use ACK when publish would work, which is safe)
	lastACK := c.lastSuccessfulACK.Load()
	now := time.Now().UnixNano()

	// Use publish if within optimistic window (direct nanosecond comparison for performance)
	usePublish := c.optimisticWindow > 0 &&
		lastACK > 0 &&
		now-lastACK < c.optimisticWindow.Nanoseconds()

	// In ACK mode the send waits for the ACK under the client's default
	// timeout when the caller's context has none; apply it before building the
	// message so the deadline headers carry it. In optimistic publish mode no
	// default is applied and the headers reflect only a caller-supplied deadline.
	if !usePublish {
		var cancel context.CancelFunc
		var err error
		ctx, cancel, err = c.ensureContextDeadline(ctx)
		if err != nil {
			return err
		}
		if cancel != nil {
			defer cancel()
		}
	}

	// Build message with headers
	msg := &types.Msg{
		Subject: c.subject,
		Data:    data,
		Header:  make(types.Header),
	}

	// Propagate the caller's deadline (when the context has one; in optimistic
	// publish mode no default is applied), then run through middleware chain
	// to allow header injection. Queue group handlers are not skipped on an
	// expired deadline: the ACK promised processing.
	setDeadlineHeaders(ctx, msg)
	msg = c.runMiddleware(ctx, msg)

	if usePublish {
		// Fast path: fire-and-forget publish with headers
		err := c.eventBus.PublishMsg(msg)
		if err != nil {
			// Reset to ACK mode on failure
			c.lastSuccessfulACK.Store(0)
			return err
		}
		return nil
	}

	// Slow path: request with ACK (the deadline was applied above)
	// Use request-reply to wait for ACK with headers
	ack, err := c.eventBus.RequestMsgWithContext(ctx, msg)
	if err != nil {
		// Wrap context deadline exceeded errors with timeout error
		if errors.Is(err, context.DeadlineExceeded) {
			return monoerrors.WrapTimeout("queue-group", c.timeout, err)
		}
		// ErrServiceUnavailable is already wrapped by eventbus.RequestWithContext
		// when nats.ErrNoResponders occurs
		return err
	}
	if err := c.checkACK(ack); err != nil {
		return err
	}

	// Success - update last ACK time (atomic store)
	c.lastSuccessfulACK.Store(now)

	return nil
}

// checkACK inspects the acknowledgement of a send. The handling instance
// answers with an error response instead of an empty ACK when it rejected the
// message: because its worker pool and pending queue are full
// (ErrServiceOverloaded) or because it is shutting down
// (ErrServiceUnavailable). Such a message was not handled, so the caller may
// retry. Any other error response is reported as a RemoteError.
func (c *queueGroupClient) checkACK(ack *types.Msg) error {
	if ack == nil || ack.Header == nil {
		return nil
	}
	errorValues := ack.Header[types.HeaderError]
	if len(errorValues) == 0 || errorValues[0] != "true" {
		return nil
	}

	errorType := ""
	if typeValues := ack.Header[types.HeaderErrorType]; len(typeValues) > 0 {
		errorType = typeValues[0]
	}
	if err := frameworkErrorResponse(c.serviceName, c.moduleName, types.ServiceTypeQueueGroup, errorType); err != nil {
		return err
	}

	message := "unknown error"
	if msgValues := ack.Header[types.HeaderErrorMessage]; len(msgValues) > 0 {
		message = msgValues[0]
	}
	return monoerrors.WrapRemoteError(c.serviceName, c.moduleName, message, errorType)
}

// sendInternalMsg implements smart switching for queue group sends with full message support.
func (c *queueGroupClient) sendInternalMsg(ctx context.Context, msg *types.Msg) error {
	// Lock-free read for performance
	lastACK := c.lastSuccessfulACK.Load()
	now := time.Now().UnixNano()

	// Use publish if within optimistic window
	usePublish := c.optimisticWindow > 0 &&
		lastACK > 0 &&
		now-lastACK < c.optimisticWindow.Nanoseconds()

	// In ACK mode the send waits for the ACK under the client's default
	// timeout when the caller's context has none; apply it before building the
	// message so the deadline headers carry it. In optimistic publish mode no
	// default is applied and the headers reflect only a caller-supplied deadline.
	if !usePublish {
		var cancel context.CancelFunc
		var err error
		ctx, cancel, err = c.ensureContextDeadline(ctx)
		if err != nil {
			return err
		}
		if cancel != nil {
			defer cancel()
		}
	}

	// Build full message with service subject and a copy of the input headers
	fullMsg := &types.Msg{
		Subject: c.subject,
		Data:    msg.Data,
		Header:  cloneHeader(msg.Header),
	}

	// Propagate the caller's deadline, then run through middleware chain to
	// allow header injection
	setDeadlineHeaders(ctx, fullMsg)
	fullMsg = c.runMiddleware(ctx, fullMsg)

	if usePublish {
		// Fast path: fire-and-forget publish with headers
		err := c.eventBus.PublishMsg(fullMsg)
		if err != nil {
			c.lastSuccessfulACK.Store(0)
			return err
		}
		return nil
	}

	// Slow path: request with ACK (the deadline was applied above)
	// Use request-reply to wait for ACK with headers
	ack, err := c.eventBus.RequestMsgWithContext(ctx, fullMsg)
	if err != nil {
		// Wrap context deadline exceeded errors with timeout error
		if errors.Is(err, context.DeadlineExceeded) {
			return monoerrors.WrapTimeout("queue-group", c.timeout, err)
		}
		return err
	}
	if err := c.checkACK(ack); err != nil {
		return err
	}

	c.lastSuccessfulACK.Store(now)
	return nil
}

// Send sends a message payload to the queue group and waits for acknowledgment.
//
// Smart Switching Behavior:
// - First send uses request-reply (ACK) to verify service availability
// - Subsequent sends within the optimistic window use fire-and-forget publish
// - On publish failure, resets to ACK mode for next send (current send fails)
//
// Errors returned indicate:
//   - Service unavailable (no handlers online, or the handling instance is shutting down)
//   - Service overloaded (the handling instance's worker pool and pending queue are full;
//     the message was not handled and may be re-sent)
//   - Timeout waiting for ACK
//   - Context cancellation
//
// Handler processing errors do NOT propagate to the sender as processing
// happens asynchronously after ACK.
//
// Example:
//
//	err := client.Send(ctx, []byte("order-data"))
//	if err != nil {
//	    if errors.Is(err, monoerrors.ErrServiceUnavailable) {
//	        // No handlers are online
//	    }
//	    // Handle other errors
//	}
func (c *queueGroupClient) Send(ctx context.Context, data []byte) error {
	return c.sendInternal(ctx, data)
}

// SendMsg sends a raw message to the queue group and waits for acknowledgment.
//
// Smart Switching Behavior:
// - First send uses request-reply (ACK) to verify service availability
// - Subsequent sends within the optimistic window use fire-and-forget publish
// - On publish failure, resets to ACK mode for next send (current send fails)
//
// Note: The message Data and Headers are transmitted. The Subject field is ignored
// as the service subject is always used for routing.
//
// Errors returned indicate:
//   - Service unavailable (no handlers online, or the handling instance is shutting down)
//   - Service overloaded (the handling instance's worker pool and pending queue are full;
//     the message was not handled and may be re-sent)
//   - Timeout waiting for ACK
//   - Context cancellation
//
// Handler processing errors do NOT propagate to the sender.
//
// Example:
//
//	msg := &types.Msg{
//	    Data: []byte("order-data"),
//	    Header: map[string][]string{"Priority": {"high"}},
//	}
//	err := client.SendMsg(ctx, msg)
func (c *queueGroupClient) SendMsg(ctx context.Context, msg *types.Msg) error {
	return c.sendInternalMsg(ctx, msg)
}
