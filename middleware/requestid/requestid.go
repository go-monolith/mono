// Package requestid provides a middleware module for request ID tracking and propagation.
//
// The requestid module implements types.MiddlewareModule to:
//   - Extract request ID from incoming message headers (X-Request-ID)
//   - Generate a new UUID if no request ID is present
//   - Inject request ID into handler context
//   - Automatically propagate request ID to outgoing messages
//
// Example usage:
//
//	requestIDMiddleware, _ := requestid.New()
//
//	framework, _ := mono.NewMonoApplication()
//	framework.Register(requestIDMiddleware)
//	framework.Start(ctx)
package requestid

import (
	"context"

	"github.com/go-monolith/mono/v1/pkg/types"
	"github.com/google/uuid"
)

// RequestIDModule implements types.MiddlewareModule for request ID tracking.
//
// The module:
//   - Wraps RequestReply, QueueGroup, and StreamConsumer handlers
//   - Extracts X-Request-ID from incoming message headers
//   - Generates UUID if no request ID is present
//   - Injects request ID into handler context
//   - Automatically adds request ID to outgoing message headers
//
// Channel services are NOT supported (no ctx.Context in handlers).
type RequestIDModule struct {
	name       string
	headerName string
}

// Ensure Module implements MiddlewareModule.
var _ types.MiddlewareModule = (*RequestIDModule)(nil)

// New creates a new request ID middleware module.
//
// Example:
//
//	middleware, err := requestid.New(
//	    requestid.WithHeaderName("X-Request-ID"),
//	)
func New(opts ...Option) (*RequestIDModule, error) {
	options := defaultOptions()
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	return &RequestIDModule{
		name:       "requestid",
		headerName: options.HeaderName,
	}, nil
}

func (m *RequestIDModule) Name() string {
	return m.name
}

func (m *RequestIDModule) Start(_ context.Context) error {
	return nil
}

func (m *RequestIDModule) Stop(_ context.Context) error {
	return nil
}

// OnModuleLifecycle passes through unchanged.
func (m *RequestIDModule) OnModuleLifecycle(_ context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
	return event
}

// OnServiceRegistration wraps handlers to extract/generate request IDs.
//
// This wraps RequestReply, QueueGroup, and StreamConsumer handlers.
// Channel services are NOT supported (no ctx.Context in handlers).
func (m *RequestIDModule) OnServiceRegistration(_ context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	switch reg.Type {
	case types.ServiceTypeRequestReply:
		if reg.RequestHandler != nil {
			reg.RequestHandler = m.wrapRequestReplyHandler(reg.RequestHandler)
		}

	case types.ServiceTypeQueueGroup:
		if len(reg.QueueHandlers) > 0 {
			wrappedPairs := make([]types.QGHP, len(reg.QueueHandlers))
			for i, pair := range reg.QueueHandlers {
				wrappedPairs[i] = types.QGHP{
					QueueGroup: pair.QueueGroup,
					Handler:    m.wrapQueueGroupHandler(pair.Handler),
				}
			}
			reg.QueueHandlers = wrappedPairs
		}

	case types.ServiceTypeStreamConsumer:
		if reg.StreamHandler != nil {
			reg.StreamHandler = m.wrapStreamConsumerHandler(reg.StreamHandler)
		}

	// Channel services are NOT supported (no ctx.Context in handlers)
	case types.ServiceTypeChannel:
		// Pass through unchanged
	}

	return reg
}

// OnConfigurationChange passes through unchanged.
func (m *RequestIDModule) OnConfigurationChange(_ context.Context, event types.ConfigurationEvent) types.ConfigurationEvent {
	return event
}

// OnOutgoingMessage injects X-Request-ID header from context.
//
// This reads the request ID from the context and injects it into the outgoing
// message header. Only processes supported service types (not channel).
func (m *RequestIDModule) OnOutgoingMessage(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
	// Only process supported service types
	if octx.ServiceType == types.ServiceTypeChannel {
		return octx
	}

	// Extract request ID from context
	requestID := GetRequestID(octx.Ctx)
	if requestID == "" {
		return octx
	}

	// Ensure Header is not nil
	if octx.Msg.Header == nil {
		octx.Msg.Header = make(types.Header)
	}

	// Inject header (only if not already present to avoid overwriting)
	if len(octx.Msg.Header[m.headerName]) == 0 {
		octx.Msg.Header[m.headerName] = []string{requestID}
	}

	return octx
}

// OnEventConsumerRegistration wraps event consumer handlers to extract/generate request IDs.
func (m *RequestIDModule) OnEventConsumerRegistration(_ context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
	if entry.Handler != nil {
		entry.Handler = m.wrapEventConsumerHandler(entry.Handler)
	}
	return entry
}

// OnEventStreamConsumerRegistration wraps event stream consumer handlers to extract/generate request IDs.
func (m *RequestIDModule) OnEventStreamConsumerRegistration(_ context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
	if entry.Handler != nil {
		entry.Handler = m.wrapEventStreamConsumerHandler(entry.Handler)
	}
	return entry
}

// wrapEventConsumerHandler wraps an event consumer handler to extract/generate request ID.
func (m *RequestIDModule) wrapEventConsumerHandler(original types.EventConsumerHandler) types.EventConsumerHandler {
	return func(ctx context.Context, msg *types.Msg) error {
		ctx = m.ensureRequestID(ctx, msg)
		return original(ctx, msg)
	}
}

// wrapRequestReplyHandler wraps a handler to extract/generate request ID.
func (m *RequestIDModule) wrapRequestReplyHandler(original types.RequestReplyHandler) types.RequestReplyHandler {
	return func(ctx context.Context, req *types.Msg) ([]byte, error) {
		ctx = m.ensureRequestID(ctx, req)
		return original(ctx, req)
	}
}

// wrapQueueGroupHandler wraps a handler to extract/generate request ID.
func (m *RequestIDModule) wrapQueueGroupHandler(original types.QueueGroupHandler) types.QueueGroupHandler {
	return func(ctx context.Context, msg *types.Msg) error {
		ctx = m.ensureRequestID(ctx, msg)
		return original(ctx, msg)
	}
}

// wrapStreamConsumerHandler wraps a handler to extract/generate request ID.
//
// For batch processing, uses request ID from first message or generates new one.
func (m *RequestIDModule) wrapStreamConsumerHandler(original types.StreamConsumerHandler) types.StreamConsumerHandler {
	return func(ctx context.Context, msgs []*types.Msg) error {
		// Use request ID from first message or generate new
		var requestID string
		if len(msgs) > 0 {
			requestID = m.extractRequestID(msgs[0])
		}
		if requestID == "" {
			requestID = uuid.NewString()
		}
		ctx = context.WithValue(ctx, requestIDKey, requestID)
		return original(ctx, msgs)
	}
}

// wrapEventStreamConsumerHandler wraps an event stream consumer handler to extract/generate request ID.
//
// For batch processing, uses request ID from first message or generates new one.
func (m *RequestIDModule) wrapEventStreamConsumerHandler(original types.EventStreamConsumerHandler) types.EventStreamConsumerHandler {
	return func(ctx context.Context, msgs []*types.Msg) error {
		// Use request ID from first message or generate new
		var requestID string
		if len(msgs) > 0 {
			requestID = m.extractRequestID(msgs[0])
		}
		if requestID == "" {
			requestID = uuid.NewString()
		}
		ctx = context.WithValue(ctx, requestIDKey, requestID)
		return original(ctx, msgs)
	}
}

// ensureRequestID extracts request ID from header or generates a new one.
func (m *RequestIDModule) ensureRequestID(ctx context.Context, msg *types.Msg) context.Context {
	requestID := m.extractRequestID(msg)
	if requestID == "" {
		requestID = uuid.NewString()
	}
	return context.WithValue(ctx, requestIDKey, requestID)
}

// extractRequestID extracts request ID from message header.
func (m *RequestIDModule) extractRequestID(msg *types.Msg) string {
	if msg.Header == nil {
		return ""
	}
	values := msg.Header[m.headerName]
	if len(values) > 0 {
		return values[0]
	}
	return ""
}
