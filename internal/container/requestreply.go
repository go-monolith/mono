package container

import (
	"context"
	"errors"
	"fmt"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// RegisterRequestReplyService registers a request-reply service over NATS.
//
// RequestReply services provide synchronous request-response communication using NATS.
// The handler function is called when a request is received, and the response is sent back to the caller.
//
// Parameters:
//   - name: Service name (must be kebab-case)
//   - handler: Function to process requests and return responses
//
// The service subject is computed as: services.<module>.<service>
//
// Example:
//
//	handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
//	    // Process request
//	    return []byte("response"), nil
//	}
//	err := container.RegisterRequestReplyService("check-stock", handler)
func (c *serviceContainer) RegisterRequestReplyService(name string, handler types.RequestReplyHandler) error {
	// Validate handler
	if handler == nil {
		return fmt.Errorf("handler cannot be nil for service '%s'", name)
	}

	// Compute subject
	subject, err := c.computeServiceSubject(name)
	if err != nil {
		return err
	}

	entry := &types.ServiceEntry{
		Type:           types.ServiceTypeRequestReply,
		RequestHandler: handler,
		Subject:        subject,
		// Use service name as queue group for "at most one" delivery semantics.
		// This ensures only one instance handles each request when multiple instances are running.
		QueueGroup: name,
	}

	return c.registerService(name, entry)
}

// GetRequestReplyService retrieves a request-reply service client.
//
// Returns a client that can be used to make requests to the service.
// Returns ErrServiceNotFound if the service is not registered.
// Returns an error if the service type is not RequestReply.
//
// Example:
//
//	client, err := container.GetRequestReplyService("check-stock")
//	if err != nil {
//	    return err
//	}
//	response, err := client.Call(ctx, []byte("product-123"))
func (c *serviceContainer) GetRequestReplyService(name string) (types.RequestReplyServiceClient, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.services[name]
	if !exists {
		if c.boundModule != nil {
			return nil, monoerrors.WrapServiceNotFound(name, c.boundModule.Name())
		}
		return nil, monoerrors.WrapServiceNotFound(name, "<unbound>")
	}

	if entry.Type != types.ServiceTypeRequestReply {
		return nil, monoerrors.WrapServiceError(name, entry.ModuleName, entry.Type,
			fmt.Errorf("service is not a RequestReply service (type: %s)", types.FormatServiceType(entry.Type)))
	}

	// Validate EventBus is available
	if c.eventBus == nil {
		return nil, monoerrors.WrapServiceError(name, entry.ModuleName, entry.Type,
			fmt.Errorf("EventBus not available (required for RequestReply services)"))
	}

	return &requestReplyClient{
		subject:         entry.Subject,
		eventBus:        c.eventBus,
		timeout:         30 * time.Second, // Default timeout
		middlewareChain: c.middlewareChain,
		serviceName:     name,
		moduleName:      entry.ModuleName,
	}, nil
}

// requestReplyClient implements types.RequestReplyServiceClient
type requestReplyClient struct {
	subject         string
	eventBus        types.EventBus
	timeout         time.Duration
	middlewareChain types.MiddlewareChainRunner
	serviceName     string
	moduleName      string
}

// ensureContextDeadline ensures the context has a deadline, applying the default timeout if needed.
// Returns an error if the deadline is already expired.
func (c *requestReplyClient) ensureContextDeadline(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if deadline, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel := context.WithTimeout(ctx, c.timeout)
		return ctx, cancel, nil
	} else {
		if time.Until(deadline) <= 0 {
			return ctx, nil, monoerrors.WrapTimeout("request-reply", 0, context.DeadlineExceeded)
		}
		return ctx, nil, nil
	}
}

// runMiddleware runs the message through the middleware chain, allowing header injection.
// Returns the potentially modified message.
func (c *requestReplyClient) runMiddleware(ctx context.Context, msg *types.Msg) *types.Msg {
	if c.middlewareChain == nil {
		return msg
	}

	octx := types.OutgoingMessageContext{
		ServiceType: types.ServiceTypeRequestReply,
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

// Call sends a request payload and waits for a response.
//
// The context can be used to enforce timeouts or cancellation.
// If the context has no deadline, the client's default timeout is applied.
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	response, err := client.Call(ctx, []byte("request data"))
func (c *requestReplyClient) Call(ctx context.Context, data []byte) (*types.Msg, error) {
	ctx, cancel, err := c.ensureContextDeadline(ctx)
	if err != nil {
		return nil, err
	}
	if cancel != nil {
		defer cancel()
	}

	// Build message to send
	msg := &types.Msg{
		Subject: c.subject,
		Data:    data,
		Header:  make(types.Header),
	}

	// Run through middleware chain to allow header injection
	msg = c.runMiddleware(ctx, msg)

	// Make request using EventBus with context and headers
	response, err := c.eventBus.RequestMsgWithContext(ctx, msg)
	if err != nil {
		// Wrap context deadline exceeded errors with timeout error
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, monoerrors.WrapTimeout("request-reply", c.timeout, err)
		}
		return nil, err
	}

	return response, nil
}

// CallMsg sends a raw request message and waits for a response.
//
// This allows sending messages with custom headers and metadata.
// Note: The input message's Subject field is ignored; the client always
// uses the service's registered subject. The input message is not modified.
// If the context has no deadline, the client's default timeout is applied.
//
// Example:
//
//	msg := &types.Msg{
//	    Data: []byte("product-123"),
//	    Header: map[string][]string{"User-ID": {"user-456"}},
//	}
//	response, err := client.CallMsg(ctx, msg)
func (c *requestReplyClient) CallMsg(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
	ctx, cancel, err := c.ensureContextDeadline(ctx)
	if err != nil {
		return nil, err
	}
	if cancel != nil {
		defer cancel()
	}

	// Build full message with service subject and input headers
	fullMsg := &types.Msg{
		Subject: c.subject, // Always use service subject
		Data:    msg.Data,
		Header:  msg.Header, // Copy headers from input
	}

	// Ensure Header is not nil
	if fullMsg.Header == nil {
		fullMsg.Header = make(types.Header)
	}

	// Run through middleware chain to allow header injection
	fullMsg = c.runMiddleware(ctx, fullMsg)

	// Make request using EventBus with context and headers
	response, err := c.eventBus.RequestMsgWithContext(ctx, fullMsg)
	if err != nil {
		// Wrap context deadline exceeded errors with timeout error
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, monoerrors.WrapTimeout("request-reply", c.timeout, err)
		}
		return nil, err
	}

	return response, nil
}
