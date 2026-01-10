package container

import (
	"context"
	"fmt"
	"strings"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// Default values for StreamConsumerConfig
const (
	defaultBatchSize    = 10
	defaultFetchTimeout = 5 * time.Second
	defaultAckWait      = 30 * time.Second
	defaultMaxDeliver   = 3
)

// RegisterStreamConsumerService registers a JetStream durable pull consumer service.
// The service will automatically create/update the stream and consumer on startup.
func (c *serviceContainer) RegisterStreamConsumerService(
	name string,
	config types.StreamConsumerConfig,
	handler types.StreamConsumerHandler,
) error {
	// Validate required fields
	if config.Stream.Name == "" {
		return fmt.Errorf("stream name is required for stream consumer service '%s'", name)
	}
	if handler == nil {
		return fmt.Errorf("handler is required for stream consumer service '%s'", name)
	}

	// Apply defaults for Fetch config
	if config.Fetch.BatchSize <= 0 {
		config.Fetch.BatchSize = defaultBatchSize
	}
	if config.Fetch.Timeout <= 0 {
		config.Fetch.Timeout = defaultFetchTimeout
	}

	// Apply defaults for Consumer config
	if config.Consumer.AckWait <= 0 {
		config.Consumer.AckWait = defaultAckWait
	}
	if config.Consumer.MaxDeliver <= 0 {
		config.Consumer.MaxDeliver = defaultMaxDeliver
	}

	// Handle Stream.Subjects: if empty, add default subject
	computedServiceName, err := c.computeServiceSubject(name)
	if err != nil {
		return err
	}
	if len(config.Stream.Subjects) == 0 {
		// Default subjects: concrete + wildcard pattern
		// - Concrete subject ensures consistency with other service types (RequestReply, QueueGroup)
		// - Wildcard pattern allows sub-topic publishing (e.g., services.<module>.<service>.priority.high)
		config.Stream.Subjects = []string{
			computedServiceName,                      // services.<module>.<service>
			fmt.Sprintf("%s.>", computedServiceName), // services.<module>.<service>.>
		}
	}

	// Derive client publish subject from first subject in Stream.Subjects
	// For default subjects, the first entry is the concrete subject (services.<module>.<service>)
	// which matches other service types (RequestReply, QueueGroup)
	// Note: Stream.Subjects is guaranteed non-empty at this point (either user-provided or defaulted above)
	subject := derivePublishSubject(config.Stream.Subjects[0])

	// Create service entry
	entry := &types.ServiceEntry{
		Type:                  types.ServiceTypeStreamConsumer,
		StreamConsumerConfig:  &config,
		StreamConsumerHandler: handler,
		Subject:               subject, // Subject for client publishing
	}

	// Use common registration helper
	return c.registerService(name, entry)
}

// derivePublishSubject derives a concrete publish subject from a potentially wildcard subject.
// If the subject contains wildcards (* or >), it replaces the wildcard segment with "default".
// If the subject is concrete (no wildcards), it returns it as-is.
//
// Examples:
//   - "orders.new" -> "orders.new" (no wildcard, use as-is)
//   - "orders.*" -> "orders.default" (single wildcard replaced)
//   - "orders.>" -> "orders.default" (multi-wildcard replaced)
//   - "services.payment.>" -> "services.payment.default"
func derivePublishSubject(subject string) string {
	// Check if subject contains wildcards
	if !strings.Contains(subject, "*") && !strings.Contains(subject, ">") {
		return subject
	}

	// Split subject into parts
	parts := strings.Split(subject, ".")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		if part == "*" || part == ">" {
			// Replace wildcard with "default"
			result = append(result, "default")
			// For ">", this is the last segment
			if part == ">" {
				break
			}
		} else {
			result = append(result, part)
		}
	}

	return strings.Join(result, ".")
}

// GetStreamConsumerService retrieves a stream consumer service client.
//
// Returns a client that can be used to publish messages to the stream.
// The messages will be persisted in JetStream and delivered to the consumer.
// Returns ErrServiceNotFound if the service is not registered.
// Returns an error if the service type is not StreamConsumer.
//
// Example:
//
//	client, err := container.GetStreamConsumerService("order-processor")
//	if err != nil {
//	    return err
//	}
//	ack, err := client.Publish(ctx, []byte("order-data"))
func (c *serviceContainer) GetStreamConsumerService(name string) (types.StreamConsumerServiceClient, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.services[name]
	if !exists {
		if c.boundModule != nil {
			return nil, monoerrors.WrapServiceNotFound(name, c.boundModule.Name())
		}
		return nil, monoerrors.WrapServiceNotFound(name, "<unbound>")
	}

	if entry.Type != types.ServiceTypeStreamConsumer {
		return nil, monoerrors.WrapServiceError(name, entry.ModuleName, entry.Type,
			fmt.Errorf("service is not a StreamConsumer service (type: %s)", types.FormatServiceType(entry.Type)))
	}

	// Validate EventBus is available
	if c.eventBus == nil {
		return nil, monoerrors.WrapServiceError(name, entry.ModuleName, entry.Type,
			fmt.Errorf("EventBus not available (required for StreamConsumer services)"))
	}

	// Retrieve JetStream lazily from EventBus
	es, err := c.eventBus.EventStream()
	if err != nil {
		return nil, monoerrors.WrapServiceError(name, entry.ModuleName, entry.Type,
			fmt.Errorf("failed to get JetStream from EventBus: %w", err))
	}

	return &streamConsumerClient{
		subject:         entry.Subject,
		eventPublisher:  es,
		middlewareChain: c.middlewareChain,
		serviceName:     name,
		moduleName:      entry.ModuleName,
	}, nil
}

// streamConsumerClient implements types.StreamConsumerServiceClient
type streamConsumerClient struct {
	subject         string
	eventPublisher  types.EventStream
	middlewareChain types.MiddlewareChainRunner
	serviceName     string
	moduleName      string
}

// runMiddleware runs the message through the middleware chain, allowing header injection.
// Returns the potentially modified message.
func (c *streamConsumerClient) runMiddleware(ctx context.Context, msg *types.Msg) *types.Msg {
	if c.middlewareChain == nil {
		return msg
	}

	octx := types.OutgoingMessageContext{
		ServiceType: types.ServiceTypeStreamConsumer,
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

// Publish publishes a message to the stream (with JetStream persistence).
//
// This publishes to the stream's subject. The message is persisted in JetStream
// and will be delivered to the stream consumer handler.
//
// Returns a PubAck with stream and sequence information on success.
//
// Example:
//
//	ack, err := client.Publish(ctx, []byte("order-data"))
//	if err != nil {
//	    // Handle publish error
//	}
//	log.Printf("Published to stream %s, sequence %d", ack.Stream(), ack.Sequence())
func (c *streamConsumerClient) Publish(ctx context.Context, data []byte) (types.MsgPubAck, error) {
	// Check if context is already cancelled
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context error before publish: %w", err)
	}

	// Build message with headers
	msg := &types.Msg{
		Subject: c.subject,
		Data:    data,
		Header:  make(types.Header),
	}

	// Run through middleware chain to allow header injection
	msg = c.runMiddleware(ctx, msg)

	// Publish using EventStream publisher
	ack, err := c.eventPublisher.PublishMsg(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish to JetStream: %w", err)
	}

	return ack, nil
}

// PublishMsg publishes a complete message to the stream.
//
// Subject Handling:
//   - If msg.Subject is empty, it will be set to the service subject
//   - If msg.Subject is already set, it will be used as-is
//   - Headers from msg.Header are included in the published message
//
// Returns a PubAck with stream and sequence information on success.
//
// Example:
//
//	msg := &types.Msg{
//	    Data:   []byte("order-data"),
//	    Header: map[string][]string{"Priority": {"high"}},
//	}
//	ack, err := client.PublishMsg(ctx, msg)
func (c *streamConsumerClient) PublishMsg(ctx context.Context, msg *types.Msg) (types.MsgPubAck, error) {
	// Check if context is already cancelled
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context error before publish: %w", err)
	}

	// If subject not set, use service subject
	if msg.Subject == "" {
		msg.Subject = c.subject
	}

	// Ensure Header is not nil
	if msg.Header == nil {
		msg.Header = make(types.Header)
	}

	// Run through middleware chain to allow header injection
	msg = c.runMiddleware(ctx, msg)

	// Publish using EventStream publisher
	ack, err := c.eventPublisher.PublishMsg(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish to JetStream: %w", err)
	}

	return ack, nil
}
