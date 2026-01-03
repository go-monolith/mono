// Package eventbus provides NATS-backed event bus implementation for publish-subscribe
// messaging patterns with support for JetStream persistence.
package eventbus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
)

// natsEventBus implements types.EventBus using NATS.
type natsEventBus struct {
	mu         sync.RWMutex
	conn       *nats.Conn
	logger     types.Logger
	runtimeCtx context.Context
	js         *NatsJetStream // cached JetStream instance
}

// Compile-time interface check ensuring natsEventBus implements EventBusWithConn.
// If this fails to compile, the implementation is missing the Conn() method.
var _ types.EventBusWithConn[*nats.Conn] = (*natsEventBus)(nil)

// NewEventBus creates a new NATS-backed event bus.
func NewEventBus(conn *nats.Conn, logger types.Logger) (types.EventBus, error) {
	if conn == nil {
		return nil, fmt.Errorf("NATS connection cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	return &natsEventBus{
		conn:   conn,
		logger: logger,
	}, nil
}

// Publish publishes data to a subject (fire-and-forget).
func (eb *natsEventBus) Publish(subject string, data []byte) error {
	if err := eb.conn.Publish(subject, data); err != nil {
		return fmt.Errorf("failed to publish to subject %q: %w", subject, err)
	}
	return nil
}

// PublishMsg publishes a complete message with headers.
func (eb *natsEventBus) PublishMsg(msg *types.Msg) error {
	natsMsg := &nats.Msg{
		Subject: msg.Subject,
		Reply:   msg.Reply,
		Data:    msg.Data,
		Header:  nats.Header(msg.Header),
	}

	if err := eb.conn.PublishMsg(natsMsg); err != nil {
		return fmt.Errorf("failed to publish message to subject %q: %w", msg.Subject, err)
	}
	return nil
}

// Request sends a request and waits for a single reply.
func (eb *natsEventBus) Request(subject string, data []byte, timeout time.Duration) (*types.Msg, error) {
	natsMsg, err := eb.conn.Request(subject, data, timeout)
	if err != nil {
		// Handle no responders error specifically
		if errors.Is(err, nats.ErrNoResponders) {
			if moduleName, serviceName, ok := parseServiceSubject(subject); ok {
				return nil, monoerrors.WrapServiceUnavailable(serviceName, moduleName, types.ServiceTypeRequestReply, err)
			}
			// Fallback if subject doesn't match expected format
			return nil, fmt.Errorf("%w: request to subject %q failed: %w", monoerrors.ErrServiceUnavailable, subject, err)
		}
		return nil, fmt.Errorf("request to subject %q failed: %w", subject, err)
	}

	return &types.Msg{
		Subject: natsMsg.Subject,
		Reply:   natsMsg.Reply,
		Data:    natsMsg.Data,
		Header:  types.Header(natsMsg.Header),
	}, nil
}

// RequestWithContext sends a request and waits for a single reply with context support.
// The context controls cancellation and deadline.
func (eb *natsEventBus) RequestWithContext(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
	natsMsg, err := eb.conn.RequestWithContext(ctx, subject, data)
	if err != nil {
		// Handle no responders error specifically
		if errors.Is(err, nats.ErrNoResponders) {
			if moduleName, serviceName, ok := parseServiceSubject(subject); ok {
				return nil, monoerrors.WrapServiceUnavailable(serviceName, moduleName, types.ServiceTypeRequestReply, err)
			}
			// Fallback if subject doesn't match expected format
			return nil, fmt.Errorf("%w: request to subject %q failed: %w", monoerrors.ErrServiceUnavailable, subject, err)
		}
		// Return context errors as-is for proper error handling upstream
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("request to subject %q failed: %w", subject, err)
	}

	return &types.Msg{
		Subject: natsMsg.Subject,
		Reply:   natsMsg.Reply,
		Data:    natsMsg.Data,
		Header:  types.Header(natsMsg.Header),
	}, nil
}

// RequestMsgWithContext sends a complete message with headers and waits for a single reply.
func (eb *natsEventBus) RequestMsgWithContext(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
	// Convert types.Msg to nats.Msg
	natsMsg := &nats.Msg{
		Subject: msg.Subject,
		Data:    msg.Data,
		Header:  nats.Header(msg.Header),
	}
	// Note: msg.Reply is ignored - NATS generates the reply inbox automatically

	// Send request and wait for response
	response, err := eb.conn.RequestMsgWithContext(ctx, natsMsg)
	if err != nil {
		// Handle no responders error specifically
		if errors.Is(err, nats.ErrNoResponders) {
			if moduleName, serviceName, ok := parseServiceSubject(msg.Subject); ok {
				return nil, monoerrors.WrapServiceUnavailable(serviceName, moduleName, types.ServiceTypeRequestReply, err)
			}
			// Fallback if subject doesn't match expected format
			return nil, fmt.Errorf("%w: request to subject %q failed: %w", monoerrors.ErrServiceUnavailable, msg.Subject, err)
		}
		// Return context errors as-is for proper error handling upstream
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("request to subject %q failed: %w", msg.Subject, err)
	}

	return &types.Msg{
		Subject: response.Subject,
		Reply:   response.Reply,
		Data:    response.Data,
		Header:  types.Header(response.Header),
	}, nil
}

// parseServiceSubject extracts module and service names from subject.
// Subject format: "services.<module>.<service>"
// Returns ok=false if subject doesn't match the exact 3-part format.
func parseServiceSubject(subject string) (moduleName, serviceName string, ok bool) {
	parts := strings.Split(subject, ".")
	if len(parts) == 3 && parts[0] == "services" {
		return parts[1], parts[2], true
	}
	return "", "", false
}

// Subscribe creates an asynchronous subscription with a message handler.
func (eb *natsEventBus) Subscribe(subject string, handler types.MsgHandler) (types.Subscription, error) {
	natsSub, err := eb.conn.Subscribe(subject, func(natsMsg *nats.Msg) {
		msg := &types.Msg{
			Subject: natsMsg.Subject,
			Reply:   natsMsg.Reply,
			Data:    natsMsg.Data,
			Header:  types.Header(natsMsg.Header),
		}
		// Call handler with runtime context for graceful shutdown support
		ctx := eb.getRuntimeCtx()
		handler(ctx, msg)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to subject %q: %w", subject, err)
	}

	return wrapSubscription(natsSub), nil
}

// SubscribeSync creates a synchronous subscription.
func (eb *natsEventBus) SubscribeSync(subject string) (types.Subscription, error) {
	natsSub, err := eb.conn.SubscribeSync(subject)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync subscription to subject %q: %w", subject, err)
	}

	return wrapSubscription(natsSub), nil
}

// QueueSubscribe creates a queue group subscription for load balancing.
func (eb *natsEventBus) QueueSubscribe(subject, queue string, handler types.MsgHandler) (types.Subscription, error) {
	natsSub, err := eb.conn.QueueSubscribe(subject, queue, func(natsMsg *nats.Msg) {
		msg := &types.Msg{
			Subject: natsMsg.Subject,
			Reply:   natsMsg.Reply,
			Data:    natsMsg.Data,
			Header:  types.Header(natsMsg.Header),
		}
		// Call handler with runtime context for graceful shutdown support
		ctx := eb.getRuntimeCtx()
		handler(ctx, msg)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create queue subscription to subject %q: %w", subject, err)
	}

	return wrapSubscription(natsSub), nil
}

// QueueSubscribeSync creates a synchronous queue group subscription.
func (eb *natsEventBus) QueueSubscribeSync(subject, queue string) (types.Subscription, error) {
	natsSub, err := eb.conn.QueueSubscribeSync(subject, queue)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync queue subscription to subject %q: %w", subject, err)
	}

	return wrapSubscription(natsSub), nil
}

// ChanSubscribe creates a channel-based subscription.
func (eb *natsEventBus) ChanSubscribe(subject string, ch chan *types.Msg) (types.Subscription, error) {
	natsCh := make(chan *nats.Msg, cap(ch))

	natsSub, err := eb.conn.ChanSubscribe(subject, natsCh)
	if err != nil {
		return nil, fmt.Errorf("failed to create channel subscription to subject %q: %w", subject, err)
	}

	// Forward messages from NATS channel to mono channel
	go func() {
		for natsMsg := range natsCh {
			ch <- &types.Msg{
				Subject: natsMsg.Subject,
				Reply:   natsMsg.Reply,
				Data:    natsMsg.Data,
				Header:  types.Header(natsMsg.Header),
			}
		}
		close(ch)
	}()

	return wrapSubscription(natsSub), nil
}

// EventStream returns a JetStream context that is compatible with EventStream
// interface for durable/persistent subscriptions.
func (eb *natsEventBus) EventStream() (types.EventStream, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.js == nil {
		var err error
		eb.js, err = NewJetStream(eb.conn, eb.logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create JetStream when lazy load EventBus.EventStream: %w", err)
		}
	}

	return eb.js, nil
}

// SetRuntimeContext sets the context that will be passed to all message handlers.
// When this context is cancelled, handlers can detect shutdown and terminate gracefully.
func (eb *natsEventBus) SetRuntimeContext(ctx context.Context) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.runtimeCtx = ctx
}

// getRuntimeCtx returns the runtime context if set, otherwise returns context.Background().
func (eb *natsEventBus) getRuntimeCtx() context.Context {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	if eb.runtimeCtx != nil {
		return eb.runtimeCtx
	}
	return context.Background()
}

// wrapSubscription wraps a NATS subscription into a types.Subscription.
func wrapSubscription(natsSub *nats.Subscription) types.Subscription {
	return &monoSubscription{
		natsSub: natsSub,
	}
}

// monoSubscription wraps nats.Subscription to implement types.Subscription.
type monoSubscription struct {
	natsSub *nats.Subscription
}

// Unsubscribe removes interest in the subscription.
func (s *monoSubscription) Unsubscribe() error {
	return s.natsSub.Unsubscribe()
}

// Drain removes interest but processes pending messages before completion.
func (s *monoSubscription) Drain() error {
	return s.natsSub.Drain()
}

// IsValid returns false if subscription has been unsubscribed.
func (s *monoSubscription) IsValid() bool {
	return s.natsSub.IsValid()
}

// Subject returns the subject pattern.
func (s *monoSubscription) Subject() string {
	return s.natsSub.Subject
}

// Queue returns the queue group name (empty string for non-queue subscriptions).
func (s *monoSubscription) Queue() string {
	return s.natsSub.Queue
}

// NextMsg fetches the next message (for sync subscriptions).
func (s *monoSubscription) NextMsg(timeout time.Duration) (*types.Msg, error) {
	natsMsg, err := s.natsSub.NextMsg(timeout)
	if err != nil {
		return nil, err
	}

	return &types.Msg{
		Subject: natsMsg.Subject,
		Reply:   natsMsg.Reply,
		Data:    natsMsg.Data,
		Header:  types.Header(natsMsg.Header),
	}, nil
}

// NextMsgWithContext fetches next message with context cancellation.
func (s *monoSubscription) NextMsgWithContext(ctx context.Context) (*types.Msg, error) {
	natsMsg, err := s.natsSub.NextMsgWithContext(ctx)
	if err != nil {
		return nil, err
	}

	return &types.Msg{
		Subject: natsMsg.Subject,
		Reply:   natsMsg.Reply,
		Data:    natsMsg.Data,
		Header:  types.Header(natsMsg.Header),
	}, nil
}

// Conn returns the underlying NATS connection.
// This method satisfies the types.EventBusWithConn[*nats.Conn] interface.
func (eb *natsEventBus) Conn() *nats.Conn {
	return eb.conn
}
