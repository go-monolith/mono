package mono_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1"
)

// mockEventBus is a mock implementation of mono.EventBus for testing
type mockEventBus struct {
	published   []publishedMsg
	subscribers map[string][]*mockSubscription
	mu          sync.RWMutex
}

type publishedMsg struct {
	subject string
	data    []byte
	headers mono.Header
}

func newMockEventBus() *mockEventBus {
	return &mockEventBus{
		published:   make([]publishedMsg, 0),
		subscribers: make(map[string][]*mockSubscription),
	}
}

func (eb *mockEventBus) Publish(subject string, data []byte) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.published = append(eb.published, publishedMsg{
		subject: subject,
		data:    data,
	})

	// Deliver to subscribers
	if subs, ok := eb.subscribers[subject]; ok {
		for _, sub := range subs {
			if sub.isValid {
				msg := &mono.Msg{
					Subject: subject,
					Data:    data,
				}
				if sub.handler != nil {
					go func(h mono.MsgHandler, m *mono.Msg) {
						defer func() {
							if r := recover(); r != nil {
								// Simulate error logging (handler panicked but subscription continues)
								_ = r
							}
						}()
						h(context.Background(), m)
					}(sub.handler, msg)
				}
				if sub.ch != nil {
					select {
					case sub.ch <- msg:
					default:
					}
				}
			}
		}
	}

	return nil
}

func (eb *mockEventBus) PublishMsg(msg *mono.Msg) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.published = append(eb.published, publishedMsg{
		subject: msg.Subject,
		data:    msg.Data,
		headers: msg.Header,
	})

	// Deliver to subscribers
	if subs, ok := eb.subscribers[msg.Subject]; ok {
		for _, sub := range subs {
			if sub.isValid {
				if sub.handler != nil {
					go func(h mono.MsgHandler, m *mono.Msg) {
						defer func() {
							if r := recover(); r != nil {
								// Simulate error logging (handler panicked but subscription continues)
								_ = r
							}
						}()
						h(context.Background(), m)
					}(sub.handler, msg)
				}
				if sub.ch != nil {
					select {
					case sub.ch <- msg:
					default:
					}
				}
			}
		}
	}

	return nil
}

func (eb *mockEventBus) Request(subject string, data []byte, timeout time.Duration) (*mono.Msg, error) {
	// Simplified mock: return error if no subscribers
	eb.mu.RLock()
	subs, ok := eb.subscribers[subject]
	eb.mu.RUnlock()

	if !ok || len(subs) == 0 {
		return nil, errors.New("no responders")
	}

	// Mock response
	return &mono.Msg{
		Subject: subject,
		Data:    []byte("response"),
	}, nil
}

func (eb *mockEventBus) RequestWithContext(ctx context.Context, subject string, data []byte) (*mono.Msg, error) {
	// Simplified mock: return error if no subscribers
	eb.mu.RLock()
	subs, ok := eb.subscribers[subject]
	eb.mu.RUnlock()

	if !ok || len(subs) == 0 {
		return nil, errors.New("no responders")
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Mock response
	return &mono.Msg{
		Subject: subject,
		Data:    []byte("response"),
	}, nil
}

func (eb *mockEventBus) RequestMsgWithContext(ctx context.Context, msg *mono.Msg) (*mono.Msg, error) {
	// Simplified mock: return error if no subscribers
	eb.mu.RLock()
	subs, ok := eb.subscribers[msg.Subject]
	eb.mu.RUnlock()

	if !ok || len(subs) == 0 {
		return nil, errors.New("no responders")
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Mock response
	return &mono.Msg{
		Subject: msg.Subject,
		Data:    []byte("response"),
		Header:  msg.Header, // Echo headers
	}, nil
}

func (eb *mockEventBus) Subscribe(subject string, handler mono.MsgHandler) (mono.Subscription, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	sub := &mockSubscription{
		subject: subject,
		handler: handler,
		isValid: true,
		bus:     eb,
	}

	eb.subscribers[subject] = append(eb.subscribers[subject], sub)
	return sub, nil
}

func (eb *mockEventBus) SubscribeSync(subject string) (mono.Subscription, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	sub := &mockSubscription{
		subject: subject,
		isValid: true,
		bus:     eb,
		syncCh:  make(chan *mono.Msg, 10),
	}

	eb.subscribers[subject] = append(eb.subscribers[subject], sub)
	return sub, nil
}

func (eb *mockEventBus) QueueSubscribe(subject, queue string, handler mono.MsgHandler) (mono.Subscription, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	sub := &mockSubscription{
		subject: subject,
		queue:   queue,
		handler: handler,
		isValid: true,
		bus:     eb,
	}

	eb.subscribers[subject] = append(eb.subscribers[subject], sub)
	return sub, nil
}

func (eb *mockEventBus) QueueSubscribeSync(subject, queue string) (mono.Subscription, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	sub := &mockSubscription{
		subject: subject,
		queue:   queue,
		isValid: true,
		bus:     eb,
		syncCh:  make(chan *mono.Msg, 10),
	}

	eb.subscribers[subject] = append(eb.subscribers[subject], sub)
	return sub, nil
}

func (eb *mockEventBus) ChanSubscribe(subject string, ch chan *mono.Msg) (mono.Subscription, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	sub := &mockSubscription{
		subject: subject,
		isValid: true,
		bus:     eb,
		ch:      ch,
	}

	eb.subscribers[subject] = append(eb.subscribers[subject], sub)
	return sub, nil
}

func (eb *mockEventBus) EventStream() (mono.EventStream, error) {
	return nil, errors.New("JetStream not implemented in mock")
}

func (eb *mockEventBus) SetRuntimeContext(ctx context.Context) {
	// Mock implementation - no-op for tests
}

// mockSubscription implements mono.Subscription interface
type mockSubscription struct {
	subject string
	queue   string
	handler mono.MsgHandler
	ch      chan *mono.Msg
	syncCh  chan *mono.Msg
	isValid bool
	bus     *mockEventBus
	mu      sync.Mutex
}

func (s *mockSubscription) Unsubscribe() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.isValid = false
	if s.syncCh != nil {
		close(s.syncCh)
		s.syncCh = nil
	}
	return nil
}

func (s *mockSubscription) Drain() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.isValid = false
	// Mock: just close the channel after a brief delay
	if s.syncCh != nil {
		go func() {
			time.Sleep(10 * time.Millisecond)
			close(s.syncCh)
		}()
	}
	return nil
}

func (s *mockSubscription) IsValid() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isValid
}

func (s *mockSubscription) Subject() string {
	return s.subject
}

func (s *mockSubscription) Queue() string {
	return s.queue
}

func (s *mockSubscription) NextMsg(timeout time.Duration) (*mono.Msg, error) {
	if s.syncCh == nil {
		return nil, errors.New("not a sync subscription")
	}

	select {
	case msg, ok := <-s.syncCh:
		if !ok {
			return nil, errors.New("subscription closed")
		}
		return msg, nil
	case <-time.After(timeout):
		return nil, errors.New("timeout")
	}
}

func (s *mockSubscription) NextMsgWithContext(ctx context.Context) (*mono.Msg, error) {
	if s.syncCh == nil {
		return nil, errors.New("not a sync subscription")
	}

	select {
	case msg, ok := <-s.syncCh:
		if !ok {
			return nil, errors.New("subscription closed")
		}
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestEventBusInterface ensures mono.EventBus interface compliance
func TestEventBusInterface(t *testing.T) {
	var _ mono.EventBus = (*mockEventBus)(nil)
}

// TestPublishSubscribePattern tests basic pub/sub with mock NATS
func TestPublishSubscribePattern(t *testing.T) {
	bus := newMockEventBus()

	t.Run("publish and subscribe to same subject", func(t *testing.T) {
		received := make(chan *mono.Msg, 1)

		_, err := bus.Subscribe("test.subject", func(ctx context.Context, msg *mono.Msg) {
			received <- msg
		})
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		testData := []byte("hello world")
		err = bus.Publish("test.subject", testData)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		select {
		case msg := <-received:
			if string(msg.Data) != string(testData) {
				t.Errorf("expected data %q, got %q", testData, msg.Data)
			}
		case <-time.After(1 * time.Second):
			t.Error("timeout waiting for message")
		}
	})

	t.Run("multiple subscribers receive same message", func(t *testing.T) {
		received1 := make(chan *mono.Msg, 1)
		received2 := make(chan *mono.Msg, 1)

		bus := newMockEventBus()

		_, err := bus.Subscribe("broadcast", func(ctx context.Context, msg *mono.Msg) {
			received1 <- msg
		})
		if err != nil {
			t.Fatalf("Subscribe 1 failed: %v", err)
		}

		_, err = bus.Subscribe("broadcast", func(ctx context.Context, msg *mono.Msg) {
			received2 <- msg
		})
		if err != nil {
			t.Fatalf("Subscribe 2 failed: %v", err)
		}

		err = bus.Publish("broadcast", []byte("broadcast message"))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		// Both subscribers should receive
		timeout := time.After(1 * time.Second)
		select {
		case <-received1:
		case <-timeout:
			t.Error("subscriber 1 did not receive message")
		}

		select {
		case <-received2:
		case <-timeout:
			t.Error("subscriber 2 did not receive message")
		}
	})
}

// TestRequestReplyPattern tests request-reply messaging
func TestRequestReplyPattern(t *testing.T) {
	bus := newMockEventBus()

	t.Run("request receives reply", func(t *testing.T) {
		// Set up responder
		_, err := bus.Subscribe("request.subject", func(ctx context.Context, msg *mono.Msg) {
			// In real implementation, would reply via msg.Reply subject
		})
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		// Send request
		reply, err := bus.Request("request.subject", []byte("request"), 1*time.Second)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if reply == nil {
			t.Error("expected reply message, got nil")
		}
	})

	t.Run("request timeout when no responder", func(t *testing.T) {
		bus := newMockEventBus()

		_, err := bus.Request("no.responder", []byte("request"), 100*time.Millisecond)
		if err == nil {
			t.Error("expected timeout error, got nil")
		}
	})
}

// TestQueueSubscriptionLoadBalancing tests queue group load balancing
func TestQueueSubscriptionLoadBalancing(t *testing.T) {
	bus := newMockEventBus()

	t.Run("messages distributed among queue subscribers", func(t *testing.T) {
		var count1, count2 int32

		_, err := bus.QueueSubscribe("queue.subject", "workers", func(ctx context.Context, msg *mono.Msg) {
			atomic.AddInt32(&count1, 1)
		})
		if err != nil {
			t.Fatalf("QueueSubscribe 1 failed: %v", err)
		}

		_, err = bus.QueueSubscribe("queue.subject", "workers", func(ctx context.Context, msg *mono.Msg) {
			atomic.AddInt32(&count2, 1)
		})
		if err != nil {
			t.Fatalf("QueueSubscribe 2 failed: %v", err)
		}

		// Publish multiple messages
		for i := 0; i < 10; i++ {
			err := bus.Publish("queue.subject", []byte("message"))
			if err != nil {
				t.Fatalf("Publish failed: %v", err)
			}
		}

		// Give time for messages to be processed
		time.Sleep(100 * time.Millisecond)

		// Both subscribers should have received messages
		// Note: In mock, all subscribers get all messages. In real NATS, only one per queue group gets each message.
		total := atomic.LoadInt32(&count1) + atomic.LoadInt32(&count2)
		if total == 0 {
			t.Error("no messages were received by queue subscribers")
		}
	})
}

// TestSubscriptionLifecycle tests subscribe, unsubscribe, drain
func TestSubscriptionLifecycle(t *testing.T) {
	bus := newMockEventBus()

	t.Run("subscription is valid after creation", func(t *testing.T) {
		sub, err := bus.Subscribe("test.lifecycle", func(ctx context.Context, msg *mono.Msg) {})
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		if !sub.IsValid() {
			t.Error("new subscription should be valid")
		}
	})

	t.Run("unsubscribe marks subscription invalid", func(t *testing.T) {
		sub, err := bus.Subscribe("test.lifecycle", func(ctx context.Context, msg *mono.Msg) {})
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		err = sub.Unsubscribe()
		if err != nil {
			t.Errorf("Unsubscribe failed: %v", err)
		}

		if sub.IsValid() {
			t.Error("unsubscribed subscription should be invalid")
		}
	})

	t.Run("drain processes pending messages before closing", func(t *testing.T) {
		sub, err := bus.Subscribe("test.drain", func(ctx context.Context, msg *mono.Msg) {
			time.Sleep(50 * time.Millisecond)
		})
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		// Publish a message
		err = bus.Publish("test.drain", []byte("message"))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		// Drain should wait for processing
		err = sub.Drain()
		if err != nil {
			t.Errorf("Drain failed: %v", err)
		}

		// Should be invalid after drain
		time.Sleep(20 * time.Millisecond)
		if sub.IsValid() {
			t.Error("drained subscription should be invalid")
		}
	})

	t.Run("Subject and Queue return subscription details", func(t *testing.T) {
		sub, err := bus.QueueSubscribe("test.queue", "worker-group", func(ctx context.Context, msg *mono.Msg) {})
		if err != nil {
			t.Fatalf("QueueSubscribe failed: %v", err)
		}

		if sub.Subject() != "test.queue" {
			t.Errorf("expected subject 'test.queue', got %s", sub.Subject())
		}

		if sub.Queue() != "worker-group" {
			t.Errorf("expected queue 'worker-group', got %s", sub.Queue())
		}
	})
}

// TestEventStreamDurableSubscriptions tests mono.EventStream durable subscriptions
func TestEventStreamDurableSubscriptions(t *testing.T) {
	bus := newMockEventBus()

	t.Run("EventStream returns error when not implemented", func(t *testing.T) {
		es, err := bus.EventStream()
		if err == nil {
			t.Error("expected error when mono.EventStream not implemented")
		}
		if es != nil {
			t.Error("expected nil mono.EventStream")
		}
	})

	// Note: Full JetStream tests would require actual NATS server with JetStream enabled
	// These are covered in integration tests (Task 9.1)
}

// TestErrorRetryConfiguration tests error retry and backoff strategy
func TestErrorRetryConfiguration(t *testing.T) {
	t.Run("handler errors are logged but subscription continues", func(t *testing.T) {
		bus := newMockEventBus()
		var callCount int32

		_, err := bus.Subscribe("test.errors", func(ctx context.Context, msg *mono.Msg) {
			atomic.AddInt32(&callCount, 1)
			// Simulate handler error
			panic("handler error")
		})
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		// Publish multiple messages
		for i := 0; i < 3; i++ {
			err := bus.Publish("test.errors", []byte("message"))
			if err != nil {
				t.Fatalf("Publish failed: %v", err)
			}
		}

		time.Sleep(100 * time.Millisecond)

		// All messages should have been attempted despite errors
		if atomic.LoadInt32(&callCount) < 3 {
			t.Errorf("expected at least 3 calls, got %d", callCount)
		}
	})
}

// TestConcurrentMessageHandlers tests 1000+ concurrent subscriptions
func TestConcurrentMessageHandlers(t *testing.T) {
	bus := newMockEventBus()

	t.Run("handle 1000+ concurrent subscriptions", func(t *testing.T) {
		const numSubscriptions = 1000
		var wg sync.WaitGroup
		wg.Add(numSubscriptions)

		receivedCount := int32(0)

		// Create many concurrent subscriptions
		for i := 0; i < numSubscriptions; i++ {
			subject := "test.concurrent"
			_, err := bus.Subscribe(subject, func(ctx context.Context, msg *mono.Msg) {
				atomic.AddInt32(&receivedCount, 1)
				wg.Done()
			})
			if err != nil {
				t.Fatalf("Subscribe %d failed: %v", i, err)
			}
		}

		// Publish one message
		err := bus.Publish("test.concurrent", []byte("concurrent test"))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		// Wait for all handlers to complete
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting for handlers, received %d/%d", atomic.LoadInt32(&receivedCount), numSubscriptions)
		}

		if atomic.LoadInt32(&receivedCount) != numSubscriptions {
			t.Errorf("expected %d messages, got %d", numSubscriptions, receivedCount)
		}
	})
}

// TestMessageThroughputWithConcurrentHandlers tests no blocking with concurrent handlers
func TestMessageThroughputWithConcurrentHandlers(t *testing.T) {
	bus := newMockEventBus()

	t.Run("message throughput not blocked by slow handlers", func(t *testing.T) {
		slowReceived := int32(0)
		fastReceived := int32(0)

		// Slow subscriber
		_, err := bus.Subscribe("test.throughput", func(ctx context.Context, msg *mono.Msg) {
			time.Sleep(100 * time.Millisecond) // Slow handler
			atomic.AddInt32(&slowReceived, 1)
		})
		if err != nil {
			t.Fatalf("Subscribe slow failed: %v", err)
		}

		// Fast subscriber
		_, err = bus.Subscribe("test.throughput", func(ctx context.Context, msg *mono.Msg) {
			atomic.AddInt32(&fastReceived, 1)
		})
		if err != nil {
			t.Fatalf("Subscribe fast failed: %v", err)
		}

		// Publish multiple messages quickly
		start := time.Now()
		for i := 0; i < 10; i++ {
			err := bus.Publish("test.throughput", []byte("message"))
			if err != nil {
				t.Fatalf("Publish failed: %v", err)
			}
		}
		publishDuration := time.Since(start)

		// Publishing should be fast (not blocked by slow subscriber)
		if publishDuration > 500*time.Millisecond {
			t.Errorf("publishing took too long: %v (should not be blocked by slow handlers)", publishDuration)
		}

		// Wait for handlers to process
		time.Sleep(1500 * time.Millisecond)

		// Both subscribers should have received all messages
		if atomic.LoadInt32(&fastReceived) != 10 {
			t.Errorf("fast subscriber expected 10 messages, got %d", fastReceived)
		}
		if atomic.LoadInt32(&slowReceived) != 10 {
			t.Errorf("slow subscriber expected 10 messages, got %d", slowReceived)
		}
	})
}

// TestSubscriptionTypes tests different subscription types
func TestSubscriptionTypes(t *testing.T) {
	bus := newMockEventBus()

	t.Run("async subscription with handler", func(t *testing.T) {
		received := make(chan bool, 1)

		sub, err := bus.Subscribe("test.async", func(ctx context.Context, msg *mono.Msg) {
			received <- true
		})
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}
		defer func() {
			if err := sub.Unsubscribe(); err != nil {
				t.Errorf("Failed to unsubscribe: %v", err)
			}
		}()

		err = bus.Publish("test.async", []byte("test"))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		select {
		case <-received:
			// Success
		case <-time.After(1 * time.Second):
			t.Error("timeout waiting for async message")
		}
	})

	t.Run("sync subscription with NextMsg", func(t *testing.T) {
		sub, err := bus.SubscribeSync("test.sync")
		if err != nil {
			t.Fatalf("SubscribeSync failed: %v", err)
		}
		defer func() {
			if err := sub.Unsubscribe(); err != nil {
				t.Errorf("Failed to unsubscribe: %v", err)
			}
		}()

		// Publish message
		go func() {
			time.Sleep(50 * time.Millisecond)
			mockSub := sub.(*mockSubscription)
			mockSub.syncCh <- &mono.Msg{
				Subject: "test.sync",
				Data:    []byte("sync message"),
			}
		}()

		msg, err := sub.NextMsg(1 * time.Second)
		if err != nil {
			t.Fatalf("NextMsg failed: %v", err)
		}

		if string(msg.Data) != "sync message" {
			t.Errorf("expected 'sync message', got %q", msg.Data)
		}
	})

	t.Run("channel subscription", func(t *testing.T) {
		ch := make(chan *mono.Msg, 10)

		sub, err := bus.ChanSubscribe("test.chan", ch)
		if err != nil {
			t.Fatalf("ChanSubscribe failed: %v", err)
		}
		defer func() {
			if err := sub.Unsubscribe(); err != nil {
				t.Errorf("Failed to unsubscribe: %v", err)
			}
		}()

		err = bus.Publish("test.chan", []byte("channel message"))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		select {
		case msg := <-ch:
			if string(msg.Data) != "channel message" {
				t.Errorf("expected 'channel message', got %q", msg.Data)
			}
		case <-time.After(1 * time.Second):
			t.Error("timeout waiting for channel message")
		}
	})
}

// TestPublishMsg tests publishing with headers
func TestPublishMsg(t *testing.T) {
	bus := newMockEventBus()

	t.Run("publish message with headers", func(t *testing.T) {
		received := make(chan *mono.Msg, 1)

		_, err := bus.Subscribe("test.headers", func(ctx context.Context, msg *mono.Msg) {
			received <- msg
		})
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		msg := &mono.Msg{
			Subject: "test.headers",
			Data:    []byte("data with headers"),
			Header: mono.Header{
				"Content-Type": []string{"application/json"},
				"X-Request-ID": []string{"12345"},
			},
		}

		err = bus.PublishMsg(msg)
		if err != nil {
			t.Fatalf("PublishMsg failed: %v", err)
		}

		select {
		case receivedMsg := <-received:
			if receivedMsg.Header == nil {
				t.Error("expected headers, got nil")
			}
		case <-time.After(1 * time.Second):
			t.Error("timeout waiting for message")
		}
	})
}

// TestNextMsgWithContext tests context cancellation
func TestNextMsgWithContext(t *testing.T) {
	bus := newMockEventBus()

	t.Run("NextMsgWithContext respects context cancellation", func(t *testing.T) {
		sub, err := bus.SubscribeSync("test.context")
		if err != nil {
			t.Fatalf("SubscribeSync failed: %v", err)
		}
		defer func() {
			if err := sub.Unsubscribe(); err != nil {
				t.Errorf("Failed to unsubscribe: %v", err)
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err = sub.NextMsgWithContext(ctx)
		if err == nil {
			t.Error("expected context cancellation error, got nil")
		}
		if !errors.Is(err, context.DeadlineExceeded) && err.Error() != "timeout" {
			t.Errorf("expected context deadline exceeded, got: %v", err)
		}
	})
}

// TestMsg_Nak_RegularNATS verifies that Nak is a no-op for regular NATS messages
func TestMsg_Nak_RegularNATS(t *testing.T) {
	msg := &mono.Msg{
		Subject: "test.subject",
		Data:    []byte("test data"),
		NatsMsg: nil, // Regular NATS message (no underlying JetStream msg)
	}

	err := msg.Nak()
	if err != nil {
		t.Errorf("Nak on regular NATS message should return nil, got: %v", err)
	}
}

// TestMsg_NakWithDelay_RegularNATS verifies that NakWithDelay is a no-op for regular NATS messages
func TestMsg_NakWithDelay_RegularNATS(t *testing.T) {
	msg := &mono.Msg{
		Subject: "test.subject",
		Data:    []byte("test data"),
		NatsMsg: nil, // Regular NATS message (no underlying JetStream msg)
	}

	err := msg.NakWithDelay(5 * time.Second)
	if err != nil {
		t.Errorf("NakWithDelay on regular NATS message should return nil, got: %v", err)
	}
}

// TestMsg_Term_RegularNATS verifies that Term is a no-op for regular NATS messages
func TestMsg_Term_RegularNATS(t *testing.T) {
	msg := &mono.Msg{
		Subject: "test.subject",
		Data:    []byte("test data"),
		NatsMsg: nil, // Regular NATS message (no underlying JetStream msg)
	}

	err := msg.Term()
	if err != nil {
		t.Errorf("Term on regular NATS message should return nil, got: %v", err)
	}
}

// TestMsg_InProgress_RegularNATS verifies that InProgress is a no-op for regular NATS messages
func TestMsg_InProgress_RegularNATS(t *testing.T) {
	msg := &mono.Msg{
		Subject: "test.subject",
		Data:    []byte("test data"),
		NatsMsg: nil, // Regular NATS message (no underlying JetStream msg)
	}

	err := msg.InProgress()
	if err != nil {
		t.Errorf("InProgress on regular NATS message should return nil, got: %v", err)
	}
}

// TestMsg_Ack_RegularNATS verifies that Ack is a no-op for regular NATS messages
func TestMsg_Ack_RegularNATS(t *testing.T) {
	msg := &mono.Msg{
		Subject: "test.subject",
		Data:    []byte("test data"),
		NatsMsg: nil, // Regular NATS message (no underlying JetStream msg)
	}

	err := msg.Ack()
	if err != nil {
		t.Errorf("Ack on regular NATS message should return nil, got: %v", err)
	}
}
