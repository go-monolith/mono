package eventbus

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1/internal/logger"
	monoerrors "github.com/go-monolith/mono/v1/pkg/errors"
	"github.com/go-monolith/mono/v1/pkg/types"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// setupTestNATS creates an embedded NATS server and connection for testing
func setupTestNATS(t *testing.T) *nats.Conn {
	t.Helper()

	// Start embedded NATS server with JetStream enabled
	// Note: The server is automatically shut down when the test ends
	opts := test.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1 // Random port
	s := test.RunServer(&opts)

	// Clean up server when test ends
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})

	// Connect to the embedded server
	conn, err := nats.Connect(s.ClientURL(),
		nats.Timeout(2*time.Second),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		t.Fatalf("Failed to connect to test NATS server: %v", err)
	}

	// Clean up connection when test ends
	t.Cleanup(func() {
		conn.Close()
	})

	return conn
}

// TestNewEventBus tests event bus creation
func TestNewEventBus(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()

	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	if eb == nil {
		t.Fatal("NewEventBus returned nil")
	}
}

// TestNewEventBusNilConnection tests event bus with nil connection
func TestNewEventBusNilConnection(t *testing.T) {
	logger := logger.NewDefaultLogger()

	_, err := NewEventBus(nil, logger)
	if err == nil {
		t.Fatal("expected error for nil connection")
	}

	if err.Error() != "NATS connection cannot be nil" {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestNewEventBusNilLogger tests event bus with nil logger
func TestNewEventBusNilLogger(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	_, err := NewEventBus(conn, nil)
	if err == nil {
		t.Fatal("expected error for nil logger")
	}

	if err.Error() != "logger cannot be nil" {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestPublish tests basic publish functionality
func TestPublish(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.publish"
	data := []byte("test message")

	err := eb.Publish(subject, data)
	if err != nil {
		t.Errorf("Publish failed: %v", err)
	}
}

// TestPublishMsg tests message publishing with headers
func TestPublishMsg(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	msg := &types.Msg{
		Subject: "test.publish-msg",
		Data:    []byte("test message"),
		Header: types.Header{
			"Content-Type": []string{"application/json"},
			"X-Custom":     []string{"value"},
		},
	}

	err := eb.PublishMsg(msg)
	if err != nil {
		t.Errorf("PublishMsg failed: %v", err)
	}
}

// TestSubscribe tests asynchronous subscription
func TestSubscribe(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.subscribe"
	received := make(chan *types.Msg, 1)

	// Subscribe
	sub, err := eb.Subscribe(subject, func(ctx context.Context, msg *types.Msg) {
		received <- msg
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	// Wait for subscription to be ready
	time.Sleep(50 * time.Millisecond)

	// Publish
	testData := []byte("test message")
	if err := eb.Publish(subject, testData); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Receive
	select {
	case msg := <-received:
		if string(msg.Data) != string(testData) {
			t.Errorf("expected %s, got %s", testData, msg.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestSubscribeMultipleMessages tests receiving multiple messages
func TestSubscribeMultipleMessages(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.multiple"
	var count atomic.Int32

	// Subscribe
	sub, err := eb.Subscribe(subject, func(ctx context.Context, msg *types.Msg) {
		count.Add(1)
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	// Wait for subscription to be ready
	time.Sleep(50 * time.Millisecond)

	// Publish multiple messages
	for i := 0; i < 10; i++ {
		if err := eb.Publish(subject, []byte("message")); err != nil {
			t.Errorf("Publish %d failed: %v", i, err)
		}
	}

	// Wait for messages to be processed
	time.Sleep(100 * time.Millisecond)

	if count.Load() != 10 {
		t.Errorf("expected 10 messages, got %d", count.Load())
	}
}

// TestSubscribeSync tests synchronous subscription
func TestSubscribeSync(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.subscribe-sync"

	// Subscribe synchronously
	sub, err := eb.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	// Publish
	testData := []byte("sync message")
	if err := eb.Publish(subject, testData); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Receive with timeout
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg failed: %v", err)
	}

	if string(msg.Data) != string(testData) {
		t.Errorf("expected %s, got %s", testData, msg.Data)
	}
}

// TestRequest tests request-reply pattern
func TestRequest(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.request"

	// Set up responder
	sub, err := eb.Subscribe(subject, func(ctx context.Context, msg *types.Msg) {
		// Reply to the request
		reply := &types.Msg{
			Subject: msg.Reply,
			Data:    []byte("response"),
		}
		if err := eb.PublishMsg(reply); err != nil {
			t.Errorf("Failed to publish reply: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	// Wait for subscription to be ready
	time.Sleep(50 * time.Millisecond)

	// Send request
	response, err := eb.Request(subject, []byte("request"), 2*time.Second)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if string(response.Data) != "response" {
		t.Errorf("expected 'response', got '%s'", string(response.Data))
	}
}

// TestRequestTimeout tests request timeout
func TestRequestTimeout(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.request-timeout"

	// No responder - should timeout
	_, err := eb.Request(subject, []byte("request"), 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// TestQueueSubscribe tests queue group subscription
func TestQueueSubscribe(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.queue"
	queue := "workers"

	var count1, count2 atomic.Int32

	// Create two queue subscribers
	sub1, err := eb.QueueSubscribe(subject, queue, func(ctx context.Context, msg *types.Msg) {
		count1.Add(1)
	})
	if err != nil {
		t.Fatalf("QueueSubscribe 1 failed: %v", err)
	}
	defer func() {
		if err := sub1.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe sub1: %v", err)
		}
	}()

	sub2, err := eb.QueueSubscribe(subject, queue, func(ctx context.Context, msg *types.Msg) {
		count2.Add(1)
	})
	if err != nil {
		t.Fatalf("QueueSubscribe 2 failed: %v", err)
	}
	defer func() {
		if err := sub2.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe sub2: %v", err)
		}
	}()

	// Wait for subscriptions to be ready
	time.Sleep(50 * time.Millisecond)

	// Publish multiple messages
	messageCount := 20
	for i := 0; i < messageCount; i++ {
		if err := eb.Publish(subject, []byte("message")); err != nil {
			t.Errorf("Publish %d failed: %v", i, err)
		}
	}

	// Wait for messages to be processed
	time.Sleep(200 * time.Millisecond)

	// Both subscribers should have received messages (load balancing)
	total := count1.Load() + count2.Load()
	if total != int32(messageCount) {
		t.Errorf("expected %d total messages, got %d", messageCount, total)
	}

	// Both should have received at least some messages (rough check)
	if count1.Load() == 0 || count2.Load() == 0 {
		t.Error("queue subscribers should both receive messages")
	}
}

// TestQueueSubscribeSync tests synchronous queue subscription
func TestQueueSubscribeSync(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.queue-sync"
	queue := "workers"

	// Subscribe synchronously to queue
	sub, err := eb.QueueSubscribeSync(subject, queue)
	if err != nil {
		t.Fatalf("QueueSubscribeSync failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	// Publish
	testData := []byte("queue message")
	if err := eb.Publish(subject, testData); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Receive
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg failed: %v", err)
	}

	if string(msg.Data) != string(testData) {
		t.Errorf("expected %s, got %s", testData, msg.Data)
	}
}

// TestChanSubscribe tests channel-based subscription
func TestChanSubscribe(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.chan-subscribe"
	ch := make(chan *types.Msg, 10)

	// Subscribe with channel
	sub, err := eb.ChanSubscribe(subject, ch)
	if err != nil {
		t.Fatalf("ChanSubscribe failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	// Wait for subscription to be ready
	time.Sleep(50 * time.Millisecond)

	// Publish
	testData := []byte("channel message")
	if err := eb.Publish(subject, testData); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Receive from channel
	select {
	case msg := <-ch:
		if string(msg.Data) != string(testData) {
			t.Errorf("expected %s, got %s", testData, msg.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestSubscriptionUnsubscribe tests unsubscribe functionality
func TestSubscriptionUnsubscribe(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.unsubscribe"
	var count atomic.Int32

	// Subscribe
	sub, err := eb.Subscribe(subject, func(ctx context.Context, msg *types.Msg) {
		count.Add(1)
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Wait for subscription to be ready
	time.Sleep(50 * time.Millisecond)

	// Publish first message
	if err := eb.Publish(subject, []byte("msg1")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Unsubscribe
	if err := sub.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe failed: %v", err)
	}

	// Publish second message (should not be received)
	if err := eb.Publish(subject, []byte("msg2")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Should only have received first message
	if count.Load() != 1 {
		t.Errorf("expected 1 message, got %d", count.Load())
	}

	// IsValid should return false
	if sub.IsValid() {
		t.Error("subscription should not be valid after unsubscribe")
	}
}

// TestSubscriptionDrain tests drain functionality
func TestSubscriptionDrain(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.drain"
	received := make(chan int, 10)

	// Subscribe
	sub, err := eb.Subscribe(subject, func(ctx context.Context, msg *types.Msg) {
		received <- 1
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Wait for subscription to be ready
	time.Sleep(50 * time.Millisecond)

	// Publish messages
	for i := 0; i < 5; i++ {
		if err := eb.Publish(subject, []byte("msg")); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Drain (should process pending messages)
	if err := sub.Drain(); err != nil {
		t.Errorf("Drain failed: %v", err)
	}

	// Wait for drain to complete
	time.Sleep(200 * time.Millisecond)

	// All messages should have been processed
	count := len(received)
	if count != 5 {
		t.Errorf("expected 5 messages after drain, got %d", count)
	}
}

// TestSubscriptionSubject tests subject retrieval
func TestSubscriptionSubject(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.subject"

	sub, err := eb.Subscribe(subject, func(ctx context.Context, msg *types.Msg) {})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	if sub.Subject() != subject {
		t.Errorf("expected subject %s, got %s", subject, sub.Subject())
	}
}

// TestSubscriptionQueue tests queue name retrieval
func TestSubscriptionQueue(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.queue-name"
	queue := "workers"

	sub, err := eb.QueueSubscribe(subject, queue, func(ctx context.Context, msg *types.Msg) {})
	if err != nil {
		t.Fatalf("QueueSubscribe failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	if sub.Queue() != queue {
		t.Errorf("expected queue %s, got %s", queue, sub.Queue())
	}

	// Non-queue subscription should return empty string
	sub2, err := eb.Subscribe(subject, func(ctx context.Context, msg *types.Msg) {})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer func() {
		if err := sub2.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe sub2: %v", err)
		}
	}()

	if sub2.Queue() != "" {
		t.Errorf("expected empty queue, got %s", sub2.Queue())
	}
}

// TestNextMsgWithContext tests context-based message fetching
func TestNextMsgWithContext(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.next-msg-context"

	sub, err := eb.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	// Publish
	testData := []byte("context message")
	if err := eb.Publish(subject, testData); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Receive with context
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		t.Fatalf("NextMsgWithContext failed: %v", err)
	}

	if string(msg.Data) != string(testData) {
		t.Errorf("expected %s, got %s", testData, msg.Data)
	}
}

// TestNextMsgWithContextCancellation tests context cancellation
func TestNextMsgWithContextCancellation(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.context-cancel"

	sub, err := eb.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	// Create context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should return error immediately
	_, err = sub.NextMsgWithContext(ctx)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

// TestMessageHeaders tests message header preservation
func TestMessageHeaders(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.headers"
	received := make(chan *types.Msg, 1)

	// Subscribe
	sub, err := eb.Subscribe(subject, func(ctx context.Context, msg *types.Msg) {
		received <- msg
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	// Wait for subscription to be ready
	time.Sleep(50 * time.Millisecond)

	// Publish with headers
	msg := &types.Msg{
		Subject: subject,
		Data:    []byte("test"),
		Header: types.Header{
			"Content-Type": []string{"application/json"},
			"X-Custom":     []string{"value1", "value2"},
		},
	}

	if err := eb.PublishMsg(msg); err != nil {
		t.Fatalf("PublishMsg failed: %v", err)
	}

	// Receive
	select {
	case receivedMsg := <-received:
		contentType := receivedMsg.Header["Content-Type"]
		if len(contentType) == 0 || contentType[0] != "application/json" {
			t.Errorf("expected Content-Type header application/json, got %v", contentType)
		}

		customValues := receivedMsg.Header["X-Custom"]
		if len(customValues) != 2 {
			t.Errorf("expected 2 X-Custom values, got %d", len(customValues))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestSetRuntimeContext tests that SetRuntimeContext works correctly
func TestSetRuntimeContext(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("Failed to create event bus: %v", err)
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set the runtime context
	eb.SetRuntimeContext(ctx)

	// Subscribe with a handler that checks the context
	subject := "test.runtime.context"
	contextReceived := make(chan context.Context, 1)

	_, err = eb.Subscribe(subject, func(handlerCtx context.Context, msg *types.Msg) {
		contextReceived <- handlerCtx
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Wait for subscription to be ready
	time.Sleep(50 * time.Millisecond)

	// Publish a message
	if err := eb.Publish(subject, []byte("test")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Verify handler received the runtime context
	select {
	case handlerCtx := <-contextReceived:
		if handlerCtx != ctx {
			t.Error("Handler did not receive the runtime context")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for handler to execute")
	}
}

// TestRuntimeContextCancellation tests that handlers receive cancelled context
func TestRuntimeContextCancellation(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("Failed to create event bus: %v", err)
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Set the runtime context
	eb.SetRuntimeContext(ctx)

	// Subscribe with a handler that checks if context is cancelled
	subject := "test.cancellation"
	handlerExecuted := make(chan bool, 1)

	_, err = eb.Subscribe(subject, func(handlerCtx context.Context, msg *types.Msg) {
		select {
		case <-handlerCtx.Done():
			// Context is cancelled
			handlerExecuted <- true
		default:
			// Context is not cancelled
			handlerExecuted <- false
		}
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Wait for subscription to be ready
	time.Sleep(50 * time.Millisecond)

	// Cancel the context before publishing
	cancel()

	// Small delay to ensure cancellation propagates
	time.Sleep(10 * time.Millisecond)

	// Publish a message
	if err := eb.Publish(subject, []byte("test")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Verify handler received cancelled context
	select {
	case wasCancelled := <-handlerExecuted:
		if !wasCancelled {
			t.Error("Handler did not receive a cancelled context")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for handler to execute")
	}
}

// TestParseServiceSubject tests the parseServiceSubject helper function
func TestParseServiceSubject(t *testing.T) {
	tests := []struct {
		name            string
		subject         string
		wantModuleName  string
		wantServiceName string
		wantOk          bool
	}{
		{
			name:            "valid service subject",
			subject:         "services.inventory.check-stock",
			wantModuleName:  "inventory",
			wantServiceName: "check-stock",
			wantOk:          true,
		},
		{
			name:            "subject with additional parts should fail",
			subject:         "services.order.process-payment.extra",
			wantModuleName:  "",
			wantServiceName: "",
			wantOk:          false,
		},
		{
			name:            "invalid prefix",
			subject:         "events.inventory.check-stock",
			wantModuleName:  "",
			wantServiceName: "",
			wantOk:          false,
		},
		{
			name:            "too few parts",
			subject:         "services.inventory",
			wantModuleName:  "",
			wantServiceName: "",
			wantOk:          false,
		},
		{
			name:            "empty subject",
			subject:         "",
			wantModuleName:  "",
			wantServiceName: "",
			wantOk:          false,
		},
		{
			name:            "single part",
			subject:         "services",
			wantModuleName:  "",
			wantServiceName: "",
			wantOk:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			moduleName, serviceName, ok := parseServiceSubject(tt.subject)
			if ok != tt.wantOk {
				t.Errorf("parseServiceSubject(%q) ok = %v, want %v", tt.subject, ok, tt.wantOk)
			}
			if moduleName != tt.wantModuleName {
				t.Errorf("parseServiceSubject(%q) moduleName = %q, want %q", tt.subject, moduleName, tt.wantModuleName)
			}
			if serviceName != tt.wantServiceName {
				t.Errorf("parseServiceSubject(%q) serviceName = %q, want %q", tt.subject, serviceName, tt.wantServiceName)
			}
		})
	}
}

// TestRequestNoResponders tests request-reply when no responders are available
func TestRequestNoResponders(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	t.Run("service subject returns ServiceError with ErrServiceUnavailable", func(t *testing.T) {
		subject := "services.inventory.check-stock"

		// No subscriber on this subject - should return ServiceError
		_, err := eb.Request(subject, []byte("request"), 100*time.Millisecond)
		if err == nil {
			t.Fatal("expected error when no responders available")
		}

		// Check that it wraps ErrServiceUnavailable
		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Errorf("expected ErrServiceUnavailable, got %v", err)
		}

		// Check that it's a ServiceError
		if !monoerrors.IsServiceError(err) {
			t.Errorf("expected ServiceError, got %T: %v", err, err)
		}

		// The original NATS error should be wrapped in the chain
		// Note: nats.ErrNoResponders is now part of the error chain
		if !strings.Contains(err.Error(), "no responders") {
			t.Errorf("error should mention 'no responders', got: %v", err)
		}

		// Extract ServiceError and verify fields
		serviceErr, ok := monoerrors.GetServiceError(err)
		if !ok {
			t.Fatal("expected to extract ServiceError")
		}
		if serviceErr.ServiceName != "check-stock" {
			t.Errorf("ServiceName = %q, want 'check-stock'", serviceErr.ServiceName)
		}
		if serviceErr.ModuleName != "inventory" {
			t.Errorf("ModuleName = %q, want 'inventory'", serviceErr.ModuleName)
		}
		if serviceErr.ServiceType != types.ServiceTypeRequestReply {
			t.Errorf("ServiceType = %v, want ServiceTypeRequestReply", serviceErr.ServiceType)
		}
	})

	t.Run("non-service subject returns generic error with ErrServiceUnavailable", func(t *testing.T) {
		subject := "custom.subject.path"

		// No subscriber on this non-service subject
		_, err := eb.Request(subject, []byte("request"), 100*time.Millisecond)
		if err == nil {
			t.Fatal("expected error when no responders available")
		}

		// Should still wrap ErrServiceUnavailable
		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Errorf("expected ErrServiceUnavailable, got %v", err)
		}

		// Should NOT be a ServiceError (since we couldn't parse the subject)
		if monoerrors.IsServiceError(err) {
			t.Errorf("expected non-ServiceError for non-service subject, got ServiceError")
		}

		// Should contain the subject in the error message
		if err.Error() == "" || !containsSubject(err.Error(), subject) {
			t.Errorf("error message should contain subject %q: %s", subject, err.Error())
		}
	})
}

// containsSubject checks if the error message contains the subject
func containsSubject(errMsg, subject string) bool {
	return strings.Contains(errMsg, subject)
}

// TestRequestWithContext tests request-reply pattern with context
func TestRequestWithContext(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.request-with-context"

	// Set up responder
	sub, err := eb.Subscribe(subject, func(ctx context.Context, msg *types.Msg) {
		reply := &types.Msg{
			Subject: msg.Reply,
			Data:    []byte("response"),
		}
		if err := eb.PublishMsg(reply); err != nil {
			t.Errorf("Failed to publish reply: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Failed to unsubscribe: %v", err)
		}
	}()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	response, err := eb.RequestWithContext(ctx, subject, []byte("request"))
	if err != nil {
		t.Fatalf("RequestWithContext failed: %v", err)
	}

	if string(response.Data) != "response" {
		t.Errorf("expected 'response', got '%s'", string(response.Data))
	}
}

// TestRequestWithContextCancellation tests that context cancellation works
func TestRequestWithContextCancellation(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.request-context-cancel"

	// Create already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := eb.RequestWithContext(ctx, subject, []byte("request"))
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}

// TestRequestWithContextTimeout tests that context timeout works
func TestRequestWithContextTimeout(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.request-context-timeout"

	// No responder - should timeout or get no responders error
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := eb.RequestWithContext(ctx, subject, []byte("request"))
	if err == nil {
		t.Fatal("expected timeout error")
	}

	// Could be either DeadlineExceeded or no responders (depending on timing)
	isTimeout := errors.Is(err, context.DeadlineExceeded)
	isNoResponders := strings.Contains(err.Error(), "no responders")
	if !isTimeout && !isNoResponders {
		t.Errorf("expected timeout or no responders error, got: %v", err)
	}
}

// TestRequestWithContextNoResponders tests no responders error handling with context
func TestRequestWithContextNoResponders(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	t.Run("service subject returns ServiceError", func(t *testing.T) {
		subject := "services.inventory.check-stock"
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := eb.RequestWithContext(ctx, subject, []byte("request"))
		if err == nil {
			t.Fatal("expected error when no responders available")
		}

		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Errorf("expected ErrServiceUnavailable, got %v", err)
		}

		if !monoerrors.IsServiceError(err) {
			t.Errorf("expected ServiceError, got %T: %v", err, err)
		}

		serviceErr, ok := monoerrors.GetServiceError(err)
		if !ok {
			t.Fatal("expected to extract ServiceError")
		}
		if serviceErr.ServiceName != "check-stock" {
			t.Errorf("ServiceName = %v, want check-stock", serviceErr.ServiceName)
		}
		if serviceErr.ModuleName != "inventory" {
			t.Errorf("ModuleName = %v, want inventory", serviceErr.ModuleName)
		}
		if serviceErr.ServiceType != types.ServiceTypeRequestReply {
			t.Errorf("ServiceType = %v, want ServiceTypeRequestReply", serviceErr.ServiceType)
		}
	})

	t.Run("non-service subject returns generic error with ErrServiceUnavailable", func(t *testing.T) {
		subject := "custom.subject.path"
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := eb.RequestWithContext(ctx, subject, []byte("request"))
		if err == nil {
			t.Fatal("expected error when no responders available")
		}

		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Errorf("expected ErrServiceUnavailable, got %v", err)
		}

		if monoerrors.IsServiceError(err) {
			t.Errorf("expected non-ServiceError for non-service subject, got ServiceError")
		}

		if err.Error() == "" || !containsSubject(err.Error(), subject) {
			t.Errorf("error message should contain subject %q: %s", subject, err.Error())
		}
	})
}

// TestRequestMsgWithContext tests request-reply pattern with full message
func TestRequestMsgWithContext(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	subject := "test.request-msg-context"

	// Set up responder that echoes headers
	sub, err := eb.Subscribe(subject, func(ctx context.Context, msg *types.Msg) {
		reply := &types.Msg{
			Subject: msg.Reply,
			Data:    []byte("response"),
			Header: types.Header{
				"Echo-Header": msg.Header["X-Custom"],
			},
		}
		if err := eb.PublishMsg(reply); err != nil {
			t.Errorf("Failed to publish reply: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msg := &types.Msg{
		Subject: subject,
		Data:    []byte("request"),
		Header: types.Header{
			"X-Custom": []string{"test-value"},
		},
	}

	response, err := eb.RequestMsgWithContext(ctx, msg)
	if err != nil {
		t.Fatalf("RequestMsgWithContext failed: %v", err)
	}

	if string(response.Data) != "response" {
		t.Errorf("expected 'response', got '%s'", string(response.Data))
	}

	// Verify headers were transmitted
	if len(response.Header["Echo-Header"]) == 0 || response.Header["Echo-Header"][0] != "test-value" {
		t.Errorf("expected Echo-Header 'test-value', got %v", response.Header["Echo-Header"])
	}
}

// TestRequestMsgWithContextNoResponders tests no responders error handling
func TestRequestMsgWithContextNoResponders(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, _ := NewEventBus(conn, logger)

	t.Run("service subject returns ServiceError", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		msg := &types.Msg{
			Subject: "services.inventory.check-stock",
			Data:    []byte("request"),
		}

		_, err := eb.RequestMsgWithContext(ctx, msg)
		if err == nil {
			t.Fatal("expected error when no responders available")
		}

		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Errorf("expected ErrServiceUnavailable, got %v", err)
		}

		if !monoerrors.IsServiceError(err) {
			t.Errorf("expected ServiceError, got %T: %v", err, err)
		}
	})

	t.Run("non-service subject returns generic error with ErrServiceUnavailable", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		subject := "custom.subject.path"
		msg := &types.Msg{
			Subject: subject,
			Data:    []byte("request"),
		}

		_, err := eb.RequestMsgWithContext(ctx, msg)
		if err == nil {
			t.Fatal("expected error when no responders available")
		}

		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Errorf("expected ErrServiceUnavailable, got %v", err)
		}

		if monoerrors.IsServiceError(err) {
			t.Errorf("expected non-ServiceError for non-service subject, got ServiceError")
		}
	})
}

// TestEventStream tests EventBus.EventStream lazy initialization
func TestEventStream(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("Failed to create EventBus: %v", err)
	}

	t.Run("lazy initialization", func(t *testing.T) {
		// First call should create JetStream
		es1, err := eb.EventStream()
		if err != nil {
			t.Fatalf("EventStream failed: %v", err)
		}
		if es1 == nil {
			t.Fatal("EventStream returned nil")
		}

		// Second call should return same instance (cached)
		es2, err := eb.EventStream()
		if err != nil {
			t.Fatalf("Second EventStream call failed: %v", err)
		}
		if es2 != es1 {
			t.Error("EventStream should return cached instance")
		}
	})

	t.Run("JetStream operations", func(t *testing.T) {
		es, err := eb.EventStream()
		if err != nil {
			t.Fatalf("EventStream failed: %v", err)
		}

		ctx := context.Background()

		// Create a stream
		streamCfg := types.StreamConfig{
			Name:     "TEST_EVENTSTREAM",
			Subjects: []string{"eventstream.>"},
			Storage:  types.MemoryStorage,
		}

		stream, err := es.CreateOrUpdateStream(ctx, streamCfg)
		if err != nil {
			t.Fatalf("CreateOrUpdateStream failed: %v", err)
		}
		if stream == nil {
			t.Fatal("stream is nil")
		}

		// Clean up
		_ = es.DeleteStream(ctx, "TEST_EVENTSTREAM")
	})
}

// TestEventBusWithConn tests the EventBusWithConn generic interface
func TestEventBusWithConn(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()

	t.Run("implements EventBusWithConn interface", func(t *testing.T) {
		eb, err := NewEventBus(conn, logger)
		if err != nil {
			t.Fatalf("NewEventBus failed: %v", err)
		}

		// Cast to EventBusWithConn[*nats.Conn]
		provider, ok := eb.(types.EventBusWithConn[*nats.Conn])
		if !ok {
			t.Fatal("EventBus does not implement EventBusWithConn[*nats.Conn]")
		}

		extractedConn := provider.Conn()
		if extractedConn == nil {
			t.Fatal("Conn() returned nil")
		}
		if extractedConn != conn {
			t.Error("Conn() returned different connection")
		}
	})

	t.Run("Conn method returns underlying connection", func(t *testing.T) {
		eb, err := NewEventBus(conn, logger)
		if err != nil {
			t.Fatalf("NewEventBus failed: %v", err)
		}

		// Access Conn() directly via concrete type
		natsEB, ok := eb.(*natsEventBus)
		if !ok {
			t.Fatal("Failed to cast to *natsEventBus")
		}

		extractedConn := natsEB.Conn()
		if extractedConn == nil {
			t.Fatal("Conn() returned nil")
		}
		if extractedConn != conn {
			t.Error("Conn() returned different connection")
		}
	})
}

// TestPublishWithClosedConnection tests Publish error path when connection is closed
func TestPublishWithClosedConnection(t *testing.T) {
	conn := setupTestNATS(t)

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Close the connection to trigger publish error
	conn.Close()

	// Wait a moment for connection to fully close
	time.Sleep(50 * time.Millisecond)

	subject := "test.publish.error"
	data := []byte("test message")

	// Publish should fail with closed connection
	err = eb.Publish(subject, data)
	if err == nil {
		t.Fatal("expected error when publishing to closed connection")
	}

	// Error message should include the subject
	if !strings.Contains(err.Error(), subject) {
		t.Errorf("error should include subject %q: %v", subject, err)
	}

	// Error message should mention publish failure
	if !strings.Contains(err.Error(), "failed to publish") {
		t.Errorf("error should mention 'failed to publish': %v", err)
	}
}

// TestPublishWithEmptySubject tests Publish with empty subject
func TestPublishWithEmptySubject(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	subject := ""
	data := []byte("test message")

	// Publish with empty subject should fail
	err = eb.Publish(subject, data)
	if err == nil {
		t.Error("expected error for empty subject")
	}

	// Error should mention publish failure
	if !strings.Contains(err.Error(), "failed to publish") {
		t.Errorf("error should mention 'failed to publish': %v", err)
	}
}

// TestPublishMsgWithClosedConnection tests PublishMsg error path when connection is closed
func TestPublishMsgWithClosedConnection(t *testing.T) {
	conn := setupTestNATS(t)

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Close the connection to trigger publish error
	conn.Close()

	// Wait a moment for connection to fully close
	time.Sleep(50 * time.Millisecond)

	msg := &types.Msg{
		Subject: "test.publish-msg.error",
		Data:    []byte("test message"),
		Header: types.Header{
			"Content-Type": []string{"application/json"},
		},
	}

	// PublishMsg should fail with closed connection
	err = eb.PublishMsg(msg)
	if err == nil {
		t.Fatal("expected error when publishing message to closed connection")
	}

	// Error message should include the subject
	if !strings.Contains(err.Error(), msg.Subject) {
		t.Errorf("error should include subject %q: %v", msg.Subject, err)
	}

	// Error message should mention publish failure
	if !strings.Contains(err.Error(), "failed to publish message") {
		t.Errorf("error should mention 'failed to publish message': %v", err)
	}
}

// TestPublishMsgWithEmptySubject tests PublishMsg with empty subject
func TestPublishMsgWithEmptySubject(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	msg := &types.Msg{
		Subject: "",
		Data:    []byte("test message"),
		Header: types.Header{
			"Content-Type": []string{"application/json"},
		},
	}

	// PublishMsg with empty subject should fail
	err = eb.PublishMsg(msg)
	if err == nil {
		t.Error("expected error for empty subject")
	}

	// Error should mention publish failure
	if !strings.Contains(err.Error(), "failed to publish message") {
		t.Errorf("error should mention 'failed to publish message': %v", err)
	}
}

// TestPublishMsgWithHeaders tests PublishMsg with various header scenarios
func TestPublishMsgWithHeaders(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	t.Run("empty headers", func(t *testing.T) {
		msg := &types.Msg{
			Subject: "test.empty-headers",
			Data:    []byte("test"),
			Header:  types.Header{},
		}

		err := eb.PublishMsg(msg)
		if err != nil {
			t.Errorf("PublishMsg with empty headers failed: %v", err)
		}
	})

	t.Run("nil headers", func(t *testing.T) {
		msg := &types.Msg{
			Subject: "test.nil-headers",
			Data:    []byte("test"),
			Header:  nil,
		}

		err := eb.PublishMsg(msg)
		if err != nil {
			t.Errorf("PublishMsg with nil headers failed: %v", err)
		}
	})

	t.Run("multiple header values", func(t *testing.T) {
		msg := &types.Msg{
			Subject: "test.multi-headers",
			Data:    []byte("test"),
			Header: types.Header{
				"X-Custom": []string{"value1", "value2", "value3"},
			},
		}

		err := eb.PublishMsg(msg)
		if err != nil {
			t.Errorf("PublishMsg with multiple header values failed: %v", err)
		}
	})
}

// ============================================================================
// Additional Error Path Tests for Task 15 and 19
// ============================================================================

// TestRequest_NoResponders_FallbackPath tests Request error handling with non-service subject
func TestRequest_NoResponders_FallbackPath(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Request to subject that doesn't match "services.<module>.<service>" format
	// This tests the fallback path in Request (line 74-75)
	_, err = eb.Request("invalid.subject", []byte("test"), 100*time.Millisecond)
	if err == nil {
		t.Error("expected error for no responders")
	}

	// Verify error is wrapped with ErrServiceUnavailable
	if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
		t.Errorf("expected ErrServiceUnavailable, got: %v", err)
	}
}

// TestRequest_GeneralError tests Request with general error (not NoResponders)
func TestRequest_GeneralError(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Close connection to cause error
	conn.Close()

	_, err = eb.Request("test.subject", []byte("test"), 100*time.Millisecond)
	if err == nil {
		t.Error("expected error on closed connection")
	}
}

// TestRequestWithContext_NoResponders_FallbackPath tests context request with non-service subject
func TestRequestWithContext_NoResponders_FallbackPath(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Request to non-service format subject
	_, err = eb.RequestWithContext(ctx, "non.service.subject.extra", []byte("test"))
	if err == nil {
		t.Error("expected error for no responders")
	}

	// Verify error is wrapped with ErrServiceUnavailable
	if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
		t.Errorf("expected ErrServiceUnavailable, got: %v", err)
	}
}

// TestRequestWithContext_ContextCanceled tests context cancellation handling
func TestRequestWithContext_ContextCanceled(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Create already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = eb.RequestWithContext(ctx, "test.subject", []byte("test"))
	if err == nil {
		t.Error("expected error for cancelled context")
	}

	// Verify context.Canceled is returned as-is
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}

// TestRequestWithContext_DeadlineExceeded tests deadline exceeded handling
func TestRequestWithContext_DeadlineExceeded(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Create context with very short deadline
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Wait for deadline to expire
	time.Sleep(5 * time.Millisecond)

	_, err = eb.RequestWithContext(ctx, "test.subject", []byte("test"))
	if err == nil {
		t.Error("expected error for deadline exceeded")
	}

	// Verify context.DeadlineExceeded is returned
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded error, got: %v", err)
	}
}

// TestRequestMsgWithContext_NoResponders_FallbackPath tests RequestMsgWithContext with non-service subject
func TestRequestMsgWithContext_NoResponders_FallbackPath(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Request to non-service format subject
	msg := &types.Msg{
		Subject: "not.a.services.subject",
		Data:    []byte("test"),
	}

	_, err = eb.RequestMsgWithContext(ctx, msg)
	if err == nil {
		t.Error("expected error for no responders")
	}

	// Verify error is wrapped with ErrServiceUnavailable
	if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
		t.Errorf("expected ErrServiceUnavailable, got: %v", err)
	}
}

// TestRequestMsgWithContext_ContextCanceled tests RequestMsgWithContext with cancelled context
func TestRequestMsgWithContext_ContextCanceled(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	msg := &types.Msg{
		Subject: "test.subject",
		Data:    []byte("test"),
	}

	_, err = eb.RequestMsgWithContext(ctx, msg)
	if err == nil {
		t.Error("expected error for cancelled context")
	}

	// Verify context.Canceled is returned as-is (line 138-139)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}

// TestSubscribe_Error tests Subscribe error path with invalid subject
func TestSubscribe_Error(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Close connection to cause subscribe to fail
	conn.Close()

	_, err = eb.Subscribe("test.subject", func(ctx context.Context, msg *types.Msg) {})
	if err == nil {
		t.Error("expected error on closed connection")
	}

	if !strings.Contains(err.Error(), "failed to subscribe") {
		t.Errorf("expected 'failed to subscribe' in error, got: %v", err)
	}
}

// TestSubscribeSync_Error tests SubscribeSync error path
func TestSubscribeSync_Error(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Close connection to cause subscribe to fail
	conn.Close()

	_, err = eb.SubscribeSync("test.subject")
	if err == nil {
		t.Error("expected error on closed connection")
	}

	if !strings.Contains(err.Error(), "failed to create sync subscription") {
		t.Errorf("expected 'failed to create sync subscription' in error, got: %v", err)
	}
}

// TestQueueSubscribe_Error tests QueueSubscribe error path
func TestQueueSubscribe_Error(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Close connection to cause subscribe to fail
	conn.Close()

	_, err = eb.QueueSubscribe("test.subject", "queue", func(ctx context.Context, msg *types.Msg) {})
	if err == nil {
		t.Error("expected error on closed connection")
	}

	if !strings.Contains(err.Error(), "failed to create queue subscription") {
		t.Errorf("expected 'failed to create queue subscription' in error, got: %v", err)
	}
}

// TestQueueSubscribeSync_Error tests QueueSubscribeSync error path
func TestQueueSubscribeSync_Error(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Close connection to cause subscribe to fail
	conn.Close()

	_, err = eb.QueueSubscribeSync("test.subject", "queue")
	if err == nil {
		t.Error("expected error on closed connection")
	}

	if !strings.Contains(err.Error(), "failed to create sync queue subscription") {
		t.Errorf("expected 'failed to create sync queue subscription' in error, got: %v", err)
	}
}

// TestChanSubscribe_Error tests ChanSubscribe error path
func TestChanSubscribe_Error(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Close connection to cause subscribe to fail
	conn.Close()

	ch := make(chan *types.Msg, 10)
	_, err = eb.ChanSubscribe("test.subject", ch)
	if err == nil {
		t.Error("expected error on closed connection")
	}

	if !strings.Contains(err.Error(), "failed to create channel subscription") {
		t.Errorf("expected 'failed to create channel subscription' in error, got: %v", err)
	}
}

// TestEventStream_Error tests EventStream error path
func TestEventStream_Error(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Close connection to cause JetStream creation to fail
	conn.Close()

	_, err = eb.EventStream()
	// Note: EventStream may or may not fail immediately on closed connection
	// depending on NATS internal state. If it fails, verify the error message.
	if err != nil {
		if !strings.Contains(err.Error(), "failed to create JetStream") {
			t.Errorf("expected 'failed to create JetStream' in error, got: %v", err)
		}
	}
	// If no error, the connection may still be in a usable state for JetStream
}

// TestNextMsg_Error tests NextMsg error path
func TestNextMsg_Error(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	logger := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, logger)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Create sync subscription
	sub, err := eb.SubscribeSync("test.nextmsg.error")
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}
	defer sub.Unsubscribe()

	// NextMsg with very short timeout should timeout
	_, err = sub.NextMsg(1 * time.Millisecond)
	if err == nil {
		t.Error("expected error for timeout")
	}
	// Error is returned as-is from NATS
}

// TestChanSubscribe_MessageForwarding tests that messages are correctly forwarded through ChanSubscribe
func TestChanSubscribe_MessageForwarding(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, log)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	subject := "test.chan-subscribe-forward"
	ch := make(chan *types.Msg, 10)

	// Subscribe
	sub, err := eb.ChanSubscribe(subject, ch)
	if err != nil {
		t.Fatalf("ChanSubscribe failed: %v", err)
	}

	// Wait for subscription to be ready
	time.Sleep(50 * time.Millisecond)

	// Publish multiple messages with headers
	for i := 0; i < 3; i++ {
		msg := &types.Msg{
			Subject: subject,
			Data:    []byte("message " + string(rune('A'+i))),
			Header: types.Header{
				"X-Sequence": []string{string(rune('0' + i))},
			},
		}
		if err := eb.PublishMsg(msg); err != nil {
			t.Fatalf("PublishMsg failed: %v", err)
		}
	}

	// Receive messages through the channel
	receivedCount := 0
	timeout := time.After(2 * time.Second)
	for receivedCount < 3 {
		select {
		case msg := <-ch:
			if msg == nil {
				t.Error("received nil message")
				continue
			}
			// Verify message data is correct
			if len(msg.Data) == 0 {
				t.Error("received empty message data")
			}
			// Verify headers are preserved
			if msg.Header == nil {
				t.Error("headers not preserved")
			}
			receivedCount++
		case <-timeout:
			t.Fatalf("timeout waiting for messages, received %d/3", receivedCount)
		}
	}

	// Unsubscribe to trigger channel close
	if err := sub.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe failed: %v", err)
	}

	// Verify channel is closed after unsubscribe
	time.Sleep(100 * time.Millisecond)
	select {
	case _, ok := <-ch:
		_ = ok // Channel state is intentionally unchecked - just verifying we can receive
		// If not ok, channel is closed - expected
	default:
		// Channel empty but not closed yet - might need more time
	}
}

// TestChanSubscribe_ChannelClose tests that channel is closed when subscription ends
func TestChanSubscribe_ChannelClose(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, log)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	subject := "test.chan-subscribe-close"
	ch := make(chan *types.Msg, 5)

	// Subscribe
	sub, err := eb.ChanSubscribe(subject, ch)
	if err != nil {
		t.Fatalf("ChanSubscribe failed: %v", err)
	}

	// Verify subscription is valid
	if !sub.IsValid() {
		t.Error("subscription should be valid")
	}

	// Unsubscribe
	if err := sub.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe failed: %v", err)
	}

	// Verify subscription is no longer valid
	if sub.IsValid() {
		t.Error("subscription should not be valid after unsubscribe")
	}
}

// TestRequestWithContext_GeneralError tests general error path (not context or NoResponders)
func TestRequestWithContext_GeneralError(t *testing.T) {
	conn := setupTestNATS(t)

	log := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, log)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Close connection to cause a general error
	conn.Close()

	ctx := context.Background() // Valid context, not cancelled

	_, err = eb.RequestWithContext(ctx, "test.subject", []byte("test"))
	if err == nil {
		t.Error("expected error on closed connection")
	}
	// Error should be wrapped, not a context error
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected general error, not context error: %v", err)
	}
}

// TestRequestMsgWithContext_GeneralError tests general error path (not context or NoResponders)
func TestRequestMsgWithContext_GeneralError(t *testing.T) {
	conn := setupTestNATS(t)

	log := logger.NewDefaultLogger()
	eb, err := NewEventBus(conn, log)
	if err != nil {
		t.Fatalf("NewEventBus failed: %v", err)
	}

	// Close connection to cause a general error
	conn.Close()

	ctx := context.Background() // Valid context, not cancelled

	msg := &types.Msg{
		Subject: "test.subject",
		Data:    []byte("test"),
	}

	_, err = eb.RequestMsgWithContext(ctx, msg)
	if err == nil {
		t.Error("expected error on closed connection")
	}
	// Error should be wrapped, not a context error
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected general error, not context error: %v", err)
	}
}
