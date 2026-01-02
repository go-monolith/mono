package container

import (
	"context"
	"errors"
	"testing"
	"time"

	monoerrors "github.com/go-monolith/mono/v1/pkg/errors"
	"github.com/go-monolith/mono/v1/pkg/types"
)

// mockEventBus implements types.EventBus for testing
type mockEventBus struct {
	publishedSubjects        []string
	requestHandler           func(subject string, data []byte, timeout time.Duration) (*types.Msg, error)
	requestWithCtxHandler    func(ctx context.Context, subject string, data []byte) (*types.Msg, error)
	requestMsgWithCtxHandler func(ctx context.Context, msg *types.Msg) (*types.Msg, error)
	publishFunc              func(subject string, data []byte) error
	publishMsgFunc           func(msg *types.Msg) error
}

func (m *mockEventBus) Request(subject string, data []byte, timeout time.Duration) (*types.Msg, error) {
	if m.requestHandler != nil {
		return m.requestHandler(subject, data, timeout)
	}
	return &types.Msg{Data: []byte("mock-response")}, nil
}

func (m *mockEventBus) RequestWithContext(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
	if m.requestWithCtxHandler != nil {
		return m.requestWithCtxHandler(ctx, subject, data)
	}
	// Fallback to requestHandler if requestWithCtxHandler not set
	if m.requestHandler != nil {
		// Extract timeout from context deadline
		timeout := 30 * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			timeout = time.Until(deadline)
		}
		return m.requestHandler(subject, data, timeout)
	}
	return &types.Msg{Data: []byte("mock-response")}, nil
}

func (m *mockEventBus) RequestMsgWithContext(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
	if m.requestMsgWithCtxHandler != nil {
		return m.requestMsgWithCtxHandler(ctx, msg)
	}
	// Fallback to requestWithCtxHandler if requestMsgWithCtxHandler not set
	if m.requestWithCtxHandler != nil {
		return m.requestWithCtxHandler(ctx, msg.Subject, msg.Data)
	}
	// Final fallback to RequestWithContext (which calls requestHandler if set)
	return m.RequestWithContext(ctx, msg.Subject, msg.Data)
}

func (m *mockEventBus) Publish(subject string, data []byte) error {
	if m.publishFunc != nil {
		return m.publishFunc(subject, data)
	}
	m.publishedSubjects = append(m.publishedSubjects, subject)
	return nil
}

func (m *mockEventBus) PublishMsg(msg *types.Msg) error {
	if m.publishMsgFunc != nil {
		return m.publishMsgFunc(msg)
	}
	// Fallback to Publish if publishMsgFunc not set
	return m.Publish(msg.Subject, msg.Data)
}

func (m *mockEventBus) Subscribe(subject string, handler types.MsgHandler) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBus) SubscribeSync(subject string) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBus) QueueSubscribe(subject string, queue string, handler types.MsgHandler) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBus) QueueSubscribeSync(subject, queue string) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBus) ChanSubscribe(subject string, ch chan *types.Msg) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBus) EventStream() (types.EventStream, error) {
	return nil, nil
}

func (m *mockEventBus) SetRuntimeContext(ctx context.Context) {
	// Mock implementation - no-op for tests
}

func TestRegisterRequestReplyService(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "test-module"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("register valid RequestReply service", func(t *testing.T) {
		// Set EventBus before registering
		eventBus := &mockEventBus{}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return []byte("response"), nil
		}

		err := container.RegisterRequestReplyService("test-service", handler)
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}

		if !container.Has("test-service") {
			t.Error("Service should be registered")
		}

		// Verify subject computation
		entries := container.Entries()
		if len(entries) != 1 {
			t.Fatalf("Expected 1 entry, got %d", len(entries))
		}

		expected := "services.test-module.test-service"
		if entries[0].Subject != expected {
			t.Errorf("Expected subject %q, got %q", expected, entries[0].Subject)
		}

		// Verify QueueGroup is set to service name
		if entries[0].QueueGroup != "test-service" {
			t.Errorf("Expected QueueGroup 'test-service', got %q", entries[0].QueueGroup)
		}
	})

	t.Run("register with nil handler", func(t *testing.T) {
		err := container.RegisterRequestReplyService("nil-handler", nil)
		if err == nil {
			t.Error("RegisterRequestReplyService should fail with nil handler")
		}
	})

	t.Run("register duplicate service", func(t *testing.T) {
		// Set EventBus before registering
		eventBus := &mockEventBus{}
		container.SetEventBus(eventBus)

		handler1 := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return []byte("response1"), nil
		}
		handler2 := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return []byte("response2"), nil
		}

		err := container.RegisterRequestReplyService("duplicate", handler1)
		if err != nil {
			t.Fatalf("First RegisterRequestReplyService failed: %v", err)
		}

		err = container.RegisterRequestReplyService("duplicate", handler2)
		if err == nil {
			t.Error("RegisterRequestReplyService should fail with duplicate name")
		}
	})

	t.Run("register without bound module", func(t *testing.T) {
		unboundContainer := NewServiceContainer(logger).(*serviceContainer)

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return []byte("response"), nil
		}

		err := unboundContainer.RegisterRequestReplyService("unbound-test", handler)
		if err == nil {
			t.Error("RegisterRequestReplyService should fail without bound module")
		}
	})
}

func TestGetRequestReplyService(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "inventory"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Set EventBus
	eventBus := &mockEventBus{}
	container.SetEventBus(eventBus)

	t.Run("get existing service", func(t *testing.T) {
		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return []byte("response"), nil
		}

		err := container.RegisterRequestReplyService("check-stock", handler)
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}

		client, err := container.GetRequestReplyService("check-stock")
		if err != nil {
			t.Fatalf("GetRequestReplyService failed: %v", err)
		}

		if client == nil {
			t.Error("GetRequestReplyService returned nil client")
		}
	})

	t.Run("get non-existent service", func(t *testing.T) {
		_, err := container.GetRequestReplyService("non-existent")
		if err == nil {
			t.Error("GetRequestReplyService should fail for non-existent service")
		}
	})

	t.Run("register without EventBus should fail", func(t *testing.T) {
		containerNoEB := NewServiceContainer(logger).(*serviceContainer)
		moduleNoEB := &mockModule{name: "test-module"}
		err := containerNoEB.BindModule(moduleNoEB)
		if err != nil {
			t.Fatalf("BindModule failed: %v", err)
		}

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return []byte("response"), nil
		}
		err = containerNoEB.RegisterRequestReplyService("test-service", handler)
		if err == nil {
			t.Error("RegisterRequestReplyService should fail without EventBus")
		}
	})
}

func TestRequestReplyClientCall(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "inventory"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("successful call", func(t *testing.T) {
		eventBus := &mockEventBus{
			requestHandler: func(subject string, data []byte, timeout time.Duration) (*types.Msg, error) {
				if subject != "services.inventory.check-stock" {
					t.Errorf("Expected subject %q, got %q", "services.inventory.check-stock", subject)
				}
				return &types.Msg{Data: []byte("in-stock")}, nil
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return req.Data, nil
		}
		err := container.RegisterRequestReplyService("check-stock", handler)
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}

		client, err := container.GetRequestReplyService("check-stock")
		if err != nil {
			t.Fatalf("GetRequestReplyService failed: %v", err)
		}

		ctx := context.Background()
		response, err := client.Call(ctx, []byte("product-123"))
		if err != nil {
			t.Fatalf("Call failed: %v", err)
		}

		if string(response.Data) != "in-stock" {
			t.Errorf("Expected %q, got %q", "in-stock", string(response.Data))
		}
	})

	t.Run("call with timeout", func(t *testing.T) {
		eventBus := &mockEventBus{
			requestHandler: func(subject string, data []byte, timeout time.Duration) (*types.Msg, error) {
				if timeout <= 0 {
					t.Error("Expected positive timeout")
				}
				return &types.Msg{Data: []byte("response")}, nil
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return req.Data, nil
		}
		err := container.RegisterRequestReplyService("timeout-test", handler)
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}

		client, err := container.GetRequestReplyService("timeout-test")
		if err != nil {
			t.Fatalf("GetRequestReplyService failed: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = client.Call(ctx, []byte("data"))
		if err != nil {
			t.Fatalf("Call failed: %v", err)
		}
	})

	t.Run("call with expired context", func(t *testing.T) {
		eventBus := &mockEventBus{}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return req.Data, nil
		}
		err := container.RegisterRequestReplyService("expired-ctx", handler)
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}

		client, err := container.GetRequestReplyService("expired-ctx")
		if err != nil {
			t.Fatalf("GetRequestReplyService failed: %v", err)
		}

		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		_, err = client.Call(ctx, []byte("data"))
		if err == nil {
			t.Error("Call should fail with expired context")
		}
	})

	t.Run("call with error from EventBus", func(t *testing.T) {
		eventBus := &mockEventBus{
			requestHandler: func(subject string, data []byte, timeout time.Duration) (*types.Msg, error) {
				return nil, errors.New("NATS connection failed")
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return req.Data, nil
		}
		err := container.RegisterRequestReplyService("error-test", handler)
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}

		client, err := container.GetRequestReplyService("error-test")
		if err != nil {
			t.Fatalf("GetRequestReplyService failed: %v", err)
		}

		ctx := context.Background()
		_, err = client.Call(ctx, []byte("data"))
		if err == nil {
			t.Error("Call should fail when EventBus returns error")
		}
	})
}

func TestRequestReplyClientCallMsg(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "inventory"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	eventBus := &mockEventBus{
		requestMsgWithCtxHandler: func(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
			return &types.Msg{
				Data:   msg.Data,
				Header: map[string][]string{"Response-Header": {"value"}},
			}, nil
		},
	}
	container.SetEventBus(eventBus)

	handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
		return req.Data, nil
	}
	err = container.RegisterRequestReplyService("msg-test", handler)
	if err != nil {
		t.Fatalf("RegisterRequestReplyService failed: %v", err)
	}

	client, err := container.GetRequestReplyService("msg-test")
	if err != nil {
		t.Fatalf("GetRequestReplyService failed: %v", err)
	}

	ctx := context.Background()
	msg := &types.Msg{
		Data:   []byte("request-data"),
		Header: map[string][]string{"Request-Header": {"value"}},
	}

	response, err := client.CallMsg(ctx, msg)
	if err != nil {
		t.Fatalf("CallMsg failed: %v", err)
	}

	if string(response.Data) != "request-data" {
		t.Errorf("Expected %q, got %q", "request-data", string(response.Data))
	}

	if response.Header == nil {
		t.Error("Response should have headers")
	}
}

func TestRequestReplyClientCallMsgTransmitsHeaders(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "inventory"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	var capturedMsg *types.Msg
	eventBus := &mockEventBus{
		requestMsgWithCtxHandler: func(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
			capturedMsg = msg
			return &types.Msg{Data: []byte("response")}, nil
		},
	}
	container.SetEventBus(eventBus)

	handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
		return req.Data, nil
	}
	err = container.RegisterRequestReplyService("header-test", handler)
	if err != nil {
		t.Fatalf("RegisterRequestReplyService failed: %v", err)
	}

	client, err := container.GetRequestReplyService("header-test")
	if err != nil {
		t.Fatalf("GetRequestReplyService failed: %v", err)
	}

	ctx := context.Background()
	msg := &types.Msg{
		Subject: "ignored-subject", // Should be overridden
		Data:    []byte("request-data"),
		Header: types.Header{
			"X-Request-ID": []string{"req-123"},
			"X-Trace-ID":   []string{"trace-456"},
		},
	}

	_, err = client.CallMsg(ctx, msg)
	if err != nil {
		t.Fatalf("CallMsg failed: %v", err)
	}

	// Verify subject was overridden
	expectedSubject := "services.inventory.header-test"
	if capturedMsg.Subject != expectedSubject {
		t.Errorf("Expected subject %q, got %q", expectedSubject, capturedMsg.Subject)
	}

	// Verify headers were transmitted
	if capturedMsg.Header["X-Request-ID"][0] != "req-123" {
		t.Errorf("Expected X-Request-ID 'req-123', got %v", capturedMsg.Header["X-Request-ID"])
	}
	if capturedMsg.Header["X-Trace-ID"][0] != "trace-456" {
		t.Errorf("Expected X-Trace-ID 'trace-456', got %v", capturedMsg.Header["X-Trace-ID"])
	}
}

func TestRequestReplyClientNoResponders(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "inventory"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("call returns ServiceError when no responders available", func(t *testing.T) {
		eventBus := &mockEventBus{
			requestHandler: func(subject string, data []byte, timeout time.Duration) (*types.Msg, error) {
				// Simulate NoRespondersError from EventBus (which wraps nats.ErrNoResponders)
				return nil, monoerrors.WrapServiceUnavailable("check-stock", "inventory", types.ServiceTypeRequestReply, errors.New("nats: no responders available for request"))
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return req.Data, nil
		}
		err := container.RegisterRequestReplyService("no-responders-test", handler)
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}

		client, err := container.GetRequestReplyService("no-responders-test")
		if err != nil {
			t.Fatalf("GetRequestReplyService failed: %v", err)
		}

		ctx := context.Background()
		_, err = client.Call(ctx, []byte("data"))
		if err == nil {
			t.Fatal("Call should fail when no responders available")
		}

		// Verify it wraps ErrServiceUnavailable
		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Errorf("expected ErrServiceUnavailable, got %v", err)
		}

		// Verify it's a ServiceError
		if !monoerrors.IsServiceError(err) {
			t.Errorf("expected ServiceError, got %T: %v", err, err)
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

	t.Run("CallMsg also propagates ServiceError", func(t *testing.T) {
		eventBus := &mockEventBus{
			requestMsgWithCtxHandler: func(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
				return nil, monoerrors.WrapServiceUnavailable("callmsg-test", "inventory", types.ServiceTypeRequestReply, errors.New("nats: no responders available for request"))
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return req.Data, nil
		}
		err := container.RegisterRequestReplyService("callmsg-no-responders", handler)
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}

		client, err := container.GetRequestReplyService("callmsg-no-responders")
		if err != nil {
			t.Fatalf("GetRequestReplyService failed: %v", err)
		}

		ctx := context.Background()
		msg := &types.Msg{Data: []byte("request-data")}
		_, err = client.CallMsg(ctx, msg)
		if err == nil {
			t.Fatal("CallMsg should fail when no responders available")
		}

		// Verify it wraps ErrServiceUnavailable
		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Errorf("expected ErrServiceUnavailable, got %v", err)
		}

		// Verify it's a ServiceError
		if !monoerrors.IsServiceError(err) {
			t.Errorf("expected ServiceError, got %T: %v", err, err)
		}
	})
}

// TestRequestReplyClientCallWithContextCancellation tests context cancellation during request-reply
func TestRequestReplyClientCallWithContextCancellation(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "inventory"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("call respects context cancellation", func(t *testing.T) {
		callStarted := make(chan struct{})
		eventBus := &mockEventBus{
			requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
				close(callStarted)
				// Simulate slow response
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(5 * time.Second):
					return &types.Msg{Data: []byte("response")}, nil
				}
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return req.Data, nil
		}
		err := container.RegisterRequestReplyService("cancel-test", handler)
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}

		client, err := container.GetRequestReplyService("cancel-test")
		if err != nil {
			t.Fatalf("GetRequestReplyService failed: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err = client.Call(ctx, []byte("data"))
		if err == nil {
			t.Error("Call should fail when context times out")
		}

		// Should get a timeout error
		if !monoerrors.IsTimeoutError(err) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected timeout error or context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("callmsg respects context cancellation", func(t *testing.T) {
		callStarted := make(chan struct{})
		eventBus := &mockEventBus{
			requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
				close(callStarted)
				// Simulate slow response
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(5 * time.Second):
					return &types.Msg{Data: []byte("response")}, nil
				}
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return req.Data, nil
		}
		err := container.RegisterRequestReplyService("cancel-test-msg", handler)
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}

		client, err := container.GetRequestReplyService("cancel-test-msg")
		if err != nil {
			t.Fatalf("GetRequestReplyService failed: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		msg := &types.Msg{Data: []byte("data")}
		_, err = client.CallMsg(ctx, msg)
		if err == nil {
			t.Error("CallMsg should fail when context times out")
		}

		// Should get a timeout error
		if !monoerrors.IsTimeoutError(err) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected timeout error or context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("call applies default timeout when context has no deadline", func(t *testing.T) {
		capturedTimeout := time.Duration(0)
		eventBus := &mockEventBus{
			requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
				// Capture the timeout from context
				if deadline, ok := ctx.Deadline(); ok {
					capturedTimeout = time.Until(deadline)
				}
				return &types.Msg{Data: []byte("response")}, nil
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return req.Data, nil
		}
		err := container.RegisterRequestReplyService("default-timeout-test", handler)
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}

		client, err := container.GetRequestReplyService("default-timeout-test")
		if err != nil {
			t.Fatalf("GetRequestReplyService failed: %v", err)
		}

		// Use context.Background() which has no deadline
		ctx := context.Background()
		_, err = client.Call(ctx, []byte("data"))
		if err != nil {
			t.Fatalf("Call failed: %v", err)
		}

		// Should have applied default 30s timeout (allow 1s tolerance)
		expectedTimeout := 30 * time.Second
		if capturedTimeout < 29*time.Second || capturedTimeout > 31*time.Second {
			t.Errorf("expected timeout around %v, got %v", expectedTimeout, capturedTimeout)
		}
	})
}

// TestRequestReplyRunMiddleware tests the runMiddleware function for request-reply clients
func TestRequestReplyRunMiddleware(t *testing.T) {
	t.Run("with nil middleware", func(t *testing.T) {
		client := &requestReplyClient{
			middlewareChain: nil,
			serviceName:     "test-service",
			moduleName:      "test-module",
			subject:         "test.subject",
		}

		msg := &types.Msg{
			Subject: "test.subject",
			Data:    []byte("test data"),
			Header:  types.Header{"Original": []string{"value"}},
		}

		result := client.runMiddleware(context.Background(), msg)

		// Should return the same message unchanged
		if result != msg {
			t.Error("should return same message when middleware is nil")
		}
	})

	t.Run("with middleware that injects headers", func(t *testing.T) {
		mockChain := &mockMiddlewareChainRunner{
			injectHeaders: map[string][]string{
				"X-Injected": {"test-value"},
			},
		}

		client := &requestReplyClient{
			middlewareChain: mockChain,
			serviceName:     "test-service",
			moduleName:      "test-module",
			subject:         "test.subject",
		}

		msg := &types.Msg{
			Subject: "test.subject",
			Data:    []byte("test data"),
			Header:  make(types.Header),
		}

		result := client.runMiddleware(context.Background(), msg)

		if !mockChain.outgoingMessageCalled {
			t.Error("middleware should have been called")
		}

		if len(result.Header["X-Injected"]) == 0 {
			t.Error("middleware should have injected header")
		}
	})
}

// Additional error case tests for GetRequestReplyService
func TestGetRequestReplyServiceErrors(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "test"}
	container.BindModule(module)
	container.SetEventBus(&mockEventBus{})

	t.Run("wrong service type", func(t *testing.T) {
		// Register a channel service
		in := make(chan *types.Msg, 1)
		out := make(chan *types.Msg, 1)
		container.RegisterChannelService("channel-svc", in, out)

		// Try to get it as RequestReply
		_, err := container.GetRequestReplyService("channel-svc")
		if err == nil {
			t.Error("should fail when service is wrong type")
		}

		close(in)
		close(out)
	})

	t.Run("EventBus not available", func(t *testing.T) {
		containerNoEB := NewServiceContainer(logger).(*serviceContainer)
		moduleNoEB := &mockModule{name: "test2"}
		containerNoEB.BindModule(moduleNoEB)
		containerNoEB.SetEventBus(&mockEventBus{})

		// Register service
		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return []byte("ok"), nil
		}
		containerNoEB.RegisterRequestReplyService("svc", handler)

		// Clear EventBus
		containerNoEB.eventBus = nil

		// Try to get service
		_, err := containerNoEB.GetRequestReplyService("svc")
		if err == nil {
			t.Error("should fail when EventBus is nil")
		}
	})
}
