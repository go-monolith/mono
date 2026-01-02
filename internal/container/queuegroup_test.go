package container

import (
	"context"
	"errors"
	"testing"
	"time"

	monoerrors "github.com/go-monolith/mono/v1/pkg/errors"
	"github.com/go-monolith/mono/v1/pkg/types"
)

func TestRegisterQueueGroupService(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "test-module"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("register valid QueueGroup service", func(t *testing.T) {
		// Set EventBus before registering
		eventBus := &mockEventBus{}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}

		err := container.RegisterQueueGroupService("test-service",
			types.QGHP{QueueGroup: "workers", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		if !container.Has("test-service") {
			t.Error("Service should be registered")
		}

		// Verify subject computation and queue handlers
		entries := container.Entries()
		if len(entries) != 1 {
			t.Fatalf("Expected 1 entry, got %d", len(entries))
		}

		expected := "services.test-module.test-service"
		if entries[0].Subject != expected {
			t.Errorf("Expected subject %q, got %q", expected, entries[0].Subject)
		}

		if len(entries[0].QueueHandlers) != 1 {
			t.Fatalf("Expected 1 queue handler, got %d", len(entries[0].QueueHandlers))
		}

		if entries[0].QueueHandlers[0].QueueGroup != "workers" {
			t.Errorf("Expected queue group %q, got %q", "workers", entries[0].QueueHandlers[0].QueueGroup)
		}
	})

	t.Run("register with empty queue group", func(t *testing.T) {
		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err := container.RegisterQueueGroupService("empty-queue",
			types.QGHP{QueueGroup: "", Handler: handler},
		)
		if err == nil {
			t.Error("RegisterQueueGroupService should fail with empty queue group")
		}
	})

	t.Run("register with nil handler", func(t *testing.T) {
		err := container.RegisterQueueGroupService("nil-handler",
			types.QGHP{QueueGroup: "workers", Handler: nil},
		)
		if err == nil {
			t.Error("RegisterQueueGroupService should fail with nil handler")
		}
	})

	t.Run("register with no pairs", func(t *testing.T) {
		err := container.RegisterQueueGroupService("no-pairs")
		if err == nil {
			t.Error("RegisterQueueGroupService should fail with no pairs")
		}
	})

	t.Run("register with multiple pairs", func(t *testing.T) {
		// Set EventBus before registering
		eventBus := &mockEventBus{}
		container.SetEventBus(eventBus)

		highHandler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		lowHandler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}

		err := container.RegisterQueueGroupService("multi-priority",
			types.QGHP{QueueGroup: "high-workers", Handler: highHandler},
			types.QGHP{QueueGroup: "low-workers", Handler: lowHandler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService with multiple pairs failed: %v", err)
		}

		if !container.Has("multi-priority") {
			t.Error("Service should be registered")
		}

		entries := container.Entries()
		var found *types.ServiceEntry
		for i := range entries {
			if entries[i].Name == "multi-priority" {
				found = entries[i]
				break
			}
		}

		if found == nil {
			t.Fatal("Service entry not found")
		}

		if len(found.QueueHandlers) != 2 {
			t.Errorf("Expected 2 queue handlers, got %d", len(found.QueueHandlers))
		}
	})

	t.Run("register with duplicate queue groups", func(t *testing.T) {
		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err := container.RegisterQueueGroupService("dup-groups",
			types.QGHP{QueueGroup: "workers", Handler: handler},
			types.QGHP{QueueGroup: "workers", Handler: handler},
		)
		if err == nil {
			t.Error("RegisterQueueGroupService should fail with duplicate queue groups")
		}
	})

	t.Run("register duplicate service", func(t *testing.T) {
		// Set EventBus before registering
		eventBus := &mockEventBus{}
		container.SetEventBus(eventBus)

		handler1 := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		handler2 := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}

		err := container.RegisterQueueGroupService("duplicate",
			types.QGHP{QueueGroup: "workers", Handler: handler1},
		)
		if err != nil {
			t.Fatalf("First RegisterQueueGroupService failed: %v", err)
		}

		err = container.RegisterQueueGroupService("duplicate",
			types.QGHP{QueueGroup: "workers", Handler: handler2},
		)
		if err == nil {
			t.Error("RegisterQueueGroupService should fail with duplicate name")
		}
	})
}

func TestGetQueueGroupService(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "orders"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Set EventBus
	eventBus := &mockEventBus{}
	container.SetEventBus(eventBus)

	t.Run("get existing service", func(t *testing.T) {
		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}

		err := container.RegisterQueueGroupService("process-order",
			types.QGHP{QueueGroup: "order-workers", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		client, err := container.GetQueueGroupService("process-order")
		if err != nil {
			t.Fatalf("GetQueueGroupService failed: %v", err)
		}

		if client == nil {
			t.Error("GetQueueGroupService returned nil client")
		}
	})

	t.Run("get non-existent service", func(t *testing.T) {
		_, err := container.GetQueueGroupService("non-existent")
		if err == nil {
			t.Error("GetQueueGroupService should fail for non-existent service")
		}
	})

	t.Run("register without EventBus should fail", func(t *testing.T) {
		containerNoEB := NewServiceContainer(logger).(*serviceContainer)
		moduleNoEB := &mockModule{name: "test-module"}
		err := containerNoEB.BindModule(moduleNoEB)
		if err != nil {
			t.Fatalf("BindModule failed: %v", err)
		}

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err = containerNoEB.RegisterQueueGroupService("test-service",
			types.QGHP{QueueGroup: "workers", Handler: handler},
		)
		if err == nil {
			t.Error("RegisterQueueGroupService should fail without EventBus")
		}
	})
}

func TestQueueGroupClientSend(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "orders"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("successful send", func(t *testing.T) {
		// Track if request was made
		var requestedSubject string
		var requestedData []byte

		eventBus := &mockEventBus{
			requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
				requestedSubject = subject
				requestedData = data
				// Return empty ACK
				return &types.Msg{Data: []byte{}}, nil
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err := container.RegisterQueueGroupService("process-order",
			types.QGHP{QueueGroup: "order-workers", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		client, err := container.GetQueueGroupService("process-order")
		if err != nil {
			t.Fatalf("GetQueueGroupService failed: %v", err)
		}

		ctx := context.Background()
		data := []byte("order-data")
		err = client.Send(ctx, data)
		if err != nil {
			t.Fatalf("Send failed: %v", err)
		}

		// Verify the request was made with correct subject and data
		expectedSubject := "services.orders.process-order"
		if requestedSubject != expectedSubject {
			t.Errorf("Expected subject %q, got %q", expectedSubject, requestedSubject)
		}
		if string(requestedData) != string(data) {
			t.Errorf("Expected data %q, got %q", string(data), string(requestedData))
		}
	})

	t.Run("send with error from EventBus", func(t *testing.T) {
		eventBus := &mockEventBus{
			requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
				return nil, errors.New("NATS request failed")
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err := container.RegisterQueueGroupService("error-test",
			types.QGHP{QueueGroup: "workers", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		client, err := container.GetQueueGroupService("error-test")
		if err != nil {
			t.Fatalf("GetQueueGroupService failed: %v", err)
		}

		ctx := context.Background()
		err = client.Send(ctx, []byte("data"))
		if err == nil {
			t.Error("Send should fail when EventBus returns error")
		}
	})

	t.Run("send with no responders", func(t *testing.T) {
		eventBus := &mockEventBus{
			requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
				// Simulate no responders error (this would normally be wrapped by eventbus)
				return nil, monoerrors.WrapServiceUnavailable("no-responders-test", "orders", types.ServiceTypeQueueGroup, errors.New("nats: no responders available for request"))
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err := container.RegisterQueueGroupService("no-responders-test",
			types.QGHP{QueueGroup: "workers", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		client, err := container.GetQueueGroupService("no-responders-test")
		if err != nil {
			t.Fatalf("GetQueueGroupService failed: %v", err)
		}

		ctx := context.Background()
		err = client.Send(ctx, []byte("data"))
		if err == nil {
			t.Fatal("Send should fail when no responders available")
		}

		// Verify it wraps ErrServiceUnavailable
		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Errorf("expected ErrServiceUnavailable, got %v", err)
		}
	})
}

func TestQueueGroupClientSendMsg(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "orders"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Track if request was made
	var requestedSubject string
	var requestedData []byte

	eventBus := &mockEventBus{
		requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
			requestedSubject = subject
			requestedData = data
			// Return empty ACK
			return &types.Msg{Data: []byte{}}, nil
		},
	}
	container.SetEventBus(eventBus)

	handler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}
	err = container.RegisterQueueGroupService("msg-test",
		types.QGHP{QueueGroup: "workers", Handler: handler},
	)
	if err != nil {
		t.Fatalf("RegisterQueueGroupService failed: %v", err)
	}

	client, err := container.GetQueueGroupService("msg-test")
	if err != nil {
		t.Fatalf("GetQueueGroupService failed: %v", err)
	}

	ctx := context.Background()
	msg := &types.Msg{
		Data:   []byte("order-data"),
		Header: map[string][]string{"Priority": {"high"}},
	}

	err = client.SendMsg(ctx, msg)
	if err != nil {
		t.Fatalf("SendMsg failed: %v", err)
	}

	// Verify the request was made with correct subject and data
	expectedSubject := "services.orders.msg-test"
	if requestedSubject != expectedSubject {
		t.Errorf("Expected subject %q, got %q", expectedSubject, requestedSubject)
	}
	if string(requestedData) != string(msg.Data) {
		t.Errorf("Expected data %q, got %q", string(msg.Data), string(requestedData))
	}
}

func TestQueueGroupSendMsgTransmitsHeaders(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "orders"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	var capturedMsg *types.Msg
	eventBus := &mockEventBus{
		requestMsgWithCtxHandler: func(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
			capturedMsg = msg
			return &types.Msg{Data: []byte{}}, nil
		},
	}
	container.SetEventBus(eventBus)

	handler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}
	err = container.RegisterQueueGroupService("header-test",
		types.QGHP{QueueGroup: "workers", Handler: handler},
	)
	if err != nil {
		t.Fatalf("RegisterQueueGroupService failed: %v", err)
	}

	client, err := container.GetQueueGroupService("header-test")
	if err != nil {
		t.Fatalf("GetQueueGroupService failed: %v", err)
	}

	ctx := context.Background()
	msg := &types.Msg{
		Subject: "ignored-subject",
		Data:    []byte("order-data"),
		Header: types.Header{
			"Priority":   []string{"high"},
			"X-Order-ID": []string{"order-789"},
		},
	}

	err = client.SendMsg(ctx, msg)
	if err != nil {
		t.Fatalf("SendMsg failed: %v", err)
	}

	// Verify subject was overridden
	expectedSubject := "services.orders.header-test"
	if capturedMsg.Subject != expectedSubject {
		t.Errorf("Expected subject %q, got %q", expectedSubject, capturedMsg.Subject)
	}

	// Verify headers were transmitted
	if capturedMsg.Header["Priority"][0] != "high" {
		t.Errorf("Expected Priority 'high', got %v", capturedMsg.Header["Priority"])
	}
	if capturedMsg.Header["X-Order-ID"][0] != "order-789" {
		t.Errorf("Expected X-Order-ID 'order-789', got %v", capturedMsg.Header["X-Order-ID"])
	}
}

func TestQueueGroupAckMechanism(t *testing.T) {
	// Verify that Send uses request-reply for ACK
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "orders"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	requestCalled := false
	eventBus := &mockEventBus{
		requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
			requestCalled = true
			// Return empty ACK
			return &types.Msg{Data: []byte{}}, nil
		},
	}
	container.SetEventBus(eventBus)

	handler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}
	err = container.RegisterQueueGroupService("ack-test",
		types.QGHP{QueueGroup: "workers", Handler: handler},
	)
	if err != nil {
		t.Fatalf("RegisterQueueGroupService failed: %v", err)
	}

	client, err := container.GetQueueGroupService("ack-test")
	if err != nil {
		t.Fatalf("GetQueueGroupService failed: %v", err)
	}

	ctx := context.Background()

	// Send should use RequestWithContext for ACK
	err = client.Send(ctx, []byte("data"))
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Verify request was made (for ACK)
	if !requestCalled {
		t.Error("QueueGroup should use RequestWithContext for ACK")
	}
}

func TestQueueGroupSmartSwitching(t *testing.T) {
	t.Run("first send uses ACK mode", func(t *testing.T) {
		logger := &mockLogger{}
		container := NewServiceContainer(logger).(*serviceContainer)
		module := &mockModule{name: "orders"}

		err := container.BindModule(module)
		if err != nil {
			t.Fatalf("BindModule failed: %v", err)
		}

		// Set optimistic window to 1 second
		container.SetQueueGroupOptimisticWindow(1 * time.Second)

		requestCalled := false
		publishCalled := false

		eventBus := &mockEventBus{
			requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
				requestCalled = true
				return &types.Msg{Data: []byte{}}, nil
			},
			publishFunc: func(subject string, data []byte) error {
				publishCalled = true
				return nil
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err = container.RegisterQueueGroupService("test-service",
			types.QGHP{QueueGroup: "workers", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		client, err := container.GetQueueGroupService("test-service")
		if err != nil {
			t.Fatalf("GetQueueGroupService failed: %v", err)
		}

		ctx := context.Background()

		// First send should use ACK (request-reply)
		err = client.Send(ctx, []byte("first"))
		if err != nil {
			t.Fatalf("Send failed: %v", err)
		}

		if !requestCalled {
			t.Error("First send should use RequestWithContext (ACK mode)")
		}
		if publishCalled {
			t.Error("First send should not use Publish")
		}
	})

	t.Run("second send within window uses publish", func(t *testing.T) {
		logger := &mockLogger{}
		container := NewServiceContainer(logger).(*serviceContainer)
		module := &mockModule{name: "orders"}

		err := container.BindModule(module)
		if err != nil {
			t.Fatalf("BindModule failed: %v", err)
		}

		// Set optimistic window to 1 second
		container.SetQueueGroupOptimisticWindow(1 * time.Second)

		requestCount := 0
		publishCount := 0

		eventBus := &mockEventBus{
			requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
				requestCount++
				return &types.Msg{Data: []byte{}}, nil
			},
			publishFunc: func(subject string, data []byte) error {
				publishCount++
				return nil
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err = container.RegisterQueueGroupService("test-service",
			types.QGHP{QueueGroup: "workers", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		client, err := container.GetQueueGroupService("test-service")
		if err != nil {
			t.Fatalf("GetQueueGroupService failed: %v", err)
		}

		ctx := context.Background()

		// First send uses ACK
		err = client.Send(ctx, []byte("first"))
		if err != nil {
			t.Fatalf("First send failed: %v", err)
		}

		// Second send within window should use publish
		time.Sleep(100 * time.Millisecond) // Small delay but within 1s window
		err = client.Send(ctx, []byte("second"))
		if err != nil {
			t.Fatalf("Second send failed: %v", err)
		}

		if requestCount != 1 {
			t.Errorf("Expected 1 request call, got %d", requestCount)
		}
		if publishCount != 1 {
			t.Errorf("Expected 1 publish call, got %d", publishCount)
		}
	})

	t.Run("send after window expires uses ACK again", func(t *testing.T) {
		logger := &mockLogger{}
		container := NewServiceContainer(logger).(*serviceContainer)
		module := &mockModule{name: "orders"}

		err := container.BindModule(module)
		if err != nil {
			t.Fatalf("BindModule failed: %v", err)
		}

		// Set optimistic window to 200ms (short for testing)
		container.SetQueueGroupOptimisticWindow(200 * time.Millisecond)

		requestCount := 0
		publishCount := 0

		eventBus := &mockEventBus{
			requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
				requestCount++
				return &types.Msg{Data: []byte{}}, nil
			},
			publishFunc: func(subject string, data []byte) error {
				publishCount++
				return nil
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err = container.RegisterQueueGroupService("test-service",
			types.QGHP{QueueGroup: "workers", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		client, err := container.GetQueueGroupService("test-service")
		if err != nil {
			t.Fatalf("GetQueueGroupService failed: %v", err)
		}

		ctx := context.Background()

		// First send uses ACK
		err = client.Send(ctx, []byte("first"))
		if err != nil {
			t.Fatalf("First send failed: %v", err)
		}

		// Second send within window uses publish
		time.Sleep(50 * time.Millisecond)
		err = client.Send(ctx, []byte("second"))
		if err != nil {
			t.Fatalf("Second send failed: %v", err)
		}

		// Wait for window to expire
		time.Sleep(200 * time.Millisecond)

		// Third send after window should use ACK again
		err = client.Send(ctx, []byte("third"))
		if err != nil {
			t.Fatalf("Third send failed: %v", err)
		}

		if requestCount != 2 {
			t.Errorf("Expected 2 request calls (first and after window), got %d", requestCount)
		}
		if publishCount != 1 {
			t.Errorf("Expected 1 publish call (within window), got %d", publishCount)
		}
	})

	t.Run("publish failure resets to ACK mode", func(t *testing.T) {
		logger := &mockLogger{}
		container := NewServiceContainer(logger).(*serviceContainer)
		module := &mockModule{name: "orders"}

		err := container.BindModule(module)
		if err != nil {
			t.Fatalf("BindModule failed: %v", err)
		}

		// Set optimistic window to 1 second
		container.SetQueueGroupOptimisticWindow(1 * time.Second)

		requestCount := 0
		publishCount := 0
		publishShouldFail := false

		eventBus := &mockEventBus{
			requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
				requestCount++
				return &types.Msg{Data: []byte{}}, nil
			},
			publishFunc: func(subject string, data []byte) error {
				publishCount++
				if publishShouldFail {
					return errors.New("publish failed")
				}
				return nil
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err = container.RegisterQueueGroupService("test-service",
			types.QGHP{QueueGroup: "workers", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		client, err := container.GetQueueGroupService("test-service")
		if err != nil {
			t.Fatalf("GetQueueGroupService failed: %v", err)
		}

		ctx := context.Background()

		// First send uses ACK
		err = client.Send(ctx, []byte("first"))
		if err != nil {
			t.Fatalf("First send failed: %v", err)
		}

		// Second send within window should use publish
		time.Sleep(50 * time.Millisecond)
		err = client.Send(ctx, []byte("second"))
		if err != nil {
			t.Fatalf("Second send failed: %v", err)
		}

		// Third send - publish will fail
		publishShouldFail = true
		time.Sleep(50 * time.Millisecond)
		err = client.Send(ctx, []byte("third"))
		if err == nil {
			t.Fatal("Third send should fail when publish fails")
		}

		// Fourth send should reset to ACK mode after publish failure
		publishShouldFail = false
		time.Sleep(50 * time.Millisecond)
		err = client.Send(ctx, []byte("fourth"))
		if err != nil {
			t.Fatalf("Fourth send failed: %v", err)
		}

		if requestCount != 2 {
			t.Errorf("Expected 2 request calls (first and after publish failure), got %d", requestCount)
		}
		if publishCount != 2 {
			t.Errorf("Expected 2 publish calls (successful and failed), got %d", publishCount)
		}
	})

	t.Run("zero window disables smart switching", func(t *testing.T) {
		logger := &mockLogger{}
		container := NewServiceContainer(logger).(*serviceContainer)
		module := &mockModule{name: "orders"}

		err := container.BindModule(module)
		if err != nil {
			t.Fatalf("BindModule failed: %v", err)
		}

		// Set optimistic window to 0 (disabled)
		container.SetQueueGroupOptimisticWindow(0)

		requestCount := 0
		publishCount := 0

		eventBus := &mockEventBus{
			requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
				requestCount++
				return &types.Msg{Data: []byte{}}, nil
			},
			publishFunc: func(subject string, data []byte) error {
				publishCount++
				return nil
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err = container.RegisterQueueGroupService("test-service",
			types.QGHP{QueueGroup: "workers", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		client, err := container.GetQueueGroupService("test-service")
		if err != nil {
			t.Fatalf("GetQueueGroupService failed: %v", err)
		}

		ctx := context.Background()

		// All sends should use ACK when window is 0
		for i := 0; i < 3; i++ {
			err = client.Send(ctx, []byte("data"))
			if err != nil {
				t.Fatalf("Send %d failed: %v", i+1, err)
			}
			time.Sleep(50 * time.Millisecond)
		}

		if requestCount != 3 {
			t.Errorf("Expected 3 request calls (all using ACK), got %d", requestCount)
		}
		if publishCount != 0 {
			t.Errorf("Expected 0 publish calls (window=0), got %d", publishCount)
		}
	})

	t.Run("SendMsg uses smart switching", func(t *testing.T) {
		logger := &mockLogger{}
		container := NewServiceContainer(logger).(*serviceContainer)
		module := &mockModule{name: "orders"}

		err := container.BindModule(module)
		if err != nil {
			t.Fatalf("BindModule failed: %v", err)
		}

		// Set optimistic window to 1 second
		container.SetQueueGroupOptimisticWindow(1 * time.Second)

		requestCount := 0
		publishCount := 0

		eventBus := &mockEventBus{
			requestMsgWithCtxHandler: func(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
				requestCount++
				return &types.Msg{Data: []byte{}}, nil
			},
			publishMsgFunc: func(msg *types.Msg) error {
				publishCount++
				return nil
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err = container.RegisterQueueGroupService("test-service",
			types.QGHP{QueueGroup: "workers", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		client, err := container.GetQueueGroupService("test-service")
		if err != nil {
			t.Fatalf("GetQueueGroupService failed: %v", err)
		}

		ctx := context.Background()
		msg := &types.Msg{Data: []byte("test")}

		// First SendMsg uses ACK
		err = client.SendMsg(ctx, msg)
		if err != nil {
			t.Fatalf("First SendMsg failed: %v", err)
		}

		// Second SendMsg within window should use publish
		time.Sleep(100 * time.Millisecond)
		err = client.SendMsg(ctx, msg)
		if err != nil {
			t.Fatalf("Second SendMsg failed: %v", err)
		}

		if requestCount != 1 {
			t.Errorf("Expected 1 request call, got %d", requestCount)
		}
		if publishCount != 1 {
			t.Errorf("Expected 1 publish call, got %d", publishCount)
		}
	})
}

// Additional error case tests for GetQueueGroupService
func TestGetQueueGroupServiceErrors(t *testing.T) {
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

		// Try to get it as QueueGroup
		_, err := container.GetQueueGroupService("channel-svc")
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
		handler := func(ctx context.Context, req *types.Msg) error {
			return nil
		}
		containerNoEB.RegisterQueueGroupService("svc", types.QGHP{QueueGroup: "workers", Handler: handler})

		// Clear EventBus
		containerNoEB.eventBus = nil

		// Try to get service
		_, err := containerNoEB.GetQueueGroupService("svc")
		if err == nil {
			t.Error("should fail when EventBus is nil")
		}
	})
}

func TestQueueGroupEnsureContextDeadline(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "test"}
	container.BindModule(module)

	eventBus := &mockEventBus{
		requestWithCtxHandler: func(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
			return &types.Msg{Data: []byte{}}, nil
		},
	}
	container.SetEventBus(eventBus)

	handler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}
	container.RegisterQueueGroupService("test-svc", types.QGHP{QueueGroup: "workers", Handler: handler})

	client, err := container.GetQueueGroupService("test-svc")
	if err != nil {
		t.Fatalf("GetQueueGroupService failed: %v", err)
	}

	qgClient := client.(*queueGroupClient)

	t.Run("context with expired deadline", func(t *testing.T) {
		// Create context with deadline already expired
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Second))
		defer cancel()

		_, _, err := qgClient.ensureContextDeadline(ctx)
		if err == nil {
			t.Error("should fail when deadline is already expired")
		}
		if !monoerrors.IsTimeoutError(err) {
			t.Errorf("expected TimeoutError, got %v", err)
		}
	})

	t.Run("context without deadline adds default timeout", func(t *testing.T) {
		ctx := context.Background()

		newCtx, cancel, err := qgClient.ensureContextDeadline(ctx)
		if err != nil {
			t.Fatalf("should not error with valid context: %v", err)
		}
		if cancel == nil {
			t.Error("should return cancel function when adding deadline")
		}
		defer cancel()

		if _, hasDeadline := newCtx.Deadline(); !hasDeadline {
			t.Error("should have deadline after ensureContextDeadline")
		}
	})

	t.Run("context with valid deadline preserves it", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		newCtx, cancelFunc, err := qgClient.ensureContextDeadline(ctx)
		if err != nil {
			t.Fatalf("should not error with valid context: %v", err)
		}
		if cancelFunc != nil {
			t.Error("should not return cancel function when deadline exists")
		}
		if newCtx != ctx {
			t.Error("should return same context when deadline exists")
		}
	})
}

func TestQueueGroupSendInternalMsgPublishPath(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "test"}
	container.BindModule(module)

	container.SetQueueGroupOptimisticWindow(1 * time.Second)

	t.Run("publish success path", func(t *testing.T) {
		publishMsgCalled := false
		eventBus := &mockEventBus{
			requestMsgWithCtxHandler: func(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
				return &types.Msg{Data: []byte{}}, nil
			},
			publishMsgFunc: func(msg *types.Msg) error {
				publishMsgCalled = true
				return nil
			},
		}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		container.RegisterQueueGroupService("test-svc", types.QGHP{QueueGroup: "workers", Handler: handler})

		client, _ := container.GetQueueGroupService("test-svc")

		// First send to establish ACK
		ctx := context.Background()
		msg1 := &types.Msg{Data: []byte("first")}
		err := client.SendMsg(ctx, msg1)
		if err != nil {
			t.Fatalf("First SendMsg failed: %v", err)
		}

		// Second send within window should use publish path
		time.Sleep(50 * time.Millisecond)
		msg2 := &types.Msg{Data: []byte("second"), Header: make(types.Header)}
		err = client.SendMsg(ctx, msg2)
		if err != nil {
			t.Fatalf("Second SendMsg failed: %v", err)
		}

		if !publishMsgCalled {
			t.Error("should use PublishMsg when within optimistic window")
		}
	})

	t.Run("publish failure resets lastACK", func(t *testing.T) {
		requestCount := 0
		publishShouldFail := false
		eventBus := &mockEventBus{
			requestMsgWithCtxHandler: func(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
				requestCount++
				return &types.Msg{Data: []byte{}}, nil
			},
			publishMsgFunc: func(msg *types.Msg) error {
				if publishShouldFail {
					return errors.New("publish failed")
				}
				return nil
			},
		}

		freshContainer := NewServiceContainer(logger).(*serviceContainer)
		freshModule := &mockModule{name: "fresh"}
		freshContainer.BindModule(freshModule)
		freshContainer.SetEventBus(eventBus)
		freshContainer.SetQueueGroupOptimisticWindow(1 * time.Second)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		freshContainer.RegisterQueueGroupService("fail-test", types.QGHP{QueueGroup: "workers", Handler: handler})

		client, _ := freshContainer.GetQueueGroupService("fail-test")
		ctx := context.Background()

		// First SendMsg to establish ACK
		err := client.SendMsg(ctx, &types.Msg{Data: []byte("first")})
		if err != nil {
			t.Fatalf("First SendMsg failed: %v", err)
		}

		// Second SendMsg within window - publish will fail
		publishShouldFail = true
		time.Sleep(50 * time.Millisecond)
		err = client.SendMsg(ctx, &types.Msg{Data: []byte("second")})
		if err == nil {
			t.Fatal("SendMsg should fail when publish fails")
		}

		// Third SendMsg should use ACK again (lastACK was reset)
		publishShouldFail = false
		time.Sleep(50 * time.Millisecond)
		err = client.SendMsg(ctx, &types.Msg{Data: []byte("third")})
		if err != nil {
			t.Fatalf("Third SendMsg failed: %v", err)
		}

		if requestCount != 2 {
			t.Errorf("Expected 2 request calls (first and after publish failure), got %d", requestCount)
		}
	})

	t.Run("sendInternalMsg with context deadline exceeded", func(t *testing.T) {
		eventBus := &mockEventBus{
			requestMsgWithCtxHandler: func(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
				return nil, context.DeadlineExceeded
			},
		}

		testContainer := NewServiceContainer(logger).(*serviceContainer)
		testModule := &mockModule{name: "timeout-test"}
		testContainer.BindModule(testModule)
		testContainer.SetEventBus(eventBus)

		handler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		testContainer.RegisterQueueGroupService("timeout-svc", types.QGHP{QueueGroup: "workers", Handler: handler})

		client, _ := testContainer.GetQueueGroupService("timeout-svc")
		ctx := context.Background()

		err := client.SendMsg(ctx, &types.Msg{Data: []byte("test")})
		if err == nil {
			t.Fatal("SendMsg should fail with deadline exceeded")
		}

		// Verify error is wrapped as TimeoutError
		if !monoerrors.IsTimeoutError(err) {
			t.Errorf("Expected TimeoutError, got %v", err)
		}
	})
}
