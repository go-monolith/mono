package container

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go/jetstream"
)

// mockEventBusWithJetStream implements types.EventBus for testing StreamConsumer services
type mockEventBusWithJetStream struct {
	jetStream types.EventStream
	jsError   error
}

func (m *mockEventBusWithJetStream) Publish(subject string, data []byte) error {
	return nil
}

func (m *mockEventBusWithJetStream) PublishMsg(msg *types.Msg) error {
	return nil
}

func (m *mockEventBusWithJetStream) Request(subject string, data []byte, timeout time.Duration) (*types.Msg, error) {
	return &types.Msg{Data: []byte("mock-response")}, nil
}

func (m *mockEventBusWithJetStream) RequestWithContext(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
	return &types.Msg{Data: []byte("mock-response")}, nil
}

func (m *mockEventBusWithJetStream) RequestMsgWithContext(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
	return &types.Msg{Data: []byte("mock-response")}, nil
}

func (m *mockEventBusWithJetStream) Subscribe(subject string, handler types.MsgHandler) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBusWithJetStream) SubscribeSync(subject string) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBusWithJetStream) QueueSubscribe(subject, queue string, handler types.MsgHandler) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBusWithJetStream) QueueSubscribeSync(subject, queue string) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBusWithJetStream) ChanSubscribe(subject string, ch chan *types.Msg) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBusWithJetStream) EventStream() (types.EventStream, error) {
	if m.jsError != nil {
		return nil, m.jsError
	}
	return m.jetStream, nil
}

func (m *mockEventBusWithJetStream) SetRuntimeContext(ctx context.Context) {
	// Mock implementation - no-op for tests
}

// mockMsgPubAck implements types.MsgPubAck interface for testing
type mockMsgPubAck struct {
	stream    string
	sequence  uint64
	duplicate bool
	domain    string
}

func (m *mockMsgPubAck) Stream() string   { return m.stream }
func (m *mockMsgPubAck) Sequence() uint64 { return m.sequence }
func (m *mockMsgPubAck) Duplicate() bool  { return m.duplicate }
func (m *mockMsgPubAck) Domain() string   { return m.domain }

// mockJetStream implements types.EventStream interface for testing
type mockJetStream struct {
	publishedSubjects []string
	publishedData     [][]byte
	publishFunc       func(ctx context.Context, subject string, data []byte) (types.MsgPubAck, error)
}

func (m *mockJetStream) Publish(ctx context.Context, subject string, data []byte) (types.MsgPubAck, error) {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, subject, data)
	}
	m.publishedSubjects = append(m.publishedSubjects, subject)
	m.publishedData = append(m.publishedData, data)
	return &mockMsgPubAck{stream: "TEST_STREAM", sequence: 1}, nil
}

func (m *mockJetStream) PublishMsg(ctx context.Context, msg *types.Msg) (types.MsgPubAck, error) {
	return m.Publish(ctx, msg.Subject, msg.Data)
}

func (m *mockJetStream) CreateOrUpdateStream(ctx context.Context, cfg types.StreamConfig) (jetstream.Stream, error) {
	return nil, nil
}

func (m *mockJetStream) CreateOrUpdateConsumer(ctx context.Context, streamName string, cfg types.ConsumerConfig) (jetstream.Consumer, error) {
	return nil, nil
}

func (m *mockJetStream) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	return nil, nil
}

func (m *mockJetStream) DeleteStream(ctx context.Context, name string) error {
	return nil
}

func TestRegisterStreamConsumerService(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "test-module"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("register valid StreamConsumer service", func(t *testing.T) {
		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "ORDERS",
				Subjects: []string{"orders.>"},
			},
			Consumer: types.ConsumerConfig{
				FilterSubject: "orders.new",
				AckWait:       60 * time.Second,
			},
			Fetch: types.FetchConfig{
				BatchSize: 20,
			},
		}

		err := container.RegisterStreamConsumerService("order-processor", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		if !container.Has("order-processor") {
			t.Error("Service should be registered")
		}

		// Verify entry
		entries := container.Entries()
		var found bool
		for _, entry := range entries {
			if entry.Name == "order-processor" {
				found = true
				if entry.Type != types.ServiceTypeStreamConsumer {
					t.Errorf("Expected type StreamConsumer, got %v", entry.Type)
				}
				if entry.StreamConsumerConfig == nil {
					t.Error("StreamConsumerConfig should not be nil")
				} else {
					if entry.StreamConsumerConfig.Stream.Name != "ORDERS" {
						t.Errorf("Expected stream name 'ORDERS', got %q", entry.StreamConsumerConfig.Stream.Name)
					}
					if entry.StreamConsumerConfig.Fetch.BatchSize != 20 {
						t.Errorf("Expected batch size 20, got %d", entry.StreamConsumerConfig.Fetch.BatchSize)
					}
				}
				if entry.StreamConsumerHandler == nil {
					t.Error("StreamConsumerHandler should not be nil")
				}
			}
		}
		if !found {
			t.Error("Service entry not found")
		}
	})

	t.Run("register with default values", func(t *testing.T) {
		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "EVENTS",
				Subjects: []string{"events.>"},
			},
			// No optional fields set - should use defaults
		}

		err := container.RegisterStreamConsumerService("event-processor", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		entries := container.Entries()
		for _, entry := range entries {
			if entry.Name == "event-processor" {
				cfg := entry.StreamConsumerConfig
				if cfg.Fetch.BatchSize != defaultBatchSize {
					t.Errorf("Expected default batch size %d, got %d", defaultBatchSize, cfg.Fetch.BatchSize)
				}
				if cfg.Fetch.Timeout != defaultFetchTimeout {
					t.Errorf("Expected default fetch timeout %v, got %v", defaultFetchTimeout, cfg.Fetch.Timeout)
				}
				if cfg.Consumer.AckWait != defaultAckWait {
					t.Errorf("Expected default ack wait %v, got %v", defaultAckWait, cfg.Consumer.AckWait)
				}
				if cfg.Consumer.MaxDeliver != defaultMaxDeliver {
					t.Errorf("Expected default max deliver %d, got %d", defaultMaxDeliver, cfg.Consumer.MaxDeliver)
				}
			}
		}
	})

	t.Run("register with empty stream name", func(t *testing.T) {
		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "", // Invalid - required field
				Subjects: []string{"orders.>"},
			},
		}

		err := container.RegisterStreamConsumerService("invalid-stream", config, handler)
		if err == nil {
			t.Error("RegisterStreamConsumerService should fail with empty stream name")
		}
	})

	t.Run("register with empty subjects defaults to computed subject", func(t *testing.T) {
		testContainer := NewServiceContainer(logger).(*serviceContainer)
		testModule := &mockModule{name: "payments"}
		_ = testContainer.BindModule(testModule)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name: "PAYMENTS",
				// No subjects - should default to concrete + wildcard subjects
			},
		}

		err := testContainer.RegisterStreamConsumerService("processor", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService should succeed with empty subjects: %v", err)
		}

		entries := testContainer.Entries()
		var entry *types.ServiceEntry
		for _, e := range entries {
			if e.Name == "processor" {
				entry = e
				break
			}
		}
		if entry == nil {
			t.Fatal("Service entry 'processor' not found")
		}

		// Default subjects should include both concrete and wildcard patterns
		expectedConcreteSubject := "services.payments.processor"
		expectedWildcardSubject := "services.payments.processor.>"
		subjects := entry.StreamConsumerConfig.Stream.Subjects
		if len(subjects) != 2 {
			t.Fatalf("Expected 2 subjects, got %d: %v", len(subjects), subjects)
		}
		if subjects[0] != expectedConcreteSubject {
			t.Errorf("Expected first subject %q, got %q", expectedConcreteSubject, subjects[0])
		}
		if subjects[1] != expectedWildcardSubject {
			t.Errorf("Expected second subject %q, got %q", expectedWildcardSubject, subjects[1])
		}
		// Publish subject should be derived from first (concrete) subject
		expectedPublishSubject := "services.payments.processor"
		if entry.Subject != expectedPublishSubject {
			t.Errorf("Expected publish subject %q, got %q", expectedPublishSubject, entry.Subject)
		}
	})

	t.Run("user-provided wildcard subject derives publish subject correctly", func(t *testing.T) {
		// When user explicitly provides only a wildcard subject (not using defaults),
		// the publish subject should be derived from that wildcard subject.
		// This test ensures backward compatibility with explicit wildcard configurations.
		testContainer := NewServiceContainer(logger).(*serviceContainer)
		testModule := &mockModule{name: "orders"}
		_ = testContainer.BindModule(testModule)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "ORDERS",
				Subjects: []string{"orders.>"}, // User-provided wildcard only
			},
		}

		err := testContainer.RegisterStreamConsumerService("create", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		entries := testContainer.Entries()
		var entry *types.ServiceEntry
		for _, e := range entries {
			if e.Name == "create" {
				entry = e
				break
			}
		}

		if entry == nil {
			t.Fatal("Service entry not found")
		}

		// Subjects should be exactly what user provided (no defaults added)
		subjects := entry.StreamConsumerConfig.Stream.Subjects
		if len(subjects) != 1 {
			t.Fatalf("Expected 1 subject (user-provided), got %d: %v", len(subjects), subjects)
		}
		if subjects[0] != "orders.>" {
			t.Errorf("Expected subject 'orders.>', got %q", subjects[0])
		}

		// Publish subject should be derived from wildcard: orders.> -> orders.default
		expectedPublishSubject := "orders.default"
		if entry.Subject != expectedPublishSubject {
			t.Errorf("Publish subject should be %q (derived from wildcard), got %q",
				expectedPublishSubject, entry.Subject)
		}
	})

	t.Run("register with nil handler", func(t *testing.T) {
		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"test.>"},
			},
		}

		err := container.RegisterStreamConsumerService("nil-handler", config, nil)
		if err == nil {
			t.Error("RegisterStreamConsumerService should fail with nil handler")
		}
	})

	t.Run("register duplicate service", func(t *testing.T) {
		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "DUPLICATE",
				Subjects: []string{"duplicate.>"},
			},
		}

		err := container.RegisterStreamConsumerService("duplicate-stream", config, handler)
		if err != nil {
			t.Fatalf("First RegisterStreamConsumerService failed: %v", err)
		}

		err = container.RegisterStreamConsumerService("duplicate-stream", config, handler)
		if err == nil {
			t.Error("RegisterStreamConsumerService should fail with duplicate name")
		}
	})

	t.Run("register without binding module", func(t *testing.T) {
		unboundContainer := NewServiceContainer(logger).(*serviceContainer)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"test.>"},
			},
		}

		err := unboundContainer.RegisterStreamConsumerService("unbound-test", config, handler)
		if err == nil {
			t.Error("RegisterStreamConsumerService should fail without bound module")
		}
	})

	t.Run("register with wildcard subject derives publish subject", func(t *testing.T) {
		testContainer := NewServiceContainer(logger).(*serviceContainer)
		testModule := &mockModule{name: "orders"}
		_ = testContainer.BindModule(testModule)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "ORDERS",
				Subjects: []string{"orders.>"},
			},
		}

		err := testContainer.RegisterStreamConsumerService("process-order", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		entries := testContainer.Entries()
		var found bool
		for _, entry := range entries {
			if entry.Name == "process-order" {
				found = true
				// Wildcard "orders.>" should derive to "orders.default"
				expectedSubject := "orders.default"
				if entry.Subject != expectedSubject {
					t.Errorf("Expected subject %q, got %q", expectedSubject, entry.Subject)
				}
			}
		}
		if !found {
			t.Error("Service entry not found")
		}
	})

	t.Run("register with concrete subject uses it directly", func(t *testing.T) {
		testContainer := NewServiceContainer(logger).(*serviceContainer)
		testModule := &mockModule{name: "orders"}
		_ = testContainer.BindModule(testModule)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "ORDERS",
				Subjects: []string{"orders.new"},
			},
		}

		err := testContainer.RegisterStreamConsumerService("new-order", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		entries := testContainer.Entries()
		var found bool
		for _, entry := range entries {
			if entry.Name == "new-order" {
				found = true
				// Concrete subject should be used directly
				expectedSubject := "orders.new"
				if entry.Subject != expectedSubject {
					t.Errorf("Expected subject %q, got %q", expectedSubject, entry.Subject)
				}
			}
		}
		if !found {
			t.Error("Service entry not found")
		}
	})
}

func TestDerivePublishSubject(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"concrete subject", "orders.new", "orders.new"},
		{"single wildcard", "orders.*", "orders.default"},
		{"multi-level wildcard", "orders.>", "orders.default"},
		{"complex wildcard", "services.payment.>", "services.payment.default"},
		{"middle single wildcard", "orders.*.items", "orders.default.items"},
		{"multiple wildcards", "orders.*.items.*", "orders.default.items.default"},
		// Edge cases: technically invalid NATS patterns but the function handles them gracefully
		{"multi-level wildcard at end only", "a.b.c.>", "a.b.c.default"},
		{"empty segments handled", "orders..new", "orders..new"},
		{"single segment with wildcard", "*", "default"},
		{"single segment with multi-wildcard", ">", "default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := derivePublishSubject(tt.input)
			if result != tt.expected {
				t.Errorf("derivePublishSubject(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestGetStreamConsumerService(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "orders"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("get non-existent service", func(t *testing.T) {
		_, err := container.GetStreamConsumerService("non-existent")
		if err == nil {
			t.Error("GetStreamConsumerService should fail for non-existent service")
		}
	})

	t.Run("get wrong service type", func(t *testing.T) {
		// Set EventBus with JetStream before registering QueueGroup service
		js := &mockJetStream{}
		eventBus := &mockEventBusWithJetStream{jetStream: js}
		container.SetEventBus(eventBus)

		// Register a QueueGroup service
		queueHandler := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		err := container.RegisterQueueGroupService("queue-svc",
			types.QGHP{QueueGroup: "workers", Handler: queueHandler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}

		_, err = container.GetStreamConsumerService("queue-svc")
		if err == nil {
			t.Error("GetStreamConsumerService should fail for wrong service type")
		}
	})

	t.Run("get without EventBus", func(t *testing.T) {
		// Create a fresh container without EventBus
		freshContainer := NewServiceContainer(logger).(*serviceContainer)
		freshModule := &mockModule{name: "fresh"}
		_ = freshContainer.BindModule(freshModule)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}
		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"test.>"},
			},
		}
		err := freshContainer.RegisterStreamConsumerService("no-eventbus", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		_, err = freshContainer.GetStreamConsumerService("no-eventbus")
		if err == nil {
			t.Error("GetStreamConsumerService should fail without EventBus")
		}
	})

	t.Run("get with JetStream error", func(t *testing.T) {
		// Create a fresh container to avoid interference with other tests
		freshContainer := NewServiceContainer(logger).(*serviceContainer)
		freshModule := &mockModule{name: "js-error"}
		_ = freshContainer.BindModule(freshModule)

		eventBus := &mockEventBusWithJetStream{
			jsError: errors.New("JetStream not available"),
		}
		freshContainer.SetEventBus(eventBus)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}
		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"test.>"},
			},
		}
		err := freshContainer.RegisterStreamConsumerService("js-error-test", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		_, err = freshContainer.GetStreamConsumerService("js-error-test")
		if err == nil {
			t.Error("GetStreamConsumerService should fail when JetStream() returns error")
		}
		if !strings.Contains(err.Error(), "failed to get JetStream from EventBus") {
			t.Errorf("Expected error about JetStream, got: %v", err)
		}
	})

	t.Run("successful get", func(t *testing.T) {
		js := &mockJetStream{}
		eventBus := &mockEventBusWithJetStream{jetStream: js}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}
		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "ORDERS",
				Subjects: []string{"orders.>"},
			},
		}
		err := container.RegisterStreamConsumerService("order-processor", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		client, err := container.GetStreamConsumerService("order-processor")
		if err != nil {
			t.Fatalf("GetStreamConsumerService failed: %v", err)
		}

		if client == nil {
			t.Error("Client should not be nil")
		}
	})
}

func TestStreamConsumerClientPublish(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "orders"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("successful publish", func(t *testing.T) {
		js := &mockJetStream{}
		eventBus := &mockEventBusWithJetStream{jetStream: js}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}
		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "ORDERS",
				Subjects: []string{"orders.>"},
			},
		}
		err := container.RegisterStreamConsumerService("publish-test", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		client, err := container.GetStreamConsumerService("publish-test")
		if err != nil {
			t.Fatalf("GetStreamConsumerService failed: %v", err)
		}

		ctx := context.Background()
		ack, err := client.Publish(ctx, []byte("order-data"))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		if ack == nil {
			t.Error("PubAck should not be nil")
		}

		// Verify the message was published to correct subject (derived from wildcard)
		if len(js.publishedSubjects) != 1 {
			t.Errorf("Expected 1 published message, got %d", len(js.publishedSubjects))
		}

		expectedSubject := "orders.default"
		if js.publishedSubjects[0] != expectedSubject {
			t.Errorf("Expected subject %q, got %q", expectedSubject, js.publishedSubjects[0])
		}
	})

	t.Run("publish with cancelled context", func(t *testing.T) {
		js := &mockJetStream{}
		eventBus := &mockEventBusWithJetStream{jetStream: js}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}
		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "CONTEXT",
				Subjects: []string{"context.>"},
			},
		}
		err := container.RegisterStreamConsumerService("context-test", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		client, err := container.GetStreamConsumerService("context-test")
		if err != nil {
			t.Fatalf("GetStreamConsumerService failed: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err = client.Publish(ctx, []byte("data"))
		if err == nil {
			t.Error("Publish should fail with cancelled context")
		}
	})

	t.Run("publish with JetStream error", func(t *testing.T) {
		js := &mockJetStream{
			publishFunc: func(ctx context.Context, subject string, data []byte) (types.MsgPubAck, error) {
				return nil, errors.New("JetStream unavailable")
			},
		}
		eventBus := &mockEventBusWithJetStream{jetStream: js}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}
		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "ERROR",
				Subjects: []string{"error.>"},
			},
		}
		err := container.RegisterStreamConsumerService("js-error-test", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		client, err := container.GetStreamConsumerService("js-error-test")
		if err != nil {
			t.Fatalf("GetStreamConsumerService failed: %v", err)
		}

		ctx := context.Background()
		_, err = client.Publish(ctx, []byte("data"))
		if err == nil {
			t.Error("Publish should fail when JetStream is unavailable")
		}
	})

	t.Run("publish with no stream configured (no responders)", func(t *testing.T) {
		js := &mockJetStream{
			publishFunc: func(ctx context.Context, subject string, data []byte) (types.MsgPubAck, error) {
				// Simulate no responders error - wrapped as EventStreamError
				return nil, monoerrors.WrapEventStreamNotAvailable(subject, "publish", errors.New("nats: no responders available for request"))
			},
		}
		eventBus := &mockEventBusWithJetStream{jetStream: js}
		container.SetEventBus(eventBus)

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}
		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name:     "NORESPONDER",
				Subjects: []string{"nostream.>"},
			},
		}
		err := container.RegisterStreamConsumerService("no-responder-test", config, handler)
		if err != nil {
			t.Fatalf("RegisterStreamConsumerService failed: %v", err)
		}

		client, err := container.GetStreamConsumerService("no-responder-test")
		if err != nil {
			t.Fatalf("GetStreamConsumerService failed: %v", err)
		}

		ctx := context.Background()
		_, err = client.Publish(ctx, []byte("data"))
		if err == nil {
			t.Fatal("Publish should fail when no stream is configured")
		}

		// Verify the error is an EventStreamError
		if !monoerrors.IsEventStreamError(err) {
			t.Error("Expected EventStreamError when no stream is configured")
		}

		// Verify error message contains relevant information
		errMsg := err.Error()
		if !strings.Contains(errMsg, "no stream configured") {
			t.Errorf("Error message should indicate no stream configured: %s", errMsg)
		}
		if !strings.Contains(errMsg, "messages not persisted") {
			t.Errorf("Error message should indicate messages not persisted: %s", errMsg)
		}
	})
}

func TestStreamConsumerClientPublishMsg(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "orders"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	js := &mockJetStream{}
	eventBus := &mockEventBusWithJetStream{jetStream: js}
	container.SetEventBus(eventBus)

	handler := func(ctx context.Context, msgs []*types.Msg) error {
		return nil
	}
	config := types.StreamConsumerConfig{
		Stream: types.StreamConfig{
			Name:     "ORDERS",
			Subjects: []string{"orders.>"},
		},
	}
	err = container.RegisterStreamConsumerService("msg-test", config, handler)
	if err != nil {
		t.Fatalf("RegisterStreamConsumerService failed: %v", err)
	}

	client, err := container.GetStreamConsumerService("msg-test")
	if err != nil {
		t.Fatalf("GetStreamConsumerService failed: %v", err)
	}

	t.Run("successful publish without headers", func(t *testing.T) {
		ctx := context.Background()
		msg := &types.Msg{
			Data: []byte("order-data"),
		}

		ack, err := client.PublishMsg(ctx, msg)
		if err != nil {
			t.Fatalf("PublishMsg failed: %v", err)
		}

		if ack == nil {
			t.Error("PubAck should not be nil")
		}

		// Verify the message was published with correct subject (derived from wildcard)
		expectedSubject := "orders.default"
		if len(js.publishedSubjects) == 0 {
			t.Error("Expected at least 1 published message")
		} else if js.publishedSubjects[len(js.publishedSubjects)-1] != expectedSubject {
			t.Errorf("Expected subject %q, got %q", expectedSubject, js.publishedSubjects[len(js.publishedSubjects)-1])
		}
	})

	t.Run("publish with headers", func(t *testing.T) {
		ctx := context.Background()
		msg := &types.Msg{
			Data:   []byte("order-data"),
			Header: map[string][]string{"Priority": {"high"}},
		}

		ack, err := client.PublishMsg(ctx, msg)
		if err != nil {
			t.Fatalf("PublishMsg failed: %v", err)
		}

		if ack == nil {
			t.Error("PubAck should not be nil")
		}

		// Verify headers are published successfully
		if ack.Stream() != "TEST_STREAM" {
			t.Errorf("Expected stream TEST_STREAM, got %s", ack.Stream())
		}
	})

	t.Run("publish with subject already set", func(t *testing.T) {
		ctx := context.Background()
		msg := &types.Msg{
			Subject: "orders.custom", // Subject already set
			Data:    []byte("order-data"),
		}

		ack, err := client.PublishMsg(ctx, msg)
		if err != nil {
			t.Fatalf("PublishMsg should succeed with subject already set: %v", err)
		}

		if ack == nil {
			t.Error("PubAck should not be nil")
		}

		// Verify the message was published with the original subject
		expectedSubject := "orders.custom"
		if len(js.publishedSubjects) == 0 {
			t.Error("Expected at least 1 published message")
		} else if js.publishedSubjects[len(js.publishedSubjects)-1] != expectedSubject {
			t.Errorf("Expected subject %q, got %q", expectedSubject, js.publishedSubjects[len(js.publishedSubjects)-1])
		}
	})

	t.Run("publish with cancelled context before call", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel before PublishMsg call

		msg := &types.Msg{Data: []byte("test")}

		_, err := client.PublishMsg(ctx, msg)
		if err == nil {
			t.Error("PublishMsg should fail with cancelled context")
		}
		if !strings.Contains(err.Error(), "context error before publish") {
			t.Errorf("Expected context error, got: %v", err)
		}
	})
}

// Additional error case tests for GetStreamConsumerService
func TestGetStreamConsumerServiceErrors(t *testing.T) {
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

		// Try to get it as StreamConsumer
		_, err := container.GetStreamConsumerService("channel-svc")
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
		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}
		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{Name: "TEST"},
		}
		containerNoEB.RegisterStreamConsumerService("svc", config, handler)

		// Clear EventBus
		containerNoEB.eventBus = nil

		// Try to get service
		_, err := containerNoEB.GetStreamConsumerService("svc")
		if err == nil {
			t.Error("should fail when EventBus is nil")
		}
	})
}

func TestStreamConsumerRunMiddleware(t *testing.T) {
	t.Run("with nil middleware", func(t *testing.T) {
		js := &mockJetStream{}
		client := &streamConsumerClient{
			middlewareChain: nil,
			serviceName:     "test-service",
			moduleName:      "test-module",
			subject:         "test.subject",
			eventPublisher:  js,
		}

		msg := &types.Msg{
			Subject: "test.subject",
			Data:    []byte("test data"),
			Header:  types.Header{"Original": []string{"value"}},
		}

		result := client.runMiddleware(context.Background(), msg)

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

		js := &mockJetStream{}
		client := &streamConsumerClient{
			middlewareChain: mockChain,
			serviceName:     "test-service",
			moduleName:      "test-module",
			subject:         "test.subject",
			eventPublisher:  js,
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
