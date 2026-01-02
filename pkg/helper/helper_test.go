package helper

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1/pkg/types"
)

func TestToKebabCase(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple camel case",
			input:    "OrderCreated",
			expected: "order-created",
		},
		{
			name:     "acronym at start",
			input:    "APIHandler",
			expected: "api-handler",
		},
		{
			name:     "acronym at start with longer word",
			input:    "HTTPServer",
			expected: "http-server",
		},
		{
			name:     "acronym at end",
			input:    "UserID",
			expected: "user-id",
		},
		{
			name:     "multiple acronyms",
			input:    "HTTPAPI",
			expected: "httpapi",
		},
		{
			name:     "acronym in middle",
			input:    "OrderAPIHandler",
			expected: "order-api-handler",
		},
		{
			name:     "space separated",
			input:    "Order Created",
			expected: "order-created",
		},
		{
			name:     "underscore separated",
			input:    "order_created",
			expected: "order-created",
		},
		{
			name:     "all uppercase",
			input:    "ABC",
			expected: "abc",
		},
		{
			name:     "single uppercase",
			input:    "A",
			expected: "a",
		},
		{
			name:     "single lowercase",
			input:    "a",
			expected: "a",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "with numbers",
			input:    "Order123Created",
			expected: "order123-created",
		},
		{
			name:     "numbers only",
			input:    "123",
			expected: "123",
		},
		{
			name:     "mixed case with numbers",
			input:    "API2Handler",
			expected: "api2-handler",
		},
		{
			name:     "trailing spaces",
			input:    "OrderCreated   ",
			expected: "order-created",
		},
		{
			name:     "special chars",
			input:    "Order-Created!",
			expected: "order-created",
		},
		{
			name:     "consecutive special chars",
			input:    "Order---Created",
			expected: "order-created",
		},
		{
			name:     "leading special chars",
			input:    "---OrderCreated",
			expected: "order-created",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toKebabCase(tt.input)
			if result != tt.expected {
				t.Errorf("toKebabCase(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// mockServiceContainer is a test double for ServiceContainer
type mockServiceContainer struct {
	requestReplyHandler    types.RequestReplyHandler
	queueGroupPairs        []types.QGHP
	streamConsumerHandler  types.StreamConsumerHandler
	streamConsumerConfig   *types.StreamConsumerConfig
	registeredServiceNames []string
	returnError            error
}

func (m *mockServiceContainer) BindModule(_ types.Module) error                  { return nil }
func (m *mockServiceContainer) SetEventBus(_ types.EventBus)                     {}
func (m *mockServiceContainer) SetQueueGroupOptimisticWindow(_ time.Duration)    {}
func (m *mockServiceContainer) SetMiddlewareChain(_ types.MiddlewareChainRunner) {}
func (m *mockServiceContainer) RegisterChannelService(_ string, _ chan *types.Msg, _ chan *types.Msg) error {
	return nil
}

func (m *mockServiceContainer) RegisterRequestReplyService(name string, handler types.RequestReplyHandler) error {
	if m.returnError != nil {
		return m.returnError
	}
	m.registeredServiceNames = append(m.registeredServiceNames, name)
	m.requestReplyHandler = handler
	return nil
}

func (m *mockServiceContainer) RegisterQueueGroupService(name string, pairs ...types.QGHP) error {
	if m.returnError != nil {
		return m.returnError
	}
	m.registeredServiceNames = append(m.registeredServiceNames, name)
	m.queueGroupPairs = pairs
	return nil
}

func (m *mockServiceContainer) RegisterStreamConsumerService(name string, config types.StreamConsumerConfig, handler types.StreamConsumerHandler) error {
	if m.returnError != nil {
		return m.returnError
	}
	m.registeredServiceNames = append(m.registeredServiceNames, name)
	m.streamConsumerConfig = &config
	m.streamConsumerHandler = handler
	return nil
}

func (m *mockServiceContainer) GetChannelService(_ string, _ string) (chan *types.Msg, chan *types.Msg, error) {
	return nil, nil, nil
}
func (m *mockServiceContainer) MustGetChannelService(_ string, _ string) (chan *types.Msg, chan *types.Msg) {
	return nil, nil
}
func (m *mockServiceContainer) GetRequestReplyService(_ string) (types.RequestReplyServiceClient, error) {
	return nil, nil
}
func (m *mockServiceContainer) GetQueueGroupService(_ string) (types.QueueGroupServiceClient, error) {
	return nil, nil
}
func (m *mockServiceContainer) GetStreamConsumerService(_ string) (types.StreamConsumerServiceClient, error) {
	return nil, nil
}
func (m *mockServiceContainer) Has(_ string) bool                     { return false }
func (m *mockServiceContainer) Unregister(_ string) error             { return nil }
func (m *mockServiceContainer) Entries() []*types.ServiceEntry        { return nil }
func (m *mockServiceContainer) StartChannelRouters(_ context.Context) {}

// Test types for typed helpers
type testRequest struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

type testResponse struct {
	ID      string `json:"id"`
	Success bool   `json:"success"`
}

func TestRegisterTypedRequestReplyService(t *testing.T) {
	tests := []struct {
		name          string
		serviceName   string
		reqData       []byte
		expectedResp  testResponse
		handlerError  error
		containerErr  error
		unmarshalErr  bool
		expectError   bool
		errorContains string
	}{
		{
			name:        "successful registration and invocation",
			serviceName: "test-service",
			reqData:     []byte(`{"name":"test","value":42}`),
			expectedResp: testResponse{
				ID:      "response-123",
				Success: true,
			},
			expectError: false,
		},
		{
			name:          "handler returns error",
			serviceName:   "error-service",
			reqData:       []byte(`{"name":"test","value":42}`),
			handlerError:  errors.New("handler failed"),
			expectError:   true,
			errorContains: "handler failed",
		},
		{
			name:          "unmarshal request fails",
			serviceName:   "bad-json-service",
			reqData:       []byte(`invalid json`),
			expectError:   true,
			errorContains: "service 'bad-json-service': failed to unmarshal request",
		},
		{
			name:          "container registration fails",
			serviceName:   "fail-service",
			containerErr:  errors.New("container error"),
			expectError:   true,
			errorContains: "container error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockServiceContainer{
				returnError: tt.containerErr,
			}

			handler := func(_ context.Context, req testRequest, _ *types.Msg) (testResponse, error) {
				if tt.handlerError != nil {
					return testResponse{}, tt.handlerError
				}
				return tt.expectedResp, nil
			}

			err := RegisterTypedRequestReplyService(
				mock,
				tt.serviceName,
				json.Unmarshal,
				json.Marshal,
				handler,
			)

			if tt.containerErr != nil {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if !errors.Is(err, tt.containerErr) {
					t.Errorf("expected error %v, got %v", tt.containerErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected registration error: %v", err)
			}

			// Verify service was registered
			if len(mock.registeredServiceNames) != 1 || mock.registeredServiceNames[0] != tt.serviceName {
				t.Errorf("service not registered correctly, got %v", mock.registeredServiceNames)
			}

			// Test the wrapped handler
			if mock.requestReplyHandler == nil {
				t.Fatal("handler not set")
			}

			msg := &types.Msg{Data: tt.reqData}
			respData, err := mock.requestReplyHandler(context.Background(), msg)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if tt.errorContains != "" && !containsString(err.Error(), tt.errorContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errorContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected handler error: %v", err)
			}

			// Verify response
			var resp testResponse
			if err := json.Unmarshal(respData, &resp); err != nil {
				t.Fatalf("failed to unmarshal response: %v", err)
			}
			if resp != tt.expectedResp {
				t.Errorf("response mismatch: got %+v, want %+v", resp, tt.expectedResp)
			}
		})
	}
}

func TestRegisterTypedQueueGroupService(t *testing.T) {
	tests := []struct {
		name          string
		serviceName   string
		queueGroups   []string
		reqData       []byte
		handlerError  error
		containerErr  error
		expectError   bool
		errorContains string
	}{
		{
			name:        "successful registration with single queue group",
			serviceName: "notification-service",
			queueGroups: []string{"workers"},
			reqData:     []byte(`{"name":"test","value":42}`),
			expectError: false,
		},
		{
			name:        "successful registration with multiple queue groups",
			serviceName: "multi-queue-service",
			queueGroups: []string{"email-workers", "sms-workers"},
			reqData:     []byte(`{"name":"test","value":42}`),
			expectError: false,
		},
		{
			name:          "handler returns error",
			serviceName:   "error-service",
			queueGroups:   []string{"workers"},
			reqData:       []byte(`{"name":"test","value":42}`),
			handlerError:  errors.New("handler failed"),
			expectError:   true,
			errorContains: "handler failed",
		},
		{
			name:          "unmarshal fails",
			serviceName:   "bad-json-service",
			queueGroups:   []string{"workers"},
			reqData:       []byte(`invalid json`),
			expectError:   true,
			errorContains: "service 'bad-json-service' queue group 'workers': failed to unmarshal message",
		},
		{
			name:          "container registration fails",
			serviceName:   "fail-service",
			queueGroups:   []string{"workers"},
			containerErr:  errors.New("container error"),
			expectError:   true,
			errorContains: "container error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockServiceContainer{
				returnError: tt.containerErr,
			}

			// Create typed pairs
			pairs := make([]types.TypedQGHP[testRequest], len(tt.queueGroups))
			for i, qg := range tt.queueGroups {
				pairs[i] = types.TypedQGHP[testRequest]{
					QueueGroup: qg,
					Handler: func(_ context.Context, _ testRequest, _ *types.Msg) error {
						return tt.handlerError
					},
				}
			}

			err := RegisterTypedQueueGroupService(
				mock,
				tt.serviceName,
				json.Unmarshal,
				pairs...,
			)

			if tt.containerErr != nil {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if !errors.Is(err, tt.containerErr) {
					t.Errorf("expected error %v, got %v", tt.containerErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected registration error: %v", err)
			}

			// Verify service was registered
			if len(mock.registeredServiceNames) != 1 || mock.registeredServiceNames[0] != tt.serviceName {
				t.Errorf("service not registered correctly, got %v", mock.registeredServiceNames)
			}

			// Verify queue groups were converted correctly
			if len(mock.queueGroupPairs) != len(tt.queueGroups) {
				t.Errorf("expected %d queue group pairs, got %d", len(tt.queueGroups), len(mock.queueGroupPairs))
			}

			for i, pair := range mock.queueGroupPairs {
				if pair.QueueGroup != tt.queueGroups[i] {
					t.Errorf("queue group %d mismatch: got %q, want %q", i, pair.QueueGroup, tt.queueGroups[i])
				}

				// Test the wrapped handler
				msg := &types.Msg{Data: tt.reqData}
				err := pair.Handler(context.Background(), msg)

				if tt.expectError && err == nil {
					t.Error("expected handler error but got nil")
				}
				if !tt.expectError && err != nil {
					t.Errorf("unexpected handler error: %v", err)
				}
				if tt.expectError && tt.errorContains != "" && err != nil {
					if !containsString(err.Error(), tt.errorContains) {
						t.Errorf("error %q should contain %q", err.Error(), tt.errorContains)
					}
				}
			}
		})
	}
}

func TestRegisterTypedStreamConsumerService(t *testing.T) {
	tests := []struct {
		name          string
		serviceName   string
		msgsData      [][]byte
		handlerError  error
		containerErr  error
		expectError   bool
		errorContains string
	}{
		{
			name:        "successful registration and batch processing",
			serviceName: "stream-service",
			msgsData: [][]byte{
				[]byte(`{"name":"test1","value":1}`),
				[]byte(`{"name":"test2","value":2}`),
				[]byte(`{"name":"test3","value":3}`),
			},
			expectError: false,
		},
		{
			name:          "handler returns error",
			serviceName:   "error-service",
			msgsData:      [][]byte{[]byte(`{"name":"test","value":42}`)},
			handlerError:  errors.New("batch processing failed"),
			expectError:   true,
			errorContains: "batch processing failed",
		},
		{
			name:        "unmarshal fails for one message",
			serviceName: "bad-json-service",
			msgsData: [][]byte{
				[]byte(`{"name":"test1","value":1}`),
				[]byte(`invalid json`),
			},
			expectError:   true,
			errorContains: "service 'bad-json-service': failed to unmarshal message",
		},
		{
			name:          "container registration fails",
			serviceName:   "fail-service",
			containerErr:  errors.New("container error"),
			expectError:   true,
			errorContains: "container error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockServiceContainer{
				returnError: tt.containerErr,
			}

			var receivedPayloads []testRequest
			handler := func(_ context.Context, payloads []testRequest, _ []*types.Msg) error {
				if tt.handlerError != nil {
					return tt.handlerError
				}
				receivedPayloads = payloads
				return nil
			}

			config := types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name: "test-stream",
				},
			}

			err := RegisterTypedStreamConsumerService(
				mock,
				tt.serviceName,
				config,
				json.Unmarshal,
				handler,
			)

			if tt.containerErr != nil {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if !errors.Is(err, tt.containerErr) {
					t.Errorf("expected error %v, got %v", tt.containerErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected registration error: %v", err)
			}

			// Verify service was registered
			if len(mock.registeredServiceNames) != 1 || mock.registeredServiceNames[0] != tt.serviceName {
				t.Errorf("service not registered correctly, got %v", mock.registeredServiceNames)
			}

			// Verify config was passed
			if mock.streamConsumerConfig == nil || mock.streamConsumerConfig.Stream.Name != "test-stream" {
				t.Error("stream config not passed correctly")
			}

			// Test the wrapped handler
			if mock.streamConsumerHandler == nil {
				t.Fatal("handler not set")
			}

			// Create test messages
			msgs := make([]*types.Msg, len(tt.msgsData))
			for i, data := range tt.msgsData {
				msgs[i] = &types.Msg{Data: data}
			}

			err = mock.streamConsumerHandler(context.Background(), msgs)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if tt.errorContains != "" && !containsString(err.Error(), tt.errorContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errorContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected handler error: %v", err)
			}

			// Verify payloads were unmarshaled correctly
			if len(receivedPayloads) != len(tt.msgsData) {
				t.Errorf("expected %d payloads, got %d", len(tt.msgsData), len(receivedPayloads))
			}
		})
	}
}

// containsString checks if s contains substr
func containsString(s, substr string) bool {
	return strings.Contains(s, substr)
}

// TestEventDefinition tests the EventDefinition function
func TestEventDefinition(t *testing.T) {
	t.Run("auto-generated subject", func(t *testing.T) {
		def := EventDefinition[testRequest]("order", "OrderCreated", "v1")
		base := def.ToBase()

		if base.Name != "OrderCreated" {
			t.Errorf("expected name 'OrderCreated', got %q", base.Name)
		}
		if base.Version != "v1" {
			t.Errorf("expected version 'v1', got %q", base.Version)
		}
		if base.ModuleName != "order" {
			t.Errorf("expected module 'order', got %q", base.ModuleName)
		}
		// Auto-generated subject should be: events.order.v1.order-created
		expectedSubject := "events.order.v1.order-created"
		if base.Subject != expectedSubject {
			t.Errorf("expected subject %q, got %q", expectedSubject, base.Subject)
		}
	})

	t.Run("custom subject", func(t *testing.T) {
		def := EventDefinition[testRequest]("order", "OrderCreated", "v1", "events.orders.v1.created")
		base := def.ToBase()

		if base.Subject != "events.orders.v1.created" {
			t.Errorf("expected custom subject, got %q", base.Subject)
		}
	})

	t.Run("panic on invalid subject", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for invalid subject")
			}
		}()
		EventDefinition[testRequest]("order", "OrderCreated", "v1", "invalid subject")
	})

	t.Run("panic on missing version in subject", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for subject without version")
			}
		}()
		// Subject doesn't contain version token 'v1'
		EventDefinition[testRequest]("order", "OrderCreated", "v1", "events.orders.created")
	})
}

// mockEventRegistry for testing event registration
type mockEventRegistry struct {
	registeredConsumers map[string]types.EventConsumerHandler
	registeredStreams   map[string]types.EventStreamConsumerHandler
	returnError         error
}

func newMockEventRegistry() *mockEventRegistry {
	return &mockEventRegistry{
		registeredConsumers: make(map[string]types.EventConsumerHandler),
		registeredStreams:   make(map[string]types.EventStreamConsumerHandler),
	}
}

func (m *mockEventRegistry) RegisterEvent(_ types.BaseEventDefinition) error {
	return nil
}

func (m *mockEventRegistry) GetEventsByModule(_ string) []types.BaseEventDefinition {
	return nil
}

func (m *mockEventRegistry) GetEventByName(_, _, _ string) (types.BaseEventDefinition, bool) {
	return types.BaseEventDefinition{}, false
}

func (m *mockEventRegistry) GetAllEvents() []types.BaseEventDefinition {
	return nil
}

func (m *mockEventRegistry) RegisterEventConsumer(def types.BaseEventDefinition, handler types.EventConsumerHandler, _ types.Module, _ ...string) error {
	if m.returnError != nil {
		return m.returnError
	}
	m.registeredConsumers[def.Subject] = handler
	return nil
}

func (m *mockEventRegistry) RegisterEventStreamConsumer(def types.BaseEventDefinition, config types.StreamConsumerConfig, handler types.EventStreamConsumerHandler, _ types.Module) error {
	if m.returnError != nil {
		return m.returnError
	}
	m.registeredStreams[def.Subject] = handler
	return nil
}

func (m *mockEventRegistry) Entries() []types.EventConsumerEntry {
	return nil
}

func (m *mockEventRegistry) StreamEntries() []*types.EventStreamConsumerEntry {
	return nil
}

func (m *mockEventRegistry) SetMiddlewareChain(_ types.MiddlewareChainRunner) {
}

func (m *mockEventRegistry) StreamConsumerEntries() []types.EventStreamConsumerEntry {
	return nil
}

// mockModule for testing
type mockModule struct{}

func (m *mockModule) Name() string                    { return "test-module" }
func (m *mockModule) Start(ctx context.Context) error { return nil }
func (m *mockModule) Stop(ctx context.Context) error  { return nil }

func TestRegisterTypedEventConsumer(t *testing.T) {
	t.Run("successful registration", func(t *testing.T) {
		registry := newMockEventRegistry()
		def := EventDefinition[testRequest]("order", "OrderCreated", "v1")
		module := &mockModule{}

		var receivedEvent testRequest
		handler := func(_ context.Context, event testRequest, _ *types.Msg) error {
			receivedEvent = event
			return nil
		}

		err := RegisterTypedEventConsumer(registry, def, handler, module)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify handler was registered
		subject := def.ToBase().Subject
		wrappedHandler, ok := registry.registeredConsumers[subject]
		if !ok {
			t.Fatal("handler not registered")
		}

		// Test the wrapped handler
		testData := testRequest{Name: "test", Value: 42}
		marshaledData, _ := json.Marshal(testData)
		msg := &types.Msg{Data: marshaledData}

		err = wrappedHandler(context.Background(), msg)
		if err != nil {
			t.Fatalf("handler error: %v", err)
		}

		if receivedEvent.Name != "test" || receivedEvent.Value != 42 {
			t.Errorf("event not unmarshaled correctly: %+v", receivedEvent)
		}
	})

	t.Run("registration error", func(t *testing.T) {
		registry := newMockEventRegistry()
		registry.returnError = errors.New("registration failed")
		def := EventDefinition[testRequest]("order", "OrderCreated", "v1")
		module := &mockModule{}

		handler := func(_ context.Context, _ testRequest, _ *types.Msg) error {
			return nil
		}

		err := RegisterTypedEventConsumer(registry, def, handler, module)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("unmarshal error", func(t *testing.T) {
		registry := newMockEventRegistry()
		def := EventDefinition[testRequest]("order", "OrderCreated", "v1")
		module := &mockModule{}

		handler := func(_ context.Context, _ testRequest, _ *types.Msg) error {
			return nil
		}

		err := RegisterTypedEventConsumer(registry, def, handler, module)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Test with invalid JSON
		subject := def.ToBase().Subject
		wrappedHandler := registry.registeredConsumers[subject]
		msg := &types.Msg{Data: []byte("invalid json")}

		err = wrappedHandler(context.Background(), msg)
		if err == nil {
			t.Error("expected unmarshal error")
		}
	})
}

func TestRegisterTypedEventStreamConsumer(t *testing.T) {
	t.Run("successful registration", func(t *testing.T) {
		registry := newMockEventRegistry()
		def := EventDefinition[testRequest]("order", "OrderCreated", "v1")
		module := &mockModule{}

		var receivedEvents []testRequest
		handler := func(_ context.Context, events []testRequest, msgs []*types.Msg) error {
			receivedEvents = events
			for _, msg := range msgs {
				msg.Ack()
			}
			return nil
		}

		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{
				Name: "test-stream",
			},
		}

		err := RegisterTypedEventStreamConsumer(registry, def, config, handler, module)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify handler was registered
		subject := def.ToBase().Subject
		wrappedHandler, ok := registry.registeredStreams[subject]
		if !ok {
			t.Fatal("handler not registered")
		}

		// Test the wrapped handler with batch
		testData := []testRequest{
			{Name: "test1", Value: 1},
			{Name: "test2", Value: 2},
		}
		msgs := make([]*types.Msg, len(testData))
		for i, data := range testData {
			marshaledData, _ := json.Marshal(data)
			msgs[i] = &types.Msg{Data: marshaledData}
		}

		err = wrappedHandler(context.Background(), msgs)
		if err != nil {
			t.Fatalf("handler error: %v", err)
		}

		if len(receivedEvents) != 2 {
			t.Errorf("expected 2 events, got %d", len(receivedEvents))
		}
		if receivedEvents[0].Name != "test1" || receivedEvents[1].Name != "test2" {
			t.Errorf("events not unmarshaled correctly: %+v", receivedEvents)
		}
	})

	t.Run("unmarshal error in batch", func(t *testing.T) {
		registry := newMockEventRegistry()
		def := EventDefinition[testRequest]("order", "OrderCreated", "v1")
		module := &mockModule{}

		handler := func(_ context.Context, _ []testRequest, _ []*types.Msg) error {
			return nil
		}

		config := types.StreamConsumerConfig{
			Stream: types.StreamConfig{Name: "test-stream"},
		}

		err := RegisterTypedEventStreamConsumer(registry, def, config, handler, module)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Test with invalid JSON in batch
		subject := def.ToBase().Subject
		wrappedHandler := registry.registeredStreams[subject]
		msgs := []*types.Msg{
			{Data: []byte(`{"name":"valid","value":1}`)},
			{Data: []byte("invalid json")},
		}

		err = wrappedHandler(context.Background(), msgs)
		if err == nil {
			t.Error("expected unmarshal error")
		}
	})
}

// mockRequestReplyServiceClient for testing
type mockRequestReplyServiceClient struct {
	callFunc func(context.Context, []byte) (*types.Msg, error)
}

func (m *mockRequestReplyServiceClient) Call(ctx context.Context, data []byte) (*types.Msg, error) {
	if m.callFunc != nil {
		return m.callFunc(ctx, data)
	}
	return &types.Msg{Data: []byte(`{"id":"response-123","success":true}`)}, nil
}

func (m *mockRequestReplyServiceClient) CallMsg(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
	return m.Call(ctx, msg.Data)
}

// mockQueueGroupServiceClient for testing
type mockQueueGroupServiceClient struct {
	sendFunc func(context.Context, []byte) error
}

func (m *mockQueueGroupServiceClient) Send(ctx context.Context, data []byte) error {
	if m.sendFunc != nil {
		return m.sendFunc(ctx, data)
	}
	return nil
}

func (m *mockQueueGroupServiceClient) SendMsg(ctx context.Context, msg *types.Msg) error {
	return m.Send(ctx, msg.Data)
}

// mockStreamConsumerServiceClient for testing
type mockStreamConsumerServiceClient struct {
	publishFunc func(context.Context, []byte) (types.MsgPubAck, error)
}

func (m *mockStreamConsumerServiceClient) Publish(ctx context.Context, data []byte) (types.MsgPubAck, error) {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, data)
	}
	return &mockMsgPubAck{sequence: 123}, nil
}

func (m *mockStreamConsumerServiceClient) PublishMsg(ctx context.Context, msg *types.Msg) (types.MsgPubAck, error) {
	return m.Publish(ctx, msg.Data)
}

// mockMsgPubAck implements types.MsgPubAck
type mockMsgPubAck struct {
	sequence uint64
}

func (m *mockMsgPubAck) Sequence() uint64 { return m.sequence }
func (m *mockMsgPubAck) Duplicate() bool  { return false }
func (m *mockMsgPubAck) Stream() string   { return "test-stream" }
func (m *mockMsgPubAck) Domain() string   { return "" }

// Enhanced mock container with service clients
type mockContainerWithClients struct {
	mockServiceContainer
	requestReplyClient *mockRequestReplyServiceClient
	queueGroupClient   *mockQueueGroupServiceClient
	streamClient       *mockStreamConsumerServiceClient
	getServiceError    error
}

func (m *mockContainerWithClients) GetRequestReplyService(_ string) (types.RequestReplyServiceClient, error) {
	if m.getServiceError != nil {
		return nil, m.getServiceError
	}
	return m.requestReplyClient, nil
}

func (m *mockContainerWithClients) GetQueueGroupService(_ string) (types.QueueGroupServiceClient, error) {
	if m.getServiceError != nil {
		return nil, m.getServiceError
	}
	return m.queueGroupClient, nil
}

func (m *mockContainerWithClients) GetStreamConsumerService(_ string) (types.StreamConsumerServiceClient, error) {
	if m.getServiceError != nil {
		return nil, m.getServiceError
	}
	return m.streamClient, nil
}

func TestCallRequestReplyService(t *testing.T) {
	t.Run("successful call", func(t *testing.T) {
		container := &mockContainerWithClients{
			requestReplyClient: &mockRequestReplyServiceClient{},
		}

		req := testRequest{Name: "test", Value: 42}
		var resp testResponse

		err := CallRequestReplyService(
			context.Background(),
			container,
			"test-service",
			json.Marshal,
			json.Unmarshal,
			req,
			&resp,
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if resp.ID != "response-123" || !resp.Success {
			t.Errorf("response not unmarshaled correctly: %+v", resp)
		}
	})

	t.Run("nil response pointer", func(t *testing.T) {
		container := &mockContainerWithClients{
			requestReplyClient: &mockRequestReplyServiceClient{},
		}

		req := testRequest{Name: "test", Value: 42}

		err := CallRequestReplyService(
			context.Background(),
			container,
			"test-service",
			json.Marshal,
			json.Unmarshal,
			req,
			(*testResponse)(nil),
		)

		if err == nil {
			t.Fatal("expected error for nil response pointer")
		}
		if !containsString(err.Error(), "response pointer cannot be nil") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("marshal error", func(t *testing.T) {
		container := &mockContainerWithClients{
			requestReplyClient: &mockRequestReplyServiceClient{},
		}

		badMarshaler := func(_ any) ([]byte, error) {
			return nil, errors.New("marshal failed")
		}

		req := testRequest{Name: "test", Value: 42}
		var resp testResponse

		err := CallRequestReplyService(
			context.Background(),
			container,
			"test-service",
			badMarshaler,
			json.Unmarshal,
			req,
			&resp,
		)

		if err == nil {
			t.Fatal("expected error")
		}
		if !containsString(err.Error(), "failed to marshal request") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("get service error", func(t *testing.T) {
		container := &mockContainerWithClients{
			getServiceError: errors.New("service not found"),
		}

		req := testRequest{Name: "test", Value: 42}
		var resp testResponse

		err := CallRequestReplyService(
			context.Background(),
			container,
			"test-service",
			json.Marshal,
			json.Unmarshal,
			req,
			&resp,
		)

		if err == nil {
			t.Fatal("expected error")
		}
		if !containsString(err.Error(), "failed to get service") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("service call error", func(t *testing.T) {
		container := &mockContainerWithClients{
			requestReplyClient: &mockRequestReplyServiceClient{
				callFunc: func(_ context.Context, _ []byte) (*types.Msg, error) {
					return nil, errors.New("call failed")
				},
			},
		}

		req := testRequest{Name: "test", Value: 42}
		var resp testResponse

		err := CallRequestReplyService(
			context.Background(),
			container,
			"test-service",
			json.Marshal,
			json.Unmarshal,
			req,
			&resp,
		)

		if err == nil {
			t.Fatal("expected error")
		}
		if !containsString(err.Error(), "failed to call service") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("unmarshal response error", func(t *testing.T) {
		container := &mockContainerWithClients{
			requestReplyClient: &mockRequestReplyServiceClient{
				callFunc: func(_ context.Context, _ []byte) (*types.Msg, error) {
					return &types.Msg{Data: []byte("invalid json")}, nil
				},
			},
		}

		req := testRequest{Name: "test", Value: 42}
		var resp testResponse

		err := CallRequestReplyService(
			context.Background(),
			container,
			"test-service",
			json.Marshal,
			json.Unmarshal,
			req,
			&resp,
		)

		if err == nil {
			t.Fatal("expected error")
		}
		if !containsString(err.Error(), "failed to parse service") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestSendQueueGroupService(t *testing.T) {
	t.Run("successful send", func(t *testing.T) {
		container := &mockContainerWithClients{
			queueGroupClient: &mockQueueGroupServiceClient{},
		}

		req := testRequest{Name: "test", Value: 42}

		err := SendQueueGroupService(
			context.Background(),
			container,
			"test-service",
			json.Marshal,
			req,
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("marshal error", func(t *testing.T) {
		container := &mockContainerWithClients{
			queueGroupClient: &mockQueueGroupServiceClient{},
		}

		badMarshaler := func(_ any) ([]byte, error) {
			return nil, errors.New("marshal failed")
		}

		req := testRequest{Name: "test", Value: 42}

		err := SendQueueGroupService(
			context.Background(),
			container,
			"test-service",
			badMarshaler,
			req,
		)

		if err == nil {
			t.Fatal("expected error")
		}
		if !containsString(err.Error(), "failed to marshal request") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("get service error", func(t *testing.T) {
		container := &mockContainerWithClients{
			getServiceError: errors.New("service not found"),
		}

		req := testRequest{Name: "test", Value: 42}

		err := SendQueueGroupService(
			context.Background(),
			container,
			"test-service",
			json.Marshal,
			req,
		)

		if err == nil {
			t.Fatal("expected error")
		}
		if !containsString(err.Error(), "failed to get service") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("send error", func(t *testing.T) {
		container := &mockContainerWithClients{
			queueGroupClient: &mockQueueGroupServiceClient{
				sendFunc: func(_ context.Context, _ []byte) error {
					return errors.New("send failed")
				},
			},
		}

		req := testRequest{Name: "test", Value: 42}

		err := SendQueueGroupService(
			context.Background(),
			container,
			"test-service",
			json.Marshal,
			req,
		)

		if err == nil {
			t.Fatal("expected error")
		}
		if !containsString(err.Error(), "failed to send to service") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestPublishStreamConsumerService(t *testing.T) {
	t.Run("successful publish", func(t *testing.T) {
		container := &mockContainerWithClients{
			streamClient: &mockStreamConsumerServiceClient{},
		}

		event := testRequest{Name: "test", Value: 42}

		ack, err := PublishStreamConsumerService(
			context.Background(),
			container,
			"test-service",
			json.Marshal,
			event,
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if ack.Sequence() != 123 {
			t.Errorf("expected sequence 123, got %d", ack.Sequence())
		}
	})

	t.Run("marshal error", func(t *testing.T) {
		container := &mockContainerWithClients{
			streamClient: &mockStreamConsumerServiceClient{},
		}

		badMarshaler := func(_ any) ([]byte, error) {
			return nil, errors.New("marshal failed")
		}

		event := testRequest{Name: "test", Value: 42}

		_, err := PublishStreamConsumerService(
			context.Background(),
			container,
			"test-service",
			badMarshaler,
			event,
		)

		if err == nil {
			t.Fatal("expected error")
		}
		if !containsString(err.Error(), "failed to marshal message") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("get service error", func(t *testing.T) {
		container := &mockContainerWithClients{
			getServiceError: errors.New("service not found"),
		}

		event := testRequest{Name: "test", Value: 42}

		_, err := PublishStreamConsumerService(
			context.Background(),
			container,
			"test-service",
			json.Marshal,
			event,
		)

		if err == nil {
			t.Fatal("expected error")
		}
		if !containsString(err.Error(), "failed to get service") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("publish error", func(t *testing.T) {
		container := &mockContainerWithClients{
			streamClient: &mockStreamConsumerServiceClient{
				publishFunc: func(_ context.Context, _ []byte) (types.MsgPubAck, error) {
					return nil, errors.New("publish failed")
				},
			},
		}

		event := testRequest{Name: "test", Value: 42}

		_, err := PublishStreamConsumerService(
			context.Background(),
			container,
			"test-service",
			json.Marshal,
			event,
		)

		if err == nil {
			t.Fatal("expected error")
		}
		if !containsString(err.Error(), "failed to publish to service") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
