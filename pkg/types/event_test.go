package types_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1/pkg/types"
	"github.com/nats-io/nats.go/jetstream"
)

// =============================================================================
// Mock Types
// =============================================================================

// mockMsgPubAck implements types.MsgPubAck for testing
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

// mockEventStream implements types.EventStream for testing
type mockEventStream struct {
	publishMsgFunc func(ctx context.Context, msg *types.Msg) (types.MsgPubAck, error)
}

func (m *mockEventStream) Publish(_ context.Context, _ string, _ []byte) (types.MsgPubAck, error) {
	return nil, errors.New("not implemented")
}

func (m *mockEventStream) PublishMsg(ctx context.Context, msg *types.Msg) (types.MsgPubAck, error) {
	if m.publishMsgFunc != nil {
		return m.publishMsgFunc(ctx, msg)
	}
	return &mockMsgPubAck{stream: "test-stream", sequence: 1}, nil
}

func (m *mockEventStream) CreateOrUpdateStream(_ context.Context, _ types.StreamConfig) (jetstream.Stream, error) {
	return nil, nil
}

func (m *mockEventStream) CreateOrUpdateConsumer(_ context.Context, _ string, _ types.ConsumerConfig) (jetstream.Consumer, error) {
	return nil, nil
}

func (m *mockEventStream) Stream(_ context.Context, _ string) (jetstream.Stream, error) {
	return nil, nil
}

func (m *mockEventStream) DeleteStream(_ context.Context, _ string) error {
	return nil
}

// mockSubscription implements types.Subscription for testing
type mockSubscription struct{}

func (m *mockSubscription) Unsubscribe() error                          { return nil }
func (m *mockSubscription) Drain() error                                { return nil }
func (m *mockSubscription) IsValid() bool                               { return true }
func (m *mockSubscription) Subject() string                             { return "" }
func (m *mockSubscription) Queue() string                               { return "" }
func (m *mockSubscription) NextMsg(_ time.Duration) (*types.Msg, error) { return nil, nil }
func (m *mockSubscription) NextMsgWithContext(_ context.Context) (*types.Msg, error) {
	return nil, nil
}

// mockEventBus implements types.EventBus for testing
type mockEventBus struct {
	eventStream    *mockEventStream
	eventStreamErr error
	publishedMsgs  []*types.Msg
}

func (m *mockEventBus) Publish(_ string, _ []byte) error { return nil }
func (m *mockEventBus) PublishMsg(msg *types.Msg) error {
	m.publishedMsgs = append(m.publishedMsgs, msg)
	return nil
}
func (m *mockEventBus) Request(_ string, _ []byte, _ time.Duration) (*types.Msg, error) {
	return nil, nil
}
func (m *mockEventBus) RequestWithContext(_ context.Context, _ string, _ []byte) (*types.Msg, error) {
	return nil, nil
}
func (m *mockEventBus) RequestMsgWithContext(_ context.Context, _ *types.Msg) (*types.Msg, error) {
	return nil, nil
}
func (m *mockEventBus) Subscribe(_ string, _ types.MsgHandler) (types.Subscription, error) {
	return &mockSubscription{}, nil
}
func (m *mockEventBus) SubscribeSync(_ string) (types.Subscription, error) {
	return &mockSubscription{}, nil
}
func (m *mockEventBus) QueueSubscribe(_ string, _ string, _ types.MsgHandler) (types.Subscription, error) {
	return &mockSubscription{}, nil
}
func (m *mockEventBus) QueueSubscribeSync(_ string, _ string) (types.Subscription, error) {
	return &mockSubscription{}, nil
}
func (m *mockEventBus) ChanSubscribe(_ string, _ chan *types.Msg) (types.Subscription, error) {
	return &mockSubscription{}, nil
}
func (m *mockEventBus) EventStream() (types.EventStream, error) {
	if m.eventStreamErr != nil {
		return nil, m.eventStreamErr
	}
	if m.eventStream == nil {
		m.eventStream = &mockEventStream{}
	}
	return m.eventStream, nil
}
func (m *mockEventBus) SetRuntimeContext(_ context.Context) {}

// =============================================================================
// BaseEventDefinition.EventStreamPublishRaw Tests
// =============================================================================

func TestBaseEventDefinition_EventStreamPublishRaw(t *testing.T) {
	tests := []struct {
		name           string
		eventBus       types.EventBus
		data           []byte
		header         types.Header
		eventStreamErr error
		publishErr     error
		expectedSeq    uint64
		expectError    bool
		errorContains  string
	}{
		{
			name: "successful publish",
			eventBus: &mockEventBus{
				eventStream: &mockEventStream{
					publishMsgFunc: func(_ context.Context, _ *types.Msg) (types.MsgPubAck, error) {
						return &mockMsgPubAck{stream: "test-stream", sequence: 42}, nil
					},
				},
			},
			data:        []byte(`{"order_id":"123"}`),
			header:      types.Header{"X-Request-ID": {"req-123"}},
			expectedSeq: 42,
			expectError: false,
		},
		{
			name:          "nil eventBus",
			eventBus:      nil,
			data:          []byte(`{}`),
			expectError:   true,
			errorContains: "eventBus cannot be nil",
		},
		{
			name: "EventStream returns error",
			eventBus: &mockEventBus{
				eventStreamErr: errors.New("JetStream not available"),
			},
			data:          []byte(`{}`),
			expectError:   true,
			errorContains: "failed to get EventStream",
		},
		{
			name: "PublishMsg returns error",
			eventBus: &mockEventBus{
				eventStream: &mockEventStream{
					publishMsgFunc: func(_ context.Context, _ *types.Msg) (types.MsgPubAck, error) {
						return nil, errors.New("stream not found")
					},
				},
			},
			data:          []byte(`{}`),
			expectError:   true,
			errorContains: "failed to publish to JetStream",
		},
		{
			name: "nil header is allowed",
			eventBus: &mockEventBus{
				eventStream: &mockEventStream{
					publishMsgFunc: func(_ context.Context, msg *types.Msg) (types.MsgPubAck, error) {
						if msg.Header != nil {
							t.Error("expected nil header")
						}
						return &mockMsgPubAck{sequence: 1}, nil
					},
				},
			},
			data:        []byte(`{}`),
			header:      nil,
			expectedSeq: 1,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDef := types.BaseEventDefinition{
				ModuleName: "test-module",
				Name:       "TestEvent",
				Subject:    "events.test.v1.test-event",
				Version:    "v1",
			}

			ctx := context.Background()
			ack, err := baseDef.EventStreamPublishRaw(ctx, tt.eventBus, tt.data, tt.header)

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
				t.Fatalf("unexpected error: %v", err)
			}

			if ack == nil {
				t.Fatal("expected non-nil ack")
			}

			if ack.Sequence() != tt.expectedSeq {
				t.Errorf("expected sequence %d, got %d", tt.expectedSeq, ack.Sequence())
			}
		})
	}
}

func TestBaseEventDefinition_EventStreamPublishRaw_SubjectAndData(t *testing.T) {
	var capturedMsg *types.Msg
	eventBus := &mockEventBus{
		eventStream: &mockEventStream{
			publishMsgFunc: func(_ context.Context, msg *types.Msg) (types.MsgPubAck, error) {
				capturedMsg = msg
				return &mockMsgPubAck{sequence: 1}, nil
			},
		},
	}

	baseDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.order.v1.order-created",
		Version:    "v1",
	}

	data := []byte(`{"order_id":"order-123","amount":99.99}`)
	header := types.Header{"X-Trace-ID": {"trace-456"}}

	ctx := context.Background()
	_, err := baseDef.EventStreamPublishRaw(ctx, eventBus, data, header)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedMsg == nil {
		t.Fatal("message was not captured")
	}

	if capturedMsg.Subject != "events.order.v1.order-created" {
		t.Errorf("expected subject %q, got %q", "events.order.v1.order-created", capturedMsg.Subject)
	}

	if string(capturedMsg.Data) != string(data) {
		t.Errorf("expected data %q, got %q", string(data), string(capturedMsg.Data))
	}

	if len(capturedMsg.Header["X-Trace-ID"]) == 0 || capturedMsg.Header["X-Trace-ID"][0] != "trace-456" {
		t.Errorf("expected header X-Trace-ID=trace-456, got %v", capturedMsg.Header)
	}
}

// =============================================================================
// EventDefinition[T].EventStreamPublish Tests
// =============================================================================

type testEvent struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

func TestEventDefinition_EventStreamPublish(t *testing.T) {
	tests := []struct {
		name           string
		eventBus       types.EventBus
		event          testEvent
		header         types.Header
		eventStreamErr error
		publishErr     error
		marshalErr     bool
		expectedSeq    uint64
		expectError    bool
		errorContains  string
	}{
		{
			name: "successful publish with typed event",
			eventBus: &mockEventBus{
				eventStream: &mockEventStream{
					publishMsgFunc: func(_ context.Context, _ *types.Msg) (types.MsgPubAck, error) {
						return &mockMsgPubAck{stream: "order-events", sequence: 100}, nil
					},
				},
			},
			event:       testEvent{OrderID: "order-123", Amount: 99.99},
			header:      types.Header{"X-Request-ID": {"req-789"}},
			expectedSeq: 100,
			expectError: false,
		},
		{
			name:          "nil eventBus",
			eventBus:      nil,
			event:         testEvent{},
			expectError:   true,
			errorContains: "eventBus cannot be nil",
		},
		{
			name: "EventStream returns error",
			eventBus: &mockEventBus{
				eventStreamErr: errors.New("JetStream disabled"),
			},
			event:         testEvent{},
			expectError:   true,
			errorContains: "failed to get EventStream",
		},
		{
			name: "PublishMsg returns error",
			eventBus: &mockEventBus{
				eventStream: &mockEventStream{
					publishMsgFunc: func(_ context.Context, _ *types.Msg) (types.MsgPubAck, error) {
						return nil, errors.New("publish timeout")
					},
				},
			},
			event:         testEvent{},
			expectError:   true,
			errorContains: "failed to publish to JetStream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventDef := types.NewEventDefinition[testEvent](
				"order",
				"OrderCreated",
				"v1",
				"events.order.v1.order-created",
			)

			ctx := context.Background()
			ack, err := eventDef.EventStreamPublish(ctx, tt.eventBus, tt.event, tt.header)

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
				t.Fatalf("unexpected error: %v", err)
			}

			if ack == nil {
				t.Fatal("expected non-nil ack")
			}

			if ack.Sequence() != tt.expectedSeq {
				t.Errorf("expected sequence %d, got %d", tt.expectedSeq, ack.Sequence())
			}
		})
	}
}

func TestEventDefinition_EventStreamPublish_MarshalError(t *testing.T) {
	eventBus := &mockEventBus{
		eventStream: &mockEventStream{},
	}

	// Create event definition with a marshaler that always fails
	failingMarshal := func(_ any) ([]byte, error) {
		return nil, errors.New("marshal failed")
	}

	eventDef := types.NewEventDefinition[testEvent](
		"order",
		"OrderCreated",
		"v1",
		"events.order.v1.order-created",
	).WithMarshaler(failingMarshal)

	ctx := context.Background()
	_, err := eventDef.EventStreamPublish(ctx, eventBus, testEvent{}, nil)

	if err == nil {
		t.Fatal("expected error but got nil")
	}

	if !containsString(err.Error(), "failed to marshal event") {
		t.Errorf("error should contain 'failed to marshal event', got: %v", err)
	}
}

func TestEventDefinition_EventStreamPublish_CustomMarshaler(t *testing.T) {
	var marshalerCalled bool
	customMarshal := func(_ any) ([]byte, error) {
		marshalerCalled = true
		return []byte(`{"custom":"data"}`), nil
	}

	var capturedData []byte
	eventBus := &mockEventBus{
		eventStream: &mockEventStream{
			publishMsgFunc: func(_ context.Context, msg *types.Msg) (types.MsgPubAck, error) {
				capturedData = msg.Data
				return &mockMsgPubAck{sequence: 1}, nil
			},
		},
	}

	eventDef := types.NewEventDefinition[testEvent](
		"order",
		"OrderCreated",
		"v1",
		"events.order.v1.order-created",
	).WithMarshaler(customMarshal)

	ctx := context.Background()
	_, err := eventDef.EventStreamPublish(ctx, eventBus, testEvent{OrderID: "123"}, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !marshalerCalled {
		t.Error("custom marshaler was not called")
	}

	if string(capturedData) != `{"custom":"data"}` {
		t.Errorf("expected custom marshaled data, got: %s", string(capturedData))
	}
}

func TestEventDefinition_EventStreamPublish_SubjectFromDefinition(t *testing.T) {
	var capturedSubject string
	eventBus := &mockEventBus{
		eventStream: &mockEventStream{
			publishMsgFunc: func(_ context.Context, msg *types.Msg) (types.MsgPubAck, error) {
				capturedSubject = msg.Subject
				return &mockMsgPubAck{sequence: 1}, nil
			},
		},
	}

	eventDef := types.NewEventDefinition[testEvent](
		"inventory",
		"StockUpdated",
		"v2",
		"events.inventory.v2.stock-updated",
	)

	ctx := context.Background()
	_, err := eventDef.EventStreamPublish(ctx, eventBus, testEvent{}, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedSubject != "events.inventory.v2.stock-updated" {
		t.Errorf("expected subject %q, got %q", "events.inventory.v2.stock-updated", capturedSubject)
	}
}

// =============================================================================
// BaseEventDefinition.PublishRaw Tests
// =============================================================================

func TestBaseEventDefinition_PublishRaw(t *testing.T) {
	tests := []struct {
		name          string
		eventBus      types.EventBus
		data          []byte
		header        types.Header
		expectError   bool
		errorContains string
	}{
		{
			name:          "nil eventBus returns error",
			eventBus:      nil,
			data:          []byte(`{"order_id":"123"}`),
			expectError:   true,
			errorContains: "eventBus cannot be nil",
		},
		{
			name:        "successful publish",
			eventBus:    &mockEventBus{},
			data:        []byte(`{"order_id":"123"}`),
			header:      types.Header{"X-Request-ID": {"req-123"}},
			expectError: false,
		},
		{
			name:        "nil header is allowed",
			eventBus:    &mockEventBus{},
			data:        []byte(`{}`),
			header:      nil,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDef := types.BaseEventDefinition{
				ModuleName: "test-module",
				Name:       "TestEvent",
				Subject:    "events.test.v1.test-event",
				Version:    "v1",
			}

			err := baseDef.PublishRaw(tt.eventBus, tt.data, tt.header)

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
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestBaseEventDefinition_PublishRaw_SubjectAndData(t *testing.T) {
	eventBus := &mockEventBus{}

	baseDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.order.v1.order-created",
		Version:    "v1",
	}

	data := []byte(`{"order_id":"order-123","amount":99.99}`)
	header := types.Header{"X-Trace-ID": {"trace-456"}}

	err := baseDef.PublishRaw(eventBus, data, header)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(eventBus.publishedMsgs) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(eventBus.publishedMsgs))
	}

	capturedMsg := eventBus.publishedMsgs[0]

	if capturedMsg.Subject != "events.order.v1.order-created" {
		t.Errorf("expected subject %q, got %q", "events.order.v1.order-created", capturedMsg.Subject)
	}

	if string(capturedMsg.Data) != string(data) {
		t.Errorf("expected data %q, got %q", string(data), string(capturedMsg.Data))
	}

	if len(capturedMsg.Header["X-Trace-ID"]) == 0 || capturedMsg.Header["X-Trace-ID"][0] != "trace-456" {
		t.Errorf("expected header X-Trace-ID=trace-456, got %v", capturedMsg.Header)
	}
}

// =============================================================================
// EventDefinition[T].Publish Tests
// =============================================================================

func TestEventDefinition_Publish(t *testing.T) {
	tests := []struct {
		name          string
		eventBus      types.EventBus
		event         testEvent
		header        types.Header
		expectError   bool
		errorContains string
	}{
		{
			name:          "nil eventBus returns error",
			eventBus:      nil,
			event:         testEvent{OrderID: "123", Amount: 99.99},
			expectError:   true,
			errorContains: "eventBus cannot be nil",
		},
		{
			name:        "successful publish",
			eventBus:    &mockEventBus{},
			event:       testEvent{OrderID: "123", Amount: 99.99},
			header:      types.Header{"X-Request-ID": {"req-123"}},
			expectError: false,
		},
		{
			name:        "nil header is allowed",
			eventBus:    &mockEventBus{},
			event:       testEvent{OrderID: "123"},
			header:      nil,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventDef := types.NewEventDefinition[testEvent](
				"order",
				"OrderCreated",
				"v1",
				"events.order.v1.order-created",
			)

			err := eventDef.Publish(tt.eventBus, tt.event, tt.header)

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
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestEventDefinition_Publish_MarshalError(t *testing.T) {
	eventBus := &mockEventBus{}

	// Create event definition with a marshaler that always fails
	failingMarshal := func(_ any) ([]byte, error) {
		return nil, errors.New("marshal failed")
	}

	eventDef := types.NewEventDefinition[testEvent](
		"order",
		"OrderCreated",
		"v1",
		"events.order.v1.order-created",
	).WithMarshaler(failingMarshal)

	err := eventDef.Publish(eventBus, testEvent{}, nil)

	if err == nil {
		t.Fatal("expected error but got nil")
	}

	if !containsString(err.Error(), "failed to marshal event") {
		t.Errorf("error should contain 'failed to marshal event', got: %v", err)
	}
}

func TestEventDefinition_Publish_SubjectAndData(t *testing.T) {
	eventBus := &mockEventBus{}

	eventDef := types.NewEventDefinition[testEvent](
		"inventory",
		"StockUpdated",
		"v2",
		"events.inventory.v2.stock-updated",
	)

	event := testEvent{OrderID: "order-123", Amount: 99.99}
	err := eventDef.Publish(eventBus, event, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(eventBus.publishedMsgs) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(eventBus.publishedMsgs))
	}

	capturedMsg := eventBus.publishedMsgs[0]

	if capturedMsg.Subject != "events.inventory.v2.stock-updated" {
		t.Errorf("expected subject %q, got %q", "events.inventory.v2.stock-updated", capturedMsg.Subject)
	}
}

// =============================================================================
// EventDefinition[T].WithUnmarshaler Tests
// =============================================================================

func TestEventDefinition_WithUnmarshaler(t *testing.T) {
	// Custom unmarshaler that tracks if it was set
	customUnmarshal := func(data []byte, v any) error {
		return errors.New("custom unmarshaler called")
	}

	original := types.NewEventDefinition[testEvent](
		"order",
		"OrderCreated",
		"v1",
		"events.order.v1.order-created",
	)

	// Apply WithUnmarshaler and verify it returns a copy
	modified := original.WithUnmarshaler(customUnmarshal)

	// Verify the modified copy has the custom unmarshaler
	msg := &types.Msg{Data: []byte(`{}`)}
	_, err := modified.Unmarshal(msg)
	if err == nil || !containsString(err.Error(), "custom unmarshaler called") {
		t.Errorf("expected custom unmarshaler to be used, got error: %v", err)
	}

	// Verify original still uses default JSON unmarshaler
	_, err = original.Unmarshal(msg)
	if err != nil {
		t.Errorf("original should use default JSON unmarshaler, got error: %v", err)
	}
}

func TestEventDefinition_WithUnmarshaler_ChainedWithMarshaler(t *testing.T) {
	customMarshal := func(_ any) ([]byte, error) {
		return []byte("custom-data"), nil
	}
	customUnmarshal := func(data []byte, v any) error {
		if string(data) == "custom-data" {
			return nil
		}
		return errors.New("unexpected data")
	}

	eventDef := types.NewEventDefinition[testEvent](
		"order",
		"OrderCreated",
		"v1",
		"events.order.v1.order-created",
	).WithMarshaler(customMarshal).WithUnmarshaler(customUnmarshal)

	// Test marshaler is correctly set
	eventBus := &mockEventBus{}
	err := eventDef.Publish(eventBus, testEvent{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(eventBus.publishedMsgs) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(eventBus.publishedMsgs))
	}

	if string(eventBus.publishedMsgs[0].Data) != "custom-data" {
		t.Errorf("expected custom marshaled data, got: %s", string(eventBus.publishedMsgs[0].Data))
	}

	// Test unmarshaler is correctly set
	msg := &types.Msg{Data: []byte("custom-data")}
	_, err = eventDef.Unmarshal(msg)
	if err != nil {
		t.Errorf("expected custom unmarshaler to succeed, got error: %v", err)
	}
}

// =============================================================================
// EventDefinition[T].Unmarshal Tests
// =============================================================================

func TestEventDefinition_Unmarshal(t *testing.T) {
	eventDef := types.NewEventDefinition[testEvent](
		"order",
		"OrderCreated",
		"v1",
		"events.order.v1.order-created",
	)

	tests := []struct {
		name          string
		msgData       []byte
		expectError   bool
		errorContains string
		expectedEvent testEvent
	}{
		{
			name:        "successful unmarshal",
			msgData:     []byte(`{"order_id":"order-123","amount":99.99}`),
			expectError: false,
			expectedEvent: testEvent{
				OrderID: "order-123",
				Amount:  99.99,
			},
		},
		{
			name:          "invalid JSON returns error",
			msgData:       []byte(`invalid-json`),
			expectError:   true,
			errorContains: "failed to unmarshal event",
		},
		{
			name:        "empty JSON object returns zero values",
			msgData:     []byte(`{}`),
			expectError: false,
			expectedEvent: testEvent{
				OrderID: "",
				Amount:  0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &types.Msg{
				Subject: "events.order.v1.order-created",
				Data:    tt.msgData,
			}

			event, err := eventDef.Unmarshal(msg)

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
				t.Fatalf("unexpected error: %v", err)
			}

			if event.OrderID != tt.expectedEvent.OrderID {
				t.Errorf("expected OrderID %q, got %q", tt.expectedEvent.OrderID, event.OrderID)
			}

			if event.Amount != tt.expectedEvent.Amount {
				t.Errorf("expected Amount %v, got %v", tt.expectedEvent.Amount, event.Amount)
			}
		})
	}
}

func TestEventDefinition_Unmarshal_CustomUnmarshaler(t *testing.T) {
	customUnmarshalCalled := false
	customUnmarshal := func(data []byte, v any) error {
		customUnmarshalCalled = true
		// Parse custom format
		event, ok := v.(*testEvent)
		if !ok {
			return errors.New("unexpected type")
		}
		event.OrderID = "custom-parsed"
		event.Amount = 123.45
		return nil
	}

	eventDef := types.NewEventDefinition[testEvent](
		"order",
		"OrderCreated",
		"v1",
		"events.order.v1.order-created",
	).WithUnmarshaler(customUnmarshal)

	msg := &types.Msg{Data: []byte("custom-data")}
	event, err := eventDef.Unmarshal(msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !customUnmarshalCalled {
		t.Error("custom unmarshaler was not called")
	}

	if event.OrderID != "custom-parsed" {
		t.Errorf("expected OrderID %q, got %q", "custom-parsed", event.OrderID)
	}

	if event.Amount != 123.45 {
		t.Errorf("expected Amount %v, got %v", 123.45, event.Amount)
	}
}

// =============================================================================
// EventDefinition[T].ToBase Tests
// =============================================================================

func TestEventDefinition_ToBase(t *testing.T) {
	eventDef := types.NewEventDefinition[testEvent](
		"order",
		"OrderCreated",
		"v1",
		"events.order.v1.order-created",
	)

	baseDef := eventDef.ToBase()

	if baseDef.ModuleName != "order" {
		t.Errorf("expected ModuleName %q, got %q", "order", baseDef.ModuleName)
	}

	if baseDef.Name != "OrderCreated" {
		t.Errorf("expected Name %q, got %q", "OrderCreated", baseDef.Name)
	}

	if baseDef.Version != "v1" {
		t.Errorf("expected Version %q, got %q", "v1", baseDef.Version)
	}

	if baseDef.Subject != "events.order.v1.order-created" {
		t.Errorf("expected Subject %q, got %q", "events.order.v1.order-created", baseDef.Subject)
	}
}

func TestEventDefinition_ToBase_PreservesAllFields(t *testing.T) {
	tests := []struct {
		name       string
		moduleName string
		eventName  string
		version    string
		subject    string
	}{
		{
			name:       "standard event",
			moduleName: "inventory",
			eventName:  "StockUpdated",
			version:    "v2",
			subject:    "events.inventory.v2.stock-updated",
		},
		{
			name:       "event with empty subject",
			moduleName: "billing",
			eventName:  "InvoiceGenerated",
			version:    "v1",
			subject:    "",
		},
		{
			name:       "event with complex names",
			moduleName: "user-management",
			eventName:  "UserRegistrationCompleted",
			version:    "v3-beta",
			subject:    "events.user-management.v3-beta.user-registration-completed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventDef := types.NewEventDefinition[testEvent](
				tt.moduleName,
				tt.eventName,
				tt.version,
				tt.subject,
			)

			baseDef := eventDef.ToBase()

			if baseDef.ModuleName != tt.moduleName {
				t.Errorf("expected ModuleName %q, got %q", tt.moduleName, baseDef.ModuleName)
			}

			if baseDef.Name != tt.eventName {
				t.Errorf("expected Name %q, got %q", tt.eventName, baseDef.Name)
			}

			if baseDef.Version != tt.version {
				t.Errorf("expected Version %q, got %q", tt.version, baseDef.Version)
			}

			if baseDef.Subject != tt.subject {
				t.Errorf("expected Subject %q, got %q", tt.subject, baseDef.Subject)
			}
		})
	}
}

// =============================================================================
// Helper
// =============================================================================

func containsString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
