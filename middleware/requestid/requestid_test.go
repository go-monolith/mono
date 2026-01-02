package requestid

import (
	"context"
	"errors"
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

func TestNew(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		m, err := New()
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}
		if m.Name() != "requestid" {
			t.Errorf("expected name 'requestid', got %q", m.Name())
		}
		if m.headerName != "X-Request-ID" {
			t.Errorf("expected header name 'X-Request-ID', got %q", m.headerName)
		}
	})

	t.Run("custom header name", func(t *testing.T) {
		m, err := New(WithHeaderName("X-Trace-ID"))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}
		if m.headerName != "X-Trace-ID" {
			t.Errorf("expected header name 'X-Trace-ID', got %q", m.headerName)
		}
	})

	t.Run("empty header name returns error", func(t *testing.T) {
		_, err := New(WithHeaderName(""))
		if err == nil {
			t.Error("expected error for empty header name, got nil")
		}
	})
}

func TestGetRequestID(t *testing.T) {
	t.Run("returns ID from context", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), requestIDKey, "test-id-123")
		id := GetRequestID(ctx)
		if id != "test-id-123" {
			t.Errorf("expected 'test-id-123', got %q", id)
		}
	})

	t.Run("returns empty string when not set", func(t *testing.T) {
		id := GetRequestID(context.Background())
		if id != "" {
			t.Errorf("expected empty string, got %q", id)
		}
	})

	t.Run("returns empty string for wrong type", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), requestIDKey, 12345)
		id := GetRequestID(ctx)
		if id != "" {
			t.Errorf("expected empty string for wrong type, got %q", id)
		}
	})
}

func TestRequestReplyHandlerWrapping(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("extracts existing request ID from header", func(t *testing.T) {
		var capturedCtx context.Context
		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			capturedCtx = ctx
			return []byte("response"), nil
		}

		wrapped := m.wrapRequestReplyHandler(handler)

		req := &types.Msg{
			Data:   []byte("request"),
			Header: types.Header{"X-Request-ID": []string{"existing-id"}},
		}

		_, err := wrapped(context.Background(), req)
		if err != nil {
			t.Fatalf("handler failed: %v", err)
		}

		id := GetRequestID(capturedCtx)
		if id != "existing-id" {
			t.Errorf("expected 'existing-id', got %q", id)
		}
	})

	t.Run("generates UUID when header missing", func(t *testing.T) {
		var capturedCtx context.Context
		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			capturedCtx = ctx
			return []byte("response"), nil
		}

		wrapped := m.wrapRequestReplyHandler(handler)

		req := &types.Msg{
			Data: []byte("request"),
		}

		_, err := wrapped(context.Background(), req)
		if err != nil {
			t.Fatalf("handler failed: %v", err)
		}

		id := GetRequestID(capturedCtx)
		if id == "" {
			t.Error("expected generated UUID, got empty string")
		}
		// UUID format validation (basic)
		if len(id) != 36 {
			t.Errorf("expected UUID length 36, got %d", len(id))
		}
	})
}

func TestQueueGroupHandlerWrapping(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("extracts existing request ID from header", func(t *testing.T) {
		var capturedCtx context.Context
		handler := func(ctx context.Context, msg *types.Msg) error {
			capturedCtx = ctx
			return nil
		}

		wrapped := m.wrapQueueGroupHandler(handler)

		msg := &types.Msg{
			Data:   []byte("message"),
			Header: types.Header{"X-Request-ID": []string{"queue-id"}},
		}

		err := wrapped(context.Background(), msg)
		if err != nil {
			t.Fatalf("handler failed: %v", err)
		}

		id := GetRequestID(capturedCtx)
		if id != "queue-id" {
			t.Errorf("expected 'queue-id', got %q", id)
		}
	})

	t.Run("generates UUID when header missing", func(t *testing.T) {
		var capturedCtx context.Context
		handler := func(ctx context.Context, msg *types.Msg) error {
			capturedCtx = ctx
			return nil
		}

		wrapped := m.wrapQueueGroupHandler(handler)

		msg := &types.Msg{
			Data: []byte("message"),
		}

		err := wrapped(context.Background(), msg)
		if err != nil {
			t.Fatalf("handler failed: %v", err)
		}

		id := GetRequestID(capturedCtx)
		if id == "" {
			t.Error("expected generated UUID, got empty string")
		}
	})
}

func TestStreamConsumerHandlerWrapping(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("uses first message request ID", func(t *testing.T) {
		var capturedCtx context.Context
		handler := func(ctx context.Context, msgs []*types.Msg) error {
			capturedCtx = ctx
			return nil
		}

		wrapped := m.wrapStreamConsumerHandler(handler)

		msgs := []*types.Msg{
			{
				Data:   []byte("msg1"),
				Header: types.Header{"X-Request-ID": []string{"stream-id"}},
			},
			{
				Data: []byte("msg2"),
			},
		}

		err := wrapped(context.Background(), msgs)
		if err != nil {
			t.Fatalf("handler failed: %v", err)
		}

		id := GetRequestID(capturedCtx)
		if id != "stream-id" {
			t.Errorf("expected 'stream-id', got %q", id)
		}
	})

	t.Run("generates UUID when first message has no ID", func(t *testing.T) {
		var capturedCtx context.Context
		handler := func(ctx context.Context, msgs []*types.Msg) error {
			capturedCtx = ctx
			return nil
		}

		wrapped := m.wrapStreamConsumerHandler(handler)

		msgs := []*types.Msg{
			{Data: []byte("msg1")},
			{Data: []byte("msg2")},
		}

		err := wrapped(context.Background(), msgs)
		if err != nil {
			t.Fatalf("handler failed: %v", err)
		}

		id := GetRequestID(capturedCtx)
		if id == "" {
			t.Error("expected generated UUID, got empty string")
		}
	})

	t.Run("generates UUID for empty batch", func(t *testing.T) {
		var capturedCtx context.Context
		handler := func(ctx context.Context, msgs []*types.Msg) error {
			capturedCtx = ctx
			return nil
		}

		wrapped := m.wrapStreamConsumerHandler(handler)

		err := wrapped(context.Background(), []*types.Msg{})
		if err != nil {
			t.Fatalf("handler failed: %v", err)
		}

		id := GetRequestID(capturedCtx)
		if id == "" {
			t.Error("expected generated UUID, got empty string")
		}
	})
}

func TestOnOutgoingMessage(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("injects header from context", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), requestIDKey, "outgoing-id")

		msg := &types.Msg{
			Data:   []byte("data"),
			Header: make(types.Header),
		}

		octx := types.OutgoingMessageContext{
			ServiceType: types.ServiceTypeRequestReply,
			ServiceName: "test-service",
			ModuleName:  "test-module",
			Subject:     "test.subject",
			Msg:         msg,
			Ctx:         ctx,
			Metadata:    make(map[string]any),
		}

		result := m.OnOutgoingMessage(octx)

		if len(result.Msg.Header["X-Request-ID"]) == 0 {
			t.Error("expected X-Request-ID header to be set")
		}
		if result.Msg.Header["X-Request-ID"][0] != "outgoing-id" {
			t.Errorf("expected 'outgoing-id', got %q", result.Msg.Header["X-Request-ID"][0])
		}
	})

	t.Run("does not overwrite existing header", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), requestIDKey, "context-id")

		msg := &types.Msg{
			Data:   []byte("data"),
			Header: types.Header{"X-Request-ID": []string{"existing-header-id"}},
		}

		octx := types.OutgoingMessageContext{
			ServiceType: types.ServiceTypeRequestReply,
			Msg:         msg,
			Ctx:         ctx,
		}

		result := m.OnOutgoingMessage(octx)

		if result.Msg.Header["X-Request-ID"][0] != "existing-header-id" {
			t.Errorf("expected 'existing-header-id', got %q", result.Msg.Header["X-Request-ID"][0])
		}
	})

	t.Run("handles nil Header gracefully", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), requestIDKey, "test-id")

		msg := &types.Msg{
			Data:   []byte("data"),
			Header: nil,
		}

		octx := types.OutgoingMessageContext{
			ServiceType: types.ServiceTypeRequestReply,
			Msg:         msg,
			Ctx:         ctx,
		}

		result := m.OnOutgoingMessage(octx)

		if result.Msg.Header == nil {
			t.Error("expected Header to be initialized")
		}
		if result.Msg.Header["X-Request-ID"][0] != "test-id" {
			t.Errorf("expected 'test-id', got %q", result.Msg.Header["X-Request-ID"][0])
		}
	})

	t.Run("skips channel service type", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), requestIDKey, "test-id")

		msg := &types.Msg{
			Data:   []byte("data"),
			Header: make(types.Header),
		}

		octx := types.OutgoingMessageContext{
			ServiceType: types.ServiceTypeChannel,
			Msg:         msg,
			Ctx:         ctx,
		}

		result := m.OnOutgoingMessage(octx)

		if len(result.Msg.Header["X-Request-ID"]) > 0 {
			t.Error("expected no header for channel service type")
		}
	})

	t.Run("does nothing when context has no request ID", func(t *testing.T) {
		msg := &types.Msg{
			Data:   []byte("data"),
			Header: make(types.Header),
		}

		octx := types.OutgoingMessageContext{
			ServiceType: types.ServiceTypeRequestReply,
			Msg:         msg,
			Ctx:         context.Background(),
		}

		result := m.OnOutgoingMessage(octx)

		if len(result.Msg.Header["X-Request-ID"]) > 0 {
			t.Error("expected no header when context has no request ID")
		}
	})
}

func TestOnServiceRegistration(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("wraps RequestReply handler", func(t *testing.T) {
		handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
			return []byte("response"), nil
		}

		reg := types.ServiceRegistration{
			Type:           types.ServiceTypeRequestReply,
			RequestHandler: handler,
		}

		result := m.OnServiceRegistration(context.Background(), reg)

		if result.RequestHandler == nil {
			t.Error("expected wrapped handler, got nil")
		}
	})

	t.Run("wraps QueueGroup handlers", func(t *testing.T) {
		handler1 := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		handler2 := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}

		reg := types.ServiceRegistration{
			Type: types.ServiceTypeQueueGroup,
			QueueHandlers: []types.QGHP{
				{QueueGroup: "group1", Handler: handler1},
				{QueueGroup: "group2", Handler: handler2},
			},
		}

		result := m.OnServiceRegistration(context.Background(), reg)

		if len(result.QueueHandlers) != 2 {
			t.Errorf("expected 2 handlers, got %d", len(result.QueueHandlers))
		}
		if result.QueueHandlers[0].QueueGroup != "group1" {
			t.Errorf("expected 'group1', got %q", result.QueueHandlers[0].QueueGroup)
		}
	})

	t.Run("wraps StreamConsumer handler", func(t *testing.T) {
		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		reg := types.ServiceRegistration{
			Type:          types.ServiceTypeStreamConsumer,
			StreamHandler: handler,
		}

		result := m.OnServiceRegistration(context.Background(), reg)

		if result.StreamHandler == nil {
			t.Error("expected wrapped handler, got nil")
		}
	})

	t.Run("passes through Channel service unchanged", func(t *testing.T) {
		inChan := make(chan *types.Msg)
		outChan := make(chan *types.Msg)

		reg := types.ServiceRegistration{
			Type:       types.ServiceTypeChannel,
			InChannel:  inChan,
			OutChannel: outChan,
		}

		result := m.OnServiceRegistration(context.Background(), reg)

		if result.InChannel != inChan || result.OutChannel != outChan {
			t.Error("expected channel service to pass through unchanged")
		}
	})
}

func TestLifecycleMethods(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("Start succeeds", func(t *testing.T) {
		err := m.Start(context.Background())
		if err != nil {
			t.Errorf("Start() failed: %v", err)
		}
	})

	t.Run("Stop succeeds", func(t *testing.T) {
		err := m.Stop(context.Background())
		if err != nil {
			t.Errorf("Stop() failed: %v", err)
		}
	})
}

// TestOnModuleLifecycle tests the OnModuleLifecycle hook (pass-through behavior).
func TestOnModuleLifecycle(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tests := []struct {
		name  string
		event types.ModuleLifecycleEvent
	}{
		{
			name: "module started event",
			event: types.ModuleLifecycleEvent{
				Type:       types.ModuleStartedEvent,
				ModuleName: "test-module",
			},
		},
		{
			name: "module stopped event",
			event: types.ModuleLifecycleEvent{
				Type:       types.ModuleStoppedEvent,
				ModuleName: "another-module",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.OnModuleLifecycle(context.Background(), tt.event)

			// Verify event passes through unchanged
			if result.Type != tt.event.Type {
				t.Errorf("expected Type=%s, got %s", tt.event.Type, result.Type)
			}
			if result.ModuleName != tt.event.ModuleName {
				t.Errorf("expected ModuleName=%s, got %s", tt.event.ModuleName, result.ModuleName)
			}
		})
	}
}

// TestOnConfigurationChange tests the OnConfigurationChange hook (pass-through behavior).
func TestOnConfigurationChange(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tests := []struct {
		name  string
		event types.ConfigurationEvent
	}{
		{
			name: "configuration updated",
			event: types.ConfigurationEvent{
				Type:       types.ConfigurationUpdatedEvent,
				OptionName: "log_level",
				OldValue:   "info",
				NewValue:   "debug",
			},
		},
		{
			name: "configuration changed",
			event: types.ConfigurationEvent{
				Type:       types.ConfigurationUpdatedEvent,
				OptionName: "feature_flag",
				OldValue:   true,
				NewValue:   false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.OnConfigurationChange(context.Background(), tt.event)

			// Verify event passes through unchanged
			if result.Type != tt.event.Type {
				t.Errorf("expected Type=%s, got %s", tt.event.Type, result.Type)
			}
			if result.OptionName != tt.event.OptionName {
				t.Errorf("expected OptionName=%s, got %s", tt.event.OptionName, result.OptionName)
			}
			if result.OldValue != tt.event.OldValue {
				t.Errorf("expected OldValue=%v, got %v", tt.event.OldValue, result.OldValue)
			}
			if result.NewValue != tt.event.NewValue {
				t.Errorf("expected NewValue=%v, got %v", tt.event.NewValue, result.NewValue)
			}
		})
	}
}

// TestOnEventConsumerRegistration tests the OnEventConsumerRegistration hook.
func TestOnEventConsumerRegistration(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("wraps handler with request ID extraction", func(t *testing.T) {
		var capturedRequestID string
		originalHandler := func(ctx context.Context, msg *types.Msg) error {
			capturedRequestID = GetRequestID(ctx)
			return nil
		}

		entry := types.EventConsumerEntry{
			Handler: originalHandler,
		}

		result := m.OnEventConsumerRegistration(context.Background(), entry)

		if result.Handler == nil {
			t.Fatal("expected wrapped handler, got nil")
		}

		// Test with request ID in header
		msg := &types.Msg{
			Header: types.Header{
				"X-Request-ID": []string{"test-request-123"},
			},
		}

		err := result.Handler(context.Background(), msg)
		if err != nil {
			t.Errorf("handler failed: %v", err)
		}

		if capturedRequestID != "test-request-123" {
			t.Errorf("expected request ID 'test-request-123', got '%s'", capturedRequestID)
		}
	})

	t.Run("generates request ID when missing", func(t *testing.T) {
		var capturedRequestID string
		originalHandler := func(ctx context.Context, msg *types.Msg) error {
			capturedRequestID = GetRequestID(ctx)
			return nil
		}

		entry := types.EventConsumerEntry{
			Handler: originalHandler,
		}

		result := m.OnEventConsumerRegistration(context.Background(), entry)

		// Test without request ID header
		msg := &types.Msg{
			Header: types.Header{},
		}

		err := result.Handler(context.Background(), msg)
		if err != nil {
			t.Errorf("handler failed: %v", err)
		}

		if capturedRequestID == "" {
			t.Error("expected generated request ID, got empty string")
		}

		// Verify it's a valid UUID format (36 characters with dashes)
		if len(capturedRequestID) != 36 {
			t.Errorf("expected UUID format (36 chars), got %d chars: %s", len(capturedRequestID), capturedRequestID)
		}
	})

	t.Run("propagates handler errors", func(t *testing.T) {
		expectedErr := errors.New("handler error")
		originalHandler := func(ctx context.Context, msg *types.Msg) error {
			return expectedErr
		}

		entry := types.EventConsumerEntry{
			Handler: originalHandler,
		}

		result := m.OnEventConsumerRegistration(context.Background(), entry)

		msg := &types.Msg{Header: types.Header{}}

		err := result.Handler(context.Background(), msg)
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("handles nil header gracefully", func(t *testing.T) {
		var capturedRequestID string
		originalHandler := func(ctx context.Context, msg *types.Msg) error {
			capturedRequestID = GetRequestID(ctx)
			return nil
		}

		entry := types.EventConsumerEntry{
			Handler: originalHandler,
		}

		result := m.OnEventConsumerRegistration(context.Background(), entry)

		// Test with nil header
		msg := &types.Msg{
			Header: nil,
		}

		err := result.Handler(context.Background(), msg)
		if err != nil {
			t.Errorf("handler failed: %v", err)
		}

		if capturedRequestID == "" {
			t.Error("expected generated request ID when header is nil")
		}
	})
}

// TestOnEventStreamConsumerRegistration tests the OnEventStreamConsumerRegistration hook.
func TestOnEventStreamConsumerRegistration(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("wraps handler with request ID from first message", func(t *testing.T) {
		var capturedRequestID string
		originalHandler := func(ctx context.Context, msgs []*types.Msg) error {
			capturedRequestID = GetRequestID(ctx)
			return nil
		}

		entry := types.EventStreamConsumerEntry{
			Handler: originalHandler,
		}

		result := m.OnEventStreamConsumerRegistration(context.Background(), entry)

		if result.Handler == nil {
			t.Fatal("expected wrapped handler, got nil")
		}

		// Test with request ID in first message
		msgs := []*types.Msg{
			{
				Header: types.Header{
					"X-Request-ID": []string{"batch-request-456"},
				},
			},
			{
				Header: types.Header{
					"X-Request-ID": []string{"different-id-789"},
				},
			},
		}

		err := result.Handler(context.Background(), msgs)
		if err != nil {
			t.Errorf("handler failed: %v", err)
		}

		// Should use request ID from first message
		if capturedRequestID != "batch-request-456" {
			t.Errorf("expected request ID 'batch-request-456', got '%s'", capturedRequestID)
		}
	})

	t.Run("generates request ID when batch is empty", func(t *testing.T) {
		var capturedRequestID string
		originalHandler := func(ctx context.Context, msgs []*types.Msg) error {
			capturedRequestID = GetRequestID(ctx)
			return nil
		}

		entry := types.EventStreamConsumerEntry{
			Handler: originalHandler,
		}

		result := m.OnEventStreamConsumerRegistration(context.Background(), entry)

		// Test with empty batch
		msgs := []*types.Msg{}

		err := result.Handler(context.Background(), msgs)
		if err != nil {
			t.Errorf("handler failed: %v", err)
		}

		if capturedRequestID == "" {
			t.Error("expected generated request ID for empty batch, got empty string")
		}
	})

	t.Run("generates request ID when first message has no header", func(t *testing.T) {
		var capturedRequestID string
		originalHandler := func(ctx context.Context, msgs []*types.Msg) error {
			capturedRequestID = GetRequestID(ctx)
			return nil
		}

		entry := types.EventStreamConsumerEntry{
			Handler: originalHandler,
		}

		result := m.OnEventStreamConsumerRegistration(context.Background(), entry)

		// Test with first message having nil header
		msgs := []*types.Msg{
			{
				Header: nil,
			},
		}

		err := result.Handler(context.Background(), msgs)
		if err != nil {
			t.Errorf("handler failed: %v", err)
		}

		if capturedRequestID == "" {
			t.Error("expected generated request ID when first message has no header")
		}
	})

	t.Run("propagates handler errors", func(t *testing.T) {
		expectedErr := errors.New("stream handler error")
		originalHandler := func(ctx context.Context, msgs []*types.Msg) error {
			return expectedErr
		}

		entry := types.EventStreamConsumerEntry{
			Handler: originalHandler,
		}

		result := m.OnEventStreamConsumerRegistration(context.Background(), entry)

		msgs := []*types.Msg{{Header: types.Header{}}}

		err := result.Handler(context.Background(), msgs)
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})
}

// TestExtractRequestID tests the extractRequestID method edge cases.
func TestExtractRequestID(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("extracts from valid header", func(t *testing.T) {
		msg := &types.Msg{
			Header: types.Header{
				"X-Request-ID": []string{"req-123"},
			},
		}

		requestID := m.extractRequestID(msg)
		if requestID != "req-123" {
			t.Errorf("expected 'req-123', got '%s'", requestID)
		}
	})

	t.Run("returns empty for nil header", func(t *testing.T) {
		msg := &types.Msg{
			Header: nil,
		}

		requestID := m.extractRequestID(msg)
		if requestID != "" {
			t.Errorf("expected empty string, got '%s'", requestID)
		}
	})

	t.Run("returns empty for missing header key", func(t *testing.T) {
		msg := &types.Msg{
			Header: types.Header{
				"Other-Header": []string{"value"},
			},
		}

		requestID := m.extractRequestID(msg)
		if requestID != "" {
			t.Errorf("expected empty string, got '%s'", requestID)
		}
	})

	t.Run("returns empty for empty header value", func(t *testing.T) {
		msg := &types.Msg{
			Header: types.Header{
				"X-Request-ID": []string{},
			},
		}

		requestID := m.extractRequestID(msg)
		if requestID != "" {
			t.Errorf("expected empty string, got '%s'", requestID)
		}
	})
}
