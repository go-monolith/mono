package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// mockServiceContainer implements mono.ServiceContainer for testing.
type mockServiceContainer struct {
	channelServices map[string]struct {
		in  chan *types.Msg
		out chan *types.Msg
	}
	mu sync.Mutex
}

func newMockServiceContainer() *mockServiceContainer {
	return &mockServiceContainer{
		channelServices: make(map[string]struct {
			in  chan *types.Msg
			out chan *types.Msg
		}),
	}
}

func (m *mockServiceContainer) BindModule(_ types.Module) error { return nil }
func (m *mockServiceContainer) SetEventBus(_ types.EventBus)    {}
func (m *mockServiceContainer) SetQueueGroupOptimisticWindow(_ time.Duration) {
}
func (m *mockServiceContainer) SetMiddlewareChain(_ types.MiddlewareChainRunner) {}

func (m *mockServiceContainer) RegisterChannelService(name string, in chan *types.Msg, out chan *types.Msg) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.channelServices[name] = struct {
		in  chan *types.Msg
		out chan *types.Msg
	}{in: in, out: out}
	return nil
}

func (m *mockServiceContainer) GetChannelService(name string, consumerModule string) (chan *types.Msg, chan *types.Msg, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if svc, ok := m.channelServices[name]; ok {
		return svc.in, svc.out, nil
	}
	return nil, nil, errors.ErrServiceNotFound
}

func (m *mockServiceContainer) MustGetChannelService(name string, consumerModule string) (chan *types.Msg, chan *types.Msg) {
	in, out, err := m.GetChannelService(name, consumerModule)
	if err != nil {
		panic(err)
	}
	return in, out
}

func (m *mockServiceContainer) RegisterRequestReplyService(_ string, _ types.RequestReplyHandler) error {
	return nil
}
func (m *mockServiceContainer) GetRequestReplyService(_ string) (types.RequestReplyServiceClient, error) {
	return nil, errors.ErrServiceNotFound
}
func (m *mockServiceContainer) MustGetRequestReplyService(_ string) types.RequestReplyServiceClient {
	return nil
}
func (m *mockServiceContainer) RegisterQueueGroupService(_ string, _ ...types.QGHP) error {
	return nil
}
func (m *mockServiceContainer) GetQueueGroupService(_ string) (types.QueueGroupServiceClient, error) {
	return nil, errors.ErrServiceNotFound
}
func (m *mockServiceContainer) MustGetQueueGroupService(_ string) types.QueueGroupServiceClient {
	return nil
}
func (m *mockServiceContainer) RegisterStreamConsumerService(_ string, _ types.StreamConsumerConfig, _ types.StreamConsumerHandler) error {
	return nil
}
func (m *mockServiceContainer) RegisterCronService(_ string, _ types.CronServiceConfig, _ types.CronHandler) error {
	return nil
}
func (m *mockServiceContainer) GetStreamConsumerService(_ string) (types.StreamConsumerServiceClient, error) {
	return nil, errors.ErrServiceNotFound
}
func (m *mockServiceContainer) Has(_ string) bool { return false }
func (m *mockServiceContainer) Unregister(_ string) error {
	return nil
}
func (m *mockServiceContainer) Entries() []*types.ServiceEntry {
	return nil
}
func (m *mockServiceContainer) StartChannelRouters(_ context.Context) {}

// syncWriter wraps a bytes.Buffer with mutex for thread-safe writes.
type syncWriter struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (w *syncWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// TestNew tests the audit module constructor.
func TestNew(t *testing.T) {
	t.Run("requires output option", func(t *testing.T) {
		_, err := New()
		if err == nil {
			t.Error("expected error when WithOutput is not provided")
		}
		if !strings.Contains(err.Error(), "WithOutput") {
			t.Errorf("expected error to mention WithOutput, got: %v", err)
		}
	})

	t.Run("with output option", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if m == nil {
			t.Fatal("expected non-nil module")
		}
		if m.hashChain != nil {
			t.Error("hash chain should be nil when not enabled")
		}
	})

	t.Run("with hash chaining enabled", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(
			WithOutput(buf),
			WithHashChaining(""),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if m.hashChain == nil {
			t.Error("hash chain should be enabled")
		}
	})

	t.Run("with initial hash", func(t *testing.T) {
		buf := &bytes.Buffer{}
		initialHash := "abc123"
		m, err := New(
			WithOutput(buf),
			WithHashChaining(initialHash),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if m.hashChain == nil {
			t.Error("hash chain should be enabled")
		}
	})

	t.Run("with user context function", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(
			WithOutput(buf),
			WithUserContext(func(_ context.Context) string {
				return "test-user"
			}),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if m.userCtxFunc == nil {
			t.Error("user context function should be set")
		}
	})

	t.Run("nil output returns error", func(t *testing.T) {
		_, err := New(WithOutput(nil))
		if err == nil {
			t.Error("expected error for nil output")
		}
	})

	t.Run("nil user context func returns error", func(t *testing.T) {
		buf := &bytes.Buffer{}
		_, err := New(
			WithOutput(buf),
			WithUserContext(nil),
		)
		if err == nil {
			t.Error("expected error for nil user context func")
		}
	})
}

func TestModule_Name(t *testing.T) {
	buf := &bytes.Buffer{}
	m, err := New(WithOutput(buf))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if m.Name() != "audit" {
		t.Errorf("expected name 'audit', got %q", m.Name())
	}
}

func TestModule_StartStop(t *testing.T) {
	t.Run("start initializes module", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = m.Start(context.Background())
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		if !m.started {
			t.Error("module should be marked as started")
		}

		// Clean up
		_ = m.Stop(context.Background())
	})

	t.Run("double start returns error", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = m.Start(context.Background())
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		err = m.Start(context.Background())
		if err == nil {
			t.Error("expected error on double start")
		}

		// Clean up
		_ = m.Stop(context.Background())
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = m.Start(context.Background())
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// Multiple stops should not panic or error
		err1 := m.Stop(context.Background())
		err2 := m.Stop(context.Background())

		if err1 != nil {
			t.Errorf("first stop failed: %v", err1)
		}
		if err2 != nil {
			t.Errorf("second stop failed: %v", err2)
		}
	})
}

func TestModule_OnModuleLifecycle(t *testing.T) {
	t.Run("logs module started event", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		event := types.ModuleLifecycleEvent{
			Type:       types.ModuleStartedEvent,
			ModuleName: "test-module",
			Duration:   100 * time.Millisecond,
		}

		result := m.OnModuleLifecycle(context.Background(), event)

		// Verify event is passed through unchanged
		if result.Type != event.Type {
			t.Error("event should be passed through unchanged")
		}
		if result.ModuleName != event.ModuleName {
			t.Error("module name should be unchanged")
		}

		// Verify log entry
		logContent := buf.String()
		if !strings.Contains(logContent, "module.started") {
			t.Error("log should contain module.started event")
		}
		if !strings.Contains(logContent, "test-module") {
			t.Error("log should contain module name")
		}
	})

	t.Run("logs module stopped event", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		event := types.ModuleLifecycleEvent{
			Type:       types.ModuleStoppedEvent,
			ModuleName: "test-module",
		}

		result := m.OnModuleLifecycle(context.Background(), event)

		if result.Type != event.Type {
			t.Error("event should be passed through unchanged")
		}

		logContent := buf.String()
		if !strings.Contains(logContent, "module.stopped") {
			t.Error("log should contain module.stopped event")
		}
	})

	t.Run("logs error on module stopped with error", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		event := types.ModuleLifecycleEvent{
			Type:       types.ModuleStoppedEvent,
			ModuleName: "test-module",
			Error:      io.EOF,
		}

		m.OnModuleLifecycle(context.Background(), event)

		logContent := buf.String()
		if !strings.Contains(logContent, "EOF") {
			t.Error("log should contain error message")
		}
	})

	t.Run("ignores unknown event types", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Use an unknown event type to test that it's passed through unchanged
		event := types.ModuleLifecycleEvent{
			Type:       types.ModuleLifecycleEventType("unknown.event"),
			ModuleName: "test-module",
		}

		result := m.OnModuleLifecycle(context.Background(), event)

		if result.Type != event.Type {
			t.Error("event should be passed through unchanged")
		}

		logContent := buf.String()
		if logContent != "" {
			t.Error("unknown event types should not be logged")
		}
	})

	t.Run("user context is added", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(
			WithOutput(buf),
			WithUserContext(func(_ context.Context) string {
				return "test-user-123"
			}),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		event := types.ModuleLifecycleEvent{
			Type:       types.ModuleStartedEvent,
			ModuleName: "test-module",
		}

		m.OnModuleLifecycle(context.Background(), event)

		logContent := buf.String()
		if !strings.Contains(logContent, "test-user-123") {
			t.Error("log should contain user context")
		}
	})
}

func TestModule_OnServiceRegistration(t *testing.T) {
	t.Run("logs service registration", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		reg := types.ServiceRegistration{
			Name:       "test-service",
			ModuleName: "test-module",
			Type:       types.ServiceTypeRequestReply,
		}

		result := m.OnServiceRegistration(context.Background(), reg)

		if result.Name != reg.Name {
			t.Error("registration should be passed through unchanged")
		}

		logContent := buf.String()
		if !strings.Contains(logContent, "service.registered") {
			t.Error("log should contain service.registered event")
		}
		if !strings.Contains(logContent, "test-service") {
			t.Error("log should contain service name")
		}
		if !strings.Contains(logContent, "test-module") {
			t.Error("log should contain module name")
		}
	})

	t.Run("includes service type in details", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		reg := types.ServiceRegistration{
			Name:       "test-service",
			ModuleName: "test-module",
			Type:       types.ServiceTypeQueueGroup,
		}

		m.OnServiceRegistration(context.Background(), reg)

		logContent := buf.String()
		// The FormatServiceType should format queue_group type
		if !strings.Contains(logContent, "queue_group") {
			t.Error("log should contain service type")
		}
	})
}

func TestModule_OnConfigurationChange(t *testing.T) {
	t.Run("logs configuration change", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		event := types.ConfigurationEvent{
			OptionName: "log_level",
			OldValue:   "info",
			NewValue:   "debug",
		}

		result := m.OnConfigurationChange(context.Background(), event)

		if result.OptionName != event.OptionName {
			t.Error("event should be passed through unchanged")
		}

		logContent := buf.String()
		if !strings.Contains(logContent, "configuration.updated") {
			t.Error("log should contain configuration.updated event")
		}
		if !strings.Contains(logContent, "log_level") {
			t.Error("log should contain option name")
		}
	})

	t.Run("redacts sensitive values", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		event := types.ConfigurationEvent{
			OptionName: "password",
			OldValue:   "secret123",
			NewValue:   "newsecret456",
		}

		m.OnConfigurationChange(context.Background(), event)

		logContent := buf.String()
		// Values should be redacted
		if strings.Contains(logContent, "secret123") {
			t.Error("old password value should be redacted")
		}
		if strings.Contains(logContent, "newsecret456") {
			t.Error("new password value should be redacted")
		}
	})
}

func TestModule_RegisterServices(t *testing.T) {
	t.Run("registers audit-trail channel service", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		container := newMockServiceContainer()
		err = m.RegisterServices(container)
		if err != nil {
			t.Fatalf("RegisterServices failed: %v", err)
		}

		// Verify service was registered
		in, out, err := container.GetChannelService("audit-trail", "test-consumer")
		if err != nil {
			t.Fatalf("failed to get audit-trail service: %v", err)
		}
		if in == nil || out == nil {
			t.Error("channels should not be nil")
		}
	})
}

func TestModule_HandleAuditTrailChannel(t *testing.T) {
	t.Run("processes custom audit entries", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = m.Start(context.Background())
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		defer m.Stop(context.Background())

		// Send a custom audit entry
		entry := Entry{
			EventType:   EventCustomAuditTrail,
			ModuleName:  "custom-module",
			ServiceName: "custom-service",
			Details: map[string]any{
				"action": "test-action",
			},
		}
		data, _ := json.Marshal(entry)

		msg := &types.Msg{
			Subject: "audit-trail",
			Reply:   "audit-trail.reply",
			Data:    data,
		}

		// Send message
		m.auditTrailIn <- msg

		// Wait for response
		select {
		case resp := <-m.auditTrailOut:
			var response SaveEntryResponse
			err := json.Unmarshal(resp.Data, &response)
			if err != nil {
				t.Fatalf("failed to unmarshal response: %v", err)
			}
			if !response.Success {
				t.Errorf("expected success, got error: %s", response.Error)
			}
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for response")
		}

		// Verify entry was written
		logContent := buf.String()
		if !strings.Contains(logContent, "custom-module") {
			t.Error("log should contain custom module name")
		}
		if !strings.Contains(logContent, "custom.audit_trail") {
			t.Error("log should contain custom event type")
		}
	})

	t.Run("handles invalid JSON", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = m.Start(context.Background())
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		defer m.Stop(context.Background())

		msg := &types.Msg{
			Subject: "audit-trail",
			Reply:   "audit-trail.reply",
			Data:    []byte("invalid json"),
		}

		m.auditTrailIn <- msg

		select {
		case resp := <-m.auditTrailOut:
			var response SaveEntryResponse
			err := json.Unmarshal(resp.Data, &response)
			if err != nil {
				t.Fatalf("failed to unmarshal response: %v", err)
			}
			if response.Success {
				t.Error("expected error for invalid JSON")
			}
			if response.Error == "" {
				t.Error("error message should not be empty")
			}
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for response")
		}
	})

	t.Run("sets timestamp automatically", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = m.Start(context.Background())
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		defer m.Stop(context.Background())

		// Entry without timestamp
		entry := Entry{
			ModuleName: "test-module",
		}
		data, _ := json.Marshal(entry)

		msg := &types.Msg{
			Subject: "audit-trail",
			Reply:   "audit-trail.reply",
			Data:    data,
		}

		beforeSend := time.Now().UTC()
		m.auditTrailIn <- msg

		select {
		case <-m.auditTrailOut:
			// Response received
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for response")
		}
		afterSend := time.Now().UTC()

		// Parse the log entry
		logContent := buf.String()
		var logEntry Entry
		if err := json.Unmarshal([]byte(strings.TrimSpace(logContent)), &logEntry); err != nil {
			t.Fatalf("failed to parse log entry: %v", err)
		}

		if logEntry.Timestamp.Before(beforeSend.Add(-time.Second)) ||
			logEntry.Timestamp.After(afterSend.Add(time.Second)) {
			t.Error("timestamp should be set to current time")
		}
	})

	t.Run("sets default event type", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = m.Start(context.Background())
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		defer m.Stop(context.Background())

		// Entry without event type
		entry := Entry{
			ModuleName: "test-module",
		}
		data, _ := json.Marshal(entry)

		msg := &types.Msg{
			Subject: "audit-trail",
			Reply:   "audit-trail.reply",
			Data:    data,
		}

		m.auditTrailIn <- msg

		select {
		case <-m.auditTrailOut:
			// Response received
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for response")
		}

		logContent := buf.String()
		if !strings.Contains(logContent, "custom.audit_trail") {
			t.Error("default event type should be custom.audit_trail")
		}
	})

	t.Run("async request no response", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = m.Start(context.Background())
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		defer m.Stop(context.Background())

		// Entry without Reply (async)
		entry := Entry{
			ModuleName: "async-module",
		}
		data, _ := json.Marshal(entry)

		msg := &types.Msg{
			Subject: "audit-trail",
			// No Reply field - async request
			Data: data,
		}

		m.auditTrailIn <- msg

		// Wait a bit for processing
		time.Sleep(100 * time.Millisecond)

		// Verify no response was sent (channel should be empty)
		select {
		case <-m.auditTrailOut:
			t.Error("no response should be sent for async requests")
		default:
			// Expected - no response
		}

		// But entry should still be written
		logContent := buf.String()
		if !strings.Contains(logContent, "async-module") {
			t.Error("async entry should still be written to log")
		}
	})
}

func TestModule_HashChainingIntegration(t *testing.T) {
	t.Run("entries are chained", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(
			WithOutput(buf),
			WithHashChaining(""),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Write multiple entries
		events := []types.ModuleLifecycleEvent{
			{Type: types.ModuleStartedEvent, ModuleName: "module-a"},
			{Type: types.ModuleStartedEvent, ModuleName: "module-b"},
			{Type: types.ModuleStartedEvent, ModuleName: "module-c"},
		}

		for _, event := range events {
			m.OnModuleLifecycle(context.Background(), event)
		}

		// Parse entries
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		var entries []Entry
		for _, line := range lines {
			if line == "" {
				continue
			}
			var entry Entry
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				t.Fatalf("failed to parse entry: %v", err)
			}
			entries = append(entries, entry)
		}

		// Verify chain
		err = VerifyChain(entries)
		if err != nil {
			t.Errorf("chain verification failed: %v", err)
		}
	})

	t.Run("entries without hash chaining have empty hashes", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		event := types.ModuleLifecycleEvent{
			Type:       types.ModuleStartedEvent,
			ModuleName: "test-module",
		}
		m.OnModuleLifecycle(context.Background(), event)

		var entry Entry
		if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry); err != nil {
			t.Fatalf("failed to parse entry: %v", err)
		}

		if entry.EntryHash != "" {
			t.Error("entry_hash should be empty when hash chaining is disabled")
		}
		if entry.PrevHash != "" {
			t.Error("prev_hash should be empty when hash chaining is disabled")
		}
	})
}

func TestModule_ConcurrentWrites(t *testing.T) {
	buf := &syncWriter{}
	m, err := New(
		WithOutput(buf),
		WithHashChaining(""),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	const numGoroutines = 50
	var wg sync.WaitGroup

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			event := types.ModuleLifecycleEvent{
				Type:       types.ModuleStartedEvent,
				ModuleName: fmt.Sprintf("module-%d", idx),
				Duration:   time.Duration(idx) * time.Millisecond,
			}
			m.OnModuleLifecycle(context.Background(), event)
		}(i)
	}

	wg.Wait()

	// Parse entries
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != numGoroutines {
		t.Errorf("expected %d entries, got %d", numGoroutines, len(lines))
	}

	// Verify all entries are valid JSON
	for i, line := range lines {
		if line == "" {
			continue
		}
		var entry Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Errorf("entry %d is not valid JSON: %v", i, err)
		}
	}
}

// TestOnOutgoingMessage tests the OnOutgoingMessage hook (pass-through behavior).
func TestOnOutgoingMessage(t *testing.T) {
	buf := &bytes.Buffer{}
	m, err := New(WithOutput(buf))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tests := []struct {
		name string
		octx types.OutgoingMessageContext
	}{
		{
			name: "passes through request-reply context",
			octx: types.OutgoingMessageContext{
				ServiceType: types.ServiceTypeRequestReply,
				ServiceName: "test-service",
			},
		},
		{
			name: "passes through queue group context",
			octx: types.OutgoingMessageContext{
				ServiceType: types.ServiceTypeQueueGroup,
				ServiceName: "queue-service",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.OnOutgoingMessage(tt.octx)

			// Verify context passes through unchanged
			if result.ServiceType != tt.octx.ServiceType {
				t.Errorf("expected ServiceType=%v, got %v", tt.octx.ServiceType, result.ServiceType)
			}
			if result.ServiceName != tt.octx.ServiceName {
				t.Errorf("expected ServiceName=%s, got %s", tt.octx.ServiceName, result.ServiceName)
			}
		})
	}
}

// TestOnEventConsumerRegistration tests the OnEventConsumerRegistration hook.
func TestOnEventConsumerRegistration(t *testing.T) {
	buf := &bytes.Buffer{}
	m, err := New(WithOutput(buf))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Create mock module
	mockModule := &mockModule{name: "test-module"}

	tests := []struct {
		name      string
		entry     types.EventConsumerEntry
		wantEvent EventType
	}{
		{
			name: "logs event consumer registration",
			entry: types.EventConsumerEntry{
				EventDef: types.BaseEventDefinition{
					Name:       "OrderCreated",
					Version:    "v1",
					ModuleName: "orders",
				},
				Module:     mockModule,
				QueueGroup: "order-processors",
			},
			wantEvent: EventServiceRegistered,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()

			result := m.OnEventConsumerRegistration(context.Background(), tt.entry)

			// Verify entry passes through unchanged
			if result.EventDef.Name != tt.entry.EventDef.Name {
				t.Errorf("expected EventDef.Name=%s, got %s", tt.entry.EventDef.Name, result.EventDef.Name)
			}
			if result.Module.Name() != tt.entry.Module.Name() {
				t.Errorf("expected Module.Name=%s, got %s", tt.entry.Module.Name(), result.Module.Name())
			}

			// Verify audit entry was written
			if buf.Len() == 0 {
				t.Fatal("expected audit entry to be written")
			}

			var auditEntry Entry
			if err := json.Unmarshal(buf.Bytes(), &auditEntry); err != nil {
				t.Fatalf("failed to parse audit entry: %v", err)
			}

			if auditEntry.EventType != tt.wantEvent {
				t.Errorf("expected EventType=%s, got %s", tt.wantEvent, auditEntry.EventType)
			}
			if auditEntry.ServiceName != tt.entry.EventDef.Name {
				t.Errorf("expected ServiceName=%s, got %s", tt.entry.EventDef.Name, auditEntry.ServiceName)
			}
			if auditEntry.ModuleName != tt.entry.Module.Name() {
				t.Errorf("expected ModuleName=%s, got %s", tt.entry.Module.Name(), auditEntry.ModuleName)
			}

			// Verify details
			details := auditEntry.Details
			if details["service_type"] != "event_consumer" {
				t.Errorf("expected service_type=event_consumer, got %v", details["service_type"])
			}
			if details["event_name"] != tt.entry.EventDef.Name {
				t.Errorf("expected event_name=%s, got %v", tt.entry.EventDef.Name, details["event_name"])
			}
			if details["queue_group"] != tt.entry.QueueGroup {
				t.Errorf("expected queue_group=%s, got %v", tt.entry.QueueGroup, details["queue_group"])
			}
		})
	}
}

// TestOnEventStreamConsumerRegistration tests the OnEventStreamConsumerRegistration hook.
func TestOnEventStreamConsumerRegistration(t *testing.T) {
	buf := &bytes.Buffer{}
	m, err := New(WithOutput(buf))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Create mock module
	mockModule := &mockModule{name: "analytics-module"}

	tests := []struct {
		name      string
		entry     types.EventStreamConsumerEntry
		wantEvent EventType
	}{
		{
			name: "logs event stream consumer registration",
			entry: types.EventStreamConsumerEntry{
				EventDef: types.BaseEventDefinition{
					Name:       "OrderShipped",
					Version:    "v2",
					ModuleName: "shipping",
				},
				Module: mockModule,
				Config: types.StreamConsumerConfig{
					Stream: types.StreamConfig{
						Name: "EVENTS",
					},
				},
				SequenceID: 123,
			},
			wantEvent: EventServiceRegistered,
		},
		{
			name: "handles nil module gracefully",
			entry: types.EventStreamConsumerEntry{
				EventDef: types.BaseEventDefinition{
					Name:       "PaymentProcessed",
					Version:    "v1",
					ModuleName: "payments",
				},
				Module: nil,
				Config: types.StreamConsumerConfig{
					Stream: types.StreamConfig{
						Name: "PAYMENTS",
					},
				},
				SequenceID: 456,
			},
			wantEvent: EventServiceRegistered,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()

			result := m.OnEventStreamConsumerRegistration(context.Background(), tt.entry)

			// Verify entry passes through unchanged
			if result.EventDef.Name != tt.entry.EventDef.Name {
				t.Errorf("expected EventDef.Name=%s, got %s", tt.entry.EventDef.Name, result.EventDef.Name)
			}

			// Verify audit entry was written
			if buf.Len() == 0 {
				t.Fatal("expected audit entry to be written")
			}

			var auditEntry Entry
			if err := json.Unmarshal(buf.Bytes(), &auditEntry); err != nil {
				t.Fatalf("failed to parse audit entry: %v", err)
			}

			if auditEntry.EventType != tt.wantEvent {
				t.Errorf("expected EventType=%s, got %s", tt.wantEvent, auditEntry.EventType)
			}

			// Verify details
			details := auditEntry.Details
			if details["service_type"] != "event_stream_consumer" {
				t.Errorf("expected service_type=event_stream_consumer, got %v", details["service_type"])
			}
			if details["stream_name"] != tt.entry.Config.Stream.Name {
				t.Errorf("expected stream_name=%s, got %v", tt.entry.Config.Stream.Name, details["stream_name"])
			}
			if tt.entry.Module == nil {
				if auditEntry.ModuleName != "<unknown>" {
					t.Errorf("expected ModuleName=<unknown> for nil module, got %s", auditEntry.ModuleName)
				}
			} else {
				if auditEntry.ModuleName != tt.entry.Module.Name() {
					t.Errorf("expected ModuleName=%s, got %s", tt.entry.Module.Name(), auditEntry.ModuleName)
				}
			}
		})
	}
}

// mockModule implements types.Module for testing.
type mockModule struct {
	name string
}

func (m *mockModule) Name() string {
	return m.name
}

func (m *mockModule) Start(ctx context.Context) error {
	return nil
}

func (m *mockModule) Stop(ctx context.Context) error {
	return nil
}

// TestWriteEntry_ErrorPaths tests writeEntry() error handling.
func TestWriteEntry_ErrorPaths(t *testing.T) {
	t.Run("handles JSON marshal error gracefully", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		// Create entry with unmarshalable Details (channel type cannot be marshaled)
		entry := Entry{
			Timestamp:   time.Now().UTC(),
			EventType:   EventCustomAuditTrail,
			ModuleName:  "test",
			ServiceName: "test-service",
			Details: map[string]any{
				"channel": make(chan int), // This will cause json.Marshal to fail
			},
		}

		// This should not panic, but log error to buffer
		m.writeEntry(context.Background(), entry)

		// Verify error was written to buffer
		output := buf.String()
		if !strings.Contains(output, "error") || !strings.Contains(output, "failed to marshal") {
			t.Errorf("expected marshal error to be written, got: %s", output)
		}
	})

	t.Run("handles writer error gracefully", func(t *testing.T) {
		// Use a writer that always fails
		failWriter := &failingWriter{failAfter: 0}
		m, err := New(WithOutput(failWriter))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		entry := Entry{
			Timestamp:   time.Now().UTC(),
			EventType:   EventServiceRegistered,
			ModuleName:  "test",
			ServiceName: "test-service",
			Details:     map[string]any{"key": "value"},
		}

		// This should not panic, errors go to stderr
		m.writeEntry(context.Background(), entry)
	})

	t.Run("handles newline write error gracefully", func(t *testing.T) {
		// Writer that fails on second write (newline)
		failWriter := &failingWriter{failAfter: 1}
		m, err := New(WithOutput(failWriter))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		entry := Entry{
			Timestamp:   time.Now().UTC(),
			EventType:   EventServiceRegistered,
			ModuleName:  "test",
			ServiceName: "test-service",
			Details:     map[string]any{"key": "value"},
		}

		// This should not panic, errors go to stderr
		m.writeEntry(context.Background(), entry)
	})

	t.Run("hash chaining integration", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(WithOutput(buf), WithHashChaining(""))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		// Write first entry
		entry1 := Entry{
			Timestamp:   time.Now().UTC(),
			EventType:   EventServiceRegistered,
			ModuleName:  "test",
			ServiceName: "service-1",
			Details:     map[string]any{"index": 1},
		}
		m.writeEntry(context.Background(), entry1)

		// Write second entry (should have prev_hash set)
		entry2 := Entry{
			Timestamp:   time.Now().UTC(),
			EventType:   EventModuleStopped,
			ModuleName:  "test",
			ServiceName: "service-2",
			Details:     map[string]any{"index": 2},
		}
		m.writeEntry(context.Background(), entry2)

		// Parse entries
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(lines))
		}

		var parsedEntry1, parsedEntry2 Entry
		if err := json.Unmarshal([]byte(lines[0]), &parsedEntry1); err != nil {
			t.Fatalf("failed to parse entry 1: %v", err)
		}
		if err := json.Unmarshal([]byte(lines[1]), &parsedEntry2); err != nil {
			t.Fatalf("failed to parse entry 2: %v", err)
		}

		// Verify hash chain
		if parsedEntry1.EntryHash == "" {
			t.Error("entry 1 should have entry_hash set")
		}
		if parsedEntry2.PrevHash != parsedEntry1.EntryHash {
			t.Errorf("entry 2 prev_hash (%s) should match entry 1 entry_hash (%s)",
				parsedEntry2.PrevHash, parsedEntry1.EntryHash)
		}
	})
}

// failingWriter is a writer that fails after a certain number of writes.
type failingWriter struct {
	writeCount int
	failAfter  int
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
	if w.writeCount >= w.failAfter {
		return 0, fmt.Errorf("forced write error")
	}
	w.writeCount++
	return len(p), nil
}

// syncErrorWriter implements Sync() that always returns error
type syncErrorWriter struct {
	buf bytes.Buffer
}

func (w *syncErrorWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *syncErrorWriter) Sync() error {
	return fmt.Errorf("simulated sync error")
}

// closeErrorWriter implements Close() that always returns error
type closeErrorWriter struct {
	buf bytes.Buffer
}

func (w *closeErrorWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *closeErrorWriter) Close() error {
	return fmt.Errorf("simulated close error")
}

// TestStop tests Stop() method coverage.
func TestStop(t *testing.T) {
	t.Run("stop succeeds after start", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		container := newMockServiceContainer()
		if err := m.RegisterServices(container); err != nil {
			t.Fatalf("RegisterServices() failed: %v", err)
		}

		ctx := context.Background()
		if err := m.Start(ctx); err != nil {
			t.Fatalf("Start() failed: %v", err)
		}

		// Stop should succeed
		if err := m.Stop(ctx); err != nil {
			t.Errorf("Stop() failed: %v", err)
		}

		// Second stop should be no-op (stopOnce)
		if err := m.Stop(ctx); err != nil {
			t.Errorf("Second Stop() failed: %v", err)
		}
	})

	t.Run("stop before start", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		// Stop without Start should still work
		ctx := context.Background()
		if err := m.Stop(ctx); err != nil {
			t.Errorf("Stop() without Start failed: %v", err)
		}
	})

	t.Run("stop with sync error", func(t *testing.T) {
		writer := &syncErrorWriter{}
		m, err := New(WithOutput(writer))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		ctx := context.Background()
		if err := m.Start(ctx); err != nil {
			t.Fatalf("Start() failed: %v", err)
		}

		err = m.Stop(ctx)
		if err == nil {
			t.Error("Stop() should return error when Sync fails")
		}
		if !strings.Contains(err.Error(), "sync audit log") {
			t.Errorf("error should mention sync failure, got: %v", err)
		}
	})

	t.Run("stop with close error", func(t *testing.T) {
		writer := &closeErrorWriter{}
		m, err := New(WithOutput(writer))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		ctx := context.Background()
		if err := m.Start(ctx); err != nil {
			t.Fatalf("Start() failed: %v", err)
		}

		err = m.Stop(ctx)
		if err == nil {
			t.Error("Stop() should return error when Close fails")
		}
		if !strings.Contains(err.Error(), "close audit log") {
			t.Errorf("error should mention close failure, got: %v", err)
		}
	})
}

// TestHandleAuditTrailChannel_EdgeCases tests edge cases in handleAuditTrailChannel
func TestHandleAuditTrailChannel_EdgeCases(t *testing.T) {
	t.Run("response timeout on full output channel", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		err = m.Start(context.Background())
		if err != nil {
			t.Fatalf("Start() failed: %v", err)
		}
		defer m.Stop(context.Background())

		// Fill the output channel to capacity
		for i := 0; i < 100; i++ {
			select {
			case m.auditTrailOut <- &types.Msg{Data: []byte("dummy")}:
			default:
				// Channel full, stop filling
				i = 100 // Exit the loop
			}
		}

		// Send an entry that will timeout trying to send response
		entry := Entry{
			ModuleName: "timeout-test",
		}
		data, _ := json.Marshal(entry)

		msg := &types.Msg{
			Subject: "audit-trail",
			Reply:   "audit-trail.reply",
			Data:    data,
		}

		// This will block on output channel (full) and timeout after 1 second
		m.auditTrailIn <- msg

		// Wait for timeout
		time.Sleep(1500 * time.Millisecond)

		// Entry should still be written despite response timeout
		logContent := buf.String()
		if !strings.Contains(logContent, "timeout-test") {
			t.Error("entry should still be written to log")
		}
	})

	t.Run("context cancellation during response", func(t *testing.T) {
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		err = m.Start(context.Background())
		if err != nil {
			t.Fatalf("Start() failed: %v", err)
		}

		// Fill the output channel to capacity
		for i := 0; i < 100; i++ {
			select {
			case m.auditTrailOut <- &types.Msg{Data: []byte("dummy")}:
			default:
				// Channel full, stop filling
				i = 100 // Exit the loop
			}
		}

		// Send an entry
		entry := Entry{
			ModuleName: "cancel-test",
		}
		data, _ := json.Marshal(entry)

		msg := &types.Msg{
			Subject: "audit-trail",
			Reply:   "audit-trail.reply",
			Data:    data,
		}

		m.auditTrailIn <- msg

		// Give it a moment to start processing
		time.Sleep(50 * time.Millisecond)

		// Now stop (which cancels context) while response is blocked
		m.Stop(context.Background())

		// Entry should still be written despite context cancellation
		logContent := buf.String()
		if !strings.Contains(logContent, "cancel-test") {
			t.Error("entry should still be written to log before cancellation")
		}
	})

	t.Run("handler timeout during stop", func(t *testing.T) {
		// This tests the 5-second timeout in Stop() when handler doesn't exit
		buf := &syncWriter{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		err = m.Start(context.Background())
		if err != nil {
			t.Fatalf("Start() failed: %v", err)
		}

		// The handler will exit gracefully when we call Stop, so this mainly
		// tests the timeout path doesn't hang
		err = m.Stop(context.Background())
		if err != nil {
			t.Errorf("Stop() failed: %v", err)
		}
	})
}

// TestWriteEntry_MarshalAndWriteError tests combined error paths
func TestWriteEntry_MarshalAndWriteError(t *testing.T) {
	t.Run("marshal error then write error", func(t *testing.T) {
		// Writer that fails on write
		failWriter := &failingWriter{failAfter: 0}
		m, err := New(WithOutput(failWriter))
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		// Create entry with unmarshalable Details that will cause json.Marshal to fail
		// The error fallback then tries to write, which will also fail
		entry := Entry{
			Timestamp:  time.Now().UTC(),
			EventType:  EventCustomAuditTrail,
			ModuleName: "test",
			Details: map[string]any{
				"channel": make(chan int), // This causes json.Marshal to fail
			},
		}

		// This should not panic even though both marshal and write fail
		m.writeEntry(context.Background(), entry)
		// Test passes if no panic occurs - errors go to stderr
	})
}
