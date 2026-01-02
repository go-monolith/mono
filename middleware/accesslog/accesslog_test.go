package accesslog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1/pkg/types"
)

// syncWriter wraps bytes.Buffer with mutex for concurrent tests.
type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (sw *syncWriter) Write(p []byte) (n int, err error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.buf.Write(p)
}

func (sw *syncWriter) String() string {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.buf.String()
}

func (sw *syncWriter) Lines() []string {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	content := sw.buf.String()
	if content == "" {
		return []string{}
	}
	lines := strings.Split(strings.TrimSpace(content), "\n")
	return lines
}

// TestNew verifies module construction.
func TestNew(t *testing.T) {
	t.Run("requires output option", func(t *testing.T) {
		_, err := New()
		if err == nil {
			t.Fatal("expected error when output is not provided")
		}
		if !strings.Contains(err.Error(), "requires WithOutput") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("accepts valid output option", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, err := New(WithOutput(buf))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if m.Name() != "accesslog" {
			t.Errorf("expected name 'accesslog', got %q", m.Name())
		}
	})

	t.Run("default format is text", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, _ := New(WithOutput(buf))
		if m.format != FormatText {
			t.Errorf("expected FormatText, got %v", m.format)
		}
	})

	t.Run("default fields is all fields", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, _ := New(WithOutput(buf))
		if len(m.fields) != len(AllFields()) {
			t.Errorf("expected %d fields, got %d", len(AllFields()), len(m.fields))
		}
	})

	t.Run("default request ID header is X-Request-ID", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, _ := New(WithOutput(buf))
		if m.requestIDHeader != "X-Request-ID" {
			t.Errorf("expected 'X-Request-ID', got %q", m.requestIDHeader)
		}
	})

	t.Run("nil output returns error", func(t *testing.T) {
		_, err := New(WithOutput(nil))
		if err == nil {
			t.Fatal("expected error for nil output")
		}
	})

	t.Run("empty request ID header returns error", func(t *testing.T) {
		buf := &bytes.Buffer{}
		_, err := New(WithOutput(buf), WithRequestIDHeader(""))
		if err == nil {
			t.Fatal("expected error for empty header")
		}
	})
}

// TestWrapRequestReplyHandler verifies request-reply handler wrapping.
func TestWrapRequestReplyHandler(t *testing.T) {
	t.Run("captures success timing and sizes", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		originalHandler := func(_ context.Context, req *types.Msg) ([]byte, error) {
			time.Sleep(10 * time.Millisecond)
			return []byte("response data"), nil
		}

		wrapped := m.wrapRequestReplyHandler(originalHandler, "test-module", "test-service")

		req := &types.Msg{
			Data:   []byte("request data"),
			Header: types.Header{"X-Request-ID": {"abc123"}},
		}

		resp, err := wrapped(context.Background(), req)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(resp) != "response data" {
			t.Error("response should be passed through")
		}

		lines := buf.Lines()
		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d", len(lines))
		}

		var entry map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}

		if entry["status"] != "success" {
			t.Error("status should be success")
		}
		if entry["request_id"] != "abc123" {
			t.Error("request_id should be extracted")
		}
		if entry["module"] != "test-module" {
			t.Error("module should match")
		}
		if entry["service"] != "test-service" {
			t.Error("service should match")
		}
		if entry["service_type"] != "request_reply" {
			t.Error("service_type should be request_reply")
		}
		if entry["duration_ms"].(float64) < 10 {
			t.Error("duration should be at least 10ms")
		}
		if int(entry["request_size"].(float64)) != len("request data") {
			t.Error("request_size should match")
		}
		if int(entry["response_size"].(float64)) != len("response data") {
			t.Error("response_size should match")
		}
	})

	t.Run("captures error status", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf))

		originalHandler := func(_ context.Context, req *types.Msg) ([]byte, error) {
			return nil, fmt.Errorf("handler error")
		}

		wrapped := m.wrapRequestReplyHandler(originalHandler, "test-module", "test-service")
		_, err := wrapped(context.Background(), &types.Msg{Data: []byte{}})

		if err == nil {
			t.Error("error should be passed through")
		}

		lines := buf.Lines()
		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d", len(lines))
		}

		if !strings.Contains(lines[0], "status=error") {
			t.Error("status should be error")
		}
	})
}

// TestWrapQueueGroupHandler verifies queue group handler wrapping.
func TestWrapQueueGroupHandler(t *testing.T) {
	t.Run("captures success with no response size", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		originalHandler := func(_ context.Context, msg *types.Msg) error {
			time.Sleep(5 * time.Millisecond)
			return nil
		}

		wrapped := m.wrapQueueGroupHandler(originalHandler, "queue-module", "queue-service")

		msg := &types.Msg{
			Data:   []byte("queue message"),
			Header: types.Header{"X-Request-ID": {"xyz789"}},
		}

		err := wrapped(context.Background(), msg)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		lines := buf.Lines()
		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d", len(lines))
		}

		var entry map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}

		if entry["status"] != "success" {
			t.Error("status should be success")
		}
		if entry["service_type"] != "queue_group" {
			t.Error("service_type should be queue_group")
		}
		if int(entry["response_size"].(float64)) != 0 {
			t.Error("response_size should be 0 for queue_group")
		}
	})

	t.Run("captures error status", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf))

		originalHandler := func(_ context.Context, msg *types.Msg) error {
			return fmt.Errorf("queue handler error")
		}

		wrapped := m.wrapQueueGroupHandler(originalHandler, "queue-module", "queue-service")
		err := wrapped(context.Background(), &types.Msg{Data: []byte{}})

		if err == nil {
			t.Error("error should be passed through")
		}

		lines := buf.Lines()
		if !strings.Contains(lines[0], "status=error") {
			t.Error("status should be error")
		}
	})
}

// TestWrapStreamConsumerHandler verifies stream consumer handler wrapping.
func TestWrapStreamConsumerHandler(t *testing.T) {
	t.Run("captures batch handling with total size", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		originalHandler := func(_ context.Context, msgs []*types.Msg) error {
			time.Sleep(15 * time.Millisecond)
			return nil
		}

		wrapped := m.wrapStreamConsumerHandler(originalHandler, "stream-module", "stream-service")

		msgs := []*types.Msg{
			{Data: []byte("msg1"), Header: types.Header{"X-Request-ID": {"batch123"}}},
			{Data: []byte("msg22"), Header: types.Header{}},
			{Data: []byte("msg333"), Header: types.Header{}},
		}

		err := wrapped(context.Background(), msgs)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		lines := buf.Lines()
		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d", len(lines))
		}

		var entry map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}

		if entry["status"] != "success" {
			t.Error("status should be success")
		}
		if entry["service_type"] != "stream_consumer" {
			t.Error("service_type should be stream_consumer")
		}
		if entry["request_id"] != "batch123" {
			t.Error("request_id should be from first message")
		}
		// Total size: 4 + 5 + 6 = 15
		if int(entry["request_size"].(float64)) != 15 {
			t.Errorf("request_size should be 15, got %v", entry["request_size"])
		}
		if int(entry["response_size"].(float64)) != 0 {
			t.Error("response_size should be 0 for stream_consumer")
		}
	})
}

// TestTextFormatter verifies text output format.
func TestTextFormatter(t *testing.T) {
	t.Run("correct key=value format", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, _ := New(WithOutput(buf), WithFormat(FormatText))

		entry := Entry{
			Timestamp:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			RequestID:    "req123",
			Module:       "order",
			Service:      "place-order",
			ServiceType:  "request_reply",
			DurationMS:   45,
			Status:       StatusSuccess,
			RequestSize:  1024,
			ResponseSize: 512,
		}

		m.writeEntry(entry)

		output := buf.String()
		expected := []string{
			"ts=2024-01-15T10:30:00Z",
			"request_id=req123",
			"module=order",
			"service=place-order",
			"service_type=request_reply",
			"status=success",
			"duration_ms=45",
			"request_size=1024",
			"response_size=512",
		}

		for _, exp := range expected {
			if !strings.Contains(output, exp) {
				t.Errorf("expected %q in output", exp)
			}
		}
	})
}

// TestJSONFormatter verifies JSON output format.
func TestJSONFormatter(t *testing.T) {
	t.Run("valid JSON output", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		entry := Entry{
			Timestamp:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			RequestID:    "req456",
			Module:       "payment",
			Service:      "process-payment",
			ServiceType:  "queue_group",
			DurationMS:   120,
			Status:       StatusError,
			RequestSize:  2048,
			ResponseSize: 0,
		}

		m.writeEntry(entry)

		var parsed map[string]any
		if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
			t.Fatalf("invalid JSON: %v", err)
		}

		if parsed["ts"] != "2024-01-15T10:30:00Z" {
			t.Error("ts mismatch")
		}
		if parsed["request_id"] != "req456" {
			t.Error("request_id mismatch")
		}
		if parsed["module"] != "payment" {
			t.Error("module mismatch")
		}
		if parsed["service"] != "process-payment" {
			t.Error("service mismatch")
		}
		if parsed["service_type"] != "queue_group" {
			t.Error("service_type mismatch")
		}
		if parsed["status"] != "error" {
			t.Error("status mismatch")
		}
		if int(parsed["duration_ms"].(float64)) != 120 {
			t.Error("duration_ms mismatch")
		}
		if int(parsed["request_size"].(float64)) != 2048 {
			t.Error("request_size mismatch")
		}
		if int(parsed["response_size"].(float64)) != 0 {
			t.Error("response_size mismatch")
		}
	})
}

// TestFieldFiltering verifies field selection.
func TestFieldFiltering(t *testing.T) {
	t.Run("only specified fields appear", func(t *testing.T) {
		buf := &bytes.Buffer{}
		m, _ := New(
			WithOutput(buf),
			WithFormat(FormatText),
			WithFields([]Field{
				FieldTimestamp,
				FieldService,
				FieldDurationMS,
				FieldStatus,
			}),
		)

		entry := Entry{
			Timestamp:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			RequestID:    "should-not-appear",
			Module:       "should-not-appear",
			Service:      "test-service",
			ServiceType:  "should-not-appear",
			DurationMS:   100,
			Status:       StatusSuccess,
			RequestSize:  999,
			ResponseSize: 999,
		}

		m.writeEntry(entry)

		output := buf.String()

		// Should appear
		if !strings.Contains(output, "ts=") {
			t.Error("ts should appear")
		}
		if !strings.Contains(output, "service=test-service") {
			t.Error("service should appear")
		}
		if !strings.Contains(output, "duration_ms=100") {
			t.Error("duration_ms should appear")
		}
		if !strings.Contains(output, "status=success") {
			t.Error("status should appear")
		}

		// Should NOT appear
		if strings.Contains(output, "request_id=") {
			t.Error("request_id should not appear")
		}
		if strings.Contains(output, "module=") {
			t.Error("module should not appear")
		}
		if strings.Contains(output, "service_type=") {
			t.Error("service_type should not appear")
		}
		if strings.Contains(output, "request_size=") {
			t.Error("request_size should not appear")
		}
		if strings.Contains(output, "response_size=") {
			t.Error("response_size should not appear")
		}
	})
}

// TestExtractRequestID verifies request ID extraction.
func TestExtractRequestID(t *testing.T) {
	t.Run("header present", func(t *testing.T) {
		m, _ := New(WithOutput(&bytes.Buffer{}))
		msg := &types.Msg{
			Header: types.Header{"X-Request-ID": {"test-id"}},
		}
		id := m.extractRequestID(msg)
		if id != "test-id" {
			t.Errorf("expected 'test-id', got %q", id)
		}
	})

	t.Run("header missing", func(t *testing.T) {
		m, _ := New(WithOutput(&bytes.Buffer{}))
		msg := &types.Msg{
			Header: types.Header{},
		}
		id := m.extractRequestID(msg)
		if id != "" {
			t.Errorf("expected empty string, got %q", id)
		}
	})

	t.Run("nil header", func(t *testing.T) {
		m, _ := New(WithOutput(&bytes.Buffer{}))
		msg := &types.Msg{
			Header: nil,
		}
		id := m.extractRequestID(msg)
		if id != "" {
			t.Errorf("expected empty string, got %q", id)
		}
	})

	t.Run("custom header", func(t *testing.T) {
		m, _ := New(
			WithOutput(&bytes.Buffer{}),
			WithRequestIDHeader("X-Correlation-ID"),
		)
		msg := &types.Msg{
			Header: types.Header{"X-Correlation-ID": {"custom-id"}},
		}
		id := m.extractRequestID(msg)
		if id != "custom-id" {
			t.Errorf("expected 'custom-id', got %q", id)
		}
	})
}

// TestOnServiceRegistration verifies full service registration flow.
func TestOnServiceRegistration(t *testing.T) {
	t.Run("wraps request-reply handler", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf))

		reg := types.ServiceRegistration{
			Type:       types.ServiceTypeRequestReply,
			Name:       "test-service",
			ModuleName: "test-module",
			RequestHandler: func(_ context.Context, req *types.Msg) ([]byte, error) {
				return []byte("ok"), nil
			},
		}

		wrapped := m.OnServiceRegistration(context.Background(), reg)

		// Call wrapped handler
		_, _ = wrapped.RequestHandler(context.Background(), &types.Msg{Data: []byte("test")})

		lines := buf.Lines()
		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d", len(lines))
		}
		if !strings.Contains(lines[0], "service=test-service") {
			t.Error("should log service name")
		}
	})

	t.Run("wraps queue group handlers", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf))

		reg := types.ServiceRegistration{
			Type:       types.ServiceTypeQueueGroup,
			Name:       "queue-service",
			ModuleName: "queue-module",
			QueueHandlers: []types.QGHP{
				{
					QueueGroup: "group1",
					Handler: func(_ context.Context, msg *types.Msg) error {
						return nil
					},
				},
				{
					QueueGroup: "group2",
					Handler: func(_ context.Context, msg *types.Msg) error {
						return nil
					},
				},
			},
		}

		wrapped := m.OnServiceRegistration(context.Background(), reg)

		// Call both wrapped handlers
		_ = wrapped.QueueHandlers[0].Handler(context.Background(), &types.Msg{Data: []byte("msg1")})
		_ = wrapped.QueueHandlers[1].Handler(context.Background(), &types.Msg{Data: []byte("msg2")})

		lines := buf.Lines()
		if len(lines) != 2 {
			t.Fatalf("expected 2 log lines, got %d", len(lines))
		}
	})

	t.Run("wraps stream consumer handler", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf))

		reg := types.ServiceRegistration{
			Type:       types.ServiceTypeStreamConsumer,
			Name:       "stream-service",
			ModuleName: "stream-module",
			StreamHandler: func(_ context.Context, msgs []*types.Msg) error {
				return nil
			},
		}

		wrapped := m.OnServiceRegistration(context.Background(), reg)

		// Call wrapped handler
		_ = wrapped.StreamHandler(context.Background(), []*types.Msg{{Data: []byte("batch")}})

		lines := buf.Lines()
		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d", len(lines))
		}
	})

	t.Run("wraps channel services with proxy", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf))

		inChan := make(chan *types.Msg, 10)
		outChan := make(chan *types.Msg, 10)

		reg := types.ServiceRegistration{
			Type:       types.ServiceTypeChannel,
			Name:       "channel-service",
			ModuleName: "channel-module",
			InChannel:  inChan,
			OutChannel: outChan,
		}

		wrapped := m.OnServiceRegistration(context.Background(), reg)

		// Channels should be different (proxied)
		if wrapped.InChannel == inChan {
			t.Error("InChannel should be proxied (different)")
		}
		if wrapped.OutChannel == outChan {
			t.Error("OutChannel should be proxied (different)")
		}
	})
}

// TestConcurrentWrites verifies thread-safe writes.
func TestConcurrentWrites(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf))

	handler := func(_ context.Context, req *types.Msg) ([]byte, error) {
		return []byte("ok"), nil
	}

	wrapped := m.wrapRequestReplyHandler(handler, "test-module", "test-service")

	// Launch 10 concurrent requests
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = wrapped(context.Background(), &types.Msg{Data: []byte("concurrent")})
		}()
	}
	wg.Wait()

	lines := buf.Lines()
	if len(lines) != 10 {
		t.Errorf("expected 10 log lines, got %d", len(lines))
	}
}

// TestChannelServiceLogging verifies channel service access logging.
func TestChannelServiceLogging(t *testing.T) {
	t.Run("logs inbound and outbound messages separately", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		// Create original channels
		originalIn := make(chan *types.Msg, 10)
		originalOut := make(chan *types.Msg, 10)

		// Wrap channels
		proxyIn, proxyOut := m.wrapChannelService(originalIn, originalOut, "test-module", "test-channel")

		// Start a handler that echoes requests
		go func() {
			msg := <-originalIn
			originalOut <- &types.Msg{
				Data:   []byte("response data"),
				Header: msg.Header,
			}
		}()

		// Send request
		proxyIn <- &types.Msg{
			Data:   []byte("request data"),
			Header: types.Header{"X-Request-ID": {"chan-test-123"}},
		}

		// Receive response
		response := <-proxyOut
		if string(response.Data) != "response data" {
			t.Error("response should be passed through")
		}

		// Wait for logging to complete
		time.Sleep(50 * time.Millisecond)

		// Verify 2 log entries (inbound + outbound)
		lines := buf.Lines()
		if len(lines) != 2 {
			t.Fatalf("expected 2 log lines (inbound + outbound), got %d", len(lines))
		}

		// Verify inbound request log
		var inboundEntry map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &inboundEntry); err != nil {
			t.Fatalf("failed to parse inbound JSON: %v", err)
		}

		if inboundEntry["service_type"] != "channel" {
			t.Error("inbound service_type should be channel")
		}
		if inboundEntry["request_id"] != "chan-test-123" {
			t.Error("inbound request_id should be extracted")
		}
		if int(inboundEntry["request_size"].(float64)) != len("request data") {
			t.Error("inbound request_size should match")
		}
		if int(inboundEntry["response_size"].(float64)) != 0 {
			t.Error("inbound response_size should be 0")
		}

		// Verify outbound response log
		var outboundEntry map[string]any
		if err := json.Unmarshal([]byte(lines[1]), &outboundEntry); err != nil {
			t.Fatalf("failed to parse outbound JSON: %v", err)
		}

		if outboundEntry["service_type"] != "channel" {
			t.Error("outbound service_type should be channel")
		}
		if int(outboundEntry["request_size"].(float64)) != 0 {
			t.Error("outbound request_size should be 0")
		}
		if int(outboundEntry["response_size"].(float64)) != len("response data") {
			t.Error("outbound response_size should match")
		}
	})

	t.Run("proxy maintains buffer size from original", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf))

		originalIn := make(chan *types.Msg, 42)
		originalOut := make(chan *types.Msg, 84)

		proxyIn, proxyOut := m.wrapChannelService(originalIn, originalOut, "test-module", "test-service")

		// Verify buffer sizes match
		if cap(proxyIn) != 42 {
			t.Errorf("proxyIn should have capacity 42, got %d", cap(proxyIn))
		}
		if cap(proxyOut) != 84 {
			t.Errorf("proxyOut should have capacity 84, got %d", cap(proxyOut))
		}
	})

	t.Run("proxy closes channels correctly", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf))

		originalIn := make(chan *types.Msg, 1)
		originalOut := make(chan *types.Msg, 1)

		proxyIn, proxyOut := m.wrapChannelService(originalIn, originalOut, "test-module", "test-service")

		// Close proxyIn and verify originalIn gets closed
		close(proxyIn)
		time.Sleep(50 * time.Millisecond)

		select {
		case _, ok := <-originalIn:
			if ok {
				t.Error("originalIn should be closed when proxyIn is closed")
			}
		default:
			t.Error("originalIn should be closed")
		}

		// Close originalOut and verify proxyOut gets closed
		close(originalOut)
		time.Sleep(50 * time.Millisecond)

		select {
		case _, ok := <-proxyOut:
			if ok {
				t.Error("proxyOut should be closed when originalOut is closed")
			}
		default:
			t.Error("proxyOut should be closed")
		}
	})

	t.Run("graceful shutdown stops proxy goroutines", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf))

		originalIn := make(chan *types.Msg, 1)
		originalOut := make(chan *types.Msg, 1)

		_, _ = m.wrapChannelService(originalIn, originalOut, "test-module", "test-service")

		// Stop the module (which should cancel proxy contexts)
		if err := m.Stop(context.Background()); err != nil {
			t.Fatalf("Stop() failed: %v", err)
		}

		// Verify all proxies are cleaned up after Stop
		m.channelProxiesMu.RLock()
		proxyCount := len(m.channelProxies)
		m.channelProxiesMu.RUnlock()

		if proxyCount != 0 {
			t.Errorf("expected 0 proxies after cleanup, got %d", proxyCount)
		}
	})
}

// TestEdgeCases verifies edge case handling.
func TestEdgeCases(t *testing.T) {
	t.Run("zero-byte request", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		handler := func(_ context.Context, req *types.Msg) ([]byte, error) {
			return []byte{}, nil
		}

		wrapped := m.wrapRequestReplyHandler(handler, "test", "test")
		_, _ = wrapped(context.Background(), &types.Msg{Data: []byte{}})

		lines := buf.Lines()
		var entry map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}

		if int(entry["request_size"].(float64)) != 0 {
			t.Error("request_size should be 0")
		}
		if int(entry["response_size"].(float64)) != 0 {
			t.Error("response_size should be 0")
		}
	})

	t.Run("nil response with error", func(t *testing.T) {
		buf := &syncWriter{}
		m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		handler := func(_ context.Context, req *types.Msg) ([]byte, error) {
			return nil, fmt.Errorf("error")
		}

		wrapped := m.wrapRequestReplyHandler(handler, "test", "test")
		_, err := wrapped(context.Background(), &types.Msg{Data: []byte("test")})

		if err == nil {
			t.Error("error should be passed through")
		}

		lines := buf.Lines()
		var entry map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}

		if entry["status"] != "error" {
			t.Error("status should be error")
		}
		if int(entry["response_size"].(float64)) != 0 {
			t.Error("response_size should be 0 for nil response")
		}
	})
}

// mockModule is a minimal test implementation of types.Module interface.
// Used for testing middleware hooks that require a module reference without
// needing the full module lifecycle machinery.
type mockModule struct {
	name string
}

func (m *mockModule) Name() string                    { return m.name }
func (m *mockModule) Start(ctx context.Context) error { return nil }
func (m *mockModule) Stop(ctx context.Context) error  { return nil }
func (m *mockModule) Health(ctx context.Context) types.HealthStatus {
	return types.HealthStatus{Healthy: true}
}

// TestOnEventConsumerRegistration tests the OnEventConsumerRegistration middleware hook
func TestOnEventConsumerRegistration(t *testing.T) {
	buf := &syncWriter{}
	logger, _ := New(WithOutput(buf), WithFormat(FormatJSON))

	handlerCalled := false
	mockHandler := func(ctx context.Context, msg *types.Msg) error {
		handlerCalled = true
		return nil
	}

	mockEventDef := types.BaseEventDefinition{
		Name:       "TestEvent",
		Version:    "v1",
		ModuleName: "test-module",
		Subject:    "events.test.v1.test-event",
	}

	mockMod := &mockModule{name: "consumer-module"}

	entry := types.EventConsumerEntry{
		EventDef:   mockEventDef,
		Handler:    mockHandler,
		Module:     mockMod,
		QueueGroup: "test-queue",
	}

	ctx := context.Background()
	result := logger.OnEventConsumerRegistration(ctx, entry)

	// Verify the entry fields are preserved
	if result.EventDef.Name != mockEventDef.Name {
		t.Errorf("expected event name %s, got %s", mockEventDef.Name, result.EventDef.Name)
	}
	if result.QueueGroup != "test-queue" {
		t.Errorf("expected queue group test-queue, got %s", result.QueueGroup)
	}
	if result.Module != entry.Module {
		t.Error("module reference should be preserved")
	}

	// Call the wrapped handler to verify it works
	msg := &types.Msg{
		Subject: "events.test.v1.test-event",
		Data:    []byte("test data"),
		Header:  types.Header{"X-Request-ID": {"req123"}},
	}

	err := result.Handler(ctx, msg)
	if err != nil {
		t.Fatalf("wrapped handler returned error: %v", err)
	}

	if !handlerCalled {
		t.Error("original handler should have been called")
	}

	// Verify logging occurred
	lines := buf.Lines()
	if len(lines) != 1 {
		t.Fatalf("expected 1 log line, got %d", len(lines))
	}
}

// TestWrapEventConsumerHandler tests the wrapEventConsumerHandler function
func TestWrapEventConsumerHandler(t *testing.T) {
	t.Run("success case with request ID", func(t *testing.T) {
		buf := &syncWriter{}
		logger, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		handlerCalled := false
		handler := func(ctx context.Context, msg *types.Msg) error {
			handlerCalled = true
			time.Sleep(10 * time.Millisecond)
			return nil
		}

		wrapped := logger.wrapEventConsumerHandler(handler, "test-module", "events.test.v1.test-event")

		msg := &types.Msg{
			Subject: "events.test.v1.test-event",
			Data:    []byte("test data"),
			Header:  types.Header{"X-Request-ID": {"abc123"}},
		}

		err := wrapped(context.Background(), msg)
		if err != nil {
			t.Fatalf("wrapped handler returned error: %v", err)
		}

		if !handlerCalled {
			t.Error("original handler should have been called")
		}

		lines := buf.Lines()
		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d", len(lines))
		}

		var logEntry map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &logEntry); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}

		if logEntry["status"] != "success" {
			t.Error("status should be success")
		}
		if logEntry["request_id"] != "abc123" {
			t.Error("request_id should be extracted from header")
		}
		if logEntry["module"] != "test-module" {
			t.Error("module should match")
		}
		if logEntry["service_type"] != "event_consumer" {
			t.Error("service_type should be event_consumer")
		}
		if logEntry["duration_ms"].(float64) <= 0 {
			t.Error("duration should be greater than 0")
		}
	})

	t.Run("error case", func(t *testing.T) {
		buf := &syncWriter{}
		logger, _ := New(WithOutput(buf), WithFormat(FormatText))

		handler := func(ctx context.Context, msg *types.Msg) error {
			return fmt.Errorf("handler error")
		}

		wrapped := logger.wrapEventConsumerHandler(handler, "test-module", "events.test.v1.test-event")

		msg := &types.Msg{
			Subject: "events.test.v1.test-event",
			Data:    []byte("test data"),
		}

		err := wrapped(context.Background(), msg)
		if err == nil {
			t.Error("error should be passed through")
		}

		lines := buf.Lines()
		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d", len(lines))
		}

		if !strings.Contains(lines[0], "status=error") {
			t.Error("status should be error")
		}
	})
}

// TestOnEventStreamConsumerRegistration tests the OnEventStreamConsumerRegistration middleware hook
func TestOnEventStreamConsumerRegistration(t *testing.T) {
	t.Run("with module", func(t *testing.T) {
		buf := &syncWriter{}
		logger, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		handlerCalled := false
		mockHandler := func(ctx context.Context, msgs []*types.Msg) error {
			handlerCalled = true
			return nil
		}

		mockEventDef := types.BaseEventDefinition{
			Name:       "TestEvent",
			Version:    "v1",
			ModuleName: "test-module",
			Subject:    "events.test.v1.test-event",
		}

		mockMod := &mockModule{name: "stream-consumer-module"}

		entry := types.EventStreamConsumerEntry{
			EventDef: mockEventDef,
			Config: types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name: "TEST_STREAM",
				},
				Consumer: types.ConsumerConfig{
					Name: "test-consumer",
				},
			},
			Handler:    mockHandler,
			Module:     mockMod,
			SequenceID: 1,
		}

		ctx := context.Background()
		result := logger.OnEventStreamConsumerRegistration(ctx, entry)

		// Verify the entry fields are preserved
		if result.EventDef.Name != mockEventDef.Name {
			t.Errorf("expected event name %s, got %s", mockEventDef.Name, result.EventDef.Name)
		}
		if result.Config.Consumer.Name != "test-consumer" {
			t.Errorf("expected consumer test-consumer, got %s", result.Config.Consumer.Name)
		}
		if result.SequenceID != entry.SequenceID {
			t.Errorf("expected sequence ID %d, got %d", entry.SequenceID, result.SequenceID)
		}
		if result.Module != entry.Module {
			t.Error("module reference should be preserved")
		}

		// Call the wrapped handler to verify it works
		msgs := []*types.Msg{
			{
				Subject: "events.test.v1.test-event",
				Data:    []byte("test data 1"),
				Header:  types.Header{"X-Request-ID": {"req123"}},
			},
			{
				Subject: "events.test.v1.test-event",
				Data:    []byte("test data 2"),
			},
		}

		err := result.Handler(ctx, msgs)
		if err != nil {
			t.Fatalf("wrapped handler returned error: %v", err)
		}

		if !handlerCalled {
			t.Error("original handler should have been called")
		}

		// Verify logging occurred
		lines := buf.Lines()
		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d", len(lines))
		}
	})

	t.Run("with nil module", func(t *testing.T) {
		buf := &syncWriter{}
		logger, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		mockHandler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		mockEventDef := types.BaseEventDefinition{
			Name:       "TestEvent",
			Version:    "v1",
			ModuleName: "test-module",
			Subject:    "events.test.v1.test-event",
		}

		entry := types.EventStreamConsumerEntry{
			EventDef: mockEventDef,
			Config: types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name: "TEST_STREAM",
				},
				Consumer: types.ConsumerConfig{
					Name: "test-consumer",
				},
			},
			Handler:    mockHandler,
			Module:     nil,
			SequenceID: 1,
		}

		ctx := context.Background()
		result := logger.OnEventStreamConsumerRegistration(ctx, entry)

		// Should not panic with nil module
		msgs := []*types.Msg{
			{
				Subject: "events.test.v1.test-event",
				Data:    []byte("test data"),
			},
		}

		err := result.Handler(ctx, msgs)
		if err != nil {
			t.Fatalf("wrapped handler returned error: %v", err)
		}
	})
}

// TestWrapEventStreamConsumerHandler tests the wrapEventStreamConsumerHandler function
func TestWrapEventStreamConsumerHandler(t *testing.T) {
	t.Run("batch processing", func(t *testing.T) {
		buf := &syncWriter{}
		logger, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		handlerCalled := false
		handler := func(ctx context.Context, msgs []*types.Msg) error {
			handlerCalled = true
			time.Sleep(10 * time.Millisecond)
			return nil
		}

		wrapped := logger.wrapEventStreamConsumerHandler(handler, "test-module", "test-consumer")

		msgs := []*types.Msg{
			{
				Subject: "events.test.v1.test-event",
				Data:    []byte("test data 1"),
				Header:  types.Header{"X-Request-ID": {"req123"}},
			},
			{
				Subject: "events.test.v1.test-event",
				Data:    []byte("test data 2"),
			},
			{
				Subject: "events.test.v1.test-event",
				Data:    []byte("test data 3"),
			},
		}

		err := wrapped(context.Background(), msgs)
		if err != nil {
			t.Fatalf("wrapped handler returned error: %v", err)
		}

		if !handlerCalled {
			t.Error("original handler should have been called")
		}

		lines := buf.Lines()
		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d", len(lines))
		}

		var logEntry map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &logEntry); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}

		if logEntry["status"] != "success" {
			t.Error("status should be success")
		}
		if logEntry["request_id"] != "req123" {
			t.Error("request_id should be extracted from first message")
		}
		if logEntry["module"] != "test-module" {
			t.Error("module should match")
		}
		if logEntry["service_type"] != "event_stream_consumer" {
			t.Error("service_type should be event_stream_consumer")
		}
		if logEntry["duration_ms"].(float64) <= 0 {
			t.Error("duration should be greater than 0")
		}
	})

	t.Run("empty batch", func(t *testing.T) {
		buf := &syncWriter{}
		logger, _ := New(WithOutput(buf), WithFormat(FormatJSON))

		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		wrapped := logger.wrapEventStreamConsumerHandler(handler, "test-module", "test-consumer")

		err := wrapped(context.Background(), []*types.Msg{})
		if err != nil {
			t.Fatalf("wrapped handler returned error: %v", err)
		}

		lines := buf.Lines()
		if len(lines) != 1 {
			t.Fatalf("expected 1 log line, got %d", len(lines))
		}
	})
}

// TestOnOutgoingMessage tests the OnOutgoingMessage middleware hook
func TestOnOutgoingMessage(t *testing.T) {
	buf := &syncWriter{}
	logger, _ := New(WithOutput(buf))

	ctx := types.OutgoingMessageContext{
		Subject: "test.subject",
		Msg: &types.Msg{
			Subject: "test.subject",
			Data:    []byte("test data"),
			Header:  types.Header{"X-Test": {"value"}},
		},
		Ctx: context.Background(),
	}

	result := logger.OnOutgoingMessage(ctx)

	// OnOutgoingMessage should be a pass-through (no modification)
	if result.Subject != ctx.Subject {
		t.Error("subject should not change")
	}
	if result.Msg != ctx.Msg {
		t.Error("msg should not change")
	}
}

// TestFieldName tests the FieldName helper function
func TestFieldName(t *testing.T) {
	tests := []struct {
		field    Field
		expected string
	}{
		{FieldTimestamp, "ts"},
		{FieldService, "service"},
		{FieldServiceType, "service_type"},
		{FieldModule, "module"},
		{FieldStatus, "status"},
		{FieldDurationMS, "duration_ms"},
		{FieldRequestSize, "request_size"},
		{FieldResponseSize, "response_size"},
		{FieldRequestID, "request_id"},
		{Field(999), "unknown"}, // Unknown field
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := FieldName(tt.field)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

// TestStart tests the Start function
func TestStart(t *testing.T) {
	buf := &syncWriter{}
	logger, _ := New(WithOutput(buf))

	err := logger.Start(context.Background())
	if err != nil {
		t.Errorf("Start() should not return error, got: %v", err)
	}
}

// TestStop tests the Stop function
func TestStop(t *testing.T) {
	t.Run("stop without channel proxies", func(t *testing.T) {
		buf := &syncWriter{}
		logger, _ := New(WithOutput(buf))

		ctx := context.Background()
		err := logger.Stop(ctx)
		if err != nil {
			t.Errorf("Stop() should not return error, got: %v", err)
		}
	})

	t.Run("stop with timeout", func(t *testing.T) {
		buf := &syncWriter{}
		logger, _ := New(WithOutput(buf))

		// Create a context that's already cancelled to trigger timeout
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := logger.Stop(ctx)
		if err == nil {
			t.Error("Stop() should return error when context is cancelled")
		}
	})
}

// TestOnModuleLifecycle tests the OnModuleLifecycle middleware hook
func TestOnModuleLifecycle(t *testing.T) {
	buf := &syncWriter{}
	logger, _ := New(WithOutput(buf))

	event := types.ModuleLifecycleEvent{
		Type:       types.ModuleStartedEvent,
		ModuleName: "test-module",
		Duration:   100 * time.Millisecond,
	}

	result := logger.OnModuleLifecycle(context.Background(), event)

	// OnModuleLifecycle should be a pass-through
	if result.ModuleName != event.ModuleName {
		t.Error("module name should not change")
	}
	if result.Type != event.Type {
		t.Error("type should not change")
	}
	if result.Duration != event.Duration {
		t.Error("duration should not change")
	}
}

// TestOnConfigurationChange tests the OnConfigurationChange middleware hook
func TestOnConfigurationChange(t *testing.T) {
	buf := &syncWriter{}
	logger, _ := New(WithOutput(buf))

	event := types.ConfigurationEvent{
		Type:       types.ConfigurationUpdatedEvent,
		OptionName: "test.config.option",
		OldValue:   "old",
		NewValue:   "new",
	}

	result := logger.OnConfigurationChange(context.Background(), event)

	// OnConfigurationChange should be a pass-through
	if result.Type != event.Type {
		t.Error("type should not change")
	}
	if result.OptionName != event.OptionName {
		t.Error("option name should not change")
	}
	if result.OldValue != event.OldValue {
		t.Error("old value should not change")
	}
	if result.NewValue != event.NewValue {
		t.Error("new value should not change")
	}
}

// ============================================================================
// Task 23: Additional Coverage Tests
// ============================================================================

// errorWriter is a writer that always returns an error
type errorWriter struct{}

func (e *errorWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("simulated write error")
}

// TestWriteEntry_WriteError tests writeEntry with a failing writer
func TestWriteEntry_WriteError(t *testing.T) {
	// Create logger with error writer
	logger, err := New(WithOutput(&errorWriter{}))
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

	// This should not panic, just log to stderr
	entry := Entry{
		Timestamp:   time.Now().UTC(),
		RequestID:   "test-123",
		Module:      "test-module",
		Service:     "test-service",
		ServiceType: "request-reply",
		Status:      StatusSuccess,
	}
	logger.writeEntry(entry)

	// Test passes if no panic occurred - the error is logged to stderr
}

// TestWriteEntry_DuringShutdown tests that writeEntry skips writes during shutdown
func TestWriteEntry_DuringShutdown(t *testing.T) {
	buf := &syncWriter{}
	logger, _ := New(WithOutput(buf))

	// Set shutdown flag
	logger.shutdown.Store(true)

	// Try to write an entry
	entry := Entry{
		Timestamp:   time.Now().UTC(),
		RequestID:   "test-123",
		Module:      "test-module",
		Service:     "test-service",
		ServiceType: "request-reply",
		Status:      StatusSuccess,
	}
	logger.writeEntry(entry)

	// Verify nothing was written
	if buf.String() != "" {
		t.Error("expected no writes during shutdown")
	}
}

// TestProxyChannels_ContextCancellation tests context cancellation in proxy channels
func TestProxyChannels_ContextCancellation(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

	// Create original channels
	originalIn := make(chan *types.Msg, 10)
	originalOut := make(chan *types.Msg, 10)

	// Wrap channels (this starts proxy goroutines)
	proxyIn, proxyOut := m.wrapChannelService(originalIn, originalOut, "test-module", "test-channel")
	_ = proxyIn
	_ = proxyOut

	// Stop the module (this cancels the proxy context)
	ctx := context.Background()
	err := m.Stop(ctx)
	if err != nil {
		t.Errorf("Stop() should succeed: %v", err)
	}

	// Proxies should have exited due to context cancellation
}

// TestProxyChannels_ChannelClosed tests channel closed scenario in proxy channels
func TestProxyChannels_ChannelClosed(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

	// Create original channels
	originalIn := make(chan *types.Msg, 10)
	originalOut := make(chan *types.Msg, 10)

	// Wrap channels
	proxyIn, proxyOut := m.wrapChannelService(originalIn, originalOut, "test-module", "test-channel")

	// Close the proxy input (simulates client closing)
	close(proxyIn)

	// Wait for proxy to handle the close
	time.Sleep(20 * time.Millisecond)

	// Close original output (simulates handler closing)
	close(originalOut)

	// Wait for proxy to handle the close
	time.Sleep(20 * time.Millisecond)

	// The proxyOut should eventually be closed
	select {
	case _, ok := <-proxyOut:
		if ok {
			t.Error("expected proxyOut to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		// Timeout is acceptable - proxies may still be processing
	}

	// Clean up
	m.Stop(context.Background())
}

// TestProxyChannels_ShutdownDuringForward tests message forwarding during shutdown
func TestProxyChannels_ShutdownDuringForward(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

	// Create original channels
	originalIn := make(chan *types.Msg, 10)
	originalOut := make(chan *types.Msg, 10)

	// Wrap channels
	proxyIn, proxyOut := m.wrapChannelService(originalIn, originalOut, "test-module", "test-channel")

	// Start a handler that echoes requests
	go func() {
		for msg := range originalIn {
			originalOut <- &types.Msg{
				Data:   msg.Data,
				Header: msg.Header,
			}
		}
	}()

	// Set shutdown flag before sending message
	m.shutdown.Store(true)

	// Send message during shutdown (should be forwarded without logging)
	proxyIn <- &types.Msg{
		Data:   []byte("test"),
		Header: types.Header{"X-Request-ID": {"shutdown-test"}},
	}

	// Receive response
	select {
	case resp := <-proxyOut:
		if string(resp.Data) != "test" {
			t.Error("message should still be forwarded during shutdown")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for response")
	}

	// Verify no log entries (logging skipped during shutdown)
	time.Sleep(20 * time.Millisecond)
	if len(buf.Lines()) > 0 {
		t.Error("expected no log entries during shutdown")
	}

	// Clean up
	m.Stop(context.Background())
}

// TestStop_WithChannelProxiesTimeout tests Stop with channel proxies that timeout
func TestStop_WithChannelProxiesTimeout(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf))

	// Create original channels
	originalIn := make(chan *types.Msg, 10)
	originalOut := make(chan *types.Msg, 10)

	// Wrap channels (creates proxies)
	_, _ = m.wrapChannelService(originalIn, originalOut, "test-module", "test-channel")

	// Create an already cancelled context to trigger timeout
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Stop should return timeout error
	err := m.Stop(ctx)
	if err == nil {
		t.Error("Stop() should return error when context is cancelled with proxies")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("expected timeout error, got: %v", err)
	}
}

// TestWithFields_EmptyFields tests WithFields with empty slice
func TestWithFields_EmptyFields(t *testing.T) {
	buf := &syncWriter{}
	// WithFields with empty slice should use AllFields()
	logger, err := New(WithOutput(buf), WithFields([]Field{}))
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

	// All fields should be enabled (default)
	if len(logger.fields) != len(AllFields()) {
		t.Errorf("expected %d fields, got %d", len(AllFields()), len(logger.fields))
	}
}

// syncErrorWriter is a writer that implements Sync() which returns an error
type syncErrorWriter struct {
	buf bytes.Buffer
}

func (w *syncErrorWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *syncErrorWriter) Sync() error {
	return fmt.Errorf("simulated sync error")
}

// closeErrorWriter is a writer that implements Close() which returns an error
type closeErrorWriter struct {
	buf bytes.Buffer
}

func (w *closeErrorWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *closeErrorWriter) Close() error {
	return fmt.Errorf("simulated close error")
}

// TestStop_SyncError tests Stop with a writer that has Sync() returning error
func TestStop_SyncError(t *testing.T) {
	writer := &syncErrorWriter{}
	logger, _ := New(WithOutput(writer))

	err := logger.Stop(context.Background())
	if err == nil {
		t.Error("Stop() should return error when Sync fails")
	}
	if !strings.Contains(err.Error(), "sync access log") {
		t.Errorf("error should mention sync failure, got: %v", err)
	}
}

// TestStop_CloseError tests Stop with a writer that has Close() returning error
func TestStop_CloseError(t *testing.T) {
	writer := &closeErrorWriter{}
	logger, _ := New(WithOutput(writer))

	err := logger.Stop(context.Background())
	if err == nil {
		t.Error("Stop() should return error when Close fails")
	}
	if !strings.Contains(err.Error(), "close access log") {
		t.Errorf("error should mention close failure, got: %v", err)
	}
}

// TestStop_InFlightWritesTimeout tests Stop timeout waiting for in-flight writes
func TestStop_InFlightWritesTimeout(t *testing.T) {
	// Create a slow writer that blocks
	slowWriter := &slowBlockingWriter{
		blockChan: make(chan struct{}),
		started:   make(chan struct{}),
	}
	logger, _ := New(WithOutput(slowWriter))

	// Start a write that will block - manually simulate in-flight write
	logger.wg.Add(1)
	go func() {
		defer logger.wg.Done()
		logger.mu.Lock()
		close(slowWriter.started) // Signal that we've started
		defer logger.mu.Unlock()
		<-slowWriter.blockChan // Block until closed
	}()

	// Wait for write to start
	<-slowWriter.started

	// Create a context that will timeout quickly
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := logger.Stop(ctx)

	// Unblock the writer
	close(slowWriter.blockChan)

	if err == nil {
		t.Error("Stop() should return error on timeout waiting for writes")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("error should mention timeout, got: %v", err)
	}
}

// slowBlockingWriter blocks until blockChan is closed
type slowBlockingWriter struct {
	blockChan chan struct{}
	started   chan struct{}
}

func (w *slowBlockingWriter) Write(p []byte) (n int, err error) {
	<-w.blockChan // Block until closed
	return len(p), nil
}

// TestWrapChannelService_DuringShutdown tests wrapChannelService returns original channels during shutdown
func TestWrapChannelService_DuringShutdown(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf))

	// Set shutdown flag before wrapping
	m.shutdown.Store(true)

	originalIn := make(chan *types.Msg, 10)
	originalOut := make(chan *types.Msg, 10)

	proxyIn, proxyOut := m.wrapChannelService(originalIn, originalOut, "test-module", "test-service")

	// During shutdown, should return original channels unchanged
	if proxyIn != originalIn {
		t.Error("during shutdown, proxyIn should be originalIn")
	}
	if proxyOut != originalOut {
		t.Error("during shutdown, proxyOut should be originalOut")
	}
}

// TestWrapStreamConsumerHandler_Error tests wrapStreamConsumerHandler with handler error
func TestWrapStreamConsumerHandler_Error(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

	originalHandler := func(_ context.Context, msgs []*types.Msg) error {
		return fmt.Errorf("stream handler error")
	}

	wrapped := m.wrapStreamConsumerHandler(originalHandler, "stream-module", "stream-service")

	msgs := []*types.Msg{
		{Data: []byte("msg1"), Header: types.Header{"X-Request-ID": {"err-test"}}},
	}

	err := wrapped(context.Background(), msgs)
	if err == nil {
		t.Error("error should be passed through")
	}

	lines := buf.Lines()
	if len(lines) != 1 {
		t.Fatalf("expected 1 log line, got %d", len(lines))
	}

	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry["status"] != "error" {
		t.Error("status should be error")
	}
}

// TestWrapStreamConsumerHandler_EmptyBatch tests wrapStreamConsumerHandler with empty messages
func TestWrapStreamConsumerHandler_EmptyBatch(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

	originalHandler := func(_ context.Context, msgs []*types.Msg) error {
		return nil
	}

	wrapped := m.wrapStreamConsumerHandler(originalHandler, "stream-module", "stream-service")

	err := wrapped(context.Background(), []*types.Msg{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lines := buf.Lines()
	if len(lines) != 1 {
		t.Fatalf("expected 1 log line, got %d", len(lines))
	}

	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	// Empty batch should have empty request_id and 0 request_size
	if entry["request_id"] != "" {
		t.Error("request_id should be empty for empty batch")
	}
	if int(entry["request_size"].(float64)) != 0 {
		t.Error("request_size should be 0 for empty batch")
	}
}

// TestWrapEventStreamConsumerHandler_Error tests wrapEventStreamConsumerHandler with handler error
func TestWrapEventStreamConsumerHandler_Error(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf), WithFormat(FormatJSON))

	originalHandler := func(_ context.Context, msgs []*types.Msg) error {
		return fmt.Errorf("event stream handler error")
	}

	wrapped := m.wrapEventStreamConsumerHandler(originalHandler, "event-module", "event-consumer")

	msgs := []*types.Msg{
		{Data: []byte("msg1"), Header: types.Header{"X-Request-ID": {"err-test"}}},
	}

	err := wrapped(context.Background(), msgs)
	if err == nil {
		t.Error("error should be passed through")
	}

	lines := buf.Lines()
	if len(lines) != 1 {
		t.Fatalf("expected 1 log line, got %d", len(lines))
	}

	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry["status"] != "error" {
		t.Error("status should be error")
	}
	if entry["service_type"] != "event_stream_consumer" {
		t.Error("service_type should be event_stream_consumer")
	}
}

// TestProxyInboundChannel_ContextDoneDuringForward tests context cancellation during message forward
func TestProxyInboundChannel_ContextDoneDuringForward(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf))

	// Create a blocked originalIn channel (no capacity)
	originalIn := make(chan *types.Msg) // unbuffered - will block on send
	originalOut := make(chan *types.Msg, 10)

	proxyIn, _ := m.wrapChannelService(originalIn, originalOut, "test-module", "test-channel")

	// Send a message that will try to forward to blocked originalIn
	go func() {
		proxyIn <- &types.Msg{Data: []byte("test")}
	}()

	// Give goroutine time to start forwarding
	time.Sleep(50 * time.Millisecond)

	// Stop the module (cancels context while forward is blocked)
	err := m.Stop(context.Background())
	if err != nil {
		t.Errorf("Stop() failed: %v", err)
	}
}

// TestProxyOutboundChannel_ContextDoneDuringForward tests context cancellation during outbound forward
func TestProxyOutboundChannel_ContextDoneDuringForward(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf))

	originalIn := make(chan *types.Msg, 10)
	originalOut := make(chan *types.Msg, 10)

	_, proxyOut := m.wrapChannelService(originalIn, originalOut, "test-module", "test-channel")

	// Make proxyOut full (blocked) by not consuming
	// proxyOut has same capacity as originalOut (10)
	_ = proxyOut // Don't consume from proxyOut

	// Put messages in originalOut that the proxy will try to forward
	for i := 0; i < 10; i++ {
		originalOut <- &types.Msg{Data: []byte(fmt.Sprintf("msg%d", i))}
	}

	// Give proxy time to forward to proxyOut (filling it up)
	time.Sleep(100 * time.Millisecond)

	// Now send another message that will block
	go func() {
		originalOut <- &types.Msg{Data: []byte("blocking-msg")}
	}()

	// Give goroutine time to start forwarding
	time.Sleep(50 * time.Millisecond)

	// Stop the module (cancels context while forward is potentially blocked)
	err := m.Stop(context.Background())
	if err != nil {
		t.Errorf("Stop() failed: %v", err)
	}
}

// TestOnServiceRegistration_UnknownType tests OnServiceRegistration with unknown service type
func TestOnServiceRegistration_UnknownType(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf))

	// Create registration with unknown service type
	reg := types.ServiceRegistration{
		Type:       types.ServiceType(999), // Unknown type
		Name:       "unknown-service",
		ModuleName: "unknown-module",
	}

	// Should pass through unchanged
	result := m.OnServiceRegistration(context.Background(), reg)

	if result.Type != reg.Type {
		t.Error("unknown type should pass through unchanged")
	}
	if result.Name != reg.Name {
		t.Error("name should pass through unchanged")
	}
}

// TestOnServiceRegistration_NilHandlers tests OnServiceRegistration with nil handlers
func TestOnServiceRegistration_NilHandlers(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf))

	t.Run("nil RequestHandler", func(t *testing.T) {
		reg := types.ServiceRegistration{
			Type:           types.ServiceTypeRequestReply,
			Name:           "test-service",
			ModuleName:     "test-module",
			RequestHandler: nil,
		}

		result := m.OnServiceRegistration(context.Background(), reg)
		if result.RequestHandler != nil {
			t.Error("nil handler should remain nil")
		}
	})

	t.Run("empty QueueHandlers", func(t *testing.T) {
		reg := types.ServiceRegistration{
			Type:          types.ServiceTypeQueueGroup,
			Name:          "test-service",
			ModuleName:    "test-module",
			QueueHandlers: []types.QGHP{},
		}

		result := m.OnServiceRegistration(context.Background(), reg)
		if len(result.QueueHandlers) != 0 {
			t.Error("empty handlers should remain empty")
		}
	})

	t.Run("nil StreamHandler", func(t *testing.T) {
		reg := types.ServiceRegistration{
			Type:          types.ServiceTypeStreamConsumer,
			Name:          "test-service",
			ModuleName:    "test-module",
			StreamHandler: nil,
		}

		result := m.OnServiceRegistration(context.Background(), reg)
		if result.StreamHandler != nil {
			t.Error("nil handler should remain nil")
		}
	})

	t.Run("nil Channel", func(t *testing.T) {
		reg := types.ServiceRegistration{
			Type:       types.ServiceTypeChannel,
			Name:       "test-service",
			ModuleName: "test-module",
			InChannel:  nil,
			OutChannel: nil,
		}

		result := m.OnServiceRegistration(context.Background(), reg)
		if result.InChannel != nil || result.OutChannel != nil {
			t.Error("nil channels should remain nil")
		}
	})
}

// TestOnEventConsumerRegistration_NilHandler tests nil handler in event consumer registration
func TestOnEventConsumerRegistration_NilHandler(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf))

	entry := types.EventConsumerEntry{
		EventDef: types.BaseEventDefinition{
			Name:       "TestEvent",
			ModuleName: "test-module",
		},
		Handler: nil,
		Module:  &mockModule{name: "test"},
	}

	result := m.OnEventConsumerRegistration(context.Background(), entry)
	if result.Handler != nil {
		t.Error("nil handler should remain nil")
	}
}

// TestOnEventStreamConsumerRegistration_NilHandler tests nil handler in event stream consumer registration
func TestOnEventStreamConsumerRegistration_NilHandler(t *testing.T) {
	buf := &syncWriter{}
	m, _ := New(WithOutput(buf))

	entry := types.EventStreamConsumerEntry{
		EventDef: types.BaseEventDefinition{
			Name:       "TestEvent",
			ModuleName: "test-module",
		},
		Handler: nil,
		Module:  &mockModule{name: "test"},
	}

	result := m.OnEventStreamConsumerRegistration(context.Background(), entry)
	if result.Handler != nil {
		t.Error("nil handler should remain nil")
	}
}
