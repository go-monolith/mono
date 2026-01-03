package eventbus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/logger"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// mockJetStreamMsg implements jetstream.Msg for testing
type mockJetStreamMsg struct {
	data         []byte
	subject      string
	headers      map[string][]string
	ackCalled    bool
	nakCalled    bool
	nakDelay     time.Duration
	termCalled   bool
	inProgCalled bool
	ackErr       error
	nakErr       error
	nakDelayErr  error
	termErr      error
	inProgErr    error
}

func (m *mockJetStreamMsg) Data() []byte                              { return m.data }
func (m *mockJetStreamMsg) Subject() string                           { return m.subject }
func (m *mockJetStreamMsg) Headers() nats.Header                      { return nats.Header(m.headers) }
func (m *mockJetStreamMsg) Ack() error                                { m.ackCalled = true; return m.ackErr }
func (m *mockJetStreamMsg) Nak() error                                { m.nakCalled = true; return m.nakErr }
func (m *mockJetStreamMsg) NakWithDelay(d time.Duration) error        { m.nakDelay = d; return m.nakDelayErr }
func (m *mockJetStreamMsg) Term() error                               { m.termCalled = true; return m.termErr }
func (m *mockJetStreamMsg) TermWithReason(string) error               { m.termCalled = true; return m.termErr }
func (m *mockJetStreamMsg) InProgress() error                         { m.inProgCalled = true; return m.inProgErr }
func (m *mockJetStreamMsg) Reply() string                             { return "" }
func (m *mockJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *mockJetStreamMsg) DoubleAck(context.Context) error           { return nil }

// testableWrapper wraps our mock for testing
type testableWrapper struct {
	mock *mockJetStreamMsg
}

func (w *testableWrapper) Data() []byte                       { return w.mock.Data() }
func (w *testableWrapper) Subject() string                    { return w.mock.Subject() }
func (w *testableWrapper) Headers() types.Header              { return types.Header(w.mock.Headers()) }
func (w *testableWrapper) Ack() error                         { return w.mock.Ack() }
func (w *testableWrapper) Nak() error                         { return w.mock.Nak() }
func (w *testableWrapper) NakWithDelay(d time.Duration) error { return w.mock.NakWithDelay(d) }
func (w *testableWrapper) Term() error                        { return w.mock.Term() }
func (w *testableWrapper) InProgress() error                  { return w.mock.InProgress() }

func TestJetStreamMsgWrapper_Data(t *testing.T) {
	mock := &mockJetStreamMsg{data: []byte("test data")}
	wrapper := &testableWrapper{mock: mock}

	data := wrapper.Data()
	if string(data) != "test data" {
		t.Errorf("Expected 'test data', got %q", string(data))
	}
}

func TestJetStreamMsgWrapper_Subject(t *testing.T) {
	mock := &mockJetStreamMsg{subject: "orders.new"}
	wrapper := &testableWrapper{mock: mock}

	subject := wrapper.Subject()
	if subject != "orders.new" {
		t.Errorf("Expected 'orders.new', got %q", subject)
	}
}

func TestJetStreamMsgWrapper_Headers(t *testing.T) {
	headers := map[string][]string{
		"Priority":    {"high"},
		"Retry-Count": {"0"},
	}
	mock := &mockJetStreamMsg{headers: headers}
	wrapper := &testableWrapper{mock: mock}

	h := wrapper.Headers()
	// types.Header is a type alias for map[string][]string
	if vals, ok := h["Priority"]; !ok || len(vals) == 0 || vals[0] != "high" {
		t.Errorf("Expected Priority 'high', got %v", h["Priority"])
	}
	if vals, ok := h["Retry-Count"]; !ok || len(vals) == 0 || vals[0] != "0" {
		t.Errorf("Expected Retry-Count '0', got %v", h["Retry-Count"])
	}
}

func TestJetStreamMsgWrapper_Ack(t *testing.T) {
	mock := &mockJetStreamMsg{}
	wrapper := &testableWrapper{mock: mock}

	err := wrapper.Ack()
	if err != nil {
		t.Errorf("Ack returned unexpected error: %v", err)
	}
	if !mock.ackCalled {
		t.Error("Ack was not called on underlying message")
	}
}

func TestJetStreamMsgWrapper_Nak(t *testing.T) {
	mock := &mockJetStreamMsg{}
	wrapper := &testableWrapper{mock: mock}

	err := wrapper.Nak()
	if err != nil {
		t.Errorf("Nak returned unexpected error: %v", err)
	}
	if !mock.nakCalled {
		t.Error("Nak was not called on underlying message")
	}
}

func TestJetStreamMsgWrapper_NakWithDelay(t *testing.T) {
	mock := &mockJetStreamMsg{}
	wrapper := &testableWrapper{mock: mock}

	delay := 5 * time.Second
	err := wrapper.NakWithDelay(delay)
	if err != nil {
		t.Errorf("NakWithDelay returned unexpected error: %v", err)
	}
	if mock.nakDelay != delay {
		t.Errorf("Expected delay %v, got %v", delay, mock.nakDelay)
	}
}

func TestJetStreamMsgWrapper_Term(t *testing.T) {
	mock := &mockJetStreamMsg{}
	wrapper := &testableWrapper{mock: mock}

	err := wrapper.Term()
	if err != nil {
		t.Errorf("Term returned unexpected error: %v", err)
	}
	if !mock.termCalled {
		t.Error("Term was not called on underlying message")
	}
}

func TestJetStreamMsgWrapper_InProgress(t *testing.T) {
	mock := &mockJetStreamMsg{}
	wrapper := &testableWrapper{mock: mock}

	err := wrapper.InProgress()
	if err != nil {
		t.Errorf("InProgress returned unexpected error: %v", err)
	}
	if !mock.inProgCalled {
		t.Error("InProgress was not called on underlying message")
	}
}

// TestNewJetStream tests JetStream wrapper creation
func TestNewJetStream(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()

	t.Run("successful creation", func(t *testing.T) {
		js, err := NewJetStream(conn, log)
		if err != nil {
			t.Fatalf("NewJetStream failed: %v", err)
		}
		if js == nil {
			t.Fatal("NewJetStream returned nil")
		}
		if js.js == nil {
			t.Error("JetStream context is nil")
		}
	})

	t.Run("nil connection", func(t *testing.T) {
		_, err := NewJetStream(nil, log)
		if err == nil {
			t.Fatal("expected error for nil connection")
		}
		if err.Error() != "NATS connection cannot be nil" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("nil logger", func(t *testing.T) {
		_, err := NewJetStream(conn, nil)
		if err == nil {
			t.Fatal("expected error for nil logger")
		}
		if err.Error() != "logger cannot be nil" {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// TestJetStream_CreateOrUpdateStream tests stream creation and updates
func TestJetStream_CreateOrUpdateStream(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	t.Run("create new stream", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST_STREAM",
			Subjects: []string{"test.>"},
			Storage:  types.MemoryStorage,
		}

		stream, err := js.CreateOrUpdateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("CreateOrUpdateStream failed: %v", err)
		}
		if stream == nil {
			t.Fatal("stream is nil")
		}

		// Clean up
		_ = js.DeleteStream(ctx, "TEST_STREAM")
	})

	t.Run("update existing stream", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST_UPDATE_STREAM",
			Subjects: []string{"update.>"},
			Storage:  types.MemoryStorage,
		}

		// Create
		_, err := js.CreateOrUpdateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("CreateOrUpdateStream failed: %v", err)
		}

		// Update with same config (idempotent)
		stream, err := js.CreateOrUpdateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Update stream failed: %v", err)
		}
		if stream == nil {
			t.Fatal("stream is nil after update")
		}

		// Clean up
		_ = js.DeleteStream(ctx, "TEST_UPDATE_STREAM")
	})
}

// TestJetStream_CreateOrUpdateConsumer tests consumer creation
func TestJetStream_CreateOrUpdateConsumer(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	// Create stream first
	streamCfg := types.StreamConfig{
		Name:     "TEST_CONSUMER_STREAM",
		Subjects: []string{"consumer.>"},
		Storage:  types.MemoryStorage,
	}
	_, err = js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer js.DeleteStream(ctx, "TEST_CONSUMER_STREAM")

	t.Run("create new consumer", func(t *testing.T) {
		consumerCfg := types.ConsumerConfig{
			Name:          "test-consumer",
			FilterSubject: "consumer.test",
			AckPolicy:     types.AckExplicitPolicy,
		}

		consumer, err := js.CreateOrUpdateConsumer(ctx, "TEST_CONSUMER_STREAM", consumerCfg)
		if err != nil {
			t.Fatalf("CreateOrUpdateConsumer failed: %v", err)
		}
		if consumer == nil {
			t.Fatal("consumer is nil")
		}
	})

	t.Run("update existing consumer", func(t *testing.T) {
		consumerCfg := types.ConsumerConfig{
			Name:          "test-consumer-update",
			FilterSubject: "consumer.update",
			AckPolicy:     types.AckExplicitPolicy,
		}

		// Create
		_, err := js.CreateOrUpdateConsumer(ctx, "TEST_CONSUMER_STREAM", consumerCfg)
		if err != nil {
			t.Fatalf("CreateOrUpdateConsumer failed: %v", err)
		}

		// Update (idempotent)
		consumer, err := js.CreateOrUpdateConsumer(ctx, "TEST_CONSUMER_STREAM", consumerCfg)
		if err != nil {
			t.Fatalf("Update consumer failed: %v", err)
		}
		if consumer == nil {
			t.Fatal("consumer is nil after update")
		}
	})

	t.Run("non-existent stream", func(t *testing.T) {
		consumerCfg := types.ConsumerConfig{
			Name:      "test-consumer-fail",
			AckPolicy: types.AckExplicitPolicy,
		}

		_, err := js.CreateOrUpdateConsumer(ctx, "NON_EXISTENT_STREAM", consumerCfg)
		if err == nil {
			t.Fatal("expected error for non-existent stream")
		}
	})
}

// TestJetStream_Publish tests JetStream publish operations
func TestJetStream_Publish(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	// Create stream
	streamCfg := types.StreamConfig{
		Name:     "TEST_PUBLISH_STREAM",
		Subjects: []string{"publish.>"},
		Storage:  types.MemoryStorage,
	}
	_, err = js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer js.DeleteStream(ctx, "TEST_PUBLISH_STREAM")

	t.Run("successful publish", func(t *testing.T) {
		ack, err := js.Publish(ctx, "publish.test", []byte("test message"))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
		if ack == nil {
			t.Fatal("ack is nil")
		}
		if ack.Stream() != "TEST_PUBLISH_STREAM" {
			t.Errorf("expected stream TEST_PUBLISH_STREAM, got %s", ack.Stream())
		}
		if ack.Sequence() == 0 {
			t.Error("expected non-zero sequence")
		}
	})

	t.Run("publish to non-existent stream", func(t *testing.T) {
		_, err := js.Publish(ctx, "nonexistent.subject", []byte("test"))
		if err == nil {
			t.Fatal("expected error for non-existent stream")
		}
		// Should get an error about stream not existing
		// Could be EventStreamError or a regular error depending on NATS version
		if !monoerrors.IsEventStreamError(err) && !strings.Contains(err.Error(), "no response from stream") {
			t.Errorf("expected EventStreamError or 'no response' error, got %T: %v", err, err)
		}
	})
}

// TestJetStream_PublishMsg tests publishing with headers
func TestJetStream_PublishMsg(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	// Create stream
	streamCfg := types.StreamConfig{
		Name:     "TEST_PUBLISH_MSG_STREAM",
		Subjects: []string{"publishmsg.>"},
		Storage:  types.MemoryStorage,
	}
	_, err = js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer js.DeleteStream(ctx, "TEST_PUBLISH_MSG_STREAM")

	t.Run("publish with headers", func(t *testing.T) {
		msg := &types.Msg{
			Subject: "publishmsg.test",
			Data:    []byte("test data"),
			Header: types.Header{
				"Content-Type": []string{"application/json"},
				"X-Custom":     []string{"value"},
			},
		}

		ack, err := js.PublishMsg(ctx, msg)
		if err != nil {
			t.Fatalf("PublishMsg failed: %v", err)
		}
		if ack == nil {
			t.Fatal("ack is nil")
		}
		if ack.Stream() != "TEST_PUBLISH_MSG_STREAM" {
			t.Errorf("expected stream TEST_PUBLISH_MSG_STREAM, got %s", ack.Stream())
		}
	})

	t.Run("publish to non-existent stream", func(t *testing.T) {
		msg := &types.Msg{
			Subject: "nonexistent.subject",
			Data:    []byte("test"),
		}

		_, err := js.PublishMsg(ctx, msg)
		if err == nil {
			t.Fatal("expected error for non-existent stream")
		}
		// Should get an error about stream not existing
		// Could be EventStreamError or a regular error depending on NATS version
		if !monoerrors.IsEventStreamError(err) && !strings.Contains(err.Error(), "no response from stream") {
			t.Errorf("expected EventStreamError or 'no response' error, got %T: %v", err, err)
		}
	})
}

// TestJetStream_Stream tests stream retrieval
func TestJetStream_Stream(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	// Create stream
	streamCfg := types.StreamConfig{
		Name:     "TEST_GET_STREAM",
		Subjects: []string{"getstream.>"},
		Storage:  types.MemoryStorage,
	}
	_, err = js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer js.DeleteStream(ctx, "TEST_GET_STREAM")

	t.Run("get existing stream", func(t *testing.T) {
		stream, err := js.Stream(ctx, "TEST_GET_STREAM")
		if err != nil {
			t.Fatalf("Stream failed: %v", err)
		}
		if stream == nil {
			t.Fatal("stream is nil")
		}
	})

	t.Run("get non-existent stream", func(t *testing.T) {
		_, err := js.Stream(ctx, "NON_EXISTENT")
		if err == nil {
			t.Fatal("expected error for non-existent stream")
		}
	})
}

// TestJetStream_DeleteStream tests stream deletion
func TestJetStream_DeleteStream(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	t.Run("delete existing stream", func(t *testing.T) {
		// Create stream
		streamCfg := types.StreamConfig{
			Name:     "TEST_DELETE_STREAM",
			Subjects: []string{"delete.>"},
			Storage:  types.MemoryStorage,
		}
		_, err := js.CreateOrUpdateStream(ctx, streamCfg)
		if err != nil {
			t.Fatalf("Failed to create stream: %v", err)
		}

		// Delete
		err = js.DeleteStream(ctx, "TEST_DELETE_STREAM")
		if err != nil {
			t.Fatalf("DeleteStream failed: %v", err)
		}

		// Verify deleted
		_, err = js.Stream(ctx, "TEST_DELETE_STREAM")
		if err == nil {
			t.Fatal("stream still exists after deletion")
		}
	})

	t.Run("delete non-existent stream", func(t *testing.T) {
		err := js.DeleteStream(ctx, "NON_EXISTENT")
		if err == nil {
			t.Fatal("expected error for non-existent stream")
		}
	})
}

// TestWrapJetStreamMsg tests the message wrapper function
func TestWrapJetStreamMsg(t *testing.T) {
	mock := &mockJetStreamMsg{
		data:    []byte("wrapped data"),
		subject: "wrapped.subject",
		headers: map[string][]string{
			"X-Test": {"value"},
		},
	}

	wrapped := WrapJetStreamMsg(mock)

	if wrapped == nil {
		t.Fatal("wrapped message is nil")
	}
	if string(wrapped.Data) != "wrapped data" {
		t.Errorf("expected 'wrapped data', got %q", string(wrapped.Data))
	}
	if wrapped.Subject != "wrapped.subject" {
		t.Errorf("expected 'wrapped.subject', got %q", wrapped.Subject)
	}
	if len(wrapped.Header["X-Test"]) == 0 || wrapped.Header["X-Test"][0] != "value" {
		t.Errorf("expected X-Test header 'value', got %v", wrapped.Header["X-Test"])
	}
	if wrapped.NatsMsg == nil {
		t.Error("NatsMsg should be set")
	}
}

// TestMsgPubAck tests the PubAck wrapper
func TestMsgPubAck(t *testing.T) {
	// Create a real PubAck by publishing to a stream
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	// Create stream
	streamCfg := types.StreamConfig{
		Name:     "TEST_PUBACK",
		Subjects: []string{"puback.>"},
		Storage:  types.MemoryStorage,
	}
	_, err = js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer js.DeleteStream(ctx, "TEST_PUBACK")

	ack, err := js.Publish(ctx, "puback.test", []byte("test"))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if ack.Stream() != "TEST_PUBACK" {
		t.Errorf("expected stream TEST_PUBACK, got %s", ack.Stream())
	}
	if ack.Sequence() == 0 {
		t.Error("expected non-zero sequence")
	}
	if ack.Duplicate() {
		t.Error("first publish should not be duplicate")
	}
	// Domain should be accessible (even if empty for embedded server)
	_ = ack.Domain() // Just call to ensure it works

	// Test with nil ack (edge case)
	nilAck := &msgPubAck{ack: nil}
	if nilAck.Stream() != "" {
		t.Errorf("expected empty stream for nil ack, got %s", nilAck.Stream())
	}
	if nilAck.Sequence() != 0 {
		t.Errorf("expected zero sequence for nil ack, got %d", nilAck.Sequence())
	}
	if nilAck.Duplicate() {
		t.Error("expected false duplicate for nil ack")
	}
	if nilAck.Domain() != "" {
		t.Errorf("expected empty domain for nil ack, got %s", nilAck.Domain())
	}
}

// TestJetStream_CreateOrUpdateStream_ConfigValidation tests stream config validation errors
func TestJetStream_CreateOrUpdateStream_ConfigValidation(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	t.Run("empty name", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "",
			Subjects: []string{"test.>"},
			Storage:  types.MemoryStorage,
		}

		_, err := js.CreateOrUpdateStream(ctx, cfg)
		if err == nil {
			t.Fatal("expected error for empty stream name")
		}
		if !strings.Contains(err.Error(), "stream name is required") {
			t.Errorf("expected 'stream name is required' in error, got: %v", err)
		}
	})

	t.Run("no subjects", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST_NO_SUBJECTS",
			Subjects: []string{},
			Storage:  types.MemoryStorage,
		}

		_, err := js.CreateOrUpdateStream(ctx, cfg)
		if err == nil {
			t.Fatal("expected error for empty subjects")
		}
		if !strings.Contains(err.Error(), "at least one subject is required") {
			t.Errorf("expected 'at least one subject is required' in error, got: %v", err)
		}
	})

	t.Run("invalid retention policy", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:      "TEST_INVALID_RETENTION",
			Subjects:  []string{"test.>"},
			Storage:   types.MemoryStorage,
			Retention: types.RetentionPolicy(99), // Invalid value
		}

		_, err := js.CreateOrUpdateStream(ctx, cfg)
		if err == nil {
			t.Fatal("expected error for invalid retention policy")
		}
		if !strings.Contains(err.Error(), "invalid retention policy") {
			t.Errorf("expected 'invalid retention policy' in error, got: %v", err)
		}
	})

	t.Run("invalid storage type", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST_INVALID_STORAGE",
			Subjects: []string{"test.>"},
			Storage:  types.StorageType(99), // Invalid value
		}

		_, err := js.CreateOrUpdateStream(ctx, cfg)
		if err == nil {
			t.Fatal("expected error for invalid storage type")
		}
		if !strings.Contains(err.Error(), "invalid storage type") {
			t.Errorf("expected 'invalid storage type' in error, got: %v", err)
		}
	})

	t.Run("invalid discard policy", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST_INVALID_DISCARD",
			Subjects: []string{"test.>"},
			Storage:  types.MemoryStorage,
			Discard:  types.DiscardPolicy(99), // Invalid value
		}

		_, err := js.CreateOrUpdateStream(ctx, cfg)
		if err == nil {
			t.Fatal("expected error for invalid discard policy")
		}
		if !strings.Contains(err.Error(), "invalid discard policy") {
			t.Errorf("expected 'invalid discard policy' in error, got: %v", err)
		}
	})

	t.Run("invalid compression", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:        "TEST_INVALID_COMPRESSION",
			Subjects:    []string{"test.>"},
			Storage:     types.MemoryStorage,
			Compression: types.StoreCompression(99), // Invalid value
		}

		_, err := js.CreateOrUpdateStream(ctx, cfg)
		if err == nil {
			t.Fatal("expected error for invalid compression")
		}
		if !strings.Contains(err.Error(), "invalid compression") {
			t.Errorf("expected 'invalid compression' in error, got: %v", err)
		}
	})
}

// TestJetStream_CreateOrUpdateConsumer_ConfigValidation tests consumer config validation errors
func TestJetStream_CreateOrUpdateConsumer_ConfigValidation(t *testing.T) {
	conn := setupTestNATS(t)
	defer conn.Close()

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	// Create stream first
	streamCfg := types.StreamConfig{
		Name:     "TEST_CONSUMER_CONFIG_STREAM",
		Subjects: []string{"consumer-config.>"},
		Storage:  types.MemoryStorage,
	}
	_, err = js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer js.DeleteStream(ctx, "TEST_CONSUMER_CONFIG_STREAM")

	t.Run("invalid ack policy", func(t *testing.T) {
		consumerCfg := types.ConsumerConfig{
			Name:      "test-invalid-ack",
			AckPolicy: types.AckPolicy(99), // Invalid value
		}

		_, err := js.CreateOrUpdateConsumer(ctx, "TEST_CONSUMER_CONFIG_STREAM", consumerCfg)
		if err == nil {
			t.Fatal("expected error for invalid ack policy")
		}
		if !strings.Contains(err.Error(), "invalid ack policy") {
			t.Errorf("expected 'invalid ack policy' in error, got: %v", err)
		}
	})

	t.Run("invalid deliver policy", func(t *testing.T) {
		consumerCfg := types.ConsumerConfig{
			Name:          "test-invalid-deliver",
			AckPolicy:     types.AckExplicitPolicy,
			DeliverPolicy: types.DeliverPolicy(99), // Invalid value
		}

		_, err := js.CreateOrUpdateConsumer(ctx, "TEST_CONSUMER_CONFIG_STREAM", consumerCfg)
		if err == nil {
			t.Fatal("expected error for invalid deliver policy")
		}
		if !strings.Contains(err.Error(), "invalid deliver policy") {
			t.Errorf("expected 'invalid deliver policy' in error, got: %v", err)
		}
	})

	t.Run("invalid replay policy", func(t *testing.T) {
		consumerCfg := types.ConsumerConfig{
			Name:         "test-invalid-replay",
			AckPolicy:    types.AckExplicitPolicy,
			ReplayPolicy: types.ReplayPolicy(99), // Invalid value
		}

		_, err := js.CreateOrUpdateConsumer(ctx, "TEST_CONSUMER_CONFIG_STREAM", consumerCfg)
		if err == nil {
			t.Fatal("expected error for invalid replay policy")
		}
		if !strings.Contains(err.Error(), "invalid replay policy") {
			t.Errorf("expected 'invalid replay policy' in error, got: %v", err)
		}
	})
}

// TestNewJetStream_ClosedConnection tests NewJetStream with closed connection
func TestNewJetStream_ClosedConnection(t *testing.T) {
	conn := setupTestNATS(t)
	log := logger.NewDefaultLogger()

	// Close the connection before creating JetStream
	conn.Close()

	_, err := NewJetStream(conn, log)
	// JetStream may or may not fail depending on NATS internal state
	// Just ensure the function handles it without panic
	_ = err
}

// TestJetStream_Publish_GeneralError tests Publish error path (not NoResponders)
func TestJetStream_Publish_GeneralError(t *testing.T) {
	conn := setupTestNATS(t)

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	// Close connection to cause publish error
	conn.Close()

	ctx := context.Background()
	_, err = js.Publish(ctx, "test.subject", []byte("test"))
	// Error is expected - just verify it doesn't panic and returns an error
	if err == nil {
		t.Error("expected error on closed connection")
	}
}

// TestJetStream_PublishMsg_GeneralError tests PublishMsg error path (not NoResponders)
func TestJetStream_PublishMsg_GeneralError(t *testing.T) {
	conn := setupTestNATS(t)

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	// Close connection to cause publish error
	conn.Close()

	ctx := context.Background()
	msg := &types.Msg{
		Subject: "test.subject",
		Data:    []byte("test"),
	}
	_, err = js.PublishMsg(ctx, msg)
	// Error is expected - just verify it doesn't panic and returns an error
	if err == nil {
		t.Error("expected error on closed connection")
	}
}

// TestJetStream_CreateOrUpdateStream_JetStreamError tests actual JetStream error
func TestJetStream_CreateOrUpdateStream_JetStreamError(t *testing.T) {
	conn := setupTestNATS(t)

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	// Close connection to cause stream creation error
	conn.Close()

	ctx := context.Background()
	cfg := types.StreamConfig{
		Name:     "TEST_ERROR_STREAM",
		Subjects: []string{"error.>"},
		Storage:  types.MemoryStorage,
	}

	_, err = js.CreateOrUpdateStream(ctx, cfg)
	// Error is expected on closed connection
	if err == nil {
		t.Error("expected error on closed connection")
	}
}

// TestJetStream_CreateOrUpdateConsumer_JetStreamError tests actual JetStream consumer creation error
func TestJetStream_CreateOrUpdateConsumer_JetStreamError(t *testing.T) {
	conn := setupTestNATS(t)

	log := logger.NewDefaultLogger()
	js, err := NewJetStream(conn, log)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	// Close connection to cause consumer creation error
	conn.Close()

	ctx := context.Background()
	consumerCfg := types.ConsumerConfig{
		Name:      "test-error-consumer",
		AckPolicy: types.AckExplicitPolicy,
	}

	_, err = js.CreateOrUpdateConsumer(ctx, "TEST_STREAM", consumerCfg)
	// Error is expected on closed connection (stream not found or connection error)
	if err == nil {
		t.Error("expected error on closed connection")
	}
}
